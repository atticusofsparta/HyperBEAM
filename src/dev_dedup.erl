%%% @doc A device that deduplicates messages in an evaluation stream, returning
%%% status `skip' if the message has already been seen.
%%%
%%% This device is typically used to ensure that a message is only executed
%%% once, even if assigned multiple times, upon a `~process@1.0' evaluation.
%%% It can, however, be used in many other contexts.
%%%
%%% This device honors the `pass' key if it is present in the message. If so,
%%% it will only run on the first pass. Additionally, the device supports
%%% a `subject-key' key that allows the caller to specify the key whose ID
%%% should be used for deduplication. If the `subject-key' key is not present,
%%% the device will use the `body' of the request as the subject. If the key is
%%% set to `request', the device will use the entire request itself as the
%%% subject.
%%%
%%% This device runs on the first pass of the `compute' key call if executed
%%% in a stack, and not in subsequent passes.
%%%
%%% When a viable store is configured in Opts, dedup state is stored as flat
%%% LMDB key-value entries at `dedup/<ProcID>/<SubjectID>`. This is O(1) per
%%% check/write and does not grow the M1 snapshot. When no store is available
%%% (e.g., in unit tests), the device falls back to the legacy in-memory trie
%%% stored under the `dedup' key in M1.
-module(dev_dedup).
-export([info/1]).
-include_lib("eunit/include/eunit.hrl").
-include("include/hb.hrl").

info(_M1) ->
    #{
        default => fun handle/4,
        exclude => [keys, set, id, commit]
    }.

%% @doc Forward the keys and `set' functions to the message device, handle all
%% others with deduplication.
handle(<<"keys">>, M1, _M2, _Opts) ->
    dev_message:keys(M1);
handle(<<"set">>, M1, M2, Opts) ->
    dev_message:set(M1, M2, Opts);
handle(Key, M1, M2, Opts) ->
    ?event({dedup_handle, {key, Key}, {base, M1}, {req, M2}}),
    % Find the relevant parameters from the messages. We search for the
    % `dedup-subject' key in the first message, and use that value as the key
    % to look for in the second message.
    SubjectKey =
        hb_ao:get_first(
            [
                {{as, <<"message@1.0">>, M1}, <<"dedup-subject">>},
                {{as, <<"message@1.0">>, M2}, <<"dedup-subject">>}
            ],
            <<"body">>,
            Opts
        ),
    % Get the subject of the second message.
    Subject =
        if SubjectKey == <<"request">> ->
            % The subject is the request itself.
            M2;
        true ->
            % The subject is the value of the subject key, which will have
            % defaulted to the `body' key if not set in the base message.
            hb_ao:get_first(
                [
                    {{as, <<"message@1.0">>, M1}, SubjectKey},
                    {{as, <<"message@1.0">>, M2}, SubjectKey}
                ],
                Opts
            )
        end,
    % Is this the first pass, if we are executing in a stack?
    FirstPass = hb_ao:get(<<"pass">>, {as, <<"message@1.0">>, M1}, 1, Opts) == 1,
    ?event({dedup_handle,
        {key, Key},
        {base, M1},
        {req, M2},
        {subject_key, SubjectKey},
        {subject, Subject}
    }),
    case {FirstPass, Subject} of
        {false, _} ->
            % If this is not the first pass, we can skip the deduplication
            % check.
            {ok, M1};
        {true, not_found} ->
            % If the subject key is not present, we can skip the deduplication
            % check.
            {ok, M1};
        {true, _} ->
            SubjectID = hb_message:id(Subject, signed, Opts),
            % Direct map lookup for the process key — see safe_proc_id/2 for
            % why we cannot use hb_ao:get here.
            RawProcess = maps:get(<<"process">>, M1, not_found),
            Store = hb_opts:get(store, no_viable_store, Opts),
            case {is_viable_store(Store), RawProcess} of
                {_, not_found} ->
                    % No stable process key: ProcID would change every slot.
                    % Fall back to in-memory trie which lives inside M1.
                    dedup_with_trie(SubjectID, M1, M2, Opts);
                {false, _} ->
                    % No viable store configured.
                    dedup_with_trie(SubjectID, M1, M2, Opts);
                {true, _} ->
                    ProcID = safe_proc_id(RawProcess, Opts),
                    DedupKey = dedup_key(ProcID, SubjectID, Store),
                    dedup_with_store(DedupKey, SubjectID, M1, M2, Store, Opts)
            end
    end.

%% @doc Check whether a store value is viable (not a sentinel or empty list).
is_viable_store(no_viable_store) -> false;
is_viable_store([]) -> false;
is_viable_store(_) -> true.

%% @doc Compute the flat LMDB dedup key for a process/subject pair.
dedup_key(ProcID, SubjectID, Store) ->
    hb_store:path(Store, [<<"dedup">>, ProcID, SubjectID]).

%% @doc Compute a stable ProcID from the raw value found at the `<<"process">>'
%% key in M1.
%%
%% The caller must use `maps:get(<<"process">>, M1, ...)' directly — we cannot
%% call `hb_ao:get(<<"process">>, M1, ...)' here because M1 may carry a
%% `stack@1.0' device that includes `dev_dedup', which would re-enter
%% `handle/4' and loop.  The `id' key is in our `exclude' list, so
%% `hb_message:id' calls are safe even for stack messages.
safe_proc_id(Process, Opts) when is_map(Process) ->
    % Process is the signed process-definition sub-message; its device is
    % not a stack containing dev_dedup, so hb_message:id is safe.
    hb_message:id(Process, none, Opts);
safe_proc_id(ProcBin, _Opts) when is_binary(ProcBin) ->
    ProcBin.

%% @doc Dedup using flat LMDB key-value storage (O(1) per check/write).
%%
%% M1 is NOT updated — dedup state lives only in the store, not in the process
%% snapshot. On first-ever encounter the slot number is written at DedupKey.
%% A migration fallback reads the old in-memory trie so that processes that
%% already have a trie-based dedup snapshot continue to work correctly.
dedup_with_store(DedupKey, SubjectID, M1, M2, Store, Opts) ->
    case hb_store:read(Store, DedupKey) of
        {ok, _} ->
            ?event({already_seen_store, {subject, SubjectID}}),
            {skip, M1};
        _ ->
            % not_found (or transient failure) — check migration trie fallback.
            OldTrie =
                hb_ao:get(
                    <<"dedup">>,
                    {as, <<"message@1.0">>, M1},
                    not_found,
                    Opts
                ),
            AlreadySeen =
                case OldTrie of
                    not_found -> false;
                    T -> hb_ao:get(SubjectID, T, Opts) =/= not_found
                end,
            case AlreadySeen of
                true ->
                    {skip, M1};
                false ->
                    ?event({not_seen, SubjectID}),
                    Slot = hb_maps:get(<<"slot">>, M2, true, Opts),
                    hb_store:write(Store, DedupKey, hb_util:bin(Slot)),
                    % M1 is intentionally NOT updated; dedup state is in the store.
                    {ok, M1}
            end
    end.

%% @doc Dedup using the legacy in-memory trie stored in M1 under `dedup'.
%%
%% Used as a fallback when no store is configured (e.g. unit tests).
dedup_with_trie(SubjectID, M1, M2, Opts) ->
    DedupTrie =
        hb_ao:get(
            <<"dedup">>,
            {as, <<"message@1.0">>, M1},
            #{ <<"device">> => <<"trie@1.0">> },
            Opts
        ),
    ?event({dedup_checking, DedupTrie}),
    case hb_ao:get(SubjectID, DedupTrie, Opts) of
        not_found ->
            ?event({not_seen, SubjectID}),
            Slot =
                hb_maps:get(
                    <<"slot">>,
                    M2,
                    true,
                    Opts
                ),
            {ok, NewDedupTrie} =
                hb_ao:resolve(
                    DedupTrie,
                    #{ <<"path">> => <<"set">>, SubjectID => Slot },
                    Opts
                ),
            ?event({dedup_updated, NewDedupTrie}),
            hb_ao:resolve(
                M1,
                #{
                    <<"path">> => <<"set">>,
                    <<"set-mode">> => <<"explicit">>,
                    <<"dedup">> => NewDedupTrie
                },
                Opts
            );
        Value ->
            ?event(
                {already_seen,
                    {subject, SubjectID},
                    {dedup_value, Value}
                }
            ),
            {skip, M1}
    end.

%%% Tests

dedup_test() ->
    hb:init(),
    % Create a stack with a dedup device and 2 devices that will append to a
    % `Result' key.
	Msg = #{
		<<"device">> => <<"stack@1.0">>,
        <<"dedup-subject">> => <<"request">>,
		<<"device-stack">> =>
			#{
				<<"1">> => <<"dedup@1.0">>,
				<<"2">> => dev_stack:generate_append_device(<<"+D2">>),
				<<"3">> => dev_stack:generate_append_device(<<"+D3">>)
			},
		<<"result">> => <<"INIT">>
	},
    % Send the same message twice, with the same binary.
    {ok, Req} = hb_ao:resolve(Msg,
        #{ <<"path">> => <<"append">>, <<"bin">> => <<"_">> }, #{}),
    {ok, Res} = hb_ao:resolve(Req,
        #{ <<"path">> => <<"append">>, <<"bin">> => <<"_">> }, #{}),
    % Send the same message twice, with another binary.
    {ok, Msg4} = hb_ao:resolve(Res,
        #{ <<"path">> => <<"append">>, <<"bin">> => <<"/">> }, #{}),
    {ok, Msg5} = hb_ao:resolve(Msg4,
        #{ <<"path">> => <<"append">>, <<"bin">> => <<"/">> }, #{}),
    % Ensure that downstream devices have only seen each message once.
    ?assertMatch(
		#{ <<"result">> := <<"INIT+D2_+D3_+D2/+D3/">> },
		Msg5
	).

dedup_with_multipass_test() ->
    % Create a stack with a dedup device and 2 devices that will append to a
    % `Result' key and a `Multipass' device that will repeat the message for
    % an additional pass. We want to ensure that Multipass is not hindered by
    % the dedup device.
	Msg = #{
		<<"device">> => <<"stack@1.0">>,
        <<"dedup-subject">> => <<"request">>,
		<<"device-stack">> =>
			#{
				<<"1">> => <<"dedup@1.0">>,
				<<"2">> => dev_stack:generate_append_device(<<"+D2">>),
				<<"3">> => dev_stack:generate_append_device(<<"+D3">>),
                <<"4">> => <<"multipass@1.0">>
			},
		<<"result">> => <<"INIT">>,
        <<"passes">> => 2
	},
    % Send the same message twice, with the same binary.
    {ok, Req} = hb_ao:resolve(Msg, #{ <<"path">> => <<"append">>, <<"bin">> => <<"_">> }, #{}),
    {ok, Res} = hb_ao:resolve(Req, #{ <<"path">> => <<"append">>, <<"bin">> => <<"_">> }, #{}),
    % Send the same message twice, with another binary.
    {ok, Msg4} = hb_ao:resolve(Res, #{ <<"path">> => <<"append">>, <<"bin">> => <<"/">> }, #{}),
    {ok, Msg5} = hb_ao:resolve(Msg4, #{ <<"path">> => <<"append">>, <<"bin">> => <<"/">> }, #{}),
    % Ensure that downstream devices have only seen each message once.
    ?assertMatch(
		#{ <<"result">> := <<"INIT+D2_+D3_+D2_+D3_+D2/+D3/+D2/+D3/">> },
		Msg5
	).
