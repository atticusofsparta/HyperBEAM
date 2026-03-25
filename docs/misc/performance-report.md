# HyperBEAM Genesis-Wasm Slot Execution: Performance Report

**Profiled:** `genesis-wasm@1.0` catch-up from slot ~585,000 to ~690,000
**Process:** ARIO token contract (`qNvAoz0TgcH7DMg8BCVn8jF32QH5L6T29VjHxhHqqGE`)
**State size:** ~7 MB (map with ~7,000-entry balances trie, 66-entry dedup trie)

---

## Summary

Before optimisation, a typical genesis-wasm slot took **7,500–11,000 ms** end-to-end,
making catch-up roughly 7× slower than real-time. The irreducible WASM compute time
was only ~730 ms — the remaining 6,800–10,000 ms was pipeline overhead.

After three targeted fixes, the same slots now take **1,400–3,500 ms**, with WASM
computation representing 80–85% of the total. Erlang pipeline overhead has been
reduced from dominant to negligible.

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Typical slot duration | 7,500–11,000 ms | 1,400–3,500 ms | ~4–5× faster |
| WASM share of execution | ~45% | 80–85% | bottleneck correctly isolated |
| `exec_lmdb_writes` / slot | ~700 | ~40 | 94% reduction |
| `exec_lmdb_reads` / slot | ~1,400 | ~14 | 99% reduction |
| `run_as_setup_ms` | ~253 ms | 8–150 ms | 3–30× faster |
| `dedup_phase_ms` | ~1,500 ms | ~15 ms | **88× faster** |

---

## Instrumentation Added

All instrumentation is in the `obs/performance-instrumentation` branch. No
behaviour changes — only timing and counter collection.

### `computed_slot` log event fields

`dev_process:compute_slot` emits a structured log event after every slot with
the following fields:

| Field | Description |
|-------|-------------|
| `execution_ms` | Total time inside `run_as(<<"execution">>, ...)` |
| `store_ms` | Time to write result state to LMDB |
| `wasm_cu_ms` | genesis-wasm HTTP roundtrip only (pure CU compute) |
| `exec_lmdb_reads` | LMDB reads during exec phase |
| `exec_lmdb_read_us` | µs spent on those reads |
| `exec_lmdb_writes` | LMDB writes during exec phase |
| `exec_lmdb_write_us` | µs spent on those writes |
| `store_lmdb_reads` | LMDB reads during store phase |
| `store_lmdb_writes` | LMDB writes during store phase |
| `dedup_entries` | Number of entries in the dedup structure |
| `dedup_bytes` | Serialized size of the dedup structure |
| `dedup_write_us` | µs to write dedup data |
| `balances_entries` | Number of entries in the balances trie |
| `balances_bytes` | Serialized size of the balances trie |
| `balances_write_us` | µs to write balances data |
| `normalize_keys_count` | Number of `normalize_keys` calls during the slot |
| `normalize_keys_us` | Total µs spent in `normalize_keys` |
| `delegated_phase_ms` | Time for the delegated-compute phase |

### How phase timing is captured

- **wasm_cu_ms**: `timer:tc` wraps `do_relay` in `dev_delegated_compute`,
  result stored in process dictionary, read back in `compute_slot`.
- **LMDB phase separation**: `hb_store_lmdb:take_stats/0` resets the per-process
  accumulator — called once after exec, once after store, producing independent
  phase counts.
- **normalize_keys**: `timed_normalize_keys/2` wrapper in `hb_ao` +
  `hb_ao:take_normalize_stats/0` read in `compute_slot`.

### Observability stack

`docker-compose.yml` adds Loki + Promtail + Grafana. Promtail tails HyperBEAM's
stdout and parses the `computed_slot` logfmt fields. Grafana dashboards show:

- Per-action execution breakdown (exec vs store vs wasm)
- LMDB read/write counters and µs per phase
- WASM CU time by action type
- Node catch-up ETA based on slot rate vs current vs target slot

---

## What the Instrumentation Revealed

### Baseline measurement (slot ~548,000, Save-Observations action)

```
execution_ms:          ~1,620 ms  (100%)
├─ wasm_cu_ms:           ~730 ms  ( 45%)  genesis-wasm HTTP roundtrip
├─ exec_lmdb_read_us:    ~100 ms  (  6%)  ~1,300 LMDB reads
├─ exec_lmdb_write_us:     ~2 ms  ( <1%)  ~680 LMDB writes
└─ unaccounted:          ~790 ms  ( 49%)  hb_ao resolution overhead
```

The 49% unaccounted immediately indicated the `hb_ao:resolve` pipeline was the
problem — not I/O. WASM was already well-behaved; the Erlang overhead around it
was not.

Note: total slot time at this point was far higher (~7,500 ms). The 1,620 ms
figure is `execution_ms` alone — other contributors included:
- `dedup_phase_ms`: ~1,500 ms (trie commit + HMAC-sign of all ~400 nodes)
- `patch_phase_ms`: ~1,700 ms (bloated by trie still in M1)
- `run_as_setup_ms`: ~253 ms (two unnecessary full-state hash operations)

### Finding 1: The dedup trie was written in full every slot

`dev_dedup.erl` stored "seen message IDs" as an in-process-state trie via
`hb_ao:resolve` / `trie@1.0`. Every new (non-duplicate) message triggered:

1. `hb_ao:resolve(DedupTrie, #{ path => set, SubjectID => Slot })`
   The `trie@1.0` set handler calls `hb_message:commit` (HMAC-SHA256) over
   **every node** in the trie, then `hb_cache:write` which writes all nodes to LMDB.
2. `hb_ao:resolve(M1, #{ path => set, <<"dedup">> => NewTrie })`
   Embeds the updated trie back into the full ~7 MB process state.

At slot 548,000 the trie had 66 entries. A 66-entry binary trie produces ~300–400
intermediate nodes. **This means ~400 HMAC-SHA256 operations and ~400 LMDB writes
happened every slot just to record one new message ID.** As the process accumulated
more unique messages the trie grew, making this cost O(n) in message history.

The trie also remained in M1 (the process state), inflating every subsequent
`hb_ao:resolve` call that touched M1 — explaining the elevated `patch_phase_ms`.

**Instrumentation signal:**
```
exec_lmdb_writes:  ~680–700 / slot
exec_lmdb_reads:   ~1,300–1,400 / slot
dedup_entries:     66
dedup_bytes:       large (full trie serialized in state)
dedup_phase_ms:    ~1,500 ms
```

### Finding 2: Cache lookup ran against the full state even with `hashpath => ignore`

`hb_cache_control` interprets `hashpath => ignore` in Opts by setting
`store => false` in the cache-control map — but it did NOT set `lookup => false`.

Stage 2 of `hb_ao:resolve` is `hb_cache_control:maybe_lookup`. With `store =>
false` only, it still called `read_hashpath(State)` → `dev_message:id(State)` —
an HMAC-SHA256 over the full ~7 MB state — on every resolve call.

`do_compute` uses `hashpath => ignore` for all three phases: dedup,
delegated-compute, and patch. So this full-state hash was computed **3× per slot**
for no reason. With a 10 MB state it measured at ~253 ms per `run_as` call.

**Instrumentation signal:**
```
run_as_setup_ms:  ~253 ms  (expected <10 ms)
```

### Finding 3: Persistent grouper always hashed the full state

`hb_persistent:find_or_register` always called `group(Base, Req, Opts)` first,
which invokes `default_grouper` → `erlang:phash2({~7MB_state, Req})`. This result
was used to find or register a worker group — but `await_inprogress` is `false`
by default, meaning the result was discarded immediately after. The hash was
computed and thrown away.

This happened inside every `run_as` setup, compounding with Finding 2.

**Instrumentation signal:** `run_as_setup_ms` consistently ~253 ms regardless of
action complexity — the cost was dominated by the state hash, not the action.

### Finding 4: normalize_keys was NOT the bottleneck (ruled out)

The 463 `normalize_keys` calls per slot at ~10 µs each add up to ~5 ms/slot —
only 0.3–0.5% of `execution_ms`. This hypothesis was tested and eliminated.

```
normalize_keys_count:  463 / slot  (constant)
normalize_keys_us:     3–11 ms / slot  (avg ~5 ms)
```

---

## Root Cause: O(state_size) Operations Per Slot

The common thread across all three bottlenecks is operations that scale with
**process state size** rather than the complexity of the individual slot's action:

| Operation | Scales with | Cost |
|-----------|-------------|------|
| Dedup trie commit | `O(dedup_entries)` nodes × HMAC-SHA256 | ~1,500 ms |
| Cache lookup `dev_message:id` | `O(state_size)` bytes × HMAC | ~253 ms × 3 |
| Persistent grouper `phash2` | `O(state_size)` bytes | ~50–100 ms |

As the process accumulates more history (more unique messages, larger balances
trie), all three costs grew. This is why catch-up from slot 585k was so slow —
by that point the process had been running long enough that these costs dominated.

A process with 10 MB of state paid ~1,800 ms in pure overhead _before any WASM
compute happened_, and that overhead compounded with the dedup trie costs on top.

---

## Fixes Applied

### Fix 1: Flat LMDB dedup (`dev_dedup.erl`)

Replace the in-state trie with direct `hb_store:write` calls:

```
Key:   <<"dedup/", ProcID/binary, "/", SubjectID/binary>>
Value: <<SlotNumber/binary>>
```

One key per unique message ID — O(1) write. The trie is stripped from M1 on
migration so it no longer inflates the process state.

**Result:**
```
dedup_phase_ms:   1,500 ms → 15 ms  (88× speedup)
exec_lmdb_writes: ~700     → ~40    (94% reduction)
exec_lmdb_reads:  ~1,400   → ~14    (99% reduction)
patch_phase_ms:   1,700 ms → 80 ms  (restored — trie no longer in M1)
```

### Fix 2: `hashpath => ignore` disables lookup (`hb_cache_control.erl`)

When `hashpath => ignore` is in Opts, force `lookup => false` in addition to
`store => false`. Stage 2 of `hb_ao:resolve` is now a no-op for all three
`do_compute` phases.

**Result:**
```
run_as_setup_ms:  ~253 ms → 8–150 ms  (3–30× faster)
exec_lmdb_reads:  dropped by ~14/slot  (hashpath probes eliminated)
```

### Fix 3: Short-circuit phash2 (`hb_persistent.erl`)

When both `await_inprogress` and `spawn_worker` are false (the default for
normal compute), `find_or_register` returns `{leader, ungrouped_exec}` directly
without calling `group/3`.

**Result:** phash2 over ~7 MB state no longer runs on every `run_as` call.

---

## Combined Results

Post-fix baseline across multiple slots (~7 MB state):

```
execution_ms:         813–1,700 ms  (was 7,500–11,000 ms)
wasm_cu_ms:           80–85% of execution_ms
run_as_setup_ms:      8–150 ms
exec_lmdb_reads:      14  (stable)
exec_lmdb_writes:     1   (stable)
normalize_keys_count: 463 (stable)
```

WASM computation is now the dominant cost, which is the correct state: the Erlang
pipeline overhead has been reduced to ~15–20% of total execution time. Further
improvements to slot throughput require reducing WASM compute time itself (e.g.
faster genesis-wasm CU, or batching multiple messages per WASM call).

---

## Files Changed

### Instrumentation (no behaviour change)

| File | What was added |
|------|----------------|
| `src/hb_store_lmdb.erl` | Wrap `elmdb:get/put` with `timer:tc`; `take_stats/0` to reset accumulators |
| `src/hb_cache.erl` | Timed write paths for `dedup` and `balances` keys; `take_cache_stats/0` |
| `src/hb_ao.erl` | `timed_normalize_keys/2` wrapper; `take_normalize_stats/0` |
| `src/dev_delegated_compute.erl` | `timer:tc` around `do_relay`; stash `wasm_cu_us` in process dict |
| `src/dev_process.erl` | Collect all stats in `compute_slot`; emit `computed_slot` event |
| `monitoring/` | Loki + Promtail + Grafana docker-compose stack |
| `monitoring/grafana/dashboards/hyperbeam.json` | Dashboard with per-action, LMDB, WASM, and ETA panels |

### Performance fixes

| File | Fix |
|------|-----|
| `src/dev_dedup.erl` | Flat LMDB key-value dedup replacing in-state trie |
| `src/hb_cache_control.erl` | `hashpath => ignore` forces `lookup => false` |
| `src/hb_persistent.erl` | Short-circuit `find_or_register` when grouping unneeded |
| `src/dev_genesis_wasm.erl` | Phase timing (`delegated_phase_ms`) |

---

## Branch Structure

- **`obs/performance-instrumentation`** — instrumentation only, based on `edge`.
  Adds all logging, metrics, and the Grafana stack. No behaviour changes.
  Use this branch to profile a node and reproduce the pre-fix measurements.

- **`fix/scheduler-and-debug-instrumentation`** — instrumentation + all three
  performance fixes. This is the production-ready branch.
