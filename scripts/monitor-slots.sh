#!/bin/bash
# Monitors slots/second over a 10-minute sliding window.
# Sends Slack alerts when computation stalls.

PROC="qNvAoz0TgcH7DMg8BCVn8jF32QH5L6T29VjHxhHqqGE"
HB_URL="http://localhost:10000"
SLACK_WEBHOOK="${SLACK_WEBHOOK:-}"
STATE_FILE="/tmp/hb-slot-window.log"
WINDOW_SECONDS=600   # 10-minute sliding window
POLL_INTERVAL=30     # sample every 30 seconds
STALL_THRESHOLD=120  # alert after 2+ minutes of no progress within the window

LAST_ALERT_SLOT=""
LAST_ALERT_TIME=0
LAST_REPORT_TIME=0

slack() {
    curl -sf -X POST "$SLACK_WEBHOOK" \
        -H "Content-Type: application/json" \
        -d "{\"text\": \"$1\"}" > /dev/null 2>&1
}

echo "[hb-monitor] Starting — window: ${WINDOW_SECONDS}s, poll: ${POLL_INTERVAL}s, stall threshold: ${STALL_THRESHOLD}s"
slack ":eyes: *HyperBEAM monitor started* — watching \`$PROC\` (10-min sliding window, alert after ${STALL_THRESHOLD}s stall)"

# Clear stale state from a previous run
> "$STATE_FILE"

while true; do
    sleep "$POLL_INTERVAL"

    NOW=$(date +%s)
    CURRENT_SLOT=$(curl -sf --max-time 5 "$HB_URL/${PROC}~process@1.0/compute/at-slot" 2>/dev/null)

    if [ -z "$CURRENT_SLOT" ]; then
        echo "[hb-monitor] $(date -u '+%H:%M:%S') — could not fetch at-slot (node down?)"
        continue
    fi

    # Append sample and prune entries outside the window
    echo "$NOW $CURRENT_SLOT" >> "$STATE_FILE"
    CUTOFF=$((NOW - WINDOW_SECONDS))
    awk -v cutoff="$CUTOFF" '$1 >= cutoff' "$STATE_FILE" > "${STATE_FILE}.tmp" && mv "${STATE_FILE}.tmp" "$STATE_FILE"

    LINE_COUNT=$(wc -l < "$STATE_FILE")

    if [ "$LINE_COUNT" -lt 2 ]; then
        echo "[hb-monitor] $(date -u '+%H:%M:%S') — collecting samples ($LINE_COUNT), slot: $CURRENT_SLOT"
        continue
    fi

    OLDEST_TIME=$(awk 'NR==1{print $1}' "$STATE_FILE")
    OLDEST_SLOT=$(awk 'NR==1{print $2}' "$STATE_FILE")

    TIME_DIFF=$((NOW - OLDEST_TIME))
    SLOT_DIFF=$((CURRENT_SLOT - OLDEST_SLOT))

    [ "$TIME_DIFF" -le 0 ] && continue

    RATE=$(awk "BEGIN {printf \"%.3f\", $SLOT_DIFF / $TIME_DIFF}")

    echo "[hb-monitor] $(date -u '+%H:%M:%S') — slot: $CURRENT_SLOT | ${RATE} slots/s over last ${TIME_DIFF}s ($LINE_COUNT samples)"

    # Periodic 10-minute report to Slack
    if [ $((NOW - LAST_REPORT_TIME)) -ge "$WINDOW_SECONDS" ]; then
        slack ":bar_chart: *HyperBEAM report* — slot \`$CURRENT_SLOT\` | *${RATE} slots/s* over last ${TIME_DIFF}s"
        LAST_REPORT_TIME="$NOW"
    fi

    # Alert if stalled: no progress for >= STALL_THRESHOLD seconds
    if [ "$SLOT_DIFF" -eq 0 ] && [ "$TIME_DIFF" -ge "$STALL_THRESHOLD" ]; then
        # Deduplicate: re-alert at most every 10 minutes for the same stuck slot
        if [ "$CURRENT_SLOT" != "$LAST_ALERT_SLOT" ] || [ $((NOW - LAST_ALERT_TIME)) -ge "$WINDOW_SECONDS" ]; then
            echo "[hb-monitor] STALL DETECTED — alerting Slack"
            slack ":red_circle: *HyperBEAM STALLED* — 0 slots computed in last ${TIME_DIFF}s. Stuck at slot \`$CURRENT_SLOT\`."
            LAST_ALERT_SLOT="$CURRENT_SLOT"
            LAST_ALERT_TIME="$NOW"
        fi
    fi
done
