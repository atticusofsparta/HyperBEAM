#!/bin/bash
# Triggered on hyperbeam container start — waits for node ready,
# checks if evaluation is already progressing, and triggers if not.

PROC="qNvAoz0TgcH7DMg8BCVn8jF32QH5L6T29VjHxhHqqGE"
HB_URL="http://localhost:10000"
MAX_WAIT=120  # seconds to wait for node to be ready
SLACK_WEBHOOK="${SLACK_WEBHOOK:-}"

slack() {
    local msg="$1"
    curl -sf -X POST "$SLACK_WEBHOOK" \
        -H "Content-Type: application/json" \
        -d "{\"text\": \"$msg\"}" > /dev/null 2>&1
}

echo "[hyperbeam-cron] Waiting for node to be ready..."
slack ":warning: HyperBEAM restarted — waiting for node to be ready..."

elapsed=0
until curl -sf "$HB_URL/" > /dev/null 2>&1; do
    sleep 2
    elapsed=$((elapsed + 2))
    if [ $elapsed -ge $MAX_WAIT ]; then
        echo "[hyperbeam-cron] Timed out waiting for node after ${MAX_WAIT}s"
        slack ":red_circle: HyperBEAM failed to become ready after ${MAX_WAIT}s — manual intervention required."
        exit 1
    fi
done
echo "[hyperbeam-cron] Node is ready."

# Check if already actively computing by polling at-slot twice
SLOT1=$(curl -sf "$HB_URL/${PROC}~process@1.0/compute/at-slot" 2>/dev/null)
sleep 3
SLOT2=$(curl -sf "$HB_URL/${PROC}~process@1.0/compute/at-slot" 2>/dev/null)

if [ -n "$SLOT1" ] && [ "$SLOT1" != "$SLOT2" ]; then
    echo "[hyperbeam-cron] Already computing (slot $SLOT1 -> $SLOT2), skipping trigger."
    slack ":white_check_mark: HyperBEAM is back and already computing (slot $SLOT1 -> $SLOT2)."
else
    echo "[hyperbeam-cron] Triggering continuous evaluation (at-slot: ${SLOT1:-unknown})..."
    RESULT=$(curl -sf "$HB_URL/~cron@1.0/every?interval=5-seconds&cron-path=${PROC}~process@1.0/now" 2>/dev/null)
    echo "[hyperbeam-cron] Triggered, task ID: $RESULT"
    slack ":white_check_mark: HyperBEAM is back — continuous evaluation triggered from slot ${SLOT1:-unknown} (task: $RESULT)."
fi
