#!/bin/sh
# Self-chaining lane loop: draft chunk after chunk without scheduler ticks.
# The codex-scheduler is wake-driven and drops interval ticks whenever the
# machine naps or the queue is busy (observed repeatedly 2026-08-26/27),
# so continuous lanes stalled ~50 min/cycle. This loop replaces tick
# dependence; the interval jobs stay as backstops (lane locks make
# overlapping starts harmless no-ops).
#
# Usage: pif_lane_loop.sh <lane> <chunk_count> <audit_rate>
# Stop:  touch work/bulk-drafts/STOP_<lane>   (checked between chunks)
set -u
LANE="$1"; COUNT="$2"; AUDIT="$3"
cd "$(dirname "$0")/.." || exit 1
STOP="work/bulk-drafts/STOP_${LANE}"
LOG="work/bulk-drafts/loop-${LANE}.log"
echo "$(date '+%F %T') loop start lane=$LANE count=$COUNT audit=$AUDIT pid=$$" >> "$LOG"
while [ ! -f "$STOP" ]; do
  /usr/bin/python3 -B -m research_factory.pif_bulk_draft_runner \
    --lane "$LANE" --count "$COUNT" --audit-rate "$AUDIT" \
    >> "$LOG" 2>&1 &
  RPID=$!
  # Stall watchdog: the runner has twice hung alive-but-idle (0 CPU, no
  # drafts, threads stuck past their socket timeouts). If no draft lands
  # for 15 minutes while the runner lives, kill and let the loop restart.
  while kill -0 "$RPID" 2>/dev/null; do
    sleep 300
    LAST=$(/usr/bin/sqlite3 work/bulk-drafts/drafts.sqlite \
      "SELECT CAST((julianday('now')-julianday(MAX(created_at)))*1440 AS INT) \
       FROM draft_labels WHERE lane='$LANE'" 2>/dev/null)
    if [ -n "$LAST" ] && [ "$LAST" -ge 15 ]; then
      echo "$(date '+%F %T') watchdog: no $LANE draft for ${LAST}m; killing $RPID" >> "$LOG"
      kill "$RPID" 2>/dev/null
      sleep 5
      break
    fi
  done
  wait "$RPID" 2>/dev/null
  sleep 60
done
echo "$(date '+%F %T') loop stopped by $STOP" >> "$LOG"
