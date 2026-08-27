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
    >> "$LOG" 2>&1
  sleep 60
done
echo "$(date '+%F %T') loop stopped by $STOP" >> "$LOG"
