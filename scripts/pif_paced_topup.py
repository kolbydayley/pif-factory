#!/usr/bin/env python3
"""One-shot paced top-up: bring each paced lane up to today's deadline-paced
count, sequenced behind the global runner lock.

For each paced lane (glm-zai, codex, grok): recompute the live paced daily
count, subtract what the lane already drafted-called since local midnight,
and run the bulk runner for the difference. Lanes at or past pace are
skipped. Waits (bounded) for the single-writer runner lock instead of
aborting, so it can be launched while a roll is still in flight.

Usage: PYTHONPATH=. python3 scripts/pif_paced_topup.py [--lanes glm-zai,codex]
"""
import argparse
import datetime as dt
import json
import subprocess
import sys
import time
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
sys.path.insert(0, str(PIF_ROOT))

from research_factory.pif_pacing import _receipt_calls_since, paced_count  # noqa: E402

SHADOW_ROOT = PIF_ROOT / "work" / "bulk-drafts"
LOCK = SHADOW_ROOT / "runner.lock"
LOCK_WAIT_LIMIT = 8 * 3600
MIN_TOPUP = 25  # not worth a run below this


def wait_for_lock() -> bool:
    start = time.time()
    while LOCK.exists():
        if time.time() - start > LOCK_WAIT_LIMIT:
            return False
        time.sleep(60)
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lanes", default="glm-zai,codex,grok")
    parser.add_argument("--audit-rate", type=float, default=0.12)
    args = parser.parse_args()

    midnight = dt.datetime.now().replace(hour=0, minute=0, second=0,
                                         microsecond=0).timestamp()
    results = {}
    for lane in [l.strip() for l in args.lanes.split(",") if l.strip()]:
        pace = paced_count(lane)
        if not pace:
            results[lane] = {"skipped": "no_pacing_signal"}
            continue
        done_today = _receipt_calls_since(lane, midnight)
        topup = pace["count"] - done_today
        if topup < MIN_TOPUP:
            results[lane] = {"skipped": "at_or_past_pace",
                             "paced": pace["count"], "done_today": done_today}
            continue
        if not wait_for_lock():
            results[lane] = {"skipped": "lock_wait_timeout"}
            continue
        print(f"[{lane}] topping up {topup} (paced {pace['count']}, "
              f"done {done_today})", flush=True)
        proc = subprocess.run(
            [sys.executable, "-B", "-m", "research_factory.pif_bulk_draft_runner",
             "--lane", lane, "--count", str(topup),
             "--audit-rate", str(args.audit_rate)],
            cwd=str(PIF_ROOT), timeout=10 * 3600)
        results[lane] = {"topup": topup, "paced": pace["count"],
                         "done_before": done_today, "exit": proc.returncode}
    print(json.dumps({"paced_topup": results}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
