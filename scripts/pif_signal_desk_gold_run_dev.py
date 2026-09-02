#!/usr/bin/env python3
"""Run or resume development Gold A/B/C and the development audit slice."""

import argparse
import asyncio
import json
import sqlite3
import sys
import time
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))
from research_factory.signal_desk_gold_runner import run_dev_gold  # noqa: E402
from research_factory.pif_budget_governor import read_weekly_snapshot  # noqa: E402


def _latest_failure(database: Path) -> tuple[bool, str]:
    conn = sqlite3.connect(database)
    try:
        terminal = conn.execute(
            """SELECT COUNT(*)
               FROM signal_desk_rebuild_tasks t
               JOIN signal_desk_rebuild_attempts a ON a.id=t.current_attempt_id
               WHERE t.status='terminal_failed' AND a.status='terminal_failed'"""
        ).fetchone()[0]
        row = conn.execute(
            """SELECT COALESCE(a.semantic_failure_detail,a.infrastructure_failure_detail,'')
               FROM signal_desk_rebuild_tasks t
               JOIN signal_desk_rebuild_attempts a ON a.id=t.current_attempt_id
               WHERE t.status IN ('pending','running','terminal_failed')
                 AND (a.semantic_failure_detail IS NOT NULL
                      OR a.infrastructure_failure_detail IS NOT NULL)
               ORDER BY a.updated_at DESC LIMIT 1"""
        ).fetchone()
        return bool(terminal), str(row[0] if row else "")
    finally:
        conn.close()


def _wait_for_retry(database: Path) -> None:
    terminal, detail = _latest_failure(database)
    if terminal:
        raise RuntimeError("semantic gold failure requires explicit resurrection")
    quota_failure = any(
        token in detail.casefold()
        for token in ("rate_limit", "rate limit", "quota", "usage", "exhaust")
    )
    if not quota_failure:
        time.sleep(60)
        return
    before = read_weekly_snapshot(Path.home() / ".codex/sessions")
    while True:
        time.sleep(300)
        after = read_weekly_snapshot(Path.home() / ".codex/sessions")
        if after and (
            not before
            or after["resets_at"] != before["resets_at"]
            or float(after["used_percent"]) < float(before["used_percent"])
        ):
            return


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--once", action="store_true", help="Exit instead of supervising retryable stalls")
    args = parser.parse_args()
    root = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
    dispatch_database = root / "dispatch-dev.sqlite"
    while True:
        try:
            receipt = asyncio.run(run_dev_gold(
                manifest_path=PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json",
                project_root=PIF_ROOT, result_root=root / "results/development",
                dispatch_database=dispatch_database, budget_database=PIF_ROOT / "data/factory.sqlite",
                grant_path=PIF_ROOT / "config/signal_desk_gold_authoring_budget_grant.json",
                session_root=Path.home() / ".codex/sessions", budget_dir=PIF_ROOT / "work/pif-ops/budget",
                seed_roots={
                    "A": PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/semantic-canary-v2/private-development",
                    "B": PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/j2-j3-v1/private-b",
                    "C": PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/j2-j3-v1/private-c",
                    "AUDIT": PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/j2-j3-v1/private-audit",
                }, concurrency=args.concurrency,
            ))
            print(json.dumps(receipt, indent=2, sort_keys=True))
            return 0
        except RuntimeError:
            if args.once:
                raise
            _wait_for_retry(dispatch_database)


if __name__ == "__main__": raise SystemExit(main())
