#!/usr/bin/env python3
"""Fetch already-verified YouTube caption transcripts in bounded batches.

Kolby authorized the caption lane 2026-08-27. Hundreds of episodes carry
verified youtube_captions URLs from earlier discovery but never got
fetched (the lane was disabled). This runner re-attaches each URL through
the official CLI (which re-enqueues the fetch job) and drains the queue in
batches, aborting if the failure rate spikes (the signature of a YouTube
IP block — back off rather than hammer).

Usage: python3 scripts/pif_caption_backfill.py [--batch 40] [--max-batches 40]
       [--since 2025-01-01] [--abort-fail-rate 0.5]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
DB = PIF_ROOT / "data" / "factory.sqlite"


def candidates(conn, since: str, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT e.id, e.verified_transcript_url AS url
        FROM episodes e
        WHERE e.verified_transcript_url IS NOT NULL
          AND e.verified_transcript_source_kind = 'youtube_captions'
          AND e.published_at >= ?
          AND e.id NOT IN (SELECT episode_id FROM transcripts)
          AND e.id NOT IN (SELECT target_id FROM jobs
                           WHERE job_type = 'fetch_transcript'
                             AND status IN ('pending', 'claimed'))
        ORDER BY e.published_at DESC LIMIT ?
        """, (since, limit)).fetchall()


def fetch_stats(conn) -> dict:
    rows = conn.execute(
        "SELECT status, COUNT(*) n FROM jobs WHERE job_type='fetch_transcript'"
        " GROUP BY status").fetchall()
    return {r[0]: r[1] for r in rows}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=40)
    ap.add_argument("--max-batches", type=int, default=40)
    ap.add_argument("--since", default="2025-01-01")
    ap.add_argument("--abort-fail-rate", type=float, default=0.5)
    ap.add_argument("--sleep", type=float, default=20.0,
                    help="pause between batches (be polite to YouTube)")
    args = ap.parse_args()
    total_ok = total_fail = 0
    for batch_no in range(1, args.max_batches + 1):
        conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        todo = candidates(conn, args.since, args.batch)
        before = fetch_stats(conn)
        conn.close()
        if not todo:
            print("no candidates left; done")
            break
        for row in todo:
            r = subprocess.run(
                [sys.executable, "-m", "research_factory",
                 "attach-transcript", "--episode-id", row["id"],
                 "--transcript-url", row["url"],
                 "--transcript-type", "text/plain",
                 "--source-kind", "youtube_captions"],
                capture_output=True, text=True, cwd=PIF_ROOT)
            if r.returncode != 0:
                print(f"attach failed {row['id']}: "
                      f"{(r.stderr or '').strip()[:120]}", file=sys.stderr)
        drain = subprocess.run(
            [sys.executable, "-m", "research_factory", "run",
             "--job-types", "fetch_transcript",
             "--limit", str(args.batch + 10),
             "--max-items", str(args.batch + 10),
             "--max-runtime-seconds", "900"],
            capture_output=True, text=True, cwd=PIF_ROOT, timeout=1200)
        conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
        after = fetch_stats(conn)
        conn.close()
        ok = after.get("completed", 0) - before.get("completed", 0)
        fail = after.get("failed", 0) - before.get("failed", 0)
        total_ok += ok
        total_fail += fail
        rate = fail / max(ok + fail, 1)
        print(f"batch {batch_no}: attached {len(todo)}, fetched ok={ok} "
              f"fail={fail} (cum ok={total_ok} fail={total_fail})",
              flush=True)
        if ok + fail >= 10 and rate >= args.abort_fail_rate:
            print(f"ABORT: failure rate {rate:.0%} looks like a YouTube "
                  "block — backing off. Re-run later.", flush=True)
            return 2
        time.sleep(args.sleep)
    print(json.dumps({"fetched_ok": total_ok, "failed": total_fail}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
