#!/usr/bin/env python3
"""Scheduler entrypoint for the production PIF daily cycle.

Runs one controller daily-cycle decision (`sdk_pipeline_controller serve
--once`) and maps the outcome to an exit code the codex-scheduler failure
policy can act on:

  exit 0  today is genuinely successful (fresh run or idempotent no-op)
  exit 1  today is red — blocked gates, stage failure, lock deferral, or
          the per-day attempt budget is exhausted

The attempt guard exists because the bounded extraction budget is 50
labels/day (two 25-segment runs): a third same-day attempt cannot go green —
it fails on `daily_cap_reached` while still recording a red receipt. Once two
runs exist for today and neither is genuinely successful, this script exits 1
immediately without spawning the controller, so scheduler retries escalate to
the operator instead of burning attempts.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
FACTORY_DB = PIF_ROOT / "data" / "factory.sqlite"
MAX_ATTEMPTS_PER_DAY = 2
CONTROLLER_TIMEOUT_SECONDS = 2 * 3600 + 600  # daily cycle hard cap is 7200s


def todays_state(run_date: str) -> dict:
    conn = sqlite3.connect(f"file:{FACTORY_DB}?mode=ro", uri=True)
    try:
        attempts = conn.execute(
            "SELECT COUNT(*) FROM pif_daily_runs WHERE run_date = ?",
            (run_date,)).fetchone()[0]
        green = conn.execute(
            "SELECT COUNT(*) FROM pif_scale_gate_state_receipts"
            " WHERE run_date = ?"
            "   AND json_extract(receipt_json, '$.genuinely_successful') = 1",
            (run_date,)).fetchone()[0]
    finally:
        conn.close()
    return {"attempts": attempts, "green": bool(green)}


def main() -> int:
    run_date = dt.date.today().isoformat()
    state = todays_state(run_date)
    if state["green"]:
        print(json.dumps({"run_date": run_date, "status": "already_green",
                          **state}))
        return 0
    if state["attempts"] >= MAX_ATTEMPTS_PER_DAY:
        print(json.dumps({"run_date": run_date,
                          "status": "attempts_exhausted_red",
                          **state}))
        return 1

    try:
        proc = subprocess.run(
            [sys.executable, "-B", "-m",
             "research_factory.sdk_pipeline_controller", "serve", "--once"],
            cwd=str(PIF_ROOT), capture_output=True, text=True,
            timeout=CONTROLLER_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        print(json.dumps({"run_date": run_date,
                          "status": "controller_timeout"}))
        return 1

    if proc.returncode == 75:  # another controller instance holds the lock
        print(json.dumps({"run_date": run_date, "status": "already_running"}))
        return 1

    result = {}
    try:  # serve --once prints exactly one JSON object on stdout
        result = json.loads(proc.stdout)
    except (json.JSONDecodeError, TypeError):
        pass
    genuine = bool(result.get("genuinely_successful"))
    print(json.dumps({
        "run_date": run_date,
        "status": result.get("status") or f"controller_exit_{proc.returncode}",
        "genuinely_successful": genuine,
        "run_id": result.get("run_id"),
        "gate_table": result.get("gate_table"),
    }))
    if proc.returncode != 0:
        sys.stderr.write((proc.stderr or "")[-2000:])
        return 1
    return 0 if genuine else 1


if __name__ == "__main__":
    raise SystemExit(main())
