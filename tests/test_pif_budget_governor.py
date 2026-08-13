import json
import sqlite3
from pathlib import Path

import pytest

from research_factory.pif_budget_governor import (
    WeeklyLedger,
    allowance,
    read_weekly_snapshot,
)

DAY = 86400


def _write_rollout(root: Path, name: str, used_percent: float, resets_at: int) -> Path:
    session_dir = root / "2026" / "08" / "13"
    session_dir.mkdir(parents=True, exist_ok=True)
    path = session_dir / name
    event = {
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "rate_limits": {
                "limit_id": "codex",
                "primary": {
                    "used_percent": used_percent,
                    "window_minutes": 10080,
                    "resets_at": resets_at,
                },
                "rate_limit_reached_type": None,
            },
        },
    }
    with open(path, "w") as fh:
        fh.write(json.dumps({"type": "session_meta"}) + "\n")
        fh.write(json.dumps(event) + "\n")
    return path


def test_read_weekly_snapshot_finds_latest(tmp_path):
    _write_rollout(tmp_path, "rollout-2026-08-13T01-00-00-aaa.jsonl", 3.0, 1787200360)
    newer = _write_rollout(tmp_path, "rollout-2026-08-13T02-00-00-bbb.jsonl", 5.5, 1787200360)
    import os, time
    os.utime(newer, (time.time() + 10, time.time() + 10))
    snap = read_weekly_snapshot(tmp_path)
    assert snap["used_percent"] == 5.5
    assert snap["resets_at"] == 1787200360


def test_read_weekly_snapshot_none_when_empty(tmp_path):
    assert read_weekly_snapshot(tmp_path) is None


def test_ledger_accumulates_within_window(tmp_path):
    ledger = WeeklyLedger(tmp_path / "ledger.sqlite")
    reset = 1787200360
    ledger.record({"used_percent": 1.0, "resets_at": reset},
                  {"used_percent": 3.5, "resets_at": reset}, run_id="r1")
    ledger.record({"used_percent": 3.5, "resets_at": reset},
                  {"used_percent": 4.0, "resets_at": reset}, run_id="r2")
    assert ledger.points_used(reset) == pytest.approx(3.0)


def test_ledger_isolates_windows(tmp_path):
    ledger = WeeklyLedger(tmp_path / "ledger.sqlite")
    ledger.record({"used_percent": 20.0, "resets_at": 100}, {"used_percent": 25.0, "resets_at": 100}, run_id="old")
    ledger.record({"used_percent": 0.0, "resets_at": 200}, {"used_percent": 2.0, "resets_at": 200}, run_id="new")
    assert ledger.points_used(200) == pytest.approx(2.0)
    assert ledger.points_used(100) == pytest.approx(5.0)


def test_ledger_window_rollover_mid_batch_clamps_to_zero(tmp_path):
    ledger = WeeklyLedger(tmp_path / "ledger.sqlite")
    # window reset between before and after: delta would be negative; clamp, attribute to after-window
    ledger.record({"used_percent": 29.0, "resets_at": 100}, {"used_percent": 1.0, "resets_at": 200}, run_id="x")
    assert ledger.points_used(200) == pytest.approx(1.0)
    assert ledger.points_used(100) == pytest.approx(0.0)


def test_allowance_hard_stop_at_cap(tmp_path):
    ledger = WeeklyLedger(tmp_path / "ledger.sqlite")
    reset = 1_000_000
    ledger.record({"used_percent": 0.0, "resets_at": reset}, {"used_percent": 30.0, "resets_at": reset}, run_id="big")
    snap = {"used_percent": 40.0, "resets_at": reset}
    result = allowance(snap, ledger, cap_points=30.0, now=reset - 3 * DAY)
    assert result["allowed"] is False
    assert result["points_remaining"] == pytest.approx(0.0)


def test_allowance_paces_over_remaining_days(tmp_path):
    ledger = WeeklyLedger(tmp_path / "ledger.sqlite")
    reset = 1_000_000
    ledger.record({"used_percent": 0.0, "resets_at": reset}, {"used_percent": 6.0, "resets_at": reset}, run_id="r")
    snap = {"used_percent": 10.0, "resets_at": reset}
    result = allowance(snap, ledger, cap_points=30.0, now=reset - 3 * DAY)
    # 24 points remaining over 3 days -> 8/day, borrow cap 2x -> 16
    assert result["allowed"] is True
    assert result["points_remaining"] == pytest.approx(24.0)
    assert result["points_today"] == pytest.approx(16.0)


def test_allowance_fail_closed_on_missing_snapshot(tmp_path):
    ledger = WeeklyLedger(tmp_path / "ledger.sqlite")
    result = allowance(None, ledger, cap_points=30.0, now=0)
    assert result["allowed"] is False
