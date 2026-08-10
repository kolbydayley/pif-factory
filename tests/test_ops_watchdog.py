"""Deterministic ops watchdog checks (durability plan Phase 4)."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from research_factory import ops_watchdog as wd


def _touch(path: Path, *, age_hours: float = 0.0, content: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    if age_hours:
        stamp = time.time() - age_hours * 3600
        os.utime(path, (stamp, stamp))


def test_quiet_system_produces_no_alerts(tmp_path: Path) -> None:
    _touch(tmp_path / "runs/headless_logs/run_recent.log", age_hours=2)
    _touch(
        tmp_path / "work/pif-ops/daily" / wd._yesterday() / "daily-receipt.json"
    )
    report = wd.run_watchdog(project_root=tmp_path)
    assert report["alerts"] == []
    assert report["ok"] is True


def test_dead_lane_alerts(tmp_path: Path) -> None:
    _touch(tmp_path / "runs/headless_logs/run_old.log", age_hours=40)
    _touch(
        tmp_path / "work/pif-ops/daily" / wd._yesterday() / "daily-receipt.json"
    )
    report = wd.run_watchdog(project_root=tmp_path)
    kinds = [alert["kind"] for alert in report["alerts"]]
    assert "lane_dead" in kinds
    assert report["ok"] is False


def test_no_logs_at_all_is_lane_dead(tmp_path: Path) -> None:
    _touch(
        tmp_path / "work/pif-ops/daily" / wd._yesterday() / "daily-receipt.json"
    )
    report = wd.run_watchdog(project_root=tmp_path)
    assert "lane_dead" in [alert["kind"] for alert in report["alerts"]]


def test_kill_and_freeze_files_alert(tmp_path: Path) -> None:
    _touch(tmp_path / "runs/headless_logs/run_recent.log", age_hours=1)
    _touch(
        tmp_path / "work/pif-ops/daily" / wd._yesterday() / "daily-receipt.json"
    )
    _touch(tmp_path / "work/pif-ops/budget/KILL", content="{}")
    _touch(
        tmp_path / "work/pif-ops/agreement/FREEZE_episode_context", content="{}"
    )
    report = wd.run_watchdog(project_root=tmp_path)
    kinds = sorted(alert["kind"] for alert in report["alerts"])
    assert "budget_kill_engaged" in kinds
    assert "agreement_freeze_engaged" in kinds


def test_missing_yesterday_receipt_alerts(tmp_path: Path) -> None:
    _touch(tmp_path / "runs/headless_logs/run_recent.log", age_hours=1)
    report = wd.run_watchdog(project_root=tmp_path)
    assert "missing_daily_receipt" in [a["kind"] for a in report["alerts"]]


def test_report_is_json_serializable_and_written(tmp_path: Path) -> None:
    _touch(tmp_path / "runs/headless_logs/run_recent.log", age_hours=1)
    report = wd.run_watchdog(project_root=tmp_path, write_report=True)
    on_disk = (
        tmp_path / "work/pif-ops/watchdog" / f"{report['day']}.json"
    )
    assert on_disk.exists()
    parsed = json.loads(on_disk.read_text(encoding="utf-8"))
    assert parsed["schema_version"] == "pif_ops_watchdog_report_v1"
