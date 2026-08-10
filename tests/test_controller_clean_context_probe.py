from __future__ import annotations

import datetime as dt
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace


SCRIPT = Path(__file__).resolve().parents[1] / "automation/pif_controller_clean_context_probe.py"


def _module():
    spec = importlib.util.spec_from_file_location("pif_clean_context_probe", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _green_db(project: Path, run_date: str = "2026-08-02") -> None:
    database = project / "data/factory.sqlite"
    database.parent.mkdir(parents=True)
    conn = sqlite3.connect(database)
    conn.execute(
        "CREATE TABLE pif_scale_gate_state_receipts (daily_run_id TEXT, run_date TEXT, genuinely_successful INTEGER, consecutive_success_days INTEGER, created_at TEXT)"
    )
    conn.execute(
        "INSERT INTO pif_scale_gate_state_receipts VALUES ('pdr_green', ?, 1, 2, ?)",
        (run_date, f"{run_date}T05:00:00+00:00"),
    )
    conn.commit()
    conn.close()


def test_probe_unreadable_project_fails_durably(tmp_path, monkeypatch) -> None:
    module = _module()
    status = tmp_path / "probe.json"
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=78, stderr="unreadable"),
    )
    code = module.run_probe(
        project_root=tmp_path / "missing",
        launcher=tmp_path / "launcher",
        status_path=status,
        boot_session_id="boot-a",
    )
    assert code == 78
    assert json.loads(status.read_text())["status"] == "failed"


def test_deadman_rejects_stale_probe_and_boot_change(tmp_path) -> None:
    module = _module()
    project = tmp_path / "project"
    _green_db(project)
    probe = tmp_path / "probe.json"
    probe.write_text(json.dumps({
        "status": "passed",
        "checked_at": "2026-08-02T00:00:00+00:00",
        "boot_session_id": "old-boot",
    }))
    deadman = tmp_path / "deadman.json"
    code = module.run_deadman(
        project_root=project,
        probe_status=probe,
        status_path=deadman,
        now=dt.datetime(2026, 8, 2, 8, tzinfo=dt.timezone.utc),
        boot_session_id="new-boot",
    )
    result = json.loads(deadman.read_text())
    assert code == 78
    assert "probe_stale" in result["reasons"]
    assert "probe_boot_session_mismatch" in result["reasons"]


def test_deadman_rejects_missing_daily_receipt(tmp_path) -> None:
    module = _module()
    project = tmp_path / "project"
    _green_db(project, run_date="2026-08-01")
    probe = tmp_path / "probe.json"
    probe.write_text(json.dumps({
        "status": "passed",
        "checked_at": "2026-08-02T00:05:00+00:00",
        "boot_session_id": "boot-a",
    }))
    deadman = tmp_path / "deadman.json"
    code = module.run_deadman(
        project_root=project,
        probe_status=probe,
        status_path=deadman,
        now=dt.datetime(2026, 8, 2, 4, 45, tzinfo=dt.timezone.utc),
        boot_session_id="boot-a",
    )
    assert code == 78
    assert "green_current_date_receipt_missing" in json.loads(deadman.read_text())["reasons"]


def test_probe_recovers_after_relocation(tmp_path, monkeypatch) -> None:
    module = _module()
    project = tmp_path / "pif-factory"
    project.mkdir()
    status = tmp_path / "probe.json"

    def fake_run(command, **_kwargs):
        launcher_status = Path(command[command.index("--status-path") + 1])
        launcher_status.write_text(json.dumps({"status": "preflight_passed"}))
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    code = module.run_probe(
        project_root=project,
        launcher=tmp_path / "launcher",
        status_path=status,
        boot_session_id="boot-new",
    )
    result = json.loads(status.read_text())
    assert code == 0
    assert result["status"] == "passed"
    assert result["project_root"] == str(project)
    assert result["environment_keys"] == ["HOME", "PATH"]


def test_deadman_passes_only_with_fresh_same_boot_probe_and_green_receipt(tmp_path) -> None:
    module = _module()
    project = tmp_path / "project"
    _green_db(project)
    probe = tmp_path / "probe.json"
    probe.write_text(json.dumps({
        "status": "passed",
        "checked_at": "2026-08-02T00:05:00+00:00",
        "boot_session_id": "boot-a",
    }))
    deadman = tmp_path / "deadman.json"
    code = module.run_deadman(
        project_root=project,
        probe_status=probe,
        status_path=deadman,
        now=dt.datetime(2026, 8, 2, 4, 45, tzinfo=dt.timezone.utc),
        boot_session_id="boot-a",
    )
    result = json.loads(deadman.read_text())
    assert code == 0
    assert result["status"] == "passed"
    assert result["green_daily_run_id"] == "pdr_green"
