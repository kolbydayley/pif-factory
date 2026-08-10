#!/Users/kolbydayley/.codex/venvs/pif-sdk-pipeline-controller/bin/python
"""Zero-provider clean-context launch probe and independent daily deadman."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
import subprocess
import tempfile
from pathlib import Path
from typing import Any

EX_CONFIG = 78
PROJECT_ROOT = Path("/Users/kolbydayley/pif-factory")
LAUNCHER = Path("/Users/kolbydayley/.local/libexec/pif-sdk-pipeline-controller")
STATUS_ROOT = Path("/Users/kolbydayley/.codex/pif-controller")
PROBE_STATUS = STATUS_ROOT / "clean-context-probe.json"
DEADMAN_STATUS = STATUS_ROOT / "clean-context-deadman.json"
MAX_PROBE_AGE_SECONDS = 7 * 60 * 60


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _iso(value: dt.datetime) -> str:
    return value.replace(microsecond=0).isoformat()


def _write_status(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def _boot_session_id() -> str:
    result = subprocess.run(
        ["/usr/sbin/sysctl", "-n", "kern.boottime"],
        text=True,
        capture_output=True,
        timeout=5,
        check=True,
        env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin"},
        cwd="/",
    )
    return hashlib.sha256(result.stdout.strip().encode()).hexdigest()


def run_probe(
    *,
    project_root: Path = PROJECT_ROOT,
    launcher: Path = LAUNCHER,
    status_path: Path = PROBE_STATUS,
    now: dt.datetime | None = None,
    boot_session_id: str | None = None,
) -> int:
    checked = now or _now()
    boot_id = boot_session_id or _boot_session_id()
    with tempfile.TemporaryDirectory(prefix="pif-clean-context-") as temporary:
        launcher_status = Path(temporary) / "launcher-status.json"
        command = [
            str(launcher),
            "--project-root",
            str(project_root),
            "--status-path",
            str(launcher_status),
            "--preflight-only",
        ]
        try:
            completed = subprocess.run(
                command,
                cwd="/",
                env={
                    "HOME": "/Users/kolbydayley",
                    "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                },
                text=True,
                capture_output=True,
                timeout=60,
            )
            launcher_receipt = (
                json.loads(launcher_status.read_text())
                if launcher_status.is_file()
                else None
            )
            passed = completed.returncode == 0 and bool(
                launcher_receipt
                and launcher_receipt.get("status") == "preflight_passed"
            )
            payload = {
                "schema_version": "pif_clean_context_probe_v1",
                "status": "passed" if passed else "failed",
                "checked_at": _iso(checked),
                "boot_session_id": boot_id,
                "project_root": str(project_root),
                "startup_cwd": "/",
                "environment_keys": ["HOME", "PATH"],
                "provider_calls": 0,
                "launcher_returncode": completed.returncode,
                "launcher_receipt": launcher_receipt,
                "stderr": completed.stderr[-2000:],
            }
        except Exception as exc:
            passed = False
            payload = {
                "schema_version": "pif_clean_context_probe_v1",
                "status": "failed",
                "checked_at": _iso(checked),
                "boot_session_id": boot_id,
                "project_root": str(project_root),
                "startup_cwd": "/",
                "environment_keys": ["HOME", "PATH"],
                "provider_calls": 0,
                "error_class": type(exc).__name__,
                "message": str(exc)[:1000],
            }
    _write_status(status_path, payload)
    return 0 if passed else EX_CONFIG


def run_deadman(
    *,
    project_root: Path = PROJECT_ROOT,
    probe_status: Path = PROBE_STATUS,
    status_path: Path = DEADMAN_STATUS,
    now: dt.datetime | None = None,
    boot_session_id: str | None = None,
    max_probe_age_seconds: int = MAX_PROBE_AGE_SECONDS,
) -> int:
    checked = now or _now()
    boot_id = boot_session_id or _boot_session_id()
    reasons: list[str] = []
    try:
        probe = json.loads(probe_status.read_text())
    except (OSError, json.JSONDecodeError):
        probe = {}
        reasons.append("probe_missing_or_unreadable")
    if probe.get("status") != "passed":
        reasons.append("probe_not_passed")
    if probe.get("boot_session_id") != boot_id:
        reasons.append("probe_boot_session_mismatch")
    try:
        probe_at = dt.datetime.fromisoformat(str(probe["checked_at"]))
        age = (checked - probe_at).total_seconds()
    except (KeyError, TypeError, ValueError):
        age = None
        reasons.append("probe_timestamp_invalid")
    if age is not None and (age < 0 or age > max_probe_age_seconds):
        reasons.append("probe_stale")

    db_path = project_root / "data/factory.sqlite"
    green = None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        green = conn.execute(
            """
            SELECT daily_run_id, consecutive_success_days
            FROM pif_scale_gate_state_receipts
            WHERE run_date = ? AND genuinely_successful = 1
            ORDER BY created_at DESC LIMIT 1
            """,
            (checked.astimezone().date().isoformat(),),
        ).fetchone()
        conn.close()
    except sqlite3.Error as exc:
        reasons.append(f"daily_receipt_query_failed:{type(exc).__name__}")
    if green is None:
        reasons.append("green_current_date_receipt_missing")
    payload = {
        "schema_version": "pif_controller_clean_context_deadman_v1",
        "status": "failed" if reasons else "passed",
        "checked_at": _iso(checked),
        "boot_session_id": boot_id,
        "probe_status_path": str(probe_status),
        "probe_age_seconds": age,
        "project_root": str(project_root),
        "green_daily_run_id": green[0] if green else None,
        "consecutive_success_days": green[1] if green else None,
        "reasons": reasons,
        "provider_calls": 0,
    }
    _write_status(status_path, payload)
    return EX_CONFIG if reasons else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("probe", "deadman"))
    args = parser.parse_args(argv)
    return run_probe() if args.mode == "probe" else run_deadman()


if __name__ == "__main__":
    raise SystemExit(main())
