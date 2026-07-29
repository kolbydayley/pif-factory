from __future__ import annotations

import argparse
import copy
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
import shlex
import sqlite3
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Iterator


SCHEMA_VERSION = "pif_pipeline_babysitter_v1"
STATE_LOCK_TIMEOUT_SECONDS = 10.0
PHASES = (
    "evaluation_watch",
    "extraction_handoff",
    "extraction_watch",
    "completion_confirmation",
    "complete",
)
DEFAULT_EVALUATION_THREAD = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
JOB_NAME = "pif-pipeline-babysitter"
HOME = Path.home()
CODEX_HOME = Path(os.environ.get("CODEX_HOME", HOME / ".codex"))
STATE_ROOT = CODEX_HOME / "memories" / "automation" / JOB_NAME
CONTROL_WORKING_DIRECTORY = CODEX_HOME / "memories" / "automation" / "pif-control-workspace"
DEFAULT_STATE_PATH = STATE_ROOT / "state.json"
_PROJECT_ROOT_OVERRIDE = os.environ.get("PIF_PROJECT_ROOT")
PROJECT_ROOT = (
    Path(_PROJECT_ROOT_OVERRIDE).expanduser()
    if _PROJECT_ROOT_OVERRIDE
    else Path(__file__).resolve().parents[1]
)
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "factory.sqlite"
DEFAULT_GOALS_DB_PATH = CODEX_HOME / "goals_1.sqlite"
DEFAULT_EVALUATION_ROOT = PROJECT_ROOT / "work" / "app-server-development-v2"
VALID_GOAL_STATUSES = frozenset({"active", "paused", "blocked", "usage_limited", "budget_limited", "complete"})
EVALUATION_RECEIPT_SCHEMA = "pif_pipeline_evaluation_receipt_v2"
EVALUATION_ARTIFACT_ROLES = frozenset({
    "judge_gate",
    "development_freeze",
    "untouched_holdout",
    "usage_telemetry",
    "production_integrity",
})
EVALUATION_BINDING_FIELDS = (
    "evaluation_id",
    "runtime_lock_sha256",
    "frozen_configuration_sha256",
    "reference_sha256",
    "holdout_manifest_sha256",
)
OPEN_TURN_ACTIVITY_GRACE_SECONDS = 15 * 60
TMUX_PATH = Path("/opt/homebrew/bin/tmux")
CAFFEINATE_PATH = Path("/usr/bin/caffeinate")
CODEX_RUNTIME_VERSION = "codex-cli 0.144.1"
CODEX_RUNTIME_SHA256 = "29915529b97697def1a957b0505e770aa6a45744435d62fc263e98d7619e167a"
CODEX_RUNTIME_TEAM_ID = "2DC432GLL2"
# Never launch unattended control work through ~/.local/bin/codex: that is a
# mutable symlink maintained by the app and changed underneath the watchdog on
# 2026-07-13. This fallback runtime stays immutable until the controller moves
# to the official Python SDK, which pins its own compatible app-server runtime.
CODEX_BIN = (
    CODEX_HOME
    / "packages"
    / "standalone"
    / "releases"
    / "0.144.1-aarch64-apple-darwin"
    / "bin"
    / "codex"
)
CODESIGN_PATH = Path("/usr/bin/codesign")


def iso_now() -> str:
    return dt.datetime.now().astimezone().replace(microsecond=0).isoformat()


def _parse_iso(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def _read_json(path: Path, default: Any = None) -> Any:
    if not path.is_file():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    os.replace(temp, path)


def codex_runtime_observation() -> dict[str, Any]:
    """Verify the exact signed Codex runtime before any unattended launch."""
    if not CODEX_BIN.is_file() or not CODESIGN_PATH.is_file():
        return {
            "available": False,
            "verified": False,
            "path": str(CODEX_BIN),
            "error_class": "pinned_runtime_unavailable",
        }
    try:
        observed_sha = _sha256_file(CODEX_BIN)
        version_proc = subprocess.run(
            [str(CODEX_BIN), "--version"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        signature_proc = subprocess.run(
            [str(CODESIGN_PATH), "-dv", "--verbose=2", str(CODEX_BIN)],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {
            "available": False,
            "verified": False,
            "path": str(CODEX_BIN),
            "error_class": "pinned_runtime_verification_failed",
        }
    observed_version = (version_proc.stdout or version_proc.stderr).strip()
    signature_text = f"{signature_proc.stdout}\n{signature_proc.stderr}"
    signature_verified = bool(
        signature_proc.returncode == 0
        and "Authority=Developer ID Application: OpenAI OpCo, LLC" in signature_text
        and f"TeamIdentifier={CODEX_RUNTIME_TEAM_ID}" in signature_text
    )
    verified = bool(
        version_proc.returncode == 0
        and observed_version == CODEX_RUNTIME_VERSION
        and observed_sha == CODEX_RUNTIME_SHA256
        and signature_verified
    )
    return {
        "available": True,
        "verified": verified,
        "path": str(CODEX_BIN),
        "version": observed_version,
        "sha256": observed_sha,
        "signature_verified": signature_verified,
        "team_id": CODEX_RUNTIME_TEAM_ID,
        "error_class": None if verified else "pinned_runtime_drift",
    }


def default_state(evaluation_thread_id: str = DEFAULT_EVALUATION_THREAD) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": "evaluation_watch",
        "evaluation_thread_id": evaluation_thread_id,
        "extraction_thread_id": None,
        "created_at": iso_now(),
        "updated_at": iso_now(),
        "last_cycle_at": None,
        "last_observation": None,
        "last_steering_sha256": None,
        "last_steering_evidence_sha256": None,
        "steering_history": [],
        "pending_control_launch": None,
        "evaluation_receipt": None,
        "handoff": None,
        "queue_baseline": None,
        "completion_checks": [],
        "notification_sent": False,
        "notification_receipt": None,
        "disabled_verified": False,
    }


def load_state(path: Path = DEFAULT_STATE_PATH) -> dict[str, Any]:
    state = _read_json(path)
    if state is None:
        return default_state()
    if state.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported babysitter state schema")
    if state.get("phase") not in PHASES:
        raise ValueError("invalid babysitter phase")
    return state


def save_state(state: dict[str, Any], path: Path = DEFAULT_STATE_PATH) -> None:
    state["updated_at"] = iso_now()
    _write_json_atomic(path, state)


def locked_state(path: Path = DEFAULT_STATE_PATH) -> Iterator[dict[str, Any]]:
    class _Lock:
        def __enter__(self) -> dict[str, Any]:
            self.lock_path = path.with_suffix(".lock")
            self.lock_path.parent.mkdir(parents=True, exist_ok=True)
            self.handle = self.lock_path.open("a+", encoding="utf-8")
            deadline = time.monotonic() + STATE_LOCK_TIMEOUT_SECONDS
            while True:
                try:
                    fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        self.handle.close()
                        raise TimeoutError("babysitter state lock timed out")
                    time.sleep(0.05)
            self.state = load_state(path)
            return self.state

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            if exc_type is None:
                save_state(self.state, path)
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()

    return _Lock()  # type: ignore[return-value]


def session_path(thread_id: str) -> Path | None:
    matches = list((CODEX_HOME / "sessions").glob(f"**/*{thread_id}.jsonl"))
    return max(matches, key=lambda item: item.stat().st_mtime) if matches else None


def control_process_observation(thread_id: str) -> dict[str, Any]:
    """Find exact-thread CLI resumes without exposing their command lines."""
    try:
        proc = subprocess.run(
            ["ps", "-axo", "pid=,comm=,args="],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {"available": False, "alive": None, "error_class": "process_observation_unavailable"}
    if proc.returncode != 0:
        return {"available": False, "alive": None, "error_class": "process_observation_unavailable"}
    pids: set[int] = set()
    runtime_verified_pids: set[int] = set()
    for line in proc.stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        parts = stripped.split(maxsplit=2)
        if len(parts) != 3 or not parts[0].isdigit():
            continue
        pid_text, _comm, command = parts
        # macOS truncates ps(1)'s comm column to MAXCOMLEN, so a full-path
        # executable such as ~/.local/bin/codex appears as /Users/.../cod.
        # The first args token remains the complete executable path. Requiring
        # that token to be codex or caffeinate also keeps shell/grep commands
        # that merely mention a resume invocation from becoming false matches.
        command_parts = command.split(maxsplit=1)
        if not command_parts or Path(command_parts[0]).name not in {CODEX_BIN.name, CAFFEINATE_PATH.name}:
            continue
        try:
            argv = shlex.split(command)
        except ValueError:
            continue
        # `--cd` must precede the `resume` subcommand, so do not depend on the
        # brittle literal substring "exec resume".
        try:
            exec_index = argv.index("exec")
            resume_index = argv.index("resume", exec_index + 1)
        except ValueError:
            continue
        if thread_id in argv[resume_index + 1 :]:
            pid = int(pid_text)
            pids.add(pid)
            executable = Path(argv[0]).expanduser()
            runtime_verified = bool(
                executable == CODEX_BIN
                or (
                    executable == CAFFEINATE_PATH
                    and str(CODEX_BIN) in argv
                )
            )
            if runtime_verified:
                runtime_verified_pids.add(pid)
    return {
        "available": True,
        "alive": bool(pids),
        "process_count": len(pids),
        "pids": sorted(pids),
        "runtime_verified_pids": sorted(runtime_verified_pids),
    }


def managed_chatgpt_auth_observation() -> dict[str, Any]:
    command = [
        "/usr/bin/env",
        "-u", "OPENAI_API_KEY",
        "-u", "OPENAI_API_KEY_PATH",
        "-u", "CODEX_API_KEY",
        "-u", "AZURE_OPENAI_API_KEY",
        str(CODEX_BIN),
        "login",
        "status",
    ]
    try:
        proc = subprocess.run(command, capture_output=True, text=True, timeout=10, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return {"available": False, "authenticated": False, "mode": "unknown"}
    status = (proc.stdout + "\n" + proc.stderr).strip()
    authenticated = proc.returncode == 0 and status == "Logged in using ChatGPT"
    return {
        "available": True,
        "authenticated": authenticated,
        "mode": "managed_chatgpt" if authenticated else "not_managed_chatgpt",
    }


def goal_observation(thread_id: str, db_path: Path = DEFAULT_GOALS_DB_PATH) -> dict[str, Any]:
    """Read sanitized authoritative goal state without exposing the objective."""
    if not db_path.is_file():
        return {"available": False, "found": False, "error_class": "goals_db_unavailable"}
    try:
        conn = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True, timeout=1.0)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                """
                SELECT goal_id, status, tokens_used, time_used_seconds, updated_at_ms
                FROM thread_goals WHERE thread_id=?
                """,
                (thread_id,),
            ).fetchone()
        finally:
            conn.close()
    except sqlite3.Error:
        return {"available": False, "found": False, "error_class": "goals_db_unavailable"}
    if row is None:
        return {"available": True, "found": False}
    status = str(row["status"])
    if status not in VALID_GOAL_STATUSES:
        return {"available": False, "found": False, "error_class": "goals_db_schema_drift"}
    updated_at = dt.datetime.fromtimestamp(int(row["updated_at_ms"]) / 1000, tz=dt.timezone.utc).astimezone().isoformat()
    return {
        "available": True,
        "found": True,
        "goal_id": str(row["goal_id"]),
        "status": status,
        "tokens_used": int(row["tokens_used"]),
        "time_used_seconds": int(row["time_used_seconds"]),
        "updated_at": updated_at,
    }


def session_observation(thread_id: str) -> dict[str, Any]:
    path = session_path(thread_id)
    if path is None:
        return {"thread_id": thread_id, "found": False}
    stat = path.stat()
    with path.open("rb") as tail_handle:
        tail_handle.seek(max(0, stat.st_size - 512_000))
        tail = tail_handle.read()
    last_timestamp = None
    last_message_type = None
    session_turn_in_progress = False
    current_turn_id = None
    latest_task_started_at = None
    latest_task_completed_at = None
    # Scan the append-only JSONL once for lifecycle state. Goal status comes from
    # Codex's goal database; matching arbitrary rollout text is not authoritative.
    with path.open("rb") as handle:
        for raw in handle:
            try:
                event = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            last_timestamp = event.get("timestamp") or last_timestamp
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            payload_type = payload.get("type")
            last_message_type = payload_type or event.get("type") or last_message_type
            if payload_type == "task_started":
                session_turn_in_progress = True
                current_turn_id = payload.get("turn_id")
                latest_task_started_at = event.get("timestamp") or latest_task_started_at
            elif payload_type in {"task_complete", "turn_aborted"}:
                if current_turn_id is None or payload.get("turn_id") == current_turn_id:
                    session_turn_in_progress = False
                    current_turn_id = None
                latest_task_completed_at = event.get("timestamp") or latest_task_completed_at
    age_seconds = max(0, int(dt.datetime.now().timestamp() - stat.st_mtime))
    process = control_process_observation(thread_id)
    recent_session_activity = age_seconds <= OPEN_TURN_ACTIVITY_GRACE_SECONDS
    confirmed_process_alive = process.get("alive") is True
    liveness_unknown = bool(session_turn_in_progress and not recent_session_activity and not process.get("available"))
    turn_in_progress = bool(
        confirmed_process_alive
        or (session_turn_in_progress and (recent_session_activity or liveness_unknown))
    )
    orphaned_open_turn = bool(
        session_turn_in_progress
        and not recent_session_activity
        and process.get("available") is True
        and not confirmed_process_alive
    )
    return {
        "thread_id": thread_id,
        "found": True,
        "path": str(path),
        "size_bytes": stat.st_size,
        "mtime": dt.datetime.fromtimestamp(stat.st_mtime).astimezone().isoformat(),
        "age_seconds": age_seconds,
        "recent_sha256": _sha256_bytes(tail),
        "last_timestamp": last_timestamp,
        "last_message_type": last_message_type,
        "session_turn_in_progress": session_turn_in_progress,
        "turn_in_progress": turn_in_progress,
        "orphaned_open_turn": orphaned_open_turn,
        "liveness_unknown": liveness_unknown,
        "recent_session_activity": recent_session_activity,
        "open_turn_activity_grace_seconds": OPEN_TURN_ACTIVITY_GRACE_SECONDS,
        "control_process": process,
        "current_turn_id": current_turn_id,
        "latest_task_started_at": latest_task_started_at,
        "latest_task_completed_at": latest_task_completed_at,
    }


def phase_progress_observation(phase: str, progress: dict[str, Any]) -> tuple[bool, list[str]]:
    if phase in {"evaluation_watch", "extraction_handoff"}:
        basis = ["goal_status_changed", "semantic_milestone_changed"]
    elif phase in {"extraction_watch", "completion_confirmation"}:
        basis = ["goal_status_changed", "queue_state_changed"]
    else:
        basis = []
    return any(progress.get(key) is True for key in basis), basis


def evaluation_milestone_snapshot(root: Path = DEFAULT_EVALUATION_ROOT) -> dict[str, Any]:
    """Fingerprint immutable semantic milestones without hydrating their contents."""
    if not root.is_dir():
        return {"available": False, "artifact_count": 0, "snapshot_sha256": None}

    def is_milestone(path: Path) -> bool:
        name = path.name
        return (
            name in {
                "capacity.json",
                "outcome.json",
                "prelaunch-capacity.json",
                "selection-result.json",
                "sidecar.json",
                "terminal.json",
            }
            or "receipt" in name
            or "runtime-lock" in name
            or "capacity-audit" in name
            or "capacity-policy" in name
        )

    records: list[tuple[str, int]] = []
    newest_path = None
    newest_mtime = 0.0
    for path in root.rglob("*.json"):
        if not path.is_file() or not is_milestone(path):
            continue
        relative = str(path.relative_to(root))
        try:
            stat = path.stat()
        except OSError:
            return {
                "available": False,
                "artifact_count": len(records),
                "snapshot_sha256": None,
                "error_class": "milestone_scan_unavailable",
            }
        # Milestone paths are append-only by contract. Path plus byte size is
        # enough to detect a newly frozen artifact without forcing FileProvider
        # hydration of every historical sidecar. Acceptance verification still
        # recomputes full SHA-256 bindings for the five trusted receipt roles.
        records.append((relative, int(stat.st_size)))
        mtime = stat.st_mtime
        if mtime > newest_mtime:
            newest_mtime = mtime
            newest_path = relative
    records.sort()
    return {
        "available": True,
        "fingerprint_basis": "immutable_relative_path_and_size",
        "artifact_count": len(records),
        "snapshot_sha256": _sha256_json(records),
        "newest_path": newest_path,
        "newest_mtime": dt.datetime.fromtimestamp(newest_mtime).astimezone().isoformat() if newest_path else None,
    }


def queue_snapshot(db_path: Path = DEFAULT_DB_PATH) -> dict[str, Any]:
    uri = f"file:{db_path.resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT job_type, status, COUNT(*) AS count
            FROM jobs
            WHERE job_type IN ('label_segment', 'episode_context', 'manual_transcript_required')
            GROUP BY job_type, status
            ORDER BY job_type, status
            """
        ).fetchall()
        counts = {f"{row['job_type']}:{row['status']}": int(row["count"]) for row in rows}
        pending_v31 = conn.execute(
            """
            SELECT COUNT(*) FROM jobs
            WHERE job_type='label_segment' AND status='pending'
              AND json_extract(payload_json, '$.label_pack')='ai_discourse_v3_1'
            """
        ).fetchone()[0]
        stale_claims = conn.execute(
            """
            SELECT COUNT(*) FROM jobs
            WHERE status='claimed' AND leased_until IS NOT NULL AND leased_until < ?
              AND job_type IN ('label_segment', 'episode_context')
            """,
            (iso_now(),),
        ).fetchone()[0]
        totals = conn.execute(
            """
            SELECT
              (SELECT COUNT(*) FROM episodes),
              (SELECT COUNT(DISTINCT episode_id) FROM segments),
              (SELECT COUNT(*) FROM segments),
              (SELECT COUNT(*) FROM labels WHERE label_pack='ai_discourse_v3_1'),
              (SELECT COUNT(*) FROM episode_context_runs WHERE label_pack='ai_discourse_v3_1' AND status='completed')
            """
        ).fetchone()
        snapshot = {
            "captured_at": iso_now(),
            "database": str(db_path.resolve()),
            "job_counts": counts,
            "pending_v31_segments": int(pending_v31),
            "stale_extraction_claims": int(stale_claims),
            "episodes": int(totals[0]),
            "segmented_episodes": int(totals[1]),
            "segments": int(totals[2]),
            "v31_labels": int(totals[3]),
            "v31_completed_episode_contexts": int(totals[4]),
        }
        snapshot["snapshot_sha256"] = _sha256_json({key: value for key, value in snapshot.items() if key != "captured_at"})
        return snapshot
    finally:
        conn.close()


def completion_audit(db_path: Path, manifest_path: Path | None) -> dict[str, Any]:
    queue = queue_snapshot(db_path)
    manifest = _read_json(manifest_path, {}) if manifest_path else {}
    required_manifest_flags = (
        "all_app_server_usage_accounted",
        "all_outputs_validated",
        "no_unresolved_audit_failures",
        "terminal_quarantine_audited",
    )
    flags = {name: manifest.get(name) is True for name in required_manifest_flags}
    uri = f"file:{db_path.resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        failed_rows = conn.execute(
            """
            SELECT id, job_type FROM jobs
            WHERE status='failed' AND job_type IN ('label_segment', 'episode_context')
            """
        ).fetchall()
        eligible_segments_without_v31_output = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM segments AS segment
                WHERE NOT EXISTS (
                  SELECT 1 FROM labels AS label
                  WHERE label.segment_id=segment.id
                    AND label.label_pack='ai_discourse_v3_1'
                    AND label.status IN ('ready', 'quarantined_needs_review')
                )
                """
            ).fetchone()[0]
        )
        segmented_episodes_without_completed_v31_context = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM (
                  SELECT DISTINCT segment.episode_id FROM segments AS segment
                ) AS eligible
                WHERE NOT EXISTS (
                  SELECT 1 FROM episode_context_runs AS context
                  WHERE context.episode_id=eligible.episode_id
                    AND context.label_pack='ai_discourse_v3_1'
                    AND context.status='completed'
                )
                """
            ).fetchone()[0]
        )
    finally:
        conn.close()
    # Failed queue rows remain blocking until a future database-backed quarantine
    # mechanism can be verified. A manifest-provided list of ids is not evidence.
    unresolved_failed_labels = sum(1 for _job_id, kind in failed_rows if kind == "label_segment")
    unresolved_failed_contexts = sum(1 for _job_id, kind in failed_rows if kind == "episode_context")
    unresolved_manual_transcript_required = sum(
        count
        for key, count in queue["job_counts"].items()
        if key.startswith("manual_transcript_required:")
        and key != "manual_transcript_required:completed"
    )
    blocking = {
        "pending_v31_segments": queue["pending_v31_segments"],
        "eligible_segments_without_v31_output": eligible_segments_without_v31_output,
        "claimed_label_segments": queue["job_counts"].get("label_segment:claimed", 0),
        "unresolved_failed_label_segments": unresolved_failed_labels,
        "pending_episode_contexts": queue["job_counts"].get("episode_context:pending", 0),
        "segmented_episodes_without_completed_v31_context": segmented_episodes_without_completed_v31_context,
        "claimed_episode_contexts": queue["job_counts"].get("episode_context:claimed", 0),
        "unresolved_failed_episode_contexts": unresolved_failed_contexts,
        "pending_manual_transcript_required": queue["job_counts"].get("manual_transcript_required:pending", 0),
        "unresolved_manual_transcript_required": unresolved_manual_transcript_required,
        "stale_extraction_claims": queue["stale_extraction_claims"],
    }
    pass_state = all(value == 0 for value in blocking.values()) and all(flags.values())
    result = {
        "schema_version": "pif_pipeline_completion_audit_v1",
        "checked_at": iso_now(),
        "pass": pass_state,
        "blocking_counts": blocking,
        "manifest_flags": flags,
        "manifest_path": str(manifest_path.resolve()) if manifest_path else None,
        "queue": queue,
    }
    result["audit_sha256"] = _sha256_json(result)
    return result


def record_steering(state: dict[str, Any], text: str) -> dict[str, Any]:
    digest = _sha256_bytes(text.strip().encode())
    if digest == state.get("last_steering_sha256"):
        raise ValueError("duplicate steering message refused")
    entry = {"at": iso_now(), "sha256": digest, "phase": state["phase"]}
    state["last_steering_sha256"] = digest
    state.setdefault("steering_history", []).append(entry)
    state["steering_history"] = state["steering_history"][-50:]
    return entry


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and value == value.lower() and all(character in "0123456789abcdef" for character in value)


def _resolve_evaluation_artifact(root: Path, relative_path: Any) -> Path:
    if not isinstance(relative_path, str) or not relative_path.strip():
        raise ValueError("evaluation artifact path is invalid")
    root = root.expanduser().resolve()
    candidate = Path(relative_path).expanduser()
    unresolved = candidate if candidate.is_absolute() else root / candidate
    try:
        resolved = unresolved.resolve(strict=True)
    except OSError as exc:
        raise ValueError("evaluation artifact is unavailable") from exc
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise ValueError("evaluation artifact escapes the trusted root")
    cursor = unresolved
    while cursor != root and cursor.is_relative_to(root):
        if cursor.is_symlink():
            raise ValueError("evaluation artifact symlinks are not allowed")
        cursor = cursor.parent
    return resolved


def _exact_nonnegative_int(value: Any, *, positive: bool = False) -> bool:
    return type(value) is int and value >= (1 if positive else 0)


def verify_evaluation_receipt(
    receipt_path: Path,
    evaluation_root: Path = DEFAULT_EVALUATION_ROOT,
) -> dict[str, Any]:
    receipt = _read_json(receipt_path)
    if not isinstance(receipt, dict):
        raise ValueError("evaluation receipt must be a JSON object")
    if receipt.get("schema_version") != EVALUATION_RECEIPT_SCHEMA:
        raise ValueError("evaluation receipt schema is invalid")
    required_true = (
        "goal_complete",
        "judge_gate_passed",
        "development_frozen",
        "untouched_holdout_passed",
        "semantic_noninferior_or_better",
        "managed_app_server_auth_only",
        "usage_and_cache_telemetry_complete",
        "production_unchanged_during_evaluation",
    )
    failures = [key for key in required_true if receipt.get(key) is not True]
    ratio = receipt.get("production_amortized_total_token_ratio")
    if type(ratio) not in (int, float) or not math.isfinite(ratio) or not 0 <= ratio <= 0.28:
        failures.append("production_amortized_total_token_ratio")
    artifact_hashes = receipt.get("artifact_hashes")
    if not isinstance(artifact_hashes, dict) or set(artifact_hashes) != EVALUATION_ARTIFACT_ROLES:
        failures.append("artifact_hashes")
    if failures:
        raise ValueError("evaluation receipt failed: " + ", ".join(failures))

    trusted_root = evaluation_root.expanduser().resolve()
    receipt_resolved = _resolve_evaluation_artifact(trusted_root, str(receipt_path.expanduser().resolve()))
    artifacts: dict[str, dict[str, Any]] = {}
    verified_bindings: dict[str, dict[str, str]] = {}
    for role in sorted(EVALUATION_ARTIFACT_ROLES):
        binding = artifact_hashes[role]
        if not isinstance(binding, dict) or set(binding) != {"path", "sha256"} or not _is_sha256(binding.get("sha256")):
            raise ValueError(f"evaluation artifact binding is invalid: {role}")
        path = _resolve_evaluation_artifact(trusted_root, binding["path"])
        measured_sha = _sha256_bytes(path.read_bytes())
        if measured_sha != binding["sha256"]:
            raise ValueError(f"evaluation artifact hash mismatch: {role}")
        payload = _read_json(path)
        if not isinstance(payload, dict) or payload.get("artifact_role") != role:
            raise ValueError(f"evaluation artifact schema is invalid: {role}")
        artifacts[role] = payload
        verified_bindings[role] = {"path": str(path), "sha256": measured_sha}

    expected_bindings = {field: receipt.get(field) for field in EVALUATION_BINDING_FIELDS}
    if not isinstance(expected_bindings["evaluation_id"], str) or not expected_bindings["evaluation_id"]:
        raise ValueError("evaluation_id is invalid")
    for field in EVALUATION_BINDING_FIELDS[1:]:
        if not _is_sha256(expected_bindings[field]):
            raise ValueError(f"evaluation binding is invalid: {field}")
    for role, payload in artifacts.items():
        for field, expected in expected_bindings.items():
            if payload.get(field) != expected:
                raise ValueError(f"evaluation binding mismatch: {role}.{field}")

    judge = artifacts["judge_gate"]
    if any(
        judge.get(field) is not True
        for field in (
            "gate_passed",
            "semantic_noninferior_or_better",
            "ab_ba_order_balanced",
            "shared_augmented_reference",
            "abstention_enabled",
        )
    ):
        raise ValueError("judge gate artifact failed")
    development = artifacts["development_freeze"]
    if any(
        development.get(field) is not True
        for field in (
            "development_frozen",
            "selection_frozen",
            "development_winner_frozen",
        )
    ):
        raise ValueError("development freeze artifact failed")
    if not isinstance(development.get("winner_system_id"), str) or not development[
        "winner_system_id"
    ].strip():
        raise ValueError("development winner identity is invalid")
    winner_system_id = development["winner_system_id"].strip()
    holdout = artifacts["untouched_holdout"]
    if any(
        holdout.get(field) is not True
        for field in (
            "untouched_holdout",
            "holdout_passed",
            "semantic_noninferior_or_better",
            "density_stratified",
            "intent_to_treat_failures_included",
        )
    ):
        raise ValueError("untouched holdout artifact failed")
    if holdout.get("paired_bootstrap_unit") != "source_cluster":
        raise ValueError("untouched holdout bootstrap design is invalid")
    if holdout.get("winner_system_id") != winner_system_id:
        raise ValueError("untouched holdout winner identity is invalid")
    baseline_system_id = holdout.get("baseline_system_id")
    if (
        not isinstance(baseline_system_id, str)
        or not baseline_system_id.strip()
        or baseline_system_id.strip() == winner_system_id
    ):
        raise ValueError("untouched holdout baseline identity is invalid")
    baseline_system_id = baseline_system_id.strip()
    if not _exact_nonnegative_int(holdout.get("holdout_item_count"), positive=True):
        raise ValueError("untouched holdout item count is invalid")
    ci_lower = holdout.get("paired_bootstrap_ci_lower")
    confidence = holdout.get("paired_bootstrap_confidence")
    exact_evidence_rate = holdout.get("exact_evidence_rate")
    if (
        type(ci_lower) not in (int, float)
        or not math.isfinite(ci_lower)
        or ci_lower > 1.0
        or ci_lower < -0.03
    ):
        raise ValueError("untouched holdout semantic noninferiority is invalid")
    if (
        type(confidence) not in (int, float)
        or not math.isfinite(confidence)
        or not math.isclose(float(confidence), 0.95, rel_tol=0.0, abs_tol=1e-12)
    ):
        raise ValueError("untouched holdout confidence is invalid")
    if (
        type(exact_evidence_rate) not in (int, float)
        or not math.isfinite(exact_evidence_rate)
        or not math.isclose(
            float(exact_evidence_rate), 1.0, rel_tol=0.0, abs_tol=1e-12
        )
    ):
        raise ValueError("untouched holdout exact evidence rate is invalid")
    integrity = artifacts["production_integrity"]
    if (
        integrity.get("managed_app_server_auth_only") is not True
        or integrity.get("production_unchanged") is not True
        or integrity.get("raw_session_token_replay") is not False
        or integrity.get("api_key_billing") is not False
    ):
        raise ValueError("production integrity artifact failed")

    usage = artifacts["usage_telemetry"]
    for field in (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
        "production_amortized_total_tokens",
    ):
        if not _exact_nonnegative_int(usage.get(field)):
            raise ValueError(f"usage telemetry is invalid: {field}")
    for field in ("production_baseline_total_tokens", "expected_turn_count", "measured_turn_count"):
        if not _exact_nonnegative_int(usage.get(field), positive=True):
            raise ValueError(f"usage telemetry is invalid: {field}")
    for field in ("paired_item_count", "baseline_item_count", "candidate_item_count"):
        if not _exact_nonnegative_int(usage.get(field), positive=True):
            raise ValueError(f"usage telemetry is invalid: {field}")
    if not _exact_nonnegative_int(usage.get("unknown_usage_turn_count")):
        raise ValueError("usage telemetry is invalid: unknown_usage_turn_count")
    wall_time = usage.get("wall_time_seconds")
    if type(wall_time) not in (int, float) or not math.isfinite(wall_time) or wall_time <= 0:
        raise ValueError("usage telemetry is invalid: wall_time_seconds")
    if usage["cached_input_tokens"] > usage["input_tokens"] or usage["reasoning_output_tokens"] > usage["output_tokens"]:
        raise ValueError("usage telemetry token subsets are invalid")
    if usage["total_tokens"] != usage["input_tokens"] + usage["output_tokens"]:
        raise ValueError("usage telemetry total is inconsistent")
    if (
        usage["measured_turn_count"] != usage["expected_turn_count"]
        or usage.get("usage_complete") is not True
        or usage.get("cache_telemetry_complete") is not True
        or usage.get("failed_and_retried_calls_included") is not True
        or usage.get("same_exact_items") is not True
        or usage.get("same_concurrency_and_workers") is not True
        or usage.get("same_retry_and_fallback_policy") is not True
        or usage.get("same_cache_policy") is not True
        or usage.get("same_quota_window") is not True
        or usage.get("same_measurement_boundary") is not True
        or usage.get("episode_context_generation_accounted") is not True
        or usage.get("unknown_usage_turn_count") != 0
    ):
        raise ValueError("usage telemetry coverage is incomplete")
    if (
        usage.get("winner_system_id") != winner_system_id
        or usage.get("baseline_system_id") != baseline_system_id
    ):
        raise ValueError("usage telemetry system identity is inconsistent")
    paired_item_count = usage["paired_item_count"]
    if (
        usage["baseline_item_count"] != paired_item_count
        or usage["candidate_item_count"] != paired_item_count
        or holdout["holdout_item_count"] != paired_item_count
    ):
        raise ValueError("usage telemetry paired item coverage is inconsistent")
    measured_ratio = usage["production_amortized_total_tokens"] / usage["production_baseline_total_tokens"]
    if not math.isclose(float(ratio), measured_ratio, rel_tol=0.0, abs_tol=1e-12):
        raise ValueError("production-amortized token ratio is inconsistent")
    return {
        "path": str(receipt_resolved),
        "sha256": _sha256_bytes(receipt_resolved.read_bytes()),
        "verified_at": iso_now(),
        "production_amortized_total_token_ratio": ratio,
        "evaluation_id": expected_bindings["evaluation_id"],
        "artifact_hashes": verified_bindings,
    }


def render_handoff(state: dict[str, Any], queue: dict[str, Any]) -> str:
    receipt = state.get("evaluation_receipt") or {}
    return f"""# Production extraction handoff

Process every eligible Podcast Intelligence Factory v3.1 segment that lacks validated extraction.

Hard constraints:
- Use only the official persistent Codex app-server with managed ChatGPT authentication for semantic calls.
- Never use API-key billing, raw session-token replay, semantic regex/keyword pruning, or the legacy pif-local-extractor command.
- Preserve exact evidence, offsets, provenance, structured output, thread lifecycle, and complete input/cached/output/reasoning usage telemetry.
- Use the frozen winning configuration from `{receipt.get('path')}` (sha256 `{receipt.get('sha256')}`).
- Work in bounded resumable batches and checkpoint after every validated batch.
- An item is terminal only when validated or quarantined with a stable reason, source evidence, attempts, and an audit record.
- Do not declare completion from prose. Write a machine-readable completion manifest with: all_app_server_usage_accounted, all_outputs_validated, no_unresolved_audit_failures, and terminal_quarantine_audited.

Queue baseline sha256: `{queue['snapshot_sha256']}`
Pending v3.1 segments at handoff: {queue['pending_v31_segments']}
Pending manual-transcript items: {queue['job_counts'].get('manual_transcript_required:pending', 0)}

First inspect the frozen evaluation receipt and current production runner code. Then implement or select the hash-bound app-server production runner, verify it on a small bounded batch, and continue processing autonomously. Keep the babysitter-compatible progress and completion artifacts sanitized: counts, hashes, usage, failure classes, and paths only; no transcript text.
"""


def explicit_telegram(message: str) -> dict[str, Any]:
    config_path = CODEX_HOME / "memories" / "automation" / "ops" / "config.json"
    config = _read_json(config_path, {})
    telegram = config.get("telegram", {})
    required = ("repo_path", "service", "channel", "target")
    if any(not telegram.get(key) for key in required):
        return {"ok": False, "status": "unconfigured"}
    remote = [
        "openclaw", "message", "send", "--channel", str(telegram["channel"]),
        "--target", str(telegram["target"]), "--message", message,
    ]
    if telegram.get("account_id"):
        remote.extend(["--account", str(telegram["account_id"])])
    import shlex
    command = [
        "railway", "ssh", "--service", str(telegram["service"]), "--",
        "bash -lc " + shlex.quote(shlex.join(remote)),
    ]
    try:
        proc = subprocess.run(
            command,
            cwd=Path(str(telegram["repo_path"])).expanduser(),
            capture_output=True,
            text=True,
            timeout=int(telegram.get("timeout_seconds", 90)),
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "status": "timeout"}
    return {"ok": proc.returncode == 0, "status": "sent" if proc.returncode == 0 else "error", "exit_code": proc.returncode}


def cmd_init(args: argparse.Namespace) -> int:
    path = Path(args.state)
    if path.exists() and not args.force:
        print(json.dumps(load_state(path), indent=2))
        return 0
    save_state(default_state(args.evaluation_thread), path)
    print(json.dumps(load_state(path), indent=2))
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    path = Path(args.state)
    # Snapshot coordination state under the lock, then release it before slow
    # session, SQLite, and FileProvider-backed artifact observations. A stalled
    # observation must never prevent launch-resume from acquiring the lock.
    with locked_state(path) as state:
        pending_extraction = _reconcile_pending_extraction_launch(state)
        state_snapshot = copy.deepcopy(state)
    previous = state_snapshot.get("last_observation") if isinstance(state_snapshot.get("last_observation"), dict) else None
    phase = state_snapshot["phase"]
    thread_id = state_snapshot["evaluation_thread_id"] if phase in {"evaluation_watch", "extraction_handoff"} else state_snapshot.get("extraction_thread_id")
    observation = session_observation(thread_id) if thread_id else {"found": False}
    goal = goal_observation(thread_id, Path(args.goals_db)) if thread_id else {"available": True, "found": False}
    if not goal.get("available"):
        prior_goal = ((state_snapshot.get("last_observation") or {}).get("goal") or {})
        if prior_goal.get("found") and prior_goal.get("status") in VALID_GOAL_STATUSES:
            goal = {**prior_goal, "available": False, "stale": True, "error_class": goal.get("error_class")}
    observation["observed_goal_status"] = goal.get("status") if goal.get("found") else None
    if phase in {"extraction_watch", "completion_confirmation"}:
        launch_receipt = state_snapshot.get("last_extraction_launch") or {}
        session_name = str(launch_receipt.get("session_name", ""))
        exit_receipt_path = Path(str(launch_receipt.get("exit_receipt_path", ""))).expanduser()
        exit_receipt = _read_json(exit_receipt_path, {})
        managed_launch_alive = bool(
            (_valid_tmux_session_name(session_name) and _tmux_has_session(session_name))
            or _pid_alive(exit_receipt.get("runner_pid"))
        )
        observation["managed_detached_launch"] = {
            "session_name": session_name or None,
            "alive": managed_launch_alive,
            "exit_status": exit_receipt.get("status"),
            "exit_code": exit_receipt.get("exit_code"),
        }
        if managed_launch_alive:
            observation["turn_in_progress"] = True
    queue = queue_snapshot(Path(args.db))
    milestones = evaluation_milestone_snapshot(Path(args.evaluation_root)) if phase in {"evaluation_watch", "extraction_handoff"} else {"available": False, "artifact_count": 0, "snapshot_sha256": None}
    prior_goal = (previous or {}).get("goal") or {}
    prior_milestones = (previous or {}).get("milestones") or {}
    progress = {
        "comparable_to_previous_cycle": previous is not None,
        "goal_status_changed": (
            goal.get("status") != prior_goal.get("status")
            if previous is not None and goal.get("found") and prior_goal.get("found") and not goal.get("stale")
            else None
        ),
        "semantic_milestone_changed": (
            milestones.get("snapshot_sha256") != prior_milestones.get("snapshot_sha256")
            if previous is not None and milestones.get("available") and prior_milestones.get("available")
            else None
        ),
        "queue_state_changed": (
            queue["snapshot_sha256"] != (previous or {}).get("queue_sha256")
            if previous is not None
            else None
        ),
    }
    progress_observed, progress_basis = phase_progress_observation(phase, progress)
    progress["progress_basis"] = progress_basis
    progress["progress_observed"] = progress_observed
    extraction_queue_incomplete = bool(
        phase in {"extraction_watch", "completion_confirmation"}
        and (
            queue["pending_v31_segments"] > 0
            or queue["job_counts"].get("label_segment:claimed", 0) > 0
            or queue["job_counts"].get("episode_context:pending", 0) > 0
            or queue["job_counts"].get("episode_context:claimed", 0) > 0
            or queue["job_counts"].get("manual_transcript_required:pending", 0) > 0
        )
    )
    goal_incomplete = bool(goal.get("found") and goal.get("status") != "complete")
    workflow_incomplete = goal_incomplete or extraction_queue_incomplete
    supervision = {
        "recovery_needed": bool(phase != "complete" and workflow_incomplete and not observation.get("turn_in_progress")),
        "reason": (
            "orphaned_open_turn"
            if observation.get("orphaned_open_turn")
            else "inactive_incomplete_thread"
            if phase != "complete" and workflow_incomplete and not observation.get("turn_in_progress")
            else None
        ),
        "extraction_queue_incomplete": extraction_queue_incomplete,
    }
    with locked_state(path) as state:
        current_thread_id = state["evaluation_thread_id"] if state["phase"] in {"evaluation_watch", "extraction_handoff"} else state.get("extraction_thread_id")
        if state["phase"] != phase or current_thread_id != thread_id:
            raise ValueError("babysitter state changed during status observation; retry")
        state["last_cycle_at"] = iso_now()
        state["last_observation"] = {
            "thread": observation,
            "goal": goal,
            "milestones": milestones,
            "queue_sha256": queue["snapshot_sha256"],
        }
        if state.get("queue_baseline") is None:
            state["queue_baseline"] = queue
        output = {
            "state": copy.deepcopy(state),
            "thread": observation,
            "goal": goal,
            "milestones": milestones,
            "progress": progress,
            "supervision": supervision,
            "pending_extraction_launch": pending_extraction,
            "queue": queue,
        }
    print(json.dumps(output, indent=2))
    return 0


def cmd_record_steering(args: argparse.Namespace) -> int:
    text = Path(args.message_file).read_text(encoding="utf-8")
    with locked_state(Path(args.state)) as state:
        result = record_steering(state, text)
    print(json.dumps(result, indent=2))
    return 0


def _valid_tmux_session_name(value: str) -> bool:
    return bool(value) and len(value) <= 80 and all(character.isalnum() or character in "-_." for character in value)


def _tmux_has_session(session_name: str) -> bool:
    proc = subprocess.run(
        [str(TMUX_PATH), "has-session", "-t", f"={session_name}"],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    return proc.returncode == 0


def _pid_alive(pid: Any) -> bool:
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except (OSError, PermissionError):
        return False
    return True


def _registered_thread(state: dict[str, Any]) -> str | None:
    if state["phase"] in {"evaluation_watch", "extraction_handoff"}:
        return state.get("evaluation_thread_id")
    if state["phase"] in {"extraction_watch", "completion_confirmation"}:
        return state.get("extraction_thread_id")
    return None


def _expected_resume_session_name(state: dict[str, Any], thread_id: str) -> str:
    lane = "evaluation" if state["phase"] in {"evaluation_watch", "extraction_handoff"} else "extraction"
    return f"pif-{lane}-{thread_id[:8]}"


def _safe_orphan_override(
    state: dict[str, Any],
    observation: dict[str, Any],
    process: dict[str, Any],
    session_name: str,
) -> bool:
    """Prove the last detached owner died before overriding a raw open turn."""
    if process.get("available") is not True or process.get("alive") is True:
        return False
    if _tmux_has_session(session_name):
        return False
    launch = next(
        (
            candidate
            for candidate in (state.get("last_control_launch"), state.get("last_extraction_launch"))
            if isinstance(candidate, dict)
            and candidate.get("thread_id") == observation.get("thread_id")
            and candidate.get("session_name") == session_name
        ),
        None,
    )
    if launch is None:
        return False
    receipt_path = Path(str(launch.get("exit_receipt_path", ""))).expanduser()
    receipt = _read_json(receipt_path, {})
    if not isinstance(receipt, dict):
        return False
    if launch.get("launch_id") is not None and receipt.get("launch_id") != launch.get("launch_id"):
        return False
    status = receipt.get("status")
    # A stale running receipt has no terminal ownership boundary and cannot
    # rule out a later app/manual turn. Only a matching terminal receipt can.
    if status != "exited":
        return False
    task_started = _parse_iso(observation.get("latest_task_started_at"))
    owner_finished = _parse_iso(receipt.get("finished_at"))
    if task_started is None or owner_finished is None:
        return False
    # A task that began after the detached owner exited may belong to a user or
    # app process we do not own. Never override that turn.
    return task_started <= owner_finished.astimezone(task_started.tzinfo)


def _detached_runner_text(
    *,
    thread_id: str,
    message_path: Path,
    events_path: Path,
    errors_path: Path,
    exit_receipt_path: Path,
    model: str,
    reasoning_effort: str,
    launch_id: str,
) -> str:
    codex_command = [
        "/usr/bin/env",
        "-u", "OPENAI_API_KEY",
        "-u", "OPENAI_API_KEY_PATH",
        "-u", "CODEX_API_KEY",
        "-u", "AZURE_OPENAI_API_KEY",
        str(CAFFEINATE_PATH),
        "-i",
        "-m",
        "-s",
        str(CODEX_BIN),
        "exec",
        "--cd", str(PROJECT_ROOT),
        "--dangerously-bypass-approvals-and-sandbox",
        "--skip-git-repo-check",
        "--json",
        "--model", model,
        "--config", f'model_reasoning_effort="{reasoning_effort}"',
        "resume",
        thread_id,
        "-",
    ]
    # Put the semantic CLI in its own session/process group.  tmux owns only the
    # small shell supervisor, while both the shell trap and the watchdog can
    # terminate the exact receipt-bound Codex group without touching a manual or
    # app-owned turn.  Starting Codex from the home-scoped control directory also
    # avoids project FileProvider access during runtime initialization; the
    # recovery prompt supplies the absolute project root for explicit tool work.
    codex_command[codex_command.index("--cd") + 1] = str(CONTROL_WORKING_DIRECTORY)
    process_group_command = [
        "/usr/bin/python3",
        "-c",
        "import os,sys; os.setsid(); os.execvpe(sys.argv[1], sys.argv[1:], os.environ)",
        *codex_command,
    ]
    runtime_json = json.dumps(
        {
            "path": str(CODEX_BIN),
            "version": CODEX_RUNTIME_VERSION,
            "sha256": CODEX_RUNTIME_SHA256,
            "team_id": CODEX_RUNTIME_TEAM_ID,
        },
        separators=(",", ":"),
    )
    launch_json = json.dumps(launch_id)
    auth_running_json = '{"schema_version":"pif_control_exit_v1","launch_id":' + launch_json + ',"status":"running","stage":"auth_preflight","started_at":"%s","runner_pid":%d,"codex_pid":null,"codex_runtime":' + runtime_json + '}\n'
    dispatch_running_json = '{"schema_version":"pif_control_exit_v1","launch_id":' + launch_json + ',"status":"running","stage":"codex_dispatched","started_at":"%s","runner_pid":%d,"codex_pid":%d,"process_group_id":%d,"codex_runtime":' + runtime_json + '}\n'
    auth_terminal_json = '{"schema_version":"pif_control_exit_v1","launch_id":' + launch_json + ',"status":"exited","stage":"auth_failed","started_at":"%s","finished_at":"%s","runner_pid":%d,"codex_pid":null,"exit_code":%d,"codex_runtime":' + runtime_json + '}\n'
    terminal_json = '{"schema_version":"pif_control_exit_v1","launch_id":' + launch_json + ',"status":"exited","stage":"codex_exited","started_at":"%s","finished_at":"%s","runner_pid":%d,"codex_pid":%d,"process_group_id":%d,"exit_code":%d,"codex_runtime":' + runtime_json + '}\n'
    receipt_temp = exit_receipt_path.with_suffix(exit_receipt_path.suffix + ".tmp")
    auth_command = codex_command[:9] + [str(CODEX_BIN), "login", "status"]
    kick_command = shlex.join([str(CODEX_HOME / "bin" / "codex-cron"), "run", "pif-pipeline-watchdog"])
    kick_script = f"/bin/sleep 2; {kick_command}"
    return "\n".join(
        [
            "#!/bin/zsh",
            "set +e",
            "umask 077",
            'STARTED_AT="$(/bin/date -u +%Y-%m-%dT%H:%M:%SZ)"',
            f"/usr/bin/printf {shlex.quote(auth_running_json)} \"$STARTED_AT\" \"$$\" > {shlex.quote(str(receipt_temp))}",
            f"/bin/mv {shlex.quote(str(receipt_temp))} {shlex.quote(str(exit_receipt_path))}",
            f'AUTH_STATUS="$({shlex.join(auth_command)} 2>&1)"',
            'if [[ "$AUTH_STATUS" != "Logged in using ChatGPT" ]]; then',
            '  FINISHED_AT="$(/bin/date -u +%Y-%m-%dT%H:%M:%SZ)"',
            f"  /usr/bin/printf {shlex.quote(auth_terminal_json)} \"$STARTED_AT\" \"$FINISHED_AT\" \"$$\" 78 > {shlex.quote(str(receipt_temp))}",
            f"  /bin/mv {shlex.quote(str(receipt_temp))} {shlex.quote(str(exit_receipt_path))}",
            "  exit 78",
            "fi",
            "unset AUTH_STATUS",
            "CODEX_PID=''",
            "terminate_codex_group() {",
            "  trap - HUP INT TERM",
            "  if [[ -n \"$CODEX_PID\" ]]; then",
            "    /bin/kill -TERM -- \"-$CODEX_PID\" 2>/dev/null || true",
            "    /bin/sleep 1",
            "    /bin/kill -KILL -- \"-$CODEX_PID\" 2>/dev/null || true",
            "  fi",
            "}",
            "trap terminate_codex_group HUP INT TERM",
            f"{shlex.join(process_group_command)} < {shlex.quote(str(message_path))} >> {shlex.quote(str(events_path))} 2>> {shlex.quote(str(errors_path))} &",
            "CODEX_PID=$!",
            f"/usr/bin/printf {shlex.quote(dispatch_running_json)} \"$STARTED_AT\" \"$$\" \"$CODEX_PID\" \"$CODEX_PID\" > {shlex.quote(str(receipt_temp))}",
            f"/bin/mv {shlex.quote(str(receipt_temp))} {shlex.quote(str(exit_receipt_path))}",
            'wait "$CODEX_PID"',
            "EXIT_CODE=$?",
            "trap - HUP INT TERM",
            'FINISHED_AT="$(/bin/date -u +%Y-%m-%dT%H:%M:%SZ)"',
            f"/usr/bin/printf {shlex.quote(terminal_json)} \"$STARTED_AT\" \"$FINISHED_AT\" \"$$\" \"$CODEX_PID\" \"$CODEX_PID\" \"$EXIT_CODE\" > {shlex.quote(str(receipt_temp))}",
            f"/bin/mv {shlex.quote(str(receipt_temp))} {shlex.quote(str(exit_receipt_path))}",
            # A completed Codex turn is a bounded unit, not the workflow daemon.
            # Kick the deterministic watchdog after tmux exits; the five-minute
            # scheduler remains a fallback if this best-effort kick is lost.
            f"/usr/bin/nohup /bin/zsh -lc {shlex.quote(kick_script)} >/dev/null 2>&1 &",
            'exit "$EXIT_CODE"',
            "",
        ]
    )


def _detached_new_runner_text(
    *,
    working_directory: Path,
    message_path: Path,
    events_path: Path,
    errors_path: Path,
    exit_receipt_path: Path,
    model: str,
    reasoning_effort: str,
) -> str:
    codex_command = [
        "/usr/bin/env",
        "-u", "OPENAI_API_KEY",
        "-u", "OPENAI_API_KEY_PATH",
        "-u", "CODEX_API_KEY",
        "-u", "AZURE_OPENAI_API_KEY",
        str(CAFFEINATE_PATH),
        "-i",
        "-m",
        "-s",
        str(CODEX_BIN),
        "exec",
        "--dangerously-bypass-approvals-and-sandbox",
        "--skip-git-repo-check",
        "--json",
        "--model", model,
        "--config", f'model_reasoning_effort="{reasoning_effort}"',
        "--cd", str(working_directory),
        "-",
    ]
    runtime_json = json.dumps(
        {
            "path": str(CODEX_BIN),
            "version": CODEX_RUNTIME_VERSION,
            "sha256": CODEX_RUNTIME_SHA256,
            "team_id": CODEX_RUNTIME_TEAM_ID,
        },
        separators=(",", ":"),
    )
    running_json = '{"schema_version":"pif_control_exit_v1","status":"running","started_at":"%s","runner_pid":%d,"codex_runtime":' + runtime_json + '}\n'
    terminal_json = '{"schema_version":"pif_control_exit_v1","status":"exited","started_at":"%s","finished_at":"%s","runner_pid":%d,"exit_code":%d,"codex_runtime":' + runtime_json + '}\n'
    receipt_temp = exit_receipt_path.with_suffix(exit_receipt_path.suffix + ".tmp")
    auth_command = codex_command[:9] + [str(CODEX_BIN), "login", "status"]
    kick_command = shlex.join([str(CODEX_HOME / "bin" / "codex-cron"), "run", "pif-pipeline-watchdog"])
    kick_script = f"/bin/sleep 2; {kick_command}"
    return "\n".join(
        [
            "#!/bin/zsh",
            "set +e",
            "umask 077",
            'STARTED_AT="$(/bin/date -u +%Y-%m-%dT%H:%M:%SZ)"',
            f"/usr/bin/printf {shlex.quote(running_json)} \"$STARTED_AT\" \"$$\" > {shlex.quote(str(receipt_temp))}",
            f"/bin/mv {shlex.quote(str(receipt_temp))} {shlex.quote(str(exit_receipt_path))}",
            f'AUTH_STATUS="$({shlex.join(auth_command)} 2>&1)"',
            'if [[ "$AUTH_STATUS" != "Logged in using ChatGPT" ]]; then',
            '  FINISHED_AT="$(/bin/date -u +%Y-%m-%dT%H:%M:%SZ)"',
            f"  /usr/bin/printf {shlex.quote(terminal_json)} \"$STARTED_AT\" \"$FINISHED_AT\" \"$$\" 78 > {shlex.quote(str(receipt_temp))}",
            f"  /bin/mv {shlex.quote(str(receipt_temp))} {shlex.quote(str(exit_receipt_path))}",
            "  exit 78",
            "fi",
            "unset AUTH_STATUS",
            f"{shlex.join(codex_command)} < {shlex.quote(str(message_path))} >> {shlex.quote(str(events_path))} 2>> {shlex.quote(str(errors_path))}",
            "EXIT_CODE=$?",
            'FINISHED_AT="$(/bin/date -u +%Y-%m-%dT%H:%M:%SZ)"',
            f"/usr/bin/printf {shlex.quote(terminal_json)} \"$STARTED_AT\" \"$FINISHED_AT\" \"$$\" \"$EXIT_CODE\" > {shlex.quote(str(receipt_temp))}",
            f"/bin/mv {shlex.quote(str(receipt_temp))} {shlex.quote(str(exit_receipt_path))}",
            f"/usr/bin/nohup /bin/zsh -lc {shlex.quote(kick_script)} >/dev/null 2>&1 &",
            'exit "$EXIT_CODE"',
            "",
        ]
    )


def _thread_started_from_events(path: Path) -> str | None:
    if not path.is_file():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get("type") != "thread.started":
                    continue
                candidate = event.get("thread_id")
                try:
                    return str(uuid.UUID(str(candidate)))
                except (ValueError, TypeError, AttributeError):
                    continue
    except OSError:
        return None
    return None


def _reconcile_pending_extraction_launch(state: dict[str, Any]) -> dict[str, Any] | None:
    pending = state.get("pending_extraction_launch")
    if state.get("phase") != "extraction_handoff" or not isinstance(pending, dict):
        return None
    events_path = Path(str(pending.get("events_path", ""))).expanduser()
    thread_id = _thread_started_from_events(events_path)
    if not thread_id:
        session_name = str(pending.get("session_name", ""))
        pending["tmux_alive"] = _tmux_has_session(session_name) if _valid_tmux_session_name(session_name) else False
        exit_receipt_path = Path(str(pending.get("exit_receipt_path", ""))).expanduser()
        pending["exit_receipt"] = _read_json(exit_receipt_path, {})
        return pending
    state["extraction_thread_id"] = thread_id
    state["phase"] = "extraction_watch"
    pending["thread_id"] = thread_id
    pending["registered_at"] = iso_now()
    state["last_extraction_launch"] = pending
    state["pending_extraction_launch"] = None
    return pending


def _control_turn_started(events_path: Path, thread_id: str) -> bool:
    """Require the exact resumed thread plus a turn-start event."""
    if not events_path.is_file():
        return False
    thread_started = False
    turn_started = False
    try:
        with events_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                event_type = event.get("type")
                if event_type == "thread.started" and event.get("thread_id") == thread_id:
                    thread_started = True
                elif event_type == "turn.started":
                    turn_started = True
                if thread_started and turn_started:
                    return True
    except OSError:
        return False
    return False


def _pending_control_observation(pending: dict[str, Any]) -> dict[str, Any]:
    """Observe one launch reservation without holding the pipeline state lock."""
    thread_id = str(pending.get("thread_id", ""))
    session_name = str(pending.get("session_name", ""))
    receipt = _read_json(Path(str(pending.get("exit_receipt_path", ""))).expanduser(), {})
    receipt_matches = bool(
        isinstance(receipt, dict)
        and receipt.get("schema_version") == "pif_control_exit_v1"
        and receipt.get("launch_id") == pending.get("launch_id")
    )
    process = control_process_observation(thread_id) if thread_id else {"available": False, "alive": None}
    tmux_alive = _tmux_has_session(session_name) if _valid_tmux_session_name(session_name) else False
    turn_started = _control_turn_started(
        Path(str(pending.get("events_path", ""))).expanduser(),
        thread_id,
    )
    receipt_stage = receipt.get("stage") if receipt_matches else None
    receipt_codex_pid = receipt.get("codex_pid") if receipt_matches else None
    process_pids = process.get("pids") if isinstance(process.get("pids"), list) else []
    owned_process_alive = bool(
        receipt_matches
        and receipt_stage in {"codex_dispatched", "codex_exited"}
        and type(receipt_codex_pid) is int
        and receipt_codex_pid in process_pids
    )
    accepted = bool(turn_started or owned_process_alive)
    retryable_exit_codes = frozenset({2, 64, 78, 126, 127})
    retryable_predispatch_failure = bool(
        receipt_matches
        and receipt.get("status") == "exited"
        and receipt_stage in {"auth_failed", "codex_exited"}
        and receipt.get("exit_code") in retryable_exit_codes
        and not turn_started
        and process.get("available") is True
        and process.get("alive") is False
        and not tmux_alive
    )
    return {
        "accepted": accepted,
        "turn_started": turn_started,
        "process": process,
        "tmux_alive": tmux_alive,
        "receipt_matches": receipt_matches,
        "receipt_status": receipt.get("status") if receipt_matches else None,
        "receipt_exit_code": receipt.get("exit_code") if receipt_matches else None,
        "receipt_stage": receipt_stage,
        "owned_process_alive": owned_process_alive,
        "retryable_predispatch_failure": retryable_predispatch_failure,
    }


def _promote_pending_control_launch(
    state: dict[str, Any],
    pending: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    """Commit an accepted reservation exactly once."""
    if (
        state.get("last_steering_sha256") != pending.get("prior_last_steering_sha256")
        or state.get("last_steering_evidence_sha256")
        != pending.get("prior_last_steering_evidence_sha256")
    ):
        raise ValueError("control launch commit preconditions changed")
    entry = {
        "at": iso_now(),
        "sha256": pending["steering_sha256"],
        "phase": pending["phase"],
    }
    if state.get("last_steering_sha256") != entry["sha256"]:
        state.setdefault("steering_history", []).append(entry)
        state["steering_history"] = state["steering_history"][-50:]
    receipt = {
        key: pending[key]
        for key in (
            "launch_id",
            "reserved_at",
            "phase",
            "thread_id",
            "session_name",
            "steering_sha256",
            "steering_evidence_sha256",
            "prompt_path",
            "events_path",
            "errors_path",
            "exit_receipt_path",
            "runner_path",
            "auth_policy",
            "auth_preflight",
            "codex_runtime",
        )
    }
    receipt.update(
        {
            "at": iso_now(),
            "status": "launched",
            "accepted_via": "turn_started" if observation.get("turn_started") else "exact_process",
            "tmux_alive": observation.get("tmux_alive"),
            "process": observation.get("process"),
        }
    )
    state["last_steering_sha256"] = pending["steering_sha256"]
    state["last_steering_evidence_sha256"] = pending["steering_evidence_sha256"]
    state["last_control_launch"] = receipt
    state.setdefault("control_launch_history", []).append(receipt)
    state["control_launch_history"] = state["control_launch_history"][-50:]
    state["pending_control_launch"] = None
    return receipt


def _clear_pending_control_launch(
    state: dict[str, Any],
    pending: dict[str, Any],
    reason: str,
) -> None:
    failure = {
        "at": iso_now(),
        "launch_id": pending.get("launch_id"),
        "phase": pending.get("phase"),
        "thread_id": pending.get("thread_id"),
        "session_name": pending.get("session_name"),
        "steering_sha256": pending.get("steering_sha256"),
        "steering_evidence_sha256": pending.get("steering_evidence_sha256"),
        "status": "predispatch_failed",
        "reason": reason,
    }
    state.setdefault("control_launch_failures", []).append(failure)
    state["control_launch_failures"] = state["control_launch_failures"][-50:]
    state["pending_control_launch"] = None


def _reconcile_pending_control_launch(state_path: Path) -> dict[str, Any]:
    """Reconcile a durable reservation before any new resume is attempted."""
    with locked_state(state_path) as state:
        pending = copy.deepcopy(state.get("pending_control_launch"))
    if not isinstance(pending, dict):
        return {"status": "none"}
    observation = _pending_control_observation(pending)
    if observation["accepted"]:
        with locked_state(state_path) as state:
            current = state.get("pending_control_launch")
            if not isinstance(current, dict) or current.get("launch_id") != pending.get("launch_id"):
                raise ValueError("pending control launch changed during reconciliation")
            receipt = _promote_pending_control_launch(state, current, observation)
        return {"status": "promoted", "receipt": receipt}
    if observation["retryable_predispatch_failure"]:
        with locked_state(state_path) as state:
            current = state.get("pending_control_launch")
            if isinstance(current, dict) and current.get("launch_id") == pending.get("launch_id"):
                _clear_pending_control_launch(
                    state,
                    current,
                    f"retryable_predispatch_exit_{observation.get('receipt_exit_code')}",
                )
        return {"status": "retryable_predispatch_failure"}
    return {
        "status": "ambiguous",
        "launch_id": pending.get("launch_id"),
        "receipt_status": observation.get("receipt_status"),
        "tmux_alive": observation.get("tmux_alive"),
        "process_available": observation.get("process", {}).get("available"),
    }


def cmd_launch_resume(args: argparse.Namespace) -> int:
    state_path = Path(args.state)
    text = Path(args.message_file).read_text(encoding="utf-8")
    if not text.strip():
        raise ValueError("steering message must not be empty")
    if not _valid_tmux_session_name(args.session_name):
        raise ValueError("invalid tmux session name")
    reconciliation = _reconcile_pending_control_launch(state_path)
    if reconciliation["status"] == "promoted":
        print(json.dumps(reconciliation["receipt"], indent=2))
        return 0
    if reconciliation["status"] == "ambiguous":
        raise ValueError(f"pending control launch is ambiguous: {reconciliation['launch_id']}")
    for required_path in (TMUX_PATH, CAFFEINATE_PATH, CODEX_BIN):
        if not required_path.is_file():
            raise ValueError(f"required launcher unavailable: {required_path.name}")
    runtime = codex_runtime_observation()
    if not runtime.get("verified"):
        raise ValueError("pinned Codex runtime verification failed")

    with locked_state(state_path) as state:
        state_snapshot = copy.deepcopy(state)
    thread_id = _registered_thread(state_snapshot)
    if not thread_id:
        raise ValueError("no registered thread for the current phase")
    expected_session_name = _expected_resume_session_name(state_snapshot, thread_id)
    if args.session_name != expected_session_name:
        raise ValueError(f"session name must be {expected_session_name}")

    # Every fallible observation runs outside the flock. The project filesystem
    # is represented only by hashes supplied by the deadline-bounded watchdog.
    observation = session_observation(thread_id)
    process_preflight = control_process_observation(thread_id)
    if process_preflight.get("available") is not True:
        raise ValueError("exact-thread process liveness is unavailable")
    if process_preflight.get("alive"):
        raise ValueError("registered thread already has a live turn")
    if observation.get("turn_in_progress") and not (
        getattr(args, "recover_orphaned_open_turn", False)
        and _safe_orphan_override(state_snapshot, observation, process_preflight, args.session_name)
    ):
        raise ValueError("registered thread already has a live or unproven turn")
    if _tmux_has_session(args.session_name):
        raise ValueError("tmux session already exists")
    auth = managed_chatgpt_auth_observation()
    if not auth.get("authenticated"):
        raise ValueError("managed ChatGPT authentication preflight failed")
    goal = goal_observation(thread_id, Path(args.goals_db))
    if state_snapshot["phase"] in {"evaluation_watch", "extraction_handoff"} and (
        not goal.get("available") or not goal.get("found")
    ):
        raise ValueError("authoritative evaluation goal is unavailable")

    milestone_sha = getattr(args, "observed_milestone_sha256", None) or None
    queue_sha = getattr(args, "observed_queue_sha256", None) or None
    for label, value in (("milestone", milestone_sha), ("queue", queue_sha)):
        if value is not None and not _is_sha256(value):
            raise ValueError(f"observed {label} fingerprint is invalid")
    digest = _sha256_bytes(text.strip().encode())
    evidence = {
        "phase": state_snapshot["phase"],
        "thread_id": thread_id,
        "goal_status": goal.get("status"),
        "goal_updated_at": goal.get("updated_at"),
        "milestone_sha256": milestone_sha,
        "queue_sha256": queue_sha,
        "session_recent_sha256": observation.get("recent_sha256"),
    }
    evidence_sha = _sha256_json(evidence)
    if digest == state_snapshot.get("last_steering_sha256"):
        raise ValueError("duplicate steering message refused")
    if evidence_sha == state_snapshot.get("last_steering_evidence_sha256"):
        raise ValueError("steering without new execution evidence refused")

    launch_id = str(uuid.uuid4())
    stamp = dt.datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
    prompt_dir = STATE_ROOT / "control-prompts"
    log_dir = STATE_ROOT / "control-logs"
    receipt_dir = STATE_ROOT / "control-receipts"
    runner_dir = STATE_ROOT / "control-runners"
    for directory in (prompt_dir, log_dir, receipt_dir, runner_dir, CONTROL_WORKING_DIRECTORY):
        directory.mkdir(parents=True, exist_ok=True)
    stem = f"{stamp}-{launch_id[:12]}"
    message_path = prompt_dir / f"{stem}.md"
    events_path = log_dir / f"{stem}.events.jsonl"
    errors_path = log_dir / f"{stem}.stderr.log"
    exit_receipt_path = receipt_dir / f"{stem}.json"
    runner_path = runner_dir / f"{stem}.zsh"
    message_path.write_text(text, encoding="utf-8")
    message_path.chmod(0o600)
    if _sha256_bytes(message_path.read_bytes()) != _sha256_bytes(text.encode()):
        raise ValueError("staged steering message hash mismatch")
    runner_path.write_text(
        _detached_runner_text(
            thread_id=thread_id,
            message_path=message_path,
            events_path=events_path,
            errors_path=errors_path,
            exit_receipt_path=exit_receipt_path,
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            launch_id=launch_id,
        ),
        encoding="utf-8",
    )
    runner_path.chmod(0o700)
    pending = {
        "schema_version": "pif_pending_control_launch_v1",
        "launch_id": launch_id,
        "reserved_at": iso_now(),
        "status": "reserved",
        "phase": state_snapshot["phase"],
        "thread_id": thread_id,
        "session_name": args.session_name,
        "steering_sha256": digest,
        "steering_evidence_sha256": evidence_sha,
        "prompt_path": str(message_path),
        "events_path": str(events_path),
        "errors_path": str(errors_path),
        "exit_receipt_path": str(exit_receipt_path),
        "runner_path": str(runner_path),
        "auth_policy": "managed_chatgpt_auth_api_keys_unset",
        "auth_preflight": auth,
        "codex_runtime": runtime,
        "prior_last_steering_sha256": state_snapshot.get("last_steering_sha256"),
        "prior_last_steering_evidence_sha256": state_snapshot.get("last_steering_evidence_sha256"),
    }
    with locked_state(state_path) as state:
        critical_unchanged = bool(
            state.get("phase") == state_snapshot.get("phase")
            and _registered_thread(state) == thread_id
            and state.get("last_steering_sha256") == state_snapshot.get("last_steering_sha256")
            and state.get("last_steering_evidence_sha256") == state_snapshot.get("last_steering_evidence_sha256")
            and not isinstance(state.get("pending_control_launch"), dict)
        )
        if not critical_unchanged:
            raise ValueError("control launch preconditions changed before reservation")
        state["pending_control_launch"] = pending

    launch = subprocess.run(
        [
            str(TMUX_PATH),
            "new-session",
            "-d",
            "-s", args.session_name,
            "-c", str(HOME),
            shlex.join(["/bin/zsh", str(runner_path)]),
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    time.sleep(1.0)
    pending_observation = _pending_control_observation(pending)
    if pending_observation["accepted"]:
        with locked_state(state_path) as state:
            current = state.get("pending_control_launch")
            if not isinstance(current, dict) or current.get("launch_id") != launch_id:
                raise ValueError("control launch reservation changed before commit")
            receipt = _promote_pending_control_launch(state, current, pending_observation)
        print(json.dumps(receipt, indent=2))
        return 0
    definite_tmux_failure = bool(
        launch.returncode != 0
        and not pending_observation["tmux_alive"]
        and pending_observation["process"].get("available") is True
        and pending_observation["process"].get("alive") is False
        and not pending_observation["receipt_matches"]
    )
    if pending_observation["retryable_predispatch_failure"] or definite_tmux_failure:
        reason = (
            f"retryable_predispatch_exit_{pending_observation.get('receipt_exit_code')}"
            if pending_observation["retryable_predispatch_failure"]
            else "tmux_predispatch_failure"
        )
        with locked_state(state_path) as state:
            current = state.get("pending_control_launch")
            if isinstance(current, dict) and current.get("launch_id") == launch_id:
                _clear_pending_control_launch(state, current, reason)
        raise ValueError(f"detached resume predispatch failed: {reason}")
    raise ValueError(f"detached resume launch is ambiguous: {launch_id}")


def cmd_launch_extraction(args: argparse.Namespace) -> int:
    state_path = Path(args.state)
    source_message_path = Path(args.message_file).expanduser().resolve()
    text = source_message_path.read_text(encoding="utf-8")
    if not text.strip():
        raise ValueError("extraction handoff must not be empty")
    if not _valid_tmux_session_name(args.session_name):
        raise ValueError("invalid tmux session name")
    if args.session_name != "pif-extraction-handoff":
        raise ValueError("session name must be pif-extraction-handoff")
    for required_path in (TMUX_PATH, CAFFEINATE_PATH, CODEX_BIN):
        if not required_path.is_file():
            raise ValueError(f"required launcher unavailable: {required_path.name}")
    runtime = codex_runtime_observation()
    if not runtime.get("verified"):
        raise ValueError("pinned Codex runtime verification failed")

    with locked_state(state_path) as state:
        _reconcile_pending_extraction_launch(state)
        if state.get("phase") == "extraction_watch" and state.get("extraction_thread_id"):
            raise ValueError("extraction thread is already registered")
        if state.get("phase") != "extraction_handoff" or not state.get("evaluation_receipt"):
            raise ValueError("verified evaluation handoff is required")
        auth = managed_chatgpt_auth_observation()
        if not auth.get("authenticated"):
            raise ValueError("managed ChatGPT authentication preflight failed")
        handoff = state.get("handoff") or {}
        if Path(str(handoff.get("path", ""))).expanduser().resolve() != source_message_path:
            raise ValueError("message file does not match the registered extraction handoff")
        source_sha = _sha256_bytes(source_message_path.read_bytes())
        if source_sha != handoff.get("sha256"):
            raise ValueError("extraction handoff hash mismatch")

        pending = state.get("pending_extraction_launch")
        if isinstance(pending, dict):
            pending_session = str(pending.get("session_name", ""))
            pending_receipt = _read_json(Path(str(pending.get("exit_receipt_path", ""))).expanduser(), {})
            runner_alive = _pid_alive(pending_receipt.get("runner_pid"))
            if (_valid_tmux_session_name(pending_session) and _tmux_has_session(pending_session)) or runner_alive:
                raise ValueError("an extraction launch is already pending")
            state.setdefault("extraction_launch_history", []).append({**pending, "reconciled_as": "failed_before_thread_started"})
            state["pending_extraction_launch"] = None

        if _tmux_has_session(args.session_name):
            raise ValueError("tmux session already exists")
        stamp = dt.datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
        prompt_dir = STATE_ROOT / "control-prompts"
        log_dir = STATE_ROOT / "control-logs"
        receipt_dir = STATE_ROOT / "control-receipts"
        runner_dir = STATE_ROOT / "control-runners"
        for directory in (prompt_dir, log_dir, receipt_dir, runner_dir):
            directory.mkdir(parents=True, exist_ok=True)
        stem = f"{stamp}-extraction-{source_sha[:12]}"
        message_path = prompt_dir / f"{stem}.md"
        events_path = log_dir / f"{stem}.events.jsonl"
        errors_path = log_dir / f"{stem}.stderr.log"
        exit_receipt_path = receipt_dir / f"{stem}.json"
        runner_path = runner_dir / f"{stem}.zsh"
        message_path.write_text(text, encoding="utf-8")
        message_path.chmod(0o600)
        if _sha256_bytes(message_path.read_bytes()) != source_sha:
            raise ValueError("staged extraction handoff hash mismatch")
        runner_path.write_text(
            _detached_new_runner_text(
                working_directory=PROJECT_ROOT,
                message_path=message_path,
                events_path=events_path,
                errors_path=errors_path,
                exit_receipt_path=exit_receipt_path,
                model=args.model,
                reasoning_effort=args.reasoning_effort,
            ),
            encoding="utf-8",
        )
        runner_path.chmod(0o700)
        pending = {
            "at": iso_now(),
            "phase": state["phase"],
            "session_name": args.session_name,
            "handoff_sha256": source_sha,
            "prompt_path": str(message_path),
            "events_path": str(events_path),
            "errors_path": str(errors_path),
            "exit_receipt_path": str(exit_receipt_path),
            "runner_path": str(runner_path),
            "auth_policy": "managed_chatgpt_auth_api_keys_unset",
            "auth_preflight": auth,
            "codex_runtime": runtime,
        }
        state["pending_extraction_launch"] = pending
        launch = subprocess.run(
            [
                str(TMUX_PATH),
                "new-session",
                "-d",
                "-s", args.session_name,
                "-c", str(PROJECT_ROOT),
                shlex.join(["/bin/zsh", str(runner_path)]),
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if launch.returncode != 0:
            state["pending_extraction_launch"] = None
            raise ValueError("detached extraction launch failed")

        deadline = time.monotonic() + 10
        thread_id = None
        while time.monotonic() < deadline and thread_id is None:
            thread_id = _thread_started_from_events(events_path)
            if thread_id is None:
                time.sleep(0.25)
        pending["tmux_alive"] = _tmux_has_session(args.session_name)
        pending["exit_receipt"] = _read_json(exit_receipt_path, {})
        if thread_id:
            state["extraction_thread_id"] = thread_id
            state["phase"] = "extraction_watch"
            pending["thread_id"] = thread_id
            pending["registered_at"] = iso_now()
            state["last_extraction_launch"] = pending
            state["pending_extraction_launch"] = None
            state.setdefault("extraction_launch_history", []).append(pending)
            state["extraction_launch_history"] = state["extraction_launch_history"][-50:]
            result = {**pending, "status": "registered"}
        elif pending["tmux_alive"] or pending["exit_receipt"].get("status") == "running":
            result = {**pending, "status": "running_unconfirmed"}
        else:
            state["pending_extraction_launch"] = None
            raise ValueError("detached extraction process exited before thread registration")
    print(json.dumps(result, indent=2))
    return 0


def cmd_accept_evaluation(args: argparse.Namespace) -> int:
    verified = verify_evaluation_receipt(Path(args.receipt), Path(args.evaluation_root))
    with locked_state(Path(args.state)) as state:
        if state["phase"] != "evaluation_watch":
            raise ValueError("evaluation can only be accepted from evaluation_watch")
        goal = goal_observation(state["evaluation_thread_id"], Path(args.goals_db))
        if not goal.get("available") or not goal.get("found") or goal.get("status") != "complete":
            raise ValueError("authoritative evaluation goal is not complete")
        state["evaluation_receipt"] = verified
        state["phase"] = "extraction_handoff"
    print(json.dumps(verified, indent=2))
    return 0


def cmd_render_handoff(args: argparse.Namespace) -> int:
    state = load_state(Path(args.state))
    if state["phase"] != "extraction_handoff" or not state.get("evaluation_receipt"):
        raise ValueError("evaluation has not been accepted")
    prompt = render_handoff(state, queue_snapshot(Path(args.db)))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(prompt, encoding="utf-8")
    with locked_state(Path(args.state)) as locked:
        locked["handoff"] = {"path": str(output.resolve()), "sha256": _sha256_bytes(prompt.encode()), "rendered_at": iso_now()}
    print(json.dumps({"path": str(output.resolve()), "sha256": _sha256_bytes(prompt.encode())}, indent=2))
    return 0


def cmd_set_extraction_thread(args: argparse.Namespace) -> int:
    with locked_state(Path(args.state)) as state:
        if state["phase"] != "extraction_handoff" or not state.get("handoff"):
            raise ValueError("handoff must be rendered before thread registration")
        state["extraction_thread_id"] = args.thread_id
        state["phase"] = "extraction_watch"
    print(json.dumps({"thread_id": args.thread_id, "phase": "extraction_watch"}, indent=2))
    return 0


def cmd_completion_check(args: argparse.Namespace) -> int:
    state_path = Path(args.state)
    result = completion_audit(Path(args.db), Path(args.manifest) if args.manifest else None)
    with locked_state(state_path) as state:
        if state["phase"] not in {"extraction_watch", "completion_confirmation"}:
            raise ValueError("completion check is only valid during extraction supervision")
        prior = state.get("completion_checks", [])
        if result["pass"]:
            if state["phase"] == "extraction_watch":
                state["phase"] = "completion_confirmation"
                state["completion_checks"] = [result]
            elif prior and prior[-1].get("pass"):
                previous_at = dt.datetime.fromisoformat(str(prior[-1]["checked_at"]).replace("Z", "+00:00"))
                current_at = dt.datetime.fromisoformat(str(result["checked_at"]).replace("Z", "+00:00"))
                if (current_at - previous_at).total_seconds() < 3.5 * 3600:
                    raise ValueError("second completion check must occur in a later four-hour scheduler cycle")
                state["completion_checks"].append(result)
            else:
                state["completion_checks"] = [result]
        else:
            state["phase"] = "extraction_watch"
            state["completion_checks"] = []
    print(json.dumps(result, indent=2))
    return 0 if result["pass"] else 2


def cmd_finalize(args: argparse.Namespace) -> int:
    state_path = Path(args.state)
    with locked_state(state_path) as state:
        checks = state.get("completion_checks", [])
        if state["phase"] != "completion_confirmation" or len(checks) < 2 or not all(item.get("pass") for item in checks[-2:]):
            raise ValueError("two passing completion checks are required")
        if not state.get("notification_sent"):
            delivery = explicit_telegram(
                "Podcast extraction is complete. The evaluation gates passed, every in-scope item is validated or terminally quarantined, and the babysitter is stopping."
            )
            state["notification_receipt"] = {**delivery, "at": iso_now()}
            if not delivery["ok"]:
                print(json.dumps(delivery, indent=2))
                return 3
            state["notification_sent"] = True
        proc = subprocess.run([str(CODEX_HOME / "bin" / "codex-cron"), "disable", JOB_NAME], capture_output=True, text=True, check=False)
        job = _read_json(CODEX_HOME / "memories" / "automation" / "jobs" / f"{JOB_NAME}.json", {})
        state["disabled_verified"] = proc.returncode == 0 and job.get("enabled") is False
        if not state["disabled_verified"]:
            print(json.dumps({"ok": False, "status": "disable_failed", "exit_code": proc.returncode}, indent=2))
            return 4
        state["phase"] = "complete"
    print(json.dumps({"ok": True, "phase": "complete", "disabled": True}, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stateful supervisor for the PIF evaluation-to-extraction workflow.")
    parser.add_argument("--state", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--goals-db", default=str(DEFAULT_GOALS_DB_PATH))
    parser.add_argument("--evaluation-root", default=str(DEFAULT_EVALUATION_ROOT))
    sub = parser.add_subparsers(dest="command", required=True)
    init = sub.add_parser("init")
    init.add_argument("--evaluation-thread", default=DEFAULT_EVALUATION_THREAD)
    init.add_argument("--force", action="store_true")
    init.set_defaults(func=cmd_init)
    status = sub.add_parser("status")
    status.set_defaults(func=cmd_status)
    steering = sub.add_parser("record-steering")
    steering.add_argument("--message-file", required=True)
    steering.set_defaults(func=cmd_record_steering)
    launch = sub.add_parser("launch-resume")
    launch.add_argument("--message-file", required=True)
    launch.add_argument("--session-name", required=True)
    launch.add_argument("--model", default="gpt-5.5")
    launch.add_argument("--reasoning-effort", default="xhigh")
    launch.add_argument("--recover-orphaned-open-turn", action="store_true")
    launch.add_argument("--observed-milestone-sha256")
    launch.add_argument("--observed-queue-sha256")
    launch.set_defaults(func=cmd_launch_resume)
    extraction_launch = sub.add_parser("launch-extraction")
    extraction_launch.add_argument("--message-file", required=True)
    extraction_launch.add_argument("--session-name", default="pif-extraction-handoff")
    extraction_launch.add_argument("--model", default="gpt-5.5")
    extraction_launch.add_argument("--reasoning-effort", default="xhigh")
    extraction_launch.set_defaults(func=cmd_launch_extraction)
    accept = sub.add_parser("accept-evaluation")
    accept.add_argument("--receipt", required=True)
    accept.set_defaults(func=cmd_accept_evaluation)
    handoff = sub.add_parser("render-handoff")
    handoff.add_argument("--output", required=True)
    handoff.set_defaults(func=cmd_render_handoff)
    thread = sub.add_parser("set-extraction-thread")
    thread.add_argument("thread_id")
    thread.set_defaults(func=cmd_set_extraction_thread)
    check = sub.add_parser("completion-check")
    check.add_argument("--manifest")
    check.set_defaults(func=cmd_completion_check)
    finalize = sub.add_parser("finalize")
    finalize.set_defaults(func=cmd_finalize)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        return int(args.func(args))
    except (ValueError, OSError, sqlite3.Error, subprocess.SubprocessError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
