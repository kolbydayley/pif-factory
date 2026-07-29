from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterator

from .pipeline_babysitter import (
    CODEX_HOME,
    DEFAULT_DB_PATH,
    DEFAULT_EVALUATION_ROOT,
    DEFAULT_GOALS_DB_PATH,
    DEFAULT_STATE_PATH,
    PROJECT_ROOT,
    STATE_ROOT,
    _expected_resume_session_name,
    _read_json,
    _registered_thread,
    _write_json_atomic,
    control_process_observation,
    evaluation_milestone_snapshot,
    goal_observation,
    load_state,
    queue_snapshot,
    session_observation,
)


SCHEMA_VERSION = "pif_pipeline_watchdog_v1"
JOB_NAME = "pif-pipeline-watchdog"
WATCHDOG_ROOT = CODEX_HOME / "memories" / "automation" / JOB_NAME
DEFAULT_WATCHDOG_STATE = WATCHDOG_ROOT / "state.json"
DEFAULT_LOG_PATH = WATCHDOG_ROOT / "events.jsonl"
DEFAULT_PYTHON = Path("/opt/homebrew/bin/python3")
DEFAULT_DEBOUNCE_CHECKS = 2
DEFAULT_LAUNCH_COOLDOWN_SECONDS = 5 * 60
# Managed semantic turns have a declared 20-minute deadline. A 30-minute
# no-session/no-milestone window leaves recovery overhead while avoiding the old
# hour-long gap before a genuinely wedged outer Codex process is recycled.
DEFAULT_LIVE_STALL_SECONDS = 30 * 60
DEFAULT_STARTUP_STALL_SECONDS = 10 * 60
DEFAULT_POST_COMPLETION_HANG_SECONDS = 5 * 60
DEFAULT_OBSERVATION_TIMEOUT_SECONDS = 30
DEFAULT_LAUNCH_TIMEOUT_SECONDS = 45
DEFAULT_PROJECT_PROBE_TIMEOUT_SECONDS = 10
TMUX_PATH = Path("/opt/homebrew/bin/tmux")


def iso_now() -> str:
    return dt.datetime.now().astimezone().replace(microsecond=0).isoformat()


def parse_iso(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def seconds_since(value: Any, now: dt.datetime) -> float | None:
    parsed = parse_iso(value)
    return None if parsed is None else max(0.0, (now - parsed.astimezone(now.tzinfo)).total_seconds())


def default_state() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "created_at": iso_now(),
        "updated_at": iso_now(),
        "last_check_at": None,
        "last_progress_at": None,
        "last_session_sha256": None,
        "last_milestone_sha256": None,
        "consecutive_inactive_checks": 0,
        "last_launch_at": None,
        "last_launch_receipt": None,
        "last_action": None,
        "recycle_count": 0,
        "launch_count": 0,
    }


def load_watchdog_state(path: Path = DEFAULT_WATCHDOG_STATE) -> dict[str, Any]:
    state = _read_json(path)
    if not isinstance(state, dict):
        return default_state()
    if state.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported pipeline watchdog state schema")
    return state


def save_watchdog_state(state: dict[str, Any], path: Path = DEFAULT_WATCHDOG_STATE) -> None:
    state["updated_at"] = iso_now()
    _write_json_atomic(path, state)


def locked_watchdog_state(path: Path = DEFAULT_WATCHDOG_STATE) -> Iterator[dict[str, Any]]:
    class _Lock:
        def __enter__(self) -> dict[str, Any]:
            self.lock_path = path.with_suffix(".lock")
            self.lock_path.parent.mkdir(parents=True, exist_ok=True)
            self.handle = self.lock_path.open("a+", encoding="utf-8")
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.state = load_watchdog_state(path)
            return self.state

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            if exc_type is None:
                save_watchdog_state(self.state, path)
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()

    return _Lock()  # type: ignore[return-value]


def append_event(event: dict[str, Any], path: Path = DEFAULT_LOG_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=False) + "\n")


def supervision_archive() -> Path | None:
    """Return the home-scoped runtime archive when running packaged."""
    raw = os.environ.get("PIF_WATCHDOG_ARCHIVE")
    if not raw:
        return None
    candidate = Path(raw).expanduser()
    return candidate if candidate.is_file() else None


def _run_bounded_command(
    command: list[str],
    *,
    timeout_seconds: int,
    cwd: Path = Path.home(),
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Run a child without ever waiting indefinitely after its deadline.

    ``subprocess.run(timeout=...)`` kills and then performs an unbounded
    ``communicate()``.  That can still wedge the parent when macOS has the child
    blocked in a non-cancellable FileProvider ``open``.  This helper abandons
    the killed child after a bounded grace period, so the scheduler-facing
    watchdog always returns.
    """
    try:
        proc = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(cwd),
            env=env,
            start_new_session=True,
        )
    except OSError:
        return {
            "exit_code": 127,
            "stdout": "",
            "stderr": "",
            "timed_out": False,
            "error_class": "process_start_os_error",
        }
    try:
        stdout, stderr = proc.communicate(timeout=max(1, timeout_seconds))
        return {
            "exit_code": int(proc.returncode or 0),
            "stdout": stdout,
            "stderr": stderr,
            "timed_out": False,
            "error_class": None,
        }
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            try:
                proc.kill()
            except OSError:
                pass
        try:
            stdout, stderr = proc.communicate(timeout=0.5)
        except subprocess.TimeoutExpired:
            stdout = ""
            stderr = ""
            for stream in (proc.stdout, proc.stderr):
                try:
                    if stream is not None:
                        stream.close()
                except OSError:
                    pass
        return {
            "exit_code": 124,
            "stdout": stdout,
            "stderr": stderr,
            "timed_out": True,
            "error_class": "process_timeout",
        }


def _archive_project_probe(
    command: str,
    path: Path,
    *,
    timeout_seconds: int = DEFAULT_PROJECT_PROBE_TIMEOUT_SECONDS,
) -> dict[str, Any] | None:
    """Read project-backed state in a disposable, deadline-bounded child."""
    archive = supervision_archive()
    if archive is None:
        return None
    result = _run_bounded_command(
        [str(archive), command, str(path.expanduser().absolute())],
        timeout_seconds=timeout_seconds,
    )
    if result["timed_out"]:
        return {
            "available": False,
            "snapshot_sha256": None,
            "error_class": f"{command.replace('-', '_')}_timeout",
        }
    if result["exit_code"] != 0:
        return {
            "available": False,
            "snapshot_sha256": None,
            "error_class": f"{command.replace('-', '_')}_failed",
        }
    try:
        payload = json.loads(result["stdout"])
    except (TypeError, json.JSONDecodeError):
        payload = None
    if not isinstance(payload, dict):
        return {
            "available": False,
            "snapshot_sha256": None,
            "error_class": f"{command.replace('-', '_')}_invalid_json",
        }
    return payload


def observe(
    pipeline_state_path: Path = DEFAULT_STATE_PATH,
    goals_db: Path = DEFAULT_GOALS_DB_PATH,
    evaluation_root: Path = DEFAULT_EVALUATION_ROOT,
    db_path: Path = DEFAULT_DB_PATH,
) -> dict[str, Any]:
    pipeline = load_state(pipeline_state_path)
    phase = str(pipeline["phase"])
    thread_id = _registered_thread(pipeline)
    thread = session_observation(thread_id) if thread_id else {"found": False, "turn_in_progress": False}
    goal = goal_observation(thread_id, goals_db) if thread_id else {"available": True, "found": False}
    if phase in {"evaluation_watch", "extraction_handoff"}:
        milestones = _archive_project_probe("milestone-snapshot", evaluation_root)
        if milestones is None:
            milestones = evaluation_milestone_snapshot(evaluation_root)
    else:
        milestones = {"available": False, "snapshot_sha256": None, "newest_path": None}
    queue_sha = None
    if phase in {"extraction_watch", "completion_confirmation"}:
        queue = _archive_project_probe("queue-snapshot", db_path)
        if queue is None:
            queue = queue_snapshot(db_path)
        queue_sha = queue.get("snapshot_sha256")
    launch = pipeline.get("last_control_launch") or pipeline.get("last_extraction_launch") or {}
    exit_receipt = _read_json(Path(str(launch.get("exit_receipt_path", ""))).expanduser(), {})
    return {
        "phase": phase,
        "thread_id": thread_id,
        "thread": thread,
        "goal": goal,
        "milestones": milestones,
        "queue_sha256": queue_sha,
        "workflow_complete": phase == "complete",
        "session_name": _expected_resume_session_name(pipeline, thread_id) if thread_id else None,
        "control_launch": {
            "launch_id": launch.get("launch_id"),
            "thread_id": launch.get("thread_id"),
            "session_name": launch.get("session_name"),
            "exit_receipt_path": launch.get("exit_receipt_path"),
        },
        "prior_exit_receipt": {
            "schema_version": exit_receipt.get("schema_version"),
            "launch_id": exit_receipt.get("launch_id"),
            "status": exit_receipt.get("status"),
            "stage": exit_receipt.get("stage"),
            "started_at": exit_receipt.get("started_at"),
            "exit_code": exit_receipt.get("exit_code"),
            "finished_at": exit_receipt.get("finished_at"),
            "runner_pid": exit_receipt.get("runner_pid"),
            "codex_pid": exit_receipt.get("codex_pid"),
            "process_group_id": exit_receipt.get("process_group_id"),
        },
    }


def observe_with_deadline(
    pipeline_state_path: Path,
    goals_db: Path,
    evaluation_root: Path,
    db_path: Path,
    timeout_seconds: int = DEFAULT_OBSERVATION_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Bound local observation so a slow FileProvider read cannot wedge recovery."""
    if timeout_seconds <= 0 or not hasattr(signal, "SIGALRM"):
        return observe(pipeline_state_path, goals_db, evaluation_root, db_path)

    def timed_out(_signum: int, _frame: Any) -> None:
        raise TimeoutError("pipeline watchdog observation timed out")

    prior_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, timed_out)
    signal.setitimer(signal.ITIMER_REAL, float(timeout_seconds))
    try:
        return observe(pipeline_state_path, goals_db, evaluation_root, db_path)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, prior_handler)


def progress_changed(state: dict[str, Any], observation: dict[str, Any]) -> bool:
    session_sha = observation["thread"].get("recent_sha256")
    milestone_sha = observation["milestones"].get("snapshot_sha256")
    queue_sha = observation.get("queue_sha256")
    previous_queue_sha = state.get("last_queue_sha256")
    return bool(
        (session_sha and session_sha != state.get("last_session_sha256"))
        or (milestone_sha and milestone_sha != state.get("last_milestone_sha256"))
        or (queue_sha and queue_sha != previous_queue_sha)
    )


def update_progress_state(state: dict[str, Any], observation: dict[str, Any], now: dt.datetime) -> bool:
    changed = progress_changed(state, observation)
    session_sha = observation["thread"].get("recent_sha256")
    milestone_sha = observation["milestones"].get("snapshot_sha256")
    queue_sha = observation.get("queue_sha256")
    # A timed-out disposable probe means "unknown", not that the prior
    # checkpoint disappeared. Preserve the last known fingerprints so a later
    # successful probe does not create false progress.
    if session_sha is not None:
        state["last_session_sha256"] = session_sha
    if milestone_sha is not None:
        state["last_milestone_sha256"] = milestone_sha
    if queue_sha is not None:
        state["last_queue_sha256"] = queue_sha
    if changed or state.get("last_progress_at") is None:
        state["last_progress_at"] = now.isoformat()
    return changed


def recovery_message(observation: dict[str, Any], recovery_id: str) -> str:
    milestone = observation["milestones"]
    prior_exit = observation["prior_exit_receipt"]
    return "\n".join(
        [
            f"Deterministic pipeline watchdog recovery {recovery_id}.",
            "The exact registered thread has no live Codex process, while the overall pipeline phase is incomplete.",
            "Resume the existing objective autonomously. Do not merely summarize status and do not wait for another user nudge.",
            f"Project root: {PROJECT_ROOT}.",
            "The controller starts from a stable home-scoped cwd to avoid initialization stalls. Read the project AGENTS.md and use the project root explicitly for every repository command or edit.",
            f"Current phase: {observation['phase']}.",
            f"Historical goal-database metadata status: {observation['goal'].get('status', 'unavailable')}.",
            "That goal-database value can lag resumed execution. The deterministic pipeline phase and verified immutable artifacts govern continuation; a historical blocked value is not permission to stop.",
            f"Newest immutable milestone: {milestone.get('newest_path') or 'none'}.",
            f"Milestone fingerprint: {milestone.get('snapshot_sha256') or 'unavailable'}.",
            f"Prior detached exit: status={prior_exit.get('status')}, code={prior_exit.get('exit_code')}, finished_at={prior_exit.get('finished_at')}.",
            "Inspect the newest immutable terminal, failure, sidecar, and prior final response, then perform the next concrete action required by the goal.",
            "Preserve predecessor attempts; create a new immutable version for a new semantic strategy. Never rerun a terminal attempt in place.",
            "Keep production unchanged until all evaluation gates pass. Do not advance untouched holdout early.",
            "All semantic work must stay on official persistent Codex app-server transport with managed ChatGPT auth and complete usage/cache telemetry.",
            "Never use API-key billing, raw session-token replay, embeddings, or semantic regex/keyword pruning.",
            "If a previous semantic process ended without complete accounting, preserve it as interrupted/unknown-usage evidence before continuing.",
            "A quality failure, design receipt, or prepared next experiment is a checkpoint, not a terminal outcome. Execute the next safe experiment in this turn instead of stopping after preparing it.",
            "End only after the deterministic pipeline phase is complete, or after recording a truly external authorization/input blocker for which no safe local diagnostic or experiment remains.",
        ]
    ) + "\n"


def tmux_session_alive(name: str | None) -> bool:
    if not name:
        return False
    proc = _run_bounded_command(
        [str(TMUX_PATH), "has-session", "-t", f"={name}"],
        timeout_seconds=5,
    )
    return proc["exit_code"] == 0


def kill_tmux_session(name: str | None) -> bool:
    if not name or not tmux_session_alive(name):
        return False
    proc = _run_bounded_command(
        [str(TMUX_PATH), "kill-session", "-t", f"={name}"],
        timeout_seconds=10,
    )
    return proc["exit_code"] == 0


def _process_start_signature(pid: int) -> str | None:
    """Return a short-lived kernel identity guard for one PID.

    PID numbers can be reused.  A recycle therefore snapshots macOS's process
    start field and re-reads it immediately before signalling; a changed or
    unreadable identity fails closed.
    """
    result = _run_bounded_command(
        ["/bin/ps", "-p", str(pid), "-o", "lstart="],
        timeout_seconds=3,
    )
    value = result.get("stdout", "").strip()
    if result.get("exit_code") != 0 or not value:
        return None
    return hashlib.sha256(f"{pid}:{value}".encode()).hexdigest()


def _process_group_alive(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _matching_owned_receipt(observation: dict[str, Any]) -> dict[str, Any]:
    """Authorize a recycle only for the exact receipt-bound detached owner."""
    launch = observation.get("control_launch") or {}
    receipt = observation.get("prior_exit_receipt") or {}
    process = observation.get("thread", {}).get("control_process") or {}
    thread_id = observation.get("thread_id")
    if (
        not isinstance(launch, dict)
        or not isinstance(receipt, dict)
        or launch.get("thread_id") != thread_id
        or launch.get("session_name") != observation.get("session_name")
        or not launch.get("launch_id")
        or receipt.get("launch_id") != launch.get("launch_id")
        or receipt.get("schema_version") != "pif_control_exit_v1"
        or receipt.get("status") != "running"
        or receipt.get("stage") != "codex_dispatched"
        or process.get("available") is not True
        or process.get("alive") is not True
    ):
        return {"ok": False, "error_class": "recycle_owner_unverified"}
    codex_pid = receipt.get("codex_pid")
    runner_pid = receipt.get("runner_pid")
    recorded_group = receipt.get("process_group_id")
    if type(codex_pid) is not int or codex_pid <= 1:
        return {"ok": False, "error_class": "recycle_owner_pid_invalid"}
    if codex_pid not in (process.get("pids") or []):
        return {"ok": False, "error_class": "recycle_owner_pid_mismatch"}
    if codex_pid not in (process.get("runtime_verified_pids") or []):
        return {"ok": False, "error_class": "recycle_owner_runtime_mismatch"}
    # New launch receipts record a dedicated child process group.  The one
    # pre-fix legacy launch can be recovered only when its group still equals
    # the dead, receipt-recorded runner PID.
    expected_group = recorded_group if type(recorded_group) is int else runner_pid
    if type(expected_group) is not int or expected_group <= 1:
        return {"ok": False, "error_class": "recycle_owner_group_invalid"}
    try:
        actual_group = os.getpgid(codex_pid)
    except (ProcessLookupError, PermissionError, OSError):
        return {"ok": False, "error_class": "recycle_owner_group_unavailable"}
    if actual_group != expected_group or actual_group == os.getpgrp():
        return {"ok": False, "error_class": "recycle_owner_group_mismatch"}
    signature = _process_start_signature(codex_pid)
    if signature is None:
        return {"ok": False, "error_class": "recycle_owner_identity_unavailable"}
    receipt_path = Path(str(launch.get("exit_receipt_path", ""))).expanduser()
    try:
        resolved_receipt = receipt_path.resolve()
        receipt_root = (STATE_ROOT / "control-receipts").resolve()
    except OSError:
        return {"ok": False, "error_class": "recycle_receipt_path_unavailable"}
    if not resolved_receipt.is_relative_to(receipt_root):
        return {"ok": False, "error_class": "recycle_receipt_path_untrusted"}
    return {
        "ok": True,
        "launch_id": launch["launch_id"],
        "receipt_path": resolved_receipt,
        "codex_pid": codex_pid,
        "process_group_id": actual_group,
        "process_start_signature": signature,
    }


def _receipt_identity_matches(ownership: dict[str, Any]) -> bool:
    latest = _read_json(ownership["receipt_path"], {})
    return bool(
        isinstance(latest, dict)
        and latest.get("schema_version") == "pif_control_exit_v1"
        and latest.get("launch_id") == ownership.get("launch_id")
        and latest.get("status") == "running"
        and latest.get("stage") == "codex_dispatched"
        and latest.get("codex_pid") == ownership.get("codex_pid")
    )


def _terminalize_recycled_receipt(
    ownership: dict[str, Any],
    *,
    reason: str,
    forced: bool,
) -> bool:
    path = ownership["receipt_path"]
    latest = _read_json(path, {})
    if not isinstance(latest, dict) or latest.get("launch_id") != ownership.get("launch_id"):
        return False
    if latest.get("status") == "exited":
        return True
    if not _receipt_identity_matches(ownership):
        return False
    latest.update(
        {
            "status": "exited",
            "stage": "watchdog_recycled",
            "finished_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "exit_code": 137 if forced else 143,
            "watchdog_recycle_reason": reason,
        }
    )
    _write_json_atomic(path, latest)
    return True


def recycle_control_owner(observation: dict[str, Any], *, reason: str) -> dict[str, Any]:
    """Terminate one verified detached Codex process group, even without tmux."""
    ownership = _matching_owned_receipt(observation)
    if not ownership.get("ok"):
        return ownership
    pid = int(ownership["codex_pid"])
    process_group_id = int(ownership["process_group_id"])
    # Close the PID-reuse race immediately before the first signal.
    if (
        not _receipt_identity_matches(ownership)
        or _process_start_signature(pid) != ownership.get("process_start_signature")
    ):
        return {"ok": False, "error_class": "recycle_owner_identity_changed"}
    try:
        os.killpg(process_group_id, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except (PermissionError, OSError):
        return {"ok": False, "error_class": "recycle_sigterm_failed"}

    deadline = time.monotonic() + 3.0
    while _process_group_alive(process_group_id) and time.monotonic() < deadline:
        time.sleep(0.1)
    forced = False
    if _process_group_alive(process_group_id):
        # Re-read both the launch receipt and exact-thread process list before
        # escalation.  A different/manual process group is never signalled.
        current = control_process_observation(str(observation.get("thread_id", "")))
        owned_group_pids: list[int] = []
        for candidate in current.get("runtime_verified_pids") or []:
            try:
                if os.getpgid(int(candidate)) == process_group_id:
                    owned_group_pids.append(int(candidate))
            except (ProcessLookupError, PermissionError, OSError):
                continue
        if not owned_group_pids or not _receipt_identity_matches(ownership):
            return {"ok": False, "error_class": "recycle_sigkill_revalidation_failed"}
        try:
            os.killpg(process_group_id, signal.SIGKILL)
            forced = True
        except ProcessLookupError:
            pass
        except (PermissionError, OSError):
            return {"ok": False, "error_class": "recycle_sigkill_failed"}
        deadline = time.monotonic() + 2.0
        while _process_group_alive(process_group_id) and time.monotonic() < deadline:
            time.sleep(0.1)
    if _process_group_alive(process_group_id):
        return {"ok": False, "error_class": "recycle_group_survived"}

    # tmux is cleanup only; it is no longer the ownership or success boundary.
    kill_tmux_session(observation.get("session_name"))
    if not _terminalize_recycled_receipt(ownership, reason=reason, forced=forced):
        return {"ok": False, "error_class": "recycle_receipt_terminalize_failed"}
    return {
        "ok": True,
        "method": "receipt_owned_process_group",
        "process_group_id": process_group_id,
        "forced": forced,
    }


def launch_resume(
    observation: dict[str, Any],
    message_path: Path,
    pipeline_state_path: Path = DEFAULT_STATE_PATH,
    python_bin: Path = DEFAULT_PYTHON,
) -> dict[str, Any]:
    arguments = [
        "--state",
        str(pipeline_state_path),
        "launch-resume",
        "--message-file",
        str(message_path),
        "--session-name",
        str(observation["session_name"]),
        "--recover-orphaned-open-turn",
    ]
    milestone_sha = observation.get("milestones", {}).get("snapshot_sha256")
    queue_sha = observation.get("queue_sha256")
    if milestone_sha:
        arguments.extend(["--observed-milestone-sha256", str(milestone_sha)])
    if queue_sha:
        arguments.extend(["--observed-queue-sha256", str(queue_sha)])
    archive = supervision_archive()
    if archive is not None:
        command = [str(archive), "babysitter", *arguments]
        child_env = dict(os.environ)
    else:
        command = [
            str(python_bin),
            "-m",
            "research_factory.pipeline_babysitter",
            *arguments,
        ]
        python_path_parts = [str(PROJECT_ROOT)]
        if os.environ.get("PYTHONPATH"):
            python_path_parts.append(str(os.environ["PYTHONPATH"]))
        child_env = {**os.environ, "PYTHONPATH": os.pathsep.join(python_path_parts)}
    proc = _run_bounded_command(
        command,
        timeout_seconds=DEFAULT_LAUNCH_TIMEOUT_SECONDS,
        env=child_env,
    )
    if proc["timed_out"]:
        return {
            "ok": False,
            "exit_code": 124,
            "receipt": {},
            "error_class": "launch_resume_timeout",
        }
    if proc["error_class"] == "process_start_os_error":
        return {
            "ok": False,
            "exit_code": 127,
            "receipt": {},
            "error_class": "launch_resume_os_error",
        }
    try:
        payload = json.loads(proc["stdout"]) if proc["stdout"].strip() else {}
    except json.JSONDecodeError:
        payload = {}
    return {
        "ok": proc["exit_code"] == 0,
        "exit_code": proc["exit_code"],
        "receipt": payload if isinstance(payload, dict) else {},
        "error_class": None if proc["exit_code"] == 0 else "launch_resume_failed",
    }


def disable_self() -> dict[str, Any]:
    proc = _run_bounded_command(
        [str(CODEX_HOME / "bin" / "codex-cron"), "disable", JOB_NAME],
        timeout_seconds=30,
    )
    return {"ok": proc["exit_code"] == 0, "exit_code": proc["exit_code"]}


def run_cycle(
    *,
    watchdog_state_path: Path = DEFAULT_WATCHDOG_STATE,
    pipeline_state_path: Path = DEFAULT_STATE_PATH,
    goals_db: Path = DEFAULT_GOALS_DB_PATH,
    evaluation_root: Path = DEFAULT_EVALUATION_ROOT,
    db_path: Path = DEFAULT_DB_PATH,
    debounce_checks: int = DEFAULT_DEBOUNCE_CHECKS,
    launch_cooldown_seconds: int = DEFAULT_LAUNCH_COOLDOWN_SECONDS,
    live_stall_seconds: int = DEFAULT_LIVE_STALL_SECONDS,
    startup_stall_seconds: int = DEFAULT_STARTUP_STALL_SECONDS,
    post_completion_hang_seconds: int = DEFAULT_POST_COMPLETION_HANG_SECONDS,
) -> dict[str, Any]:
    now = dt.datetime.now().astimezone().replace(microsecond=0)
    observation = observe_with_deadline(
        pipeline_state_path,
        goals_db,
        evaluation_root,
        db_path,
    )
    with locked_watchdog_state(watchdog_state_path) as state:
        changed = update_progress_state(state, observation, now)
        state["last_check_at"] = now.isoformat()
        thread = observation["thread"]
        process_observation = thread.get("control_process") or {}
        process_available = process_observation.get("available") is True
        process_alive = process_observation.get("alive") is True
        raw_turn_open = thread.get("session_turn_in_progress") is True
        turn_claimed_live = thread.get("turn_in_progress") is True
        proven_orphan = thread.get("orphaned_open_turn") is True

        if observation["workflow_complete"]:
            disabled = disable_self()
            state["consecutive_inactive_checks"] = 0
            state["last_action"] = "complete_disabled" if disabled["ok"] else "complete_disable_failed"
            result = {"ok": disabled["ok"], "status": state["last_action"], "phase": observation["phase"]}
        elif process_alive:
            state["consecutive_inactive_checks"] = 0
            progress_age = seconds_since(state.get("last_progress_at"), now) or 0.0
            receipt = observation.get("prior_exit_receipt") or {}
            launch_started = parse_iso(receipt.get("started_at"))
            task_started = parse_iso(thread.get("latest_task_started_at"))
            task_completed = parse_iso(thread.get("latest_task_completed_at"))
            launch_age = seconds_since(receipt.get("started_at"), now)
            current_task_started = bool(
                launch_started is not None
                and task_started is not None
                and task_started >= launch_started.astimezone(task_started.tzinfo)
            )
            current_task_completed = bool(
                current_task_started
                and task_completed is not None
                and task_started is not None
                and task_completed >= task_started.astimezone(task_completed.tzinfo)
            )
            completed_age = seconds_since(thread.get("latest_task_completed_at"), now)
            post_completion_hang = bool(
                not raw_turn_open
                and current_task_completed
                and completed_age is not None
                and completed_age >= post_completion_hang_seconds
            )
            startup_stall = bool(
                receipt.get("status") == "running"
                and receipt.get("stage") == "codex_dispatched"
                and not current_task_started
                and launch_age is not None
                and launch_age >= startup_stall_seconds
            )
            live_stall = bool(progress_age >= live_stall_seconds)
            if post_completion_hang or startup_stall or live_stall:
                recycle_reason = (
                    "post_completion_hang"
                    if post_completion_hang
                    else "startup_stall"
                    if startup_stall
                    else "live_stall"
                )
                recycle = recycle_control_owner(observation, reason=recycle_reason)
                recycled = recycle.get("ok") is True
                state["recycle_count"] = int(state.get("recycle_count", 0)) + (1 if recycled else 0)
                state["last_action"] = f"recycled_{recycle_reason}" if recycled else "recycle_failed"
                result = {
                    "ok": recycled,
                    "status": state["last_action"],
                    "phase": observation["phase"],
                    "progress_age_seconds": int(progress_age),
                    "recycle_reason": recycle_reason,
                    "recycle_method": recycle.get("method"),
                    "recycle_error_class": recycle.get("error_class"),
                }
            else:
                state["last_action"] = "active_progress" if changed else "active_wait"
                result = {
                    "ok": True,
                    "status": state["last_action"],
                    "phase": observation["phase"],
                    "progress_age_seconds": int(progress_age),
                }
        elif not process_available:
            # A ps failure is unknown liveness, never proof that the lane is
            # inactive. Fail the scheduler check so the health alert surfaces
            # it, but do not launch a potentially duplicate turn.
            state["consecutive_inactive_checks"] = 0
            state["last_action"] = "liveness_unavailable"
            result = {
                "ok": False,
                "status": "liveness_unavailable",
                "phase": observation["phase"],
            }
        elif (turn_claimed_live or raw_turn_open) and not proven_orphan:
            # Recent raw lifecycle activity may outlive a discoverable process
            # briefly. Only stale, proven-dead orphan state reaches recovery.
            state["consecutive_inactive_checks"] = 0
            state["last_action"] = "raw_open_wait"
            result = {
                "ok": True,
                "status": "raw_open_wait",
                "phase": observation["phase"],
            }
        else:
            state["consecutive_inactive_checks"] = int(state.get("consecutive_inactive_checks", 0)) + 1
            inactive_checks = int(state["consecutive_inactive_checks"])
            launch_age = seconds_since(state.get("last_launch_at"), now)
            cooldown_clear = launch_age is None or launch_age >= launch_cooldown_seconds
            if inactive_checks < debounce_checks:
                state["last_action"] = "inactive_debounce"
                result = {"ok": True, "status": "inactive_debounce", "phase": observation["phase"], "inactive_checks": inactive_checks}
            elif not cooldown_clear:
                state["last_action"] = "inactive_cooldown"
                result = {"ok": True, "status": "inactive_cooldown", "phase": observation["phase"], "inactive_checks": inactive_checks}
            else:
                if tmux_session_alive(observation.get("session_name")):
                    kill_tmux_session(observation.get("session_name"))
                recovery_id = now.strftime("%Y%m%dT%H%M%S%z")
                message_path = WATCHDOG_ROOT / "recovery-prompts" / f"recovery-{recovery_id}.md"
                message_path.parent.mkdir(parents=True, exist_ok=True)
                message_path.write_text(recovery_message(observation, recovery_id), encoding="utf-8")
                message_path.chmod(0o600)
                launch = launch_resume(observation, message_path, pipeline_state_path)
                state["last_launch_at"] = now.isoformat()
                state["last_launch_receipt"] = launch
                state["launch_count"] = int(state.get("launch_count", 0)) + (1 if launch["ok"] else 0)
                state["consecutive_inactive_checks"] = 0 if launch["ok"] else inactive_checks
                state["last_action"] = "resume_launched" if launch["ok"] else "resume_failed"
                result = {
                    "ok": launch["ok"],
                    "status": state["last_action"],
                    "phase": observation["phase"],
                    "inactive_checks": inactive_checks,
                    "launch_exit_code": launch["exit_code"],
                    "launch_error_class": launch.get("error_class"),
                }

    event = {
        "at": now.isoformat(),
        **result,
        "thread_id": observation.get("thread_id"),
        "milestone_sha256": observation["milestones"].get("snapshot_sha256"),
        "milestone_error_class": observation["milestones"].get("error_class"),
    }
    append_event(event)
    return event


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deterministic liveness watchdog for the PIF evaluation-to-extraction pipeline.")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--state", default=str(DEFAULT_WATCHDOG_STATE))
    run.add_argument("--pipeline-state", default=str(DEFAULT_STATE_PATH))
    run.add_argument("--goals-db", default=str(DEFAULT_GOALS_DB_PATH))
    run.add_argument("--evaluation-root", default=str(DEFAULT_EVALUATION_ROOT))
    run.add_argument("--db", default=str(DEFAULT_DB_PATH))
    run.add_argument("--debounce-checks", type=int, default=DEFAULT_DEBOUNCE_CHECKS)
    run.add_argument("--launch-cooldown-seconds", type=int, default=DEFAULT_LAUNCH_COOLDOWN_SECONDS)
    run.add_argument("--live-stall-seconds", type=int, default=DEFAULT_LIVE_STALL_SECONDS)
    run.add_argument("--startup-stall-seconds", type=int, default=DEFAULT_STARTUP_STALL_SECONDS)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = run_cycle(
            watchdog_state_path=Path(args.state),
            pipeline_state_path=Path(args.pipeline_state),
            goals_db=Path(args.goals_db),
            evaluation_root=Path(args.evaluation_root),
            db_path=Path(args.db),
            debounce_checks=max(1, int(args.debounce_checks)),
            launch_cooldown_seconds=max(60, int(args.launch_cooldown_seconds)),
            live_stall_seconds=max(15 * 60, int(args.live_stall_seconds)),
            startup_stall_seconds=max(5 * 60, int(args.startup_stall_seconds)),
        )
    except BlockingIOError:
        print(json.dumps({"ok": True, "status": "skipped_locked"}))
        return 0
    except (OSError, TimeoutError, ValueError, subprocess.SubprocessError) as exc:
        failure = {
            "at": iso_now(),
            "ok": False,
            "status": "error",
            "error_class": type(exc).__name__,
        }
        try:
            append_event(failure)
        except OSError:
            pass
        print(json.dumps(failure))
        return 1
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
