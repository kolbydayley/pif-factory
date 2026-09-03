"""Keep the Signal Desk Gold resume runner alive across resumable stops.

The runner is deliberately fail-closed: a single infrastructure fault, a
weekly-gate stall, or (before quarantine) one malformed answer stops the
swarm with "checkpoint preserved" and exits.  Every such stop is resumable
from dispatch leases, but nothing relaunched the process, so a stop at
15:02 cost the campaign the rest of the afternoon on 2026-09-02.

This module decides, from durable state only, whether a dead runner should
be relaunched.  It never relaunches over an operator KILL receipt, a
completed campaign, or a checkpoint whose error names a contract, manifest,
authorization, or prerequisite fault that needs a human.  Relaunches back
off exponentially so a runner that dies immediately cannot flap.
"""
from __future__ import annotations

import json
import shlex
import sqlite3
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .signal_desk_gold_budget import gold_kill_path

TMUX_SESSION = "signal-desk-gold"
# Loose on purpose: the interpreter may present as "python3" or the
# framework "Python" in argv; the executable allowlist below is the filter.
RUNNER_PATTERN = r"scripts/pif_signal_desk_gold_resume\.py"
RELAUNCH_ARGV = (
    "/usr/bin/caffeinate", "-i", "/usr/bin/python3", "-B",
    "scripts/pif_signal_desk_gold_resume.py", "--allow-sealed-holdout",
    "--concurrency", "8",
)
MIN_BACKOFF_SECONDS = 60
MAX_BACKOFF_SECONDS = 1800
HEALTHY_UPTIME_SECONDS = 1800

# Checkpoint errors that require a human before any relaunch.  Everything
# else ("Gold stopped during A; checkpoint preserved", a non-transient
# deferred state, a crash with no checkpoint at all) resumes from leases.
OPERATOR_REQUIRED_MARKERS = (
    "hash verification failed",
    "symbolic link",
    "permissions are not 0700",
    "requires explicit authorization",
    "unknown Gold split",
    "unsupported phase",
    "must not repeat a phase",
    "no frozen",
    "non-complete receipt",
    "is incomplete",
    "worker pool is empty",
    "invalid turn type",
    "requires the complete, frozen",
    "1,000-event floor",
    "split-specific dispatch",
    "cannot reach",
)


def pause_path(gold_root: Path) -> Path:
    """Touch this file to hold the keepalive (loop and cron) without disabling anything."""

    return gold_root / "artifacts" / "keepalive.PAUSE"


@dataclass(frozen=True)
class Decision:
    action: str  # noop | relaunch | hold | done
    reason: str
    wait_seconds: int = 0


def classify_checkpoint(checkpoint: Mapping[str, Any] | None) -> str:
    """complete | operator_required | resumable."""

    if not checkpoint:
        return "resumable"
    status = str(checkpoint.get("status") or "")
    if status == "complete":
        return "complete"
    error = str(checkpoint.get("error") or checkpoint.get("reason") or "")
    if any(marker in error for marker in OPERATOR_REQUIRED_MARKERS):
        return "operator_required"
    return "resumable"


def decide(
    *,
    runner_alive: bool,
    kill_present: bool,
    checkpoint: Mapping[str, Any] | None,
    state: Mapping[str, Any],
    now: float,
    paused: bool = False,
) -> Decision:
    consecutive = int(state.get("consecutive_relaunches") or 0)
    last_launch = float(state.get("last_launch_at") or 0.0)
    if runner_alive:
        return Decision("noop", "runner alive")
    if paused:
        # An explicit operator pause.  Without it, a lagging keepalive tick
        # relaunched the runner two minutes after a pause on 2026-09-03,
        # seconds before a reboot killed it again.
        return Decision("hold", "PAUSE file present; not relaunching")
    if kill_present:
        return Decision("hold", "operator KILL receipt present; not relaunching")
    kind = classify_checkpoint(checkpoint)
    if kind == "complete":
        return Decision("done", "campaign checkpoint is complete")
    if kind == "operator_required":
        error = str((checkpoint or {}).get("error") or (checkpoint or {}).get("reason") or "")
        return Decision("hold", f"checkpoint needs an operator: {error[:200]}")
    backoff = min(MAX_BACKOFF_SECONDS, MIN_BACKOFF_SECONDS * (2 ** consecutive))
    elapsed = now - last_launch
    if last_launch and elapsed < backoff:
        return Decision("noop", f"backing off {int(backoff - elapsed)}s before relaunch", int(backoff - elapsed))
    return Decision("relaunch", f"runner dead with resumable checkpoint ({kind}); relaunch #{consecutive + 1}")


def next_state(state: Mapping[str, Any], decision: Decision, *, now: float) -> dict[str, Any]:
    result = dict(state)
    if decision.action == "relaunch":
        result["consecutive_relaunches"] = int(state.get("consecutive_relaunches") or 0) + 1
        result["last_launch_at"] = now
        result["last_relaunch_reason"] = decision.reason
    elif decision.action == "noop" and decision.reason == "runner alive":
        last_launch = float(state.get("last_launch_at") or 0.0)
        # A runner that has stayed up for a healthy stretch earns a fresh
        # backoff ladder for whatever kills it next.
        if last_launch and now - last_launch >= HEALTHY_UPTIME_SECONDS:
            result["consecutive_relaunches"] = 0
    result["last_check_at"] = now
    result["last_action"] = decision.action
    result["last_reason"] = decision.reason
    return result


# --- runtime -----------------------------------------------------------------

RUNNER_COMMANDS = frozenset({"python", "python3", "Python", "caffeinate"})


def runner_pids(pgrep_pids: Sequence[str], comm_by_pid: Mapping[str, str]) -> list[str]:
    """Runner processes among pgrep matches: the interpreter or its caffeinate wrapper.

    ``pgrep -f`` matches any argv containing the pattern: a shell whose script
    text mentions it (a monitor loop, an `until` waiter) and the tmux server
    that was started with the runner command.  On 2026-09-02 both made a dead
    runner look alive.  Only an allowlisted executable counts.
    """

    return [
        pid for pid in pgrep_pids
        if comm_by_pid.get(pid, "").rsplit("/", 1)[-1] in RUNNER_COMMANDS
    ]


def runner_alive() -> bool:
    proc = subprocess.run(["pgrep", "-f", RUNNER_PATTERN], capture_output=True, text=True, check=False)
    pids = [pid for pid in proc.stdout.split() if pid.isdigit()]
    if not pids:
        return False
    ps = subprocess.run(
        ["ps", "-o", "pid=,comm=", "-p", ",".join(pids)], capture_output=True, text=True, check=False
    )
    comm_by_pid: dict[str, str] = {}
    for line in ps.stdout.splitlines():
        parts = line.split(None, 1)
        if len(parts) == 2:
            comm_by_pid[parts[0]] = parts[1].strip()
    return bool(runner_pids(pids, comm_by_pid))


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def tmux_relaunch(project_root: Path, log_path: Path) -> None:
    inner = (
        f"cd {shlex.quote(str(project_root))} && exec "
        + " ".join(shlex.quote(part) for part in RELAUNCH_ARGV)
        + f" >> {shlex.quote(str(log_path))} 2>&1"
    )
    window = f"runner-{time.strftime('%H%M%S')}"
    has_session = subprocess.run(
        ["tmux", "has-session", "-t", TMUX_SESSION], capture_output=True, check=False
    ).returncode == 0
    argv = (
        ["tmux", "new-window", "-d", "-t", TMUX_SESSION, "-n", window, inner]
        if has_session
        else ["tmux", "new-session", "-d", "-s", TMUX_SESSION, "-n", window, inner]
    )
    subprocess.run(argv, check=True, capture_output=True, text=True)


GOLD_LANE = "gpt_5_6_sol_gold_authoring"


def release_orphaned_admissions(budget_database: Path) -> int:
    """Free capacity slots left behind by a runner that died mid-call.

    Admission leases last 30 minutes; a relaunched runner otherwise defers on
    ``gold_model_capacity_slots_full`` until they lapse.  The keepalive only
    relaunches when no runner is alive, so every lease on the Gold lane is
    orphaned at that moment.  Returns the number released.
    """

    if not budget_database.exists():
        return 0
    from .signal_desk_gold_capacity import release_gold_admission

    conn = sqlite3.connect(budget_database)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT admission_id FROM signal_desk_gold_capacity_leases WHERE lane = ?",
            (GOLD_LANE,),
        ).fetchall()
        for row in rows:
            release_gold_admission(conn, admission_id=str(row["admission_id"]))
        return len(rows)
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()


def notify(kind: str, detail: str, next_step: str, *, severity: str = "high") -> None:
    subprocess.run(
        ["codex-ops", "notify", "--source", "signal-desk-gold-keepalive",
         "--summary", f"Signal Desk gold keepalive: {kind}",
         "--severity", severity, "--details", detail, "--next-step", next_step,
         "--dedupe-key", f"signal-desk-gold-keepalive:{kind}", "--telegram-mode", "prefer", "--json"],
        capture_output=True, text=True, timeout=30, check=False,
    )


def run_once(
    *,
    project_root: Path,
    gold_root: Path,
    budget_dir: Path,
    budget_database: Path | None = None,
    now: float | None = None,
    alive: Callable[[], bool] = runner_alive,
    relaunch: Callable[[Path, Path], None] = tmux_relaunch,
    notifier: Callable[..., None] = notify,
    release_admissions: Callable[[Path], int] = release_orphaned_admissions,
) -> Decision:
    now = time.time() if now is None else now
    state_path = gold_root / "artifacts" / "keepalive-state.json"
    log_path = gold_root / "logs" / "keepalive.log"
    state = read_json(state_path) or {}
    decision = decide(
        runner_alive=alive(),
        kill_present=gold_kill_path(budget_dir).exists(),
        checkpoint=read_json(gold_root / "artifacts" / "gold-resume-supervisor.json"),
        state=state,
        now=now,
        paused=pause_path(gold_root).exists(),
    )
    if decision.action == "relaunch":
        released = release_admissions(budget_database) if budget_database is not None else 0
        relaunch(project_root, gold_root / "logs" / "resume.log")
        decision = Decision(
            decision.action,
            f"{decision.reason}; released {released} orphaned capacity admission(s)",
            decision.wait_seconds,
        )
        notifier(
            "relaunched", decision.reason,
            "No action needed unless relaunches keep repeating; see logs/keepalive.log.",
            severity="medium",
        )
    elif decision.action == "hold" and state.get("last_action") != "hold":
        notifier("held", decision.reason, "Resolve the operator condition, then the keepalive resumes on its own.")
    updated = next_state(state, decision, now=now)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if decision.action != "noop" or decision.reason != "runner alive":
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S%z', time.localtime(now))} {decision.action}: {decision.reason}\n")
    return decision
