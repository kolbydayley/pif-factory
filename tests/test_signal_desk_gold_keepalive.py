import json
from pathlib import Path

from research_factory.signal_desk_gold_keepalive import (
    MAX_BACKOFF_SECONDS,
    MIN_BACKOFF_SECONDS,
    classify_checkpoint,
    decide,
    next_state,
    run_once,
)

STOPPED = {"status": "checkpointed_failure", "error": "validation Gold stopped during A; checkpoint preserved"}


def test_alive_runner_is_left_alone_and_resets_backoff_after_healthy_uptime():
    state = {"consecutive_relaunches": 4, "last_launch_at": 1000.0}
    decision = decide(runner_alive=True, kill_present=False, checkpoint=STOPPED, state=state, now=1000.0 + 1800)
    assert decision.action == "noop"
    assert next_state(state, decision, now=1000.0 + 1800)["consecutive_relaunches"] == 0
    assert next_state(state, decision, now=1000.0 + 60)["consecutive_relaunches"] == 4


def test_resumable_stop_relaunches_with_exponential_backoff():
    first = decide(runner_alive=False, kill_present=False, checkpoint=STOPPED, state={}, now=5000.0)
    assert first.action == "relaunch"
    state = next_state({}, first, now=5000.0)
    assert state["consecutive_relaunches"] == 1
    # Dies again immediately: must wait the doubled backoff, not flap.
    again = decide(runner_alive=False, kill_present=False, checkpoint=STOPPED, state=state, now=5000.0 + 30)
    assert again.action == "noop" and again.wait_seconds > 0
    later = decide(
        runner_alive=False, kill_present=False, checkpoint=STOPPED, state=state,
        now=5000.0 + MIN_BACKOFF_SECONDS * 2,
    )
    assert later.action == "relaunch"
    capped = decide(
        runner_alive=False, kill_present=False, checkpoint=STOPPED,
        state={"consecutive_relaunches": 20, "last_launch_at": 5000.0}, now=5000.0 + MAX_BACKOFF_SECONDS - 1,
    )
    assert capped.action == "noop"


def test_kill_complete_and_operator_faults_never_relaunch():
    assert decide(runner_alive=False, kill_present=True, checkpoint=STOPPED, state={}, now=1.0).action == "hold"
    assert decide(runner_alive=False, kill_present=False, checkpoint={"status": "complete"}, state={}, now=1.0).action == "done"
    for error in (
        "split manifest hash verification failed",
        "sealed-holdout Gold requires explicit authorization",
        "validation Gold phase C is incomplete",
        "Gold stage returned a non-complete receipt",
    ):
        checkpoint = {"status": "checkpointed_failure", "error": error}
        assert classify_checkpoint(checkpoint) == "operator_required"
        assert decide(runner_alive=False, kill_present=False, checkpoint=checkpoint, state={}, now=1.0).action == "hold"
    # Crash with no checkpoint, or a transient deferred state, is resumable.
    assert classify_checkpoint(None) == "resumable"
    assert classify_checkpoint({"status": "deferred", "reason": "foreground_codex_active"}) == "resumable"
    assert classify_checkpoint({"status": "checkpointed_failure", "error": "unexpected non-transient deferred state: gold_weekly_gate"}) == "resumable"


def test_run_once_relaunches_persists_state_and_notifies_holds_once(tmp_path: Path):
    gold_root = tmp_path / "gold"
    (gold_root / "artifacts").mkdir(parents=True)
    (gold_root / "artifacts" / "gold-resume-supervisor.json").write_text(json.dumps(STOPPED))
    budget_dir = tmp_path / "budget"
    budget_dir.mkdir()
    launches: list[tuple[Path, Path]] = []
    notes: list[str] = []
    kwargs = dict(
        project_root=tmp_path, gold_root=gold_root, budget_dir=budget_dir,
        alive=lambda: False, relaunch=lambda root, log: launches.append((root, log)),
        notifier=lambda kind, *a, **k: notes.append(kind),
    )
    assert run_once(now=100.0, **kwargs).action == "relaunch"
    assert launches == [(tmp_path, gold_root / "logs" / "resume.log")]
    state = json.loads((gold_root / "artifacts" / "keepalive-state.json").read_text())
    assert state["consecutive_relaunches"] == 1 and state["last_launch_at"] == 100.0
    assert (gold_root / "logs" / "keepalive.log").read_text().count("\n") == 1
    # Within backoff: no second launch.
    assert run_once(now=110.0, **kwargs).action == "noop"
    assert len(launches) == 1
    # Operator KILL appears: hold, notify exactly once across repeated checks.
    (budget_dir / "KILL-signal-desk-gold-authoring.json").write_text("{}")
    assert run_once(now=5000.0, **kwargs).action == "hold"
    assert run_once(now=5100.0, **kwargs).action == "hold"
    assert notes == ["relaunched", "held"]
    assert len(launches) == 1
