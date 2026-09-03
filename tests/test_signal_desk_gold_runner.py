import asyncio
import json
import sqlite3
from pathlib import Path

import pytest

from research_factory.signal_desk_gold_runner import (
    RESERVE_TOKENS,
    SYSTEM_PROMPTS,
    GoldCapacityDeferred,
    GoldResumePlanError,
    archive_retryable_sidecar_for_retry,
    acquire_pipeline_lease,
    build_gold_resume_plan,
    compact_adjudication_output,
    repair_unique_evidence_offsets,
    run_gold_resume_supervisor,
    run_gold_split_phase,
)
from research_factory.signal_desk_rebuild_dispatch import (
    complete_attempt,
    enqueue_task,
    initialize_dispatch_schema,
)


def test_runner_reserves_above_measured_p90_and_keeps_turn_prompts_distinct():
    measured_p90 = {"A": 39288, "B": 40725.1, "C": 47078.8, "AUDIT": 39774.4}
    assert all(RESERVE_TOKENS[key] > value for key, value in measured_p90.items())
    assert set(SYSTEM_PROMPTS) == {"A", "B", "C", "AUDIT"}
    assert len(set(SYSTEM_PROMPTS.values())) == 4


def test_unique_exact_excerpt_repairs_offsets_without_changing_semantics():
    output = {
        "window_id": "w1",
        "events": [
            {
                "evidence_text": "unique excerpt",
                "evidence_start": 3,
                "evidence_end": 17,
                "claim_text": "unchanged",
            }
        ],
    }
    repaired, count = repair_unique_evidence_offsets(
        output, transcript_window="prefix unique excerpt suffix"
    )
    assert count == 1
    assert repaired["events"][0]["evidence_start"] == 7
    assert repaired["events"][0]["evidence_end"] == 21
    assert repaired["events"][0]["claim_text"] == "unchanged"
    assert output["events"][0]["evidence_start"] == 3


def test_compact_adjudication_input_drops_reconstructable_bulk():
    compact = compact_adjudication_output({
        "window_disposition": "publishable",
        "events": [{
            "claim_text": "A consequence",
            "evidence_text": "exact source text",
            "evidence_start": 10,
            "evidence_end": 27,
            "issue_label": "AI employment",
            "issue_aliases": ["jobs"],
            "speaker_id": "speaker-1",
        }],
    })
    assert compact["events"][0]["claim_text"] == "A consequence"
    assert compact["events"][0]["evidence_text"] == "exact source text"
    assert compact["events"][0]["speaker_id"] == "speaker-1"
    assert "evidence_start" not in compact["events"][0]
    assert "evidence_end" not in compact["events"][0]
    assert "issue_aliases" not in compact["events"][0]


def test_pipeline_lease_finishes_c_then_b_before_admitting_more_a():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    initialize_dispatch_schema(conn)
    for turn_type, window_id in (("A", "w1"), ("A", "w2"), ("B", "w3"), ("C", "w4")):
        enqueue_task(
            conn,
            task_key=f"validation:{turn_type}:{window_id}",
            task_type="gold_window",
            payload={"turn_type": turn_type, "window_id": window_id},
        )
    observed = []
    for index in range(4):
        lease = acquire_pipeline_lease(
            conn,
            task_namespace="validation",
            lease_owner=f"worker-{index}",
            lease_seconds=60,
        )
        assert lease is not None
        observed.append(lease["payload"]["turn_type"])
        complete_attempt(
            conn,
            attempt_id=lease["current_attempt_id"],
            lease_owner=lease["lease_owner"],
            lease_generation=lease["lease_generation"],
            output={"ok": True},
        )
    assert observed == ["C", "B", "A", "A"]


def test_ambiguous_excerpt_is_never_rebound():
    output = {
        "events": [
            {"evidence_text": "same", "evidence_start": 1, "evidence_end": 5}
        ]
    }
    repaired, count = repair_unique_evidence_offsets(
        output, transcript_window="same and same"
    )
    assert count == 0
    assert repaired == output


def test_declared_span_may_restore_source_whitespace_only():
    output = {
        "events": [
            {"evidence_text": "caption text", "evidence_start": 0, "evidence_end": 11}
        ]
    }
    repaired, count = repair_unique_evidence_offsets(
        output, transcript_window="captiontext"
    )
    assert count == 1
    assert repaired["events"][0]["evidence_text"] == "captiontext"

    changed, changed_count = repair_unique_evidence_offsets(
        {"events": [{"evidence_text": "caption best", "evidence_start": 0, "evidence_end": 11}]},
        transcript_window="captiontext",
    )
    assert changed_count == 0
    assert changed["events"][0]["evidence_text"] == "caption best"


def test_cancelled_sidecar_is_preserved_before_same_lineage_retry(tmp_path):
    sidecar = tmp_path / "sidecars" / "window.json"
    sidecar.parent.mkdir()
    sidecar.write_text('{"state":"cancelled","turn_id":"t1"}\n', encoding="utf-8")
    output = tmp_path / "results" / "window.json"

    archived = archive_retryable_sidecar_for_retry(
        sidecar_path=sidecar,
        output_path=output,
        recovery_root=tmp_path / "recovery",
        attempt_id=12,
        lease_generation=3,
    )

    assert archived == tmp_path / "recovery/window.attempt-12.generation-3.json"
    assert archived.read_text(encoding="utf-8").startswith('{"state":"cancelled"')
    assert not sidecar.exists()


def test_completed_sidecar_is_never_archived_for_retry(tmp_path):
    sidecar = tmp_path / "window.json"
    sidecar.write_text('{"state":"completed"}\n', encoding="utf-8")

    assert archive_retryable_sidecar_for_retry(
        sidecar_path=sidecar,
        output_path=tmp_path / "output.json",
        recovery_root=tmp_path / "recovery",
        attempt_id=1,
        lease_generation=1,
    ) is None
    assert sidecar.exists()


def test_allowlisted_provider_failure_is_preserved_for_retry(tmp_path):
    sidecar = tmp_path / "window.json"
    sidecar.write_text(
        '{"state":"failed","error_class":"turn_failed",'
        '"turn_error":{"codex_error_info":"serverOverloaded"}}\n',
        encoding="utf-8",
    )

    archived = archive_retryable_sidecar_for_retry(
        sidecar_path=sidecar,
        output_path=tmp_path / "output.json",
        recovery_root=tmp_path / "recovery",
        attempt_id=8,
        lease_generation=2,
    )

    assert archived is not None
    assert archived.exists()
    assert not sidecar.exists()


def test_unknown_provider_failure_remains_fail_closed(tmp_path):
    sidecar = tmp_path / "window.json"
    sidecar.write_text(
        '{"state":"failed","error_class":"turn_failed",'
        '"turn_error":{"codex_error_info":"unknown"}}\n',
        encoding="utf-8",
    )

    assert archive_retryable_sidecar_for_retry(
        sidecar_path=sidecar,
        output_path=tmp_path / "output.json",
        recovery_root=tmp_path / "recovery",
        attempt_id=8,
        lease_generation=2,
    ) is None
    assert sidecar.exists()


def _manifest_path() -> Path:
    return Path("work/signal-desk-rebuild/benchmark/partial-manifest.json")


def test_resume_plan_is_exactly_staged_split_safe_and_never_mentions_a1_a2(tmp_path: Path):
    plan = build_gold_resume_plan(
        manifest_path=_manifest_path(),
        gold_root=tmp_path / "gold",
        allow_sealed_holdout=True,
    )

    assert [(stage["split"], stage["turn_type"]) for stage in plan["stages"]] == [
        ("development", "C"),
        ("validation", "PIPELINE"),
        ("sealed_holdout", "PIPELINE"),
        ("development", "AUDIT"),
        ("validation", "AUDIT"),
        ("sealed_holdout", "AUDIT"),
    ]
    assert plan["contains_a1_or_a2"] is False
    assert plan["configured_max_concurrency"] == 8
    assert plan["initial_adaptive_concurrency"] == 2
    assert plan["lease_seconds"] == 1800
    assert plan["deadline_seconds"] == 900
    assert len({stage["dispatch_database"] for stage in plan["stages"]}) == 3
    assert all(
        stage["sealed_output_root"] is not None
        for stage in plan["stages"]
        if stage["split"] in {"validation", "sealed_holdout"}
    )


def test_resume_plan_refuses_holdout_before_any_prefix_can_start(tmp_path: Path):
    with pytest.raises(GoldResumePlanError, match="explicit authorization"):
        build_gold_resume_plan(
            manifest_path=_manifest_path(),
            gold_root=tmp_path / "gold",
            allow_sealed_holdout=False,
        )


def test_pipeline_entrypoint_maps_to_per_window_a_b_c_flow(tmp_path: Path, monkeypatch):
    import research_factory.signal_desk_gold_runner as runner

    captured = {}

    async def fake_run(**kwargs):
        captured.update(kwargs)
        return {"complete": True}

    monkeypatch.setattr(runner, "_run_gold_split_phases", fake_run)
    receipt = asyncio.run(
        run_gold_split_phase(
            manifest_path=_manifest_path(),
            project_root=Path.cwd(),
            result_root=tmp_path / "results",
            dispatch_database=tmp_path / "dispatch.sqlite",
            budget_database=tmp_path / "budget.sqlite",
            grant_path=tmp_path / "grant.json",
            session_root=tmp_path / "sessions",
            budget_dir=tmp_path / "budget-dir",
            split="validation",
            turn_type="PIPELINE",
            task_namespace="validation",
        )
    )
    assert receipt == {"complete": True}
    assert captured["phase_order"] == ("A", "B", "C")


def test_c_phase_preflight_requires_full_a_and_b_before_any_c_dispatch(tmp_path: Path):
    dispatch = tmp_path / "dispatch-dev.sqlite"
    budget = tmp_path / "budget.sqlite"
    with pytest.raises(RuntimeError, match="Gold A is incomplete"):
        asyncio.run(
            run_gold_split_phase(
                manifest_path=_manifest_path(),
                project_root=Path.cwd(),
                result_root=tmp_path / "results" / "development",
                dispatch_database=dispatch,
                budget_database=budget,
                grant_path=tmp_path / "unused-grant.json",
                session_root=tmp_path / "sessions",
                budget_dir=tmp_path / "budget-dir",
                split="development",
                turn_type="C",
                task_namespace="dev",
                concurrency=8,
            )
        )
    with sqlite3.connect(dispatch) as conn:
        assert conn.execute("SELECT COUNT(*) FROM signal_desk_rebuild_tasks").fetchone()[0] == 0
    with sqlite3.connect(budget) as conn:
        row = conn.execute(
            "SELECT effective_limit FROM signal_desk_adaptive_concurrency_state WHERE lane='gold'"
        ).fetchone()
    assert row == (2,)


@pytest.mark.parametrize("turn_type, expected_predecessor", [("B", "Gold A"), ("AUDIT", "Gold C")])
def test_public_phase_entrypoint_enforces_predecessors(
    tmp_path: Path, turn_type: str, expected_predecessor: str
):
    with pytest.raises(RuntimeError, match=expected_predecessor):
        asyncio.run(
            run_gold_split_phase(
                manifest_path=_manifest_path(),
                project_root=Path.cwd(),
                result_root=tmp_path / "results" / "development",
                dispatch_database=tmp_path / "dispatch.sqlite",
                budget_database=tmp_path / "budget.sqlite",
                grant_path=tmp_path / "grant.json",
                session_root=tmp_path / "sessions",
                budget_dir=tmp_path / "budget-dir",
                split="development",
                turn_type=turn_type,
                task_namespace="dev",
            )
        )


def test_open_capacity_circuit_defers_before_constructing_an_app_server_client(tmp_path: Path, monkeypatch):
    import datetime as dt
    import research_factory.signal_desk_gold_capacity as capacity
    import research_factory.signal_desk_gold_runner as runner

    budget = tmp_path / "budget.sqlite"
    with sqlite3.connect(budget) as conn:
        capacity.ensure_gold_capacity_schema(conn)
        conn.execute(
            "UPDATE signal_desk_gold_capacity_state SET state='open', next_probe_at=?",
            ((dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)).isoformat(),),
        )
        conn.commit()

    class MustNotConstructClient:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("capacity backoff must not spawn an app-server client")

    monkeypatch.setattr(runner, "CodexAppServerClient", MustNotConstructClient)
    result_root = tmp_path / "results" / "development"
    with pytest.raises(GoldCapacityDeferred, match="capacity_backoff"):
        asyncio.run(
            run_gold_split_phase(
                manifest_path=_manifest_path(),
                project_root=Path.cwd(),
                result_root=result_root,
                dispatch_database=tmp_path / "dispatch.sqlite",
                budget_database=budget,
                grant_path=tmp_path / "grant.json",
                session_root=tmp_path / "sessions",
                budget_dir=tmp_path / "budget-dir",
                split="development",
                turn_type="A",
                task_namespace="dev",
            )
        )
    status = json.loads((result_root / "gold-development-status.json").read_text())
    assert status["status"] == "deferred"
    assert status["reason"] == "gold_model_capacity_backoff"
    assert status["provider_calls_started"] == 0


def test_holdout_phase_rejects_missing_explicit_flag_without_opening_packets(tmp_path: Path, monkeypatch):
    import research_factory.signal_desk_gold_runner as runner

    called = False

    def never_build(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("sealed packet materialization must not run")

    monkeypatch.setattr(runner, "build_gold_packets", never_build)
    with pytest.raises(GoldResumePlanError, match="explicit authorization"):
        asyncio.run(
            run_gold_split_phase(
                manifest_path=_manifest_path(),
                project_root=Path.cwd(),
                result_root=tmp_path / "sealed" / "sealed_holdout",
                dispatch_database=tmp_path / "dispatch.sqlite",
                budget_database=tmp_path / "budget.sqlite",
                grant_path=tmp_path / "grant.json",
                session_root=tmp_path / "sessions",
                budget_dir=tmp_path / "budget-dir",
                split="sealed_holdout",
                turn_type="A",
                allow_sealed_holdout=False,
                sealed_output_root=tmp_path / "sealed",
            )
        )
    assert called is False


def test_explicit_holdout_phase_secures_root_before_packet_materialization(tmp_path: Path, monkeypatch):
    import research_factory.signal_desk_gold_runner as runner

    def never_build(*_args, **_kwargs):
        assert sealed_root.stat().st_mode & 0o777 == 0o700
        assert result_root.stat().st_mode & 0o777 == 0o700
        raise RuntimeError("packet materialization sentinel")

    monkeypatch.setattr(runner, "build_gold_packets", never_build)
    sealed_root = tmp_path / "sealed"
    result_root = sealed_root / "sealed_holdout"
    with pytest.raises(RuntimeError, match="packet materialization sentinel"):
        asyncio.run(
            run_gold_split_phase(
                manifest_path=_manifest_path(),
                project_root=Path.cwd(),
                result_root=result_root,
                dispatch_database=tmp_path / "dispatch.sqlite",
                budget_database=tmp_path / "budget.sqlite",
                grant_path=tmp_path / "grant.json",
                session_root=tmp_path / "sessions",
                budget_dir=tmp_path / "budget-dir",
                split="sealed_holdout",
                turn_type="A",
                allow_sealed_holdout=True,
                sealed_output_root=sealed_root,
            )
        )
    assert sealed_root.stat().st_mode & 0o777 == 0o700
    assert result_root.stat().st_mode & 0o777 == 0o700


def test_supervisor_calls_only_the_authorized_stages_in_order_and_checkpoints_each_stage(
    tmp_path: Path, monkeypatch
):
    import research_factory.signal_desk_gold_runner as runner

    calls: list[tuple[str, str]] = []
    checkpoints: list[dict] = []

    def capture_checkpoint(*, gold_root, receipt):
        checkpoints.append(dict(receipt))
        return gold_root / "checkpoint.json"

    monkeypatch.setattr(runner, "_write_resume_supervision_checkpoint", capture_checkpoint)

    async def fake_phase_runner(**kwargs):
        calls.append((kwargs["split"], kwargs["turn_type"]))
        return {
            "complete": True,
            "receipt_sha256": f"receipt-{len(calls)}",
            "phases": [{"target_windows": 1}],
        }

    receipt = asyncio.run(
        run_gold_resume_supervisor(
            manifest_path=_manifest_path(),
            project_root=Path.cwd(),
            gold_root=tmp_path / "gold",
            budget_database=tmp_path / "budget.sqlite",
            grant_path=tmp_path / "grant.json",
            session_root=tmp_path / "sessions",
            budget_dir=tmp_path / "budget-dir",
            allow_sealed_holdout=True,
            phase_runner=fake_phase_runner,
        )
    )
    assert receipt["status"] == "complete"
    assert calls == [
        ("development", "C"),
        ("validation", "PIPELINE"),
        ("sealed_holdout", "PIPELINE"),
        ("development", "AUDIT"),
        ("validation", "AUDIT"),
        ("sealed_holdout", "AUDIT"),
    ]
    stage_completions = [item for item in checkpoints if item["status"] == "stage_complete"]
    assert len(stage_completions) == len(calls)
    assert stage_completions[0]["last_completed_stage"] == {
        "split": "development",
        "turn_type": "C",
        "receipt_sha256": "receipt-1",
        "target_windows": 1,
        "quarantined_windows": 0,
    }
    assert checkpoints[-1]["status"] == "complete"


def test_contract_quarantine_is_durable_and_never_revived_by_reenqueue():
    from research_factory.signal_desk_gold_runner import (
        _phase_required_ids,
        quarantined_window_ids,
    )
    from research_factory.signal_desk_rebuild_dispatch import (
        acquire_lease,
        fail_attempt_semantically,
    )

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    initialize_dispatch_schema(conn)
    for window_id in ("w1", "w2", "w3"):
        enqueue_task(
            conn,
            task_key=f"validation:A:{window_id}",
            task_type="gold_window",
            payload={"turn_type": "A", "window_id": window_id},
        )
    # A different split's failure must never leak into this namespace.
    enqueue_task(
        conn, task_key="dev:A:w9", task_type="gold_window",
        payload={"turn_type": "A", "window_id": "w9"},
    )
    assert quarantined_window_ids(conn, task_namespace="validation") == {}

    lease = acquire_lease(
        conn, lease_owner="worker-0", lease_seconds=60, task_key_prefix="validation:A:"
    )
    assert lease["payload"]["window_id"] == "w1"
    fail_attempt_semantically(
        conn,
        attempt_id=lease["current_attempt_id"],
        lease_owner=lease["lease_owner"],
        lease_generation=lease["lease_generation"],
        failure_code="gold_contract_failure",
        failure_detail="EvidenceContractError: excerpt does not match declared offsets",
    )
    other = acquire_lease(
        conn, lease_owner="worker-9", lease_seconds=60, task_key_prefix="dev:A:"
    )
    fail_attempt_semantically(
        conn, attempt_id=other["current_attempt_id"], lease_owner=other["lease_owner"],
        lease_generation=other["lease_generation"], failure_code="gold_contract_failure",
        failure_detail="other split",
    )

    quarantined = quarantined_window_ids(conn, task_namespace="validation")
    assert set(quarantined) == {"w1"}
    assert quarantined["w1"]["turn_type"] == "A"
    assert quarantined["w1"]["failure_code"] == "gold_contract_failure"
    assert quarantined["w1"]["attempt_number"] == 1
    assert quarantined["w1"]["task_key"] == "validation:A:w1"
    # Resume re-enqueues every missing window; the quarantined one stays terminal.
    outcome = enqueue_task(
        conn, task_key="validation:A:w1", task_type="gold_window",
        payload={"turn_type": "A", "window_id": "w1"},
    )
    assert outcome["status"] == "terminal_failed"
    assert quarantined_window_ids(conn, task_namespace="validation") == quarantined
    # The other windows are still leasable, so the campaign keeps moving.
    remaining = []
    while (lease := acquire_lease(
        conn, lease_owner="worker-1", lease_seconds=60, task_key_prefix="validation:A:"
    )) is not None:
        remaining.append(lease["payload"]["window_id"])
    assert remaining == ["w2", "w3"]
    assert _phase_required_ids(["w1", "w2", "w3"], quarantined) == ["w2", "w3"]


def test_contract_failure_retries_once_then_quarantines(tmp_path: Path):
    from research_factory.signal_desk_gold_runner import (
        MAX_GOLD_CONTRACT_ATTEMPTS,
        archive_semantic_rejected_sidecar,
        contract_failure_action,
        quarantined_window_ids,
    )
    from research_factory.signal_desk_rebuild_dispatch import (
        acquire_lease,
        fail_attempt_semantically,
        resurrect_task,
    )

    assert MAX_GOLD_CONTRACT_ATTEMPTS == 2
    assert contract_failure_action(1) == "retry"
    assert contract_failure_action(2) == "quarantine"
    assert contract_failure_action(7) == "quarantine"

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    initialize_dispatch_schema(conn)
    enqueue_task(conn, task_key="validation:A:w1", task_type="gold_window",
                 payload={"turn_type": "A", "window_id": "w1"})
    # Attempt 1 fails the contract: retried as a fresh audited lineage.
    lease = acquire_lease(conn, lease_owner="worker-0", lease_seconds=60, task_key_prefix="validation:A:")
    assert lease["attempt_number"] == 1
    fail_attempt_semantically(conn, attempt_id=lease["current_attempt_id"], lease_owner=lease["lease_owner"],
                              lease_generation=lease["lease_generation"], failure_code="gold_contract_failure",
                              failure_detail="EvidenceContractError: not exact")
    assert contract_failure_action(lease["attempt_number"]) == "retry"
    snap = resurrect_task(conn, task_key="validation:A:w1", resurrected_by="gold-runner", reason="bounded contract retry 2/2")
    assert (snap["status"], snap["attempt_number"]) == ("pending", 2)
    assert quarantined_window_ids(conn, task_namespace="validation") == {}
    # Attempt 2 fails again: quarantined, and nothing leasable remains.
    lease = acquire_lease(conn, lease_owner="worker-1", lease_seconds=60, task_key_prefix="validation:A:")
    assert lease["attempt_number"] == 2
    assert contract_failure_action(lease["attempt_number"]) == "quarantine"
    fail_attempt_semantically(conn, attempt_id=lease["current_attempt_id"], lease_owner=lease["lease_owner"],
                              lease_generation=lease["lease_generation"], failure_code="gold_contract_failure",
                              failure_detail="EvidenceContractError: not exact again")
    assert set(quarantined_window_ids(conn, task_namespace="validation")) == {"w1"}
    assert acquire_lease(conn, lease_owner="worker-2", lease_seconds=60, task_key_prefix="validation:A:") is None

    # The completed sidecar of the failed call is preserved, never overwritten.
    sidecar = tmp_path / "sidecars" / "A" / "w1.json"
    sidecar.parent.mkdir(parents=True)
    sidecar.write_text('{"state": "completed"}')
    recovery = tmp_path / "recovery-sidecars" / "A"
    target = archive_semantic_rejected_sidecar(sidecar_path=sidecar, recovery_root=recovery, attempt_id=9)
    assert target == recovery / "w1.attempt-9.semantic-rejected.json" and target.exists()
    assert not sidecar.exists()
    assert archive_semantic_rejected_sidecar(sidecar_path=sidecar, recovery_root=recovery, attempt_id=9) is None


def test_startup_resurrects_quarantine_with_attempts_to_spare_only():
    from research_factory.signal_desk_gold_runner import (
        quarantined_window_ids,
        resurrect_retryable_quarantine,
    )
    from research_factory.signal_desk_rebuild_dispatch import (
        acquire_lease,
        fail_attempt_semantically,
        resurrect_task,
    )

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    initialize_dispatch_schema(conn)

    def fail(task_key: str, prefix: str, code: str = "gold_contract_failure") -> None:
        lease = acquire_lease(conn, lease_owner="w", lease_seconds=60, task_key_prefix=prefix)
        assert lease["task_key"] == task_key
        fail_attempt_semantically(conn, attempt_id=lease["current_attempt_id"], lease_owner=lease["lease_owner"],
                                  lease_generation=lease["lease_generation"], failure_code=code, failure_detail="x")

    for window_id in ("once", "twice", "other"):
        enqueue_task(conn, task_key=f"validation:A:{window_id}", task_type="gold_window",
                     payload={"turn_type": "A", "window_id": window_id})
    fail("validation:A:once", "validation:A:once")          # attempt 1: retryable
    fail("validation:A:twice", "validation:A:twice")
    resurrect_task(conn, task_key="validation:A:twice", resurrected_by="t", reason="r")
    fail("validation:A:twice", "validation:A:twice")        # attempt 2: exhausted
    fail("validation:A:other", "validation:A:other", code="gold_other_semantic")  # not a contract failure

    before = quarantined_window_ids(conn, task_namespace="validation")
    assert {k: v["attempt_number"] for k, v in before.items()} == {"once": 1, "twice": 2, "other": 1}
    remaining = resurrect_retryable_quarantine(conn, task_namespace="validation")
    assert set(remaining) == {"twice", "other"}
    assert set(quarantined_window_ids(conn, task_namespace="validation")) == {"twice", "other"}
    lease = acquire_lease(conn, lease_owner="w2", lease_seconds=60, task_key_prefix="validation:A:")
    assert lease is not None and lease["task_key"] == "validation:A:once" and lease["attempt_number"] == 2
    # Idempotent: a second startup pass resurrects nothing further.
    assert set(resurrect_retryable_quarantine(conn, task_namespace="validation")) == {"twice", "other"}


def test_timed_out_interrupted_sidecar_is_archived_for_retry(tmp_path):
    # A 900s turn timeout writes state=interrupted/status=timeout and no output;
    # it must be archivable so a relaunch does not trip _assert_new_sidecar.
    sidecar = tmp_path / "sidecars" / "A" / "w1.json"
    sidecar.parent.mkdir(parents=True)
    sidecar.write_text(
        '{"state":"interrupted","status":"timeout","error_class":"turn_timeout",'
        '"wall_elapsed_seconds":900.05}\n',
        encoding="utf-8",
    )
    archived = archive_retryable_sidecar_for_retry(
        sidecar_path=sidecar,
        output_path=tmp_path / "A" / "w1.json",
        recovery_root=tmp_path / "recovery",
        attempt_id=14,
        lease_generation=2,
    )
    assert archived == tmp_path / "recovery" / "w1.attempt-14.generation-2.json"
    assert archived.exists()
    assert not sidecar.exists()


def test_interrupted_sidecar_without_timeout_status_stays_fail_closed(tmp_path):
    # An interrupted sidecar that is not a timeout is not proven output-free.
    sidecar = tmp_path / "w1.json"
    sidecar.write_text('{"state":"interrupted","status":"aborted"}\n', encoding="utf-8")
    assert archive_retryable_sidecar_for_retry(
        sidecar_path=sidecar,
        output_path=tmp_path / "output.json",
        recovery_root=tmp_path / "recovery",
        attempt_id=1,
        lease_generation=1,
    ) is None
    assert sidecar.exists()
