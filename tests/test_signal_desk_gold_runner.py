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
    build_gold_resume_plan,
    compact_adjudication_output,
    repair_unique_evidence_offsets,
    run_gold_resume_supervisor,
    run_gold_split_phase,
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
    }
    assert checkpoints[-1]["status"] == "complete"
