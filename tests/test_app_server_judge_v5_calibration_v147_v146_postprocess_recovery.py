from __future__ import annotations

from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v147_v146_postprocess_recovery import (
    _validate_v146_failure,
    build_v147_plan,
    run_v147,
)


def test_v147_recovers_complete_v146_usage_and_latent_quality_failure():
    source = _validate_v146_failure()

    assert source["usage"]["total_tokens"] == 517102
    assert len(source["attempts"]) == 24
    assert source["score"]["passed"] is False
    assert source["score"]["failed_checks"] == ["unstable_owner_repeat_exact_rate"]
    assert source["score"]["metrics"]["control_exact_count"] == 5
    assert source["score"]["metrics"]["repeat_exact_count"] == 3


def test_v147_isolates_two_unsupported_inference_repeats_and_one_missing_receipt():
    plan = build_v147_plan(_validate_v146_failure())

    assert plan["failed_repeat_count"] == 2
    assert plan["failed_repeat_field_counts"] == {"unsupported_inference": 2}
    assert plan["missing_fresh_support_unit_count"] == 1
    assert plan["fresh_turn_count"] == 2
    assert sum(row["pointwise_support_receipt_available"] for row in plan["units"]) == 1
    assert plan["pointwise_support_projection_rule"] == {
        "supported": "correct",
        "unsupported": "incorrect",
        "abstain": "abstain",
    }


def test_v147_writes_idempotent_zero_token_inactive_terminal(tmp_path: Path):
    first = run_v147(output_dir=tmp_path / "v147")
    second = run_v147(output_dir=tmp_path / "v147")

    assert first == second
    assert first["state"] == "inactive"
    assert first["terminal_reason"] == "inactive_incomplete_recovery_required"
    assert first["support_projection_repair_authorized"] is True
    assert first["semantic_attempt_started"] is False
    assert first["usage_status"] == "complete"
    assert first["usage"]["total_tokens"] == 0
    assert first["predecessor_v146_usage"]["total_tokens"] == 517102
    assert first["selection_authorized"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False


def test_v147_does_not_mutate_v146(tmp_path: Path):
    before = _validate_v146_failure()["records"]
    run_v147(output_dir=tmp_path / "v147")
    after = _validate_v146_failure()["records"]
    assert before == after
