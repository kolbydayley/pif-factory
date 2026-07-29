from __future__ import annotations

from pathlib import Path

import pytest

from research_factory.app_server_judge_v5_selection_v201_residual_repair_score import (
    RESIDUAL_SYSTEM_ID,
    _validate_v200_success,
    build_residual_score_inputs,
    freeze_v201,
    score_v201,
)


@pytest.fixture(scope="module")
def score_bundle():
    predecessor = _validate_v200_success()
    return predecessor, score_v201(predecessor)


def test_v201_binds_zero_token_v200_scoring_authorization():
    predecessor = _validate_v200_success()
    assert predecessor["terminal"]["scoring_authorized"] is True
    assert predecessor["terminal"]["usage"]["total_tokens"] == 0
    assert predecessor["terminal"]["cumulative_unknown_usage_turn_count"] == 2


def test_v201_augments_only_existing_outputs_and_preserves_exactness():
    predecessor = _validate_v200_success()
    sources, consensus, audit = build_residual_score_inputs(predecessor)
    assert audit["repair_case_count"] == 5
    assert audit["repair_witness_count"] == 30
    assert RESIDUAL_SYSTEM_ID in sources["membership"]["systems"]
    assert len(consensus["cases"]) == 22
    rows = sources["membership"]["system_cases"][RESIDUAL_SYSTEM_ID].values()
    assert all(row["submitted_event_count"] == row["exact_evidence_event_count"] for row in rows)
    assert max(row["submitted_event_count"] for row in rows) == 25


def test_v201_quality_fails_but_token_gate_passes(score_bundle):
    _predecessor, result = score_bundle
    assert result["gate"]["passed"] is False
    assert result["audit"]["promotion_passed"] is False
    assert result["audit"]["production_amortized_total_token_ratio"] == 0.236727
    assert result["audit"]["promotion_checks"][
        "production_amortized_total_token_ratio_lte_0_28"
    ] is True
    assert result["gate"]["bootstrap"]["candidate_minus_baseline"] == -0.263555
    assert result["gate"]["bootstrap"]["ci_lower"] == -0.352446


def test_v201_conflict_perfect_upper_bound_still_cannot_pass(score_bundle):
    _predecessor, result = score_bundle
    assert result["audit"]["conflict_recovery_could_change_verdict"] is False
    assert result["audit"]["more_development_alignment_cases_can_change_selection"] is False
    oracle = result["audit"]["conflict_perfect_oracle_gate"]
    assert oracle["semantic_passed"] is False


def test_v201_freeze_is_idempotent_zero_token_and_keeps_holdout_closed(tmp_path: Path):
    root = tmp_path / "v201"
    first = freeze_v201(output_dir=root)
    second = freeze_v201(output_dir=root)
    assert first["terminal"] == second["terminal"]
    assert first["terminal"]["state"] == "inactive"
    assert first["terminal"]["development_quality_passed"] is False
    assert first["terminal"]["production_amortized_token_target_passed"] is True
    assert first["terminal"]["conflict_recovery_could_change_verdict"] is False
    assert first["terminal"]["holdout_authorized"] is False
    assert first["terminal"]["usage"]["total_tokens"] == 0
    assert first["spec"]["semantic_model_calls_started"] == 0
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
