from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import (
    app_server_judge_v5_selection_v226_alignment_nonacceptance as v226,
)


def test_v226_validates_measured_quality_failure_and_token_pass():
    predecessor = v226._validate_v225_nonacceptance()
    score = predecessor["score"]
    assert score["passed"] is False
    assert score["metrics"][
        "development_supported_event_semantic_macro_f1"
    ] == 0.742424
    assert score["metrics"]["production_amortized_total_token_ratio"] == 0.164877
    assert predecessor["terminal"]["usage"]["total_tokens"] == 186787


def test_v226_proves_canary_adds_no_hidden_reference_matches():
    predecessor = v226._validate_v225_nonacceptance()
    audit = v226._permutation_audit(predecessor)
    assert audit["base_represented_reference_witness_count"] == 16
    assert audit["canary_represented_reference_witness_count"] == 14
    assert audit["union_represented_reference_witness_count"] == 16
    assert audit["additional_reference_witnesses_hidden_by_permutation"] == 0
    assert audit["canary_is_strict_subset_of_base"] is True


def test_v226_binds_exhausted_fixed_strategy_space():
    strategy = v226._validate_v219_strategy_space()["report"]
    assert strategy["fixed_bundle_count"] == 26
    assert strategy["fixed_quality_pass_count_under_perfect_support_selector"] == 0
    assert strategy["fixed_path_feasible"] is False
    assert strategy["adaptive_external_router_plus_selector_feasible"] is False


def test_v226_freezes_zero_token_nonacceptance_and_keeps_holdout_closed(
    tmp_path: Path,
):
    root = tmp_path / "v226"
    first = v226.freeze_v226(output_dir=root)
    second = v226.freeze_v226(output_dir=root)
    assert first == second
    assert first["state"] == "development_strategy_not_accepted"
    assert first["terminal_classification"] == "inactive_incomplete_recovery_required"
    assert first["overall_evaluation_complete"] is False
    assert first["development_quality_passed"] is False
    assert first["development_winner_frozen"] is False
    assert first["production_amortized_token_target_passed"] is True
    assert first["production_amortized_total_token_ratio"] == 0.164877
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["usage"]["total_tokens"] == 0
    report = json.loads(
        (root / "development-nonacceptance-report.json").read_text()
    )
    assert report["viable_systems_meeting_joint_gates"] == []
    assert report[
        "more_current_strategy_development_cases_can_change_verdict"
    ] is False
    assert report["remaining_production_amortized_token_headroom_tokens"] == 1158762
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))


def test_v226_rejects_nonempty_unfinished_root(tmp_path: Path):
    root = tmp_path / "v226"
    root.mkdir()
    (root / "partial.json").write_text("{}", encoding="utf-8")
    with pytest.raises(v226.JudgeV5SelectionV226Error):
        v226.freeze_v226(output_dir=root)
