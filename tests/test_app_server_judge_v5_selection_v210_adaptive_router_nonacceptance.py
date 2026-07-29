from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v210_adaptive_router_nonacceptance import (
    EXPECTED_FAILED_CHECKS,
    EXPECTED_USAGE,
    _validate_v209_measured_output_failure,
    freeze_v210,
)


def test_v210_binds_measured_v209_output_without_replay():
    predecessor = _validate_v209_measured_output_failure()
    assert predecessor["terminal"]["usage_status"] == "complete"
    assert predecessor["terminal"]["usage"] == EXPECTED_USAGE
    assert predecessor["sidecar"]["state"] == "completed"
    assert predecessor["sidecar"]["status"] == "completed"
    assert predecessor["sidecar"]["usage_complete"] is True
    assert predecessor["terminal"]["cumulative_unknown_usage_turn_count"] == 3


def test_v210_freezes_quality_and_token_nonacceptance(tmp_path: Path):
    root = tmp_path / "v210"
    first = freeze_v210(output_dir=root)
    second = freeze_v210(output_dir=root)
    report = json.loads((root / "development-nonacceptance-report.json").read_text())
    gate = json.loads((root / "adaptive-router-canary-gate.json").read_text())
    next_strategy = json.loads((root / "next-strategy.json").read_text())
    spec = json.loads((root / "nonacceptance-spec.json").read_text())
    assert first == second
    assert first["state"] == "waiting_for_external_strategy_authorization"
    assert first["evaluation_accepted"] is False
    assert first["semantic_quality_passed"] is False
    assert first["production_amortized_token_target_passed"] is False
    assert first["development_winner_frozen"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert gate["failed_checks"] == EXPECTED_FAILED_CHECKS
    assert gate["candidate_mean_f1"] == 0.734343
    assert gate["mean_f1_regret_to_oracle"] == 0.098198
    assert gate["projected_full_router_total_tokens"] == 100_488
    assert report["viable_systems_meeting_joint_gates"] == []
    assert report["more_development_cases_can_change_current_strategy_verdict"] is False
    assert report["full_quality_ceiling"]["projected_budget_overrun_tokens"] == (
        42_301.633333
    )
    assert next_strategy["state"] == "prepared_not_authorized"
    assert next_strategy["holdout_remains_closed"] is True
    assert spec["semantic_model_calls_started"] == 0
    assert spec["extraction_model_calls_started"] == 0
    assert spec["new_usage_tokens"] == 0


def test_v210_preserves_cumulative_intent_to_treat_accounting(tmp_path: Path):
    terminal = freeze_v210(output_dir=tmp_path / "v210")
    assert terminal["cumulative_known_usage_lower_bound"]["total_tokens"] == 8_931_442
    assert terminal["cumulative_unknown_usage_turn_count"] == 3
    assert terminal["cumulative_conservative_unknown_usage_upper_bound"] == 245_000
    assert terminal["usage"]["total_tokens"] == 0
