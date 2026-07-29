from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v125_final_field_owner import (
    _validate_v124,
    build_reference_candidate_v125,
    build_v125_inputs,
    freeze_v125,
    reconcile_v125,
    score_v125,
)


def _passing_primary(truth):
    return {
        "decisions": [
            {
                "task_id": row["task_id"],
                "field_status": row.get("control_expected_status") or "correct",
                "source_evidence_spans": ["evidence"],
                "rationale": "synthetic decisive decision",
            }
            for row in truth["tasks"]
        ]
    }


def test_v125_selects_six_disputes_six_distinct_controls_and_six_canaries():
    value, truth, canary, selection = build_v125_inputs(_validate_v124())
    assert value["task_count"] == 12
    assert truth["final_owner_dispute_count"] == 6
    assert truth["matched_control_count"] == 6
    assert canary["task_count"] == 6
    assert selection["permutation_canary_count"] == 6
    assert selection["control_field_count"] == 6
    assert selection["prior_labels_in_model_input"] is False
    assert selection["prior_model_decisions_in_model_input"] is False
    assert selection["dispute_reasons_in_model_input"] is False
    assert selection["final_owner_is_decisive"] is True
    assert selection["majority_voting_used"] is False
    assert value["system_identity_present"] is False


def test_v125_score_requires_all_controls_canaries_evidence_and_no_abstentions():
    _, truth, _, _ = build_v125_inputs(_validate_v124())
    primary = _passing_primary(truth)
    owner = {row["task_id"]: row for row in primary["decisions"]}
    canary = {
        "decisions": [
            {
                **owner[mapping["owner_task_id"]],
                "task_id": mapping["canary_task_id"],
            }
            for mapping in truth["canary_map"]
        ]
    }
    passed = score_v125(primary, canary, truth)
    assert passed["passed"] is True
    assert passed["retained_field_reference_patch_authorized"] is True

    canary["decisions"][0]["field_status"] = "abstain"
    failed = score_v125(primary, canary, truth)
    assert failed["passed"] is False
    assert failed["checks"]["permutation_canary_exact_rate"] is False
    assert failed["checks"]["permutation_canary_abstention_count"] is False


def test_v125_reference_candidate_applies_all_contested_and_two_control_disputes():
    predecessor = _validate_v124()
    _, truth, _, _ = build_v125_inputs(predecessor)
    primary = _passing_primary(truth)
    reconciled = reconcile_v125(
        base_primary=predecessor["v122"]["values"]["primary"],
        truth=truth,
        owner=primary,
    )
    candidate = build_reference_candidate_v125(
        current_reference=__import__(
            "research_factory.app_server_judge_v5_calibration_v125_final_field_owner",
            fromlist=["_validate_v119"],
        )._validate_v119()["values"]["truth"],
        v121_truth=predecessor["v122"]["v121"]["values"]["truth"],
        reconciled=reconciled,
        owner_truth=truth,
    )
    assert len(candidate["cases"]) == 66
    assert candidate["retained_field_final_owner_case_count"] == 6
    assert candidate["retained_field_reference_patch_authorized"] is True
    assert candidate["proposition_reference_frozen"] is False
    assert candidate["alignment_reference_frozen"] is False


def test_v125_freeze_is_idempotent_presemantic_and_keeps_later_gates_closed(
    tmp_path: Path,
):
    root = tmp_path / "v125"
    first = freeze_v125(output_dir=root)
    second = freeze_v125(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-sol"
    assert first["spec"]["turn_plan"] == [
        "final_retained_field_owner",
        "final_retained_field_owner_canary",
    ]
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["retained_field_reference_patch_authorized"] is False
    assert first["spec"]["proposition_reference_frozen"] is False
    assert first["spec"]["alignment_reference_frozen"] is False
    assert first["spec"]["fresh_diagnostic_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list(root.glob("turns/*/capacity.json"))
    assert not list(root.glob("turns/*/sidecar.json"))
    assert not (root / "terminal.json").exists()

    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["ordered_turn_names"] == first["spec"]["turn_plan"]
    assert policy["phase_total_token_bound"] == 140000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
