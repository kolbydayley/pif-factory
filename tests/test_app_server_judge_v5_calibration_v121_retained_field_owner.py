from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v121_retained_field_owner import (
    _validate_v106,
    _validate_v120,
    build_v121_inputs,
    freeze_v121,
    score_v121,
)
from research_factory.app_server_judge_v5_calibration_v120_retained_case_diagnostic import (
    _validate_v119,
)


def test_v121_selects_95_disputed_fields_20_controls_and_12_canaries():
    value, truth, canary, selection = build_v121_inputs(
        _validate_v119(), _validate_v120(), _validate_v106()
    )
    assert len(value["tasks"]) == 115
    assert truth["contested_task_count"] == 95
    assert truth["matched_control_count"] == 20
    assert len(canary["tasks"]) == 12
    assert selection["contested_role_counts"] == {
        "consensus_reference_dispute": 30,
        "model_disagreement": 65,
    }
    assert selection["source_field_decision_count"] == 780
    assert selection["unanimous_decision_count"] == 685
    assert selection["prior_labels_in_model_input"] is False
    assert selection["prior_model_decisions_in_model_input"] is False
    assert selection["majority_voting_used"] is False
    assert value["system_identity_present"] is False


def _passing_output(truth):
    decisions = []
    for row in truth["tasks"]:
        status = row["control_expected_status"] or row["current_status"]
        if row["field"] == "unsupported_inference":
            status = {"supported": "correct", "unsupported": "incorrect", "abstain": "abstain"}[
                row["proposition_status"]
            ]
        decisions.append(
            {
                "task_id": row["task_id"],
                "field_status": status,
                "source_evidence_spans": ["evidence"],
                "rationale": "synthetic",
            }
        )
    return {"decisions": decisions}


def test_v121_score_requires_controls_canaries_and_zero_abstentions():
    _, truth, _, _ = build_v121_inputs(_validate_v119(), _validate_v120(), _validate_v106())
    primary = _passing_output(truth)
    source = {row["task_id"]: row for row in primary["decisions"]}
    canary = {
        "decisions": [
            {
                **source[mapping["owner_task_id"]],
                "task_id": mapping["canary_task_id"],
            }
            for mapping in truth["canary_map"]
        ]
    }
    passed = score_v121(primary, canary, truth)
    assert passed["passed"] is True
    assert passed["retained_field_reference_patch_authorized"] is True

    canary["decisions"][0]["field_status"] = "abstain"
    failed = score_v121(primary, canary, truth)
    assert failed["passed"] is False
    assert failed["capped_repair_authorized"] is True


def test_v121_freeze_is_idempotent_presemantic_and_later_gates_closed(tmp_path: Path):
    root = tmp_path / "v121"
    first = freeze_v121(output_dir=root)
    second = freeze_v121(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-terra"
    assert first["spec"]["task_count"] == 115
    assert first["spec"]["turn_plan"][-1] == "retained_field_owner_canary"
    assert len(first["spec"]["turn_plan"]) == 6
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["fresh_diagnostic_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not (root / "terminal.json").exists()


def test_v121_capacity_policy_bounds_six_turns(tmp_path: Path):
    frozen = freeze_v121(output_dir=tmp_path / "v121")
    policy = json.loads(Path(frozen["capacity_policy"]).read_text())
    assert len(policy["ordered_turn_names"]) == 6
    assert policy["phase_total_token_bound"] == 420000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
