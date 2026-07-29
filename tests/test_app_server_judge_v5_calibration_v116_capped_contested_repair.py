from __future__ import annotations

from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v116_capped_contested_repair import (
    _trigger_map,
    _validate_v114_controls,
    _validate_v115,
    build_v116_inputs,
    freeze_v116,
    reconcile_v115,
    score_v116,
)
from research_factory.app_server_judge_v5_calibration_v115_full_contested_field_owner import (
    build_reference_candidate,
)


def test_v116_selects_exactly_nine_unique_observable_repairs_and_four_controls():
    v115 = _validate_v115()
    triggers = _trigger_map(v115)
    value, truth, selection = build_v116_inputs(v115, _validate_v114_controls())

    assert len(triggers) == 9
    assert len(value["tasks"]) == 13
    assert selection["repair_task_count"] == 9
    assert selection["repair_limit"] == 12
    assert selection["matched_control_count"] == 4
    assert selection["trigger_reason_counts"] == {
        "canary_disagreement": 3,
        "primary_abstention": 5,
        "unsupported_consistency": 1,
    }
    assert selection["trigger_field_counts"] == {
        "attribution": 5,
        "event_boundary": 1,
        "evidence": 1,
        "stance": 1,
        "unsupported_inference": 1,
    }
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False
    assert selection["majority_voting_used"] is False
    assert sum(row["role"] == "observable_repair" for row in truth["tasks"]) == 9


def _output_for(truth):
    decisions = []
    for row in truth["tasks"]:
        status = row.get("control_expected_status") or "correct"
        if row["role"] == "observable_repair" and row["field"] == "unsupported_inference":
            status = {
                "supported": "correct",
                "unsupported": "incorrect",
                "abstain": "abstain",
            }[row["proposition_status"]]
        decisions.append(
            {
                "task_id": row["task_id"],
                "field_status": status,
                "source_evidence_spans": ["evidence"],
                "rationale": "synthetic test decision",
            }
        )
    return {"decisions": decisions}


def test_v116_score_requires_controls_all_repairs_evidence_and_support_consistency():
    value, truth, _ = build_v116_inputs(_validate_v115(), _validate_v114_controls())
    output = _output_for(truth)
    passed = score_v116(output, truth)
    assert passed["passed"] is True
    assert passed["pointwise_reference_patch_authorized"] is True

    repair_id = next(row["task_id"] for row in truth["tasks"] if row["role"] == "observable_repair")
    next(row for row in output["decisions"] if row["task_id"] == repair_id)["field_status"] = "abstain"
    failed = score_v116(output, truth)
    assert failed["passed"] is False
    assert failed["checks"]["repair_abstention_count"] is False


def test_v116_freeze_is_idempotent_presemantic_and_keeps_later_gates_closed(tmp_path: Path):
    root = tmp_path / "v116"
    first = freeze_v116(output_dir=root)
    second = freeze_v116(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-terra"
    assert first["spec"]["repair_task_count"] == 9
    assert first["spec"]["repair_limit"] == 12
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["pointwise_reference_patch_authorized"] is False
    assert first["spec"]["alignment_reference_frozen"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list(root.glob("turns/*/capacity.json"))
    assert not list(root.glob("turns/*/sidecar.json"))
    assert not (root / "terminal.json").exists()


def test_v116_capacity_policy_is_one_turn_and_preserves_reserve(tmp_path: Path):
    import json

    frozen = freeze_v116(output_dir=tmp_path / "v116")
    policy = json.loads(Path(frozen["capacity_policy"]).read_text())
    assert policy["ordered_turn_names"] == ["capped_contested_field_repair"]
    assert policy["phase_total_token_bound"] == 70000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0


def test_v116_reference_candidate_applies_repairs_to_full_v109_reference():
    v115 = _validate_v115()
    _, truth, _ = build_v116_inputs(v115, _validate_v114_controls())
    output = _output_for(truth)
    reconciled = reconcile_v115(
        v115=v115,
        repair_truth=truth,
        repair_output=output,
    )
    candidate = build_reference_candidate(
        current_truth=v115["values"]["current_reference"],
        truth=v115["values"]["truth"],
        primary=reconciled,
    )

    assert len(candidate["cases"]) == 18
    assert sum(len(case["proposition"]) for case in candidate["cases"].values()) == 53
    assert candidate["pointwise_reference_patch_authorized"] is True
