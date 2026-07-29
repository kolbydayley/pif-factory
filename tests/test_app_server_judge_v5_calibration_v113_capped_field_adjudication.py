from __future__ import annotations

from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v113_capped_field_adjudication import (
    _validate_v112,
    build_v113_inputs,
    freeze_v113,
    score_v113,
)


def test_v113_selects_exact_observable_failures_and_four_stable_controls():
    value, truth, selection = build_v113_inputs(_validate_v112())

    assert len(value["tasks"]) == 6
    assert selection["role_counts"] == {
        "matched_control": 4,
        "failed_settled_control": 1,
        "observable_permutation_disagreement": 1,
    }
    assert selection["control_status_counts"] == {"correct": 2, "incorrect": 2}
    assert selection["observable_failure_fields"] == ["evidence", "unsupported_inference"]
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False
    assert selection["majority_voting_used"] is False
    assert sorted(
        row["field"]
        for row in truth["tasks"]
        if row["role"] != "matched_control"
    ) == ["evidence", "unsupported_inference"]


def _output_for(truth, *, failed_status="incorrect", unstable_status="correct"):
    decisions = []
    for row in truth["tasks"]:
        status = row["control_expected_status"]
        if row["role"] == "failed_settled_control":
            status = failed_status
        elif row["role"] == "observable_permutation_disagreement":
            status = unstable_status
        decisions.append(
            {
                "task_id": row["task_id"],
                "field_status": status,
                "source_evidence_spans": ["evidence"],
                "rationale": "synthetic test decision",
            }
        )
    return {"decisions": decisions}


def test_v113_score_requires_controls_failed_control_repair_decision_and_evidence():
    _, truth, _ = build_v113_inputs(_validate_v112())
    passed = score_v113(_output_for(truth), truth)
    assert passed["passed"] is True
    assert passed["contested_field_reference_owner_authorized"] is True

    failed = score_v113(_output_for(truth, failed_status="correct"), truth)
    assert failed["passed"] is False
    assert failed["checks"]["failed_control_repaired"] is False


def test_v113_freeze_is_idempotent_presemantic_and_keeps_later_gates_closed(tmp_path: Path):
    root = tmp_path / "v113"
    first = freeze_v113(output_dir=root)
    second = freeze_v113(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.4"
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["contested_field_reference_owner_authorized"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list(root.glob("turns/*/capacity.json"))
    assert not list(root.glob("turns/*/sidecar.json"))
    assert not (root / "terminal.json").exists()


def test_v113_capacity_policy_is_one_turn_and_preserves_reserve(tmp_path: Path):
    import json

    frozen = freeze_v113(output_dir=tmp_path / "v113")
    policy = json.loads(Path(frozen["capacity_policy"]).read_text())
    assert policy["ordered_turn_names"] == ["capped_side_free_field_adjudication"]
    assert policy["phase_total_token_bound"] == 70000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
