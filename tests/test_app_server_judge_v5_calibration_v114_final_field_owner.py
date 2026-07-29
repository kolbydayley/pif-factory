from __future__ import annotations

from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v114_final_field_owner import (
    _validate_v113,
    build_v114_inputs,
    freeze_v114,
    score_v114,
)


def test_v114_preserves_four_controls_and_treats_both_prior_failures_as_disputes():
    value, truth, selection = build_v114_inputs(_validate_v113())

    assert len(value["tasks"]) == 6
    assert selection["role_counts"] == {
        "matched_control": 4,
        "fixture_truth_dispute": 1,
        "permutation_dispute": 1,
    }
    assert selection["final_owner_accepts_either_nonabstain_dispute_status"] is True
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False
    assert selection["majority_voting_used"] is False
    assert sorted(row["field"] for row in truth["tasks"] if row["role"] != "matched_control") == [
        "evidence",
        "unsupported_inference",
    ]


def _output_for(truth, *, fixture="correct", permutation="incorrect"):
    decisions = []
    for row in truth["tasks"]:
        status = row["control_expected_status"]
        if row["role"] == "failed_settled_control":
            status = fixture
        elif row["role"] == "observable_permutation_disagreement":
            status = permutation
        decisions.append(
            {
                "task_id": row["task_id"],
                "field_status": status,
                "source_evidence_spans": ["evidence"],
                "rationale": "synthetic test decision",
            }
        )
    return {"decisions": decisions}


def test_v114_accepts_either_nonabstain_dispute_verdict_after_controls_pass():
    _, truth, _ = build_v114_inputs(_validate_v113())
    first = score_v114(_output_for(truth, fixture="correct", permutation="incorrect"), truth)
    second = score_v114(_output_for(truth, fixture="incorrect", permutation="correct"), truth)
    assert first["passed"] is True
    assert second["passed"] is True
    assert first["contested_field_reference_owner_authorized"] is True

    failed = score_v114(_output_for(truth, fixture="abstain"), truth)
    assert failed["passed"] is False
    assert failed["checks"]["fixture_truth_dispute_decided"] is False


def test_v114_freeze_is_idempotent_presemantic_and_keeps_later_gates_closed(tmp_path: Path):
    root = tmp_path / "v114"
    first = freeze_v114(output_dir=root)
    second = freeze_v114(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-terra"
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["reference_patch_authorized"] is False
    assert first["spec"]["contested_field_reference_owner_authorized"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list(root.glob("turns/*/capacity.json"))
    assert not list(root.glob("turns/*/sidecar.json"))
    assert not (root / "terminal.json").exists()


def test_v114_capacity_policy_is_one_turn_and_preserves_reserve(tmp_path: Path):
    import json

    frozen = freeze_v114(output_dir=tmp_path / "v114")
    policy = json.loads(Path(frozen["capacity_policy"]).read_text())
    assert policy["ordered_turn_names"] == ["final_side_free_field_owner"]
    assert policy["phase_total_token_bound"] == 70000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
