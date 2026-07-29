from __future__ import annotations

import json
from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v140_layered_field_diagnostic as v140
from research_factory.app_server_judge_v5_calibration_v126_singleton_field_owner import (
    _validate_v125,
)
from research_factory.app_server_judge_v5_calibration_v141_luna_field_reference_owner import (
    TURN_NAMES,
    _validate_v140,
    build_v141_inputs,
    freeze_v141,
    patch_reference_v141,
    patch_v140_truth,
    score_v141,
)


def _expected_outputs(truth):
    outputs = {}
    for index, row in enumerate(truth["tasks"]):
        status = row.get("control_expected_status", row.get("v140_status"))
        outputs[f"turn_{index}"] = {
            "decisions": [
                {
                    "task_id": row["task_id"],
                    "field_status": status,
                    "source_evidence_spans": ["evidence"],
                    "rationale": "expected",
                }
            ]
        }
    return outputs


def test_v141_selects_nine_stable_disputes_and_five_same_field_controls():
    rows, truth, selection = build_v141_inputs(_validate_v140(), _validate_v125())

    assert selection["stable_reference_dispute_count"] == 9
    assert selection["dispute_field_counts"] == {
        "event_boundary": 1,
        "evidence": 1,
        "metric": 3,
        "reported_actor": 2,
        "stance": 2,
    }
    assert selection["control_count"] == 5
    assert selection["owner_count"] == 9
    assert selection["maximum_tasks_per_turn"] == 1
    assert selection["prior_labels_in_model_input"] is False
    assert selection["prior_model_decisions_in_model_input"] is False
    assert selection["majority_voting_used"] is False
    assert len(rows) == len(TURN_NAMES) == 14
    assert truth["task_count"] == 14
    assert all(row["value"]["task_count"] == 1 for row in rows)


def test_v141_owner_patch_makes_original_v140_gates_pass_without_relaxation():
    predecessor = _validate_v140()
    _, truth, _ = build_v141_inputs(predecessor, _validate_v125())
    outputs = _expected_outputs(truth)
    owner_score = score_v141(outputs, truth)

    assert owner_score["passed"] is True
    assert owner_score["reference_patch_authorized"] is True
    patched_truth = patch_v140_truth(
        current_truth=predecessor["values"]["truth"],
        owner_truth=truth,
        outputs=outputs,
    )
    patched_reference = patch_reference_v141(
        current_reference=predecessor["v139"]["v138"]["values"]["reference"],
        owner_truth=truth,
        outputs=outputs,
    )
    assert len(patched_reference["cases"]) == 66
    assert patched_reference["v141_owner_task_count"] == 9
    rescored = v140.score_v140(
        support=predecessor["values"]["support"],
        support_canary=predecessor["values"]["support_canary"],
        fields=predecessor["values"]["fields"],
        field_canary=predecessor["values"]["field_canary"],
        truth=patched_truth,
    )
    assert rescored["passed"] is True
    assert rescored["checks"]["field_decision_accuracy"] is True
    assert rescored["checks"]["pointwise_field_issue_f1"] is True


def test_v141_freeze_is_idempotent_presemantic_and_14_turn_bounded(tmp_path: Path):
    root = tmp_path / "v141"
    first = freeze_v141(output_dir=root)
    second = freeze_v141(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-luna"
    assert first["spec"]["reasoning_effort"] == "high"
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["control_count"] == 5
    assert first["spec"]["owner_count"] == 9
    assert first["spec"]["maximum_tasks_per_turn"] == 1
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False

    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 980000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v141_predecessors_are_immutable(tmp_path: Path):
    before_v140 = _validate_v140()["records"]
    before_v121 = _validate_v125()["v124"]["v122"]["v121"]["records"]
    freeze_v141(output_dir=tmp_path / "v141")
    after_v140 = _validate_v140()["records"]
    after_v121 = _validate_v125()["v124"]["v122"]["v121"]["records"]

    assert before_v140 == after_v140
    assert before_v121 == after_v121
