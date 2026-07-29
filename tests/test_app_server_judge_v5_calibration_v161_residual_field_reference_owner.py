from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v160_luna_field_diagnostic as v160
from research_factory.app_server_judge_v5_calibration_v161_residual_field_reference_owner import (
    TURN_NAMES,
    _validate_v160,
    build_v161_inputs,
    freeze_v161,
    patch_truth_and_reference,
    score_v161,
)


def _outputs(data: dict, *, controls_exact: bool = True) -> dict:
    truth = {row["task_id"]: row for row in data["truth"]["field_tasks"]}
    primary_status = {}
    result = {}
    for turn in data["turns"]:
        task_id = turn["task_id"]
        status = primary_status.get(task_id)
        if status is None:
            status = truth[task_id]["expected_status"]
            if task_id in data["residual_ids"]:
                status = "incorrect" if status == "correct" else "correct"
            if not controls_exact and task_id in data["control_ids"]:
                status = "incorrect" if status == "correct" else "correct"
            primary_status[task_id] = status
        result[turn["turn_name"]] = {
            "decisions": [
                {
                    "task_id": task_id,
                    "field_status": status,
                    "source_evidence_spans": ["fixture"],
                    "rationale": "fixture",
                }
            ]
        }
    return result


def test_v161_preserves_complete_v160_quality_terminal():
    source = _validate_v160()
    assert source["values"]["terminal"]["state"] == "inactive"
    assert source["values"]["terminal"]["usage_status"] == "complete"
    assert source["values"]["terminal"]["usage"]["total_tokens"] == 239995
    assert len(source["sidecars"]) == 12


def test_v161_inputs_are_blinded_and_repeat_exactly_six_residuals():
    source = _validate_v160()
    data = build_v161_inputs(source)
    assert len(data["turns"]) == 18
    assert len(data["primary_ids"]) == 12
    assert set(data["repeat_ids"]) == set(data["residual_ids"])

    def keys(value):
        if isinstance(value, dict):
            return set(value) | set().union(*(keys(item) for item in value.values()))
        if isinstance(value, list):
            return set().union(*(keys(item) for item in value))
        return set()

    for row in data["turns"]:
        rendered = str(row["value"])
        assert "expected_status" not in rendered
        assert "truth" not in rendered.lower()
        assert not ({"repeat", "is_repeat", "repeat_role", "canary"} & keys(row["value"]))


def test_v161_consistent_owner_and_controls_authorize_reference_patch():
    source = _validate_v160()
    data = build_v161_inputs(source)
    outputs = _outputs(data)
    score = score_v161(outputs=outputs, data=data)
    assert score["passed"] is True
    assert score["metrics"]["repeat_exact_count"] == 6
    assert score["metrics"]["control_exact_count"] == 6
    assert score["fresh_field_diagnostic_authorized"] is True


def test_v161_repeat_disagreement_and_control_regression_fail_closed():
    source = _validate_v160()
    data = build_v161_inputs(source)
    outputs = _outputs(data, controls_exact=False)
    repeated_turn = next(
        row for row in data["turns"] if row["turn_role"] == "reference_repeat"
    )
    decision = outputs[repeated_turn["turn_name"]]["decisions"][0]
    decision["field_status"] = (
        "incorrect" if decision["field_status"] == "correct" else "correct"
    )
    score = score_v161(outputs=outputs, data=data)
    assert score["passed"] is False
    assert set(score["failed_checks"]) == {"control_exact_rate", "repeat_exact_rate"}


def test_v161_patch_changes_only_residual_fields():
    source = _validate_v160()
    data = build_v161_inputs(source)
    outputs = _outputs(data)
    before_truth = deepcopy(data["truth"])
    before_reference = deepcopy(source["reference"])
    truth, reference, changed = patch_truth_and_reference(
        outputs=outputs, data=data, current_reference=source["reference"]
    )
    assert changed == 6
    before = {row["task_id"]: row["expected_status"] for row in before_truth["field_tasks"]}
    after = {row["task_id"]: row["expected_status"] for row in truth["field_tasks"]}
    assert {key for key in before if before[key] != after[key]} == set(data["residual_ids"])
    assert before_reference != reference
    assert reference["reference_frozen"] is True
    assert reference["selection_authorized"] is False


def test_v161_freeze_is_idempotent_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v161"
    first = freeze_v161(output_dir=root)
    second = freeze_v161(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["maximum_turn_count"] == 18
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["majority_voting_used"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v161_freeze_does_not_mutate_v160(tmp_path: Path):
    root = v160.DEFAULT_OUTPUT_ROOT
    paths = [root / "terminal.json", root / "luna-field-diagnostic-score.json"]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v161(output_dir=tmp_path / "v161")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
