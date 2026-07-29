from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v161_residual_field_reference_owner as v161
from research_factory.app_server_judge_v5_calibration_v162_layer_corrected_field_adjudication import (
    TURN_NAMES,
    _validate_v161,
    build_v162_inputs,
    freeze_v162,
    patch_truth_and_reference,
    retrospective_rescore,
    score_v162,
)


def _outputs(data: dict) -> dict:
    truth = {row["task_id"]: row for row in data["truth"]["field_tasks"]}
    result = {}
    for turn in data["turns"]:
        task_id = turn["task_id"]
        status = truth[task_id]["expected_status"]
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


def test_v162_preserves_complete_v161_quality_terminal():
    source = _validate_v161()
    assert source["values"]["terminal"]["state"] == "inactive"
    assert source["values"]["terminal"]["usage_status"] == "complete"
    assert source["values"]["terminal"]["usage"]["total_tokens"] == 380122
    assert source["values"]["score"]["failed_checks"] == [
        "control_exact_rate",
        "repeat_exact_rate",
    ]
    assert len(source["sidecars"]) == 18


def test_v162_corrects_unsupported_inference_to_support_layer():
    source = _validate_v161()
    data = build_v162_inputs(source)
    truth = {row["task_id"]: row for row in data["truth"]["field_tasks"]}
    assert truth[data["support_control_id"]]["field"] == "unsupported_inference"
    assert data["support_projection"] == truth[data["support_control_id"]][
        "expected_status"
    ]
    assert data["support_control_id"] not in {row["task_id"] for row in data["turns"]}


def test_v162_sends_one_target_and_five_blinded_controls():
    source = _validate_v161()
    data = build_v162_inputs(source)
    assert len(data["turns"]) == 6
    assert len(data["control_ids"]) == 5
    assert sum(row["task_id"] == data["target_id"] for row in data["turns"]) == 1
    for row in data["turns"]:
        rendered = str(row["value"])
        assert "expected_status" not in rendered
        assert "truth" not in rendered.lower()


def test_v162_clean_adjudication_authorizes_reference_patch():
    source = _validate_v161()
    data = build_v162_inputs(source)
    score = score_v162(outputs=_outputs(data), data=data)
    assert score["passed"] is True
    assert score["metrics"]["control_exact_count"] == 5
    assert score["metrics"]["support_projection_exact_count"] == 1
    assert score["fresh_field_diagnostic_authorized"] is True


def test_v162_control_regression_fails_closed():
    source = _validate_v161()
    data = build_v162_inputs(source)
    outputs = _outputs(data)
    control = data["control_ids"][0]
    turn = next(row for row in data["turns"] if row["task_id"] == control)
    decision = outputs[turn["turn_name"]]["decisions"][0]
    decision["field_status"] = (
        "incorrect" if decision["field_status"] == "correct" else "correct"
    )
    score = score_v162(outputs=outputs, data=data)
    assert score["passed"] is False
    assert score["failed_checks"] == ["control_exact_rate"]


def test_v162_patch_changes_only_six_residual_fields_and_rescores():
    source = _validate_v161()
    data = build_v162_inputs(source)
    outputs = _outputs(data)
    before = {row["task_id"]: row["expected_status"] for row in data["truth"]["field_tasks"]}
    truth, reference, changed = patch_truth_and_reference(
        outputs=outputs,
        data=data,
        current_reference=source["source"]["reference"],
    )
    after = {row["task_id"]: row["expected_status"] for row in truth["field_tasks"]}
    assert {key for key in before if before[key] != after[key]} <= set(data["residual_ids"])
    assert changed <= 6
    assert reference["reference_frozen"] is True
    assert reference["unsupported_inference_owned_by_support_projection"] is True
    rescore = retrospective_rescore(patched_truth=truth, data=data)
    assert set(rescore["candidates"]) == {"sol_v159", "luna_v160"}
    assert rescore["fresh_diagnostic_still_required"] is True


def test_v162_freeze_is_idempotent_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v162"
    first = freeze_v162(output_dir=root)
    second = freeze_v162(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["maximum_turn_count"] == 6
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["majority_voting_used"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v162_freeze_does_not_mutate_v161(tmp_path: Path):
    root = v161.DEFAULT_OUTPUT_ROOT
    paths = [root / "terminal.json", root / "residual-field-reference-owner-score.json"]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v162(output_dir=tmp_path / "v162")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
