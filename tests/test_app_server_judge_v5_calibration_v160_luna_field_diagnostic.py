from __future__ import annotations

from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v159_replacement_model_diagnostic as v159
from research_factory.app_server_judge_v5_calibration_v160_luna_field_diagnostic import (
    TURN_NAMES,
    _validate_v159,
    build_v160_inputs,
    freeze_v160,
    score_v160,
)


def _perfect_output(data: dict) -> dict:
    truth = {row["task_id"]: row for row in data["truth"]["field_tasks"]}
    return {
        "decisions": [
            {
                "task_id": task_id,
                "field_status": truth[task_id]["expected_status"],
                "source_evidence_spans": ["fixture"],
                "rationale": "fixture",
            }
            for task_id in data["selected_field_ids"]
        ]
    }


def test_v160_preserves_complete_v159_quality_terminal():
    source = _validate_v159()
    assert source["values"]["terminal"]["state"] == "inactive"
    assert source["values"]["terminal"]["usage_status"] == "complete"
    assert source["values"]["terminal"]["usage"]["total_tokens"] == 419537
    assert source["values"]["score"]["passed"] is False
    assert len(source["sidecars"]) == 16


def test_v160_reuses_exact_field_inputs_without_truth_labels():
    source = _validate_v159()
    data = build_v160_inputs(source)
    assert len(data["turns"]) == 12
    assert len(set(data["selected_field_ids"])) == 12
    for row in data["turns"]:
        rendered = str(row["value"])
        assert "expected_status" not in rendered
        assert "truth" not in rendered.lower()


def test_v160_perfect_field_output_clears_frozen_diagnostic_gate():
    source = _validate_v159()
    data = build_v160_inputs(source)
    score = score_v160(field_output=_perfect_output(data), data=data, source=source)
    assert score["passed"] is True
    assert score["failed_checks"] == []
    assert score["bounded_alignment_verifier_diagnostic_authorized"] is True
    assert score["fresh_full_replacement_calibration_authorized"] is False


def test_v160_one_control_regression_fails_closed():
    source = _validate_v159()
    data = build_v160_inputs(source)
    output = _perfect_output(data)
    control = source["source"]["values"]["diagnostic"]["field_control_task_ids"][0]
    row = next(item for item in output["decisions"] if item["task_id"] == control)
    row["field_status"] = "incorrect" if row["field_status"] == "correct" else "correct"
    score = score_v160(field_output=output, data=data, source=source)
    assert score["passed"] is False
    assert "field_control_exact_count" in score["failed_checks"]


def test_v160_freeze_is_idempotent_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v160"
    first = freeze_v160(output_dir=root)
    second = freeze_v160(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["maximum_turn_count"] == 12
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v160_freeze_does_not_mutate_v159(tmp_path: Path):
    root = v159.DEFAULT_OUTPUT_ROOT
    paths = [root / "terminal.json", root / "replacement-model-diagnostic-score.json"]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v160(output_dir=tmp_path / "v160")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
