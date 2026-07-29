from __future__ import annotations

from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v162_layer_corrected_field_adjudication as v162
from research_factory.app_server_judge_v5_calibration_v163_fresh_corrected_field_diagnostic import (
    TURN_NAMES,
    _validate_v162,
    build_v163_inputs,
    freeze_v163,
    score_v163,
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
            for task_id in data["model_ids"]
        ]
    }


def test_v163_preserves_frozen_v162_reference_and_accounting():
    source = _validate_v162()
    assert source["values"]["terminal"]["state"] == "completed"
    assert source["values"]["terminal"]["reference_frozen"] is True
    assert source["values"]["terminal"]["usage"]["total_tokens"] == 129894
    assert source["values"]["terminal"]["cumulative_calibration_usage"][
        "total_tokens"
    ] == 2676438
    assert len(source["sidecars"]) == 6


def test_v163_splits_eleven_fields_from_one_support_projection():
    source = _validate_v162()
    data = build_v163_inputs(source)
    truth = {row["task_id"]: row for row in data["truth"]["field_tasks"]}
    assert len(data["turns"]) == 11
    assert len(data["selected_ids"]) == 12
    assert truth[data["support_id"]]["field"] == "unsupported_inference"
    assert data["support_id"] not in data["model_ids"]
    assert data["support_projection"] == truth[data["support_id"]]["expected_status"]


def test_v163_inputs_are_blinded():
    source = _validate_v162()
    data = build_v163_inputs(source)
    for row in data["turns"]:
        rendered = str(row["value"])
        assert "expected_status" not in rendered
        assert "truth" not in rendered.lower()


def test_v163_perfect_output_clears_field_gate():
    source = _validate_v162()
    data = build_v163_inputs(source)
    score = score_v163(field_output=_perfect_output(data), data=data)
    assert score["passed"] is True
    assert score["failed_checks"] == []
    assert score["field_protocol_frozen"] is True
    assert score["bounded_alignment_verifier_diagnostic_authorized"] is True


def test_v163_one_residual_miss_is_noninferior_but_control_miss_fails():
    source = _validate_v162()
    data = build_v163_inputs(source)
    output = _perfect_output(data)
    residual = next(key for key in data["residual_ids"] if key in data["model_ids"])
    row = next(item for item in output["decisions"] if item["task_id"] == residual)
    row["field_status"] = "incorrect" if row["field_status"] == "correct" else "correct"
    assert score_v163(field_output=output, data=data)["passed"] is True
    output = _perfect_output(data)
    control = next(key for key in data["control_ids"] if key in data["model_ids"])
    row = next(item for item in output["decisions"] if item["task_id"] == control)
    row["field_status"] = "incorrect" if row["field_status"] == "correct" else "correct"
    score = score_v163(field_output=output, data=data)
    assert score["passed"] is False
    assert "field_control_exact_count" in score["failed_checks"]


def test_v163_freeze_is_idempotent_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v163"
    first = freeze_v163(output_dir=root)
    second = freeze_v163(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["maximum_turn_count"] == 11
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v163_freeze_does_not_mutate_v162(tmp_path: Path):
    root = v162.DEFAULT_OUTPUT_ROOT
    paths = [root / "terminal.json", root / "fixture-reference-v14-v162.private.json"]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v163(output_dir=tmp_path / "v163")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
