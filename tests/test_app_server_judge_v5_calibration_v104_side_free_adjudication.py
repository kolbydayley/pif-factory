from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v104_side_free_adjudication import (
    MODEL,
    _validate_predecessor,
    build_v104_input,
    freeze_v104,
    score_v104,
)


def _built():
    predecessor = _validate_predecessor()
    return build_v104_input(
        v103_input=predecessor["values"]["v103_input"],
        disagreement=predecessor["disagreement"],
    )


def _decision(task_id: str, status: str) -> dict:
    return {
        "task_id": task_id,
        "field_status": status,
        "source_evidence_spans": ["evidence"],
        "rationale": "test",
    }


def test_v104_is_side_free_with_one_dispute_and_three_controls():
    value, truth = _built()
    rendered = json.dumps(value, sort_keys=True)

    assert value["task_count"] == 4
    assert len(truth["control_truth"]) == 3
    assert truth["prior_reference_status"] == "incorrect"
    assert "prior_reference_status" not in rendered
    assert "expected_status" not in rendered
    assert "field_status" not in rendered
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False


def test_v104_score_requires_controls_and_returns_owner_proposal():
    _value, truth = _built()
    decisions = [
        _decision(task_id, status) for task_id, status in truth["control_truth"].items()
    ]
    decisions.append(_decision(truth["dispute_task_id"], "incorrect"))
    score = score_v104({"decisions": decisions}, truth)

    assert score["passed"] is True
    assert score["owner_status"] == "incorrect"
    assert score["reference_change_proposed"] is False
    assert score["reconciliation_authorized"] is True

    failed = deepcopy(decisions)
    failed[0]["field_status"] = "incorrect" if failed[0]["field_status"] == "correct" else "correct"
    assert score_v104({"decisions": failed}, truth)["passed"] is False


def test_v104_freeze_is_one_turn_immutable_and_presemantic(tmp_path: Path):
    root = tmp_path / "v104"
    first = freeze_v104(output_dir=root)
    second = freeze_v104(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == MODEL
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["reconciliation_authorized"] is False
    assert first["spec"]["fresh_full_development_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert first["spec"]["turn_plan"] == ["side_free_adjudication"]
    assert not (root / "terminal.json").exists()
