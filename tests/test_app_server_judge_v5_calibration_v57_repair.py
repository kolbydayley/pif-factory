from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pytest

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v56_structured import (
    DEFAULT_OUTPUT_ROOT as V56_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v57_repair import (
    JudgeV5CalibrationV57RepairError,
    build_v57_input,
    build_v57_taxonomy,
    build_v57_truth,
    freeze_v57_repair,
    score_v57,
    validate_v55_output,
    _validate_predecessor,
)


def _source() -> tuple[dict, dict, dict]:
    value = json.loads((V56_ROOT / "structured-checklist-input-full.private.json").read_text())
    output = json.loads((V56_ROOT / "structured-checklist-output-full.private.json").read_text())
    truth = json.loads((V56_ROOT / "diagnostic-truth.private.json").read_text())
    return value, output, truth


def test_v57_taxonomy_and_correction_floor_are_frozen() -> None:
    _value, output, truth = _source()
    taxonomy = build_v57_taxonomy(output, truth)
    assert taxonomy["failed_witness_count"] == 16
    assert taxonomy["selected_unit_count"] == 18
    assert len(taxonomy["controls"]) == 2
    assert taxonomy["current_counts"] == {
        "field_true_positive": 46,
        "field_false_positive": 22,
        "field_false_negative": 1,
        "verdict_correct": 31,
        "checklist_cells_correct": 517,
        "checklist_cells_total": 540,
        "field_f1": 0.8,
    }
    assert taxonomy["minimum_corrections_to_frozen_gates"] == {
        "field_decisions": 19,
        "structured_verdicts": 4,
        "checklist_cells": 18,
        "attribution_false_positives": 8,
    }


def test_v57_truth_projection_can_score_a_perfect_output() -> None:
    value, prior_output, full_truth = _source()
    taxonomy = build_v57_taxonomy(prior_output, full_truth)
    diagnostic_input = build_v57_input(value, prior_output, taxonomy)
    truth = build_v57_truth(full_truth, taxonomy)
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    output = {"units": []}
    for row in diagnostic_input["units"]:
        key = (row["case_id"], row["witness_id"])
        checklist = [
            {
                "field": field,
                "decision": "different" if field in expected[key] else "same",
                "source_evidence_spans": [],
                "rationale": "Synthetic fixture decision.",
            }
            for field in CHECKLIST_FIELDS
        ]
        output["units"].append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "structured_field_verdict": "incorrect" if expected[key] else "correct",
                "checklist": checklist,
            }
        )
    assert validate_v55_output(output, diagnostic_input) == []
    assert score_v57(output, truth, taxonomy)["passed"] is True


def test_v57_freeze_is_idempotent_and_presemantic() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v57"
        frozen = freeze_v57_repair(output_dir=root)
        again = freeze_v57_repair(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["turn_plan"] == [
            "root_projection_shard_00",
            "root_projection_shard_01",
        ]
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["full_calibration_authorized"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert not list(root.glob("turns/*/capacity.json"))
        assert not list(root.glob("turns/*/sidecar.json"))


def test_v57_rejects_mutated_predecessor_semantic_artifact() -> None:
    with tempfile.TemporaryDirectory() as temp:
        copied = Path(temp) / "v56"
        shutil.copytree(V56_ROOT, copied)
        target = copied / "turns/structured-checklist-shard-02/sanitized-output.private.json"
        target.write_bytes(target.read_bytes() + b"\n")
        with pytest.raises(JudgeV5CalibrationV57RepairError):
            _validate_predecessor(copied)
