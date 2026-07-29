from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v55_structured import (
    DEFAULT_OUTPUT_ROOT as V55_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v64_reference_freeze import (
    DEFAULT_OUTPUT_ROOT as V64_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v65_fresh_structured import (
    build_v65_input,
    build_v65_truth,
    freeze_v65,
    score_v65,
)


def _fixture() -> tuple[dict, dict]:
    value = build_v65_input(
        json.loads((V55_ROOT / "structured-checklist-input-full.private.json").read_text())
    )
    truth = build_v65_truth(
        json.loads((V64_ROOT / "calibration-truth.private.json").read_text()), value
    )
    return value, truth


def test_v65_is_fresh_all_36_without_prior_outputs() -> None:
    value, truth = _fixture()
    assert len(value["units"]) == 36
    assert truth["case_count"] == 12
    assert truth["witness_count"] == 36
    assert value["prior_labels_present"] is False
    assert value["candidate_outputs_present"] is False
    assert value["system_identity_present"] is False
    rendered = json.dumps(value)
    assert "prior_value_differences" not in rendered
    assert "candidate_a" not in rendered


def test_v65_perfect_output_passes_every_gate() -> None:
    value, truth = _fixture()
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    output = {"units": []}
    for unit in value["units"]:
        key = (unit["case_id"], unit["witness_id"])
        output["units"].append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "structured_field_verdict": "incorrect" if expected[key] else "correct",
                "checklist": [
                    {
                        "field": field,
                        "value_relation": "different" if field in expected[key] else "same",
                        "independent_root_status": "root" if field in expected[key] else "not_different",
                        "dependency_fields": [],
                        "source_evidence_spans": [],
                        "rationale": "Synthetic fresh decision.",
                    }
                    for field in CHECKLIST_FIELDS
                ],
            }
        )
    assert score_v65(output, truth)["passed"] is True


def test_v65_freeze_is_three_presemantic_turns() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v65"
        frozen = freeze_v65(output_dir=root)
        again = freeze_v65(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == "gpt-5.5"
        assert frozen["spec"]["units_per_turn"] == 12
        assert len(frozen["spec"]["turn_plan"]) == 3
        assert frozen["spec"]["prior_labels_in_model_input"] is False
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert not list(root.glob("turns/*/capacity.json"))
        assert not list(root.glob("turns/*/sidecar.json"))
