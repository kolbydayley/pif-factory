from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v55_structured import (
    freeze_v55_structured,
    score_v55,
    validate_v55_output,
)


def _perfect_output(value, truth):
    units = []
    for unit in value["units"]:
        case = truth["cases"][unit["case_id"]]
        fields = set(case["field_issues"][unit["witness_id"]])
        units.append(
            {
                "case_id": unit["case_id"],
                "witness_id": unit["witness_id"],
                "structured_field_verdict": case["structured_fields"][unit["witness_id"]],
                "checklist": [
                    {
                        "field": field,
                        "decision": "different" if field in fields else "same",
                        "source_evidence_spans": [],
                        "rationale": "Frozen field truth.",
                    }
                    for field in CHECKLIST_FIELDS
                ],
            }
        )
    return {"units": units}


def test_v55_freeze_and_perfect_score() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v55"
        frozen = freeze_v55_structured(output_dir=root)
        again = freeze_v55_structured(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["witness_count"] == 36
        assert len(frozen["spec"]["turn_plan"]) == 4
        assert not list(root.glob("turns/*/sidecar.json"))
        output = _perfect_output(frozen["input"], frozen["truth"])
        assert validate_v55_output(output, frozen["input"]) == []
        score = score_v55(output, frozen["truth"])
        assert score["passed"] is True
        assert all(score["checks"].values())


def test_v55_validator_enforces_scope_and_support_projection() -> None:
    with tempfile.TemporaryDirectory() as temp:
        frozen = freeze_v55_structured(output_dir=Path(temp) / "v55")
        output = _perfect_output(frozen["input"], frozen["truth"])
        output["units"][0]["checklist"][4]["decision"] = "different"
        errors = validate_v55_output(output, frozen["input"])
        assert "unit_0_event_boundary_scope" in errors
