from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v61_adjudication import (
    DEFAULT_OUTPUT_ROOT as V61_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v62_root_status import (
    build_v62_cohort,
    freeze_v62,
    project_v62_contract,
    score_v62,
    validate_v62_output,
)


def _cohort() -> tuple[dict, dict, dict]:
    spec = json.loads((V61_ROOT / "adjudication-spec.json").read_text())
    v60 = Path(spec["predecessor"]["v60_terminal"]["path"]).parent
    base = {
        "units": [
            row
            for index in range(2)
            for row in json.loads(
                (v60 / "turns" / f"root-projection-shard-{index:02d}" / "input.private.json").read_text()
            )["units"]
        ]
    }
    output = json.loads((V61_ROOT / "merged-output.private.json").read_text())
    truth = json.loads((V61_ROOT / "diagnostic-truth.private.json").read_text())
    return build_v62_cohort(base, output, truth)


def test_v62_cohort_is_eight_failures_plus_two_controls() -> None:
    value, truth, roles = _cohort()
    assert len(value["units"]) == 10
    assert sum(row["role"] == "failure" for row in roles["units"]) == 8
    assert sum(row["role"] == "control" for row in roles["units"]) == 2
    assert truth["witness_count"] == 10
    assert value["side_labels_present"] is False


def test_v62_perfect_root_status_output_passes() -> None:
    value, truth, roles = _cohort()
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
                        "rationale": "Synthetic independent-root decision.",
                    }
                    for field in CHECKLIST_FIELDS
                ],
            }
        )
    assert validate_v62_output(output, value) == []
    assert score_v62(output, truth, roles)["passed"] is True


def test_v62_contract_projection_uses_frozen_support_receipt() -> None:
    value, _truth, _roles = _cohort()
    unsupported = next(
        unit for unit in value["units"] if unit["frozen_proposition_verdict"] == "unsupported"
    )
    output = {"units": []}
    for unit in value["units"]:
        output["units"].append(
            {
                "case_id": unit["case_id"],
                "witness_id": unit["witness_id"],
                "structured_field_verdict": "correct",
                "checklist": [
                    {
                        "field": field,
                        "value_relation": "same",
                        "independent_root_status": "not_different",
                        "dependency_fields": [],
                        "source_evidence_spans": [],
                        "rationale": "Synthetic decision.",
                    }
                    for field in CHECKLIST_FIELDS
                ],
            }
        )
    projected, operations = project_v62_contract(output, value)
    row = next(
        row
        for row in projected["units"]
        if row["case_id"] == unsupported["case_id"]
        and row["witness_id"] == unsupported["witness_id"]
    )
    field = next(item for item in row["checklist"] if item["field"] == "unsupported_inference")
    assert (field["value_relation"], field["independent_root_status"]) == ("different", "root")
    assert any(op["operation_type"] == "project_frozen_support_receipt" for op in operations)


def test_v62_freeze_is_one_presemantic_turn() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v62"
        frozen = freeze_v62(output_dir=root)
        again = freeze_v62(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == "gpt-5.5"
        assert frozen["spec"]["failure_unit_count"] == 8
        assert frozen["spec"]["control_unit_count"] == 2
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert not list(root.glob("turns/*/capacity.json"))
        assert not list(root.glob("turns/*/sidecar.json"))
