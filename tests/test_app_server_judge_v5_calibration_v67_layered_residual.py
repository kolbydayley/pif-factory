from __future__ import annotations

import json
import tempfile
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v64_reference_freeze import (
    DEFAULT_OUTPUT_ROOT as V64_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v65_fresh_structured import (
    DEFAULT_OUTPUT_ROOT as V65_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v66_external_blocker import (
    DEFAULT_OUTPUT_ROOT as V66_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v67_layered_residual import (
    POINTWISE_EFFORT,
    POINTWISE_MODEL,
    ROOT_EFFORT,
    ROOT_MODEL,
    TURN_NAMES,
    build_pointwise_prompt,
    build_root_input,
    build_root_prompt,
    build_v67_cohort,
    freeze_pointwise_receipts,
    freeze_v67,
    pointwise_output_schema,
    project_root_contract,
    root_output_schema,
    score_v67,
    validate_pointwise_output,
    validate_root_output,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _cohort() -> tuple[dict, dict, dict]:
    return build_v67_cohort(
        _load(V65_ROOT / "fresh-structured-input.private.json"),
        _load(V65_ROOT / "fresh-structured-output.private.json"),
        _load(V64_ROOT / "calibration-truth.private.json"),
        _load(V66_ROOT / "reference-owner-adjudication.private.json"),
    )


def _pointwise_output(value: dict) -> dict:
    return {
        "units": [
            {
                "case_id": unit["case_id"],
                "witness_id": unit["witness_id"],
                "field_facts": [
                    {
                        "field": field,
                        "field_state": "correct",
                        "source_evidence_spans": [],
                        "rationale": "Independent pointwise fixture decision.",
                    }
                    for field in CHECKLIST_FIELDS
                ],
            }
            for unit in value["units"]
        ]
    }


def _perfect_root_output(value: dict, truth: dict) -> dict:
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    return {
        "units": [
            {
                "case_id": unit["case_id"],
                "witness_id": unit["witness_id"],
                "structured_field_verdict": (
                    "incorrect"
                    if expected[(unit["case_id"], unit["witness_id"])]
                    else "correct"
                ),
                "checklist": [
                    {
                        "field": field,
                        "root_status": (
                            "root"
                            if field
                            in expected[(unit["case_id"], unit["witness_id"])]
                            else "not_root"
                        ),
                        "source_evidence_spans": [],
                        "rationale": "Independent root fixture decision.",
                    }
                    for field in CHECKLIST_FIELDS
                ],
            }
            for unit in value["units"]
        ]
    }


def test_v67_cohort_is_all_ten_residuals_plus_two_label_selected_controls() -> None:
    value, truth, roles = _cohort()
    assert len(value["units"]) == 12
    assert truth["witness_count"] == 12
    assert roles["role_counts"] == {
        "evidence_residual": 6,
        "semantic_residual": 4,
        "empty_control": 1,
        "nonempty_control": 1,
    }
    assert roles["selection_uses_source_text"] is False
    assert value["prior_labels_present"] is False
    assert value["candidate_outputs_present"] is False
    assert all(
        child not in (None, "", [], {})
        for unit in value["units"]
        for child in unit["structured_event"].values()
    )
    assert all(unit["exact_evidence_receipt"] is True for unit in value["units"])


def test_v67_schemas_and_prompts_fit_the_frozen_app_server_caps() -> None:
    value, _, _ = _cohort()
    pointwise = _pointwise_output(value)
    assert validate_pointwise_output(pointwise, value) == []
    receipts = freeze_pointwise_receipts(pointwise, value)
    root_value = build_root_input(value, receipts)
    assert pointwise_output_schema(value)["properties"]["units"]["minItems"] == 12
    assert root_output_schema(root_value)["properties"]["units"]["minItems"] == 12
    assert len(build_pointwise_prompt(value).encode("utf-8")) <= 96 * 1024
    assert len(build_root_prompt(root_value).encode("utf-8")) <= 96 * 1024
    assert len(json.dumps(pointwise_output_schema(value), sort_keys=True).encode("utf-8")) <= 64 * 1024
    assert len(json.dumps(root_output_schema(root_value), sort_keys=True).encode("utf-8")) <= 64 * 1024


def test_v67_projection_uses_only_exact_evidence_and_frozen_support_receipts() -> None:
    value, truth, _ = _cohort()
    receipts = freeze_pointwise_receipts(_pointwise_output(value), value)
    root_value = build_root_input(value, receipts)
    output = _perfect_root_output(root_value, truth)
    for unit in output["units"]:
        for row in unit["checklist"]:
            if row["field"] in {"evidence", "unsupported_inference"}:
                row["root_status"] = "abstain"
        unit["structured_field_verdict"] = "abstain"
    projected, operations = project_root_contract(output, root_value)
    assert validate_root_output(projected, root_value) == []
    assert {row["operation_type"] for row in operations} == {
        "project_exact_evidence_receipt",
        "project_frozen_support_receipt",
    }
    for unit in projected["units"]:
        source = next(
            row
            for row in root_value["units"]
            if (row["case_id"], row["witness_id"])
            == (unit["case_id"], unit["witness_id"])
        )
        rows = {row["field"]: row for row in unit["checklist"]}
        assert rows["evidence"]["root_status"] == "not_root"
        assert rows["unsupported_inference"]["root_status"] == {
            "supported": "not_root",
            "unsupported": "root",
            "abstain": "abstain",
        }[source["frozen_proposition_verdict"]]


def test_v67_perfect_output_is_required_for_promotion() -> None:
    value, truth, roles = _cohort()
    receipts = freeze_pointwise_receipts(_pointwise_output(value), value)
    root_value = build_root_input(value, receipts)
    perfect = _perfect_root_output(root_value, truth)
    score = score_v67(perfect, truth, roles)
    assert score["passed"] is True
    assert all(score["checks"].values())
    changed = deepcopy(perfect)
    changed["units"][0]["checklist"][0]["root_status"] = (
        "not_root"
        if changed["units"][0]["checklist"][0]["root_status"] == "root"
        else "root"
    )
    assert score_v67(changed, truth, roles)["passed"] is False


def test_v67_freeze_is_presemantic_and_pins_two_distinct_model_stages() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v67"
        frozen = freeze_v67(output_dir=root)
        again = freeze_v67(output_dir=root)
        assert frozen["spec"] == again["spec"]
        assert frozen["spec"]["turn_plan"] == [
            {"turn_name": TURN_NAMES[0], "model": POINTWISE_MODEL, "effort": POINTWISE_EFFORT},
            {"turn_name": TURN_NAMES[1], "model": ROOT_MODEL, "effort": ROOT_EFFORT},
        ]
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        assert not (root / "turns/pointwise-field-facts/sidecar.json").exists()
        assert not (root / "turns/neutral-root-verification/input.private.json").exists()
