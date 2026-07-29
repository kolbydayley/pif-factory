from __future__ import annotations

import json
import tempfile
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v70_blind_reference_audit import (
    DEFAULT_OUTPUT_ROOT as V70_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v71_field_microtasks import (
    EFFORT,
    MODEL,
    TURN_NAME,
    _validate_v70,
    assemble_checklist,
    build_v71_input,
    freeze_v71,
    output_schema,
    project_output,
    score_v71,
    validate_output,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _source() -> tuple[dict, dict, dict]:
    value = build_v71_input(_load(V70_ROOT / "blind-reference-input.private.json"))
    truth = _load(V70_ROOT / "diagnostic-truth.private.json")
    roles = _load(V70_ROOT / "cohort-roles.json")
    return value, truth, roles


def _perfect_output(value: dict, truth: dict) -> dict:
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    return {
        "decisions": [
            {
                "task_id": task["task_id"],
                "root_status": (
                    "root"
                    if task["field"]
                    in expected[(task["case_id"], task["witness_id"])]
                    else "not_root"
                ),
                "source_evidence_spans": [],
                "rationale": "Independent field decision.",
            }
            for task in value["tasks"]
        ]
    }


def test_v71_predecessor_and_input_are_exact_and_side_free() -> None:
    assert _validate_v70(V70_ROOT)["v70_terminal"]["sha256"]
    value, _, _ = _source()
    assert len(value["units"]) == 6
    assert value["task_count"] == 90
    assert len(value["tasks"]) == 90
    assert len({row["task_id"] for row in value["tasks"]}) == 90
    assert {row["field"] for row in value["tasks"]} == set(CHECKLIST_FIELDS)
    assert value["prior_labels_present"] is False
    assert value["candidate_outputs_present"] is False
    assert value["system_identity_present"] is False
    assert all(
        set(row) == {"task_id", "case_id", "witness_id", "field"}
        for row in value["tasks"]
    )
    assert all("field_issues" not in row for row in value["units"])


def test_v71_schema_and_prompt_records_fit_declared_transport_caps() -> None:
    value, _, _ = _source()
    schema = output_schema(value)
    schema_bytes = len(json.dumps(schema, sort_keys=True).encode("utf-8"))
    assert schema_bytes <= 64 * 1024
    with tempfile.TemporaryDirectory() as temp:
        frozen = freeze_v71(output_dir=Path(temp) / "v71")
        assert frozen["paths"]["prompt"].stat().st_size <= 96 * 1024
        assert frozen["paths"]["schema"].stat().st_size <= 64 * 1024


def test_v71_perfect_projection_assembly_and_score_pass() -> None:
    value, truth, roles = _source()
    output = _perfect_output(value, truth)
    projected, operations = project_output(output, value)
    assert validate_output(projected, value) == []
    assert operations == []
    checklist = assemble_checklist(projected, value)
    score = score_v71(checklist, truth, roles)
    assert score["passed"] is True
    assert score["failed_checks"] == []
    assert score["metrics"]["exact_case_rate"] == 1.0
    assert score["metrics"]["abstention_count"] == 0


def test_v71_one_wrong_semantic_decision_fails_strict_gate() -> None:
    value, truth, roles = _source()
    output = _perfect_output(value, truth)
    task = next(
        row
        for row in value["tasks"]
        if row["field"] not in {"evidence", "unsupported_inference"}
    )
    decision = next(row for row in output["decisions"] if row["task_id"] == task["task_id"])
    decision["root_status"] = (
        "not_root" if decision["root_status"] == "root" else "root"
    )
    projected, _ = project_output(output, value)
    score = score_v71(assemble_checklist(projected, value), truth, roles)
    assert score["passed"] is False
    assert score["metrics"]["exact_case_rate"] < 1.0


def test_v71_projection_is_limited_to_exact_evidence_and_frozen_support() -> None:
    value, _, _ = _source()
    output = _perfect_output(value, _load(V70_ROOT / "diagnostic-truth.private.json"))
    evidence_task = next(row for row in value["tasks"] if row["field"] == "evidence")
    support_task = next(
        row for row in value["tasks"] if row["field"] == "unsupported_inference"
    )
    evidence_decision = next(
        row for row in output["decisions"] if row["task_id"] == evidence_task["task_id"]
    )
    support_decision = next(
        row for row in output["decisions"] if row["task_id"] == support_task["task_id"]
    )
    evidence_decision["root_status"] = (
        "root" if evidence_decision["root_status"] == "not_root" else "not_root"
    )
    evidence_decision["source_evidence_spans"] = ["not an exact source span"]
    support_decision["root_status"] = (
        "root" if support_decision["root_status"] == "not_root" else "not_root"
    )
    projected, operations = project_output(output, value)
    assert {row["operation_type"] for row in operations} == {
        "drop_nonexact_microtask_span",
        "project_exact_evidence_receipt",
        "project_frozen_support_receipt",
    }
    assert validate_output(projected, value) == []


def test_v71_freeze_is_one_presemantic_mini_turn_and_is_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v71"
        frozen = freeze_v71(output_dir=root)
        again = freeze_v71(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["reasoning_effort"] == EFFORT
        assert frozen["spec"]["turn_plan"] == [TURN_NAME]
        assert frozen["spec"]["task_count"] == 90
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        assert not (root / "turns/field_microtask_diagnostic/capacity.json").exists()
        assert not (root / "turns/field_microtask_diagnostic/sidecar.json").exists()
        assert not (root / "turns/field_microtask_diagnostic/output.private.json").exists()
