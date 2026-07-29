from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v140_layered_field_diagnostic import (
    TURN_NAMES,
    _validate_v139,
    build_v140_inputs,
    freeze_v140,
    score_v140,
)


def _expected_outputs(truth):
    support_units = []
    for case_id, case in truth["cases"].items():
        for witness_id, verdict in case["proposition"].items():
            support_units.append(
                {
                    "case_id": case_id,
                    "witness_id": witness_id,
                    "proposition_verdict": verdict,
                    "exact_source_spans": ["evidence"],
                    "rationale": "expected",
                }
            )
    field_decisions = [
        {
            "task_id": row["task_id"],
            "field_status": row["expected_status"],
            "source_evidence_spans": ["evidence"],
            "rationale": "expected",
        }
        for row in truth["field_tasks"]
    ]
    return (
        {"units": support_units},
        {"units": deepcopy(support_units)},
        {"decisions": field_decisions},
        {"decisions": deepcopy(field_decisions)},
    )


def test_v140_builds_two_cases_support_first_and_all_15_field_pairs():
    selection, truth, support_input, field_inputs = build_v140_inputs(_validate_v139())

    assert selection["selected_case_count"] == 2
    assert selection["unsupported_case_count"] == 1
    assert selection["supported_control_case_count"] == 1
    assert 4 <= selection["witness_count"] <= 6
    assert selection["field_count"] == len(CHECKLIST_FIELDS) == 15
    assert selection["support_primary_turn_count"] == 1
    assert selection["support_order_canary_turn_count"] == 1
    assert selection["field_primary_turn_count"] == 15
    assert selection["field_order_canary_turn_count"] == 15
    assert selection["canary_marker_in_model_input"] is False
    assert selection["selection_uses_source_text"] is False
    assert selection["model_outputs_used_for_selection"] is False
    assert len(support_input["units"]) == truth["witness_count"]
    assert set(field_inputs) == set(CHECKLIST_FIELDS)
    assert all(
        value["task_count"] == truth["witness_count"]
        for value in field_inputs.values()
    )
    assert len(truth["field_tasks"]) == truth["witness_count"] * 15


def test_v140_score_requires_support_field_and_both_order_canaries():
    _, truth, _, _ = build_v140_inputs(_validate_v139())
    support, support_canary, fields, field_canary = _expected_outputs(truth)
    passed = score_v140(
        support=support,
        support_canary=support_canary,
        fields=fields,
        field_canary=field_canary,
        truth=truth,
    )

    assert passed["passed"] is True
    assert passed["expanded_layered_field_diagnostic_authorized"] is True
    assert passed["alignment_diagnostic_authorized"] is False
    assert passed["fresh_full_calibration_authorized"] is False

    field_canary["decisions"][0]["field_status"] = (
        "incorrect"
        if field_canary["decisions"][0]["field_status"] == "correct"
        else "correct"
    )
    failed = score_v140(
        support=support,
        support_canary=support_canary,
        fields=fields,
        field_canary=field_canary,
        truth=truth,
    )
    assert failed["passed"] is False
    assert failed["checks"]["field_order_canary_exact_rate"] is False


def test_v140_freeze_is_idempotent_presemantic_and_32_turn_bounded(tmp_path: Path):
    root = tmp_path / "v140"
    first = freeze_v140(output_dir=root)
    second = freeze_v140(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-sol"
    assert first["spec"]["reasoning_effort"] == "high"
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert len(TURN_NAMES) == 32
    assert first["spec"]["field_count"] == 15
    assert first["spec"]["canary_marker_in_model_input"] is False
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False

    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 2240000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v140_predecessor_is_immutable(tmp_path: Path):
    before = _validate_v139()["records"]
    freeze_v140(output_dir=tmp_path / "v140")
    after = _validate_v139()["records"]

    assert before == after
