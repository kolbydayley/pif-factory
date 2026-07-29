from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v108_layered_diagnostic as v108
from research_factory.app_server_judge_v5_calibration_v139_sol_low_pointwise_diagnostic import (
    CANARY_CASE_COUNT,
    CASE_COUNT,
    TURN_NAMES,
    _validate_v138,
    build_v139_selection,
    freeze_v139,
    score_v139,
)


def _expected_output(expected, case_ids=None):
    selected = set(case_ids) if case_ids is not None else set(expected["cases"])
    rows = []
    for case_id, case in expected["cases"].items():
        if case_id not in selected:
            continue
        for witness_id, proposition in case["proposition"].items():
            rows.append(
                {
                    "case_id": case_id,
                    "witness_id": witness_id,
                    "proposition_verdict": proposition,
                    "proposition_evidence_spans": ["evidence"],
                    "proposition_rationale": "expected",
                    "structured_field_verdict": case["structured_fields"][witness_id],
                    "field_issue_fields": deepcopy(case["field_issues"][witness_id]),
                    "field_evidence_spans": ["evidence"],
                    "field_rationale": "expected",
                }
            )
    return {"units": rows}


def test_v139_selects_12_cases_outside_the_recent_18_case_repair_cohort():
    predecessor = _validate_v138()
    selection, expected, pointwise = build_v139_selection(
        predecessor, v108._validate_predecessors()
    )
    excluded = set(
        predecessor["v137"]["v136"]["v134"]["v133"]["values"]["truth"][
            "cases"
        ]
    )

    assert selection["candidate_case_count"] == 48
    assert selection["excluded_previously_targeted_case_count"] == 18
    assert selection["selected_case_count"] == CASE_COUNT == 12
    assert selection["canary_case_count"] == CANARY_CASE_COUNT == 4
    assert not (set(selection["selected"]) & excluded)
    assert set(selection["canary"]) <= set(selection["selected"])
    assert selection["maximum_cases_per_turn"] == 1
    assert selection["selection_uses_source_text"] is False
    assert selection["model_outputs_used_for_selection"] is False
    assert len(expected["cases"]) == CASE_COUNT
    assert set(expected["canary_case_ids"]) == set(selection["canary"])
    assert {row["case_id"] for row in pointwise["units"]} == set(selection["selected"])


def test_v139_score_requires_every_support_field_and_order_gate():
    predecessor = _validate_v138()
    _, expected, _ = build_v139_selection(predecessor, v108._validate_predecessors())
    pointwise = _expected_output(expected)
    canary = _expected_output(expected, expected["canary_case_ids"])

    passed = score_v139(pointwise=pointwise, canary=canary, expected=expected)
    assert passed["passed"] is True
    assert passed["expanded_sol_low_pointwise_diagnostic_authorized"] is True
    assert passed["fresh_full_calibration_authorized"] is False

    canary["units"][0]["field_issue_fields"] = ["actor"]
    failed = score_v139(pointwise=pointwise, canary=canary, expected=expected)
    assert failed["passed"] is False
    assert failed["checks"]["order_bias"] is False
    assert failed["expanded_sol_low_pointwise_diagnostic_authorized"] is False


def test_v139_freeze_is_idempotent_presemantic_and_16_turn_bounded(tmp_path: Path):
    root = tmp_path / "v139"
    first = freeze_v139(output_dir=root)
    second = freeze_v139(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-sol"
    assert first["spec"]["reasoning_effort"] == "low"
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["case_count"] == CASE_COUNT
    assert first["spec"]["canary_case_count"] == CANARY_CASE_COUNT
    assert first["spec"]["canary_marker_in_model_input"] is False
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False

    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 1120000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v139_predecessors_are_immutable(tmp_path: Path):
    before_v138 = _validate_v138()["records"]
    before_v106 = v108._validate_predecessors()["records"]
    freeze_v139(output_dir=tmp_path / "v139")
    after_v138 = _validate_v138()["records"]
    after_v106 = v108._validate_predecessors()["records"]

    assert before_v138 == after_v138
    assert before_v106 == after_v106
