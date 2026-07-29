from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v120_retained_case_diagnostic import (
    _validate_v119,
)
from research_factory.app_server_judge_v5_calibration_v134_gpt55_singleton_pointwise import (
    CANARY_CASE_COUNT,
    CASE_COUNT,
    FRESH_CASE_COUNT,
    SUPPORT_CONTROL_COUNT,
    TURN_NAMES,
    _validate_v133,
    build_v134_selection,
    freeze_v134,
    score_v134,
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


def test_v134_selection_is_six_cases_without_v133_output_selection():
    v133 = _validate_v133()
    audited = _validate_v119()["audited_case_ids"]
    selection, expected = build_v134_selection(v133, audited)

    assert selection["case_count"] == CASE_COUNT == 6
    assert selection["fresh_case_count"] == FRESH_CASE_COUNT == 4
    assert selection["audited_support_control_count"] == SUPPORT_CONTROL_COUNT == 2
    assert selection["canary_case_count"] == CANARY_CASE_COUNT == 3
    assert selection["maximum_cases_per_turn"] == 1
    assert selection["selection_uses_source_text"] is False
    assert selection["v133_model_outputs_used_for_selection"] is False
    assert len(expected["cases"]) == CASE_COUNT
    assert set(selection["canary"]) == set(expected["canary_case_ids"])
    assert set(selection["selected"]) == set(expected["cases"])


def test_v134_score_requires_all_pointwise_and_exact_canary_gates():
    v133 = _validate_v133()
    _, expected = build_v134_selection(v133, _validate_v119()["audited_case_ids"])
    pointwise = _expected_output(expected)
    canary = _expected_output(expected, expected["canary_case_ids"])

    passed = score_v134(pointwise=pointwise, canary=canary, expected=expected)
    assert passed["passed"] is True
    assert passed["expanded_singleton_pointwise_diagnostic_authorized"] is True
    assert passed["alignment_diagnostic_authorized"] is False
    assert passed["fresh_full_calibration_authorized"] is False

    canary["units"][0]["structured_field_verdict"] = "incorrect"
    failed = score_v134(pointwise=pointwise, canary=canary, expected=expected)
    assert failed["passed"] is False
    assert failed["checks"]["order_bias"] is False
    assert failed["expanded_singleton_pointwise_diagnostic_authorized"] is False


def test_v134_freeze_is_idempotent_presemantic_and_nine_turn_bounded(tmp_path: Path):
    root = tmp_path / "v134"
    first = freeze_v134(output_dir=root)
    second = freeze_v134(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.5"
    assert first["spec"]["case_count"] == CASE_COUNT
    assert first["spec"]["canary_case_count"] == CANARY_CASE_COUNT
    assert first["spec"]["maximum_cases_per_turn"] == 1
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["expanded_singleton_pointwise_diagnostic_authorized"] is False
    assert first["spec"]["alignment_diagnostic_authorized"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False

    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 630000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v134_predecessors_are_immutable(tmp_path: Path):
    before_v133 = _validate_v133()["records"]
    freeze_v134(output_dir=tmp_path / "v134")
    after_v133 = _validate_v133()["records"]

    assert before_v133 == after_v133
