from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v120_retained_case_diagnostic import (
    _validate_v119,
)
from research_factory.app_server_judge_v5_calibration_v121_retained_field_owner import (
    _validate_v120,
)
from research_factory.app_server_judge_v5_calibration_v133_terra_pointwise_diagnostic import (
    _validate_v132,
    build_v133_selection,
    freeze_v133,
    score_v133,
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


def test_v133_selection_uses_15_fresh_cases_and_three_support_controls():
    selection, expected = build_v133_selection(
        _validate_v132(), _validate_v119(), _validate_v120()
    )
    assert selection["candidate_case_count"] == 30
    assert selection["selected_case_count"] == 18
    assert selection["fresh_case_count"] == 15
    assert selection["audited_support_control_count"] == 3
    assert selection["canary_case_count"] == 6
    assert selection["unsupported_witness_count"] >= 3
    assert selection["v119_audited_overlap_count"] == 3
    assert selection["v120_v132_repair_lineage_overlap_count"] == 0
    assert selection["selection_uses_source_text"] is False
    assert selection["model_outputs_used_for_selection"] is False
    assert len(expected["cases"]) == 18
    assert set(expected["canary_case_ids"]) <= set(expected["cases"])


def test_v133_score_requires_all_frozen_pointwise_and_order_gates():
    _, expected = build_v133_selection(
        _validate_v132(), _validate_v119(), _validate_v120()
    )
    pointwise = _expected_output(expected)
    canary = _expected_output(expected, expected["canary_case_ids"])
    passed = score_v133(pointwise=pointwise, canary=canary, expected=expected)
    assert passed["passed"] is True
    assert passed["alignment_diagnostic_authorized"] is True
    assert passed["fresh_full_calibration_authorized"] is False

    canary["units"][0]["field_issue_fields"] = ["actor"]
    failed = score_v133(pointwise=pointwise, canary=canary, expected=expected)
    assert failed["passed"] is False
    assert failed["checks"]["order_bias"] is False
    assert failed["alignment_diagnostic_authorized"] is False


def test_v133_freeze_is_idempotent_presemantic_and_four_turn_bounded(tmp_path: Path):
    root = tmp_path / "v133"
    first = freeze_v133(output_dir=root)
    second = freeze_v133(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-terra"
    assert first["spec"]["case_count"] == 18
    assert first["spec"]["canary_case_count"] == 6
    assert first["spec"]["turn_plan"] == [
        "terra_pointwise_shard_00",
        "terra_pointwise_shard_01",
        "terra_pointwise_shard_02",
        "terra_pointwise_order_canary",
    ]
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["alignment_diagnostic_authorized"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 280000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not (root / "terminal.json").exists()


def test_v133_predecessors_are_immutable(tmp_path: Path):
    before = _validate_v132()["records"]
    freeze_v133(output_dir=tmp_path / "v133")
    after = _validate_v132()["records"]
    assert before == after
