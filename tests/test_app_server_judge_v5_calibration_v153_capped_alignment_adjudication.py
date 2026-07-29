from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5 import validate_neutral_alignment_output
from research_factory.app_server_judge_v5_calibration_v153_capped_alignment_adjudication import (
    MODEL,
    TURN_NAME,
    _validate_v152,
    build_v153_input,
    freeze_v153,
    reconcile_and_score_v153,
)


def test_v153_preserves_v152_zero_token_authorization_and_v151_usage():
    source = _validate_v152()

    assert source["values"]["terminal"]["usage"]["total_tokens"] == 0
    assert source["values"]["terminal"]["capped_side_free_alignment_adjudication_authorized"] is True
    assert source["v151"]["usage"]["total_tokens"] == 75098
    assert len(source["v151"]["attempts"]) == 2


def test_v153_input_is_four_case_origin_neutral_balanced_and_one_call():
    source = _validate_v152()
    value = build_v153_input(source)

    packet = value["packet"]
    alignment = value["alignment_input"]
    assert packet["adjudication_required"] is True
    assert packet["call_cap"] == 1
    assert len(packet["cases"]) == 4
    assert packet["anonymous_candidate_order_balanced"] is True
    assert packet["anonymous_candidate_swap_count"] == 2
    assert alignment["side_labels_present"] is False
    assert alignment["system_identity_present"] is False
    assert alignment["adjudication_only"] is True
    assert len(alignment["cases"]) == 4


def test_v153_perfect_side_free_owner_would_clear_all_frozen_alignment_gates():
    source = _validate_v152()
    value = build_v153_input(source)
    truth = {row["case_id"]: row for row in source["truth"]["cases"]}
    primary_raw = {row["case_id"]: row for row in value["primary_output"]["cases"]}
    canary_raw = {row["case_id"]: row for row in value["canary_output"]["cases"]}
    plan = {row["case_id"]: row for row in source["values"]["plan"]["cases"]}
    cases = []
    for case in value["alignment_input"]["cases"]:
        case_id = case["case_id"]
        row = plan[case_id]
        chosen = primary_raw[case_id] if row["primary_truth_exact"] else canary_raw[case_id]
        cases.append(deepcopy(chosen))
        assert row["primary_truth_exact"] or row["canary_truth_exact"]
        assert truth[case_id]["expected"]
    output = {"cases": cases}
    assert validate_neutral_alignment_output(output, value["alignment_input"]) == []

    _normalized, reconciled, score = reconcile_and_score_v153(
        source=source, value=value, output=output
    )
    assert reconciled["observable_disagreement_case_count"] == 4
    assert reconciled["adjudication_call_count"] == 1
    assert score["passed"] is True
    assert score["failed_checks"] == []
    assert score["metrics"]["alignment_f1"] == 1.0
    assert score["metrics"]["order_bias"] == 0.0


def test_v153_freeze_is_idempotent_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v153"
    first = freeze_v153(output_dir=root)
    second = freeze_v153(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == MODEL == "gpt-5.5"
    assert first["spec"]["turn_plan"] == [TURN_NAME]
    assert first["spec"]["adjudication_call_cap"] == 1
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["majority_voting_used"] is False
    assert first["spec"]["prompt_bytes"] <= 512000
    assert first["spec"]["schema_bytes"] <= 128000
    assert first["spec"]["frozen_inputs"]["truth"] == first["spec"]["predecessor"]["v152_truth"]
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v153_freeze_does_not_mutate_v152(tmp_path: Path):
    before = _validate_v152()["records"]
    freeze_v153(output_dir=tmp_path / "v153")
    after = _validate_v152()["records"]
    assert before == after
