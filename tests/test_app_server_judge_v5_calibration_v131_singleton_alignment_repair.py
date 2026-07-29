from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v131_singleton_alignment_repair import (
    _validate_v130,
    build_v131_inputs,
    freeze_v131,
    reconcile_alignment_reference,
    score_v131,
)


def _visible_projection(expected, visible_ids):
    pairs = [pair for pair in expected["pairs"] if set(pair["witness_ids"]) <= visible_ids]
    paired = {witness_id for pair in pairs for witness_id in pair["witness_ids"]}
    groups = []
    for group in expected["equivalence_groups"]:
        retained = sorted(set(group) & visible_ids)
        if retained and retained not in groups:
            groups.append(retained)
    for witness_id in sorted(visible_ids):
        if not any(witness_id in group for group in groups):
            groups.append([witness_id])
    groups.sort()
    return {
        "pairs": pairs,
        "equivalence_groups": groups,
        "unpaired_witness_ids": sorted(visible_ids - paired),
    }


def _normalized_row(case_id, projection):
    pairs = []
    for pair in projection["pairs"]:
        mismatch = set(pair["mismatch_fields"])
        pairs.append(
            {
                **pair,
                "checklist_decisions": {
                    field: "different" if field in mismatch else "same"
                    for field in CHECKLIST_FIELDS
                },
            }
        )
    return {
        "case_id": case_id,
        "alignment_pairs": pairs,
        "equivalence_groups": projection["equivalence_groups"],
        "unpaired_witness_ids": projection["unpaired_witness_ids"],
    }


def _passing_outputs(rows, truth):
    expected = {row["turn_name"]: row for row in truth["cases"]}
    outputs = {}
    for row in rows:
        witness_ids = {item["witness_id"] for item in row["value"]["cases"][0]["witnesses"]}
        projection = _visible_projection(expected[row["turn_name"]]["current_reference"], witness_ids)
        outputs[row["turn_name"]] = {
            "cases": [_normalized_row(row["case_id"], projection)]
        }
    return outputs


def test_v131_builds_three_controls_and_one_isolated_owner():
    v130 = _validate_v130()
    rows, truth, selection = build_v131_inputs(v130)
    assert len(rows) == 4
    assert truth["singleton_control_count"] == 3
    assert truth["singleton_owner_count"] == 1
    assert selection["observable_repair_trigger_count"] == 1
    assert selection["maximum_cases_per_turn"] == 1
    assert selection["prior_alignment_outputs_in_model_input"] is False
    assert selection["fixture_truth_in_model_input"] is False
    assert selection["trigger_reason_in_model_input"] is False
    assert all(len(row["value"]["cases"]) == 1 for row in rows)
    assert all(row["value"]["supported_witnesses_only"] is True for row in rows)
    assert len({row["case_id"] for row in rows}) == 4


def test_v131_score_requires_all_controls_and_decisive_owner():
    rows, truth, _ = build_v131_inputs(_validate_v130())
    outputs = _passing_outputs(rows, truth)
    passed = score_v131(outputs, truth)
    assert passed["passed"] is True
    assert passed["alignment_reference_frozen"] is True
    assert passed["fresh_diagnostic_authorized"] is True
    assert passed["fresh_full_calibration_authorized"] is False

    owner = next(row for row in truth["cases"] if row["role"] == "singleton_owner")
    owner_row = outputs[owner["turn_name"]]["cases"][0]
    assert owner_row["alignment_pairs"]
    owner_row["alignment_pairs"][0]["checklist_decisions"]["actor"] = "abstain"
    failed = score_v131(outputs, truth)
    assert failed["passed"] is False
    assert failed["checks"]["singleton_owner_abstention_count"] is False


def test_v131_reconciles_five_stable_cases_and_singleton_trigger():
    v130 = _validate_v130()
    rows, truth, _ = build_v131_inputs(v130)
    outputs = _passing_outputs(rows, truth)
    reference = reconcile_alignment_reference(v130=v130, outputs=outputs, truth=truth)
    assert len(reference["cases"]) == 66
    assert reference["retained_alignment_stable_v130_case_count"] == 5
    assert reference["retained_alignment_singleton_owner_count"] == 1
    assert reference["alignment_reference_frozen"] is True
    assert reference["fresh_diagnostic_authorized"] is True
    assert reference["fresh_full_calibration_authorized"] is False
    assert reference["selection_authorized"] is False
    assert reference["holdout_authorized"] is False


def test_v131_freeze_is_idempotent_presemantic_and_four_turn_bounded(tmp_path: Path):
    root = tmp_path / "v131"
    first = freeze_v131(output_dir=root)
    second = freeze_v131(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == [
        "singleton_alignment_control_00",
        "singleton_alignment_control_01",
        "singleton_alignment_control_02",
        "singleton_alignment_owner",
    ]
    assert first["spec"]["maximum_cases_per_turn"] == 1
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["alignment_reference_frozen"] is False
    assert first["spec"]["fresh_diagnostic_authorized"] is False
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


def test_v131_predecessor_is_immutable(tmp_path: Path):
    before = _validate_v130()["records"]
    freeze_v131(output_dir=tmp_path / "v131")
    after = _validate_v130()["records"]
    assert before == after
