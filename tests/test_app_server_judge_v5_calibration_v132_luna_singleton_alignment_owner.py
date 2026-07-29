from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v132_luna_singleton_alignment_owner import (
    _validate_v131,
    build_v132_inputs,
    freeze_v132,
    reconcile_alignment_reference,
    score_v132,
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


def test_v132_excludes_only_failed_v131_control_and_reuses_no_owner_output():
    v131 = _validate_v131()
    rows, truth, selection = build_v132_inputs(v131)
    assert len(rows) == 4
    assert truth["singleton_control_count"] == 3
    assert truth["singleton_owner_count"] == 1
    assert selection["v131_failed_control_exclusion_count"] == 1
    assert selection["v131_owner_output_reused"] is False
    assert selection["maximum_cases_per_turn"] == 1
    assert selection["prior_alignment_outputs_in_model_input"] is False
    assert selection["fixture_truth_in_model_input"] is False
    assert v131["failed_control_case_id"] not in {row["case_id"] for row in rows}
    assert any(row["case_id"] == v131["owner_case_id"] for row in rows)
    assert all(len(row["value"]["cases"]) == 1 for row in rows)


def test_v132_score_and_reconciliation_keep_later_gates_closed():
    v131 = _validate_v131()
    rows, truth, _ = build_v132_inputs(v131)
    outputs = _passing_outputs(rows, truth)
    score = score_v132(outputs, truth)
    assert score["passed"] is True
    assert score["alignment_reference_frozen"] is True
    assert score["fresh_diagnostic_authorized"] is True
    assert score["fresh_full_calibration_authorized"] is False
    assert score["metrics"]["v131_failed_control_exclusion_count"] == 1

    reference = reconcile_alignment_reference(v131=v131, outputs=outputs, truth=truth)
    assert len(reference["cases"]) == 66
    assert reference["retained_alignment_stable_v130_case_count"] == 5
    assert reference["retained_alignment_singleton_owner_count"] == 1
    assert reference["retained_alignment_singleton_owner_model"] == "gpt-5.6-luna"
    assert reference["v131_owner_output_reused"] is False
    assert reference["alignment_reference_frozen"] is True
    assert reference["fresh_diagnostic_authorized"] is True
    assert reference["fresh_full_calibration_authorized"] is False
    assert reference["selection_authorized"] is False
    assert reference["holdout_authorized"] is False


def test_v132_freeze_is_idempotent_presemantic_and_four_turn_bounded(tmp_path: Path):
    root = tmp_path / "v132"
    first = freeze_v132(output_dir=root)
    second = freeze_v132(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-luna"
    assert first["spec"]["turn_plan"] == [
        "luna_singleton_alignment_control_00",
        "luna_singleton_alignment_control_01",
        "luna_singleton_alignment_control_02",
        "luna_singleton_alignment_owner",
    ]
    assert first["spec"]["maximum_cases_per_turn"] == 1
    assert first["spec"]["v131_failed_control_excluded"] is True
    assert first["spec"]["v131_owner_output_reused"] is False
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


def test_v132_predecessors_are_immutable(tmp_path: Path):
    before = _validate_v131()["records"]
    freeze_v132(output_dir=tmp_path / "v132")
    after = _validate_v131()["records"]
    assert before == after
