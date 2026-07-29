from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v107_recovery_receipt import (
    _validate_v106 as _validate_v106_full,
)
from research_factory.app_server_judge_v5_calibration_v121_retained_field_owner import (
    _validate_v120,
)
from research_factory.app_server_judge_v5_calibration_v130_retained_alignment_owner import (
    _load_v120_alignment_sources,
    _support_only_projection,
    _validate_v129,
    build_v130_inputs,
    freeze_v130,
    reconcile_alignment_reference,
    score_v130,
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


def _passing_outputs(primary_input, truth):
    expected = {row["case_id"]: row for row in truth["cases"]}
    primary_rows = {}
    for case in primary_input["cases"]:
        case_id = case["case_id"]
        visible = {row["witness_id"] for row in case["witnesses"]}
        projection = _visible_projection(expected[case_id]["current_reference"], visible)
        primary_rows[case_id] = _normalized_row(case_id, projection)
    primary = {"cases": list(primary_rows.values())}
    canary = {
        "cases": [deepcopy(primary_rows[case_id]) for case_id in truth["canary_case_ids"]]
    }
    return primary, canary


def test_v130_builds_six_controls_six_disputes_and_support_only_inputs():
    v129 = _validate_v129()
    v120 = _load_v120_alignment_sources()
    primary, canary, truth, selection, receipts = build_v130_inputs(
        v129, v120, _validate_v106_full()
    )

    assert len(primary["cases"]) == 12
    assert len(canary["cases"]) == 6
    assert truth["control_count"] == 6
    assert truth["dispute_count"] == 6
    assert selection["unanimous_control_pool_count"] == 12
    assert selection["dispute_source_counts"] == {
        "consensus_reference_dispute": 1,
        "model_disagreement": 5,
    }
    assert selection["permutation_canary_contains_every_dispute"] is True
    assert selection["prior_alignment_outputs_in_model_input"] is False
    assert selection["fixture_truth_in_model_input"] is False
    assert len(receipts["units"]) == 52
    statuses = {row["witness_id"]: row["proposition_verdict"] for row in receipts["units"]}
    assert all(
        statuses[witness["witness_id"]] == "supported"
        for value in (primary, canary)
        for case in value["cases"]
        for witness in case["witnesses"]
    )
    assert all(case["witnesses"] for case in primary["cases"])


def test_v130_support_projection_excludes_unsupported_pairs_structurally():
    case = {
        "proposition": {"a": "supported", "b": "unsupported"},
        "pairs": [
            {
                "witness_ids": ["a", "b"],
                "relation": "non_equivalent",
                "mismatch_fields": ["unsupported_inference"],
            }
        ],
        "equivalence_groups": [["a"], ["b"]],
        "unpaired_witness_ids": [],
    }
    projected = _support_only_projection(
        {
            "alignment_pairs": case["pairs"],
            "equivalence_groups": case["equivalence_groups"],
            "unpaired_witness_ids": case["unpaired_witness_ids"],
        },
        case["proposition"],
    )
    assert projected == {
        "pairs": [],
        "equivalence_groups": [["a"], ["b"]],
        "unpaired_witness_ids": ["a", "b"],
    }


def test_v130_score_requires_controls_zero_abstentions_and_exact_canary():
    v129 = _validate_v129()
    primary_input, _, truth, _, _ = build_v130_inputs(
        v129, _load_v120_alignment_sources(), _validate_v106_full()
    )
    primary, canary = _passing_outputs(primary_input, truth)
    passed = score_v130(primary, canary, truth)
    assert passed["passed"] is True
    assert passed["alignment_reference_frozen"] is True
    assert passed["fresh_diagnostic_authorized"] is True
    assert passed["fresh_full_calibration_authorized"] is False

    case_id = next(
        row["case_id"]
        for row in truth["cases"]
        if row["role"] == "alignment_dispute"
        and next(item for item in canary["cases"] if item["case_id"] == row["case_id"])[
            "alignment_pairs"
        ]
    )
    row = next(item for item in canary["cases"] if item["case_id"] == case_id)
    row["alignment_pairs"][0]["checklist_decisions"]["actor"] = "abstain"
    failed = score_v130(primary, canary, truth)
    assert failed["passed"] is False
    assert failed["checks"]["permutation_canary_exact_rate"] is False
    assert failed["capped_repair_authorized"] is True


def test_v130_reconciles_disputes_and_keeps_later_gates_closed():
    v129 = _validate_v129()
    primary_input, _, truth, _, _ = build_v130_inputs(
        v129, _load_v120_alignment_sources(), _validate_v106_full()
    )
    primary, _ = _passing_outputs(primary_input, truth)
    reference = reconcile_alignment_reference(
        current_reference=v129["values"]["reference"], primary=primary, truth=truth
    )
    assert len(reference["cases"]) == 66
    assert reference["retained_alignment_owner_case_count"] == 6
    assert reference["proposition_reference_frozen"] is True
    assert reference["alignment_reference_frozen"] is True
    assert reference["reference_frozen"] is True
    assert reference["fresh_diagnostic_authorized"] is True
    assert reference["fresh_full_calibration_authorized"] is False
    assert reference["selection_authorized"] is False
    assert reference["holdout_authorized"] is False


def test_v130_freeze_is_idempotent_presemantic_and_two_turn_bounded(tmp_path: Path):
    root = tmp_path / "v130"
    first = freeze_v130(output_dir=root)
    second = freeze_v130(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == [
        "retained_alignment_owner",
        "retained_alignment_owner_canary",
    ]
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["support_positive_witnesses_only"] is True
    assert first["spec"]["alignment_reference_frozen"] is False
    assert first["spec"]["fresh_diagnostic_authorized"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 140000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not (root / "terminal.json").exists()


def test_v130_reuses_verified_v120_predecessor_without_mutation(tmp_path: Path):
    before = _validate_v120()["records"]
    freeze_v130(output_dir=tmp_path / "v130")
    after = _validate_v120()["records"]
    assert before == after
