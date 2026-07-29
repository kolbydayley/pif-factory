from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v110_sol_reference_audit import (
    _truth_alignment,
)
from research_factory.app_server_judge_v5_calibration_v117_alignment_reference_owner import (
    _validate_v111,
    _validate_v116,
    build_v117_inputs,
    freeze_v117,
    reconcile_alignment_reference,
    score_v117,
)


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


def _passing_outputs(v116, truth):
    current = v116["values"]["reference"]
    rows = {
        row["case_id"]: _normalized_row(
            row["case_id"], _truth_alignment(current["cases"][row["case_id"]])
        )
        for row in truth["cases"]
    }
    primary = {"cases": list(rows.values())}
    canary = {
        "cases": [deepcopy(rows[case_id]) for case_id in truth["canary_case_ids"]]
    }
    return primary, canary


def test_v117_builds_seven_controls_five_disputes_and_small_canary():
    v116 = _validate_v116()
    primary, canary, truth, selection = build_v117_inputs(v116, _validate_v111())

    assert len(primary["cases"]) == 12
    assert len(canary["cases"]) == 6
    assert truth["control_count"] == 7
    assert truth["dispute_count"] == 5
    assert selection["permutation_canary_contains_every_dispute"] is True
    assert selection["support_receipt_reference_mismatch_count"] == 0
    assert selection["prior_alignment_outputs_in_model_input"] is False
    assert selection["fixture_truth_in_model_input"] is False
    assert primary["side_labels_present"] is False
    assert canary["system_identity_present"] is False
    assert all(
        list(reversed(next(c for c in primary["cases"] if c["case_id"] == row["case_id"])["witnesses"]))
        == row["witnesses"]
        for row in canary["cases"]
    )


def test_v117_score_requires_controls_zero_abstentions_and_exact_canary():
    v116 = _validate_v116()
    _, _, truth, _ = build_v117_inputs(v116, _validate_v111())
    primary, canary = _passing_outputs(v116, truth)
    passed = score_v117(primary, canary, truth)
    assert passed["passed"] is True
    assert passed["alignment_reference_frozen"] is True
    assert passed["fresh_full_calibration_authorized"] is True

    case_id = truth["canary_case_ids"][0]
    row = next(item for item in canary["cases"] if item["case_id"] == case_id)
    row["alignment_pairs"][0]["checklist_decisions"]["actor"] = "abstain"
    failed = score_v117(primary, canary, truth)
    assert failed["passed"] is False
    assert failed["checks"]["permutation_canary_exact_rate"] is False
    assert failed["capped_repair_authorized"] is True


def test_v117_reconciles_only_disputes_and_freezes_reference():
    v116 = _validate_v116()
    _, _, truth, _ = build_v117_inputs(v116, _validate_v111())
    primary, _ = _passing_outputs(v116, truth)
    reference = reconcile_alignment_reference(
        current_reference=v116["values"]["reference"],
        primary=primary,
        truth=truth,
    )
    assert len(reference["cases"]) == 18
    assert reference["alignment_owner_case_count"] == 5
    assert reference["pointwise_reference_patch_authorized"] is True
    assert reference["alignment_reference_frozen"] is True
    assert reference["reference_frozen"] is True
    assert reference["fresh_full_calibration_authorized"] is True


def test_v117_freeze_is_idempotent_and_presemantic(tmp_path: Path):
    root = tmp_path / "v117"
    first = freeze_v117(output_dir=root)
    second = freeze_v117(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == [
        "final_alignment_owner",
        "final_alignment_owner_canary",
    ]
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["alignment_reference_frozen"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not (root / "terminal.json").exists()


def test_v117_capacity_policy_bounds_exactly_two_turns(tmp_path: Path):
    frozen = freeze_v117(output_dir=tmp_path / "v117")
    policy = json.loads(Path(frozen["capacity_policy"]).read_text())
    assert policy["ordered_turn_names"] == [
        "final_alignment_owner",
        "final_alignment_owner_canary",
    ]
    assert policy["phase_total_token_bound"] == 140000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
