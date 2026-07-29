from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v118_capped_alignment_repair import (
    _validate_v117,
    build_v118_input,
    freeze_v118,
    reconcile_reference,
    score_v118,
)
from research_factory.app_server_judge_v5_calibration_v117_alignment_reference_owner import (
    _validate_v116,
)


def _normalized_row(case_id, projection):
    pairs = []
    for pair in projection["pairs"]:
        mismatch = set(pair["mismatch_fields"])
        pairs.append(
            {
                **deepcopy(pair),
                "checklist_decisions": {
                    field: "different" if field in mismatch else "same"
                    for field in CHECKLIST_FIELDS
                },
            }
        )
    return {
        "case_id": case_id,
        "alignment_pairs": pairs,
        "equivalence_groups": deepcopy(projection["equivalence_groups"]),
        "unpaired_witness_ids": deepcopy(projection["unpaired_witness_ids"]),
    }


def _passing_output(truth):
    return {
        "cases": [
            _normalized_row(row["case_id"], row["current_reference"])
            for row in truth["cases"]
        ]
    }


def test_v118_selects_only_one_trigger_and_four_controls_without_prior_outputs():
    v117 = _validate_v117()
    v116 = _validate_v116()
    value, truth, selection = build_v118_input(
        v117, v116["values"]["reference"]
    )
    assert len(value["cases"]) == 5
    assert sum(row["role"] == "observable_repair" for row in truth["cases"]) == 1
    assert sum(row["role"] == "matched_control" for row in truth["cases"]) == 4
    assert selection["prior_alignment_outputs_in_model_input"] is False
    assert selection["fixture_truth_in_model_input"] is False
    assert selection["majority_voting_used"] is False
    assert value["side_labels_present"] is False
    assert value["system_identity_present"] is False


def test_v118_score_requires_all_controls_and_decisive_repair():
    v117 = _validate_v117()
    v116 = _validate_v116()
    _, truth, _ = build_v118_input(v117, v116["values"]["reference"])
    output = _passing_output(truth)
    passed = score_v118(output, truth)
    assert passed["passed"] is True
    assert passed["alignment_reference_frozen"] is True
    assert passed["fresh_full_calibration_authorized"] is True

    control_id = next(
        row["case_id"] for row in truth["cases"] if row["role"] == "matched_control"
    )
    control = next(row for row in output["cases"] if row["case_id"] == control_id)
    control["alignment_pairs"][0]["checklist_decisions"]["actor"] = "abstain"
    failed = score_v118(output, truth)
    assert failed["passed"] is False
    assert failed["checks"]["abstention_case_count"] is False


def test_v118_reconciles_five_primary_disputes_and_one_final_repair():
    v117 = _validate_v117()
    v116 = _validate_v116()
    _, truth, _ = build_v118_input(v117, v116["values"]["reference"])
    output = _passing_output(truth)
    reference = reconcile_reference(
        current_reference=v116["values"]["reference"],
        v117_truth=v117["values"]["truth"],
        v117_primary=v117["values"]["primary"],
        v118_truth=truth,
        v118_output=output,
    )
    assert len(reference["cases"]) == 18
    assert reference["alignment_primary_owner_case_count"] == 5
    assert reference["alignment_capped_repair_case_count"] == 1
    assert reference["reference_frozen"] is True
    assert reference["fresh_full_calibration_authorized"] is True


def test_v118_freeze_is_idempotent_presemantic_and_keeps_later_gates_closed(
    tmp_path: Path,
):
    root = tmp_path / "v118"
    first = freeze_v118(output_dir=root)
    second = freeze_v118(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.4"
    assert first["spec"]["turn_plan"] == ["capped_alignment_repair"]
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["alignment_reference_frozen"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not (root / "terminal.json").exists()


def test_v118_capacity_policy_is_one_turn_and_preserves_reserve(tmp_path: Path):
    frozen = freeze_v118(output_dir=tmp_path / "v118")
    policy = json.loads(Path(frozen["capacity_policy"]).read_text())
    assert policy["ordered_turn_names"] == ["capped_alignment_repair"]
    assert policy["phase_total_token_bound"] == 70000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
