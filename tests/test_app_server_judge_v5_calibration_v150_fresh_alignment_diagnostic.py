from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v150_fresh_alignment_diagnostic import (
    CANARY_TURNS,
    PRIMARY_TURNS,
    TURN_NAMES,
    _validate_v106_sources,
    _validate_v149,
    build_v150_inputs,
    freeze_v150,
    score_v150,
)


def _normalized_from_truth(truth):
    cases = []
    for row in truth["cases"]:
        expected = row["expected"]
        pairs = []
        for pair in expected["pairs"]:
            mismatches = set(pair["mismatch_fields"])
            pairs.append(
                {
                    "witness_ids": pair["witness_ids"],
                    "relation": pair["relation"],
                    "mismatch_fields": pair["mismatch_fields"],
                    "checklist_decisions": {
                        field: "different" if field in mismatches else "same"
                        for field in CHECKLIST_FIELDS
                    },
                }
            )
        cases.append(
            {
                "case_id": row["case_id"],
                "alignment_pairs": pairs,
                "equivalence_groups": expected["equivalence_groups"],
                "unpaired_witness_ids": expected["unpaired_witness_ids"],
            }
        )
    return {"cases": cases}


def test_v150_selects_twelve_stratified_cases_and_identical_shard_membership():
    rows, truth, selection, receipts = build_v150_inputs(
        _validate_v149(), _validate_v106_sources()
    )

    assert len(rows) == len(TURN_NAMES) == 4
    assert truth["case_count"] == 12
    assert truth["relation_bucket_counts"] == {
        "equivalent": 3,
        "multi_equivalent": 3,
        "partial": 3,
        "non_equivalent": 3,
    }
    assert truth["unpaired_case_count"] >= 4
    assert selection["cases_per_shard"] == 6
    assert selection["primary_canary_membership_identical_by_shard"] is True
    by_name = {row["turn_name"]: row for row in rows}
    for primary, canary in zip(PRIMARY_TURNS, CANARY_TURNS, strict=True):
        assert by_name[primary]["case_ids"] == by_name[canary]["case_ids"]
    assert all(row["structured_field_verdict"] == "abstain" for row in receipts["units"])
    assert all(row["field_issue_fields"] == [] for row in receipts["units"])
    assert selection["selection_uses_source_text"] is False
    assert selection["fixture_truth_in_model_input"] is False


def test_v150_score_passes_exact_alignment_and_rejects_order_drift():
    _, truth, _, _ = build_v150_inputs(_validate_v149(), _validate_v106_sources())
    primary = _normalized_from_truth(truth)
    canary = deepcopy(primary)
    passed = score_v150(primary=primary, canary=canary, truth=truth)

    assert passed["passed"] is True
    assert passed["metrics"]["alignment_f1"] == 1.0
    assert passed["metrics"]["relation_accuracy"] == 1.0
    assert passed["metrics"]["mismatch_field_f1"] == 1.0
    assert passed["metrics"]["order_bias"] == 0.0

    broken = deepcopy(canary)
    broken["cases"][0]["alignment_pairs"][0]["relation"] = "non_equivalent"
    assert score_v150(primary=primary, canary=broken, truth=truth)["passed"] is False


def test_v150_freeze_is_idempotent_four_turn_bounded_and_private(tmp_path: Path):
    root = tmp_path / "v150"
    first = freeze_v150(output_dir=root)
    second = freeze_v150(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-luna"
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["structured_field_receipts_withheld"] is True
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 280000
    assert policy["minimum_remaining_reserve_percent"] == 20
    for row in first["spec"]["frozen_inputs"]["turns"]:
        prompt = Path(row["prompt"]["path"]).read_text()
        assert '"expected":' not in prompt
        assert '"relation_bucket":' not in prompt
        assert '"fixture_reference":' not in prompt
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v150_freeze_does_not_mutate_v149_or_reference(tmp_path: Path):
    before = _validate_v149()
    before_records = (before["records"], before["v148"]["records"]["reference"])
    freeze_v150(output_dir=tmp_path / "v150")
    after = _validate_v149()
    assert before_records == (after["records"], after["v148"]["records"]["reference"])
