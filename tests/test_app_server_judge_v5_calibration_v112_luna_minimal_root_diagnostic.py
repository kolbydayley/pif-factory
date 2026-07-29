from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v112_luna_minimal_root_diagnostic import (
    TURN_NAMES,
    _validate_sources,
    build_v112_inputs,
    freeze_v112,
    score_v112,
)


def test_v112_selection_is_balanced_fresh_side_free_and_distinct():
    sources = _validate_sources()
    value, truth, canary, selection = build_v112_inputs(sources)

    assert len(value["tasks"]) == 18
    assert len({row["witness_id"] for row in truth["tasks"]}) == 18
    assert selection["settled_control_status_counts"] == {"correct": 4, "incorrect": 4}
    assert selection["consensus_reference_dispute_count"] == 6
    assert selection["model_disagreement_count"] == 4
    assert len(canary["tasks"]) == 6
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False
    assert selection["selection_uses_source_text"] is False
    assert selection["majority_voting_used"] is False
    assert all(
        (row["case_id"], row["witness_id"], row["field"])
        not in sources["prior_luna_pairs"]
        for row in truth["tasks"]
    )


def _passing_outputs(truth):
    decisions = []
    for row in truth["tasks"]:
        status = row["current_status"] if row["role"] == "settled_control" else "incorrect"
        decisions.append(
            {
                "task_id": row["task_id"],
                "field_status": status,
                "source_evidence_spans": ["evidence"],
                "rationale": "synthetic test decision",
            }
        )
    primary = {"decisions": decisions}
    by_id = {row["task_id"]: row for row in decisions}
    canary = {
        "decisions": [
            {
                **deepcopy(by_id[row["owner_task_id"]]),
                "task_id": row["canary_task_id"],
            }
            for row in truth["canary_map"]
        ]
    }
    return primary, canary


def test_v112_score_requires_all_controls_canaries_evidence_and_no_abstentions():
    _, truth, _, _ = build_v112_inputs(_validate_sources())
    primary, canary = _passing_outputs(truth)
    passed = score_v112(primary, canary, truth)
    assert passed["passed"] is True
    assert passed["contested_field_reference_owner_authorized"] is True

    canary["decisions"][0]["field_status"] = "abstain"
    failed = score_v112(primary, canary, truth)
    assert failed["passed"] is False
    assert failed["checks"]["permutation_canary_exact_rate"] is False
    assert failed["checks"]["abstention_count"] is False


def test_v112_freeze_is_idempotent_presemantic_and_keeps_later_gates_closed(tmp_path: Path):
    root = tmp_path / "v112"
    first = freeze_v112(output_dir=root)
    second = freeze_v112(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-luna"
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["contested_field_reference_owner_authorized"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert len(TURN_NAMES) == 4
    assert not list(root.glob("turns/*/capacity.json"))
    assert not list(root.glob("turns/*/sidecar.json"))
    assert not (root / "terminal.json").exists()


def test_v112_capacity_bound_is_four_turns_and_preserves_reserve(tmp_path: Path):
    frozen = freeze_v112(output_dir=tmp_path / "v112")
    import json

    policy = json.loads(Path(frozen["capacity_policy"]).read_text())
    assert policy["ordered_turn_names"] == list(TURN_NAMES)
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["phase_total_token_bound"] == 280000
    assert policy["projected_phase_quota_points"] == 5
    assert policy["retry_count_per_turn"] == 0
    assert policy["managed_chatgpt_auth_only"] is True
