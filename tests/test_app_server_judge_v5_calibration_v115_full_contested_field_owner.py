from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v115_full_contested_field_owner import (
    TURN_NAMES,
    _validate_v114,
    build_reference_candidate,
    build_v115_inputs,
    freeze_v115,
    score_v115,
)
from research_factory.app_server_judge_v5_calibration_v112_luna_minimal_root_diagnostic import (
    _validate_sources,
)


def test_v115_selects_exactly_all_nonunanimous_field_decisions_side_free():
    value, truth, canary, selection = build_v115_inputs(_validate_sources())

    assert len(value["tasks"]) == len(truth["tasks"]) == 145
    assert len({row["task_id"] for row in value["tasks"]}) == 145
    assert selection["unanimous_retained_count"] == 650
    assert selection["contested_role_counts"] == {
        "consensus_reference_dispute": 93,
        "model_disagreement": 52,
    }
    assert len(canary["tasks"]) == 12
    assert len({row["witness_id"] for row in truth["tasks"]}) > 18
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False
    assert selection["selection_uses_source_text"] is False
    assert selection["semantic_pruning_performed"] is False
    assert selection["majority_voting_used"] is False


def _outputs_for(truth):
    primary_rows = []
    for row in truth["tasks"]:
        status = row["current_status"]
        if row["field"] == "unsupported_inference":
            status = {
                "supported": "correct",
                "unsupported": "incorrect",
                "abstain": "abstain",
            }[row["proposition_status"]]
        primary_rows.append(
            {
                "task_id": row["task_id"],
                "field_status": status,
                "source_evidence_spans": ["evidence"],
                "rationale": "synthetic test decision",
            }
        )
    by_id = {row["task_id"]: row for row in primary_rows}
    canary_rows = [
        {
            **deepcopy(by_id[row["owner_task_id"]]),
            "task_id": row["canary_task_id"],
        }
        for row in truth["canary_map"]
    ]
    return {"decisions": primary_rows}, {"decisions": canary_rows}


def test_v115_score_requires_exact_canaries_evidence_no_abstention_and_support_consistency():
    _, truth, _, _ = build_v115_inputs(_validate_sources())
    primary, canary = _outputs_for(truth)
    passed = score_v115(primary, canary, truth)
    assert passed["passed"] is True
    assert passed["pointwise_reference_patch_authorized"] is True

    canary["decisions"][0]["field_status"] = (
        "incorrect" if canary["decisions"][0]["field_status"] == "correct" else "correct"
    )
    failed = score_v115(primary, canary, truth)
    assert failed["passed"] is False
    assert failed["capped_repair_authorized"] is True
    assert failed["checks"]["permutation_canary_exact_rate"] is False


def test_v115_reference_candidate_changes_only_llm_owned_contested_fields():
    sources = _validate_sources()
    _, truth, _, _ = build_v115_inputs(sources)
    primary, _ = _outputs_for(truth)
    candidate = build_reference_candidate(
        current_truth=sources["values"]["v109_truth"],
        truth=truth,
        primary=primary,
    )

    assert candidate["pointwise_reference_patch_authorized"] is True
    assert candidate["alignment_reference_frozen"] is False
    assert candidate["reference_version"] == "fixture_reference_v9_pointwise_owner_candidate"
    assert candidate["pointwise_field_change_count"] >= 0


def test_v115_freeze_is_idempotent_presemantic_and_keeps_later_gates_closed(tmp_path: Path):
    assert _validate_v114()["values"]["terminal"]["contested_field_reference_owner_authorized"] is True
    root = tmp_path / "v115"
    first = freeze_v115(output_dir=root)
    second = freeze_v115(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-luna"
    assert len(TURN_NAMES) == 11
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["pointwise_reference_patch_authorized"] is False
    assert first["spec"]["alignment_reference_frozen"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list(root.glob("turns/*/capacity.json"))
    assert not list(root.glob("turns/*/sidecar.json"))
    assert not (root / "terminal.json").exists()


def test_v115_capacity_policy_covers_all_turns_and_preserves_reserve(tmp_path: Path):
    import json

    frozen = freeze_v115(output_dir=tmp_path / "v115")
    policy = json.loads(Path(frozen["capacity_policy"]).read_text())
    assert policy["ordered_turn_names"] == list(TURN_NAMES)
    assert policy["phase_total_token_bound"] == 770000
    assert policy["projected_phase_quota_points"] == 14
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
