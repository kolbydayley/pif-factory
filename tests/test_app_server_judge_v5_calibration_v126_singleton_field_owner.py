from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v126_singleton_field_owner import (
    _validate_v125,
    build_v126_inputs,
    finalize_v125_owner,
    freeze_v126,
    score_v126,
)


def _passing_outputs(rows):
    result = {}
    for row in rows:
        truth = row["truth"]
        result[row["turn_name"]] = {
            "decisions": [
                {
                    "task_id": truth["task_id"],
                    "field_status": truth.get("control_expected_status") or "correct",
                    "source_evidence_spans": ["evidence"],
                    "rationale": "synthetic singleton decision",
                }
            ]
        }
    return result


def test_v126_uses_six_isolated_single_task_turns_with_matched_field_controls():
    rows, truth, selection = build_v126_inputs(_validate_v125())
    assert len(rows) == 6
    assert truth["singleton_control_count"] == 3
    assert truth["singleton_owner_count"] == 3
    assert selection["owner_field_counts"] == {"attribution": 2, "unsupported_inference": 1}
    assert selection["control_field_counts"] == selection["owner_field_counts"]
    assert selection["maximum_tasks_per_turn"] == 1
    assert selection["task_order_effect_removed_by_construction"] is True
    assert selection["prior_labels_in_model_input"] is False
    assert selection["prior_model_decisions_in_model_input"] is False
    assert selection["majority_voting_used"] is False
    assert all(row["value"]["task_count"] == 1 for row in rows)
    assert all(row["value"]["system_identity_present"] is False for row in rows)


def test_v126_score_requires_all_controls_evidence_and_decisive_owners():
    rows, truth, _ = build_v126_inputs(_validate_v125())
    outputs = _passing_outputs(rows)
    passed = score_v126(outputs, truth)
    assert passed["passed"] is True
    assert passed["metrics"]["singleton_control_exact_count"] == 3
    assert passed["metrics"]["singleton_owner_abstention_count"] == 0

    owner = next(row for row in truth["tasks"] if row["role"] == "singleton_owner")
    for output in outputs.values():
        if output["decisions"][0]["task_id"] == owner["task_id"]:
            output["decisions"][0]["field_status"] = "abstain"
    failed = score_v126(outputs, truth)
    assert failed["passed"] is False
    assert failed["checks"]["singleton_owner_abstention_count"] is False


def test_v126_final_projection_replaces_only_three_unstable_owner_rows():
    predecessor = _validate_v125()
    rows, truth, _ = build_v126_inputs(predecessor)
    outputs = _passing_outputs(rows)
    final = finalize_v125_owner(v125=predecessor, truth=truth, outputs=outputs)
    assert final["singleton_owner_change_count"] == 3
    assert len(final["decisions"]) == 12


def test_v126_freeze_is_idempotent_presemantic_and_keeps_later_gates_closed(
    tmp_path: Path,
):
    root = tmp_path / "v126"
    first = freeze_v126(output_dir=root)
    second = freeze_v126(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-sol"
    assert len(first["spec"]["turn_plan"]) == 6
    assert first["spec"]["maximum_tasks_per_turn"] == 1
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["retained_field_reference_patch_authorized"] is False
    assert first["spec"]["proposition_reference_frozen"] is False
    assert first["spec"]["alignment_reference_frozen"] is False
    assert first["spec"]["fresh_diagnostic_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list(root.glob("turns/*/capacity.json"))
    assert not list(root.glob("turns/*/sidecar.json"))
    assert not (root / "terminal.json").exists()
    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["ordered_turn_names"] == first["spec"]["turn_plan"]
    assert policy["phase_total_token_bound"] == 420000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
