from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v121_retained_field_owner import (
    score_v121,
)
from research_factory.app_server_judge_v5_calibration_v123_capped_field_repair import (
    _validate_v122,
    build_v123_inputs,
    freeze_v123,
    reconcile_v122,
    score_v123,
)


def _passing_output(truth):
    return {
        "decisions": [
            {
                "task_id": row["task_id"],
                "field_status": row.get("required_gate_status")
                or row["control_expected_status"],
                "source_evidence_spans": ["evidence"],
                "rationale": "synthetic independent decision",
            }
            for row in truth["tasks"]
        ]
    }


def test_v123_selects_exactly_five_repairs_and_four_distinct_controls():
    value, truth, selection = build_v123_inputs(_validate_v122())
    assert value["task_count"] == 9
    assert truth["repair_task_count"] == 5
    assert truth["matched_control_count"] == 4
    assert selection["trigger_reason_counts"] == {
        "canary_disagreement": 3,
        "matched_control_mismatch": 1,
        "unsupported_consistency": 1,
    }
    assert selection["control_field_count"] == 4
    assert selection["prior_labels_in_model_input"] is False
    assert selection["prior_model_decisions_in_model_input"] is False
    assert selection["trigger_reasons_in_model_input"] is False
    assert value["system_identity_present"] is False
    assert all("trigger_reasons" not in row for row in value["tasks"])


def test_v123_repair_clears_the_full_v121_gate_and_scores_fail_closed():
    predecessor = _validate_v122()
    _, truth, _ = build_v123_inputs(predecessor)
    output = _passing_output(truth)
    reconciled = reconcile_v122(
        primary=predecessor["values"]["primary"],
        truth=truth,
        repair_output=output,
    )
    full_score = score_v121(
        reconciled,
        predecessor["values"]["canary"],
        predecessor["v121"]["values"]["truth"],
    )
    assert full_score["passed"] is True
    passed = score_v123(output, truth, full_score)
    assert passed["passed"] is True
    assert passed["metrics"]["full_v121_matched_control_exact_count"] == 20
    assert passed["metrics"]["full_v121_permutation_canary_exact_count"] == 12
    assert passed["metrics"]["full_v121_unsupported_inference_conflict_count"] == 0

    repair_id = next(
        row["task_id"] for row in truth["tasks"] if row["role"] == "observable_repair"
    )
    next(row for row in output["decisions"] if row["task_id"] == repair_id)[
        "field_status"
    ] = "abstain"
    failed = score_v123(output, truth, {**full_score, "passed": False})
    assert failed["passed"] is False
    assert failed["checks"]["repair_gate_exact_rate"] is False
    assert failed["checks"]["repair_abstention_count"] is False
    assert failed["checks"]["full_v121_gate_recomputed"] is False


def test_v123_freeze_is_idempotent_presemantic_and_keeps_later_gates_closed(
    tmp_path: Path,
):
    root = tmp_path / "v123"
    first = freeze_v123(output_dir=root)
    second = freeze_v123(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.4"
    assert first["spec"]["repair_task_count"] == 5
    assert first["spec"]["matched_control_count"] == 4
    assert first["spec"]["turn_plan"] == ["capped_retained_field_repair"]
    assert first["spec"]["retry_count_per_turn"] == 0
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
    assert policy["ordered_turn_names"] == ["capped_retained_field_repair"]
    assert policy["phase_total_token_bound"] == 70000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
