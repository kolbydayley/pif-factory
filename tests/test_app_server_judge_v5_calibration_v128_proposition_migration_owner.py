from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v117_alignment_reference_owner import _validate_v116
from research_factory.app_server_judge_v5_calibration_v121_retained_field_owner import _validate_v106, _validate_v120
from research_factory.app_server_judge_v5_calibration_v128_proposition_migration_owner import (
    _validate_v127,
    build_reference_candidate_v128,
    build_v128_inputs,
    freeze_v128,
    score_v128,
)


def _built():
    predecessor = _validate_v127()
    return predecessor, build_v128_inputs(
        predecessor, _validate_v120(), _validate_v106(), _validate_v116()
    )


def _passing_primary(truth):
    return {
        "decisions": [
            {
                "task_id": row["task_id"],
                "proposition_status": row.get("control_expected_status") or "supported",
                "source_evidence_spans": ["evidence"],
                "rationale": "synthetic proposition decision",
            }
            for row in truth["tasks"]
        ]
    }


def test_v128_migrates_six_labels_and_uses_four_balanced_audited_controls():
    _, (value, truth, canary, selection) = _built()
    assert value["task_count"] == 10
    assert truth["migration_owner_count"] == 6
    assert truth["audited_control_count"] == 4
    assert canary["task_count"] == 6
    assert selection["migration_role_counts"] == {
        "consensus_reference_dispute": 1,
        "legacy_unsupported_definition_migration": 3,
        "model_disagreement": 2,
    }
    assert selection["audited_control_status_counts"] == {
        "supported": 2,
        "unsupported": 2,
    }
    assert selection["legacy_unsupported_labels_treated_as_migration_targets_not_controls"] is True
    assert selection["prior_labels_in_model_input"] is False
    assert selection["prior_model_decisions_in_model_input"] is False
    assert value["system_identity_present"] is False


def test_v128_score_requires_controls_canary_evidence_and_decisive_owners():
    _, (_, truth, _, _) = _built()
    primary = _passing_primary(truth)
    owner = {row["task_id"]: row for row in primary["decisions"]}
    canary = {
        "decisions": [
            {**owner[row["owner_task_id"]], "task_id": row["canary_task_id"]}
            for row in truth["canary_map"]
        ]
    }
    passed = score_v128(primary, canary, truth)
    assert passed["passed"] is True
    assert passed["metrics"]["audited_control_exact_count"] == 4
    assert passed["metrics"]["permutation_canary_exact_count"] == 6
    canary["decisions"][0]["proposition_status"] = "abstain"
    assert score_v128(primary, canary, truth)["passed"] is False


def test_v128_reference_projection_freezes_proposition_but_not_alignment():
    predecessor, (_, truth, _, _) = _built()
    primary = _passing_primary(truth)
    candidate = build_reference_candidate_v128(
        current_reference=predecessor["v126"]["values"]["reference"],
        truth=truth,
        primary=primary,
    )
    assert len(candidate["cases"]) == 66
    assert candidate["retained_proposition_reference_patch_authorized"] is True
    assert candidate["proposition_reference_frozen"] is True
    assert candidate["alignment_reference_frozen"] is False


def test_v128_freeze_is_idempotent_presemantic_and_keeps_later_gates_closed(
    tmp_path: Path,
):
    root = tmp_path / "v128"
    first = freeze_v128(output_dir=root)
    second = freeze_v128(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.4"
    assert first["spec"]["turn_plan"] == [
        "proposition_migration_owner",
        "proposition_migration_owner_canary",
    ]
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["retained_proposition_reference_patch_authorized"] is False
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
    assert policy["phase_total_token_bound"] == 140000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
