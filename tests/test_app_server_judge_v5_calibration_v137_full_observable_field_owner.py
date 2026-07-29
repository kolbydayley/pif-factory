from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v126_singleton_field_owner import (
    _validate_v125,
)
from research_factory.app_server_judge_v5_calibration_v137_full_observable_field_owner import (
    CANARY_TURNS,
    OBSERVABLE_COUNTS,
    PRIMARY_TURNS,
    TURN_NAMES,
    _validate_v136,
    build_reference_v137,
    build_v137_inputs,
    freeze_v137,
    score_v137,
)


def _expected_outputs(truth):
    primary = []
    for row in truth["tasks"]:
        status = (
            row["control_expected_status"]
            if row["role"] == "control"
            else row["current_status"]
        )
        primary.append(
            {
                "task_id": row["task_id"],
                "field_status": status,
                "source_evidence_spans": ["evidence"],
                "rationale": "expected",
            }
        )
    primary_by_id = {row["task_id"]: row for row in primary}
    canary = [deepcopy(primary_by_id[task_id]) for task_id in truth["canary_task_ids"]]
    return {"decisions": primary}, {"decisions": canary}


def test_v137_covers_all_48_disputes_and_reverses_identical_canary_membership():
    v136 = _validate_v136()
    rows, truth, selection, inherited = build_v137_inputs(
        v134_source=v136["v134"],
        v136_source=v136,
        controls_source=_validate_v125(),
    )

    assert selection["observable_disagreement_counts"] == OBSERVABLE_COUNTS
    assert selection["observable_owner_task_count"] == 48
    assert selection["inherited_v136_owner_task_count"] == 6
    assert selection["new_owner_task_count"] == 42
    assert selection["primary_turn_count"] == len(PRIMARY_TURNS) == 12
    assert selection["order_canary_turn_count"] == len(CANARY_TURNS) == 9
    assert selection["canary_marker_in_model_input"] is False
    assert selection["prior_labels_in_model_input"] is False
    assert selection["prior_model_decisions_in_model_input"] is False
    assert selection["majority_voting_used"] is False
    assert len(rows) == len(TURN_NAMES) == 21
    assert len(inherited) == 6
    assert len(truth["inherited_owner_tasks"]) == 6
    assert len([row for row in truth["tasks"] if row["role"] == "owner"]) == 42

    primary = {
        row["field"]: row
        for row in rows
        if row["turn_role"] == "primary" and row["shard"] == 0
    }
    canary = {
        row["field"]: row for row in rows if row["turn_role"] == "order_canary"
    }
    for field, source in primary.items():
        repeated = canary[field]
        assert "permutation_canary" not in repeated["value"]
        assert repeated["value"]["tasks"] == list(reversed(source["value"]["tasks"]))


def test_v137_score_and_reference_patch_require_all_frozen_gates():
    v136 = _validate_v136()
    _, truth, _, _ = build_v137_inputs(
        v134_source=v136["v134"],
        v136_source=v136,
        controls_source=_validate_v125(),
    )
    primary, canary = _expected_outputs(truth)
    passed = score_v137(primary=primary, canary=canary, truth=truth)

    assert passed["passed"] is True
    assert passed["reference_patch_authorized"] is True
    assert passed["fresh_pointwise_diagnostic_authorized"] is True
    assert passed["fresh_full_calibration_authorized"] is False
    reference = build_reference_v137(
        current_reference=v136["v134"]["v133"]["v132"]["values"]["reference"],
        primary=primary,
        truth=truth,
    )
    assert len(reference["cases"]) == 66
    assert reference["v137_observable_owner_task_count"] == 48

    canary["decisions"][0]["field_status"] = (
        "incorrect"
        if canary["decisions"][0]["field_status"] == "correct"
        else "correct"
    )
    failed = score_v137(primary=primary, canary=canary, truth=truth)
    assert failed["passed"] is False
    assert failed["checks"]["order_canary_exact_rate"] is False
    assert failed["reference_patch_authorized"] is False


def test_v137_freeze_is_idempotent_presemantic_and_21_turn_bounded(tmp_path: Path):
    root = tmp_path / "v137"
    first = freeze_v137(output_dir=root)
    second = freeze_v137(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-sol"
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["new_owner_task_count"] == 42
    assert first["spec"]["inherited_owner_task_count"] == 6
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["reference_patch_authorized"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False

    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 1470000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v137_predecessors_are_immutable(tmp_path: Path):
    before_v136 = _validate_v136()["records"]
    before_v121 = _validate_v125()["v124"]["v122"]["v121"]["records"]
    freeze_v137(output_dir=tmp_path / "v137")
    after_v136 = _validate_v136()["records"]
    after_v121 = _validate_v125()["v124"]["v122"]["v121"]["records"]

    assert before_v136 == after_v136
    assert before_v121 == after_v121
