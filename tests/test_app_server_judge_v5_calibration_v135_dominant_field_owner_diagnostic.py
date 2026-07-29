from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v126_singleton_field_owner import (
    _validate_v125,
)
from research_factory.app_server_judge_v5_calibration_v135_dominant_field_owner_diagnostic import (
    FIELDS,
    TASKS_PER_TURN,
    TURN_NAMES,
    _validate_v134,
    build_v135_inputs,
    freeze_v135,
    score_v135,
)


def _expected_outputs(truth):
    decisions = []
    for row in truth["tasks"]:
        status = (
            row["control_expected_status"]
            if row["role"] == "control"
            else row["v134_status"]
        )
        decisions.append(
            {
                "task_id": row["task_id"],
                "field_status": status,
                "source_evidence_spans": ["evidence"],
                "rationale": "expected",
            }
        )
    return {"decisions": decisions}


def test_v135_builds_three_field_specific_primary_and_unmarked_canary_pairs():
    rows, truth, selection = build_v135_inputs(_validate_v134(), _validate_v125())

    assert selection["fields"] == list(FIELDS)
    assert selection["candidate_disagreement_counts"] == {
        "attribution": 8,
        "certainty": 11,
        "speaker": 9,
    }
    assert selection["selected_owner_count"] == 6
    assert selection["matched_control_count"] == 6
    assert selection["tasks_per_turn"] == TASKS_PER_TURN == 4
    assert selection["canary_marker_in_model_input"] is False
    assert selection["prior_labels_in_model_input"] is False
    assert selection["prior_model_decisions_in_model_input"] is False
    assert selection["majority_voting_used"] is False
    assert len(rows) == len(TURN_NAMES) == 6
    assert truth["owner_task_count"] == 6
    assert truth["control_task_count"] == 6

    by_field_role = {(row["field"], row["turn_role"]): row for row in rows}
    for field in FIELDS:
        primary = by_field_role[(field, "primary")]["value"]
        canary = by_field_role[(field, "order_canary")]["value"]
        assert "permutation_canary" not in primary
        assert "permutation_canary" not in canary
        assert canary["tasks"] == list(reversed(primary["tasks"]))
        assert {row["task_id"] for row in primary["tasks"]} == {
            row["task_id"] for row in canary["tasks"]
        }


def test_v135_score_requires_all_controls_repeats_evidence_and_no_abstention():
    _, truth, _ = build_v135_inputs(_validate_v134(), _validate_v125())
    primary = _expected_outputs(truth)
    canary = deepcopy(primary)

    passed = score_v135(primary=primary, canary=canary, truth=truth)
    assert passed["passed"] is True
    assert passed["expanded_field_owner_diagnostic_authorized"] is True
    assert passed["reference_patch_authorized"] is False
    assert passed["fresh_full_calibration_authorized"] is False

    canary["decisions"][0]["field_status"] = (
        "incorrect"
        if canary["decisions"][0]["field_status"] == "correct"
        else "correct"
    )
    failed = score_v135(primary=primary, canary=canary, truth=truth)
    assert failed["passed"] is False
    assert failed["checks"]["order_canary_exact_rate"] is False
    assert failed["expanded_field_owner_diagnostic_authorized"] is False


def test_v135_freeze_is_idempotent_presemantic_and_six_turn_bounded(tmp_path: Path):
    root = tmp_path / "v135"
    first = freeze_v135(output_dir=root)
    second = freeze_v135(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-sol"
    assert first["spec"]["fields"] == list(FIELDS)
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["canary_marker_in_model_input"] is False
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["expanded_field_owner_diagnostic_authorized"] is False
    assert first["spec"]["reference_patch_authorized"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False

    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 420000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v135_predecessors_are_immutable(tmp_path: Path):
    before_v134 = _validate_v134()["records"]
    before_v121 = _validate_v125()["v124"]["v122"]["v121"]["records"]
    freeze_v135(output_dir=tmp_path / "v135")
    after_v134 = _validate_v134()["records"]
    after_v121 = _validate_v125()["v124"]["v122"]["v121"]["records"]

    assert before_v134 == after_v134
    assert before_v121 == after_v121
