from __future__ import annotations

import json
from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v137_full_observable_field_owner as v137
from research_factory.app_server_judge_v5_calibration_v126_singleton_field_owner import (
    _validate_v125,
)
from research_factory.app_server_judge_v5_calibration_v138_singleton_field_repair import (
    TURN_NAMES,
    _validate_v137,
    build_v138_inputs,
    freeze_v138,
    repair_v137_outputs,
    score_v138,
)


def _expected_outputs(truth):
    outputs = {}
    for index, row in enumerate(truth["tasks"]):
        status = row.get("control_expected_status", "correct")
        outputs[f"turn_{index}"] = {
            "decisions": [
                {
                    "task_id": row["task_id"],
                    "field_status": status,
                    "source_evidence_spans": ["evidence"],
                    "rationale": "expected",
                }
            ]
        }
    return outputs


def test_v138_selects_exactly_four_observable_triggers_and_two_controls():
    rows, truth, selection = build_v138_inputs(_validate_v137(), _validate_v125())

    assert selection["observable_trigger_count"] == 4
    assert selection["trigger_field_role_signature"] == [
        ("certainty", "owner"),
        ("evidence", "control"),
        ("metric", "owner"),
        ("temporal_horizon", "control"),
    ]
    assert selection["independent_control_count"] == 2
    assert selection["singleton_turn_count"] == 6
    assert selection["maximum_tasks_per_turn"] == 1
    assert selection["prior_labels_in_model_input"] is False
    assert selection["prior_model_decisions_in_model_input"] is False
    assert selection["majority_voting_used"] is False
    assert len(rows) == len(TURN_NAMES) == 6
    assert truth["repair_owner_count"] == 2
    assert truth["repair_control_count"] == 2
    assert all(row["value"]["task_count"] == 1 for row in rows)


def test_v138_repair_passes_original_v137_gates_without_relaxation():
    predecessor = _validate_v137()
    _, truth, _ = build_v138_inputs(predecessor, _validate_v125())
    outputs = _expected_outputs(truth)
    repair_score = score_v138(outputs, truth)

    assert repair_score["passed"] is True
    assert repair_score["repaired_v137_rescore_authorized"] is True
    repaired_primary, repaired_canary = repair_v137_outputs(
        outputs=outputs,
        truth=truth,
        primary=predecessor["values"]["primary"],
        canary=predecessor["values"]["canary"],
    )
    rescored = v137.score_v137(
        primary=repaired_primary,
        canary=repaired_canary,
        truth=predecessor["values"]["truth"],
    )
    assert rescored["passed"] is True
    assert rescored["checks"]["matched_control_exact_rate"] is True
    assert rescored["checks"]["order_canary_exact_rate"] is True


def test_v138_freeze_is_idempotent_presemantic_and_six_turn_bounded(tmp_path: Path):
    root = tmp_path / "v138"
    first = freeze_v138(output_dir=root)
    second = freeze_v138(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-sol"
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["observable_trigger_count"] == 4
    assert first["spec"]["maximum_tasks_per_turn"] == 1
    assert first["spec"]["retry_count_per_turn"] == 0
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


def test_v138_predecessors_are_immutable(tmp_path: Path):
    before_v137 = _validate_v137()["records"]
    before_v121 = _validate_v125()["v124"]["v122"]["v121"]["records"]
    freeze_v138(output_dir=tmp_path / "v138")
    after_v137 = _validate_v137()["records"]
    after_v121 = _validate_v125()["v124"]["v122"]["v121"]["records"]

    assert before_v137 == after_v137
    assert before_v121 == after_v121
