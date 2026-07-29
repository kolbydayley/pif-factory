from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v26_diagnostic import _load_json
from research_factory.app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import V23_ROOT
from research_factory.app_server_judge_v5_calibration_v90_reference_v5_freeze import (
    DEFAULT_OUTPUT_ROOT as V90_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v94_systematic_field_audit import (
    CHALLENGE_COUNTS,
    CONTROL_COUNT,
    PRIMARY_TASK_COUNT,
    TURN_NAMES,
    _write_failure,
    build_v94_inputs,
    field_rubric_v94,
    freeze_v94,
    primary_shards,
    score_v94,
)


def _load_sources():
    return (
        _load_json(V23_ROOT / "pointwise-input-full.private.json", "pointwise"),
        _load_json(V90_ROOT / "calibration-truth-v5.private.json", "reference"),
    )


def _built():
    pointwise, reference = _load_sources()
    return build_v94_inputs(
        pointwise=pointwise,
        reference=reference,
        rubric=field_rubric_v94(),
    )


def _decision(task_id: str, status: str) -> dict:
    return {
        "task_id": task_id,
        "field_status": status,
        "source_evidence_spans": ["evidence"],
        "rationale": "bounded test decision",
    }


def _passing_outputs(truth: dict) -> tuple[dict, dict]:
    statuses = {}
    owner = []
    for row in truth["tasks"]:
        status = (
            row["control_expected_status"]
            if row["role"] == "settled_control"
            else row["prior_status"]
        )
        statuses[row["task_id"]] = status
        owner.append(_decision(row["task_id"], status))
    canary = [
        _decision(row["canary_task_id"], statuses[row["owner_task_id"]])
        for row in truth["canary_map"]
    ]
    return {"decisions": owner}, {"decisions": canary}


def test_v94_rubric_defines_neutral_medium_without_semantic_rules():
    rubric = field_rubric_v94()
    certainty = rubric["field_contracts"]["certainty"]["decision_rule"]
    assert "Medium is a valid neutral mapping" in certainty
    assert "never upgrade medium to high" in certainty
    assert rubric["semantic_regex_or_keyword_rules_used"] is False
    assert rubric["majority_voting_used"] is False


def test_v94_builds_complete_challenge_scope_and_blinded_controls():
    value, truth, canary, selection = _built()
    assert value["task_count"] == PRIMARY_TASK_COUNT == 35
    assert len(value["tasks"]) == len(truth["tasks"]) == 35
    assert canary["task_count"] == 6
    roles = Counter(row["role"] for row in truth["tasks"])
    assert roles == {"reference_challenge": 25, "settled_control": CONTROL_COUNT}
    challenge_fields = Counter(
        row["field"] for row in truth["tasks"] if row["role"] == "reference_challenge"
    )
    assert challenge_fields == CHALLENGE_COUNTS
    assert selection["empty_event_fields_omitted_only"] is True
    assert selection["semantic_pruning_performed"] is False
    rendered = json.dumps(value, sort_keys=True)
    assert "control_expected_status" not in rendered
    assert "prior_status" not in rendered
    assert "reference_challenge" not in rendered
    assert all(
        child not in (None, "", [], {})
        for task in value["tasks"]
        for child in task["structured_event"].values()
    )


def test_v94_shards_cover_every_task_once_in_fixed_order():
    value, _truth, canary, _selection = _built()
    shards = primary_shards(value)
    assert len(shards) + 1 == len(TURN_NAMES) == 10
    assert [len(row["tasks"]) for row in shards] == [4] * 8 + [3]
    ids = [task["task_id"] for shard in shards for task in shard["tasks"]]
    assert ids == [task["task_id"] for task in value["tasks"]]
    assert len(ids) == len(set(ids)) == 35
    assert len({row["task_id"] for row in canary["tasks"]}) == 6


def test_v94_score_passes_only_with_all_controls_canaries_and_evidence():
    _value, truth, _canary_input, _selection = _built()
    owner, canary = _passing_outputs(truth)
    score = score_v94(owner, canary, truth)
    assert score["passed"] is True
    assert score["reference_patch_authorized"] is True
    assert score["reference_freeze_authorized"] is False
    assert len(score["reference_patch_proposal"]) == 25
    assert score["metrics"]["settled_control_exact_rate"] == 1.0
    assert score["metrics"]["permutation_canary_exact_rate"] == 1.0
    assert score["metrics"]["evidence_complete_rate"] == 1.0


def test_v94_score_fails_closed_on_control_or_canary_drift():
    _value, truth, _canary_input, _selection = _built()
    owner, canary = _passing_outputs(truth)
    control_id = next(
        row["task_id"] for row in truth["tasks"] if row["role"] == "settled_control"
    )
    decision = next(row for row in owner["decisions"] if row["task_id"] == control_id)
    decision["field_status"] = "incorrect" if decision["field_status"] == "correct" else "correct"
    assert score_v94(owner, canary, truth)["passed"] is False

    owner, canary = _passing_outputs(truth)
    canary["decisions"][0]["field_status"] = (
        "incorrect" if canary["decisions"][0]["field_status"] == "correct" else "correct"
    )
    failed = score_v94(owner, canary, truth)
    assert failed["passed"] is False
    assert failed["checks"]["permutation_canary_exact_rate"] is False


def test_v94_score_fails_closed_on_exact_payload_inconsistency():
    _value, truth, _canary_input, _selection = _built()
    grouped = defaultdict(list)
    for row in truth["tasks"]:
        if row["role"] == "reference_challenge":
            grouped[row["payload_group"]].append(row["task_id"])
    repeated = next(ids for ids in grouped.values() if len(ids) > 1)
    owner, canary = _passing_outputs(truth)
    by_id = {row["task_id"]: row for row in owner["decisions"]}
    by_id[repeated[0]]["field_status"] = "correct"
    by_id[repeated[1]]["field_status"] = "incorrect"
    score = score_v94(owner, canary, truth)
    assert score["passed"] is False
    assert score["checks"]["exact_payload_consistency"] is False


def test_v94_freeze_is_immutable_and_presemantic(tmp_path: Path):
    root = tmp_path / "v94"
    first = freeze_v94(output_dir=root)
    second = freeze_v94(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["state"] == "frozen_before_model_calls"
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["production_mutation_allowed"] is False
    assert len(first["turns"]) == 10
    assert not (root / "terminal.json").exists()
    assert not list(root.glob("*.sidecar.private.json"))


def test_v94_failure_terminal_uses_infrastructure_classification(tmp_path: Path):
    terminal = _write_failure(tmp_path, "systematic_field_shard_00", "SyntheticError")
    assert terminal["state"] == "failed"
    assert terminal["terminal_reason"] == "infrastructure_or_judge_attempt_failed"
    assert terminal["reference_patch_authorized"] is False
    assert terminal["production_mutated"] is False
    assert terminal["usage_status"] == "complete"
    assert terminal["usage"]["total_tokens"] == 0
