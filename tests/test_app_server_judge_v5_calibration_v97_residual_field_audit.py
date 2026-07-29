from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v26_diagnostic import _load_json
from research_factory.app_server_judge_v5_calibration_v96_fresh_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V96_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v97_residual_field_audit import (
    DISPUTE_FIELDS,
    TURN_NAMES,
    _requested_field_value,
    build_v97_inputs,
    field_rubric_v97,
    freeze_v97,
    primary_shards,
    score_v97,
)


def _built():
    return build_v97_inputs(
        v96_input=_load_json(V96_ROOT / "fresh-luna-v6-input.private.json", "input"),
        v96_truth=_load_json(V96_ROOT / "fresh-luna-v6-truth.private.json", "truth"),
        v96_output=_load_json(V96_ROOT / "fresh-luna-v6-output.private.json", "output"),
        rubric=field_rubric_v97(),
    )


def _decision(task_id: str, status: str) -> dict:
    return {
        "task_id": task_id,
        "field_status": status,
        "source_evidence_spans": ["evidence"],
        "rationale": "test",
    }


def _passing(truth: dict):
    statuses = {}
    owner = []
    for row in truth["tasks"]:
        status = row.get("control_expected_status") or row["prior_status"]
        statuses[row["task_id"]] = status
        owner.append(_decision(row["task_id"], status))
    canary = [
        _decision(row["canary_task_id"], statuses[row["owner_task_id"]])
        for row in truth["canary_map"]
    ]
    return {"decisions": owner}, {"decisions": canary}


def test_v97_builds_four_disputes_eight_controls_and_explicit_presence():
    value, truth, canary, selection = _built()
    assert value["task_count"] == truth["task_count"] == 12
    assert canary["task_count"] == 4
    roles = Counter(row["role"] for row in truth["tasks"])
    assert roles == {"reference_dispute": 4, "settled_control": 8}
    assert Counter(
        row["field"] for row in truth["tasks"] if row["role"] == "reference_dispute"
    ) == Counter(DISPUTE_FIELDS)
    assert selection["requested_empty_field_presence_explicit"] is True
    assert all("requested_field_value" in task for task in value["tasks"])
    rendered = json.dumps(value, sort_keys=True)
    assert "control_expected_status" not in rendered
    assert "prior_status" not in rendered


def test_v97_requested_field_projection_does_not_borrow_other_roles():
    event = {"actor_name": "Analyst", "speaker_name": "Analyst", "metric_direction": "not_applicable"}
    assert _requested_field_value("reported_actor", event) == {
        "presence": "absent",
        "values": {},
    }
    assert _requested_field_value("metric", event) == {"presence": "absent", "values": {}}


def test_v97_rubric_requires_entailment_not_exact_text_only():
    rubric = field_rubric_v97()
    evidence = rubric["field_contracts"]["evidence"]["decision_rule"]
    assert "necessary but not sufficient" in evidence
    assert "materially support every material claim" in evidence
    assert rubric["semantic_regex_or_keyword_rules_used"] is False


def test_v97_shards_cover_all_tasks_once():
    value, _truth, canary, _selection = _built()
    shards = primary_shards(value)
    assert len(shards) + 1 == len(TURN_NAMES) == 5
    ids = [task["task_id"] for shard in shards for task in shard["tasks"]]
    assert ids == [task["task_id"] for task in value["tasks"]]
    assert len(ids) == len(set(ids)) == 12
    assert len({task["task_id"] for task in canary["tasks"]}) == 4


def test_v97_score_is_fail_closed_on_controls_canaries_or_abstention():
    _value, truth, _canary_input, _selection = _built()
    owner, canary = _passing(truth)
    assert score_v97(owner, canary, truth)["passed"] is True
    canary["decisions"][0]["field_status"] = "abstain"
    failed = score_v97(owner, canary, truth)
    assert failed["passed"] is False
    assert failed["reference_patch_authorized"] is False


def test_v97_freeze_is_five_turns_immutable_and_presemantic(tmp_path: Path):
    root = tmp_path / "v97"
    first = freeze_v97(output_dir=root)
    second = freeze_v97(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.5"
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["production_mutation_allowed"] is False
    assert len(first["turns"]) == 5
    assert not (root / "terminal.json").exists()
