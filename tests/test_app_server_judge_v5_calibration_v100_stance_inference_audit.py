from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v26_diagnostic import _load_json
from research_factory.app_server_judge_v5_calibration_v99_refined_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V99_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v100_stance_inference_audit import (
    build_v100_inputs,
    field_rubric_v100,
    freeze_v100,
    score_v100,
)


def _built():
    return build_v100_inputs(
        v99_input=_load_json(V99_ROOT / "refined-luna-input.private.json", "input"),
        v99_truth=_load_json(V99_ROOT / "refined-luna-truth.private.json", "truth"),
        v99_output=_load_json(V99_ROOT / "refined-luna-output.private.json", "output"),
        v99_canary=_load_json(V99_ROOT / "permutation-canary-output.private.json", "canary"),
        rubric=field_rubric_v100(),
    )


def _decision(task_id: str, status: str) -> dict:
    return {"task_id": task_id, "field_status": status, "source_evidence_spans": ["evidence"], "rationale": "test"}


def test_v100_builds_two_disputes_and_four_blinded_controls():
    value, truth, canary = _built()
    assert value["task_count"] == truth["task_count"] == 6
    assert canary["task_count"] == 2
    roles = Counter(row["role"] for row in truth["tasks"])
    assert roles == {"reference_dispute": 2, "settled_control": 4}
    assert Counter(row["field"] for row in truth["tasks"] if row["role"] == "reference_dispute") == {
        "stance": 1,
        "unsupported_inference": 1,
    }
    rendered = json.dumps(value, sort_keys=True)
    assert "prior_status" not in rendered
    assert "control_expected_status" not in rendered


def test_v100_rubric_separates_functional_recommendation_and_supported_merge():
    rubric = field_rubric_v100()
    assert "describing a capability" in rubric["field_contracts"]["stance"]["decision_rule"]
    assert "merging two supported" in rubric["field_contracts"]["unsupported_inference"]["decision_rule"]
    assert rubric["semantic_regex_or_keyword_rules_used"] is False


def test_v100_score_requires_all_controls_canaries_and_evidence():
    _value, truth, _canary = _built()
    statuses = {}
    owner = []
    for row in truth["tasks"]:
        status = row.get("control_expected_status") or row["prior_status"]
        statuses[row["task_id"]] = status
        owner.append(_decision(row["task_id"], status))
    canary = [_decision(row["canary_task_id"], statuses[row["owner_task_id"]]) for row in truth["canary_map"]]
    score = score_v100({"decisions": owner}, {"decisions": canary}, truth)
    assert score["passed"] is True
    canary[0]["field_status"] = "abstain"
    assert score_v100({"decisions": owner}, {"decisions": canary}, truth)["passed"] is False


def test_v100_freeze_is_three_turns_immutable_and_presemantic(tmp_path: Path):
    root = tmp_path / "v100"
    first = freeze_v100(output_dir=root)
    second = freeze_v100(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["production_mutation_allowed"] is False
    assert len(first["turns"]) == 3
    assert not (root / "terminal.json").exists()
