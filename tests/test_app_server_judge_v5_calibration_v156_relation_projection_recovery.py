from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from research_factory import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from research_factory.app_server_judge_v5 import validate_neutral_alignment_output
from research_factory.app_server_judge_v5_calibration_v156_relation_projection_recovery import (
    MAXIMUM_TOTAL_TOKENS_PER_TURN,
    TURN_NAMES,
    JudgeV5CalibrationV156Error,
    _build_turns,
    _validate_v155_failure,
    freeze_v156,
    project_relation_from_checklist,
    validate_relation_projectable_output,
)


def test_v156_preserves_v155_failure_usage_and_attempts():
    source = _validate_v155_failure()
    assert source["usage"] == {
        "input_tokens": 991174,
        "cached_input_tokens": 61440,
        "output_tokens": 43261,
        "reasoning_output_tokens": 16060,
        "total_tokens": 1034435,
    }
    assert len(source["attempts"]) == 46
    assert source["projection_audit"]["relation_projection_count"] == 1
    assert validate_neutral_alignment_output(
        source["projected_alignment_00"],
        source["values"]["failed_alignment_input"],
    ) == []


def test_v156_projection_changes_only_redundant_relation():
    source = _validate_v155_failure()
    raw = source["values"]["failed_alignment_output"]
    alignment_input = source["values"]["failed_alignment_input"]
    projected, audit = project_relation_from_checklist(raw, alignment_input)
    assert audit["input_validation_errors"] == [
        "case_2_pair_0_relation_precedence_mismatch"
    ]
    assert audit["relation_projection_count"] == 1
    assert audit["semantic_checklist_decisions_changed"] is False
    raw_without_relation = deepcopy(raw)
    projected_without_relation = deepcopy(projected)
    for before_case, after_case in zip(
        raw_without_relation["cases"], projected_without_relation["cases"], strict=True
    ):
        for before_pair, after_pair in zip(
            before_case["alignment_pairs"], after_case["alignment_pairs"], strict=True
        ):
            before_pair.pop("relation")
            after_pair.pop("relation")
    assert raw_without_relation == projected_without_relation


def test_v156_projectable_validator_rejects_non_relation_errors():
    source = _validate_v155_failure()
    alignment_input = source["values"]["failed_alignment_input"]
    bad = deepcopy(source["values"]["failed_alignment_output"])
    bad["cases"][0]["unpaired_witness_ids"].append("not_a_witness")
    errors = validate_relation_projectable_output(bad, alignment_input)
    assert any(error.endswith("_alignment_partition_mismatch") for error in errors)
    with pytest.raises(JudgeV5CalibrationV156Error):
        project_relation_from_checklist(bad, alignment_input)


def test_v156_turns_cover_only_missing_primary_and_canaries():
    source = _validate_v155_failure()
    turns = _build_turns(source)
    assert len(turns) == 12
    assert sum(row["turn_role"] == "alignment_primary" for row in turns) == 10
    assert sum(row["turn_role"] == "alignment_canary" for row in turns) == 2
    assert all(len(row["case_ids"]) == 6 for row in turns)
    first_v155_case_ids = {
        case["case_id"] for case in source["values"]["failed_alignment_input"]["cases"]
    }
    fresh_primary_ids = {
        case_id
        for row in turns
        if row["turn_role"] == "alignment_primary"
        for case_id in row["case_ids"]
    }
    assert not (first_v155_case_ids & fresh_primary_ids)
    assert len(first_v155_case_ids | fresh_primary_ids) == 66


def test_v156_freeze_is_idempotent_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v156"
    first = freeze_v156(output_dir=root)
    second = freeze_v156(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert len(TURN_NAMES) == 13
    assert first["spec"]["minimum_turn_count"] == 12
    assert first["spec"]["maximum_turn_count"] == 13
    assert first["spec"]["v155_completed_turn_count_reused"] == 46
    assert first["spec"]["v155_turn_count_replayed"] == 0
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    policy = json.loads((root / "capacity-policy.json").read_text())
    assert policy["phase_total_token_bound"] == len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v156_freeze_does_not_mutate_v155(tmp_path: Path):
    root = v155.DEFAULT_OUTPUT_ROOT
    paths = [root / "terminal.json", root / "failure.json"]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v156(output_dir=tmp_path / "v156")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
