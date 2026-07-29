from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from research_factory import app_server_judge_v5_calibration_v156_relation_projection_recovery as v156
from research_factory.app_server_judge_v5 import validate_neutral_alignment_output
from research_factory.app_server_judge_v5_calibration_v157_exact_span_canary_recovery import (
    MAXIMUM_TOTAL_TOKENS_PER_TURN,
    TURN_NAMES,
    JudgeV5CalibrationV157Error,
    _validate_v156_failure,
    freeze_v157,
    project_exact_spans_and_relation,
    validate_structurally_projectable_output,
)


def test_v157_preserves_v156_failure_usage_and_attempts():
    source = _validate_v156_failure()
    assert source["usage"] == {
        "input_tokens": 276214,
        "cached_input_tokens": 0,
        "output_tokens": 124860,
        "reasoning_output_tokens": 41789,
        "total_tokens": 401074,
    }
    assert source["cumulative_usage"]["total_tokens"] == 1435509
    assert len(source["attempts"]) == 11
    assert source["canary_00_audit"]["dropped_nonexact_span_count"] == 3
    assert validate_neutral_alignment_output(
        source["projected_canary_00"], source["values"]["failed_canary_input"]
    ) == []


def test_v157_projection_changes_only_structural_span_and_relation_fields():
    source = _validate_v156_failure()
    raw = source["values"]["failed_canary_output"]
    alignment_input = source["values"]["failed_canary_input"]
    projected, audit = project_exact_spans_and_relation(raw, alignment_input)
    assert audit["dropped_nonexact_span_count"] == 3
    assert audit["affected_checklist_count"] == 3
    assert audit["semantic_checklist_decisions_changed"] is False
    before = deepcopy(raw)
    after = deepcopy(projected)
    for before_case, after_case in zip(before["cases"], after["cases"], strict=True):
        for before_pair, after_pair in zip(
            before_case["alignment_pairs"], after_case["alignment_pairs"], strict=True
        ):
            before_pair["relation"] = "ignored"
            after_pair["relation"] = "ignored"
            for before_row, after_row in zip(
                before_pair["checklist"], after_pair["checklist"], strict=True
            ):
                before_row["source_evidence_spans"] = []
                after_row["source_evidence_spans"] = []
    assert before == after


def test_v157_projectable_validator_rejects_semantic_partition_error():
    source = _validate_v156_failure()
    alignment_input = source["values"]["failed_canary_input"]
    bad = deepcopy(source["values"]["failed_canary_output"])
    bad["cases"][0]["unpaired_witness_ids"].append("not_a_witness")
    errors = validate_structurally_projectable_output(bad, alignment_input)
    assert any(error.endswith("_alignment_partition_mismatch") for error in errors)
    with pytest.raises(JudgeV5CalibrationV157Error):
        project_exact_spans_and_relation(bad, alignment_input)


def test_v157_freeze_is_idempotent_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v157"
    first = freeze_v157(output_dir=root)
    second = freeze_v157(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert len(TURN_NAMES) == 2
    assert first["spec"]["minimum_turn_count"] == 1
    assert first["spec"]["maximum_turn_count"] == 2
    assert first["spec"]["predecessor_semantic_turn_count_reused"] == 57
    assert first["spec"]["predecessor_turn_count_replayed"] == 0
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    policy = json.loads((root / "capacity-policy.json").read_text())
    assert policy["phase_total_token_bound"] == len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v157_freeze_does_not_mutate_v156(tmp_path: Path):
    root = v156.DEFAULT_OUTPUT_ROOT
    paths = [root / "terminal.json", root / "failure.json"]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v157(output_dir=tmp_path / "v157")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
