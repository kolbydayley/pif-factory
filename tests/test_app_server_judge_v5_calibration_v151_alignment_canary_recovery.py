from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5 import validate_neutral_alignment_output
from research_factory.app_server_judge_v5_calibration_v151_alignment_canary_recovery import (
    TURN_NAMES,
    _validate_v150_failure,
    build_v151_inputs,
    freeze_v151,
    project_exact_source_spans,
)


def test_v151_preserves_v150_failure_and_missing_canary_contract():
    source = _validate_v150_failure()

    assert source["usage"]["total_tokens"] == 74124
    assert len(source["attempts"]) == 2
    assert set(source["attempts"]) == {
        "fresh_alignment_primary_00",
        "fresh_alignment_primary_01",
    }
    assert len(build_v151_inputs(source)) == 2


def test_v151_exact_span_projection_drops_only_one_nonexact_speaker_span():
    source = _validate_v150_failure()
    projected, audit = project_exact_source_spans(
        source["outputs"]["fresh_alignment_primary_01"],
        source["inputs"]["fresh_alignment_primary_01"],
    )

    assert audit["dropped_span_count"] == 1
    assert audit["affected_checklist_count"] == 1
    assert audit["affected_rows"][0]["field"] == "speaker"
    assert audit["semantic_decisions_changed"] is False
    assert validate_neutral_alignment_output(
        projected, source["inputs"]["fresh_alignment_primary_01"]
    ) == []


def test_v151_projection_preserves_every_non_evidence_semantic_field():
    source = _validate_v150_failure()
    original = source["outputs"]["fresh_alignment_primary_01"]
    projected, _ = project_exact_source_spans(
        original, source["inputs"]["fresh_alignment_primary_01"]
    )
    stripped_original = deepcopy(original)
    stripped_projected = deepcopy(projected)
    for value in (stripped_original, stripped_projected):
        for case in value["cases"]:
            for pair in case["alignment_pairs"]:
                for row in pair["checklist"]:
                    row.pop("source_evidence_spans")
    assert stripped_original == stripped_projected


def test_v151_freeze_is_idempotent_two_canary_turn_bounded_and_private(tmp_path: Path):
    root = tmp_path / "v151"
    first = freeze_v151(output_dir=root)
    second = freeze_v151(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-luna"
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["reused_primary_turn_count"] == 2
    assert first["spec"]["replayed_primary_turn_count"] == 0
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert (
        first["spec"]["frozen_inputs"]["truth"]
        == first["spec"]["predecessor"]["v150_truth"]
    )
    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 140000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v151_freeze_does_not_mutate_v150(tmp_path: Path):
    before = _validate_v150_failure()["records"]
    freeze_v151(output_dir=tmp_path / "v151")
    after = _validate_v150_failure()["records"]
    assert before == after
