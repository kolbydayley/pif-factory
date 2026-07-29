from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory.app_server_judge_v5 import normalize_neutral_alignment_output
from research_factory.app_server_judge_v5_selection_v198_residual_repair_alignment import (
    TURN_NAMES,
    _load_alignment_sources,
    _validate_v197_success,
    build_alignment_turns,
    build_augmented_alignment_pool,
    build_reference_conflict_audit,
    freeze_v198,
    project_alignment_output_v198,
)


def _singleton_output(turn):
    case = turn["value"]["cases"][0]
    witness_ids = [row["witness_id"] for row in case["witnesses"]]
    return {
        "cases": [
            {
                "case_id": case["case_id"],
                "alignment_pairs": [],
                "unpaired_witness_ids": witness_ids,
                "equivalence_groups": [
                    {
                        "witness_ids": [witness_id],
                        "rationale": "Synthetic singleton group.",
                    }
                    for witness_id in witness_ids
                ],
            }
        ]
    }


@pytest.fixture(scope="module")
def alignment_bundle():
    sources = _load_alignment_sources()
    pool, receipts, mapping = build_augmented_alignment_pool(sources)
    turns = build_alignment_turns(pool, receipts, mapping)
    return sources, pool, receipts, mapping, turns


def test_v198_preserves_v197_measured_support_success():
    predecessor = _validate_v197_success()
    assert predecessor["terminal"]["usage"]["total_tokens"] == 25668
    assert predecessor["terminal"]["support_status_counts"] == {
        "supported": 30,
        "unsupported": 0,
        "abstain": 0,
    }


def test_v198_builds_one_neutral_representative_per_reference_group(
    alignment_bundle,
):
    _sources, pool, receipts, mapping, _turns = alignment_bundle
    assert len(pool["cases"]) == 4
    assert mapping["reference_representative_count"] == 100
    assert mapping["repair_witness_count"] == 30
    assert len(receipts["units"]) == 130
    assert len({row["witness_id"] for row in receipts["units"]}) == 130
    assert all(row["proposition_verdict"] == "supported" for row in receipts["units"])
    assert mapping[
        "no_signal_case_excluded_because_both_reference_and_repair_are_empty"
    ] is True


def test_v198_compact_turns_and_structural_projection_are_valid(alignment_bundle):
    _sources, _pool, _receipts, mapping, turns = alignment_bundle
    assert [row["turn_name"] for row in turns] == list(TURN_NAMES)
    assert sum(row["witness_count"] for row in turns) == 130
    assert max(row["prompt_bytes"] for row in turns) <= 90000
    assert max(row["schema_bytes"] for row in turns) <= 20000
    normalized = []
    for turn in turns:
        projected, audit = project_alignment_output_v198(
            _singleton_output(turn), turn["value"]
        )
        assert audit["dropped_nonexact_span_count"] == 0
        normalized.extend(
            normalize_neutral_alignment_output(projected, turn["value"])["cases"]
        )
    audit = build_reference_conflict_audit(normalized, mapping)
    assert audit["frozen_reference_partition_conflict_count"] == 0
    assert audit["repair_witness_count"] == 30


def test_v198_reference_conflict_audit_rejects_reference_merge(alignment_bundle):
    _sources, _pool, _receipts, mapping, turns = alignment_bundle
    normalized = []
    for turn in turns:
        raw = _singleton_output(turn)
        normalized.extend(
            normalize_neutral_alignment_output(raw, turn["value"])["cases"]
        )
    first_mapping = mapping["cases"][0]
    first_case = next(
        row for row in normalized if row["case_id"] == first_mapping["case_id"]
    )
    references = [
        row["representative_witness_id"]
        for row in first_mapping["reference_groups"][:2]
    ]
    first_case["equivalence_groups"] = [
        group
        for group in first_case["equivalence_groups"]
        if group[0] not in set(references)
    ]
    first_case["equivalence_groups"].append(references)
    audit = build_reference_conflict_audit(normalized, mapping)
    assert audit["frozen_reference_partition_conflict_count"] == 1
    assert audit["scoring_safe"] is False


def test_v198_freeze_is_idempotent_and_presemantic(tmp_path: Path):
    root = tmp_path / "v198"
    first = freeze_v198(output_dir=root)
    second = freeze_v198(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["alignment_case_count"] == 4
    assert first["spec"]["frozen_reference_representative_count"] == 100
    assert first["spec"]["supported_repair_witness_count"] == 30
    assert first["spec"]["new_judge_prompt_or_rubric_created"] is False
    assert first["spec"]["scoring_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 400000
    assert policy["projected_phase_quota_points"] == 7
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()
