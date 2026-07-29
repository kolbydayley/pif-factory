from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v179_primary_alignment_phase1 import (
    _validate_v178_success,
    build_primary_partition,
    freeze_v179,
)


def test_v179_preserves_zero_token_v178_scale_authorization():
    predecessor = _validate_v178_success()
    assert predecessor["values"]["terminal"]["full_alignment_authorized"] is True
    assert predecessor["values"]["terminal"]["semantic_turn_count"] == 0
    assert predecessor["values"]["receipt"]["maximum_observed_total_tokens_per_turn"] == 88821
    assert predecessor["cumulative_usage"]["total_tokens"] == 6842317


def test_v179_partition_adopts_one_case_and_freezes_three_nine_case_phases():
    predecessor = _validate_v178_success()
    partition = build_primary_partition(predecessor)
    assert len(partition["all_candidates"]) == 28
    assert len(partition["phases"]) == 3
    assert [len(phase) for phase in partition["phases"]] == [9, 9, 9]
    case_ids = [partition["adopted_case"]["case_id"]] + [
        row["case_id"] for phase in partition["phases"] for row in phase
    ]
    assert len(case_ids) == len(set(case_ids)) == 28
    assert all(
        row["prompt_bytes"] <= 90000 and row["schema_bytes"] <= 20000
        for row in partition["all_candidates"]
    )
    remaining_prompt_sizes = [
        row["prompt_bytes"] for phase in partition["phases"] for row in phase
    ]
    assert remaining_prompt_sizes == sorted(remaining_prompt_sizes, reverse=True)


def test_v179_freeze_is_idempotent_presemantic_and_phase_bounded(tmp_path: Path):
    root = tmp_path / "v179"
    first = freeze_v179(output_dir=root)
    second = freeze_v179(output_dir=root)
    assert first["spec"] == second["spec"]
    assert len(first["spec"]["turn_plan"]) == 9
    assert first["spec"]["v177_primary_turn_replayed"] is False
    assert first["spec"]["v177_primary_output_adopted"] is True
    assert first["spec"]["primary_alignment_complete"] is False
    assert first["spec"]["balanced_canary_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 900000
    assert policy["projected_phase_quota_points"] == 16
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()
