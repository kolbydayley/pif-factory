from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v185_primary_phase2 import (
    _validate_v184_success,
    freeze_v185,
)


def test_v185_validates_v184_success_and_preserves_unknown_predecessor_usage():
    predecessor = _validate_v184_success()
    assert predecessor["values"]["terminal"]["state"] == "completed"
    assert predecessor["usage"]["total_tokens"] == 62719
    assert predecessor["cumulative_known_lower_bound"]["total_tokens"] == 7682501
    assert predecessor["cumulative_unknown_usage_turn_count"] == 1
    assert predecessor["cumulative_unknown_usage_upper_bound"] == 120000
    assert len(predecessor["normalized"]["cases"]) == 9


def test_v185_freeze_runs_exact_phase_two_without_replay(tmp_path: Path):
    root = tmp_path / "v185"
    first = freeze_v185(output_dir=root)
    second = freeze_v185(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["v184_turns_replayed"] is False
    assert first["spec"]["adopted_phase1_case_count"] == 9
    assert first["spec"]["fresh_phase2_case_count"] == 9
    partition = first["predecessor"]["partition"]["phases"][1]
    assert [turn["case_id"] for turn in first["turns"]] == [
        row["case_id"] for row in partition
    ]
    assert len({turn["case_id"] for turn in first["turns"]}) == 9


def test_v185_freeze_is_bounded_structural_and_presemantic(tmp_path: Path):
    root = tmp_path / "v185"
    frozen = freeze_v185(output_dir=root)
    spec = frozen["spec"]
    assert spec["structural_projection_changes_semantic_groups_pairs_or_checklists"] is False
    assert spec["semantic_fields_pruned"] is False
    assert spec["semantic_similarity_used"] is False
    assert spec["semantic_regex_or_keyword_rules_used"] is False
    assert spec["predecessor_cumulative_usage_status"] == "unknown"
    assert spec["predecessor_unknown_usage_turn_count"] == 1
    policy = json.loads(frozen["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 1440000
    assert policy["projected_phase_quota_points"] == 25
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()
