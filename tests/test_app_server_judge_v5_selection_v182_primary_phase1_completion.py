from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v182_primary_phase1_completion import (
    _validate_v181_success,
    freeze_v182,
)


def test_v182_preserves_complete_v181_accounting_and_five_cases():
    predecessor = _validate_v181_success()
    assert predecessor["values"]["terminal"]["state"] == "completed"
    assert predecessor["usage"]["total_tokens"] == 154981
    assert predecessor["cumulative_usage"]["total_tokens"] == 7258599
    assert len(predecessor["normalized"]["cases"]) == 5
    assert len(predecessor["completed"]) == 2


def test_v182_freeze_runs_exact_final_four_phase1_cases(tmp_path: Path):
    root = tmp_path / "v182"
    first = freeze_v182(output_dir=root)
    second = freeze_v182(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["adopted_case_count"] == 5
    assert first["spec"]["fresh_case_count"] == 4
    assert len(first["spec"]["turn_plan"]) == 4
    partition = first["predecessor"]["partition"]["phases"][0]
    assert [turn["case_id"] for turn in first["turns"]] == [
        row["case_id"] for row in partition[5:]
    ]
    adopted = {row["case_id"] for row in first["adopted_normalized"]["cases"]}
    assert not adopted.intersection(turn["case_id"] for turn in first["turns"])


def test_v182_freeze_is_bounded_structural_and_presemantic(tmp_path: Path):
    root = tmp_path / "v182"
    frozen = freeze_v182(output_dir=root)
    assert frozen["spec"]["structural_projection_changes_semantic_groups_pairs_or_checklists"] is False
    assert frozen["spec"]["semantic_fields_pruned"] is False
    assert frozen["spec"]["semantic_similarity_used"] is False
    assert frozen["spec"]["semantic_regex_or_keyword_rules_used"] is False
    policy = json.loads(frozen["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 640000
    assert policy["projected_phase_quota_points"] == 11
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()
