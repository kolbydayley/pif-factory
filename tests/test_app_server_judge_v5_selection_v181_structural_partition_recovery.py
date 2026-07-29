from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from research_factory.app_server_judge_v5_selection_v181_structural_partition_recovery import (
    JudgeV5SelectionV181Error,
    _validate_v180_failure,
    freeze_v181,
    project_structural_unpaired_and_exact_spans,
)


def test_v181_adopts_both_v180_turns_without_replay_and_preserves_usage():
    predecessor = _validate_v180_failure()
    assert predecessor["values"]["terminal"]["state"] == "failed"
    assert predecessor["values"]["failure"]["error_class"] == "JudgeV5CalibrationV26DiagnosticError"
    assert predecessor["usage"]["total_tokens"] == 151024
    assert len(predecessor["completed"]) == 2
    assert predecessor["completed"][0]["audit"]["structural_unpaired_addition_count"] == 0
    assert predecessor["completed"][1]["audit"]["structural_unpaired_addition_count"] == 50
    assert len(predecessor["normalized"]["cases"]) == 3


def test_v181_projection_changes_only_structural_unpaired_coverage():
    predecessor = _validate_v180_failure()
    item = predecessor["completed"][1]
    projected, audit = project_structural_unpaired_and_exact_spans(
        item["output"], item["row"]["value"]
    )
    assert audit["structural_unpaired_addition_count"] == 50
    assert audit["semantic_equivalence_groups_changed"] is False
    assert audit["semantic_alignment_pairs_changed"] is False
    assert audit["semantic_checklist_decisions_changed"] is False
    assert projected["cases"][0]["equivalence_groups"] == item["output"]["cases"][0]["equivalence_groups"]
    assert projected["cases"][0]["alignment_pairs"] == item["output"]["cases"][0]["alignment_pairs"]


def test_v181_projection_rejects_non_structural_defects():
    predecessor = _validate_v180_failure()
    item = predecessor["completed"][1]
    broken = deepcopy(item["output"])
    broken["cases"][0]["equivalence_groups"] = []
    with pytest.raises(JudgeV5SelectionV181Error):
        project_structural_unpaired_and_exact_spans(broken, item["row"]["value"])


def test_v181_freeze_is_idempotent_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v181"
    first = freeze_v181(output_dir=root)
    second = freeze_v181(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["v180_completed_turns_replayed"] is False
    assert first["spec"]["v180_completed_turns_adopted"] == 2
    assert first["spec"]["fresh_case_count"] == 2
    assert first["spec"]["structural_projection_changes_semantic_groups_pairs_or_checklists"] is False
    assert len(first["spec"]["turn_plan"]) == 2
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 320000
    assert policy["projected_phase_quota_points"] == 6
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v181_runs_only_v180_never_started_cases(tmp_path: Path):
    frozen = freeze_v181(output_dir=tmp_path / "v181")
    partition = frozen["predecessor"]["partition"]["phases"][0]
    assert [turn["case_id"] for turn in frozen["turns"]] == [
        row["case_id"] for row in partition[3:5]
    ]
    adopted = {row["case_id"] for row in frozen["adopted_normalized"]["cases"]}
    assert not adopted.intersection(turn["case_id"] for turn in frozen["turns"])
