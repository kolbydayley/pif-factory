from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v180_phase1_bound_recovery import (
    MAXIMUM_TOTAL_TOKENS_PER_TURN,
    _validate_v179_policy_failure,
    freeze_v180,
)


def test_v180_preserves_v179_as_complete_usage_policy_failure():
    predecessor = _validate_v179_policy_failure()
    assert predecessor["values"]["terminal"]["state"] == "failed"
    assert predecessor["values"]["failure"]["error_class"] == "ReserveCapacityError"
    assert predecessor["usage"]["total_tokens"] == 110277
    assert predecessor["usage"]["total_tokens"] > 100000
    assert len(predecessor["normalized"]["cases"]) == 1
    assert predecessor["projection_audit"]["dropped_nonexact_span_count"] == 0


def test_v180_freeze_adopts_without_replay_and_bounds_four_fresh_turns(tmp_path: Path):
    root = tmp_path / "v180"
    first = freeze_v180(output_dir=root)
    second = freeze_v180(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["v179_completed_output_replayed"] is False
    assert first["spec"]["v179_completed_output_adopted"] is True
    assert first["spec"]["fresh_case_count"] == 4
    assert len(first["spec"]["turn_plan"]) == 4
    assert first["spec"]["maximum_total_tokens_per_turn"] == 160000
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["maximum_total_tokens_per_turn"] == MAXIMUM_TOTAL_TOKENS_PER_TURN
    assert policy["phase_total_token_bound"] == 640000
    assert policy["projected_phase_quota_points"] == 11
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v180_turns_continue_after_v179_without_overlap(tmp_path: Path):
    frozen = freeze_v180(output_dir=tmp_path / "v180")
    case_ids = [turn["case_id"] for turn in frozen["turns"]]
    partition = frozen["predecessor"]["partition"]["phases"][0]
    assert case_ids == [row["case_id"] for row in partition[1:5]]
    assert frozen["predecessor"]["row"]["case_id"] == partition[0]["case_id"]
    assert frozen["predecessor"]["row"]["case_id"] not in case_ids
