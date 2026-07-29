from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v187_composite_stratified_diagnostic import (
    SELECTED_CASE_IDS,
    SELECTED_SOURCE_IDS,
    _selected_rows,
    _validate_v186_checkpoint,
    freeze_v187,
)


def test_v187_validates_v186_nonacceptance_without_replaying_v185():
    predecessor = _validate_v186_checkpoint()
    assert predecessor["values"]["terminal"]["state"] == "inactive"
    assert predecessor["stop"]["usage"]["total_tokens"] == 405354
    assert predecessor["cumulative_known_lower_bound"]["total_tokens"] == 8087855
    assert predecessor["cumulative_unknown_usage_turn_count"] == 1


def test_v187_selects_largest_unjudged_case_in_each_critical_source():
    predecessor = _validate_v186_checkpoint()
    rows = _selected_rows(predecessor)
    assert tuple(row["case_id"] for row in rows) == SELECTED_CASE_IDS
    assert SELECTED_SOURCE_IDS == ("odd-lots", "latent-space")
    assert [row["prompt_bytes"] for row in rows] == [69202, 67047]


def test_v187_freeze_is_two_turn_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v187"
    first = freeze_v187(output_dir=root)
    second = freeze_v187(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["selected_case_ids"] == list(SELECTED_CASE_IDS)
    assert first["spec"]["selected_source_ids"] == list(SELECTED_SOURCE_IDS)
    assert first["spec"]["existing_extraction_outputs_only"] is True
    assert first["spec"]["extraction_model_calls_allowed"] is False
    assert first["spec"]["new_judge_prompt_or_rubric_created"] is False
    assert len(first["spec"]["turn_plan"]) == 2
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 320000
    assert policy["projected_phase_quota_points"] == 6
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()
