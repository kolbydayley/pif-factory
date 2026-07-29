from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v189_composite_minimal_alignment import (
    MAXIMUM_TOTAL_TOKENS_PER_TURN,
    TURN_NAME,
    _selected_row,
    _validate_v188_authorization,
    freeze_v189,
)


def test_v189_validates_v188_one_case_authorization():
    predecessor = _validate_v188_authorization()
    assert predecessor["terminal"]["additional_alignment_authorized"] is True
    assert predecessor["next_case"]["critical_source_id"] == "odd-lots"
    row = _selected_row(predecessor)
    assert row["case_id"] == predecessor["next_case"]["case_id"]
    assert row["prompt_bytes"] == 65371
    assert row["witness_count"] == 55


def test_v189_freeze_is_one_turn_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v189"
    first = freeze_v189(output_dir=root)
    second = freeze_v189(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == [TURN_NAME]
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["existing_extraction_outputs_only"] is True
    assert first["spec"]["extraction_model_calls_allowed"] is False
    assert first["spec"]["new_judge_prompt_or_rubric_created"] is False
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == MAXIMUM_TOTAL_TOKENS_PER_TURN
    assert policy["projected_phase_quota_points"] == 3
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()
