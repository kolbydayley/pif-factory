from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v196_residual_repair_support import (
    TURN_NAME,
    _repair_pool,
    _validate_v195_authorization,
    freeze_v196,
)
from research_factory.app_server_judge_v5 import build_pointwise_support_input


def test_v196_builds_side_free_pool_for_all_repaired_events():
    predecessor = _validate_v195_authorization()
    pool, mapping = _repair_pool(predecessor)
    pointwise = build_pointwise_support_input(pool)
    assert len(pool["cases"]) == 5
    assert len(pointwise["units"]) == 30
    assert pointwise["side_labels_present"] is False
    assert pointwise["system_identity_present"] is False
    assert len(mapping["cases"]) == 5


def test_v196_freeze_reuses_frozen_support_rubric_and_is_presemantic(tmp_path: Path):
    root = tmp_path / "v196"
    first = freeze_v196(output_dir=root)
    second = freeze_v196(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == [TURN_NAME]
    assert first["spec"]["witness_count"] == 30
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["support_rubric_changed"] is False
    assert first["spec"]["side_labels_present"] is False
    assert first["spec"]["system_identity_present"] is False
    assert first["spec"]["alignment_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 80000
    assert policy["projected_phase_quota_points"] == 2
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
