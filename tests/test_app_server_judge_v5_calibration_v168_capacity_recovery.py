from __future__ import annotations

import json
from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v167_fresh_full_replacement as v167
from research_factory.app_server_judge_v5_calibration_v168_capacity_recovery import (
    TURN_NAMES,
    _validate_v167_nonlaunch,
    build_v168_inputs,
    freeze_v168,
)


def test_v168_adopts_v167_only_as_presemantic_nonlaunch():
    source = _validate_v167_nonlaunch()
    assert source["values"]["spec"]["state"] == "frozen_before_model_calls"
    assert source["values"]["policy"]["projected_phase_quota_points"] == 46
    assert not (source["root"] / "terminal.json").exists()
    assert not list(source["root"].rglob("capacity.json"))
    assert not list(source["root"].rglob("sidecar.json"))
    assert not list(source["root"].rglob("output.private.json"))
    assert source["cumulative_usage"]["total_tokens"] == 3216474


def test_v168_reduces_only_support_shard_count_and_reuses_no_outputs():
    source = _validate_v167_nonlaunch()
    data = build_v168_inputs(source)
    roles = [row["turn_role"] for row in data["static_turns"]]
    assert len(data["support_case_shards"]) == 7
    assert sorted(value for shard in data["support_case_shards"] for value in shard) == sorted(
        row["case_id"] for row in data["pool"]["cases"]
    )
    assert roles.count("support_primary") == 7
    assert roles.count("support_canary") == 1
    assert roles.count("field_singleton") == 28
    assert roles.count("field_repeat_batch") == 1
    assert data["truth"] == source["v166"]["values"]["truth"]
    assert data["selection"]["capacity_recovery_from_v167"] is True
    assert data["selection"]["v167_usage_tokens"] == 0


def test_v168_freeze_is_idempotent_and_preserves_reserve(tmp_path: Path):
    root = tmp_path / "v168"
    first = freeze_v168(output_dir=root)
    second = freeze_v168(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert len(TURN_NAMES) == 64
    assert first["spec"]["minimum_turn_count"] == 64
    assert first["spec"]["maximum_turn_count"] == 64
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["all_semantic_turns_fresh"] is True
    assert first["spec"]["v167_semantic_attempt_replayed"] is False
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["maximum_total_tokens_per_turn"] == 39500
    assert policy["phase_total_token_bound"] == 2528000
    assert policy["projected_phase_quota_points"] == 43
    assert policy["minimum_remaining_reserve_percent"] == 20
    audit = json.loads((root / "capacity-policy-audit.json").read_text())
    assert audit["v167_presemantic_nonlaunch"]["v167_cleared_for_semantic_turn"] is False
    assert audit["v167_presemantic_nonlaunch"]["semantic_usage_tokens"] == 0
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v168_freeze_keeps_selection_and_holdout_closed(tmp_path: Path):
    frozen = freeze_v168(output_dir=tmp_path / "v168")
    assert frozen["spec"]["selection_authorized"] is False
    assert frozen["spec"]["holdout_authorized"] is False
    assert frozen["spec"]["production_mutation_allowed"] is False
    assert frozen["spec"]["prior_model_outputs_reused"] is False


def test_v168_freeze_does_not_mutate_v167(tmp_path: Path):
    paths = [
        v167.DEFAULT_OUTPUT_ROOT / "fresh-full-replacement-spec.json",
        v167.DEFAULT_OUTPUT_ROOT / "capacity-policy.json",
    ]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v168(output_dir=tmp_path / "v168")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
