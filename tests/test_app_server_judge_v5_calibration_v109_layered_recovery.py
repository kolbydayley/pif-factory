from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v109_layered_recovery import (
    _validate_v108_terminal,
    finalize_v108_presemantic_failure,
    freeze_v109,
)


def test_v109_finalizes_v108_as_zero_token_presemantic_failure():
    terminal = finalize_v108_presemantic_failure()
    validated = _validate_v108_terminal()

    assert terminal == validated["terminal"]
    assert terminal["state"] == "failed"
    assert terminal["semantic_attempt_started"] is False
    assert terminal["usage"]["total_tokens"] == 0
    assert terminal["production_mutated"] is False


def test_v109_freeze_is_idempotent_and_version_isolated(tmp_path: Path):
    root = tmp_path / "v109"
    first = freeze_v109(output_dir=root)
    second = freeze_v109(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["supersedes_presemantic_attempt"] == "v108"
    assert first["spec"]["v108_semantic_attempt_replayed"] is False
    assert first["spec"]["turn_plan"] == second["spec"]["turn_plan"]
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list((root / "semantic-execution").glob("turns/*/capacity.json"))
    assert not list((root / "semantic-execution").glob("turns/*/sidecar.json"))
    assert not (root / "terminal.json").exists()


def test_v109_capacity_policy_uses_stable_reserve_semantics(tmp_path: Path):
    root = tmp_path / "v109"
    frozen = freeze_v109(output_dir=root)
    policy = json.loads(Path(frozen["capacity_policy"]).read_text())

    assert policy["managed_chatgpt_auth_only"] is True
    assert policy["official_persistent_codex_app_server_only"] is True
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    assert policy["unknown_usage_hard_stop"] is True
    assert policy["semantic_output_root"] == str(root / "semantic-execution")
    assert policy["phase_total_token_bound"] == 630000
    assert policy["projected_phase_quota_points"] == 11
