from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import (
    app_server_judge_v5_selection_v219_feasibility_blocker as v219,
)


@pytest.fixture(scope="module")
def frozen_v219(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict]:
    root = tmp_path_factory.mktemp("v219") / "attempt"
    terminal = v219.freeze_v219(output_dir=root)
    return root, terminal


def test_v219_binds_the_passing_v218_canary():
    checkpoint = v219._validate_v218_checkpoint()
    assert checkpoint["terminal"]["state"] == "completed"
    assert checkpoint["gate"]["passed"] is True
    assert checkpoint["sidecar"]["usage"]["total_tokens"] == 33_423
    assert checkpoint["terminal"]["cumulative_known_usage_lower_bound"][
        "total_tokens"
    ] == 9_086_613
    assert checkpoint["terminal"]["holdout_authorized"] is False
    assert checkpoint["terminal"]["production_mutated"] is False


def test_v219_proves_fixed_and_adaptive_external_paths_infeasible():
    report, private = v219._feasibility_audit()
    assert report["v218_canary_quality_and_cost_passed"] is True
    assert report["fixed_bundle_count"] == 26
    assert report["fixed_cost_and_cap_eligible_count"] == 9
    assert report["fixed_quality_pass_count_under_perfect_support_selector"] == 0
    assert report["selector_projected_full_development_tokens"] == 150_404
    assert report["adaptive_minimum_passing_extraction_tokens"] == 755_000
    assert report["adaptive_remaining_router_and_selector_headroom_tokens"] == (
        58_186.366667
    )
    assert report["selector_tokens_over_adaptive_headroom"] == 92_218
    assert report["fixed_path_feasible"] is False
    assert report["adaptive_external_router_plus_selector_feasible"] is False
    assert private["deterministic_semantic_decisions_made"] is False
    assert private["new_semantic_model_calls"] == 0


def test_v219_freezes_truthful_external_authorization_blocker(frozen_v219):
    root, terminal = frozen_v219
    report = json.loads(
        (root / "full-coverage-feasibility-report.json").read_text()
    )
    assert terminal["state"] == "waiting_for_external_authorization"
    assert terminal["external_blocker"] is True
    assert terminal["blocker_is_quality_failure"] is False
    assert terminal["blocker_is_transport_failure"] is False
    assert terminal["v218_canary_quality_and_cost_passed"] is True
    assert terminal["new_semantic_model_calls"] == 0
    assert terminal["new_extraction_model_calls"] == 0
    assert terminal["cumulative_known_usage_lower_bound"]["total_tokens"] == (
        9_086_613
    )
    assert terminal["development_winner_frozen"] is False
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    assert report["current_goal_prohibits_required_fresh_extraction"] is True


def test_v219_runtime_lock_and_idempotence_are_exact(frozen_v219):
    root, terminal = frozen_v219
    lock = v219.verify_runtime_lock(root / "runtime-lock.json")
    assert len(lock["v218_attempt"]) == 19
    assert len(lock["semantic_sources"]) == 2
    assert lock["semantic_model_calls_authorized"] == 0
    assert lock["production_mutation_allowed"] is False
    assert v219.freeze_v219(output_dir=root) == terminal


def test_v219_runtime_lock_rejects_missing_v218_artifact(frozen_v219):
    root, _terminal = frozen_v219
    lock = json.loads((root / "runtime-lock.json").read_text())
    lock["v218_attempt"] = lock["v218_attempt"][1:]
    mutated = root / "runtime-lock-mutated.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(v219.JudgeV5SelectionV219Error, match="drifted"):
        v219.verify_runtime_lock(mutated)
