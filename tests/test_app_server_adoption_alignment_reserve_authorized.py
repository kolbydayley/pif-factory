from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import app_server_adoption_alignment_reserve_authorized as v3


def test_v2_predecessor_is_immutable_and_presemantic() -> None:
    predecessor = v3.validate_v2_predecessor()

    assert predecessor["blocker"]["recovery_semantic_attempt_started"] is False
    assert predecessor["blocker"]["production_mutated"] is False
    assert predecessor["lock_record"]["sha256"] == (
        "5f0ec4f63f9c0e36c20a276ae8623114bec1a028b79667c47b22f9c7106bf258"
    )


def test_authorized_policy_clears_last_measured_headroom(tmp_path: Path) -> None:
    frozen = v3.freeze_recovery(output_dir=tmp_path / "v3")
    policy = v3.load_authorized_capacity_policy(frozen["capacity_policy"])
    evaluation = v3.reserve.evaluate_reserve_capacity(
        {"primary_used_percent": 81, "rate_limit_reached_type": None},
        policy=policy,
        remaining_turn_count=2,
    )

    assert evaluation["minimum_remaining_reserve_percent"] == 10
    assert evaluation["projected_remaining_quota_points"] == 7
    assert evaluation["projected_terminal_remaining_percent"] == 12
    assert evaluation["cleared_for_semantic_turn"] is True


def test_authorized_policy_still_fails_closed_at_insufficient_headroom(
    tmp_path: Path,
) -> None:
    frozen = v3.freeze_recovery(output_dir=tmp_path / "v3")
    policy = v3.load_authorized_capacity_policy(frozen["capacity_policy"])

    insufficient = v3.reserve.evaluate_reserve_capacity(
        {"primary_used_percent": 84, "rate_limit_reached_type": None},
        policy=policy,
        remaining_turn_count=2,
    )
    reached = v3.reserve.evaluate_reserve_capacity(
        {"primary_used_percent": 50, "rate_limit_reached_type": "primary"},
        policy=policy,
        remaining_turn_count=2,
    )

    assert insufficient["cleared_for_semantic_turn"] is False
    assert reached["cleared_for_semantic_turn"] is False


def test_v3_freeze_binds_requests_without_semantic_changes(tmp_path: Path) -> None:
    root = tmp_path / "v3"
    frozen = v3.freeze_recovery(output_dir=root)
    lock = v3.verify_runtime_lock(frozen["runtime_lock"])
    authorization = json.loads(
        (root / "operator-authorization.json").read_text(encoding="utf-8")
    )

    assert lock["authorized_minimum_remaining_reserve_percent"] == 10
    assert lock["request_bytes_changed"] is False
    assert lock["support_replayed"] is False
    assert lock["extraction_replayed"] is False
    assert len(lock["frozen_source_request_records"]) == 6
    assert authorization["authority"] == "direct_user_instruction"
    assert authorization["frozen_quality_threshold"] == 0.97


def test_authorization_or_policy_drift_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "v3"
    frozen = v3.freeze_recovery(output_dir=root)
    auth_path = root / "operator-authorization.json"
    authorization = json.loads(auth_path.read_text(encoding="utf-8"))
    authorization["authorized_minimum_remaining_reserve_percent"] = 9
    auth_path.write_text(json.dumps(authorization), encoding="utf-8")

    with pytest.raises(v3.AuthorizedAlignmentRecoveryError):
        v3.verify_runtime_lock(frozen["runtime_lock"])


def test_quality_threshold_is_unchanged() -> None:
    assert v3.v2.semantic.QUALITY_THRESHOLD == 0.97
    assert v3.MODEL == "gpt-5.5"
    assert v3.EFFORT == "high"
