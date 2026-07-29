from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import app_server_adoption_alignment_second_turn_recovery as v4


def test_v3_is_one_measured_turn_and_one_unstarted_turn() -> None:
    predecessor = v4.validate_v3_predecessor()

    assert predecessor["first_sidecar"]["usage"]["total_tokens"] == 77_168
    assert predecessor["terminal"]["accounting_complete"] is True
    assert predecessor["terminal"]["recovery_attempted_turn_count"] == 1


def test_id_projection_changes_only_redundant_unpaired_coverage() -> None:
    predecessor = v4.validate_v3_predecessor()
    projected, audit = v4.project_nonsemantic_id_coverage(
        predecessor["first_output"], predecessor["source"]["turns"][0]["value"]
    )

    assert audit["raw_error_classes"] == ["case_0_alignment_partition_mismatch"]
    assert audit["projection_applied"] is True
    assert audit["semantic_payload_changed"] is False
    assert audit["case_audits"][0]["paired_unpaired_overlap_before_count"] == 1
    assert audit["case_audits"][0]["missing_before_count"] == 2
    assert v4.v3.v2.semantic.judge.validate_neutral_alignment_output(
        projected, predecessor["source"]["turns"][0]["value"]
    ) == []


def test_v4_freezes_only_never_started_request(tmp_path: Path) -> None:
    root = tmp_path / "v4"
    frozen = v4.freeze_recovery(output_dir=root)
    lock = v4.verify_runtime_lock(frozen["runtime_lock"])

    assert lock["declared_turn_count"] == 1
    assert lock["completed_first_turn_replayed"] is False
    assert lock["request_bytes_changed"] is False
    assert lock["semantic_projection_performed"] is False
    assert len(lock["second_request_records"]) == 3
    assert lock["minimum_remaining_reserve_percent"] == 10
    assert lock["projected_phase_quota_points"] == 4


def test_one_turn_policy_retains_headroom_at_eighty_four_percent(tmp_path: Path) -> None:
    frozen = v4.freeze_recovery(output_dir=tmp_path / "v4")
    policy = v4.load_capacity_policy(frozen["capacity_policy"])
    decision = v4.reserve.evaluate_reserve_capacity(
        {"primary_used_percent": 84, "rate_limit_reached_type": None},
        policy=policy,
        remaining_turn_count=1,
    )

    assert decision["projected_remaining_quota_points"] == 4
    assert decision["projected_terminal_remaining_percent"] == 12
    assert decision["cleared_for_semantic_turn"] is True


def test_v4_authorization_drift_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "v4"
    frozen = v4.freeze_recovery(output_dir=root)
    path = root / "operator-authorization.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    value["completed_first_turn_replayed"] = True
    path.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(v4.SecondTurnRecoveryError):
        v4.verify_runtime_lock(frozen["runtime_lock"])
