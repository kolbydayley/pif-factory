from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from research_factory import app_server_adoption_alignment_recovery as recovery


def test_source_failure_is_zero_turn_capacity_evidence() -> None:
    source = recovery.validate_source_failure()

    assert source["parent_terminal"]["error_class"] == "ReserveCapacityError"
    assert source["parent_terminal"]["attempted_turn_count"] == 0
    assert source["parent_terminal"]["usage"] == recovery._zero_usage()
    assert source["support_terminal"]["alignment_audit_authorized"] is True
    assert len(source["predecessor_manifests"]) == 3


def test_freeze_reuses_exact_requests_and_capacity_path_contract(tmp_path: Path) -> None:
    root = tmp_path / "recovery"
    frozen = recovery.freeze_recovery(output_dir=root)
    lock = recovery.verify_recovery_lock(frozen["runtime_lock"])
    binding = json.loads(
        (root / "alignment" / "request-bindings.json").read_text(encoding="utf-8")
    )

    assert lock["request_bytes_changed"] is False
    assert lock["support_replayed"] is False
    assert lock["extraction_replayed"] is False
    assert len(binding["turns"]) == 2
    for turn_name in recovery.TURN_NAMES:
        paths = recovery.semantic._turn_paths(root / "alignment", turn_name)
        assert paths["capacity"] == (
            root
            / "alignment"
            / "turns"
            / turn_name.replace("_", "-")
            / "capacity.json"
        )


def test_recovery_lock_rejects_request_binding_drift(tmp_path: Path) -> None:
    root = tmp_path / "recovery"
    frozen = recovery.freeze_recovery(output_dir=root)
    path = root / "alignment" / "request-bindings.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    value["request_bytes_changed"] = True
    path.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(recovery.AdoptionAlignmentRecoveryError):
        recovery.verify_recovery_lock(frozen["runtime_lock"])


def test_capacity_stop_before_checkpoint_starts_no_semantic_turn(tmp_path: Path) -> None:
    root = tmp_path / "recovery"

    class StopClient:
        calls = 0

        async def __aenter__(self) -> "StopClient":
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        async def run_ephemeral_structured_turn(self, **kwargs: object) -> None:
            self.calls += 1
            raise recovery.reserve.ReserveCapacityError(
                "minimum_reserve_or_projected_phase_bound_lost"
            )

    client = StopClient()
    terminal = asyncio.run(
        recovery.run_recovery(
            output_dir=root,
            client_factory=lambda policy_path: client,
        )
    )

    assert client.calls == 1
    assert terminal["error_class"] == "ReserveCapacityError"
    assert terminal["recovery_attempted_turn_count"] == 0
    assert terminal["recovery_usage"] == recovery._zero_usage()
    assert terminal["accounting_complete"] is True
    assert terminal["holdout_authorized"] is False
    assert not any(
        recovery.semantic._turn_paths(root / "alignment", turn_name)["sidecar"].exists()
        for turn_name in recovery.TURN_NAMES
    )


def test_source_request_mutation_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "recovery"
    frozen = recovery.freeze_recovery(output_dir=root)
    binding_path = root / "alignment" / "request-bindings.json"
    binding = json.loads(binding_path.read_text(encoding="utf-8"))
    prompt_record = dict(binding["turns"][0]["prompt"])
    prompt_record["sha256"] = "0" * 64
    binding["turns"][0]["prompt"] = prompt_record
    binding_path.write_text(json.dumps(binding), encoding="utf-8")

    with pytest.raises(recovery.AdoptionAlignmentRecoveryError):
        recovery.verify_recovery_lock(frozen["runtime_lock"])
