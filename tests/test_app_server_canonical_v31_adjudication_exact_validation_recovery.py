from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_adjudication_exact_validation_recovery as recovery


def test_exact_completed_failure_binds_artifacts_usage_and_errors() -> None:
    failure = recovery.validate_completed_failure()

    assert failure["usage"] == recovery.EXPECTED_USAGE
    assert failure["validation_error_count"] == 2
    assert failure["validation_errors_sha256"] == recovery.EXPECTED_ERRORS_SHA256
    assert set(failure["artifact_records"]) == set(recovery.EXPECTED_ARTIFACT_HASHES)
    assert all(
        row["sha256"] == recovery.EXPECTED_ARTIFACT_HASHES[role]
        for role, row in failure["artifact_records"].items()
    )


def test_zero_call_freeze_and_recovery_receipt(tmp_path: Path) -> None:
    root = tmp_path / "epoch30"

    lock = recovery.freeze_run(root)
    receipt = recovery.run(root)

    assert lock["new_semantic_model_call_cap"] == 0
    assert receipt["state"] == "rejected"
    assert receipt["semantic_model_call_count"] == 0
    assert receipt["recovered_predecessor_semantic_model_call_count"] == 1
    assert receipt["recovered_predecessor_usage"] == recovery.EXPECTED_USAGE
    assert receipt["aggregate_architecture_semantic_model_call_count"] == 8
    assert receipt["aggregate_architecture_unknown_usage_turn_count"] == 2
    assert receipt["aggregate_architecture_measured_total_tokens"] == 265_792
    assert receipt["semantic_output_repair_applied"] is False
    assert receipt["production_mutated"] is False
    assert recovery.verify_receipt(root) == receipt


def test_predecessor_artifact_tamper_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = recovery._sha256_file  # noqa: SLF001
    output_path = recovery._artifact_paths()["output"]  # noqa: SLF001

    def tampered(path: Path) -> str:
        if path.expanduser().resolve() == output_path.expanduser().resolve():
            return "0" * 64
        return original(path)

    monkeypatch.setattr(recovery, "_sha256_file", tampered)
    with pytest.raises(recovery.Epoch30RecoveryError, match="output drifted"):
        recovery.validate_completed_failure()


def test_single_valid_terminal_mirror_repairs_without_semantic_work(
    tmp_path: Path,
) -> None:
    root = tmp_path / "epoch30"
    receipt = recovery.run(root)
    (root / "terminal.json").unlink()

    recovered = recovery.run(root)

    assert recovered == receipt
    assert json.loads((root / "terminal.json").read_text(encoding="utf-8")) == receipt


def test_invalid_single_terminal_mirror_does_not_repair(tmp_path: Path) -> None:
    root = tmp_path / "epoch30"
    recovery.freeze_run(root)
    (root / "plan-step-receipt.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(recovery.Epoch30RecoveryError, match="terminal mirror drifted"):
        recovery.verify_receipt(root)

    assert not (root / "terminal.json").exists()


def test_cli_exposes_all_bounded_phases() -> None:
    for command in ("freeze", "verify-runtime", "run", "verify-receipt", "status"):
        assert recovery._parser().parse_args([command]).command == command  # noqa: SLF001
