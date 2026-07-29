from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_epoch7_capacity_readiness as readiness


@pytest.fixture
def project_temp_root() -> Path:
    root = Path(
        tempfile.mkdtemp(
            prefix="canonical-v31-capacity-readiness-test-",
            dir=readiness.PROJECT_ROOT / "work",
        )
    ).resolve()
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_real_measured_evidence_proves_source_bound_cannot_fit() -> None:
    analysis = readiness.build_readiness_analysis()

    assert analysis["state"] == "waiting"
    assert analysis["measured_evidence_record_count"] == 48
    assert analysis["measured_sidecar_count"] == 24
    assert analysis["measured_total_tokens"] == 854_550
    assert analysis["measured_maximum_turn_tokens"] == 50_988
    assert analysis["exact_model_call_count"] == 48
    assert analysis["source_maximum_total_tokens_per_turn"] == 102_000
    assert analysis["source_phase_total_token_bound"] == 4_896_000
    assert analysis["source_projected_phase_quota_points"] == 84
    assert analysis["maximum_possible_usable_quota_points"] == 79
    assert analysis["maximum_admissible_total_tokens_per_turn"] == 96_813
    assert analysis["required_per_turn_bound_reduction_tokens"] == 5_187
    assert analysis["canonical_policy_emitted"] is False
    assert analysis["semantic_model_call_count"] == 0


def test_waiting_receipt_round_trips_without_policy_or_probe(
    project_temp_root: Path,
) -> None:
    root = project_temp_root / "receipt"

    receipt = readiness.freeze_readiness_receipt(root=root)

    assert receipt["state"] == "waiting"
    assert receipt["canonical_policy_emitted"] is False
    assert receipt["capacity_probe_performed"] is False
    assert receipt["semantic_model_call_count"] == 0
    assert receipt["production_mutated"] is False
    assert readiness.verify_readiness_receipt(root) == receipt
    assert not (root / "capacity-policy.json").exists()


def test_readiness_payload_tamper_is_rejected(project_temp_root: Path) -> None:
    root = project_temp_root / "tamper"
    receipt = readiness.freeze_readiness_receipt(root=root)
    path = Path(receipt["capacity_readiness"]["path"])
    payload = json.loads(path.read_text())
    payload["source_projected_phase_quota_points"] = 1
    path.write_text(json.dumps(payload, sort_keys=True) + "\n")

    with pytest.raises(
        readiness.CanonicalV31Epoch7CapacityReadinessError,
        match="capacity readiness record drifted",
    ):
        readiness.verify_readiness_receipt(root)


def test_measured_summary_drift_fails_closed(project_temp_root: Path) -> None:
    audit = json.loads(readiness.DEFAULT_AUDIT_PATH.read_text())
    audit["measured_total_tokens"] -= 1
    path = project_temp_root / "drifted-audit.json"
    path.write_text(json.dumps(audit, sort_keys=True, indent=2) + "\n")
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    policy = json.loads(readiness.DEFAULT_POLICY_PATH.read_text())
    policy["audit"] = {
        "path": str(path),
        "sha256": checksum,
        "size_bytes": path.stat().st_size,
    }
    policy_path = project_temp_root / "drifted-policy.json"
    policy_path.write_text(json.dumps(policy, sort_keys=True, indent=2) + "\n")
    policy_checksum = hashlib.sha256(policy_path.read_bytes()).hexdigest()

    with pytest.raises(
        readiness.CanonicalV31Epoch7CapacityReadinessError,
        match="measured capacity summary drifted",
    ):
            readiness.build_readiness_analysis(
                audit_path=path,
                audit_sha256=checksum,
                policy_path=policy_path,
                policy_sha256=policy_checksum,
            )


def test_terminal_mirror_drift_is_rejected(project_temp_root: Path) -> None:
    root = project_temp_root / "mirror"
    readiness.freeze_readiness_receipt(root=root)
    terminal_path = root / readiness.TERMINAL_FILENAME
    terminal = json.loads(terminal_path.read_text())
    terminal["state"] = "passed"
    terminal_path.write_text(json.dumps(terminal, sort_keys=True) + "\n")

    with pytest.raises(
        readiness.CanonicalV31Epoch7CapacityReadinessError,
        match="receipt mirrors drifted",
    ):
        readiness.verify_readiness_receipt(root)
