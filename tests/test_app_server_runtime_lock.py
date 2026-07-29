from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from research_factory.app_server_runtime_lock import (
    JUDGE_FIXTURE_REFERENCE_ADJUDICATION_RUNTIME_LOCK_VERSION,
    JUDGE_FIXTURE_TRUTH_AUDIT_RECOVERY_RUNTIME_LOCK_VERSION,
    JUDGE_FIXTURE_TRUTH_AUDIT_RUNTIME_LOCK_VERSION,
    JUDGE_FULL_CALIBRATION_RECOVERY_RUNTIME_LOCK_VERSION,
    JUDGE_FULL_CALIBRATION_RUNTIME_LOCK_VERSION,
    JUDGE_DIAGNOSTIC_V6_RUNTIME_LOCK_VERSION,
    JUDGE_DIAGNOSTIC_V5_RUNTIME_LOCK_VERSION,
    JUDGE_DIAGNOSTIC_V4_RUNTIME_LOCK_VERSION,
    JUDGE_DIAGNOSTIC_V3_RUNTIME_LOCK_VERSION,
    JUDGE_DIAGNOSTIC_V2_RUNTIME_LOCK_VERSION,
    JUDGE_RECOVERY_RUNTIME_LOCK_VERSION,
    OVERLOAD_RECOVERY_RUNTIME_LOCK_VERSION,
    RUNTIME_LOCK_VERSION,
    SCHEMA_COMPAT_RUNTIME_LOCK_VERSION,
    SHARDED_RUNTIME_LOCK_VERSION,
    SUPERSEDING_RUNTIME_LOCK_VERSION,
    RuntimeLockError,
    verify_runtime_lock,
)


class RuntimeLockTests(unittest.TestCase):
    def test_verifies_exact_files_and_fails_closed_on_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            frozen = root / "frozen.txt"
            frozen.write_text("exact\n", encoding="utf-8")
            payload = frozen.read_bytes()
            manifest = root / "lock.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": RUNTIME_LOCK_VERSION,
                        "files": [
                            {
                                "path": "frozen.txt",
                                "sha256": hashlib.sha256(payload).hexdigest(),
                                "size_bytes": len(payload),
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
            self.assertTrue(report["ok"])
            self.assertEqual(report["verified_file_count"], 1)

            frozen.write_text("drifted\n", encoding="utf-8")
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v14_binds_inadmissible_v2_and_unlaunched_fixture_audit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current = root / "fixture-audit-v2.py"
            current.write_text("six-case fixture audit\n", encoding="utf-8")
            labels = (
                "runtime_lock_v13",
                "launch_receipt_v13",
                "terminal_receipt_v13",
                "calibration_v2_terminal",
                "calibration_v2_score",
                "fixture_truth_audit_receipt",
                "judge_v5_4_freeze_receipt",
                "unlaunched_fixture_audit_v1_spec",
            )
            artifacts = []
            for label in labels:
                path = root / (label + ".json")
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": path.name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v14.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": JUDGE_FIXTURE_TRUTH_AUDIT_RUNTIME_LOCK_VERSION,
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v13"
                            ),
                            "prior_runtime_lock_version": (
                                JUDGE_FULL_CALIBRATION_RECOVERY_RUNTIME_LOCK_VERSION
                            ),
                            "calibration_v2_status": "reference_truth_audit_required",
                            "calibration_v2_usage_status": "complete",
                            "calibration_v2_total_tokens": 543162,
                            "calibration_v2_selection_authorized": False,
                            "calibration_v2_reference_truth_admissible": False,
                            "calibration_v2_output_reuse_for_audit": False,
                            "unlaunched_audit_v1_semantic_turn_count": 0,
                            "unlaunched_audit_v1_replay_allowed": False,
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": current.name,
                                "sha256": hashlib.sha256(
                                    current.read_bytes()
                                ).hexdigest(),
                                "size_bytes": current.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
            self.assertEqual(
                report["schema_version"],
                JUDGE_FIXTURE_TRUTH_AUDIT_RUNTIME_LOCK_VERSION,
            )
            self.assertEqual(report["verified_superseded_artifact_count"], 8)
            (root / "unlaunched_fixture_audit_v1_spec.json").write_text(
                "drift\n", encoding="utf-8"
            )
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v15_binds_complete_audit_outputs_and_one_turn_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current = root / "fixture-audit-recovery.py"
            current.write_text("one turn recovery\n", encoding="utf-8")
            labels = (
                "runtime_lock_v14",
                "launch_intent_v14",
                "launch_receipt_v14",
                "audit_v2_spec",
                "audit_v2_terminal",
                "audit_v2_failure",
                "audit_v2_canary_output",
            )
            artifacts = []
            for label in labels:
                path = root / (label + ".json")
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": path.name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v15.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": (
                            JUDGE_FIXTURE_TRUTH_AUDIT_RECOVERY_RUNTIME_LOCK_VERSION
                        ),
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v14"
                            ),
                            "prior_runtime_lock_version": (
                                JUDGE_FIXTURE_TRUTH_AUDIT_RUNTIME_LOCK_VERSION
                            ),
                            "audit_v2_status": "failed",
                            "audit_v2_incident_classification": (
                                "infrastructure_or_judge_attempt_failed"
                            ),
                            "audit_v2_error_class": "JudgeV5ProtocolError",
                            "audit_v2_failed_turn": "neutral_alignment_canary",
                            "audit_v2_completed_turn_count": 23,
                            "audit_v2_usage_status": "complete",
                            "audit_v2_total_tokens": 827876,
                            "audit_v2_replay_allowed": False,
                            "audit_v2_partial_output_selection_allowed": False,
                            "recovery_new_semantic_turn_count": 1,
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": current.name,
                                "sha256": hashlib.sha256(
                                    current.read_bytes()
                                ).hexdigest(),
                                "size_bytes": current.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
            self.assertEqual(
                report["schema_version"],
                JUDGE_FIXTURE_TRUTH_AUDIT_RECOVERY_RUNTIME_LOCK_VERSION,
            )
            self.assertEqual(report["verified_superseded_artifact_count"], 7)
            (root / "audit_v2_canary_output.json").write_text(
                "drift\n", encoding="utf-8"
            )
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v16_binds_audit_proposal_and_exact_reference_disputes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current = root / "reference-adjudication.py"
            current.write_text("side free reference adjudication\n", encoding="utf-8")
            labels = (
                "runtime_lock_v15",
                "launch_intent_v15",
                "launch_receipt_v15",
                "audit_v3_terminal",
                "audit_v3_comparison",
                "audit_v3_adoption_receipt",
                "audit_v3_reconciled_alignment",
            )
            artifacts = []
            for label in labels:
                path = root / (label + ".json")
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": path.name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v16.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": (
                            JUDGE_FIXTURE_REFERENCE_ADJUDICATION_RUNTIME_LOCK_VERSION
                        ),
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v15"
                            ),
                            "prior_runtime_lock_version": (
                                JUDGE_FIXTURE_TRUTH_AUDIT_RECOVERY_RUNTIME_LOCK_VERSION
                            ),
                            "audit_v3_status": "completed",
                            "audit_v3_terminal_reason": (
                                "fixture_truth_proposal_completed_reference_adjudication_required"
                            ),
                            "audit_v3_cumulative_tokens": 854550,
                            "audit_v3_reference_freeze_authorized": False,
                            "audit_v3_selection_authorized": False,
                            "reference_pointwise_disputed_witness_count": 102,
                            "reference_pointwise_disputed_case_count": 49,
                            "reference_alignment_disputed_case_count": 18,
                            "reference_adjudication_turn_count": 12,
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": current.name,
                                "sha256": hashlib.sha256(
                                    current.read_bytes()
                                ).hexdigest(),
                                "size_bytes": current.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
            self.assertEqual(
                report["schema_version"],
                JUDGE_FIXTURE_REFERENCE_ADJUDICATION_RUNTIME_LOCK_VERSION,
            )
            self.assertEqual(report["verified_superseded_artifact_count"], 7)
            (root / "audit_v3_reconciled_alignment.json").write_text(
                "drift\n", encoding="utf-8"
            )
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_rejects_repository_escape(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = root / "lock.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": RUNTIME_LOCK_VERSION,
                        "files": [
                            {"path": "../escape", "sha256": "0" * 64, "size_bytes": 0}
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v2_requires_and_verifies_immutable_incident_supersession(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            frozen = root / "runtime.py"
            frozen.write_text("current\n", encoding="utf-8")
            artifacts = []
            for label, name in (
                ("runtime_lock_v1", "lock-v1.json"),
                ("launch_receipt_v1", "receipt-v1.json"),
                ("incident_snapshot_v1", "incident-v1.json"),
            ):
                path = root / name
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v2.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": SUPERSEDING_RUNTIME_LOCK_VERSION,
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v1"
                            ),
                            "incident_status": "failed_closed",
                            "incident_error_class": "RecoveryError",
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": "runtime.py",
                                "sha256": hashlib.sha256(frozen.read_bytes()).hexdigest(),
                                "size_bytes": frozen.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)

            self.assertEqual(report["schema_version"], SUPERSEDING_RUNTIME_LOCK_VERSION)
            self.assertEqual(report["verified_superseded_artifact_count"], 3)
            (root / "incident-v1.json").write_text("drift\n", encoding="utf-8")
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v3_binds_v2_lock_and_pipeline_v1_failed_judge_incident(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current = root / "pipeline-v2.py"
            current.write_text("current v2 runner\n", encoding="utf-8")
            artifacts = []
            for label, name in (
                ("runtime_lock_v2", "lock-v2.json"),
                ("launch_receipt_v2", "receipt-v2.json"),
                ("pipeline_v1_terminal", "pipeline-terminal-v1.json"),
                ("pipeline_v1_selection_result", "selection-v1.json"),
                ("pipeline_v1_failed_ab_sidecar", "failed-ab-v1.json"),
            ):
                path = root / name
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v3.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": SHARDED_RUNTIME_LOCK_VERSION,
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v2"
                            ),
                            "prior_runtime_lock_version": SUPERSEDING_RUNTIME_LOCK_VERSION,
                            "pipeline_v1_status": "blocked",
                            "pipeline_v1_incident_classification": (
                                "infrastructure_or_judge_attempt_failed"
                            ),
                            "pipeline_v1_replay_allowed": False,
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": current.name,
                                "sha256": hashlib.sha256(current.read_bytes()).hexdigest(),
                                "size_bytes": current.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)

            self.assertEqual(report["schema_version"], SHARDED_RUNTIME_LOCK_VERSION)
            self.assertEqual(report["verified_superseded_artifact_count"], 5)
            (root / "failed-ab-v1.json").write_text("drift\n", encoding="utf-8")
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v4_binds_failed_pipeline_v2_shard_without_permitting_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current = root / "pipeline-v3.py"
            current.write_text("schema compatible runner\n", encoding="utf-8")
            artifacts = []
            for label, name in (
                ("runtime_lock_v3", "lock-v3.json"),
                ("launch_receipt_v3", "receipt-v3.json"),
                ("pipeline_v2_terminal", "pipeline-v2-terminal.json"),
                ("pipeline_v2_failed_shard", "pipeline-v2-shard-failure.json"),
                ("pipeline_v2_failed_ab_sidecar", "pipeline-v2-ab.json"),
            ):
                path = root / name
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v4.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": SCHEMA_COMPAT_RUNTIME_LOCK_VERSION,
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v3"
                            ),
                            "prior_runtime_lock_version": SHARDED_RUNTIME_LOCK_VERSION,
                            "pipeline_v2_status": "blocked",
                            "pipeline_v2_incident_classification": (
                                "infrastructure_or_judge_attempt_failed"
                            ),
                            "pipeline_v2_replay_allowed": False,
                            "failed_v2_shard_retry_allowed": False,
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": current.name,
                                "sha256": hashlib.sha256(current.read_bytes()).hexdigest(),
                                "size_bytes": current.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
            self.assertEqual(report["schema_version"], SCHEMA_COMPAT_RUNTIME_LOCK_VERSION)
            self.assertEqual(report["verified_superseded_artifact_count"], 5)
            (root / "pipeline-v2-shard-failure.json").write_text(
                "drift\n", encoding="utf-8"
            )
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v5_binds_v3_overload_and_forbids_retry_or_partial_scoring(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current = root / "pipeline-v4.py"
            current.write_text("overload recovery runner\n", encoding="utf-8")
            artifacts = []
            entries = (
                ("runtime_lock_v4", "lock-v4.json"),
                ("launch_receipt_v4", "receipt-v4.json"),
                ("pipeline_v3_terminal", "pipeline-v3-terminal.json"),
                ("pipeline_v3_calibration_report", "pipeline-v3-calibration.json"),
                ("pipeline_v3_failed_shard", "pipeline-v3-shard-failure.json"),
                ("pipeline_v3_failed_ba_sidecar", "pipeline-v3-ba.json"),
                ("pipeline_v3_failed_ba_capacity", "pipeline-v3-ba-capacity.json"),
                ("pipeline_v3_reuse_contract", "pipeline-v3-reuse.json"),
            )
            for label, name in entries:
                path = root / name
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v5.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": OVERLOAD_RECOVERY_RUNTIME_LOCK_VERSION,
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v4"
                            ),
                            "prior_runtime_lock_version": (
                                SCHEMA_COMPAT_RUNTIME_LOCK_VERSION
                            ),
                            "pipeline_v3_status": "blocked",
                            "pipeline_v3_incident_classification": (
                                "infrastructure_or_judge_attempt_failed"
                            ),
                            "provider_error_code": "serverOverloaded",
                            "pipeline_v3_replay_allowed": False,
                            "failed_v3_shard_retry_allowed": False,
                            "pipeline_v3_partial_calibration_scoring_allowed": False,
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": current.name,
                                "sha256": hashlib.sha256(
                                    current.read_bytes()
                                ).hexdigest(),
                                "size_bytes": current.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
            self.assertEqual(
                report["schema_version"], OVERLOAD_RECOVERY_RUNTIME_LOCK_VERSION
            )
            self.assertEqual(report["verified_superseded_artifact_count"], 8)
            (root / "pipeline-v3-ba.json").write_text("drift\n", encoding="utf-8")
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v6_binds_v4_quality_failure_and_v5_truth_audit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current = root / "pipeline-v5.py"
            current.write_text("judge diagnostic runner\n", encoding="utf-8")
            labels = (
                "runtime_lock_v5",
                "launch_receipt_v5",
                "terminal_receipt_v5",
                "pipeline_v4_terminal",
                "pipeline_v4_calibration_report",
                "pipeline_v4_reuse_contract",
                "pipeline_v5_reuse_contract",
                "pipeline_v5_fixture_truth_audit_receipt",
            )
            artifacts = []
            for label in labels:
                path = root / (label + ".json")
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": path.name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v6.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": JUDGE_RECOVERY_RUNTIME_LOCK_VERSION,
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v5"
                            ),
                            "prior_runtime_lock_version": (
                                OVERLOAD_RECOVERY_RUNTIME_LOCK_VERSION
                            ),
                            "pipeline_v4_status": "blocked",
                            "pipeline_v4_incident_classification": (
                                "judge_calibration_gate_not_passed"
                            ),
                            "pipeline_v4_transport_failed": False,
                            "pipeline_v4_completed_turn_count": 22,
                            "pipeline_v4_usage_status": "complete",
                            "pipeline_v4_replay_allowed": False,
                            "pipeline_v4_judge_outputs_admissible_as_passing_evidence": False,
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": current.name,
                                "sha256": hashlib.sha256(
                                    current.read_bytes()
                                ).hexdigest(),
                                "size_bytes": current.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
            self.assertEqual(
                report["schema_version"], JUDGE_RECOVERY_RUNTIME_LOCK_VERSION
            )
            self.assertEqual(report["verified_superseded_artifact_count"], 8)
            (root / "pipeline_v5_fixture_truth_audit_receipt.json").write_text(
                "drift\n", encoding="utf-8"
            )
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v7_binds_failed_diagnostic_v1_and_corrected_v2_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current = root / "diagnostic-v2.py"
            current.write_text("fresh diagnostic v2\n", encoding="utf-8")
            labels = (
                "runtime_lock_v6",
                "launch_receipt_v6",
                "terminal_receipt_v6",
                "diagnostic_v1_spec",
                "diagnostic_v1_terminal",
                "diagnostic_v1_score",
                "diagnostic_v1_truth_audit_receipt",
                "diagnostic_v2_reuse_contract",
            )
            artifacts = []
            for label in labels:
                path = root / (label + ".json")
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": path.name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v7.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": JUDGE_DIAGNOSTIC_V2_RUNTIME_LOCK_VERSION,
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v6"
                            ),
                            "prior_runtime_lock_version": JUDGE_RECOVERY_RUNTIME_LOCK_VERSION,
                            "diagnostic_v1_status": "blocked",
                            "diagnostic_v1_incident_classification": (
                                "judge_diagnostic_quality_gate_not_passed"
                            ),
                            "diagnostic_v1_transport_failed": False,
                            "diagnostic_v1_completed_turn_count": 4,
                            "diagnostic_v1_usage_status": "complete",
                            "diagnostic_v1_total_tokens": 138211,
                            "diagnostic_v1_replay_allowed": False,
                            "diagnostic_v1_outputs_admissible_as_passing_evidence": False,
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": current.name,
                                "sha256": hashlib.sha256(
                                    current.read_bytes()
                                ).hexdigest(),
                                "size_bytes": current.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
            self.assertEqual(
                report["schema_version"], JUDGE_DIAGNOSTIC_V2_RUNTIME_LOCK_VERSION
            )
            self.assertEqual(report["verified_superseded_artifact_count"], 8)
            (root / "diagnostic_v1_truth_audit_receipt.json").write_text(
                "drift\n", encoding="utf-8"
            )
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v8_binds_v2_validation_failure_and_v3_fresh_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current = root / "diagnostic-v3.py"
            current.write_text("fresh diagnostic v3\n", encoding="utf-8")
            labels = (
                "runtime_lock_v7",
                "launch_receipt_v7",
                "terminal_receipt_v7",
                "diagnostic_v2_spec",
                "diagnostic_v2_terminal",
                "diagnostic_v2_failure",
                "diagnostic_v2_attempt_audit_receipt",
                "diagnostic_v3_reuse_contract",
            )
            artifacts = []
            for label in labels:
                path = root / (label + ".json")
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": path.name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v8.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": JUDGE_DIAGNOSTIC_V3_RUNTIME_LOCK_VERSION,
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v7"
                            ),
                            "prior_runtime_lock_version": (
                                JUDGE_DIAGNOSTIC_V2_RUNTIME_LOCK_VERSION
                            ),
                            "diagnostic_v2_status": "failed",
                            "diagnostic_v2_incident_classification": (
                                "infrastructure_or_judge_attempt_failed"
                            ),
                            "diagnostic_v2_transport_failed": False,
                            "diagnostic_v2_quality_scored": False,
                            "diagnostic_v2_completed_turn_count": 3,
                            "diagnostic_v2_sidecar_usage_status": "complete",
                            "diagnostic_v2_total_tokens": 107187,
                            "diagnostic_v2_replay_allowed": False,
                            "diagnostic_v2_output_reuse_allowed": False,
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": current.name,
                                "sha256": hashlib.sha256(
                                    current.read_bytes()
                                ).hexdigest(),
                                "size_bytes": current.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
            self.assertEqual(
                report["schema_version"], JUDGE_DIAGNOSTIC_V3_RUNTIME_LOCK_VERSION
            )
            self.assertEqual(report["verified_superseded_artifact_count"], 8)
            (root / "diagnostic_v2_attempt_audit_receipt.json").write_text(
                "drift\n", encoding="utf-8"
            )
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v9_binds_v3_order_failure_and_v4_consistency_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current = root / "diagnostic-v4.py"
            current.write_text("fresh diagnostic v4\n", encoding="utf-8")
            labels = (
                "runtime_lock_v8",
                "launch_receipt_v8",
                "terminal_receipt_v8",
                "diagnostic_v3_spec",
                "diagnostic_v3_terminal",
                "diagnostic_v3_score",
                "diagnostic_v3_quality_audit_receipt",
                "diagnostic_v4_reuse_contract",
            )
            artifacts = []
            for label in labels:
                path = root / (label + ".json")
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": path.name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v9.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": JUDGE_DIAGNOSTIC_V4_RUNTIME_LOCK_VERSION,
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v8"
                            ),
                            "prior_runtime_lock_version": (
                                JUDGE_DIAGNOSTIC_V3_RUNTIME_LOCK_VERSION
                            ),
                            "diagnostic_v3_status": "blocked",
                            "diagnostic_v3_incident_classification": (
                                "judge_diagnostic_quality_gate_not_passed"
                            ),
                            "diagnostic_v3_only_failed_gate": "order_bias",
                            "diagnostic_v3_completed_turn_count": 4,
                            "diagnostic_v3_usage_status": "complete",
                            "diagnostic_v3_total_tokens": 150769,
                            "diagnostic_v3_replay_allowed": False,
                            "diagnostic_v3_output_reuse_allowed": False,
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": current.name,
                                "sha256": hashlib.sha256(
                                    current.read_bytes()
                                ).hexdigest(),
                                "size_bytes": current.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
            self.assertEqual(
                report["schema_version"], JUDGE_DIAGNOSTIC_V4_RUNTIME_LOCK_VERSION
            )
            self.assertEqual(report["verified_superseded_artifact_count"], 8)
            (root / "diagnostic_v3_quality_audit_receipt.json").write_text(
                "drift\n", encoding="utf-8"
            )
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v10_binds_v4_fixture_failure_and_v5_fresh_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current = root / "diagnostic-v5.py"
            current.write_text("fresh diagnostic v5\n", encoding="utf-8")
            labels = (
                "runtime_lock_v9",
                "launch_receipt_v9",
                "terminal_receipt_v9",
                "diagnostic_v4_spec",
                "diagnostic_v4_terminal",
                "diagnostic_v4_score",
                "diagnostic_v4_disagreements",
                "diagnostic_v4_fixture_audit_receipt",
                "diagnostic_v5_reuse_contract",
                "diagnostic_fixture_patch_v3",
            )
            artifacts = []
            for label in labels:
                path = root / (label + ".json")
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": path.name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v10.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": JUDGE_DIAGNOSTIC_V5_RUNTIME_LOCK_VERSION,
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v9"
                            ),
                            "prior_runtime_lock_version": (
                                JUDGE_DIAGNOSTIC_V4_RUNTIME_LOCK_VERSION
                            ),
                            "diagnostic_v4_status": "blocked",
                            "diagnostic_v4_incident_classification": (
                                "judge_diagnostic_quality_gate_not_passed"
                            ),
                            "diagnostic_v4_only_failed_gate": "order_bias",
                            "diagnostic_v4_completed_turn_count": 4,
                            "diagnostic_v4_usage_status": "complete",
                            "diagnostic_v4_total_tokens": 147819,
                            "diagnostic_v4_fixture_truth_defect": "event_type",
                            "diagnostic_v4_replay_allowed": False,
                            "diagnostic_v4_output_reuse_allowed": False,
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": current.name,
                                "sha256": hashlib.sha256(
                                    current.read_bytes()
                                ).hexdigest(),
                                "size_bytes": current.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
            self.assertEqual(
                report["schema_version"], JUDGE_DIAGNOSTIC_V5_RUNTIME_LOCK_VERSION
            )
            self.assertEqual(report["verified_superseded_artifact_count"], 10)
            (root / "diagnostic_v4_fixture_audit_receipt.json").write_text(
                "drift\n", encoding="utf-8"
            )
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v11_binds_v5_order_failure_and_v6_fresh_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current = root / "diagnostic-v6.py"
            current.write_text("fresh diagnostic v6\n", encoding="utf-8")
            labels = (
                "runtime_lock_v10",
                "launch_receipt_v10",
                "terminal_receipt_v10",
                "diagnostic_v5_spec",
                "diagnostic_v5_terminal",
                "diagnostic_v5_score",
                "diagnostic_v5_quality_audit_receipt",
                "diagnostic_v6_reuse_contract",
                "diagnostic_fixture_patch_v3",
            )
            artifacts = []
            for label in labels:
                path = root / (label + ".json")
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": path.name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v11.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": JUDGE_DIAGNOSTIC_V6_RUNTIME_LOCK_VERSION,
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v10"
                            ),
                            "prior_runtime_lock_version": (
                                JUDGE_DIAGNOSTIC_V5_RUNTIME_LOCK_VERSION
                            ),
                            "diagnostic_v5_status": "blocked",
                            "diagnostic_v5_incident_classification": (
                                "judge_diagnostic_quality_gate_not_passed"
                            ),
                            "diagnostic_v5_only_failed_gate": "order_bias",
                            "diagnostic_v5_completed_turn_count": 4,
                            "diagnostic_v5_usage_status": "complete",
                            "diagnostic_v5_total_tokens": 149074,
                            "diagnostic_v5_replay_allowed": False,
                            "diagnostic_v5_output_reuse_allowed": False,
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": current.name,
                                "sha256": hashlib.sha256(
                                    current.read_bytes()
                                ).hexdigest(),
                                "size_bytes": current.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
            self.assertEqual(
                report["schema_version"], JUDGE_DIAGNOSTIC_V6_RUNTIME_LOCK_VERSION
            )
            self.assertEqual(report["verified_superseded_artifact_count"], 9)
            (root / "diagnostic_v5_quality_audit_receipt.json").write_text(
                "drift\n", encoding="utf-8"
            )
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v12_binds_passing_v6_and_frozen_full_calibration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current = root / "full-calibration.py"
            current.write_text("frozen full calibration\n", encoding="utf-8")
            labels = (
                "runtime_lock_v11",
                "launch_receipt_v11",
                "terminal_receipt_v11",
                "diagnostic_v6_spec",
                "diagnostic_v6_terminal",
                "diagnostic_v6_score",
                "judge_v5_4_freeze_receipt",
                "diagnostic_fixture_patch_v3",
            )
            artifacts = []
            for label in labels:
                path = root / (label + ".json")
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": path.name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v12.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": JUDGE_FULL_CALIBRATION_RUNTIME_LOCK_VERSION,
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v11"
                            ),
                            "prior_runtime_lock_version": (
                                JUDGE_DIAGNOSTIC_V6_RUNTIME_LOCK_VERSION
                            ),
                            "diagnostic_v6_status": "passed",
                            "diagnostic_v6_terminal_classification": (
                                "judge_diagnostic_passed_full_calibration_authorized"
                            ),
                            "diagnostic_v6_protocol_version": (
                                "pif_app_server_judge_protocol_v5_4"
                            ),
                            "diagnostic_v6_completed_turn_count": 3,
                            "diagnostic_v6_usage_status": "complete",
                            "diagnostic_v6_total_tokens": 122896,
                            "diagnostic_v6_full_calibration_authorized": True,
                            "diagnostic_v6_replay_allowed": False,
                            "diagnostic_v6_output_reuse_allowed": False,
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": current.name,
                                "sha256": hashlib.sha256(
                                    current.read_bytes()
                                ).hexdigest(),
                                "size_bytes": current.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
            self.assertEqual(
                report["schema_version"], JUDGE_FULL_CALIBRATION_RUNTIME_LOCK_VERSION
            )
            self.assertEqual(report["verified_superseded_artifact_count"], 8)
            (root / "judge_v5_4_freeze_receipt.json").write_text(
                "drift\n", encoding="utf-8"
            )
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)

    def test_v13_binds_failed_calibration_v1_and_fresh_v2(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            current = root / "full-calibration-v2.py"
            current.write_text("fresh full calibration v2\n", encoding="utf-8")
            labels = (
                "runtime_lock_v12",
                "launch_receipt_v12",
                "terminal_receipt_v12",
                "calibration_v1_spec",
                "calibration_v1_terminal",
                "calibration_v1_failure",
                "calibration_v1_failed_output",
                "calibration_v1_failed_sidecar",
                "calibration_v1_audit_receipt",
                "judge_v5_4_freeze_receipt",
            )
            artifacts = []
            for label in labels:
                path = root / (label + ".json")
                path.write_text(label + "\n", encoding="utf-8")
                artifacts.append(
                    {
                        "label": label,
                        "path": path.name,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "size_bytes": path.stat().st_size,
                    }
                )
            manifest = root / "lock-v13.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": (
                            JUDGE_FULL_CALIBRATION_RECOVERY_RUNTIME_LOCK_VERSION
                        ),
                        "supersedes": {
                            "schema_version": (
                                "pif_app_server_unattended_runtime_lock_supersession_v12"
                            ),
                            "prior_runtime_lock_version": (
                                JUDGE_FULL_CALIBRATION_RUNTIME_LOCK_VERSION
                            ),
                            "calibration_v1_status": "failed",
                            "calibration_v1_incident_classification": (
                                "infrastructure_or_judge_attempt_failed"
                            ),
                            "calibration_v1_quality_scored": False,
                            "calibration_v1_completed_turn_count": 12,
                            "calibration_v1_usage_status": "complete",
                            "calibration_v1_total_tokens": 496703,
                            "calibration_v1_replay_allowed": False,
                            "calibration_v1_output_reuse_allowed": False,
                            "artifacts": artifacts,
                        },
                        "files": [
                            {
                                "path": current.name,
                                "sha256": hashlib.sha256(
                                    current.read_bytes()
                                ).hexdigest(),
                                "size_bytes": current.stat().st_size,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
            self.assertEqual(
                report["schema_version"],
                JUDGE_FULL_CALIBRATION_RECOVERY_RUNTIME_LOCK_VERSION,
            )
            self.assertEqual(report["verified_superseded_artifact_count"], 10)
            (root / "calibration_v1_audit_receipt.json").write_text(
                "drift\n", encoding="utf-8"
            )
            with self.assertRaises(RuntimeLockError):
                verify_runtime_lock(repo_root=root, manifest_path=manifest)


if __name__ == "__main__":
    unittest.main()
