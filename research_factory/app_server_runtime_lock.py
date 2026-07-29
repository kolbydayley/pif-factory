from __future__ import annotations

"""Fail-closed verifier for the unattended app-server runtime manifest."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Optional, Sequence


RUNTIME_LOCK_VERSION = "pif_app_server_unattended_runtime_lock_v1"
SUPERSEDING_RUNTIME_LOCK_VERSION = "pif_app_server_unattended_runtime_lock_v2"
SHARDED_RUNTIME_LOCK_VERSION = "pif_app_server_unattended_runtime_lock_v3"
SCHEMA_COMPAT_RUNTIME_LOCK_VERSION = "pif_app_server_unattended_runtime_lock_v4"
OVERLOAD_RECOVERY_RUNTIME_LOCK_VERSION = "pif_app_server_unattended_runtime_lock_v5"
JUDGE_RECOVERY_RUNTIME_LOCK_VERSION = "pif_app_server_unattended_runtime_lock_v6"
JUDGE_DIAGNOSTIC_V2_RUNTIME_LOCK_VERSION = (
    "pif_app_server_unattended_runtime_lock_v7"
)
JUDGE_DIAGNOSTIC_V3_RUNTIME_LOCK_VERSION = (
    "pif_app_server_unattended_runtime_lock_v8"
)
JUDGE_DIAGNOSTIC_V4_RUNTIME_LOCK_VERSION = (
    "pif_app_server_unattended_runtime_lock_v9"
)
JUDGE_DIAGNOSTIC_V5_RUNTIME_LOCK_VERSION = (
    "pif_app_server_unattended_runtime_lock_v10"
)
JUDGE_DIAGNOSTIC_V6_RUNTIME_LOCK_VERSION = (
    "pif_app_server_unattended_runtime_lock_v11"
)
JUDGE_FULL_CALIBRATION_RUNTIME_LOCK_VERSION = (
    "pif_app_server_unattended_runtime_lock_v12"
)
JUDGE_FULL_CALIBRATION_RECOVERY_RUNTIME_LOCK_VERSION = (
    "pif_app_server_unattended_runtime_lock_v13"
)
JUDGE_FIXTURE_TRUTH_AUDIT_RUNTIME_LOCK_VERSION = (
    "pif_app_server_unattended_runtime_lock_v14"
)
JUDGE_FIXTURE_TRUTH_AUDIT_RECOVERY_RUNTIME_LOCK_VERSION = (
    "pif_app_server_unattended_runtime_lock_v15"
)
JUDGE_FIXTURE_REFERENCE_ADJUDICATION_RUNTIME_LOCK_VERSION = (
    "pif_app_server_unattended_runtime_lock_v16"
)
SUPPORTED_RUNTIME_LOCK_VERSIONS = {
    RUNTIME_LOCK_VERSION,
    SUPERSEDING_RUNTIME_LOCK_VERSION,
    SHARDED_RUNTIME_LOCK_VERSION,
    SCHEMA_COMPAT_RUNTIME_LOCK_VERSION,
    OVERLOAD_RECOVERY_RUNTIME_LOCK_VERSION,
    JUDGE_RECOVERY_RUNTIME_LOCK_VERSION,
    JUDGE_DIAGNOSTIC_V2_RUNTIME_LOCK_VERSION,
    JUDGE_DIAGNOSTIC_V3_RUNTIME_LOCK_VERSION,
    JUDGE_DIAGNOSTIC_V4_RUNTIME_LOCK_VERSION,
    JUDGE_DIAGNOSTIC_V5_RUNTIME_LOCK_VERSION,
    JUDGE_DIAGNOSTIC_V6_RUNTIME_LOCK_VERSION,
    JUDGE_FULL_CALIBRATION_RUNTIME_LOCK_VERSION,
    JUDGE_FULL_CALIBRATION_RECOVERY_RUNTIME_LOCK_VERSION,
    JUDGE_FIXTURE_TRUTH_AUDIT_RUNTIME_LOCK_VERSION,
    JUDGE_FIXTURE_TRUTH_AUDIT_RECOVERY_RUNTIME_LOCK_VERSION,
    JUDGE_FIXTURE_REFERENCE_ADJUDICATION_RUNTIME_LOCK_VERSION,
}


class RuntimeLockError(RuntimeError):
    """Raised when a frozen runtime file is missing, malformed, or changed."""


def _resolve_frozen_file(
    *, root: Path, relative: Any, expected_sha: Any, expected_size: Any, purpose: str
) -> Path:
    if (
        not isinstance(relative, str)
        or not relative
        or Path(relative).is_absolute()
        or not isinstance(expected_sha, str)
        or len(expected_sha) != 64
        or isinstance(expected_size, bool)
        or not isinstance(expected_size, int)
        or expected_size < 0
    ):
        raise RuntimeLockError(f"{purpose} file entry is invalid")
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RuntimeLockError(f"{purpose} path escapes the repository") from exc
    if not path.is_file():
        raise RuntimeLockError(f"{purpose} file is missing: {relative}")
    if _sha256_file(path) != expected_sha or path.stat().st_size != expected_size:
        raise RuntimeLockError(f"{purpose} file drift: {relative}")
    return path


def _verify_supersession(root: Path, manifest: dict[str, Any]) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("superseding runtime lock has no incident provenance")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v1"
        or supersedes.get("incident_status") != "failed_closed"
        or supersedes.get("incident_error_class") != "RecoveryError"
    ):
        raise RuntimeLockError("superseding runtime lock incident provenance is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 3:
        raise RuntimeLockError("superseding runtime lock has incomplete incident artifacts")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("superseding runtime lock artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("superseding runtime lock artifact label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {"runtime_lock_v1", "launch_receipt_v1", "incident_snapshot_v1"}
    if not required.issubset(labels):
        raise RuntimeLockError("superseding runtime lock is missing required incident artifacts")
    return len(artifacts)


def _verify_sharded_supersession(root: Path, manifest: dict[str, Any]) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("sharded runtime lock has no supersession provenance")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v2"
        or supersedes.get("prior_runtime_lock_version")
        != SUPERSEDING_RUNTIME_LOCK_VERSION
        or supersedes.get("pipeline_v1_status") != "blocked"
        or supersedes.get("pipeline_v1_incident_classification")
        != "infrastructure_or_judge_attempt_failed"
        or supersedes.get("pipeline_v1_replay_allowed") is not False
    ):
        raise RuntimeLockError("sharded runtime lock supersession is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 5:
        raise RuntimeLockError("sharded runtime lock supersession is incomplete")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("sharded supersession artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("sharded supersession artifact label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {
        "runtime_lock_v2",
        "launch_receipt_v2",
        "pipeline_v1_terminal",
        "pipeline_v1_selection_result",
        "pipeline_v1_failed_ab_sidecar",
    }
    if not required.issubset(labels):
        raise RuntimeLockError(
            "sharded runtime lock is missing required v1 incident artifacts"
        )
    return len(artifacts)


def _verify_schema_compat_supersession(root: Path, manifest: dict[str, Any]) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("schema-compatible runtime lock has no supersession")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v3"
        or supersedes.get("prior_runtime_lock_version")
        != SHARDED_RUNTIME_LOCK_VERSION
        or supersedes.get("pipeline_v2_status") != "blocked"
        or supersedes.get("pipeline_v2_incident_classification")
        != "infrastructure_or_judge_attempt_failed"
        or supersedes.get("pipeline_v2_replay_allowed") is not False
        or supersedes.get("failed_v2_shard_retry_allowed") is not False
    ):
        raise RuntimeLockError("schema-compatible runtime supersession is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 5:
        raise RuntimeLockError("schema-compatible runtime supersession is incomplete")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("schema-compatible supersession artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("schema-compatible artifact label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {
        "runtime_lock_v3",
        "launch_receipt_v3",
        "pipeline_v2_terminal",
        "pipeline_v2_failed_shard",
        "pipeline_v2_failed_ab_sidecar",
    }
    if not required.issubset(labels):
        raise RuntimeLockError(
            "schema-compatible runtime lock is missing pipeline-v2 incident evidence"
        )
    return len(artifacts)


def _verify_overload_recovery_supersession(
    root: Path, manifest: dict[str, Any]
) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("overload-recovery runtime lock has no supersession")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v4"
        or supersedes.get("prior_runtime_lock_version")
        != SCHEMA_COMPAT_RUNTIME_LOCK_VERSION
        or supersedes.get("pipeline_v3_status") != "blocked"
        or supersedes.get("pipeline_v3_incident_classification")
        != "infrastructure_or_judge_attempt_failed"
        or supersedes.get("provider_error_code") != "serverOverloaded"
        or supersedes.get("pipeline_v3_replay_allowed") is not False
        or supersedes.get("failed_v3_shard_retry_allowed") is not False
        or supersedes.get("pipeline_v3_partial_calibration_scoring_allowed")
        is not False
    ):
        raise RuntimeLockError("overload-recovery runtime supersession is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 8:
        raise RuntimeLockError("overload-recovery runtime supersession is incomplete")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("overload-recovery artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("overload-recovery artifact label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {
        "runtime_lock_v4",
        "launch_receipt_v4",
        "pipeline_v3_terminal",
        "pipeline_v3_calibration_report",
        "pipeline_v3_failed_shard",
        "pipeline_v3_failed_ba_sidecar",
        "pipeline_v3_failed_ba_capacity",
        "pipeline_v3_reuse_contract",
    }
    if not required.issubset(labels):
        raise RuntimeLockError(
            "overload-recovery runtime lock is missing pipeline-v3 evidence"
        )
    return len(artifacts)


def _verify_judge_recovery_supersession(
    root: Path, manifest: dict[str, Any]
) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("judge-recovery runtime lock has no supersession")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v5"
        or supersedes.get("prior_runtime_lock_version")
        != OVERLOAD_RECOVERY_RUNTIME_LOCK_VERSION
        or supersedes.get("pipeline_v4_status") != "blocked"
        or supersedes.get("pipeline_v4_incident_classification")
        != "judge_calibration_gate_not_passed"
        or supersedes.get("pipeline_v4_transport_failed") is not False
        or supersedes.get("pipeline_v4_completed_turn_count") != 22
        or supersedes.get("pipeline_v4_usage_status") != "complete"
        or supersedes.get("pipeline_v4_replay_allowed") is not False
        or supersedes.get("pipeline_v4_judge_outputs_admissible_as_passing_evidence")
        is not False
    ):
        raise RuntimeLockError("judge-recovery runtime supersession is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 8:
        raise RuntimeLockError("judge-recovery runtime supersession is incomplete")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("judge-recovery artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("judge-recovery artifact label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {
        "runtime_lock_v5",
        "launch_receipt_v5",
        "terminal_receipt_v5",
        "pipeline_v4_terminal",
        "pipeline_v4_calibration_report",
        "pipeline_v4_reuse_contract",
        "pipeline_v5_reuse_contract",
        "pipeline_v5_fixture_truth_audit_receipt",
    }
    if not required.issubset(labels):
        raise RuntimeLockError(
            "judge-recovery runtime lock is missing pipeline-v4/v5 evidence"
        )
    return len(artifacts)


def _verify_judge_diagnostic_v2_supersession(
    root: Path, manifest: dict[str, Any]
) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("diagnostic-v2 runtime lock has no supersession")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v6"
        or supersedes.get("prior_runtime_lock_version")
        != JUDGE_RECOVERY_RUNTIME_LOCK_VERSION
        or supersedes.get("diagnostic_v1_status") != "blocked"
        or supersedes.get("diagnostic_v1_incident_classification")
        != "judge_diagnostic_quality_gate_not_passed"
        or supersedes.get("diagnostic_v1_transport_failed") is not False
        or supersedes.get("diagnostic_v1_completed_turn_count") != 4
        or supersedes.get("diagnostic_v1_usage_status") != "complete"
        or supersedes.get("diagnostic_v1_total_tokens") != 138211
        or supersedes.get("diagnostic_v1_replay_allowed") is not False
        or supersedes.get("diagnostic_v1_outputs_admissible_as_passing_evidence")
        is not False
    ):
        raise RuntimeLockError("diagnostic-v2 runtime supersession is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 8:
        raise RuntimeLockError("diagnostic-v2 runtime supersession is incomplete")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("diagnostic-v2 supersession artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("diagnostic-v2 supersession label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {
        "runtime_lock_v6",
        "launch_receipt_v6",
        "terminal_receipt_v6",
        "diagnostic_v1_spec",
        "diagnostic_v1_terminal",
        "diagnostic_v1_score",
        "diagnostic_v1_truth_audit_receipt",
        "diagnostic_v2_reuse_contract",
    }
    if not required.issubset(labels):
        raise RuntimeLockError("diagnostic-v2 runtime lock is missing v1 evidence")
    return len(artifacts)


def _verify_judge_diagnostic_v3_supersession(
    root: Path, manifest: dict[str, Any]
) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("diagnostic-v3 runtime lock has no supersession")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v7"
        or supersedes.get("prior_runtime_lock_version")
        != JUDGE_DIAGNOSTIC_V2_RUNTIME_LOCK_VERSION
        or supersedes.get("diagnostic_v2_status") != "failed"
        or supersedes.get("diagnostic_v2_incident_classification")
        != "infrastructure_or_judge_attempt_failed"
        or supersedes.get("diagnostic_v2_transport_failed") is not False
        or supersedes.get("diagnostic_v2_quality_scored") is not False
        or supersedes.get("diagnostic_v2_completed_turn_count") != 3
        or supersedes.get("diagnostic_v2_sidecar_usage_status") != "complete"
        or supersedes.get("diagnostic_v2_total_tokens") != 107187
        or supersedes.get("diagnostic_v2_replay_allowed") is not False
        or supersedes.get("diagnostic_v2_output_reuse_allowed") is not False
    ):
        raise RuntimeLockError("diagnostic-v3 runtime supersession is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 8:
        raise RuntimeLockError("diagnostic-v3 runtime supersession is incomplete")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("diagnostic-v3 supersession artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("diagnostic-v3 supersession label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {
        "runtime_lock_v7",
        "launch_receipt_v7",
        "terminal_receipt_v7",
        "diagnostic_v2_spec",
        "diagnostic_v2_terminal",
        "diagnostic_v2_failure",
        "diagnostic_v2_attempt_audit_receipt",
        "diagnostic_v3_reuse_contract",
    }
    if not required.issubset(labels):
        raise RuntimeLockError("diagnostic-v3 runtime lock is missing v2 evidence")
    return len(artifacts)


def _verify_judge_diagnostic_v4_supersession(
    root: Path, manifest: dict[str, Any]
) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("diagnostic-v4 runtime lock has no supersession")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v8"
        or supersedes.get("prior_runtime_lock_version")
        != JUDGE_DIAGNOSTIC_V3_RUNTIME_LOCK_VERSION
        or supersedes.get("diagnostic_v3_status") != "blocked"
        or supersedes.get("diagnostic_v3_incident_classification")
        != "judge_diagnostic_quality_gate_not_passed"
        or supersedes.get("diagnostic_v3_only_failed_gate") != "order_bias"
        or supersedes.get("diagnostic_v3_completed_turn_count") != 4
        or supersedes.get("diagnostic_v3_usage_status") != "complete"
        or supersedes.get("diagnostic_v3_total_tokens") != 150769
        or supersedes.get("diagnostic_v3_replay_allowed") is not False
        or supersedes.get("diagnostic_v3_output_reuse_allowed") is not False
    ):
        raise RuntimeLockError("diagnostic-v4 runtime supersession is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 8:
        raise RuntimeLockError("diagnostic-v4 runtime supersession is incomplete")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("diagnostic-v4 supersession artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("diagnostic-v4 supersession label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {
        "runtime_lock_v8",
        "launch_receipt_v8",
        "terminal_receipt_v8",
        "diagnostic_v3_spec",
        "diagnostic_v3_terminal",
        "diagnostic_v3_score",
        "diagnostic_v3_quality_audit_receipt",
        "diagnostic_v4_reuse_contract",
    }
    if not required.issubset(labels):
        raise RuntimeLockError("diagnostic-v4 runtime lock is missing v3 evidence")
    return len(artifacts)


def _verify_judge_diagnostic_v5_supersession(
    root: Path, manifest: dict[str, Any]
) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("diagnostic-v5 runtime lock has no supersession")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v9"
        or supersedes.get("prior_runtime_lock_version")
        != JUDGE_DIAGNOSTIC_V4_RUNTIME_LOCK_VERSION
        or supersedes.get("diagnostic_v4_status") != "blocked"
        or supersedes.get("diagnostic_v4_incident_classification")
        != "judge_diagnostic_quality_gate_not_passed"
        or supersedes.get("diagnostic_v4_only_failed_gate") != "order_bias"
        or supersedes.get("diagnostic_v4_completed_turn_count") != 4
        or supersedes.get("diagnostic_v4_usage_status") != "complete"
        or supersedes.get("diagnostic_v4_total_tokens") != 147819
        or supersedes.get("diagnostic_v4_fixture_truth_defect") != "event_type"
        or supersedes.get("diagnostic_v4_replay_allowed") is not False
        or supersedes.get("diagnostic_v4_output_reuse_allowed") is not False
    ):
        raise RuntimeLockError("diagnostic-v5 runtime supersession is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 10:
        raise RuntimeLockError("diagnostic-v5 runtime supersession is incomplete")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("diagnostic-v5 supersession artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("diagnostic-v5 supersession label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {
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
    }
    if not required.issubset(labels):
        raise RuntimeLockError("diagnostic-v5 runtime lock is missing v4 evidence")
    return len(artifacts)


def _verify_judge_diagnostic_v6_supersession(
    root: Path, manifest: dict[str, Any]
) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("diagnostic-v6 runtime lock has no supersession")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v10"
        or supersedes.get("prior_runtime_lock_version")
        != JUDGE_DIAGNOSTIC_V5_RUNTIME_LOCK_VERSION
        or supersedes.get("diagnostic_v5_status") != "blocked"
        or supersedes.get("diagnostic_v5_incident_classification")
        != "judge_diagnostic_quality_gate_not_passed"
        or supersedes.get("diagnostic_v5_only_failed_gate") != "order_bias"
        or supersedes.get("diagnostic_v5_completed_turn_count") != 4
        or supersedes.get("diagnostic_v5_usage_status") != "complete"
        or supersedes.get("diagnostic_v5_total_tokens") != 149074
        or supersedes.get("diagnostic_v5_replay_allowed") is not False
        or supersedes.get("diagnostic_v5_output_reuse_allowed") is not False
    ):
        raise RuntimeLockError("diagnostic-v6 runtime supersession is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 9:
        raise RuntimeLockError("diagnostic-v6 runtime supersession is incomplete")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("diagnostic-v6 supersession artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("diagnostic-v6 supersession label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {
        "runtime_lock_v10",
        "launch_receipt_v10",
        "terminal_receipt_v10",
        "diagnostic_v5_spec",
        "diagnostic_v5_terminal",
        "diagnostic_v5_score",
        "diagnostic_v5_quality_audit_receipt",
        "diagnostic_v6_reuse_contract",
        "diagnostic_fixture_patch_v3",
    }
    if not required.issubset(labels):
        raise RuntimeLockError("diagnostic-v6 runtime lock is missing v5 evidence")
    return len(artifacts)


def _verify_judge_full_calibration_supersession(
    root: Path, manifest: dict[str, Any]
) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("full-calibration runtime lock has no supersession")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v11"
        or supersedes.get("prior_runtime_lock_version")
        != JUDGE_DIAGNOSTIC_V6_RUNTIME_LOCK_VERSION
        or supersedes.get("diagnostic_v6_status") != "passed"
        or supersedes.get("diagnostic_v6_terminal_classification")
        != "judge_diagnostic_passed_full_calibration_authorized"
        or supersedes.get("diagnostic_v6_protocol_version")
        != "pif_app_server_judge_protocol_v5_4"
        or supersedes.get("diagnostic_v6_completed_turn_count") != 3
        or supersedes.get("diagnostic_v6_usage_status") != "complete"
        or supersedes.get("diagnostic_v6_total_tokens") != 122896
        or supersedes.get("diagnostic_v6_full_calibration_authorized") is not True
        or supersedes.get("diagnostic_v6_replay_allowed") is not False
        or supersedes.get("diagnostic_v6_output_reuse_allowed") is not False
    ):
        raise RuntimeLockError("full-calibration runtime supersession is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 8:
        raise RuntimeLockError("full-calibration runtime supersession is incomplete")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("full-calibration supersession artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("full-calibration supersession label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {
        "runtime_lock_v11",
        "launch_receipt_v11",
        "terminal_receipt_v11",
        "diagnostic_v6_spec",
        "diagnostic_v6_terminal",
        "diagnostic_v6_score",
        "judge_v5_4_freeze_receipt",
        "diagnostic_fixture_patch_v3",
    }
    if not required.issubset(labels):
        raise RuntimeLockError("full-calibration runtime lock is missing passing evidence")
    return len(artifacts)


def _verify_judge_full_calibration_recovery_supersession(
    root: Path, manifest: dict[str, Any]
) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("full-calibration recovery lock has no supersession")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v12"
        or supersedes.get("prior_runtime_lock_version")
        != JUDGE_FULL_CALIBRATION_RUNTIME_LOCK_VERSION
        or supersedes.get("calibration_v1_status") != "failed"
        or supersedes.get("calibration_v1_incident_classification")
        != "infrastructure_or_judge_attempt_failed"
        or supersedes.get("calibration_v1_quality_scored") is not False
        or supersedes.get("calibration_v1_completed_turn_count") != 12
        or supersedes.get("calibration_v1_usage_status") != "complete"
        or supersedes.get("calibration_v1_total_tokens") != 496703
        or supersedes.get("calibration_v1_replay_allowed") is not False
        or supersedes.get("calibration_v1_output_reuse_allowed") is not False
    ):
        raise RuntimeLockError("full-calibration recovery supersession is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 10:
        raise RuntimeLockError("full-calibration recovery supersession is incomplete")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("full-calibration recovery artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("full-calibration recovery label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {
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
    }
    if not required.issubset(labels):
        raise RuntimeLockError("full-calibration recovery lock is missing v1 evidence")
    return len(artifacts)


def _verify_judge_fixture_truth_audit_supersession(
    root: Path, manifest: dict[str, Any]
) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("fixture-truth audit lock has no supersession")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v13"
        or supersedes.get("prior_runtime_lock_version")
        != JUDGE_FULL_CALIBRATION_RECOVERY_RUNTIME_LOCK_VERSION
        or supersedes.get("calibration_v2_status") != "reference_truth_audit_required"
        or supersedes.get("calibration_v2_usage_status") != "complete"
        or supersedes.get("calibration_v2_total_tokens") != 543162
        or supersedes.get("calibration_v2_selection_authorized") is not False
        or supersedes.get("calibration_v2_reference_truth_admissible") is not False
        or supersedes.get("calibration_v2_output_reuse_for_audit") is not False
        or supersedes.get("unlaunched_audit_v1_semantic_turn_count") != 0
        or supersedes.get("unlaunched_audit_v1_replay_allowed") is not False
    ):
        raise RuntimeLockError("fixture-truth audit supersession is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 8:
        raise RuntimeLockError("fixture-truth audit supersession is incomplete")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("fixture-truth audit artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("fixture-truth audit label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {
        "runtime_lock_v13",
        "launch_receipt_v13",
        "terminal_receipt_v13",
        "calibration_v2_terminal",
        "calibration_v2_score",
        "fixture_truth_audit_receipt",
        "judge_v5_4_freeze_receipt",
        "unlaunched_fixture_audit_v1_spec",
    }
    if not required.issubset(labels):
        raise RuntimeLockError("fixture-truth audit lock is missing predecessor evidence")
    return len(artifacts)


def _verify_judge_fixture_truth_audit_recovery_supersession(
    root: Path, manifest: dict[str, Any]
) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("fixture-audit recovery lock has no supersession")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v14"
        or supersedes.get("prior_runtime_lock_version")
        != JUDGE_FIXTURE_TRUTH_AUDIT_RUNTIME_LOCK_VERSION
        or supersedes.get("audit_v2_status") != "failed"
        or supersedes.get("audit_v2_incident_classification")
        != "infrastructure_or_judge_attempt_failed"
        or supersedes.get("audit_v2_error_class") != "JudgeV5ProtocolError"
        or supersedes.get("audit_v2_failed_turn") != "neutral_alignment_canary"
        or supersedes.get("audit_v2_completed_turn_count") != 23
        or supersedes.get("audit_v2_usage_status") != "complete"
        or supersedes.get("audit_v2_total_tokens") != 827876
        or supersedes.get("audit_v2_replay_allowed") is not False
        or supersedes.get("audit_v2_partial_output_selection_allowed") is not False
        or supersedes.get("recovery_new_semantic_turn_count") != 1
    ):
        raise RuntimeLockError("fixture-audit recovery supersession is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 7:
        raise RuntimeLockError("fixture-audit recovery supersession is incomplete")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("fixture-audit recovery artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("fixture-audit recovery label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {
        "runtime_lock_v14",
        "launch_intent_v14",
        "launch_receipt_v14",
        "audit_v2_spec",
        "audit_v2_terminal",
        "audit_v2_failure",
        "audit_v2_canary_output",
    }
    if not required.issubset(labels):
        raise RuntimeLockError("fixture-audit recovery lock is missing v2 evidence")
    return len(artifacts)


def _verify_judge_fixture_reference_adjudication_supersession(
    root: Path, manifest: dict[str, Any]
) -> int:
    supersedes = manifest.get("supersedes")
    if not isinstance(supersedes, dict):
        raise RuntimeLockError("fixture-reference adjudication lock has no supersession")
    if (
        supersedes.get("schema_version")
        != "pif_app_server_unattended_runtime_lock_supersession_v15"
        or supersedes.get("prior_runtime_lock_version")
        != JUDGE_FIXTURE_TRUTH_AUDIT_RECOVERY_RUNTIME_LOCK_VERSION
        or supersedes.get("audit_v3_status") != "completed"
        or supersedes.get("audit_v3_terminal_reason")
        != "fixture_truth_proposal_completed_reference_adjudication_required"
        or supersedes.get("audit_v3_cumulative_tokens") != 854550
        or supersedes.get("audit_v3_reference_freeze_authorized") is not False
        or supersedes.get("audit_v3_selection_authorized") is not False
        or supersedes.get("reference_pointwise_disputed_witness_count") != 102
        or supersedes.get("reference_pointwise_disputed_case_count") != 49
        or supersedes.get("reference_alignment_disputed_case_count") != 18
        or supersedes.get("reference_adjudication_turn_count") != 12
    ):
        raise RuntimeLockError("fixture-reference adjudication supersession is malformed")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) < 7:
        raise RuntimeLockError("fixture-reference adjudication supersession is incomplete")
    labels: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise RuntimeLockError("fixture-reference adjudication artifact is malformed")
        label = item.get("label")
        if not isinstance(label, str) or not label or label in labels:
            raise RuntimeLockError("fixture-reference adjudication label is invalid")
        _resolve_frozen_file(
            root=root,
            relative=item.get("path"),
            expected_sha=item.get("sha256"),
            expected_size=item.get("size_bytes"),
            purpose=f"superseded {label}",
        )
        labels.add(label)
    required = {
        "runtime_lock_v15",
        "launch_intent_v15",
        "launch_receipt_v15",
        "audit_v3_terminal",
        "audit_v3_comparison",
        "audit_v3_adoption_receipt",
        "audit_v3_reconciled_alignment",
    }
    if not required.issubset(labels):
        raise RuntimeLockError("fixture-reference lock is missing audit evidence")
    return len(artifacts)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_runtime_lock(*, repo_root: Path, manifest_path: Path) -> dict[str, Any]:
    root = Path(repo_root).expanduser().resolve()
    manifest_file = Path(manifest_path).expanduser().resolve()
    try:
        manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeLockError("runtime lock manifest is unreadable") from exc
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") not in SUPPORTED_RUNTIME_LOCK_VERSIONS
    ):
        raise RuntimeLockError("runtime lock manifest version is unsupported")
    superseded_artifact_count = 0
    if manifest.get("schema_version") == SUPERSEDING_RUNTIME_LOCK_VERSION:
        superseded_artifact_count = _verify_supersession(root, manifest)
    elif manifest.get("schema_version") == SHARDED_RUNTIME_LOCK_VERSION:
        superseded_artifact_count = _verify_sharded_supersession(root, manifest)
    elif manifest.get("schema_version") == SCHEMA_COMPAT_RUNTIME_LOCK_VERSION:
        superseded_artifact_count = _verify_schema_compat_supersession(root, manifest)
    elif manifest.get("schema_version") == OVERLOAD_RECOVERY_RUNTIME_LOCK_VERSION:
        superseded_artifact_count = _verify_overload_recovery_supersession(
            root, manifest
        )
    elif manifest.get("schema_version") == JUDGE_RECOVERY_RUNTIME_LOCK_VERSION:
        superseded_artifact_count = _verify_judge_recovery_supersession(
            root, manifest
        )
    elif manifest.get("schema_version") == JUDGE_DIAGNOSTIC_V2_RUNTIME_LOCK_VERSION:
        superseded_artifact_count = _verify_judge_diagnostic_v2_supersession(
            root, manifest
        )
    elif manifest.get("schema_version") == JUDGE_DIAGNOSTIC_V3_RUNTIME_LOCK_VERSION:
        superseded_artifact_count = _verify_judge_diagnostic_v3_supersession(
            root, manifest
        )
    elif manifest.get("schema_version") == JUDGE_DIAGNOSTIC_V4_RUNTIME_LOCK_VERSION:
        superseded_artifact_count = _verify_judge_diagnostic_v4_supersession(
            root, manifest
        )
    elif manifest.get("schema_version") == JUDGE_DIAGNOSTIC_V5_RUNTIME_LOCK_VERSION:
        superseded_artifact_count = _verify_judge_diagnostic_v5_supersession(
            root, manifest
        )
    elif manifest.get("schema_version") == JUDGE_DIAGNOSTIC_V6_RUNTIME_LOCK_VERSION:
        superseded_artifact_count = _verify_judge_diagnostic_v6_supersession(
            root, manifest
        )
    elif manifest.get("schema_version") == JUDGE_FULL_CALIBRATION_RUNTIME_LOCK_VERSION:
        superseded_artifact_count = _verify_judge_full_calibration_supersession(
            root, manifest
        )
    elif (
        manifest.get("schema_version")
        == JUDGE_FULL_CALIBRATION_RECOVERY_RUNTIME_LOCK_VERSION
    ):
        superseded_artifact_count = (
            _verify_judge_full_calibration_recovery_supersession(root, manifest)
        )
    elif (
        manifest.get("schema_version")
        == JUDGE_FIXTURE_TRUTH_AUDIT_RUNTIME_LOCK_VERSION
    ):
        superseded_artifact_count = _verify_judge_fixture_truth_audit_supersession(
            root, manifest
        )
    elif (
        manifest.get("schema_version")
        == JUDGE_FIXTURE_TRUTH_AUDIT_RECOVERY_RUNTIME_LOCK_VERSION
    ):
        superseded_artifact_count = (
            _verify_judge_fixture_truth_audit_recovery_supersession(root, manifest)
        )
    elif (
        manifest.get("schema_version")
        == JUDGE_FIXTURE_REFERENCE_ADJUDICATION_RUNTIME_LOCK_VERSION
    ):
        superseded_artifact_count = (
            _verify_judge_fixture_reference_adjudication_supersession(root, manifest)
        )
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise RuntimeLockError("runtime lock manifest contains no files")
    observed: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in files:
        if not isinstance(item, dict):
            raise RuntimeLockError("runtime lock file entry is malformed")
        relative = item.get("path")
        expected_sha = item.get("sha256")
        expected_size = item.get("size_bytes")
        if not isinstance(relative, str) or relative in seen:
            raise RuntimeLockError("runtime lock file entry is invalid")
        path = _resolve_frozen_file(
            root=root,
            relative=relative,
            expected_sha=expected_sha,
            expected_size=expected_size,
            purpose="frozen runtime",
        )
        observed_sha = _sha256_file(path)
        observed_size = path.stat().st_size
        seen.add(relative)
        observed.append(
            {"path": relative, "sha256": observed_sha, "size_bytes": observed_size}
        )
    return {
        "ok": True,
        "schema_version": manifest["schema_version"],
        "manifest_path": str(manifest_file),
        "manifest_sha256": _sha256_file(manifest_file),
        "verified_file_count": len(observed),
        "verified_superseded_artifact_count": superseded_artifact_count,
        "production_mutation_performed": False,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify the frozen app-server runtime files.")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", required=True)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = verify_runtime_lock(
            repo_root=Path(args.repo_root), manifest_path=Path(args.manifest)
        )
    except RuntimeLockError:
        print(json.dumps({"ok": False, "status": "failed_closed"}, sort_keys=True))
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
