from __future__ import annotations

"""Build runtime-lock v13 for full calibration-v2 recovery."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Optional, Sequence

from .app_server_judge_v5_calibration_runner import (
    verify_calibration_v1_audit_receipt,
    verify_judge_freeze_receipt,
)
from .app_server_runtime_lock import (
    JUDGE_FULL_CALIBRATION_RECOVERY_RUNTIME_LOCK_VERSION,
    JUDGE_FULL_CALIBRATION_RUNTIME_LOCK_VERSION,
    verify_runtime_lock,
)
from .app_server_runtime_lock_v12 import CURRENT_RUNTIME_FILES as V12_RUNTIME_FILES
from .util import now_iso, write_text_atomic


CURRENT_RUNTIME_FILES = tuple(
    "automation/resume-app-server-evaluation-v14.sh"
    if path == "automation/resume-app-server-evaluation-v13.sh"
    else "research_factory/app_server_runtime_lock_v13.py"
    if path == "research_factory/app_server_runtime_lock_v12.py"
    else path
    for path in V12_RUNTIME_FILES
) + (
    "research_factory/app_server_judge_v5_calibration_v1_audit.py",
    "work/app-server-development-v2/unattended-pipeline-v5/calibration-v1-failure-audit-receipt-v1.json",
)


SUPERSEDED_ARTIFACTS = (
    ("runtime_lock_v12", "work/app-server-development-v2/unattended-runtime-lock-v12.json"),
    (
        "launch_receipt_v12",
        "work/app-server-development-v2/unattended-control-v12/launch-receipt-v12.json",
    ),
    (
        "terminal_receipt_v12",
        "work/app-server-development-v2/unattended-control-v12/terminal-receipt-v12.json",
    ),
    (
        "calibration_v1_spec",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-calibration-v5_4-v1/calibration-spec.json",
    ),
    (
        "calibration_v1_terminal",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-calibration-v5_4-v1/terminal.json",
    ),
    (
        "calibration_v1_failure",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-calibration-v5_4-v1/failure.json",
    ),
    (
        "calibration_v1_failed_output",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-calibration-v5_4-v1/turns/neutral-alignment-base-shard-05/output.private.json",
    ),
    (
        "calibration_v1_failed_sidecar",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-calibration-v5_4-v1/turns/neutral-alignment-base-shard-05/sidecar.json",
    ),
    (
        "calibration_v1_audit_receipt",
        "work/app-server-development-v2/unattended-pipeline-v5/calibration-v1-failure-audit-receipt-v1.json",
    ),
    (
        "judge_v5_4_freeze_receipt",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-v5_4-freeze-receipt-v1.json",
    ),
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record(root: Path, relative: str) -> dict[str, Any]:
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ValueError("runtime lock path escapes the repository") from exc
    if not path.is_file():
        raise ValueError("runtime lock input is missing: %s" % relative)
    return {
        "path": relative,
        "sha256": _sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def build_runtime_lock_v13(
    *,
    repo_root: Path,
    manifest_path: Path,
    judge_freeze_receipt_path: Path,
    v1_audit_receipt_path: Path,
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    manifest = manifest_path.expanduser().resolve()
    freeze_path = judge_freeze_receipt_path.expanduser().resolve()
    audit_path = v1_audit_receipt_path.expanduser().resolve()
    verify_judge_freeze_receipt(freeze_path)
    audit = verify_calibration_v1_audit_receipt(audit_path)
    if (
        audit.get("calibration_v1_output_reuse_allowed") is not False
        or audit.get("calibration_v2_all_turns_must_run_fresh") is not True
    ):
        raise ValueError("runtime lock v13 requires canonical v1 recovery audit")
    if manifest.exists():
        report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
        value = json.loads(manifest.read_text(encoding="utf-8"))
        if (
            report.get("schema_version")
            != JUDGE_FULL_CALIBRATION_RECOVERY_RUNTIME_LOCK_VERSION
        ):
            raise ValueError("existing runtime lock is not v13")
        return value
    payload = {
        "schema_version": JUDGE_FULL_CALIBRATION_RECOVERY_RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "purpose": (
            "Supersede immutable full calibration-v1 after its scoreable semantic output "
            "was misclassified by an overconstrained validator, then run a full fresh v2."
        ),
        "supersedes": {
            "schema_version": "pif_app_server_unattended_runtime_lock_supersession_v12",
            "prior_runtime_lock_version": JUDGE_FULL_CALIBRATION_RUNTIME_LOCK_VERSION,
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
            "artifacts": [
                {"label": label, **_record(root, relative)}
                for label, relative in SUPERSEDED_ARTIFACTS
            ],
        },
        "policy": {
            "managed_chatgpt_auth_only": True,
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
            "codex_exec_semantic_calls_allowed": False,
            "official_persistent_codex_app_server_only": True,
            "production_mutation_allowed": False,
            "semantic_turn_maximum_primary_used_percent": 20,
            "per_turn_capacity_checkpoint_required": True,
            "retry_count_per_semantic_turn": 0,
            "all_semantic_turns_run_fresh": True,
            "calibration_v1_output_reuse_allowed": False,
            "judge_protocol_version": "pif_app_server_judge_protocol_v5_4",
            "judge_prompt_changes_allowed": False,
            "judge_rubric_changes_allowed": False,
            "gate_threshold_changes_allowed": False,
            "complete_semantic_outputs_are_scored": True,
            "unsupported_without_specific_root_is_hard_failure": False,
            "structural_validation_retained": True,
            "system_selection_allowed_before_calibration_v2_pass": False,
            "verify_before_semantic_work": True,
        },
        "files": [_record(root, relative) for relative in CURRENT_RUNTIME_FILES],
    }
    manifest.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(
        manifest,
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )
    verify_runtime_lock(repo_root=root, manifest_path=manifest)
    return payload


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build app-server runtime lock v13")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--judge-freeze-receipt", required=True)
    parser.add_argument("--v1-audit-receipt", required=True)
    args = parser.parse_args(argv)
    payload = build_runtime_lock_v13(
        repo_root=Path(args.repo_root),
        manifest_path=Path(args.manifest),
        judge_freeze_receipt_path=Path(args.judge_freeze_receipt),
        v1_audit_receipt_path=Path(args.v1_audit_receipt),
    )
    print(json.dumps({
        "ok": True,
        "schema_version": payload["schema_version"],
        "file_count": len(payload["files"]),
        "superseded_artifact_count": len(payload["supersedes"]["artifacts"]),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
