from __future__ import annotations

"""Build runtime-lock v14 for the independent fixture-truth audit."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Optional, Sequence

from .app_server_judge_v5_calibration_runner import (
    verify_calibration_v1_audit_receipt,
    verify_fixture_truth_audit_receipt,
    verify_judge_freeze_receipt,
)
from .app_server_runtime_lock import (
    JUDGE_FIXTURE_TRUTH_AUDIT_RUNTIME_LOCK_VERSION,
    JUDGE_FULL_CALIBRATION_RECOVERY_RUNTIME_LOCK_VERSION,
    verify_runtime_lock,
)
from .app_server_runtime_lock_v13 import CURRENT_RUNTIME_FILES as V13_RUNTIME_FILES
from .util import now_iso, write_text_atomic


CURRENT_RUNTIME_FILES = tuple(
    "automation/resume-app-server-evaluation-v15.sh"
    if path == "automation/resume-app-server-evaluation-v14.sh"
    else "research_factory/app_server_runtime_lock_v14.py"
    if path == "research_factory/app_server_runtime_lock_v13.py"
    else path
    for path in V13_RUNTIME_FILES
)

SUPERSEDED_ARTIFACTS = (
    ("runtime_lock_v13", "work/app-server-development-v2/unattended-runtime-lock-v13.json"),
    (
        "launch_receipt_v13",
        "work/app-server-development-v2/unattended-control-v13/launch-receipt-v13.json",
    ),
    (
        "terminal_receipt_v13",
        "work/app-server-development-v2/unattended-control-v13/terminal-receipt-v13.json",
    ),
    (
        "calibration_v2_terminal",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-calibration-v5_4-v2/terminal.json",
    ),
    (
        "calibration_v2_score",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-calibration-v5_4-v2/calibration-score.json",
    ),
    (
        "fixture_truth_audit_receipt",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-receipt-v1.json",
    ),
    (
        "judge_v5_4_freeze_receipt",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-v5_4-freeze-receipt-v1.json",
    ),
    (
        "unlaunched_fixture_audit_v1_spec",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v1/audit-run-spec.json",
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


def _load_object(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("%s is missing or invalid" % purpose) from exc
    if not isinstance(value, dict):
        raise ValueError("%s is not an object" % purpose)
    return value


def build_runtime_lock_v14(
    *,
    repo_root: Path,
    manifest_path: Path,
    judge_freeze_receipt_path: Path,
    v1_audit_receipt_path: Path,
    fixture_truth_audit_receipt_path: Path,
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    manifest = manifest_path.expanduser().resolve()
    verify_judge_freeze_receipt(judge_freeze_receipt_path.expanduser().resolve())
    verify_calibration_v1_audit_receipt(v1_audit_receipt_path.expanduser().resolve())
    verify_fixture_truth_audit_receipt(
        fixture_truth_audit_receipt_path.expanduser().resolve()
    )
    terminal_receipt = _load_object(
        root
        / "work/app-server-development-v2/unattended-control-v13/terminal-receipt-v13.json",
        "calibration-v2 terminal receipt",
    )
    if (
        terminal_receipt.get("status") != "reference_truth_audit_required"
        or terminal_receipt.get("accounting_complete") is not True
        or terminal_receipt.get("usage_status") != "complete"
        or (terminal_receipt.get("usage") or {}).get("total_tokens") != 543162
        or terminal_receipt.get("selection_authorized") is not False
        or terminal_receipt.get("calibration_quality_score_admissible") is not False
        or terminal_receipt.get("calibration_v2_replay_allowed") is not False
        or terminal_receipt.get("calibration_v2_output_reuse_as_passing_evidence_allowed")
        is not False
    ):
        raise ValueError("calibration-v2 terminal receipt is unsafe for fixture audit")
    unlaunched_root = (
        root
        / "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v1"
    )
    if [path.name for path in unlaunched_root.iterdir()] != ["audit-run-spec.json"]:
        raise ValueError("fixture-audit v1 is not an unlaunched immutable specification")
    if manifest.exists():
        report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
        if report.get("schema_version") != JUDGE_FIXTURE_TRUTH_AUDIT_RUNTIME_LOCK_VERSION:
            raise ValueError("existing runtime lock is not v14")
        return _load_object(manifest, "runtime lock v14")
    payload = {
        "schema_version": JUDGE_FIXTURE_TRUTH_AUDIT_RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "purpose": (
            "Supersede the unlaunched 11-case audit draft and independently audit the "
            "66-case fixture truth with six-case side-free shards before any new calibration."
        ),
        "supersedes": {
            "schema_version": "pif_app_server_unattended_runtime_lock_supersession_v13",
            "prior_runtime_lock_version": JUDGE_FULL_CALIBRATION_RECOVERY_RUNTIME_LOCK_VERSION,
            "calibration_v2_status": "reference_truth_audit_required",
            "calibration_v2_usage_status": "complete",
            "calibration_v2_total_tokens": 543162,
            "calibration_v2_selection_authorized": False,
            "calibration_v2_reference_truth_admissible": False,
            "calibration_v2_output_reuse_for_audit": False,
            "unlaunched_audit_v1_semantic_turn_count": 0,
            "unlaunched_audit_v1_replay_allowed": False,
            "artifacts": [
                {"label": label, **_record(root, relative)}
                for label, relative in SUPERSEDED_ARTIFACTS
            ],
        },
        "policy": {
            "execution_purpose": "fixture_truth_audit",
            "model": "gpt-5.5",
            "reasoning_effort": "high",
            "case_count": 66,
            "witness_count": 182,
            "cases_per_shard": 6,
            "pointwise_shard_count": 11,
            "alignment_shard_count": 11,
            "permutation_canary_case_count": 12,
            "minimum_semantic_turn_count": 23,
            "maximum_semantic_turn_count": 24,
            "managed_chatgpt_auth_only": True,
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
            "codex_exec_semantic_calls_allowed": False,
            "official_persistent_codex_app_server_only": True,
            "semantic_turn_maximum_primary_used_percent": 20,
            "per_turn_capacity_checkpoint_required": True,
            "retry_count_per_semantic_turn": 0,
            "all_semantic_turns_run_fresh": True,
            "provisional_truth_exposed_to_model": False,
            "proposal_can_authorize_reference_freeze": False,
            "proposal_can_authorize_selection": False,
            "separate_disagreement_adjudication_required": True,
            "production_mutation_allowed": False,
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
    parser = argparse.ArgumentParser(description="Build fixture-truth audit runtime lock v14")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--judge-freeze-receipt", required=True)
    parser.add_argument("--v1-audit-receipt", required=True)
    parser.add_argument("--fixture-truth-audit-receipt", required=True)
    args = parser.parse_args(argv)
    payload = build_runtime_lock_v14(
        repo_root=Path(args.repo_root),
        manifest_path=Path(args.manifest),
        judge_freeze_receipt_path=Path(args.judge_freeze_receipt),
        v1_audit_receipt_path=Path(args.v1_audit_receipt),
        fixture_truth_audit_receipt_path=Path(args.fixture_truth_audit_receipt),
    )
    print(
        json.dumps(
            {
                "ok": True,
                "schema_version": payload["schema_version"],
                "file_count": len(payload["files"]),
                "superseded_artifact_count": len(payload["supersedes"]["artifacts"]),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
