from __future__ import annotations

"""Build runtime-lock v16 for independent fixture-reference adjudication."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Optional, Sequence

from .app_server_runtime_lock import (
    JUDGE_FIXTURE_REFERENCE_ADJUDICATION_RUNTIME_LOCK_VERSION,
    JUDGE_FIXTURE_TRUTH_AUDIT_RECOVERY_RUNTIME_LOCK_VERSION,
    verify_runtime_lock,
)
from .app_server_runtime_lock_v15 import CURRENT_RUNTIME_FILES as V15_RUNTIME_FILES
from .util import now_iso, write_text_atomic


CURRENT_RUNTIME_FILES = tuple(
    "automation/resume-app-server-evaluation-v17.sh"
    if path == "automation/resume-app-server-evaluation-v16.sh"
    else "research_factory/app_server_runtime_lock_v16.py"
    if path == "research_factory/app_server_runtime_lock_v15.py"
    else path
    for path in V15_RUNTIME_FILES
) + ("research_factory/app_server_judge_v5_reference_adjudication.py",)

SUPERSEDED_ARTIFACTS = (
    ("runtime_lock_v15", "work/app-server-development-v2/unattended-runtime-lock-v15.json"),
    (
        "launch_intent_v15",
        "work/app-server-development-v2/unattended-control-v15/launch-intent-v15.json",
    ),
    (
        "launch_receipt_v15",
        "work/app-server-development-v2/unattended-control-v15/launch-receipt-v15.json",
    ),
    (
        "audit_v3_terminal",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v3/terminal.json",
    ),
    (
        "audit_v3_comparison",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v3/comparison-vs-provisional.json",
    ),
    (
        "audit_v3_adoption_receipt",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v3/v2-adoption-receipt.json",
    ),
    (
        "audit_v3_reconciled_alignment",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v3/reconciled-alignment.private.json",
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


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("fixture-reference predecessor is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError("fixture-reference predecessor is not an object")
    return value


def build_runtime_lock_v16(*, repo_root: Path, manifest_path: Path) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    manifest = manifest_path.expanduser().resolve()
    terminal = _load(
        root
        / "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v3/terminal.json"
    )
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "fixture_truth_proposal_completed_reference_adjudication_required"
        or terminal.get("fixture_truth_proposal_completed") is not True
        or terminal.get("reference_freeze_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("usage_status") != "complete"
        or (terminal.get("cumulative_proposal_usage") or {}).get("total_tokens")
        != 854550
        or terminal.get("semantic_retry_count") != 0
    ):
        raise ValueError("fixture-audit v3 is unsafe for reference adjudication")
    if manifest.exists():
        report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
        if (
            report.get("schema_version")
            != JUDGE_FIXTURE_REFERENCE_ADJUDICATION_RUNTIME_LOCK_VERSION
        ):
            raise ValueError("existing runtime lock is not v16")
        return _load(manifest)
    payload = {
        "schema_version": JUDGE_FIXTURE_REFERENCE_ADJUDICATION_RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "purpose": (
            "Independently adjudicate only provisional-versus-proposal fixture disputes "
            "without exposing candidate labels or model identities."
        ),
        "supersedes": {
            "schema_version": "pif_app_server_unattended_runtime_lock_supersession_v15",
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
            "artifacts": [
                {"label": label, **_record(root, relative)}
                for label, relative in SUPERSEDED_ARTIFACTS
            ],
        },
        "policy": {
            "execution_purpose": "fixture_reference_adjudication",
            "model": "gpt-5.6-luna",
            "reasoning_effort": "high",
            "pointwise_disputed_witness_count": 102,
            "pointwise_disputed_case_count": 49,
            "alignment_disputed_case_count": 18,
            "cases_per_shard_maximum": 6,
            "pointwise_turn_count": 9,
            "alignment_turn_count": 3,
            "total_turn_count": 12,
            "retry_count_per_turn": 0,
            "adjudication_passes_per_item": 1,
            "candidate_labels_exposed_to_adjudicator": False,
            "model_identities_exposed_to_adjudicator": False,
            "majority_voting_allowed": False,
            "managed_chatgpt_auth_only": True,
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
            "codex_exec_semantic_calls_allowed": False,
            "official_persistent_codex_app_server_only": True,
            "semantic_turn_maximum_primary_used_percent": 20,
            "per_turn_capacity_checkpoint_required": True,
            "reference_freeze_requires_zero_abstentions": True,
            "selection_authorized": False,
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
    parser = argparse.ArgumentParser(description="Build fixture-reference lock v16")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args(argv)
    payload = build_runtime_lock_v16(
        repo_root=Path(args.repo_root), manifest_path=Path(args.manifest)
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
