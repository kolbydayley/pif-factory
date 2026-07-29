from __future__ import annotations

"""Build runtime-lock v15 for the one-turn fixture-audit recovery."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Optional, Sequence

from .app_server_runtime_lock import (
    JUDGE_FIXTURE_TRUTH_AUDIT_RECOVERY_RUNTIME_LOCK_VERSION,
    JUDGE_FIXTURE_TRUTH_AUDIT_RUNTIME_LOCK_VERSION,
    verify_runtime_lock,
)
from .app_server_runtime_lock_v14 import CURRENT_RUNTIME_FILES as V14_RUNTIME_FILES
from .util import now_iso, write_text_atomic


CURRENT_RUNTIME_FILES = tuple(
    "automation/resume-app-server-evaluation-v16.sh"
    if path == "automation/resume-app-server-evaluation-v15.sh"
    else "research_factory/app_server_runtime_lock_v15.py"
    if path == "research_factory/app_server_runtime_lock_v14.py"
    else path
    for path in V14_RUNTIME_FILES
) + ("research_factory/app_server_judge_v5_fixture_audit_recovery.py",)

SUPERSEDED_ARTIFACTS = (
    ("runtime_lock_v14", "work/app-server-development-v2/unattended-runtime-lock-v14.json"),
    (
        "launch_intent_v14",
        "work/app-server-development-v2/unattended-control-v14/launch-intent-v14.json",
    ),
    (
        "launch_receipt_v14",
        "work/app-server-development-v2/unattended-control-v14/launch-receipt-v14.json",
    ),
    (
        "audit_v2_spec",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v2/fixture-audit-spec.json",
    ),
    (
        "audit_v2_terminal",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v2/terminal.json",
    ),
    (
        "audit_v2_failure",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v2/failure.json",
    ),
    (
        "audit_v2_canary_output",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v2/turns/neutral-alignment-canary/output.private.json",
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
        raise ValueError("fixture-audit recovery predecessor is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError("fixture-audit recovery predecessor is not an object")
    return value


def build_runtime_lock_v15(*, repo_root: Path, manifest_path: Path) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    manifest = manifest_path.expanduser().resolve()
    terminal = _load(
        root
        / "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v2/terminal.json"
    )
    failure = _load(
        root
        / "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v2/failure.json"
    )
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("usage_status") != "complete"
        or (terminal.get("usage") or {}).get("total_tokens") != 827876
        or terminal.get("selection_authorized") is not False
        or failure.get("failed_turn_name") != "neutral_alignment_canary"
        or failure.get("error_class") != "JudgeV5ProtocolError"
        or failure.get("unknown_usage_turn_count") != 0
        or len(failure.get("attempts") or []) != 23
    ):
        raise ValueError("fixture-audit v2 terminal is unsafe for recovery")
    if manifest.exists():
        report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
        if (
            report.get("schema_version")
            != JUDGE_FIXTURE_TRUTH_AUDIT_RECOVERY_RUNTIME_LOCK_VERSION
        ):
            raise ValueError("existing runtime lock is not v15")
        return _load(manifest)
    payload = {
        "schema_version": JUDGE_FIXTURE_TRUTH_AUDIT_RECOVERY_RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "purpose": (
            "Adopt all 23 complete fixture-audit v2 outputs without semantic mutation, "
            "then run one capped side-free permutation adjudication."
        ),
        "supersedes": {
            "schema_version": "pif_app_server_unattended_runtime_lock_supersession_v14",
            "prior_runtime_lock_version": JUDGE_FIXTURE_TRUTH_AUDIT_RUNTIME_LOCK_VERSION,
            "audit_v2_status": "failed",
            "audit_v2_incident_classification": "infrastructure_or_judge_attempt_failed",
            "audit_v2_error_class": "JudgeV5ProtocolError",
            "audit_v2_failed_turn": "neutral_alignment_canary",
            "audit_v2_completed_turn_count": 23,
            "audit_v2_usage_status": "complete",
            "audit_v2_total_tokens": 827876,
            "audit_v2_replay_allowed": False,
            "audit_v2_partial_output_selection_allowed": False,
            "recovery_new_semantic_turn_count": 1,
            "artifacts": [
                {"label": label, **_record(root, relative)}
                for label, relative in SUPERSEDED_ARTIFACTS
            ],
        },
        "policy": {
            "execution_purpose": "fixture_truth_audit_permutation_recovery",
            "source_attempt_count": 23,
            "source_outputs_adopted_as_complete_set": True,
            "source_output_selection_allowed": False,
            "source_semantic_replay_allowed": False,
            "semantic_fields_modified_during_adoption": False,
            "scoreable_root_omissions_preserved_as_model_errors": True,
            "model": "gpt-5.5",
            "reasoning_effort": "high",
            "new_semantic_turn_count": 1,
            "new_semantic_retry_count": 0,
            "managed_chatgpt_auth_only": True,
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
            "codex_exec_semantic_calls_allowed": False,
            "official_persistent_codex_app_server_only": True,
            "semantic_turn_maximum_primary_used_percent": 20,
            "per_turn_capacity_checkpoint_required": True,
            "proposal_can_authorize_reference_freeze": False,
            "proposal_can_authorize_selection": False,
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
    parser = argparse.ArgumentParser(description="Build fixture-audit recovery lock v15")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args(argv)
    payload = build_runtime_lock_v15(
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
