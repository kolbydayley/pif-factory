from __future__ import annotations

"""Build runtime-lock v11 for pipeline-v5 diagnostic-v6."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Optional, Sequence

from .app_server_runtime_lock import (
    JUDGE_DIAGNOSTIC_V5_RUNTIME_LOCK_VERSION,
    JUDGE_DIAGNOSTIC_V6_RUNTIME_LOCK_VERSION,
    verify_runtime_lock,
)
from .app_server_runtime_lock_v10 import CURRENT_RUNTIME_FILES as V10_RUNTIME_FILES
from .app_server_v5_diagnostic_v6_reuse import verify_diagnostic_v6_reuse_contract
from .util import now_iso, write_text_atomic


CURRENT_RUNTIME_FILES = tuple(
    "automation/resume-app-server-evaluation-v12.sh"
    if path == "automation/resume-app-server-evaluation-v11.sh"
    else "research_factory/app_server_runtime_lock_v11.py"
    if path == "research_factory/app_server_runtime_lock_v10.py"
    else path
    for path in V10_RUNTIME_FILES
) + (
    "research_factory/app_server_judge_v5_diagnostic_v5_audit.py",
    "research_factory/app_server_v5_diagnostic_v6_reuse.py",
    "research_factory/evaluation/judge_v5_diagnostic_v5_quality_audit.json",
    "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v9.json",
    "work/app-server-development-v2/unattended-pipeline-v5/diagnostic-v5-quality-audit-receipt-v1.json",
)


SUPERSEDED_ARTIFACTS = (
    ("runtime_lock_v10", "work/app-server-development-v2/unattended-runtime-lock-v10.json"),
    (
        "launch_receipt_v10",
        "work/app-server-development-v2/unattended-control-v10/launch-receipt-v10.json",
    ),
    (
        "terminal_receipt_v10",
        "work/app-server-development-v2/unattended-control-v10/terminal-receipt-v10.json",
    ),
    (
        "diagnostic_v5_spec",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-diagnostic-v5/diagnostic-spec.json",
    ),
    (
        "diagnostic_v5_terminal",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-diagnostic-v5/terminal.json",
    ),
    (
        "diagnostic_v5_score",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-diagnostic-v5/diagnostic-score.json",
    ),
    (
        "diagnostic_v5_quality_audit_receipt",
        "work/app-server-development-v2/unattended-pipeline-v5/diagnostic-v5-quality-audit-receipt-v1.json",
    ),
    (
        "diagnostic_v6_reuse_contract",
        "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v9.json",
    ),
    (
        "diagnostic_fixture_patch_v3",
        "research_factory/evaluation/judge_v5_diagnostic_v3.json",
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


def build_runtime_lock_v11(
    *, repo_root: Path, manifest_path: Path, reuse_contract_path: Path
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    manifest = manifest_path.expanduser().resolve()
    reuse_path = reuse_contract_path.expanduser().resolve()
    contract = verify_diagnostic_v6_reuse_contract(reuse_path)
    expected = (
        root / "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v9.json"
    ).resolve()
    if (
        reuse_path != expected
        or Path(str(contract.get("target_diagnostic_root") or "")).resolve()
        != expected.parent / "judge-diagnostic-v6"
    ):
        raise ValueError("runtime lock v11 requires canonical diagnostic-v6 reuse")
    if manifest.exists():
        report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
        value = json.loads(manifest.read_text(encoding="utf-8"))
        if report.get("schema_version") != JUDGE_DIAGNOSTIC_V6_RUNTIME_LOCK_VERSION:
            raise ValueError("existing runtime lock is not v11")
        return value
    payload = {
        "schema_version": JUDGE_DIAGNOSTIC_V6_RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "purpose": (
            "Supersede immutable diagnostic-v5 after its order-only boundary/event-type "
            "failure, then run one full fresh diagnostic-v6 with explicit boundary/evidence "
            "consistency and unchanged fixture truth and gates."
        ),
        "supersedes": {
            "schema_version": "pif_app_server_unattended_runtime_lock_supersession_v10",
            "prior_runtime_lock_version": JUDGE_DIAGNOSTIC_V5_RUNTIME_LOCK_VERSION,
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
            "diagnostic_v5_output_reuse_allowed": False,
            "fixture_truth_changed": False,
            "gate_thresholds_changed": False,
            "boundary_evidence_consistency_rule_added": True,
            "unsupported_assertion_not_boundary_rule_added": True,
            "event_type_direct_category_rule_added": True,
            "support_ab_ba_allowed": False,
            "adjudication_call_cap": 1,
            "full_calibration_allowed_before_diagnostic_v6_pass": False,
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
    parser = argparse.ArgumentParser(description="Build app-server runtime lock v11")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--reuse-contract", required=True)
    args = parser.parse_args(argv)
    payload = build_runtime_lock_v11(
        repo_root=Path(args.repo_root),
        manifest_path=Path(args.manifest),
        reuse_contract_path=Path(args.reuse_contract),
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
