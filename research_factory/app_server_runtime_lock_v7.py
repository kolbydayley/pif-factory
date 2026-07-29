from __future__ import annotations

"""Build runtime-lock v7 for pipeline-v5 diagnostic-v2."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Optional, Sequence

from .app_server_runtime_lock import (
    JUDGE_DIAGNOSTIC_V2_RUNTIME_LOCK_VERSION,
    JUDGE_RECOVERY_RUNTIME_LOCK_VERSION,
    verify_runtime_lock,
)
from .app_server_v5_diagnostic_reuse import verify_diagnostic_v2_reuse_contract
from .util import now_iso, write_text_atomic


CURRENT_RUNTIME_FILES = (
    "automation/resume-app-server-evaluation-v8.sh",
    "research_factory/app_server_runtime_lock.py",
    "research_factory/app_server_runtime_lock_v7.py",
    "research_factory/app_server_capacity.py",
    "research_factory/codex_app_server.py",
    "research_factory/app_server_llm_judge.py",
    "research_factory/app_server_judge_v5_fixture.py",
    "research_factory/app_server_judge_v5_diagnostic_audit.py",
    "research_factory/app_server_judge_v5.py",
    "research_factory/app_server_judge_v5_diagnostic.py",
    "research_factory/app_server_v2_reuse.py",
    "research_factory/app_server_v5_reuse.py",
    "research_factory/app_server_v5_diagnostic_reuse.py",
    "research_factory/util.py",
    "research_factory/evaluation/judge_calibration_v2.json",
    "research_factory/evaluation/judge_fixture_truth_audit_v3.json",
    "research_factory/evaluation/judge_v5_diagnostic_v1.json",
    "research_factory/evaluation/judge_v5_diagnostic_v1_truth_audit.json",
    "research_factory/evaluation/judge_v5_diagnostic_v2.json",
    "research_factory/protocol/codex_app_server_0_144_1/codex_app_server_protocol.v2.schemas.json",
    "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v4.json",
    "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v5.json",
    "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-receipt-v1.json",
    "work/app-server-development-v2/unattended-pipeline-v5/diagnostic-v1-truth-audit-receipt-v1.json",
)


SUPERSEDED_ARTIFACTS = (
    ("runtime_lock_v6", "work/app-server-development-v2/unattended-runtime-lock-v6.json"),
    (
        "launch_receipt_v6",
        "work/app-server-development-v2/unattended-control-v6/launch-receipt-v6.json",
    ),
    (
        "terminal_receipt_v6",
        "work/app-server-development-v2/unattended-control-v6/terminal-receipt-v6.json",
    ),
    (
        "diagnostic_v1_spec",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-diagnostic-v1/diagnostic-spec.json",
    ),
    (
        "diagnostic_v1_terminal",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-diagnostic-v1/terminal.json",
    ),
    (
        "diagnostic_v1_score",
        "work/app-server-development-v2/unattended-pipeline-v5/judge-diagnostic-v1/diagnostic-score.json",
    ),
    (
        "diagnostic_v1_truth_audit_receipt",
        "work/app-server-development-v2/unattended-pipeline-v5/diagnostic-v1-truth-audit-receipt-v1.json",
    ),
    (
        "diagnostic_v2_reuse_contract",
        "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v5.json",
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


def build_runtime_lock_v7(
    *, repo_root: Path, manifest_path: Path, reuse_contract_path: Path
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    manifest = manifest_path.expanduser().resolve()
    reuse_path = reuse_contract_path.expanduser().resolve()
    contract = verify_diagnostic_v2_reuse_contract(reuse_path)
    expected = (
        root / "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v5.json"
    ).resolve()
    if (
        reuse_path != expected
        or Path(str(contract.get("target_diagnostic_root") or "")).resolve()
        != expected.parent / "judge-diagnostic-v2"
    ):
        raise ValueError("runtime lock v7 requires canonical diagnostic-v2 reuse")
    if manifest.exists():
        report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
        value = json.loads(manifest.read_text(encoding="utf-8"))
        if report.get("schema_version") != JUDGE_DIAGNOSTIC_V2_RUNTIME_LOCK_VERSION:
            raise ValueError("existing runtime lock is not v7")
        return value
    payload = {
        "schema_version": JUDGE_DIAGNOSTIC_V2_RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "purpose": (
            "Supersede immutable diagnostic-v1 after its measured quality failure and "
            "truth audit, then run one fresh diagnostic-v2 before full calibration."
        ),
        "supersedes": {
            "schema_version": "pif_app_server_unattended_runtime_lock_supersession_v6",
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
            "support_claim_and_structured_event_separate": True,
            "support_ab_ba_allowed": False,
            "alignment_origin_neutral_required": True,
            "canary_permuted_axes": [
                "anonymous_case_order",
                "anonymous_witness_order"
            ],
            "canary_rubric_and_decision_order_fixed": True,
            "adjudication_call_cap": 1,
            "fresh_diagnostic_case_count": 18,
            "full_calibration_allowed_before_diagnostic_v2_pass": False,
            "diagnostic_v1_replay_allowed": False,
            "pipeline_v1_replay_allowed": False,
            "pipeline_v2_replay_allowed": False,
            "pipeline_v3_replay_allowed": False,
            "pipeline_v4_replay_allowed": False,
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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build app-server runtime lock v7")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--reuse-contract", required=True)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    payload = build_runtime_lock_v7(
        repo_root=Path(args.repo_root),
        manifest_path=Path(args.manifest),
        reuse_contract_path=Path(args.reuse_contract),
    )
    print(
        json.dumps(
            {
                "ok": True,
                "schema_version": payload["schema_version"],
                "file_count": len(payload["files"]),
                "superseded_artifact_count": len(payload["supersedes"]["artifacts"]),
                "manifest_path": str(Path(args.manifest).expanduser().resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
