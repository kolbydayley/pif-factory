from __future__ import annotations

"""Build the superseding runtime lock for the sharded pipeline-v2 launcher."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Optional, Sequence

from .app_server_runtime_lock import (
    SHARDED_RUNTIME_LOCK_VERSION,
    SUPERSEDING_RUNTIME_LOCK_VERSION,
    verify_runtime_lock,
)
from .app_server_v2_reuse import verify_v2_reuse_contract
from .util import now_iso, write_text_atomic


CURRENT_RUNTIME_FILES = (
    "automation/resume-app-server-evaluation-v4.sh",
    "research_factory/app_server_runtime_lock.py",
    "research_factory/app_server_runtime_lock_v3.py",
    "research_factory/app_server_capacity.py",
    "research_factory/app_server_capacity_probe.py",
    "research_factory/app_server_checkpoint.py",
    "research_factory/codex_app_server.py",
    "research_factory/app_server_evaluation.py",
    "research_factory/efficient_backtest.py",
    "research_factory/windowed_evaluation.py",
    "research_factory/labels.py",
    "research_factory/paths.py",
    "research_factory/util.py",
    "research_factory/worker.py",
    "research_factory/db.py",
    "research_factory/app_server_llm_judge.py",
    "research_factory/app_server_dev_selection.py",
    "research_factory/app_server_sharded_calibration.py",
    "research_factory/app_server_sharded_selection.py",
    "research_factory/app_server_v2_reuse.py",
    "research_factory/app_server_holdout.py",
    "research_factory/app_server_holdout_stratifier.py",
    "research_factory/app_server_holdout_execution.py",
    "research_factory/app_server_holdout_judge.py",
    "research_factory/app_server_interrupted_arm_recovery.py",
    "research_factory/unattended_app_server_pipeline.py",
    "research_factory/unattended_app_server_pipeline_v2.py",
    "label_packs/ai_discourse_v3_1/prompt.md",
    "label_packs/ai_discourse_v3_1/schema.json",
    "label_packs/ai_discourse_v3_1/codebook.md",
    "research_factory/prompt_guidelines/windowed_event_core_v1.json",
    "research_factory/evaluation/judge_calibration_v2.json",
    "research_factory/evaluation/windowed_acceptance_v2.json",
    "work/app-server-development-v2/run-spec-v2.json",
    "work/app-server-development-v2/manifest.json",
    "work/app-server-development-v2/unattended-pipeline-v2/reuse-contract-v1.json",
    "work/windowed-acceptance-v1/paired-run-v2/evaluator-v2/context-usage-recovery-report.json",
)

SUPERSEDED_ARTIFACTS = (
    (
        "runtime_lock_v2",
        "work/app-server-development-v2/unattended-runtime-lock-v2.json",
    ),
    (
        "launch_receipt_v2",
        "work/app-server-development-v2/unattended-control-v2/launch-receipt-v2.json",
    ),
    (
        "pipeline_v1_terminal",
        "work/app-server-development-v2/unattended-pipeline-v1/pipeline-terminal.json",
    ),
    (
        "pipeline_v1_selection_result",
        "work/app-server-development-v2/unattended-pipeline-v1/development-selection/selection-result.json",
    ),
    (
        "pipeline_v1_failed_ab_sidecar",
        "work/app-server-development-v2/unattended-pipeline-v1/development-selection/calibration/judge/sidecars/ab.json",
    ),
    (
        "pipeline_v1_development_marker",
        "work/app-server-development-v2/unattended-pipeline-v1/phase-markers/03_development_selection.json",
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


def build_runtime_lock_v3(
    *, repo_root: Path, manifest_path: Path, reuse_contract_path: Path
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    manifest = manifest_path.expanduser().resolve()
    reuse_path = reuse_contract_path.expanduser().resolve()
    contract = verify_v2_reuse_contract(reuse_path)
    expected_reuse = (
        root / "work/app-server-development-v2/unattended-pipeline-v2/reuse-contract-v1.json"
    ).resolve()
    if reuse_path != expected_reuse or contract.get("target_pipeline_root") != str(
        expected_reuse.parent
    ):
        raise ValueError("runtime lock v3 requires the canonical pipeline-v2 reuse contract")
    if manifest.exists():
        verify_runtime_lock(repo_root=root, manifest_path=manifest)
        value = json.loads(manifest.read_text(encoding="utf-8"))
        if value.get("schema_version") != SHARDED_RUNTIME_LOCK_VERSION:
            raise ValueError("existing runtime lock is not v3")
        return value
    files = [_record(root, relative) for relative in CURRENT_RUNTIME_FILES]
    superseded = [
        {"label": label, **_record(root, relative)}
        for label, relative in SUPERSEDED_ARTIFACTS
    ]
    payload = {
        "schema_version": SHARDED_RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "purpose": (
            "Supersede runtime-lock v2 with a version-isolated sharded calibration and "
            "selection pipeline while preserving pipeline-v1 as immutable ITT evidence."
        ),
        "supersedes": {
            "schema_version": "pif_app_server_unattended_runtime_lock_supersession_v2",
            "prior_runtime_lock_version": SUPERSEDING_RUNTIME_LOCK_VERSION,
            "pipeline_v1_status": "blocked",
            "pipeline_v1_incident_classification": (
                "infrastructure_or_judge_attempt_failed"
            ),
            "pipeline_v1_replay_allowed": False,
            "artifacts": superseded,
        },
        "policy": {
            "managed_chatgpt_auth_only": True,
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
            "codex_exec_semantic_calls_allowed": False,
            "production_mutation_allowed": False,
            "maximum_primary_used_percent": 20,
            "per_turn_capacity_checkpoint_required": True,
            "retry_count": 0,
            "pipeline_v1_replay_allowed": False,
            "batch_5_same_thread_rerun_allowed": False,
            "failed_v1_ab_retry_allowed": False,
            "aggregate_requires_all_ab_ba_shards": True,
            "verify_before_pipeline": True,
        },
        "files": files,
    }
    write_text_atomic(
        manifest,
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )
    verify_runtime_lock(repo_root=root, manifest_path=manifest)
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build superseding app-server runtime lock v3")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--reuse-contract", required=True)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    payload = build_runtime_lock_v3(
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
