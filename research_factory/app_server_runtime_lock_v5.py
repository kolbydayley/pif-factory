from __future__ import annotations

"""Build runtime-lock v5 for the pipeline-v4 overload recovery."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Optional, Sequence

from .app_server_runtime_lock import (
    OVERLOAD_RECOVERY_RUNTIME_LOCK_VERSION,
    SCHEMA_COMPAT_RUNTIME_LOCK_VERSION,
    verify_runtime_lock,
)
from .app_server_v2_reuse import verify_v4_reuse_contract
from .util import now_iso, write_text_atomic


CURRENT_RUNTIME_FILES = (
    "automation/resume-app-server-evaluation-v6.sh",
    "research_factory/app_server_runtime_lock.py",
    "research_factory/app_server_runtime_lock_v5.py",
    "research_factory/app_server_recovery_readiness.py",
    "research_factory/app_server_capacity.py",
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
    "research_factory/unattended_app_server_pipeline.py",
    "research_factory/unattended_app_server_pipeline_v2.py",
    "research_factory/unattended_app_server_pipeline_v4.py",
    "label_packs/ai_discourse_v3_1/prompt.md",
    "label_packs/ai_discourse_v3_1/schema.json",
    "label_packs/ai_discourse_v3_1/codebook.md",
    "research_factory/prompt_guidelines/windowed_event_core_v1.json",
    "research_factory/evaluation/judge_calibration_v2.json",
    "research_factory/evaluation/windowed_acceptance_v2.json",
    "work/app-server-development-v2/run-spec-v2.json",
    "work/app-server-development-v2/manifest.json",
    "work/app-server-development-v2/unattended-pipeline-v4/reuse-contract-v3.json",
    "work/windowed-acceptance-v1/paired-run-v2/evaluator-v2/context-usage-recovery-report.json",
)

SUPERSEDED_ARTIFACTS = (
    ("runtime_lock_v4", "work/app-server-development-v2/unattended-runtime-lock-v4.json"),
    (
        "launch_receipt_v4",
        "work/app-server-development-v2/unattended-control-v4/launch-receipt-v4.json",
    ),
    (
        "pipeline_v3_terminal",
        "work/app-server-development-v2/unattended-pipeline-v3/pipeline-terminal.json",
    ),
    (
        "pipeline_v3_calibration_report",
        "work/app-server-development-v2/unattended-pipeline-v3/development-selection-sharded-v2/calibration/report.json",
    ),
    (
        "pipeline_v3_failed_shard",
        "work/app-server-development-v2/unattended-pipeline-v3/development-selection-sharded-v2/calibration/shards/calibration-shard-002/failure.json",
    ),
    (
        "pipeline_v3_failed_ba_sidecar",
        "work/app-server-development-v2/unattended-pipeline-v3/development-selection-sharded-v2/calibration/shards/calibration-shard-002/judge/sidecars/ba.json",
    ),
    (
        "pipeline_v3_failed_ba_capacity",
        "work/app-server-development-v2/unattended-pipeline-v3/development-selection-sharded-v2/calibration/shards/calibration-shard-002/judge/sidecars/ba.capacity.json",
    ),
    (
        "pipeline_v3_reuse_contract",
        "work/app-server-development-v2/unattended-pipeline-v3/reuse-contract-v2.json",
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


def build_runtime_lock_v5(
    *, repo_root: Path, manifest_path: Path, reuse_contract_path: Path
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    manifest = manifest_path.expanduser().resolve()
    reuse_path = reuse_contract_path.expanduser().resolve()
    contract = verify_v4_reuse_contract(reuse_path)
    expected = (
        root / "work/app-server-development-v2/unattended-pipeline-v4/reuse-contract-v3.json"
    ).resolve()
    if reuse_path != expected or contract.get("target_pipeline_root") != str(expected.parent):
        raise ValueError("runtime lock v5 requires the canonical pipeline-v4 contract")
    if manifest.exists():
        report = verify_runtime_lock(repo_root=root, manifest_path=manifest)
        value = json.loads(manifest.read_text(encoding="utf-8"))
        if report.get("schema_version") != OVERLOAD_RECOVERY_RUNTIME_LOCK_VERSION:
            raise ValueError("existing runtime lock is not v5")
        return value
    payload = {
        "schema_version": OVERLOAD_RECOVERY_RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "purpose": (
            "Supersede immutable pipeline-v3 after external serverOverloaded, then run "
            "one full fresh six-case-sharded calibration behind a delayed readiness gate."
        ),
        "supersedes": {
            "schema_version": "pif_app_server_unattended_runtime_lock_supersession_v4",
            "prior_runtime_lock_version": SCHEMA_COMPAT_RUNTIME_LOCK_VERSION,
            "pipeline_v3_status": "blocked",
            "pipeline_v3_incident_classification": (
                "infrastructure_or_judge_attempt_failed"
            ),
            "provider_error_code": "serverOverloaded",
            "pipeline_v3_replay_allowed": False,
            "failed_v3_shard_retry_allowed": False,
            "pipeline_v3_partial_calibration_scoring_allowed": False,
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
            "provider_overload_cooldown_seconds": 1800,
            "launch_maximum_primary_used_percent": 5,
            "launch_required_consecutive_clear_probes": 2,
            "launch_minimum_clear_probe_interval_seconds": 60,
            "semantic_turn_maximum_primary_used_percent": 20,
            "per_turn_capacity_checkpoint_required": True,
            "retry_count_per_ab_or_ba_turn": 0,
            "case_count_per_calibration_shard": 6,
            "required_calibration_shard_count": 11,
            "required_calibration_turn_count": 22,
            "pipeline_v1_replay_allowed": False,
            "pipeline_v2_replay_allowed": False,
            "pipeline_v3_replay_allowed": False,
            "pipeline_v3_partial_calibration_scoring_allowed": False,
            "full_fresh_v4_calibration_required": True,
            "aggregate_requires_all_ab_ba_shards": True,
            "corrected_structured_output_schema_required": True,
            "verify_before_pipeline": True,
        },
        "files": [_record(root, relative) for relative in CURRENT_RUNTIME_FILES],
    }
    write_text_atomic(
        manifest,
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )
    verify_runtime_lock(repo_root=root, manifest_path=manifest)
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build app-server runtime lock v5")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--reuse-contract", required=True)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    payload = build_runtime_lock_v5(
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
