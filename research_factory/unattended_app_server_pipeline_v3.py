from __future__ import annotations

"""Superseding pipeline after the immutable v2 schema-envelope incident."""

import argparse
import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Optional, Sequence

from .app_server_v2_reuse import (
    PIPELINE_V3_REUSE_CONTRACT_VERSION,
    build_v3_reuse_contract,
    verify_v3_reuse_contract,
)
from .unattended_app_server_pipeline import (
    HoldoutExecutionAdapter,
    HoldoutFreezeAdapter,
    HoldoutJudgeGateAdapter,
    HoldoutReferenceStratificationAdapter,
    PhaseDefinition,
    PhaseResult,
    PipelineContext,
    PipelineError,
    PipelinePaths,
    PipelineStopped,
    ProspectiveEpochAdapter,
    UnattendedAppServerPipeline,
    _artifact_ref,
)
from .unattended_app_server_pipeline_v2 import ShardedDevelopmentSelectionAdapter


PIPELINE_V3_VERSION = "pif_app_server_sharded_selection_pipeline_v3"
PIPELINE_V3_PHASE_VERSION = "pif_app_server_sharded_selection_pipeline_phase_v3"
PIPELINE_V3_TERMINAL_VERSION = "pif_app_server_sharded_selection_pipeline_terminal_v3"


class PriorEvidenceReuseAdapter:
    def run(self, context: PipelineContext) -> PhaseResult:
        contract_path = context.paths.pipeline_root / "reuse-contract-v2.json"
        if contract_path.exists():
            contract = verify_v3_reuse_contract(contract_path)
        else:
            contract = build_v3_reuse_contract(
                repo_root=context.paths.repo_root,
                output_path=contract_path,
            )
        policy = contract.get("policy") or {}
        if (
            contract.get("schema_version") != PIPELINE_V3_REUSE_CONTRACT_VERSION
            or Path(str(contract.get("target_pipeline_root"))).resolve()
            != context.paths.pipeline_root
            or policy.get("pipeline_v1_replay_allowed") is not False
            or policy.get("pipeline_v2_replay_allowed") is not False
            or policy.get("failed_v2_shard_retry_allowed") is not False
            or policy.get("extraction_model_calls_allowed") is not False
        ):
            raise PipelineError("pipeline-v3 prior-evidence contract is unsafe")
        return PhaseResult(
            status="succeeded",
            artifacts=(
                _artifact_ref(
                    contract_path,
                    expected_schema=PIPELINE_V3_REUSE_CONTRACT_VERSION,
                ),
            ),
            metrics={
                "pipeline_v1_replay_allowed": False,
                "pipeline_v2_replay_allowed": False,
                "failed_v2_shard_retry_allowed": False,
                "extraction_model_calls_performed": 0,
                "clean_arm_count": len(contract["clean_arms"]),
                "prior_failed_shard_usage_status": contract[
                    "v2_failed_sharded_calibration_incident"
                ]["usage_status"],
            },
        )


class PipelineV3SelectionAdapter(ShardedDevelopmentSelectionAdapter):
    REUSE_CONTRACT_NAME = "reuse-contract-v2.json"


def default_v3_paths(
    *, repo_root: Path, pipeline_root: Optional[Path] = None
) -> PipelinePaths:
    repo = repo_root.expanduser().resolve()
    root = (
        pipeline_root.expanduser().resolve()
        if pipeline_root is not None
        else repo / "work/app-server-development-v2/unattended-pipeline-v3"
    )
    forbidden = (
        repo / "work/app-server-development-v2/unattended-pipeline-v1",
        repo / "work/app-server-development-v2/unattended-pipeline-v2",
    )
    for source in forbidden:
        if root == source or str(root).startswith(str(source) + "/"):
            raise PipelineError("pipeline-v3 root cannot overlap an immutable predecessor")
    return PipelinePaths(
        repo_root=repo,
        pipeline_root=root,
        run_spec=repo / "work/app-server-development-v2/run-spec-v2.json",
        matrix_root=repo / "work/app-server-development-v2/matrix-v1",
        context_usage_recovery=(
            repo
            / "work/windowed-acceptance-v1/paired-run-v2/evaluator-v2/context-usage-recovery-report.json"
        ),
        development_manifest=repo / "work/app-server-development-v2/manifest.json",
        provenance_root=(
            repo / "work/app-server-development-v2/unattended-pipeline-v1/provenance"
        ),
        development_selection_root=root / "development-selection-sharded-v2",
        frozen_winner=root / "development-selection-sharded-v2/selection-result.json",
        holdout_root=root / "holdout-v1",
        holdout_stratification_root=root / "holdout-stratification-v1",
        holdout_execution_root=root / "holdout-execution-v1",
        holdout_judge_root=root / "holdout-judge-v1",
        prospective_root=root / "prospective-epoch-v1",
    )


def default_v3_phases() -> list[PhaseDefinition]:
    return [
        PhaseDefinition("01_reuse_prior_evidence", PriorEvidenceReuseAdapter(), True),
        PhaseDefinition(
            "02_sharded_development_selection", PipelineV3SelectionAdapter(), True
        ),
        PhaseDefinition("03_holdout_freeze", HoldoutFreezeAdapter(), True),
        PhaseDefinition(
            "04_holdout_reference_stratification",
            HoldoutReferenceStratificationAdapter(),
            True,
        ),
        PhaseDefinition("05_holdout_execution", HoldoutExecutionAdapter(), True),
        PhaseDefinition("06_holdout_shared_judge_gate", HoldoutJudgeGateAdapter(), True),
        PhaseDefinition("07_prospective_epoch", ProspectiveEpochAdapter(), True),
    ]


def build_v3_pipeline(paths: PipelinePaths) -> UnattendedAppServerPipeline:
    return UnattendedAppServerPipeline(
        paths=paths,
        phases=default_v3_phases(),
        state_schema_version=PIPELINE_V3_VERSION,
        phase_marker_schema_version=PIPELINE_V3_PHASE_VERSION,
        terminal_schema_version=PIPELINE_V3_TERMINAL_VERSION,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the superseding schema-compatible sharded app-server pipeline"
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--pipeline-root")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    paths = default_v3_paths(
        repo_root=Path(args.repo_root),
        pipeline_root=Path(args.pipeline_root) if args.pipeline_root else None,
    )
    pipeline = build_v3_pipeline(paths)
    try:
        report = pipeline.run()
    except PipelineStopped:
        print(json.dumps({"ok": False, "status": "stopped"}, sort_keys=True))
        return 130
    except (PipelineError, OSError, sqlite3.Error, subprocess.SubprocessError):
        print(
            json.dumps({"ok": False, "status": "failed_closed"}, sort_keys=True),
            file=sys.stderr,
        )
        return 1
    status = str(report.get("status"))
    print(
        json.dumps(
            {
                "ok": status == "completed",
                "status": status,
                "terminal_phase": report.get("terminal_phase"),
                "reason_code": report.get("reason_code"),
                "terminal_report": str(pipeline.terminal_report_path),
                "production_mutation_performed": False,
            },
            sort_keys=True,
        )
    )
    return 0 if status in {"completed", "waiting_for_future_data"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
