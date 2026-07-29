from __future__ import annotations

"""Pipeline-v4 recovery after the immutable v3 provider-overload incident."""

import argparse
import asyncio
import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Optional, Sequence

from .app_server_recovery_readiness import (
    READINESS_FAILURE_VERSION,
    READINESS_TERMINAL_VERSION,
    RecoveryReadinessError,
    verify_recovery_readiness,
    wait_for_recovery_readiness,
)
from .app_server_v2_reuse import (
    PIPELINE_V4_REUSE_CONTRACT_VERSION,
    build_v4_reuse_contract,
    verify_v4_reuse_contract,
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


PIPELINE_V4_VERSION = "pif_app_server_sharded_selection_pipeline_v4"
PIPELINE_V4_PHASE_VERSION = "pif_app_server_sharded_selection_pipeline_phase_v4"
PIPELINE_V4_TERMINAL_VERSION = "pif_app_server_sharded_selection_pipeline_terminal_v4"


class PredecessorEvidenceAdapter:
    def run(self, context: PipelineContext) -> PhaseResult:
        contract_path = context.paths.pipeline_root / "reuse-contract-v3.json"
        if contract_path.exists():
            contract = verify_v4_reuse_contract(contract_path)
        else:
            contract = build_v4_reuse_contract(
                repo_root=context.paths.repo_root,
                output_path=contract_path,
            )
        policy = contract.get("policy") or {}
        incident = contract.get("v3_failed_sharded_calibration_incident") or {}
        if (
            contract.get("schema_version") != PIPELINE_V4_REUSE_CONTRACT_VERSION
            or Path(str(contract.get("target_pipeline_root"))).resolve()
            != context.paths.pipeline_root
            or policy.get("pipeline_v1_replay_allowed") is not False
            or policy.get("pipeline_v2_replay_allowed") is not False
            or policy.get("pipeline_v3_replay_allowed") is not False
            or policy.get("pipeline_v3_partial_calibration_scoring_allowed")
            is not False
            or policy.get("full_fresh_v4_calibration_required") is not True
            or policy.get("extraction_model_calls_allowed") is not False
            or incident.get("provider_error_code") != "serverOverloaded"
            or incident.get("usage_status") != "unknown"
            or incident.get("retry_allowed") is not False
        ):
            raise PipelineError("pipeline-v4 predecessor evidence contract is unsafe")
        return PhaseResult(
            status="succeeded",
            artifacts=(
                _artifact_ref(
                    contract_path,
                    expected_schema=PIPELINE_V4_REUSE_CONTRACT_VERSION,
                ),
            ),
            metrics={
                "pipeline_v1_replay_allowed": False,
                "pipeline_v2_replay_allowed": False,
                "pipeline_v3_replay_allowed": False,
                "pipeline_v3_partial_calibration_scoring_allowed": False,
                "full_fresh_v4_calibration_required": True,
                "extraction_model_calls_performed": 0,
                "clean_arm_count": len(contract["clean_arms"]),
                "v3_completed_measured_turns": incident[
                    "completed_measured_turns"
                ],
                "v3_failed_unknown_usage_turns": incident[
                    "failed_unknown_usage_turns"
                ],
                "v3_whole_version_usage_status": incident["usage_status"],
            },
        )


class ProviderReadinessAdapter:
    def run(self, context: PipelineContext) -> PhaseResult:
        contract_path = context.paths.pipeline_root / "reuse-contract-v3.json"
        readiness_root = context.paths.pipeline_root / "provider-readiness-v1"
        try:
            terminal = asyncio.run(
                wait_for_recovery_readiness(
                    reuse_contract_path=contract_path,
                    output_dir=readiness_root,
                    stop_check=context.pipeline.check_stop,
                )
            )
        except RecoveryReadinessError:
            failure_path = readiness_root / "readiness-failure.json"
            if not failure_path.is_file():
                raise PipelineError("provider readiness failed without a terminal artifact")
            return PhaseResult(
                status="blocked",
                artifacts=(
                    _artifact_ref(
                        failure_path, expected_schema=READINESS_FAILURE_VERSION
                    ),
                ),
                reason_code="infrastructure_or_auth_readiness_failed",
                metrics={
                    "semantic_turns_started": 0,
                    "production_mutation_performed": False,
                },
            )
        verified = verify_recovery_readiness(
            reuse_contract_path=contract_path, output_dir=readiness_root
        )
        if terminal != verified:
            raise PipelineError("provider readiness terminal changed after verification")
        terminal_path = readiness_root / "readiness-terminal.json"
        return PhaseResult(
            status="succeeded",
            artifacts=(
                _artifact_ref(
                    terminal_path, expected_schema=READINESS_TERMINAL_VERSION
                ),
            ),
            metrics={
                "managed_chatgpt_auth_verified": True,
                "launch_primary_used_percent": terminal[
                    "final_primary_used_percent"
                ],
                "launch_maximum_primary_used_percent": terminal[
                    "launch_maximum_primary_used_percent"
                ],
                "semantic_turn_maximum_primary_used_percent": terminal[
                    "semantic_turn_maximum_primary_used_percent"
                ],
                "semantic_turns_started": 0,
            },
        )


class PipelineV4SelectionAdapter(ShardedDevelopmentSelectionAdapter):
    REUSE_CONTRACT_NAME = "reuse-contract-v3.json"


def default_v4_paths(
    *, repo_root: Path, pipeline_root: Optional[Path] = None
) -> PipelinePaths:
    repo = repo_root.expanduser().resolve()
    root = (
        pipeline_root.expanduser().resolve()
        if pipeline_root is not None
        else repo / "work/app-server-development-v2/unattended-pipeline-v4"
    )
    forbidden = tuple(
        repo / "work/app-server-development-v2" / ("unattended-pipeline-v%d" % version)
        for version in (1, 2, 3)
    )
    for source in forbidden:
        if root == source or str(root).startswith(str(source) + "/"):
            raise PipelineError("pipeline-v4 root cannot overlap an immutable predecessor")
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
        development_selection_root=root / "development-selection-sharded-v3",
        frozen_winner=root / "development-selection-sharded-v3/selection-result.json",
        holdout_root=root / "holdout-v1",
        holdout_stratification_root=root / "holdout-stratification-v1",
        holdout_execution_root=root / "holdout-execution-v1",
        holdout_judge_root=root / "holdout-judge-v1",
        prospective_root=root / "prospective-epoch-v1",
    )


def default_v4_phases() -> list[PhaseDefinition]:
    return [
        PhaseDefinition("01_freeze_predecessor_evidence", PredecessorEvidenceAdapter(), True),
        PhaseDefinition("02_provider_readiness", ProviderReadinessAdapter(), True),
        PhaseDefinition(
            "03_full_fresh_sharded_development_selection",
            PipelineV4SelectionAdapter(),
            True,
        ),
        PhaseDefinition("04_holdout_freeze", HoldoutFreezeAdapter(), True),
        PhaseDefinition(
            "05_holdout_reference_stratification",
            HoldoutReferenceStratificationAdapter(),
            True,
        ),
        PhaseDefinition("06_holdout_execution", HoldoutExecutionAdapter(), True),
        PhaseDefinition("07_holdout_shared_judge_gate", HoldoutJudgeGateAdapter(), True),
        PhaseDefinition("08_prospective_epoch", ProspectiveEpochAdapter(), True),
    ]


def build_v4_pipeline(paths: PipelinePaths) -> UnattendedAppServerPipeline:
    return UnattendedAppServerPipeline(
        paths=paths,
        phases=default_v4_phases(),
        state_schema_version=PIPELINE_V4_VERSION,
        phase_marker_schema_version=PIPELINE_V4_PHASE_VERSION,
        terminal_schema_version=PIPELINE_V4_TERMINAL_VERSION,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run pipeline-v4 after the immutable provider-overload incident"
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--pipeline-root")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    paths = default_v4_paths(
        repo_root=Path(args.repo_root),
        pipeline_root=Path(args.pipeline_root) if args.pipeline_root else None,
    )
    pipeline = build_v4_pipeline(paths)
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
