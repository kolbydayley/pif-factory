from __future__ import annotations

"""Version-isolated unattended pipeline for sharded development calibration."""

import argparse
import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Optional, Sequence

from .app_server_dev_selection import DEV_SELECTION_VERSION
from .app_server_holdout import FROZEN_WINNER_VERSION, load_frozen_winner
from .app_server_v2_reuse import (
    REUSE_CONTRACT_VERSION,
    build_v2_reuse_contract,
    verify_selection_reuse_contract,
    verify_v2_reuse_contract,
)
from .unattended_app_server_pipeline import (
    ArtifactRef,
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
    _read_json,
    _sha256_file,
)


PIPELINE_V2_VERSION = "pif_app_server_sharded_selection_pipeline_v2"
PIPELINE_V2_PHASE_VERSION = "pif_app_server_sharded_selection_pipeline_phase_v2"
PIPELINE_V2_TERMINAL_VERSION = "pif_app_server_sharded_selection_pipeline_terminal_v2"


class V1ReuseAdapter:
    def run(self, context: PipelineContext) -> PhaseResult:
        contract_path = context.paths.pipeline_root / "reuse-contract-v1.json"
        if contract_path.exists():
            contract = verify_v2_reuse_contract(contract_path)
        else:
            contract = build_v2_reuse_contract(
                repo_root=context.paths.repo_root,
                output_path=contract_path,
            )
        if (
            contract.get("schema_version") != REUSE_CONTRACT_VERSION
            or Path(str(contract.get("target_pipeline_root"))).resolve()
            != context.paths.pipeline_root
            or (contract.get("policy") or {}).get("pipeline_v1_replay_allowed") is not False
            or (contract.get("policy") or {}).get("extraction_model_calls_allowed") is not False
            or (contract.get("v1_failed_calibration_incident") or {}).get(
                "classification"
            )
            != "infrastructure_or_judge_attempt_failed"
        ):
            raise PipelineError("pipeline-v2 reuse contract is not safe to adopt")
        return PhaseResult(
            status="succeeded",
            artifacts=(
                _artifact_ref(contract_path, expected_schema=REUSE_CONTRACT_VERSION),
            ),
            metrics={
                "source_pipeline_v1_replay_allowed": False,
                "extraction_model_calls_performed": 0,
                "clean_arm_count": len(contract["clean_arms"]),
                "failed_v1_ab_usage_status": contract[
                    "v1_failed_calibration_incident"
                ]["usage_status"],
                "failed_v1_ba_status": contract["v1_failed_calibration_incident"][
                    "ba_status"
                ],
            },
        )


class ShardedDevelopmentSelectionAdapter:
    MODULE = "research_factory.app_server_sharded_selection"
    REUSE_CONTRACT_NAME = "reuse-contract-v1.json"

    def run(self, context: PipelineContext) -> PhaseResult:
        contract_path = context.paths.pipeline_root / self.REUSE_CONTRACT_NAME
        verify_selection_reuse_contract(contract_path)
        output_dir = context.paths.development_selection_root
        output_path = context.paths.frozen_winner
        if not output_path.exists():
            return_code = context.run_module(
                self.MODULE,
                [
                    "--reuse-contract",
                    str(contract_path),
                    "--output-dir",
                    str(output_dir),
                    "--selection-output",
                    str(output_path),
                ],
            )
            if return_code != 0 and not output_path.exists():
                raise PipelineError("sharded development selection left no terminal artifact")
        artifact = _artifact_ref(output_path)
        payload = _read_json(output_path, purpose="sharded development selection result")
        if payload.get("production_changed") is not False:
            raise PipelineError("sharded selection did not preserve production")
        if payload.get("selection_status") == "frozen_winner":
            try:
                validated = load_frozen_winner(output_path)["payload"]
            except ValueError as exc:
                raise PipelineError("sharded selection winner is invalid") from exc
            if (
                validated.get("schema_version") == FROZEN_WINNER_VERSION
                and validated.get("winner_frozen") is True
                and (validated.get("gates") or {}).get("quality_noninferior") is True
                and (validated.get("gates") or {}).get(
                    "production_amortized_total_token_ratio_lte_0_28"
                )
                is True
                and validated.get("holdout_preparation_authorized") is True
                and validated.get("holdout_model_calls_authorized") is False
            ):
                return PhaseResult(status="succeeded", artifacts=(artifact,))
            raise PipelineError("sharded selection winner authorization is inconsistent")
        if (
            payload.get("schema_version") == DEV_SELECTION_VERSION
            and payload.get("winner_frozen") is False
            and payload.get("holdout_preparation_authorized") is False
            and payload.get("holdout_model_calls_authorized") is False
        ):
            classification = payload.get("terminal_classification")
            allowed = {
                "infrastructure_or_judge_attempt_failed",
                "judge_calibration_gate_not_passed",
                "development_quality_or_cost_gate_not_passed",
                "preflight_contract_failed",
            }
            if classification not in allowed:
                raise PipelineError("sharded selection has no accurate terminal classification")
            return PhaseResult(
                status="blocked",
                artifacts=(artifact,),
                reason_code=str(classification),
            )
        raise PipelineError("sharded development selection terminal schema is unsupported")


def default_v2_paths(
    *, repo_root: Path, pipeline_root: Optional[Path] = None
) -> PipelinePaths:
    repo = repo_root.expanduser().resolve()
    root = (
        pipeline_root.expanduser().resolve()
        if pipeline_root is not None
        else repo / "work/app-server-development-v2/unattended-pipeline-v2"
    )
    v1_root = repo / "work/app-server-development-v2/unattended-pipeline-v1"
    if root == v1_root or str(root).startswith(str(v1_root) + "/"):
        raise PipelineError("pipeline-v2 root cannot overlap immutable pipeline-v1")
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
        provenance_root=v1_root / "provenance",
        development_selection_root=root / "development-selection-sharded-v1",
        frozen_winner=root / "development-selection-sharded-v1/selection-result.json",
        holdout_root=root / "holdout-v1",
        holdout_stratification_root=root / "holdout-stratification-v1",
        holdout_execution_root=root / "holdout-execution-v1",
        holdout_judge_root=root / "holdout-judge-v1",
        prospective_root=root / "prospective-epoch-v1",
    )


def default_v2_phases() -> list[PhaseDefinition]:
    return [
        PhaseDefinition("01_reuse_v1_evidence", V1ReuseAdapter(), True),
        PhaseDefinition(
            "02_sharded_development_selection",
            ShardedDevelopmentSelectionAdapter(),
            True,
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


def build_v2_pipeline(paths: PipelinePaths) -> UnattendedAppServerPipeline:
    return UnattendedAppServerPipeline(
        paths=paths,
        phases=default_v2_phases(),
        state_schema_version=PIPELINE_V2_VERSION,
        phase_marker_schema_version=PIPELINE_V2_PHASE_VERSION,
        terminal_schema_version=PIPELINE_V2_TERMINAL_VERSION,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the version-isolated sharded app-server evaluation pipeline"
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--pipeline-root")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    paths = default_v2_paths(
        repo_root=Path(args.repo_root),
        pipeline_root=Path(args.pipeline_root) if args.pipeline_root else None,
    )
    pipeline = build_v2_pipeline(paths)
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
