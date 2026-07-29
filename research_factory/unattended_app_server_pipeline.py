from __future__ import annotations

import argparse
import asyncio
import fcntl
import hashlib
import importlib.util
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional, Protocol, Sequence

from .util import now_iso, write_text_atomic


PIPELINE_VERSION = "pif_unattended_app_server_pipeline_v1"
PHASE_MARKER_VERSION = "pif_unattended_app_server_pipeline_phase_v1"
TERMINAL_REPORT_VERSION = "pif_unattended_app_server_pipeline_terminal_v1"

_TERMINAL_PHASE_STATUSES = {"succeeded", "blocked", "waiting_for_future_data"}
_SUCCESS_PHASE_STATUS = "succeeded"


class PipelineError(RuntimeError):
    """The durable evaluation pipeline failed closed."""


class PipelineLockUnavailable(PipelineError):
    """A second pipeline process attempted to acquire the lifetime lock."""


class PipelineStopped(PipelineError):
    """The stop sentinel or a process signal stopped the pipeline."""


@dataclass(frozen=True)
class ArtifactRef:
    path: Path
    sha256: str
    schema_version: Optional[str] = None

    def marker_record(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "sha256": self.sha256,
            "schema_version": self.schema_version,
        }


@dataclass(frozen=True)
class PhaseResult:
    status: str
    artifacts: tuple[ArtifactRef, ...] = ()
    reason_code: Optional[str] = None
    metrics: dict[str, Any] = field(default_factory=dict)


class PhaseAdapter(Protocol):
    def run(self, context: "PipelineContext") -> PhaseResult:
        ...


@dataclass(frozen=True)
class PhaseDefinition:
    name: str
    adapter: PhaseAdapter
    resumable_after_interruption: bool


@dataclass(frozen=True)
class PipelinePaths:
    repo_root: Path
    pipeline_root: Path
    run_spec: Path
    matrix_root: Path
    context_usage_recovery: Path
    development_manifest: Path
    provenance_root: Path
    development_selection_root: Path
    frozen_winner: Path
    holdout_root: Path
    holdout_stratification_root: Path
    holdout_execution_root: Path
    holdout_judge_root: Path
    prospective_root: Path


class PipelineContext:
    def __init__(self, pipeline: "UnattendedAppServerPipeline"):
        self.pipeline = pipeline
        self.paths = pipeline.paths

    def heartbeat(self) -> None:
        self.pipeline.heartbeat()

    def check_stop(self) -> None:
        self.pipeline.check_stop()

    def run_module(self, module: str, arguments: Sequence[str]) -> int:
        return self.pipeline.run_module(module, arguments)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()


def _read_json(path: Path, *, purpose: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PipelineError(f"{purpose} is not valid local JSON") from exc
    if not isinstance(payload, dict):
        raise PipelineError(f"{purpose} is not a JSON object")
    return payload


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    write_text_atomic(
        path,
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )


def _write_immutable_json(path: Path, payload: dict[str, Any]) -> None:
    rendered = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != rendered:
            raise PipelineError("immutable pipeline artifact already exists with different content")
        return
    write_text_atomic(path, rendered)


def _append_journal(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = (
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        if os.write(descriptor, line) != len(line):
            raise PipelineError("short pipeline journal write")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _artifact_ref(
    path: Path,
    *,
    expected_schema: Optional[str] = None,
    required_fields: Optional[dict[str, Any]] = None,
) -> ArtifactRef:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise PipelineError("required immutable terminal artifact is missing")
    payload = _read_json(resolved, purpose="terminal artifact")
    schema = payload.get("schema_version")
    if expected_schema is not None and schema != expected_schema:
        raise PipelineError("terminal artifact schema does not match its phase contract")
    for key, expected in (required_fields or {}).items():
        if payload.get(key) != expected:
            raise PipelineError(f"terminal artifact did not satisfy required field: {key}")
    return ArtifactRef(
        path=resolved,
        sha256=_sha256_file(resolved),
        schema_version=str(schema) if isinstance(schema, str) else None,
    )


class _LifetimeLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle: Optional[Any] = None

    def __enter__(self) -> "_LifetimeLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.handle.close()
            self.handle = None
            raise PipelineLockUnavailable("another unattended pipeline owns the lifetime lock") from exc
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(f"pid={os.getpid()} acquired_at={now_iso()}\n")
        self.handle.flush()
        os.fsync(self.handle.fileno())
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # noqa: ANN001
        if self.handle is None:
            return
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


class MatrixAdapter:
    def run(self, context: PipelineContext) -> PhaseResult:
        from .unattended_app_server_eval import (
            LockUnavailable,
            UnattendedAppServerMatrixSupervisor,
        )

        context.check_stop()
        recovered_path = context.paths.matrix_root / "recovered-matrix-report.json"
        if recovered_path.is_file():
            recovered = _read_json(recovered_path, purpose="recovered matrix report")
            if (
                recovered.get("schema_version")
                != "pif_app_server_recovered_development_matrix_v1"
                or recovered.get("clean_arm_count") != 5
                or recovered.get("five_clean_arm_selection_eligible") is not True
                or recovered.get("selection_eligible") is not True
                or recovered.get("clean_six_arm_matrix_achieved") is not False
            ):
                raise PipelineError("recovered matrix is not the frozen five-clean-arm design")
            return PhaseResult(
                status="succeeded",
                artifacts=(
                    _artifact_ref(
                        recovered_path,
                        expected_schema="pif_app_server_recovered_development_matrix_v1",
                        required_fields={
                            "clean_arm_count": 5,
                            "selection_eligible": True,
                        },
                    ),
                ),
                metrics={"matrix_mode": "five_clean_plus_terminal_interrupted"},
            )
        while True:
            supervisor = UnattendedAppServerMatrixSupervisor(
                repo_root=context.paths.repo_root,
                run_spec_path=context.paths.run_spec,
                matrix_root=context.paths.matrix_root,
            )
            try:
                matrix = supervisor.run()
                break
            except LockUnavailable:
                # A separately launched instance owns the same frozen matrix.  Wait on
                # its durable state rather than starting a competing semantic process.
                state_path = context.paths.matrix_root / "supervisor" / "state.json"
                if state_path.is_file():
                    state = _read_json(state_path, purpose="matrix supervisor state")
                    if state.get("status") in {"failed", "stopped"}:
                        raise PipelineError("the active matrix supervisor terminated unsuccessfully")
                    if (
                        state.get("status") == "completed"
                        and (context.paths.matrix_root / "matrix-report.json").is_file()
                    ):
                        matrix = _read_json(
                            context.paths.matrix_root / "matrix-report.json",
                            purpose="matrix report",
                        )
                        break
                context.heartbeat()
                context.check_stop()
                time.sleep(2.0)
        if (
            matrix.get("schema_version") != "pif_app_server_core_matrix_v3"
            or matrix.get("arm_count") != 6
            or matrix.get("selection_eligible") is not False
            or matrix.get("semantic_quality_status")
            != "pending_calibrated_llm_support_and_alignment_adjudication"
        ):
            raise PipelineError("matrix terminal report is not the frozen unscored six-arm matrix")
        artifact = _artifact_ref(
            context.paths.matrix_root / "matrix-report.json",
            expected_schema="pif_app_server_core_matrix_v3",
            required_fields={"arm_count": 6, "selection_eligible": False},
        )
        return PhaseResult(status="succeeded", artifacts=(artifact,))


class ProvenanceAdapter:
    def run(self, context: PipelineContext) -> PhaseResult:
        from .app_server_eval_provenance import (
            build_instruction_provenance,
            build_reconstructed_exclusion_ledger,
            build_reference_transform_noise,
        )
        from .paths import db_path

        root = context.paths.provenance_root
        root.mkdir(parents=True, exist_ok=True)
        exclusion_path = root / "reconstructed-exclusions-v1.json"
        transform_path = root / "reference-transform-noise-v1.json"
        instruction_path = root / "instruction-provenance-v1.json"
        itt_path = (
            context.paths.matrix_root
            / "batch-5"
            / "same_thread"
            / "intent-to-treat-interruption-v1.json"
        )
        if itt_path.is_file():
            arm_reports = [
                context.paths.matrix_root / "batch-3" / "new_thread" / "report.json",
                context.paths.matrix_root / "batch-3" / "same_thread" / "report.json",
                context.paths.matrix_root / "batch-5" / "new_thread" / "report.json",
                context.paths.matrix_root / "batch-8" / "new_thread" / "report.json",
                context.paths.matrix_root / "batch-8" / "same_thread" / "report.json",
            ]
        else:
            arm_reports = [
                context.paths.matrix_root / f"batch-{batch_size}" / thread_mode / "report.json"
                for batch_size in (3, 5, 8)
                for thread_mode in ("new_thread", "same_thread")
            ]
        context.heartbeat()
        database = db_path().expanduser().resolve()
        if not database.is_file():
            raise PipelineError("production database is missing")
        connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        try:
            exclusion = build_reconstructed_exclusion_ledger(
                connection,
                work_root=context.paths.repo_root / "work",
                output_path=exclusion_path,
            )
            transform = build_reference_transform_noise(
                connection,
                manifest_path=context.paths.development_manifest,
                output_path=transform_path,
            )
        finally:
            connection.close()
        instruction = build_instruction_provenance(
            run_spec_path=context.paths.run_spec,
            arm_report_paths=arm_reports,
            output_path=instruction_path,
        )
        if exclusion.get("complete") is not True or instruction.get("complete") is not True:
            raise PipelineError("provenance terminal artifacts are incomplete")
        if transform.get("summary", {}).get("all_declared_raw_artifacts_verified") is not True:
            raise PipelineError("reference transform provenance did not verify raw artifacts")
        artifacts = (
            _artifact_ref(exclusion_path, expected_schema="pif_reconstructed_exclusion_ledger_v1"),
            _artifact_ref(transform_path, expected_schema="pif_reference_transform_noise_v1"),
            _artifact_ref(instruction_path, expected_schema="pif_app_server_eval_provenance_v1"),
        )
        return PhaseResult(status="succeeded", artifacts=artifacts)


class DevelopmentSelectionAdapter:
    MODULE = "research_factory.app_server_dev_selection"

    def run(self, context: PipelineContext) -> PhaseResult:
        from .app_server_dev_selection import DEV_SELECTION_VERSION
        from .app_server_holdout import FROZEN_WINNER_VERSION, load_frozen_winner

        if importlib.util.find_spec(self.MODULE) is None:
            return PhaseResult(status="blocked", reason_code="development_selection_adapter_absent")
        output_dir = context.paths.development_selection_root
        output_path = context.paths.frozen_winner
        if not output_path.exists():
            arguments = [
                "--manifest",
                str(context.paths.development_manifest),
                "--context-usage-recovery",
                str(context.paths.context_usage_recovery),
                "--output-dir",
                str(output_dir),
                "--run-spec",
                str(context.paths.run_spec),
                "--selection-output",
                str(output_path),
            ]
            itt_path = (
                context.paths.matrix_root
                / "batch-5"
                / "same_thread"
                / "intent-to-treat-interruption-v1.json"
            )
            arms = (
                (
                    (3, "new_thread"),
                    (3, "same_thread"),
                    (5, "new_thread"),
                    (8, "new_thread"),
                    (8, "same_thread"),
                )
                if itt_path.is_file()
                else tuple(
                    (batch_size, thread_mode)
                    for batch_size in (3, 5, 8)
                    for thread_mode in ("new_thread", "same_thread")
                )
            )
            for batch_size, thread_mode in arms:
                arguments.extend(
                    [
                        "--arm-report",
                        str(
                            context.paths.matrix_root
                            / f"batch-{batch_size}"
                            / thread_mode
                            / "report.json"
                        ),
                    ]
                )
            if itt_path.is_file():
                arguments.extend(
                    ["--interrupted-arm-provenance", str(itt_path)]
                )
            return_code = context.run_module(self.MODULE, arguments)
            if return_code != 0 and not output_path.exists():
                raise PipelineError("development selection exited without a terminal report")
        artifact = _artifact_ref(output_path)
        payload = _read_json(output_path, purpose="development selection result")
        if payload.get("production_changed") is not False:
            raise PipelineError("development selection did not prove production remained unchanged")
        if payload.get("selection_status") == "frozen_winner":
            try:
                validated = load_frozen_winner(output_path)["payload"]
            except ValueError as exc:
                raise PipelineError(
                    "development selection emitted an invalid frozen-winner artifact"
                ) from exc
            if (
                validated.get("schema_version") == FROZEN_WINNER_VERSION
                and validated.get("winner_frozen") is True
                and validated.get("gates", {}).get("quality_noninferior") is True
                and validated.get("gates", {}).get(
                    "production_amortized_total_token_ratio_lte_0_28"
                )
                is True
                and validated.get("holdout_preparation_authorized") is True
                and validated.get("holdout_model_calls_authorized") is False
            ):
                return PhaseResult(status="succeeded", artifacts=(artifact,))
            raise PipelineError(
                "development selection frozen-winner authorization is inconsistent"
            )
        if (
            payload.get("schema_version") == DEV_SELECTION_VERSION
            and payload.get("winner_frozen") is False
            and payload.get("holdout_preparation_authorized") is not True
            and payload.get("holdout_model_calls_authorized") is False
        ):
            classification = payload.get("terminal_classification")
            if classification not in {
                "infrastructure_or_judge_attempt_failed",
                "judge_calibration_gate_not_passed",
                "development_quality_or_cost_gate_not_passed",
                "preflight_contract_failed",
            }:
                classification = "development_quality_or_cost_gate_not_passed"
            return PhaseResult(
                status="blocked",
                artifacts=(artifact,),
                reason_code=str(classification),
            )
        raise PipelineError("development selection terminal report is not an accepted schema")


class HoldoutFreezeAdapter:
    def run(self, context: PipelineContext) -> PhaseResult:
        from .app_server_holdout import (
            HOLDOUT_COVENANT_VERSION,
            load_frozen_winner,
            prepare_frozen_holdout_reservoirs,
            verify_frozen_holdout,
        )
        from .paths import db_path

        load_frozen_winner(context.paths.frozen_winner)
        covenant_path = context.paths.holdout_root / "covenant.json"
        connection = sqlite3.connect(
            f"file:{db_path().expanduser().resolve()}?mode=ro", uri=True
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        try:
            if not covenant_path.exists():
                result = prepare_frozen_holdout_reservoirs(
                    connection,
                    frozen_winner_path=context.paths.frozen_winner,
                    output_dir=context.paths.holdout_root,
                    exclude_roots=[context.paths.repo_root / "work"],
                )
            else:
                verify_frozen_holdout(connection, covenant_path=covenant_path)
                covenant = _read_json(covenant_path, purpose="holdout covenant")
                result = {
                    "ok": covenant.get("holdout_model_calls_authorized") is True,
                    "selection_status": (
                        "ready"
                        if covenant.get("holdout_model_calls_authorized") is True
                        else "blocked_future_only_reservoir_undercapacity"
                    ),
                }
        finally:
            connection.close()
        artifacts = tuple(
            _artifact_ref(context.paths.holdout_root / name)
            for name in (
                "covenant.json",
                "exclusions.json",
                "paired-quality-reservoir.json",
                "terminal-position-no-signal-candidates.json",
            )
        )
        covenant = _read_json(covenant_path, purpose="holdout covenant")
        if covenant.get("schema_version") != HOLDOUT_COVENANT_VERSION:
            raise PipelineError("holdout covenant schema drift")
        if result.get("ok") is not True:
            return PhaseResult(
                status="waiting_for_future_data",
                artifacts=artifacts,
                reason_code="holdout_reservoir_undercapacity",
            )
        if covenant.get("holdout_model_calls_authorized") is not True:
            raise PipelineError("holdout preparation did not explicitly authorize model calls")
        return PhaseResult(status="succeeded", artifacts=artifacts)


class HoldoutReferenceStratificationAdapter:
    """Bind only to an explicit pre-candidate immutable stratification module."""

    MODULE = "research_factory.app_server_holdout_stratifier"

    def run(self, context: PipelineContext) -> PhaseResult:
        if importlib.util.find_spec(self.MODULE) is None:
            return PhaseResult(
                status="blocked",
                reason_code="holdout_reference_stratification_adapter_absent",
            )
        output = context.paths.holdout_stratification_root / "stratified-selection.json"
        if not output.exists():
            from .paths import db_path

            return_code = context.run_module(
                self.MODULE,
                [
                    "--database",
                    str(db_path()),
                    "--covenant",
                    str(context.paths.holdout_root / "covenant.json"),
                    "--output-dir",
                    str(context.paths.holdout_stratification_root),
                    "--execution-dir",
                    str(context.paths.holdout_execution_root),
                ],
            )
            report_path = context.paths.holdout_stratification_root / "report.json"
            if return_code != 0 and not output.exists():
                if not report_path.is_file():
                    raise PipelineError("holdout stratification exited without a terminal report")
                report = _read_json(report_path, purpose="holdout stratifier report")
                artifact = _artifact_ref(
                    report_path,
                    expected_schema="pif_app_server_holdout_stratifier_report_v1",
                )
                if (
                    report.get("status") == "underpowered_reference_strata"
                    and report.get("candidate_outputs_observed") is False
                    and report.get("baseline_outputs_observed") is False
                    and report.get("adaptive_top_up") is False
                ):
                    return PhaseResult(
                        status="waiting_for_future_data",
                        artifacts=(artifact,),
                        reason_code="holdout_reference_strata_underpowered",
                    )
                return PhaseResult(
                    status="blocked",
                    artifacts=(artifact,),
                    reason_code="holdout_reference_calls_failed_or_accounting_incomplete",
                )
        payload = _read_json(output, purpose="holdout stratification selection")
        required = {
            "schema_version": "pif_app_server_holdout_stratified_selection_v1",
            "selection_status": "frozen_stratified_selection",
            "selection_basis": "llm_reference_only_pre_candidate_pre_baseline",
            "adaptive_top_up": False,
            "candidate_outputs_observed": False,
            "baseline_outputs_observed": False,
            "selection_frozen": True,
        }
        for key, expected in required.items():
            if payload.get(key) != expected:
                raise PipelineError(f"holdout stratification did not freeze required field: {key}")
        counts = payload.get("stratum_counts")
        expected_counts = {
            "paired_quality:dense": 15,
            "paired_quality:low": 15,
            "paired_quality:medium": 15,
            "paired_quality:no_signal": 15,
            "clean_no_signal_power:no_signal": 60,
        }
        if counts != expected_counts:
            raise PipelineError("holdout stratification is not the precommitted 15x4 plus 60 design")
        return PhaseResult(status="succeeded", artifacts=(_artifact_ref(output),))


class HoldoutExecutionAdapter:
    MODULE = "research_factory.app_server_holdout_execution"

    def run(self, context: PipelineContext) -> PhaseResult:
        if importlib.util.find_spec(self.MODULE) is None:
            return PhaseResult(status="blocked", reason_code="holdout_execution_adapter_absent")
        selection = context.paths.holdout_stratification_root / "stratified-selection.json"
        if not selection.is_file():
            return PhaseResult(
                status="blocked", reason_code="holdout_stratified_selection_missing"
            )
        terminal = context.paths.holdout_execution_root / "pipeline-execution-terminal.json"
        if not terminal.exists():
            from .paths import db_path

            return_code = context.run_module(
                self.MODULE,
                [
                    "--database",
                    str(db_path()),
                    "--covenant",
                    str(context.paths.holdout_root / "covenant.json"),
                    "--winner",
                    str(context.paths.frozen_winner),
                    "--stratified-selection",
                    str(selection),
                    "--execution-dir",
                    str(context.paths.holdout_execution_root),
                    "--phase",
                    "all",
                ],
            )
            phase_contracts = (
                (
                    "A",
                    context.paths.holdout_execution_root / "phase-a-contexts" / "report.json",
                    "pif_app_server_holdout_context_phase_v1",
                ),
                (
                    "B",
                    context.paths.holdout_execution_root / "phase-b-candidate" / "report.json",
                    "pif_app_server_holdout_candidate_phase_v1",
                ),
                (
                    "C",
                    context.paths.holdout_execution_root / "phase-c-baseline" / "report.json",
                    "pif_app_server_holdout_baseline_phase_v1",
                ),
            )
            phase_records = []
            for phase_name, report_path, schema in phase_contracts:
                if not report_path.is_file():
                    phase_records.append(
                        {
                            "phase": phase_name,
                            "present": False,
                            "schema_version": schema,
                        }
                    )
                    continue
                report = _read_json(report_path, purpose=f"holdout phase {phase_name} report")
                if report.get("schema_version") != schema:
                    raise PipelineError("holdout phase report schema drift")
                if (
                    report.get("production_database_mutation") is not False
                    or report.get("production_promotion") is not False
                ):
                    raise PipelineError("holdout execution reported a prohibited production change")
                phase_records.append(
                    {
                        "phase": phase_name,
                        "present": True,
                        "schema_version": schema,
                        "report_path": str(report_path),
                        "report_sha256": _sha256_file(report_path),
                        "ok": report.get("ok") is True,
                        "execution_complete": report.get("execution_complete") is True,
                        "accounting_complete": report.get("accounting_complete") is True,
                    }
                )
            execution_ok = bool(
                return_code == 0
                and len(phase_records) == 3
                and all(
                    row.get("present")
                    and row.get("ok")
                    and row.get("execution_complete")
                    and row.get("accounting_complete")
                    for row in phase_records
                )
            )
            _write_immutable_json(
                terminal,
                {
                    "schema_version": "pif_app_server_holdout_execution_terminal_v1",
                    "execution_complete": all(
                        row.get("present") and row.get("execution_complete")
                        for row in phase_records
                    ),
                    "accounting_complete": all(
                        row.get("present") and row.get("accounting_complete")
                        for row in phase_records
                    ),
                    "ok": execution_ok,
                    "phase_reports": phase_records,
                    "subprocess_return_code": return_code,
                    "production_database_mutation": False,
                    "production_promotion": False,
                    "privacy": "phase_report_paths_hashes_counts_status_and_accounting_only",
                },
            )
        payload = _read_json(terminal, purpose="holdout execution terminal report")
        artifact = _artifact_ref(terminal)
        if (
            payload.get("ok") is True
            and payload.get("execution_complete") is True
            and payload.get("accounting_complete") is True
        ):
            return PhaseResult(status="succeeded", artifacts=(artifact,))
        return PhaseResult(
            status="blocked",
            artifacts=(artifact,),
            reason_code="holdout_execution_not_complete",
        )


class HoldoutJudgeGateAdapter:
    MODULE = "research_factory.app_server_holdout_judge"

    def run(self, context: PipelineContext) -> PhaseResult:
        if importlib.util.find_spec(self.MODULE) is None:
            return PhaseResult(
                status="blocked", reason_code="holdout_shared_judge_adapter_absent"
            )
        output = context.paths.holdout_judge_root / "holdout-gate-report.json"
        if not output.exists():
            return_code = context.run_module(
                self.MODULE,
                [
                    "--covenant",
                    str(context.paths.holdout_root / "covenant.json"),
                    "--selection",
                    str(
                        context.paths.holdout_stratification_root
                        / "stratified-selection.json"
                    ),
                    "--execution-dir",
                    str(context.paths.holdout_execution_root),
                    "--output-dir",
                    str(context.paths.holdout_judge_root),
                    "--gate-output",
                    str(output),
                ],
            )
            if return_code != 0 and not output.exists():
                raise PipelineError("holdout judge exited without a terminal gate report")
        payload = _read_json(output, purpose="holdout judge gate")
        artifact = _artifact_ref(output)
        if (
            payload.get("quality_noninferior") is True
            and payload.get("production_amortized_total_token_ratio_lte_0_28") is True
            and payload.get("gate_passed") is True
        ):
            return PhaseResult(status="succeeded", artifacts=(artifact,))
        return PhaseResult(
            status="blocked",
            artifacts=(artifact,),
            reason_code="holdout_quality_or_cost_gate_not_passed",
        )


class ProspectiveEpochAdapter:
    MODULE = "research_factory.app_server_prospective_epoch"

    def run(self, context: PipelineContext) -> PhaseResult:
        if importlib.util.find_spec(self.MODULE) is None:
            return self._truthful_waiting_fallback(context)
        output = context.paths.prospective_root / "prospective-epoch-status.json"
        if not output.exists():
            return_code = context.run_module(
                self.MODULE,
                [
                    "--holdout-gate",
                    str(context.paths.holdout_judge_root / "holdout-gate-report.json"),
                    "--output-dir",
                    str(context.paths.prospective_root),
                    "--status-output",
                    str(output),
                ],
            )
            if return_code != 0 and not output.exists():
                raise PipelineError("prospective epoch exited without a terminal status")
        payload = _read_json(output, purpose="prospective epoch status")
        artifact = _artifact_ref(output)
        status = payload.get("status")
        if status == "waiting_for_future_data":
            return PhaseResult(
                status="waiting_for_future_data",
                artifacts=(artifact,),
                reason_code="prospective_epoch_has_no_eligible_future_data",
            )
        if status == "completed" and payload.get("gate_passed") is True:
            return PhaseResult(status="succeeded", artifacts=(artifact,))
        return PhaseResult(
            status="blocked",
            artifacts=(artifact,),
            reason_code="prospective_epoch_terminal_status_not_accepted",
        )

    def _truthful_waiting_fallback(self, context: PipelineContext) -> PhaseResult:
        from .app_server_holdout import _parse_timestamp
        from .paths import db_path

        output = context.paths.prospective_root / "prospective-epoch-status.json"
        if output.exists():
            payload = _read_json(output, purpose="prospective epoch fallback status")
            artifact = _artifact_ref(
                output, expected_schema="pif_app_server_prospective_epoch_status_v1"
            )
            status = payload.get("status")
            return PhaseResult(
                status=("waiting_for_future_data" if status == "waiting_for_future_data" else "blocked"),
                artifacts=(artifact,),
                reason_code=(
                    "prospective_epoch_has_no_eligible_future_data"
                    if status == "waiting_for_future_data"
                    else "prospective_execution_adapter_absent_with_eligible_data"
                ),
            )
        covenant_path = context.paths.holdout_root / "covenant.json"
        covenant = _read_json(covenant_path, purpose="holdout covenant")
        watermarks = covenant.get("prospective_epoch_watermarks")
        if not isinstance(watermarks, dict):
            raise PipelineError("holdout covenant has no frozen prospective watermarks")
        acquisition_watermark = _parse_timestamp(
            (watermarks.get("acquisition") or {}).get("timestamp"),
            field="prospective acquisition watermark",
        )
        publication_watermark = _parse_timestamp(
            (watermarks.get("publication") or {}).get("timestamp"),
            field="prospective publication watermark",
        )
        connection = sqlite3.connect(
            f"file:{db_path().expanduser().resolve()}?mode=ro", uri=True
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        try:
            rows = connection.execute(
                """
                SELECT t.id AS transcript_id, t.episode_id, t.fetched_at,
                       t.created_at AS transcript_created_at, e.published_at,
                       e.source_id,
                       (SELECT COUNT(*) FROM segments s WHERE s.transcript_id = t.id) AS segment_count
                FROM transcripts t
                JOIN episodes e ON e.id = t.episode_id
                WHERE t.status = 'ready' AND e.published_at IS NOT NULL
                ORDER BY t.id
                """
            ).fetchall()
        finally:
            connection.close()
        eligible = []
        for row in rows:
            acquired = _parse_timestamp(
                row["fetched_at"] or row["transcript_created_at"],
                field=f"prospective transcript {row['transcript_id']} acquisition",
            )
            published = _parse_timestamp(
                row["published_at"],
                field=f"prospective episode {row['episode_id']} publication",
            )
            if acquired > acquisition_watermark and published > publication_watermark:
                eligible.append(row)
        episode_ids = sorted({str(row["episode_id"]) for row in eligible})
        source_ids = sorted({str(row["source_id"]) for row in eligible})
        transcript_ids = sorted({str(row["transcript_id"]) for row in eligible})
        has_future = bool(eligible)
        sanitized_watermarks = {
            name: {
                key: value
                for key, value in (watermarks.get(name) or {}).items()
                if key != "ids_at_watermark"
            }
            for name in ("acquisition", "publication")
        }
        sanitized_watermarks["prospective_eligibility"] = watermarks.get(
            "prospective_eligibility"
        )
        payload = {
            "schema_version": "pif_app_server_prospective_epoch_status_v1",
            "status": (
                "blocked_execution_adapter_absent_with_eligible_future_data"
                if has_future
                else "waiting_for_future_data"
            ),
            "observed_at": now_iso(),
            "covenant_path": str(covenant_path),
            "covenant_sha256": _sha256_file(covenant_path),
            "frozen_watermarks": sanitized_watermarks,
            "eligible_episode_count": len(episode_ids),
            "eligible_source_count": len(source_ids),
            "eligible_transcript_count": len(transcript_ids),
            "eligible_segment_count": sum(int(row["segment_count"] or 0) for row in eligible),
            "eligible_episode_ids_sha256": hashlib.sha256(
                "\n".join(episode_ids).encode("utf-8")
            ).hexdigest(),
            "eligible_source_ids_sha256": hashlib.sha256(
                "\n".join(source_ids).encode("utf-8")
            ).hexdigest(),
            "eligible_transcript_ids_sha256": hashlib.sha256(
                "\n".join(transcript_ids).encode("utf-8")
            ).hexdigest(),
            "model_calls_performed": 0,
            "shadow_completed": False,
            "production_database_mutation": False,
            "production_promotion": False,
            "privacy": "frozen_watermarks_counts_and_hashed_ids_no_transcript_text",
        }
        _write_immutable_json(output, payload)
        artifact = _artifact_ref(
            output, expected_schema="pif_app_server_prospective_epoch_status_v1"
        )
        if has_future:
            return PhaseResult(
                status="blocked",
                artifacts=(artifact,),
                reason_code="prospective_execution_adapter_absent_with_eligible_data",
            )
        return PhaseResult(
            status="waiting_for_future_data",
            artifacts=(artifact,),
            reason_code="prospective_epoch_has_no_eligible_future_data",
        )


def default_paths(
    *, repo_root: Path, pipeline_root: Optional[Path] = None
) -> PipelinePaths:
    repo = repo_root.expanduser().resolve()
    root = (
        pipeline_root.expanduser().resolve()
        if pipeline_root is not None
        else repo / "work" / "app-server-development-v2" / "unattended-pipeline-v1"
    )
    return PipelinePaths(
        repo_root=repo,
        pipeline_root=root,
        run_spec=repo / "work" / "app-server-development-v2" / "run-spec-v2.json",
        matrix_root=repo / "work" / "app-server-development-v2" / "matrix-v1",
        context_usage_recovery=(
            repo
            / "work"
            / "windowed-acceptance-v1"
            / "paired-run-v2"
            / "evaluator-v2"
            / "context-usage-recovery-report.json"
        ),
        development_manifest=repo / "work" / "app-server-development-v2" / "manifest.json",
        provenance_root=root / "provenance",
        development_selection_root=root / "development-selection",
        frozen_winner=root / "development-selection" / "selection-result.json",
        holdout_root=root / "holdout-v1",
        holdout_stratification_root=root / "holdout-stratification-v1",
        holdout_execution_root=root / "holdout-execution-v1",
        holdout_judge_root=root / "holdout-judge-v1",
        prospective_root=root / "prospective-epoch-v1",
    )


def default_phases() -> list[PhaseDefinition]:
    return [
        PhaseDefinition("01_matrix", MatrixAdapter(), True),
        PhaseDefinition("02_provenance", ProvenanceAdapter(), True),
        PhaseDefinition("03_development_selection", DevelopmentSelectionAdapter(), True),
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


class UnattendedAppServerPipeline:
    def __init__(
        self,
        *,
        paths: PipelinePaths,
        phases: Optional[Sequence[PhaseDefinition]] = None,
        heartbeat_seconds: float = 30.0,
        subprocess_poll_seconds: float = 1.0,
        popen_factory: Callable[..., Any] = subprocess.Popen,
        state_schema_version: str = PIPELINE_VERSION,
        phase_marker_schema_version: str = PHASE_MARKER_VERSION,
        terminal_schema_version: str = TERMINAL_REPORT_VERSION,
    ):
        self.paths = paths
        self.phases = list(phases or default_phases())
        if len({phase.name for phase in self.phases}) != len(self.phases):
            raise PipelineError("pipeline phase names must be unique")
        self.heartbeat_seconds = max(0.05, heartbeat_seconds)
        self.subprocess_poll_seconds = max(0.01, subprocess_poll_seconds)
        self.popen_factory = popen_factory
        self.state_schema_version = state_schema_version
        self.phase_marker_schema_version = phase_marker_schema_version
        self.terminal_schema_version = terminal_schema_version
        self.state_path = paths.pipeline_root / "state.json"
        self.journal_path = paths.pipeline_root / "journal.jsonl"
        self.lock_path = paths.pipeline_root / "pipeline.lock"
        self.stop_path = paths.pipeline_root / "STOP"
        self.markers_root = paths.pipeline_root / "phase-markers"
        self.terminal_report_path = paths.pipeline_root / "pipeline-terminal.json"
        self.state: dict[str, Any] = {}
        self._write_lock = threading.RLock()
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_requested = False
        self._stop_reason: Optional[str] = None
        self._active_process: Optional[Any] = None
        self._interrupt_forwarded = False
        self._previous_signal_handlers: dict[int, Any] = {}

    def run(self) -> dict[str, Any]:
        os.environ.pop("OPENAI_API_KEY", None)
        self.paths.pipeline_root.mkdir(parents=True, exist_ok=True)
        with _LifetimeLock(self.lock_path):
            self._install_signal_handlers()
            self._start_heartbeat_thread()
            try:
                self._load_or_initialize_state()
                self._record("pipeline_started", status="running")
                context = PipelineContext(self)
                for phase in self.phases:
                    self.check_stop()
                    adopted = self._adopt_marker(phase)
                    if adopted is not None:
                        if adopted.status != _SUCCESS_PHASE_STATUS:
                            return self._finish_terminal(adopted.status, phase.name, adopted.reason_code)
                        continue
                    phase_state = (self.state.get("phases") or {}).get(phase.name) or {}
                    if (
                        phase_state.get("status") == "running"
                        and not phase.resumable_after_interruption
                    ):
                        result = PhaseResult(
                            status="blocked",
                            reason_code="interrupted_nonresumable_phase_requires_operator_audit",
                        )
                        self._commit_phase_marker(phase, result)
                        return self._finish_terminal(result.status, phase.name, result.reason_code)
                    self._set_phase_running(phase)
                    try:
                        result = phase.adapter.run(context)
                    except PipelineStopped:
                        raise
                    except BaseException as exc:
                        self._record(
                            "phase_failed_closed",
                            status="failed",
                            active_phase=phase.name,
                            error_class=type(exc).__name__,
                        )
                        raise
                    self.check_stop()
                    self._validate_phase_result(result)
                    self._commit_phase_marker(phase, result)
                    if result.status != _SUCCESS_PHASE_STATUS:
                        return self._finish_terminal(result.status, phase.name, result.reason_code)
                return self._finish_terminal("completed", None, None)
            except PipelineStopped:
                self._record(
                    "pipeline_stopped",
                    status="stopped",
                    stop_reason=self._stop_reason or "stop_requested",
                )
                raise
            except BaseException as exc:
                self._record(
                    "pipeline_failed",
                    status="failed",
                    error_class=type(exc).__name__,
                )
                raise
            finally:
                self._stop_heartbeat_thread()
                self._restore_signal_handlers()

    def _load_or_initialize_state(self) -> None:
        expected_names = [phase.name for phase in self.phases]
        if self.state_path.exists():
            state = _read_json(self.state_path, purpose="pipeline state")
            if state.get("schema_version") != self.state_schema_version:
                raise PipelineError("existing pipeline state schema is unsupported")
            if state.get("phase_order") != expected_names:
                raise PipelineError("pipeline phase order changed after state initialization")
            if Path(str(state.get("pipeline_root"))).resolve() != self.paths.pipeline_root:
                raise PipelineError("pipeline state belongs to another root")
            self.state = state
            return
        self.state = {
            "schema_version": self.state_schema_version,
            "created_at": now_iso(),
            "updated_at": now_iso(),
            "pipeline_root": str(self.paths.pipeline_root),
            "phase_order": expected_names,
            "status": "initializing",
            "active_phase": None,
            "production_mutation_allowed": False,
            "codex_exec_semantic_calls_allowed": False,
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
            "phases": {name: {"status": "pending"} for name in expected_names},
        }
        _write_json_atomic(self.state_path, self.state)

    def _record(self, event: str, **changes: Any) -> None:
        with self._write_lock:
            timestamp = now_iso()
            _append_journal(
                self.journal_path,
                {
                    "schema_version": self.state_schema_version,
                    "at": timestamp,
                    "event": event,
                    **changes,
                    "privacy": "phase_status_hashes_counts_and_failure_classes_only",
                },
            )
            self.state.update(changes)
            self.state["updated_at"] = timestamp
            _write_json_atomic(self.state_path, self.state)

    def _set_phase_running(self, phase: PhaseDefinition) -> None:
        phases = dict(self.state.get("phases") or {})
        phases[phase.name] = {
            "status": "running",
            "started_at": now_iso(),
            "resumable_after_interruption": phase.resumable_after_interruption,
        }
        self._record(
            "phase_started",
            status="running",
            active_phase=phase.name,
            phases=phases,
        )

    def _marker_path(self, phase: PhaseDefinition) -> Path:
        return self.markers_root / f"{phase.name}.json"

    def _validate_phase_result(self, result: PhaseResult) -> None:
        if result.status not in _TERMINAL_PHASE_STATUSES:
            raise PipelineError("phase returned a nonterminal or unsupported status")
        if result.status == "succeeded" and not result.artifacts:
            raise PipelineError("successful phase returned no immutable terminal artifacts")
        if result.reason_code is not None and not result.reason_code.replace("_", "").isalnum():
            raise PipelineError("phase reason code is not sanitized")
        for artifact in result.artifacts:
            if not artifact.path.is_file() or _sha256_file(artifact.path) != artifact.sha256:
                raise PipelineError("phase terminal artifact changed before marker commit")

    def _commit_phase_marker(
        self, phase: PhaseDefinition, result: PhaseResult
    ) -> dict[str, Any]:
        marker = {
            "schema_version": self.phase_marker_schema_version,
            "phase": phase.name,
            "status": result.status,
            "finished_at": now_iso(),
            "reason_code": result.reason_code,
            "artifacts": [artifact.marker_record() for artifact in result.artifacts],
            "metrics": result.metrics,
            "production_mutation_performed": False,
            "privacy": "artifact_paths_hashes_schemas_counts_and_status_only",
        }
        path = self._marker_path(phase)
        _write_immutable_json(path, marker)
        phases = dict(self.state.get("phases") or {})
        phases[phase.name] = {
            "status": result.status,
            "marker_path": str(path),
            "marker_sha256": _sha256_file(path),
            "reason_code": result.reason_code,
        }
        self._record(
            "phase_terminal",
            status="running" if result.status == "succeeded" else result.status,
            active_phase=None,
            phases=phases,
            last_phase=phase.name,
            last_phase_status=result.status,
        )
        return marker

    def _adopt_marker(self, phase: PhaseDefinition) -> Optional[PhaseResult]:
        path = self._marker_path(phase)
        if not path.exists():
            return None
        marker = _read_json(path, purpose="phase marker")
        if (
            marker.get("schema_version") != self.phase_marker_schema_version
            or marker.get("phase") != phase.name
        ):
            raise PipelineError("phase marker identity or schema drift")
        status = marker.get("status")
        if status not in _TERMINAL_PHASE_STATUSES:
            raise PipelineError("phase marker has a nonterminal status")
        artifacts = []
        for item in marker.get("artifacts") or []:
            if not isinstance(item, dict):
                raise PipelineError("phase marker artifact record is malformed")
            path_value = Path(str(item.get("path"))).expanduser().resolve()
            expected_sha = item.get("sha256")
            if not path_value.is_file() or _sha256_file(path_value) != expected_sha:
                raise PipelineError("phase marker artifact is missing or drifted")
            artifacts.append(
                ArtifactRef(
                    path=path_value,
                    sha256=str(expected_sha),
                    schema_version=(
                        str(item["schema_version"])
                        if isinstance(item.get("schema_version"), str)
                        else None
                    ),
                )
            )
        result = PhaseResult(
            status=str(status),
            artifacts=tuple(artifacts),
            reason_code=(
                str(marker["reason_code"])
                if isinstance(marker.get("reason_code"), str)
                else None
            ),
            metrics=marker.get("metrics") if isinstance(marker.get("metrics"), dict) else {},
        )
        self._validate_phase_result(result)
        phases = dict(self.state.get("phases") or {})
        phases[phase.name] = {
            "status": result.status,
            "marker_path": str(path),
            "marker_sha256": _sha256_file(path),
            "reason_code": result.reason_code,
            "adopted": True,
        }
        self._record(
            "phase_marker_adopted",
            phases=phases,
            active_phase=None,
            last_phase=phase.name,
            last_phase_status=result.status,
        )
        return result

    def _finish_terminal(
        self, status: str, terminal_phase: Optional[str], reason_code: Optional[str]
    ) -> dict[str, Any]:
        report = {
            "schema_version": self.terminal_schema_version,
            "status": status,
            "finished_at": now_iso(),
            "terminal_phase": terminal_phase,
            "reason_code": reason_code,
            "phase_markers": [
                {
                    "phase": phase.name,
                    "path": str(self._marker_path(phase)),
                    "sha256": _sha256_file(self._marker_path(phase)),
                }
                for phase in self.phases
                if self._marker_path(phase).is_file()
            ],
            "production_mutation_performed": False,
            "production_promotion_performed": False,
            "privacy": "phase_marker_paths_hashes_and_terminal_status_only",
        }
        if self.terminal_report_path.exists():
            existing = _read_json(self.terminal_report_path, purpose="pipeline terminal report")
            comparable_existing = dict(existing)
            comparable_report = dict(report)
            comparable_existing.pop("finished_at", None)
            comparable_report.pop("finished_at", None)
            if comparable_existing == comparable_report:
                report = existing
            else:
                raise PipelineError("a different immutable pipeline terminal report already exists")
        else:
            _write_immutable_json(self.terminal_report_path, report)
        self._record(
            "pipeline_terminal",
            status=status,
            active_phase=None,
            terminal_phase=terminal_phase,
            reason_code=reason_code,
            terminal_report_path=str(self.terminal_report_path),
            terminal_report_sha256=_sha256_file(self.terminal_report_path),
        )
        return report

    def heartbeat(self) -> None:
        if not self.state:
            return
        active = self.state.get("active_phase")
        if active:
            self._record("pipeline_heartbeat", active_phase=active)

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(self.heartbeat_seconds):
            try:
                self.heartbeat()
            except Exception:
                # The foreground path will detect state/journal failures.  The heartbeat
                # thread must not invent a successful phase transition.
                return

    def _start_heartbeat_thread(self) -> None:
        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="app-server-pipeline-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _stop_heartbeat_thread(self) -> None:
        self._heartbeat_stop.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=max(1.0, self.heartbeat_seconds * 2))
        self._heartbeat_thread = None

    def _install_signal_handlers(self) -> None:
        def handle(signum: int, _frame: Any) -> None:
            if self._stop_requested:
                return
            self._stop_requested = True
            try:
                self._stop_reason = signal.Signals(signum).name
            except ValueError:
                self._stop_reason = f"signal_{signum}"

        for signum in (signal.SIGINT, signal.SIGTERM):
            self._previous_signal_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, handle)

    def _restore_signal_handlers(self) -> None:
        for signum, previous in self._previous_signal_handlers.items():
            signal.signal(signum, previous)
        self._previous_signal_handlers.clear()

    def check_stop(self) -> None:
        if self.stop_path.exists() and not self._stop_requested:
            self._stop_requested = True
            self._stop_reason = "stop_sentinel"
        if not self._stop_requested:
            return
        if self._active_process is not None and not self._interrupt_forwarded:
            self._interrupt_forwarded = True
            try:
                os.killpg(int(self._active_process.pid), signal.SIGINT)
            except ProcessLookupError:
                pass
        if self._active_process is None:
            raise PipelineStopped(self._stop_reason or "stop_requested")

    def run_module(self, module: str, arguments: Sequence[str]) -> int:
        if not module.startswith("research_factory.app_server_"):
            raise PipelineError("pipeline subprocess module is outside the app-server allowlist")
        if any("codex exec" in value.lower() for value in [module, *arguments]):
            raise PipelineError("codex exec is prohibited in the unattended app-server pipeline")
        semantic_modules = {
            "research_factory.app_server_dev_selection",
            "research_factory.app_server_sharded_selection",
            "research_factory.app_server_holdout_stratifier",
            "research_factory.app_server_holdout_execution",
            "research_factory.app_server_holdout_judge",
            "research_factory.app_server_prospective_epoch",
        }
        if module in semantic_modules:
            self._wait_for_semantic_capacity(module)
        command = [sys.executable, "-m", module, *[str(value) for value in arguments]]
        env = dict(os.environ)
        env.pop("OPENAI_API_KEY", None)
        env["PYTHONUNBUFFERED"] = "1"
        log_root = self.paths.pipeline_root / "subprocess-logs"
        log_root.mkdir(parents=True, exist_ok=True)
        phase_name = str(self.state.get("active_phase") or module.rsplit(".", 1)[-1])
        safe_phase = "".join(
            character if character.isalnum() or character in {"-", "_"} else "_"
            for character in phase_name
        )
        stdout_path = log_root / f"{safe_phase}.stdout.log"
        stderr_path = log_root / f"{safe_phase}.stderr.log"
        return_code: Optional[int] = None
        with stdout_path.open("ab", buffering=0) as stdout_handle, stderr_path.open(
            "ab", buffering=0
        ) as stderr_handle:
            process = self.popen_factory(
                command,
                cwd=str(self.paths.repo_root),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=stdout_handle,
                stderr=stderr_handle,
                start_new_session=True,
            )
            self._active_process = process
            self._interrupt_forwarded = False
            self._record(
                "phase_subprocess_started",
                subprocess_module=module,
                subprocess_pid=int(process.pid),
                subprocess_command_sha256=hashlib.sha256(
                    "\0".join(command).encode("utf-8")
                ).hexdigest(),
                subprocess_stdout_log=str(stdout_path),
                subprocess_stderr_log=str(stderr_path),
            )
            while return_code is None:
                self.check_stop()
                try:
                    return_code = process.wait(timeout=self.subprocess_poll_seconds)
                except subprocess.TimeoutExpired:
                    return_code = None
            self._active_process = None
        self._record(
            "phase_subprocess_terminal",
            subprocess_module=module,
            subprocess_return_code=int(return_code),
        )
        if self._stop_requested:
            raise PipelineStopped(self._stop_reason or "stop_requested")
        return int(return_code)

    def _wait_for_semantic_capacity(self, module: str) -> None:
        from .app_server_interrupted_arm_recovery import probe_app_server_rate_limits

        self._verify_instruction_contract()
        while True:
            self.check_stop()
            try:
                probe = asyncio.run(
                    probe_app_server_rate_limits(maximum_primary_used_percent=20)
                )
            except Exception as exc:
                raise PipelineError("managed app-server rate-limit preflight failed") from exc
            cleared = probe.get("cleared_for_semantic_work") is True
            self._record(
                "semantic_phase_rate_limit_probe",
                semantic_module=module,
                primary_used_percent=probe.get("primary_used_percent"),
                primary_resets_at=probe.get("primary_resets_at"),
                rate_limit_reached_type=probe.get("rate_limit_reached_type"),
                managed_chatgpt_auth_verified=probe.get("managed_chatgpt_auth_verified"),
                thread_started=False,
                turn_started=False,
            )
            if cleared:
                return
            now = int(time.time())
            reset_at = probe.get("primary_resets_at")
            wait_seconds = 300
            if isinstance(reset_at, int) and reset_at > now:
                wait_seconds = min(wait_seconds, max(30, reset_at - now + 30))
            time.sleep(float(wait_seconds))

    def _verify_instruction_contract(self) -> None:
        spec = _read_json(self.paths.run_spec, purpose="frozen app-server run spec")
        contract = spec.get("instruction_contract")
        if not isinstance(contract, dict):
            raise PipelineError("run spec has no frozen instruction contract")
        sources = contract.get("sources")
        if not isinstance(sources, list) or not sources:
            raise PipelineError("run spec instruction source contract is empty")
        paths = []
        for source in sources:
            if not isinstance(source, dict):
                raise PipelineError("run spec instruction source is malformed")
            path = Path(str(source.get("path") or "")).expanduser().resolve()
            expected_sha = source.get("content_sha256")
            expected_size = source.get("size_bytes")
            if (
                not path.is_file()
                or not isinstance(expected_sha, str)
                or _sha256_file(path) != expected_sha
                or isinstance(expected_size, bool)
                or not isinstance(expected_size, int)
                or path.stat().st_size != expected_size
            ):
                raise PipelineError("frozen instruction source content drift")
            paths.append(str(path))
        if _canonical_sha(paths) != contract.get("expected_path_set_sha256"):
            raise PipelineError("frozen instruction source path set drift")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Durably run the private app-server-only matrix, provenance, selection, "
            "untouched holdout, and prospective evidence pipeline without production promotion."
        )
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--pipeline-root")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    paths = default_paths(
        repo_root=Path(args.repo_root),
        pipeline_root=Path(args.pipeline_root) if args.pipeline_root else None,
    )
    pipeline = UnattendedAppServerPipeline(paths=paths)
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
