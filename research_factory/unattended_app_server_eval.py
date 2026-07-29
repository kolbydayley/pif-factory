from __future__ import annotations

import argparse
import asyncio
import fcntl
import hashlib
import json
import os
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

from .app_server_evaluation import (
    APP_SERVER_CORE_ARM_VERSION,
    APP_SERVER_CORE_MATRIX_VERSION,
    APP_SERVER_EPISODE_BATCH_SCHEMA_VERSION,
    aggregate_app_server_development_matrix,
)
from .codex_app_server import (
    APP_SERVER_CLIENT_VERSION,
    PINNED_CODEX_CLI_VERSION,
    PROTOCOL_SCHEMA_SHA256,
    CodexAppServerClient,
)
from .util import now_iso, write_text_atomic


SUPERVISOR_VERSION = "pif_unattended_app_server_matrix_supervisor_v2"
RUN_SPEC_VERSION = "pif_app_server_development_run_spec_v3"
INSTRUCTION_PROVENANCE_VERSION = "pif_instruction_content_provenance_v1"
HANDOFF_GUARD_VERSION = "pif_supervisor_handoff_guard_v1"

_ARM_COMMAND = "efficiency-app-server-core-arm"
_REQUIRED_USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
_EXPECTED_TRANSPORT_POLICY = {
    "interface": "official_codex_app_server_stdio_json_rpc",
    "managed_chatgpt_auth_only": True,
    "public_openai_api_allowed": False,
    "api_key_billing_allowed": False,
    "raw_session_token_access_allowed": False,
    "codex_exec_semantic_calls_allowed": False,
    "approval_policy": "never",
    "sandbox": "read_only",
    "dynamic_tools": False,
    "silent_retry": False,
}
_EXPECTED_SUPERVISION_POLICY = {
    "lock_scope": "full_process_lifetime",
    "adopt_exact_live_process": True,
    "rerun_partial_or_interrupted_arm": False,
    "stop_on_unknown_usage": True,
    "stop_on_incomplete_accounting": True,
    "continue_on_audited_metric_grounding_diagnostics": True,
    "production_changes_allowed": False,
}
_HANDOFF_GUARD = {
    "schema_version": HANDOFF_GUARD_VERSION,
    "created_by": "unattended_app_server_eval_takeover",
    "purpose": "prevent_legacy_controller_from_starting_next_arm_during_supervisor_handoff",
    "safe_to_remove_only_by": "unattended_app_server_eval_supervisor",
    "model_calls_started": False,
    "privacy": "no_prompt_transcript_or_output_text",
}


class SupervisorError(RuntimeError):
    """A fail-closed supervisor invariant was violated."""


class LockUnavailable(SupervisorError):
    """Another supervisor owns the full-lifetime lock."""


class SupervisorStopped(SupervisorError):
    """The stop sentinel or a process signal requested a safe stop."""


@dataclass(frozen=True)
class Arm:
    name: str
    batch_size: int
    thread_mode: str


@dataclass(frozen=True)
class ProcessRecord:
    pid: int
    ppid: int
    command: str


@dataclass(frozen=True)
class ParsedArmProcess:
    record: ProcessRecord
    arm: Arm
    values: Mapping[str, str]


def _sha256_bytes(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SupervisorError(f"cannot read valid JSON artifact: {path}") from exc
    if not isinstance(value, dict):
        raise SupervisorError(f"JSON artifact is not an object: {path}")
    return value


def _write_json_atomic(path: Path, value: Any) -> None:
    write_text_atomic(
        path,
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )


def _append_jsonl_durable(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        written = os.write(descriptor, encoded)
        if written != len(encoded):
            raise SupervisorError("short durable journal write")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_process_table() -> list[ProcessRecord]:
    completed = subprocess.run(
        ["ps", "-ww", "-axo", "pid=,ppid=,command="],
        check=True,
        capture_output=True,
        text=True,
    )
    records = []
    for raw_line in completed.stdout.splitlines():
        fields = raw_line.strip().split(None, 2)
        if len(fields) != 3:
            continue
        try:
            records.append(
                ProcessRecord(pid=int(fields[0]), ppid=int(fields[1]), command=fields[2])
            )
        except ValueError:
            continue
    return records


def _read_codex_version() -> str:
    completed = subprocess.run(
        ["codex", "--version"],
        check=True,
        capture_output=True,
        text=True,
    )
    value = completed.stdout.strip()
    prefix = "codex-cli "
    return value[len(prefix) :] if value.startswith(prefix) else value


async def _probe_instruction_sources_async(
    *, model: str, cwd: Path
) -> tuple[str, int]:
    async with CodexAppServerClient() as client:
        thread = await client.start_thread(
            model=model,
            base_instructions=(
                "Instruction-source preflight only. No semantic task or model turn is requested."
            ),
            cwd=cwd,
            ephemeral=True,
        )
        return thread.instruction_sources_sha256, thread.instruction_sources_count


def _probe_instruction_sources(*, model: str, cwd: Path) -> tuple[str, int]:
    return asyncio.run(_probe_instruction_sources_async(model=model, cwd=cwd))


class SupervisorLock:
    def __init__(self, path: Path):
        self.path = path
        self._handle: Optional[Any] = None

    def __enter__(self) -> "SupervisorLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._handle.close()
            self._handle = None
            raise LockUnavailable("another matrix supervisor owns the lifetime lock") from exc
        self._handle.seek(0)
        self._handle.truncate()
        self._handle.write(f"pid={os.getpid()} acquired_at={now_iso()}\n")
        self._handle.flush()
        os.fsync(self._handle.fileno())
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # noqa: ANN001
        if self._handle is None:
            return
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None


class UnattendedAppServerMatrixSupervisor:
    def __init__(
        self,
        *,
        repo_root: Path,
        run_spec_path: Path,
        matrix_root: Optional[Path] = None,
        process_reader: Callable[[], list[ProcessRecord]] = _read_process_table,
        popen_factory: Callable[..., Any] = subprocess.Popen,
        codex_version_reader: Callable[[], str] = _read_codex_version,
        instruction_probe: Callable[..., tuple[str, int]] = _probe_instruction_sources,
        monotonic: Callable[[], float] = time.monotonic,
        poll_seconds: float = 1.0,
        heartbeat_seconds: float = 30.0,
    ):
        self.repo_root = repo_root.expanduser().resolve()
        self.run_spec_path = run_spec_path.expanduser().resolve()
        self.spec = _read_json(self.run_spec_path)
        default_matrix_root = self.run_spec_path.parent / "matrix-v1"
        self.matrix_root = (matrix_root or default_matrix_root).expanduser().resolve()
        self.control_root = self.matrix_root / "supervisor"
        self.state_path = self.control_root / "state.json"
        self.journal_path = self.control_root / "journal.jsonl"
        self.lock_path = self.control_root / "supervisor.lock"
        self.stop_path = self.control_root / "STOP"
        self.instruction_provenance_path = (
            self.control_root / "instruction-provenance-v1.json"
        )
        self.matrix_report_path = self.matrix_root / "matrix-report.json"
        self.process_reader = process_reader
        self.popen_factory = popen_factory
        self.codex_version_reader = codex_version_reader
        self.instruction_probe = instruction_probe
        self.monotonic = monotonic
        self.poll_seconds = max(0.01, poll_seconds)
        self.heartbeat_seconds = max(self.poll_seconds, heartbeat_seconds)
        self.run_spec_sha256 = _sha256_bytes(self.run_spec_path)
        self.state: dict[str, Any] = {}
        self._stop_requested = False
        self._stop_reason: Optional[str] = None
        self._active_pid: Optional[int] = None
        self._active_owned = False
        self._forwarded_interrupt = False
        self._previous_signal_handlers: dict[int, Any] = {}

    def run(self) -> dict[str, Any]:
        with SupervisorLock(self.lock_path):
            self._install_signal_handlers()
            try:
                self._load_or_initialize_state()
                self._record("supervisor_started", status="running")
                self._static_preflight()
                arms = self._expected_arms()
                for arm in arms:
                    self._check_stop()
                    self._run_or_adopt_arm(arm)
                self._check_stop()
                matrix = self._aggregate_or_adopt(arms)
                self._record(
                    "matrix_completed",
                    status="completed",
                    active_arm=None,
                    matrix_report_path=str(self.matrix_report_path),
                    matrix_report_sha256=_sha256_bytes(self.matrix_report_path),
                )
                return matrix
            except SupervisorStopped as exc:
                self._record(
                    "supervisor_stopped",
                    status="stopped",
                    stop_reason=self._stop_reason or str(exc),
                )
                raise
            except BaseException as exc:
                if self._active_pid is not None:
                    self._forward_interrupt(signal.SIGINT, "supervisor_failure")
                self._record(
                    "supervisor_failed",
                    status="failed",
                    error_class=type(exc).__name__,
                    error_message=str(exc),
                )
                raise
            finally:
                self._restore_signal_handlers()

    def _load_or_initialize_state(self) -> None:
        self.control_root.mkdir(parents=True, exist_ok=True)
        if self.state_path.exists():
            state = _read_json(self.state_path)
            if state.get("schema_version") != SUPERVISOR_VERSION:
                raise SupervisorError("unsupported existing supervisor state")
            if state.get("run_spec_sha256") != self.run_spec_sha256:
                raise SupervisorError("run spec changed after supervisor state was created")
            if Path(str(state.get("matrix_root"))).resolve() != self.matrix_root:
                raise SupervisorError("existing supervisor state belongs to another matrix root")
            self.state = state
            return
        arms = {arm.name: {"status": "pending"} for arm in self._expected_arms()}
        self.state = {
            "schema_version": SUPERVISOR_VERSION,
            "created_at": now_iso(),
            "updated_at": now_iso(),
            "run_spec_path": str(self.run_spec_path),
            "run_spec_sha256": self.run_spec_sha256,
            "matrix_root": str(self.matrix_root),
            "status": "initializing",
            "active_arm": None,
            "arms": arms,
        }
        _write_json_atomic(self.state_path, self.state)

    def _record(self, event: str, **changes: Any) -> None:
        timestamp = now_iso()
        journal_item = {
            "schema_version": SUPERVISOR_VERSION,
            "at": timestamp,
            "event": event,
            **changes,
        }
        _append_jsonl_durable(self.journal_path, journal_item)
        self.state.update(changes)
        self.state["updated_at"] = timestamp
        _write_json_atomic(self.state_path, self.state)

    def _record_arm(self, arm: Arm, event: str, **changes: Any) -> None:
        arms = dict(self.state.get("arms") or {})
        arm_state = dict(arms.get(arm.name) or {})
        arm_state.update(changes)
        arm_state["updated_at"] = now_iso()
        arms[arm.name] = arm_state
        self.state["arms"] = arms
        self._record(
            event,
            active_arm=arm.name,
            arms=arms,
            last_arm_event={"arm": arm.name, **changes},
        )

    def _resolve_repo_artifact(self, raw_path: str) -> Path:
        path = Path(raw_path).expanduser()
        return path.resolve() if path.is_absolute() else (self.repo_root / path).resolve()

    def _verify_artifact(self, contract: Mapping[str, Any], label: str) -> Path:
        raw_path = contract.get("artifact_path")
        expected_sha = contract.get("artifact_sha256")
        if not isinstance(raw_path, str) or not isinstance(expected_sha, str):
            raise SupervisorError(f"{label} has no frozen path and hash")
        path = self._resolve_repo_artifact(raw_path)
        if not path.is_file():
            raise SupervisorError(f"frozen {label} artifact is missing: {path}")
        observed_sha = _sha256_bytes(path)
        if observed_sha != expected_sha:
            raise SupervisorError(
                f"frozen {label} artifact drift: expected {expected_sha}, observed {observed_sha}"
            )
        return path

    def _static_preflight(self) -> None:
        if self.spec.get("schema_version") != RUN_SPEC_VERSION:
            raise SupervisorError("unsupported unattended app-server run spec")
        parent_spec = self.spec.get("parent_run_spec")
        if not isinstance(parent_spec, dict):
            raise SupervisorError("run spec has no frozen parent")
        self._verify_artifact(parent_spec, "parent run spec")
        for label in ("manifest", "shared_reference_seed", "reference_noise"):
            contract = self.spec.get(label)
            if not isinstance(contract, dict):
                raise SupervisorError(f"run spec has no {label} contract")
            self._verify_artifact(contract, label)
        frozen = self.spec.get("frozen_artifacts")
        if not isinstance(frozen, dict) or not frozen:
            raise SupervisorError("run spec has no frozen artifacts")
        for label, contract in frozen.items():
            if not isinstance(contract, dict):
                raise SupervisorError(f"invalid frozen artifact contract: {label}")
            self._verify_artifact(contract, str(label))

        candidate = self._candidate()
        if int(candidate.get("retry_count", -1)) != 0:
            raise SupervisorError("candidate retry policy is not frozen to zero")
        if candidate.get("fallback_policy") != "none":
            raise SupervisorError("candidate fallback policy is not frozen to none")
        if candidate.get("core_arm_version") != APP_SERVER_CORE_ARM_VERSION:
            raise SupervisorError("candidate core arm version drift")
        if candidate.get("output_schema_version") != APP_SERVER_EPISODE_BATCH_SCHEMA_VERSION:
            raise SupervisorError("candidate output schema version drift")

        transport = self.spec.get("transport")
        if not isinstance(transport, dict):
            raise SupervisorError("run spec has no transport contract")
        for key, expected in _EXPECTED_TRANSPORT_POLICY.items():
            if transport.get(key) != expected:
                raise SupervisorError(f"unsafe or drifted transport policy: {key}")
        if transport.get("max_message_bytes") != 32 * 1024 * 1024:
            raise SupervisorError("app-server maximum message size drift")
        if transport.get("client_version") != APP_SERVER_CLIENT_VERSION:
            raise SupervisorError("app-server client version drift")
        if transport.get("protocol_schema_sha256") != PROTOCOL_SCHEMA_SHA256:
            raise SupervisorError("app-server protocol schema pin drift")
        expected_cli = str(transport.get("cli_version") or "")
        if expected_cli != PINNED_CODEX_CLI_VERSION:
            raise SupervisorError("run spec Codex CLI pin differs from client pin")
        try:
            observed_cli = self.codex_version_reader()
        except Exception as exc:
            raise SupervisorError("cannot verify the pinned Codex CLI") from exc
        if observed_cli != expected_cli:
            raise SupervisorError(
                f"Codex CLI version drift: expected {expected_cli}, observed {observed_cli}"
            )

        expected_order = [arm.name for arm in self._arms_from_candidate()]
        if self.spec.get("execution_order") != expected_order:
            raise SupervisorError("execution order is not the complete symmetric frozen matrix")
        supervision = self.spec.get("supervision")
        if not isinstance(supervision, dict):
            raise SupervisorError("run spec has no supervision policy")
        for key, expected in _EXPECTED_SUPERVISION_POLICY.items():
            if supervision.get(key) != expected:
                raise SupervisorError(f"unsafe or drifted supervision policy: {key}")
        if self.spec.get("selection_eligible") is not False:
            raise SupervisorError("development run spec must remain selection-ineligible")
        retrospective = self.spec.get("retrospective_adoptions")
        effective_from = self.spec.get("effective_from_arm")
        if (
            not isinstance(retrospective, list)
            or not all(isinstance(item, str) for item in retrospective)
            or not isinstance(effective_from, str)
            or effective_from not in expected_order
            or retrospective != expected_order[: expected_order.index(effective_from)]
        ):
            raise SupervisorError("retrospective adoption boundary is not frozen consistently")
        self._verify_instruction_contract(freeze_if_missing=True)
        self._record("static_preflight_passed", static_preflight_status="passed")

    def _candidate(self) -> dict[str, Any]:
        candidate = self.spec.get("candidate")
        if not isinstance(candidate, dict):
            raise SupervisorError("run spec has no candidate contract")
        return candidate

    def _arms_from_candidate(self) -> list[Arm]:
        candidate = self._candidate()
        batch_sizes = candidate.get("batch_sizes")
        thread_modes = candidate.get("thread_modes")
        if batch_sizes != [3, 5, 8] or thread_modes != ["new_thread", "same_thread"]:
            raise SupervisorError("candidate matrix must contain the frozen six symmetric arms")
        return [
            Arm(
                name=f"batch_{int(batch_size)}_{thread_mode}",
                batch_size=int(batch_size),
                thread_mode=str(thread_mode),
            )
            for batch_size in batch_sizes
            for thread_mode in thread_modes
        ]

    def _expected_arms(self) -> list[Arm]:
        by_name = {arm.name: arm for arm in self._arms_from_candidate()}
        order = self.spec.get("execution_order")
        if not isinstance(order, list) or not all(isinstance(item, str) for item in order):
            raise SupervisorError("run spec has no valid execution order")
        try:
            return [by_name[name] for name in order]
        except KeyError as exc:
            raise SupervisorError(f"unknown arm in execution order: {exc.args[0]}") from exc

    def _instruction_contract_payload(self) -> dict[str, Any]:
        contract = self.spec.get("instruction_contract")
        if not isinstance(contract, dict):
            raise SupervisorError("run spec has no instruction content contract")
        expected_path_set_sha = contract.get("expected_path_set_sha256")
        sources = contract.get("sources")
        if not isinstance(expected_path_set_sha, str) or not isinstance(sources, list) or not sources:
            raise SupervisorError("instruction content contract is incomplete")
        normalized_sources = []
        path_values = []
        for item in sources:
            if not isinstance(item, dict):
                raise SupervisorError("instruction source contract is malformed")
            raw_path = item.get("path")
            expected_sha = item.get("content_sha256")
            expected_size = item.get("size_bytes")
            if (
                not isinstance(raw_path, str)
                or not isinstance(expected_sha, str)
                or not isinstance(expected_size, int)
            ):
                raise SupervisorError("instruction source path, hash, or size is missing")
            source_path = Path(raw_path).expanduser()
            if not source_path.is_absolute():
                raise SupervisorError("instruction source paths must be absolute")
            path_values.append(str(source_path))
            if not source_path.is_file():
                raise SupervisorError(f"instruction source is missing: {source_path}")
            observed_sha = _sha256_bytes(source_path)
            observed_size = source_path.stat().st_size
            if observed_sha != expected_sha or observed_size != expected_size:
                raise SupervisorError(f"instruction source content drift: {source_path}")
            normalized_sources.append(
                {
                    "path": str(source_path),
                    "content_sha256": observed_sha,
                    "size_bytes": observed_size,
                }
            )
        observed_path_set_sha = _canonical_json_sha256(path_values)
        if observed_path_set_sha != expected_path_set_sha:
            raise SupervisorError("instruction source path set drift")
        return {
            "schema_version": INSTRUCTION_PROVENANCE_VERSION,
            "run_spec_sha256": self.run_spec_sha256,
            "expected_path_set_sha256": expected_path_set_sha,
            "source_count": len(normalized_sources),
            "sources": normalized_sources,
            "content_set_sha256": _canonical_json_sha256(normalized_sources),
            "privacy": "local_instruction_paths_hashes_and_sizes_only_no_instruction_text",
        }

    def _verify_instruction_contract(self, *, freeze_if_missing: bool) -> dict[str, Any]:
        observed = self._instruction_contract_payload()
        if self.instruction_provenance_path.exists():
            frozen = _read_json(self.instruction_provenance_path)
            comparable_frozen = dict(frozen)
            comparable_frozen.pop("frozen_at", None)
            if comparable_frozen != observed:
                raise SupervisorError("frozen instruction provenance drift")
            return observed
        if not freeze_if_missing:
            raise SupervisorError("instruction provenance has not been frozen")
        artifact = {**observed, "frozen_at": now_iso()}
        _write_json_atomic(self.instruction_provenance_path, artifact)
        return observed

    def _probe_instruction_contract(self) -> None:
        contract = self._verify_instruction_contract(freeze_if_missing=False)
        candidate = self._candidate()
        try:
            observed_sha, observed_count = self.instruction_probe(
                model=str(candidate["model"]), cwd=self.repo_root
            )
        except Exception as exc:
            raise SupervisorError("app-server instruction-source preflight failed") from exc
        if (
            observed_sha != contract["expected_path_set_sha256"]
            or observed_count != contract["source_count"]
        ):
            raise SupervisorError("app-server returned an unexpected instruction source set")
        self._record(
            "instruction_source_probe_passed",
            instruction_source_probe_status="passed",
            instruction_source_path_set_sha256=observed_sha,
            instruction_source_count=observed_count,
        )

    def _arm_dir(self, arm: Arm) -> Path:
        return self.matrix_root / f"batch-{arm.batch_size}" / arm.thread_mode

    def _arm_report_path(self, arm: Arm) -> Path:
        return self._arm_dir(arm) / "report.json"

    def _expected_command_values(self, arm: Arm) -> dict[str, str]:
        candidate = self._candidate()
        guideline = self.spec["frozen_artifacts"]["candidate_guideline"]["artifact_path"]
        manifest = self.spec["manifest"]["artifact_path"]
        return {
            "--manifest": str(self._resolve_repo_artifact(str(manifest))),
            "--output-dir": str(self._arm_dir(arm)),
            "--batch-size": str(arm.batch_size),
            "--thread-mode": arm.thread_mode,
            "--model": str(candidate["model"]),
            "--reasoning-effort": str(candidate["reasoning_effort"]),
            "--concurrency": str(int(candidate["concurrency"])),
            "--timeout-seconds": str(float(candidate["timeout_seconds"])),
            "--window-count": str(int(candidate["window_count"])),
            "--context-chars": str(int(candidate["context_chars"])),
            "--max-events": str(int(candidate["event_cap"])),
            "--guidelines": str(self._resolve_repo_artifact(str(guideline))),
        }

    def _command(self, arm: Arm) -> list[str]:
        values = self._expected_command_values(arm)
        ordered_flags = (
            "--manifest",
            "--output-dir",
            "--batch-size",
            "--thread-mode",
            "--model",
            "--reasoning-effort",
            "--concurrency",
            "--timeout-seconds",
            "--window-count",
            "--context-chars",
            "--max-events",
            "--guidelines",
        )
        command = [sys.executable, "-m", "research_factory", _ARM_COMMAND]
        for flag in ordered_flags:
            value = values[flag]
            if flag in {"--manifest", "--output-dir", "--guidelines"}:
                try:
                    value = str(Path(value).relative_to(self.repo_root))
                except ValueError:
                    pass
            elif flag == "--timeout-seconds" and float(value).is_integer():
                value = str(int(float(value)))
            command.extend([flag, value])
        return command

    def _parse_arm_process(self, record: ProcessRecord) -> Optional[ParsedArmProcess]:
        try:
            tokens = shlex.split(record.command)
        except ValueError:
            return None
        if len(tokens) < 4 or tokens[1:4] != ["-m", "research_factory", _ARM_COMMAND]:
            return None
        try:
            if Path(tokens[0]).resolve() != Path(sys.executable).resolve():
                return None
        except OSError:
            return None
        allowed_flags = set(self._expected_command_values(self._expected_arms()[0]))
        remainder = tokens[4:]
        if len(remainder) != len(allowed_flags) * 2:
            return None
        values: dict[str, str] = {}
        for index in range(0, len(remainder), 2):
            flag, value = remainder[index : index + 2]
            if flag not in allowed_flags or flag in values or value.startswith("--"):
                return None
            values[flag] = value
        try:
            arm = Arm(
                name=f"batch_{int(values['--batch-size'])}_{values['--thread-mode']}",
                batch_size=int(values["--batch-size"]),
                thread_mode=values["--thread-mode"],
            )
        except (KeyError, ValueError):
            return None
        expected = self._expected_command_values(arm)
        if set(values) != set(expected):
            return None
        for flag, expected_value in expected.items():
            observed = values[flag]
            if flag in {"--manifest", "--output-dir", "--guidelines"}:
                observed = str(self._resolve_repo_artifact(observed))
            elif flag in {
                "--batch-size",
                "--concurrency",
                "--window-count",
                "--context-chars",
                "--max-events",
            }:
                try:
                    observed = str(int(observed))
                except ValueError:
                    return None
            elif flag == "--timeout-seconds":
                try:
                    observed = str(float(observed))
                except ValueError:
                    return None
            if observed != expected_value:
                return None
        return ParsedArmProcess(record=record, arm=arm, values=values)

    def _target_processes(self) -> list[ParsedArmProcess]:
        parsed = []
        for record in self.process_reader():
            try:
                raw_tokens = shlex.split(record.command)
            except ValueError:
                continue
            is_core = len(raw_tokens) >= 4 and raw_tokens[1:4] == [
                "-m",
                "research_factory",
                _ARM_COMMAND,
            ]
            if not is_core:
                continue
            item = self._parse_arm_process(record)
            if item is None:
                raw_values = {}
                for index in range(4, len(raw_tokens) - 1):
                    if raw_tokens[index] in {"--manifest", "--output-dir"}:
                        raw_values.setdefault(raw_tokens[index], raw_tokens[index + 1])
                targets_manifest = False
                targets_matrix = False
                if "--manifest" in raw_values:
                    targets_manifest = self._resolve_repo_artifact(
                        raw_values["--manifest"]
                    ) == self._resolve_repo_artifact(
                        str(self.spec["manifest"]["artifact_path"])
                    )
                if "--output-dir" in raw_values:
                    output_path = self._resolve_repo_artifact(raw_values["--output-dir"])
                    targets_matrix = output_path == self.matrix_root or self.matrix_root in output_path.parents
                if targets_manifest or targets_matrix:
                    raise SupervisorError(
                        "a non-exact app-server core-arm process targets this frozen matrix"
                    )
                continue
            manifest = self._resolve_repo_artifact(item.values["--manifest"])
            expected_manifest = self._resolve_repo_artifact(
                str(self.spec["manifest"]["artifact_path"])
            )
            if manifest == expected_manifest:
                parsed.append(item)
        return parsed

    def _assert_single_expected_process(
        self, arm: Arm
    ) -> Optional[ParsedArmProcess]:
        processes = self._target_processes()
        if len(processes) > 1:
            raise SupervisorError("multiple app-server core-arm processes target this manifest")
        if not processes:
            return None
        process = processes[0]
        if process.arm != arm:
            raise SupervisorError(
                f"live process is {process.arm.name}, but the next frozen arm is {arm.name}"
            )
        return process

    def _run_or_adopt_arm(self, arm: Arm) -> None:
        report_path = self._arm_report_path(arm)
        if report_path.exists():
            live_processes = self._target_processes()
            if len(live_processes) > 1:
                raise SupervisorError("multiple app-server core-arm processes target this manifest")
            if live_processes and live_processes[0].arm == arm:
                raise SupervisorError("a completed arm report exists while that arm is still live")
            report = self._validate_arm_report(arm, report_path)
            self._record_arm(
                arm,
                "arm_adopted_completed",
                status="completed",
                adoption="retrospective_report",
                report_path=str(report_path),
                report_sha256=_sha256_bytes(report_path),
                validator_clean_calls=int(report.get("validator_clean_calls") or 0),
            )
            self.state["active_arm"] = None
            _write_json_atomic(self.state_path, self.state)
            return

        arm_state = dict((self.state.get("arms") or {}).get(arm.name) or {})
        prior_status = arm_state.get("status")
        live = self._assert_single_expected_process(arm)
        if live is not None:
            if prior_status in {"failed", "interrupted"}:
                raise SupervisorError(
                    f"arm {arm.name} has a terminal recovery-required state while a process is live"
                )
            self._assert_no_unowned_partial_files(arm, permit_live=True)
            self._record_arm(
                arm,
                "arm_adopted_live",
                status="running",
                adoption="exact_live_process",
                pid=live.record.pid,
                command_sha256=hashlib.sha256(live.record.command.encode("utf-8")).hexdigest(),
            )
            self._active_pid = live.record.pid
            self._active_owned = False
            self._wait_for_adopted_process(live)
            self._active_pid = None
            self._active_owned = False
            self._check_stop()
            if not report_path.exists():
                raise SupervisorError(f"adopted arm exited without a report: {arm.name}")
            report = self._validate_arm_report(arm, report_path)
            self._record_arm(
                arm,
                "arm_adopted_live_completed",
                status="completed",
                adoption="exact_live_process",
                report_path=str(report_path),
                report_sha256=_sha256_bytes(report_path),
                validator_clean_calls=int(report.get("validator_clean_calls") or 0),
            )
            self.state["active_arm"] = None
            _write_json_atomic(self.state_path, self.state)
            return

        if prior_status in {"running", "failed", "interrupted", "completed"}:
            raise SupervisorError(
                f"arm {arm.name} has state {prior_status} without a validated report or exact live process; "
                "automatic rerun is prohibited"
            )
        if arm.name in self.spec.get("retrospective_adoptions", []):
            raise SupervisorError(
                f"retrospective arm is missing both a completed report and an exact live process: {arm.name}"
            )

        self._static_preflight()
        self._prepare_output_dir_for_launch(arm)
        self._probe_instruction_contract()
        self._check_stop()
        if self._assert_single_expected_process(arm) is not None:
            raise SupervisorError("core-arm process appeared during prospective preflight")
        try:
            self._launch_and_wait(arm)
        except SupervisorStopped:
            self._record_arm(
                arm,
                "arm_interrupted",
                status="interrupted",
                automatic_retry_prohibited=True,
            )
            raise
        except BaseException as exc:
            self._record_arm(
                arm,
                "arm_failed",
                status="failed",
                error_class=type(exc).__name__,
                automatic_retry_prohibited=True,
            )
            raise
        self._check_stop()
        if not report_path.exists():
            raise SupervisorError(f"supervisor-launched arm exited without a report: {arm.name}")
        report = self._validate_arm_report(arm, report_path)
        self._record_arm(
            arm,
            "arm_launched_completed",
            status="completed",
            adoption="supervisor_launched",
            report_path=str(report_path),
            report_sha256=_sha256_bytes(report_path),
            validator_clean_calls=int(report.get("validator_clean_calls") or 0),
        )
        self.state["active_arm"] = None
        _write_json_atomic(self.state_path, self.state)

    def _assert_no_unowned_partial_files(self, arm: Arm, *, permit_live: bool) -> None:
        arm_dir = self._arm_dir(arm)
        if not arm_dir.exists():
            if permit_live:
                raise SupervisorError("live arm process has not created its output directory")
            return
        entries = list(arm_dir.iterdir())
        if not entries:
            return
        if permit_live:
            return
        raise SupervisorError(f"partial or orphaned arm output must not be rerun: {arm.name}")

    def _prepare_output_dir_for_launch(self, arm: Arm) -> None:
        arm_dir = self._arm_dir(arm)
        guard_path = arm_dir / "private-mapping.json"
        if guard_path.exists():
            guard = _read_json(guard_path)
            if guard != _HANDOFF_GUARD:
                raise SupervisorError(f"partial or unknown private mapping blocks arm: {arm.name}")
            other_entries = [path for path in arm_dir.iterdir() if path != guard_path]
            if other_entries:
                raise SupervisorError("handoff guard directory contains additional partial artifacts")
            guard_path.unlink()
            self._record_arm(
                arm,
                "handoff_guard_removed",
                status="preflight",
                handoff_guard_removed=True,
            )
        self._assert_no_unowned_partial_files(arm, permit_live=False)

    def _wait_for_adopted_process(self, live: ParsedArmProcess) -> None:
        last_heartbeat = self.monotonic()
        while True:
            self._check_stop(forward_if_active=True)
            matching_pid = None
            for record in self.process_reader():
                if record.pid == live.record.pid:
                    parsed = self._parse_arm_process(record)
                    if parsed is not None and parsed.arm == live.arm:
                        matching_pid = parsed
                    break
            if matching_pid is None:
                restarted = self._target_processes()
                if restarted:
                    raise SupervisorError("adopted arm process was replaced or restarted")
                return
            now = self.monotonic()
            if now - last_heartbeat >= self.heartbeat_seconds:
                self._record_arm(
                    live.arm,
                    "arm_heartbeat",
                    status="running",
                    pid=live.record.pid,
                    ownership="adopted",
                )
                last_heartbeat = now
            time.sleep(self.poll_seconds)

    def _launch_and_wait(self, arm: Arm) -> None:
        command = self._command(arm)
        log_root = self.control_root / "logs"
        log_root.mkdir(parents=True, exist_ok=True)
        stdout_path = log_root / f"{arm.name}.stdout.log"
        stderr_path = log_root / f"{arm.name}.stderr.log"
        env = dict(os.environ)
        env.pop("OPENAI_API_KEY", None)
        env["PYTHONUNBUFFERED"] = "1"
        with stdout_path.open("ab", buffering=0) as stdout_handle, stderr_path.open(
            "ab", buffering=0
        ) as stderr_handle:
            process = self.popen_factory(
                command,
                cwd=str(self.repo_root),
                env=env,
                stdout=stdout_handle,
                stderr=stderr_handle,
                start_new_session=True,
            )
            self._active_pid = int(process.pid)
            self._active_owned = True
            self._forwarded_interrupt = False
            self._record_arm(
                arm,
                "arm_launched",
                status="running",
                adoption="supervisor_launched",
                pid=int(process.pid),
                command_sha256=hashlib.sha256(
                    "\0".join(command).encode("utf-8")
                ).hexdigest(),
                stdout_log=str(stdout_path),
                stderr_log=str(stderr_path),
            )
            last_heartbeat = self.monotonic()
            return_code: Optional[int] = None
            while return_code is None:
                self._check_stop(forward_if_active=True)
                try:
                    return_code = process.wait(timeout=self.poll_seconds)
                except subprocess.TimeoutExpired:
                    return_code = None
                now = self.monotonic()
                if return_code is None and now - last_heartbeat >= self.heartbeat_seconds:
                    self._record_arm(
                        arm,
                        "arm_heartbeat",
                        status="running",
                        pid=int(process.pid),
                        ownership="supervisor_launched",
                    )
                    last_heartbeat = now
            self._active_pid = None
            self._active_owned = False
            if self._stop_requested:
                raise SupervisorStopped(self._stop_reason or "stop requested")
            if return_code != 0:
                raise SupervisorError(
                    f"supervisor-launched arm exited nonzero ({return_code}): {arm.name}"
                )

    def _validate_arm_report(self, arm: Arm, path: Path) -> dict[str, Any]:
        report = _read_json(path)
        candidate = self._candidate()
        expected = {
            "schema_version": APP_SERVER_CORE_ARM_VERSION,
            "manifest_sha256": self.spec["manifest"]["artifact_sha256"],
            "core_instructions_sha256": candidate["core_instructions_sha256"],
            "episode_base_instructions_set_sha256": candidate[
                "episode_base_instructions_set_sha256"
            ],
            "guideline_artifact_sha256": self.spec["frozen_artifacts"][
                "candidate_guideline"
            ]["artifact_sha256"],
            "output_schema_version": candidate["output_schema_version"],
            "transport_client_version": self.spec["transport"]["client_version"],
            "model": candidate["model"],
            "reasoning_effort": candidate["reasoning_effort"],
            "batch_size_ceiling": arm.batch_size,
            "thread_mode": arm.thread_mode,
            "concurrency": candidate["concurrency"],
            "retry_count": 0,
            "window_count": candidate["window_count"],
            "context_chars": candidate["context_chars"],
            "max_events_per_segment": candidate["event_cap"],
            "requested_segments": self.spec["manifest"]["segment_count"],
            "accounting_complete": True,
            "usage_status": "complete",
            "semantic_quality_status": (
                "pending_calibrated_llm_support_and_alignment_adjudication"
            ),
            "selection_eligible": False,
        }
        for key, expected_value in expected.items():
            if report.get(key) != expected_value:
                raise SupervisorError(f"arm report {arm.name} has drifted field: {key}")
        requested_calls = report.get("requested_calls")
        if (
            not isinstance(requested_calls, int)
            or requested_calls < 1
            or report.get("attempted_calls") != requested_calls
            or report.get("terminal_sidecars") != requested_calls
            or report.get("usage_measured_attempts") != requested_calls
            or report.get("usage_unknown_attempts") != 0
        ):
            raise SupervisorError(f"arm report {arm.name} has incomplete call accounting")
        usage = report.get("usage")
        if not isinstance(usage, dict):
            raise SupervisorError(f"arm report {arm.name} has no complete usage object")
        for field in _REQUIRED_USAGE_FIELDS:
            value = usage.get(field)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise SupervisorError(f"arm report {arm.name} has invalid usage field: {field}")
        source_sets = report.get("instruction_source_sets")
        instruction = self._verify_instruction_contract(freeze_if_missing=False)
        if not isinstance(source_sets, list) or len(source_sets) != 1:
            raise SupervisorError(f"arm report {arm.name} has ambiguous instruction sources")
        source_set = source_sets[0]
        if (
            not isinstance(source_set, dict)
            or source_set.get("instruction_sources_sha256")
            != instruction["expected_path_set_sha256"]
            or source_set.get("instruction_sources_count") != instruction["source_count"]
            or not isinstance(source_set.get("thread_count"), int)
            or int(source_set["thread_count"]) < 1
        ):
            raise SupervisorError(f"arm report {arm.name} instruction source set drift")
        return report

    def _aggregate_or_adopt(self, arms: Sequence[Arm]) -> dict[str, Any]:
        reports = [self._arm_report_path(arm) for arm in arms]
        if self.matrix_report_path.exists():
            matrix = _read_json(self.matrix_report_path)
            if matrix.get("schema_version") != APP_SERVER_CORE_MATRIX_VERSION:
                raise SupervisorError("existing matrix report has an unsupported schema")
            embedded = matrix.get("arms")
            expected_reports = [_read_json(path) for path in reports]
            expected_reports.sort(
                key=lambda item: (int(item["batch_size_ceiling"]), str(item["thread_mode"]))
            )
            if embedded != expected_reports:
                raise SupervisorError("existing matrix report differs from validated arm reports")
            return matrix
        try:
            return aggregate_app_server_development_matrix(
                arm_report_paths=[str(path) for path in reports],
                output_path=self.matrix_report_path,
            )
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise SupervisorError("matrix aggregation failed closed") from exc

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

    def _check_stop(self, *, forward_if_active: bool = False) -> None:
        if self.stop_path.exists() and not self._stop_requested:
            self._stop_requested = True
            self._stop_reason = "stop_sentinel"
        if not self._stop_requested:
            return
        if forward_if_active:
            self._forward_interrupt(signal.SIGINT, self._stop_reason or "stop_requested")
        if self._active_pid is None:
            raise SupervisorStopped(self._stop_reason or "stop requested")

    def _forward_interrupt(self, signum: int, reason: str) -> None:
        if self._active_pid is None or self._forwarded_interrupt:
            return
        self._forwarded_interrupt = True
        try:
            if self._active_owned:
                os.killpg(self._active_pid, signum)
            else:
                os.kill(self._active_pid, signum)
        except ProcessLookupError:
            pass
        self._record(
            "active_arm_interrupt_forwarded",
            forwarded_signal=int(signum),
            forwarded_reason=reason,
            forwarded_pid=self._active_pid,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run or adopt the frozen six-arm official Codex app-server development matrix "
            "without retries or paid API-key billing."
        )
    )
    parser.add_argument(
        "--run-spec",
        default="work/app-server-development-v2/run-spec-v3.json",
    )
    parser.add_argument("--matrix-root")
    parser.add_argument("--repo-root", default=".")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    supervisor = UnattendedAppServerMatrixSupervisor(
        repo_root=Path(args.repo_root),
        run_spec_path=Path(args.run_spec),
        matrix_root=Path(args.matrix_root) if args.matrix_root else None,
    )
    try:
        matrix = supervisor.run()
    except SupervisorStopped as exc:
        print(
            json.dumps(
                {"ok": False, "status": "stopped", "error_class": type(exc).__name__},
                sort_keys=True,
            )
        )
        return 130
    except (SupervisorError, OSError, subprocess.SubprocessError) as exc:
        print(
            json.dumps(
                {"ok": False, "status": "failed", "error_class": type(exc).__name__},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    print(
        json.dumps(
            {
                "ok": True,
                "status": "completed",
                "matrix_report": str(supervisor.matrix_report_path),
                "arm_count": matrix.get("arm_count"),
                "selection_eligible": bool(matrix.get("selection_eligible")),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
