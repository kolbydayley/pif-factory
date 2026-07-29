from __future__ import annotations

import argparse
import asyncio
import fcntl
import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, Sequence

from .app_server_capacity import CapacityGatedCodexAppServerClient
from .app_server_evaluation import run_app_server_core_arm
from .codex_app_server import CodexAppServerClient
from .efficient_backtest import DEFAULT_WINDOWED_GUIDELINES_PATH
from .paths import db_path
from .util import now_iso, sha256_text, write_text_atomic


RECOVERY_VERSION = "pif_app_server_interrupted_arm_recovery_v1"
RECOVERY_PLAN_VERSION = "pif_app_server_interrupted_arm_recovery_plan_v2"
RECOVERY_OUTCOME_VERSION = "pif_app_server_interrupted_batch_outcome_v1"
RECOVERY_REPORT_VERSION = "pif_app_server_interrupted_arm_recovery_report_v1"
CONSOLIDATED_ARM_VERSION = "pif_app_server_interrupted_arm_consolidated_v1"
RECOVERED_MATRIX_VERSION = "pif_app_server_recovered_development_matrix_v1"

_USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)


class RecoveryError(RuntimeError):
    """The interrupted arm cannot be recovered without violating no-rerun policy."""


class RecoveryStopped(RecoveryError):
    pass


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path, purpose: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RecoveryError(f"invalid {purpose} JSON") from exc
    if not isinstance(payload, dict):
        raise RecoveryError(f"{purpose} is not a JSON object")
    return payload


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    write_text_atomic(
        path,
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )


def _write_immutable(path: Path, payload: dict[str, Any]) -> None:
    rendered = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != rendered:
            raise RecoveryError("immutable recovery artifact already exists with different content")
        return
    write_text_atomic(path, rendered)


def _append_journal(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        if os.write(descriptor, encoded) != len(encoded):
            raise RecoveryError("short recovery journal write")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _canonical_sha(value: Any) -> str:
    return sha256_text(
        json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    )


def _terminal_sidecar_usage(sidecar: dict[str, Any]) -> tuple[Optional[dict[str, int]], bool]:
    usage = sidecar.get("usage")
    complete = sidecar.get("usage_complete") is True
    if not complete or not isinstance(usage, dict):
        return None, False
    normalized = {}
    for field in _USAGE_FIELDS:
        value = usage.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            return None, False
        normalized[field] = value
    return normalized, True


def _output_hash_matches(path: Path, expected: Any) -> bool:
    if not isinstance(expected, str):
        return False
    text = path.read_text(encoding="utf-8")
    candidates = {sha256_text(text)}
    if text.endswith("\n"):
        candidates.add(sha256_text(text[:-1]))
    return expected in candidates


def finalize_operator_interrupted_sidecar(
    sidecar_path: Path,
    *,
    interruption_evidence_sha256: str,
    finished_at: Optional[str] = None,
) -> dict[str, Any]:
    """Finalize only a provably interrupted in-progress sidecar, without model work."""

    sidecar = _read_json(sidecar_path, "turn sidecar")
    if sidecar.get("state") in {"cancelled", "interrupted"}:
        if (
            sidecar.get("usage") is not None
            or sidecar.get("usage_complete") is not False
            or sidecar.get("usage_status") != "unknown"
            or sidecar.get("recovery_reran_model") is not False
        ):
            raise RecoveryError("terminal interrupted sidecar does not preserve unknown usage")
        return sidecar
    if (
        sidecar.get("state") != "in_progress"
        or sidecar.get("usage") is not None
        or sidecar.get("usage_complete") not in {None, False}
        or not isinstance(sidecar.get("turn_id"), str)
    ):
        raise RecoveryError("sidecar is not eligible for operator-interruption finalization")
    original_sha = _sha256_file(sidecar_path)
    finalized = {
        **sidecar,
        "state": "cancelled",
        "status": "cancelled",
        "error_class": "operator_cancelled",
        "finished_at": finished_at or now_iso(),
        "usage": None,
        "usage_complete": False,
        "usage_status": "unknown",
        "thread_total_usage": None,
        "interrupt_attempted": True,
        "protocol_interrupt_sent": False,
        "interrupt_acknowledged": None,
        "interruption_method": "supervisor_stop_sentinel_operator_recovery",
        "recovery_reran_model": False,
        "pre_recovery_sidecar_sha256": original_sha,
        "interruption_evidence_sha256": interruption_evidence_sha256,
    }
    _write_json_atomic(sidecar_path, finalized)
    return finalized


async def probe_app_server_rate_limits(
    *,
    client_factory: Callable[[], CodexAppServerClient] = CodexAppServerClient,
    maximum_primary_used_percent: int = 20,
) -> dict[str, Any]:
    """Read managed-ChatGPT Codex limits without any thread or model turn."""

    async with client_factory() as client:
        account = client.account_summary
        if not isinstance(account, dict) or account.get("type") != "chatgpt":
            raise RecoveryError("rate-limit probe requires managed ChatGPT auth")
        response = await client._request("account/rateLimits/read", {})  # noqa: SLF001
    if not isinstance(response, dict):
        raise RecoveryError("app-server rate-limit response is malformed")
    snapshot = None
    by_id = response.get("rateLimitsByLimitId")
    if isinstance(by_id, dict):
        direct = by_id.get("codex")
        if isinstance(direct, dict):
            snapshot = direct
        if snapshot is None:
            for value in by_id.values():
                if isinstance(value, dict) and value.get("limitId") == "codex":
                    snapshot = value
                    break
    if snapshot is None and isinstance(response.get("rateLimits"), dict):
        snapshot = response["rateLimits"]
    if not isinstance(snapshot, dict):
        raise RecoveryError("app-server returned no Codex rate-limit snapshot")
    primary = snapshot.get("primary")
    if not isinstance(primary, dict):
        raise RecoveryError("Codex rate-limit snapshot has no primary window")
    used_percent = primary.get("usedPercent")
    resets_at = primary.get("resetsAt")
    if isinstance(used_percent, bool) or not isinstance(used_percent, int):
        raise RecoveryError("Codex primary rate-limit percentage is malformed")
    if resets_at is not None and (
        isinstance(resets_at, bool) or not isinstance(resets_at, int)
    ):
        raise RecoveryError("Codex primary reset timestamp is malformed")
    reached_type = snapshot.get("rateLimitReachedType")
    cleared = bool(
        reached_type is None and used_percent <= int(maximum_primary_used_percent)
    )
    return {
        "schema_version": "pif_app_server_rate_limit_probe_v1",
        "managed_chatgpt_auth_verified": True,
        "plan_type": account.get("plan_type"),
        "limit_id": snapshot.get("limitId") or "codex",
        "primary_used_percent": used_percent,
        "primary_resets_at": resets_at,
        "rate_limit_reached_type": reached_type,
        "maximum_primary_used_percent": int(maximum_primary_used_percent),
        "cleared_for_semantic_work": cleared,
        "thread_started": False,
        "turn_started": False,
        "privacy": "managed_auth_plan_limit_percent_reset_and_status_no_credentials",
    }


def _sync_rate_limit_probe(
    *,
    client_factory: Callable[[], CodexAppServerClient],
    maximum_primary_used_percent: int,
) -> dict[str, Any]:
    return asyncio.run(
        probe_app_server_rate_limits(
            client_factory=client_factory,
            maximum_primary_used_percent=maximum_primary_used_percent,
        )
    )


def _read_only_connection() -> sqlite3.Connection:
    resolved = db_path().expanduser().resolve()
    if not resolved.is_file():
        raise RecoveryError(f"production database is missing: {resolved}")
    conn = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def inspect_batch_inventory(
    *, mapping: dict[str, Any], arm_dir: Path
) -> list[dict[str, Any]]:
    """Classify every frozen batch without semantic inference or reruns."""

    inventory = []
    seen = set()
    for position, batch in enumerate(mapping.get("batches") or []):
        batch_id = str(batch.get("batch_id") or "")
        if not batch_id or batch_id in seen:
            raise RecoveryError("private mapping has missing or duplicate batch IDs")
        seen.add(batch_id)
        sidecar_path = Path(str(batch["sidecar_path"])).expanduser().resolve()
        raw_path = Path(str(batch["raw_output_path"])).expanduser().resolve()
        normalized_path = Path(str(batch["normalized_output_path"])).expanduser().resolve()
        if sidecar_path.parent.parent != arm_dir.resolve():
            raise RecoveryError("mapped sidecar escapes the interrupted arm directory")
        record = {
            "position": position,
            "batch_id": batch_id,
            "episode_id": str(batch["episode_id"]),
            "segment_ids": [str(item) for item in batch.get("segment_ids") or []],
            "sidecar_path": str(sidecar_path),
            "raw_output_path": str(raw_path),
            "normalized_output_path": str(normalized_path),
        }
        if sidecar_path.exists():
            sidecar = _read_json(sidecar_path, "mapped sidecar")
            state = sidecar.get("state")
            usage, usage_complete = _terminal_sidecar_usage(sidecar)
            if state == "completed":
                if not usage_complete or not raw_path.is_file() or not normalized_path.is_file():
                    raise RecoveryError("completed batch is missing measured usage or output artifacts")
                declared_output = sidecar.get("output_sha256")
                if not _output_hash_matches(raw_path, declared_output):
                    raise RecoveryError("completed batch raw output hash drift")
                record.update(
                    {
                        "status": "completed_adopted",
                        "usage": usage,
                        "usage_complete": True,
                        "sidecar_sha256": _sha256_file(sidecar_path),
                        "raw_output_sha256": _sha256_file(raw_path),
                        "normalized_output_sha256": _sha256_file(normalized_path),
                    }
                )
            elif state in {"cancelled", "interrupted"}:
                if usage_complete or sidecar.get("usage") is not None:
                    raise RecoveryError("interrupted batch unexpectedly has asserted complete usage")
                if raw_path.exists() or normalized_path.exists():
                    raise RecoveryError("interrupted batch has ambiguous output artifacts")
                record.update(
                    {
                        "status": "operator_interrupted_usage_unknown",
                        "usage": None,
                        "usage_complete": False,
                        "sidecar_sha256": _sha256_file(sidecar_path),
                        "turn_start_observed": bool(sidecar.get("turn_id")),
                        "model_usage_observed": False,
                        "intent_to_treat_failure": True,
                    }
                )
            elif state == "in_progress":
                record.update(
                    {
                        "status": "in_progress_requires_finalization",
                        "usage": None,
                        "usage_complete": False,
                        "sidecar_sha256": _sha256_file(sidecar_path),
                    }
                )
            else:
                raise RecoveryError("mapped sidecar has an unsupported recovery state")
        else:
            if raw_path.exists() or normalized_path.exists():
                raise RecoveryError("batch has output artifacts but no attempt sidecar")
            record.update(
                {
                    "status": "unstarted",
                    "usage": None,
                    "usage_complete": False,
                }
            )
        inventory.append(record)
    if not inventory:
        raise RecoveryError("private mapping contains no batches")
    return inventory


class InterruptedArmRecovery:
    def __init__(
        self,
        *,
        repo_root: Path,
        run_spec_path: Path,
        matrix_root: Path,
        not_before_epoch: int,
        boundary_grace_seconds: int = 30,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
        client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
        full_arm_runner: Callable[..., Any] = run_app_server_core_arm,
        rate_limit_probe: Callable[..., dict[str, Any]] = _sync_rate_limit_probe,
        async_rate_limit_probe: Callable[..., Awaitable[dict[str, Any]]] = (
            probe_app_server_rate_limits
        ),
        async_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        maximum_primary_used_percent: int = 20,
        rate_limit_recheck_seconds: int = 300,
    ):
        self.repo_root = repo_root.expanduser().resolve()
        self.run_spec_path = run_spec_path.expanduser().resolve()
        self.matrix_root = matrix_root.expanduser().resolve()
        self.arm_dir = self.matrix_root / "batch-5" / "same_thread"
        self.recovery_root = self.matrix_root / "recovery-batch-5-same-v1"
        self.plan_path = self.recovery_root / "plan-v2.json"
        self.state_path = self.recovery_root / "state.json"
        self.journal_path = self.recovery_root / "journal.jsonl"
        self.lock_path = self.recovery_root / "recovery.lock"
        self.stop_path = self.recovery_root / "STOP"
        self.report_path = self.recovery_root / "recovery-report.json"
        self.consolidated_path = self.recovery_root / "consolidated-arm-report.json"
        self.itt_provenance_path = self.arm_dir / "intent-to-treat-interruption-v1.json"
        self.matrix_report_path = self.matrix_root / "recovered-matrix-report.json"
        self.rate_limit_clearance_path = self.recovery_root / "rate-limit-clearance.json"
        self.not_before_epoch = int(not_before_epoch)
        self.boundary_epoch = int(not_before_epoch) + int(boundary_grace_seconds)
        self.clock = clock
        self.sleep = sleep
        self.client_factory = client_factory
        self.full_arm_runner = full_arm_runner
        self.rate_limit_probe = rate_limit_probe
        self.async_rate_limit_probe = async_rate_limit_probe
        self.async_sleep = async_sleep
        self.maximum_primary_used_percent = int(maximum_primary_used_percent)
        self.rate_limit_recheck_seconds = max(30, int(rate_limit_recheck_seconds))
        self.spec = _read_json(self.run_spec_path, "recovery run spec")
        self.state: dict[str, Any] = {}
        self._stop_requested = False
        self._previous_handlers: dict[int, Any] = {}

    def run(self) -> dict[str, Any]:
        os.environ.pop("OPENAI_API_KEY", None)
        self.recovery_root.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RecoveryError("another interrupted-arm recovery owns the lifetime lock") from exc
            self._install_signals()
            try:
                self._load_state()
                mapping, inventory = self._preflight_and_inventory()
                self._write_plan(mapping, inventory)
                self._write_intent_to_treat_provenance(mapping, inventory)
                self._wait_for_boundary()
                self._wait_for_live_rate_limit_clear()
                mapping, inventory = self._preflight_and_inventory()
                report, consolidated = self._build_recovery_reports(mapping, inventory)
                asyncio.run(self._run_clean_batch8_arms())
                matrix = self._build_recovered_matrix(consolidated)
                self._record(
                    "recovery_completed",
                    status="completed",
                    recovered_matrix_path=str(self.matrix_report_path),
                    recovered_matrix_sha256=_sha256_file(self.matrix_report_path),
                )
                return {
                    "status": "completed",
                    "recovery_report": report,
                    "consolidated_arm": consolidated,
                    "matrix": matrix,
                }
            except RecoveryStopped:
                self._record("recovery_stopped", status="stopped")
                raise
            except BaseException as exc:
                self._record(
                    "recovery_failed_closed",
                    status="failed",
                    error_class=type(exc).__name__,
                )
                raise
            finally:
                self._restore_signals()

    def _load_state(self) -> None:
        if self.state_path.exists():
            state = _read_json(self.state_path, "recovery state")
            if state.get("schema_version") != RECOVERY_VERSION:
                raise RecoveryError("unsupported recovery state version")
            if state.get("boundary_epoch") != self.boundary_epoch:
                raise RecoveryError("recovery boundary changed after initialization")
            if (
                state.get("maximum_primary_used_percent") is None
                and state.get("status") == "initializing"
            ):
                state["maximum_primary_used_percent"] = self.maximum_primary_used_percent
                _write_json_atomic(self.state_path, state)
            elif state.get("maximum_primary_used_percent") != self.maximum_primary_used_percent:
                raise RecoveryError("live rate-limit clearance threshold changed after initialization")
            self.state = state
            return
        self.state = {
            "schema_version": RECOVERY_VERSION,
            "created_at": now_iso(),
            "updated_at": now_iso(),
            "status": "initializing",
            "not_before_epoch": self.not_before_epoch,
            "boundary_epoch": self.boundary_epoch,
            "maximum_primary_used_percent": self.maximum_primary_used_percent,
            "rerun_completed_or_interrupted_turns_allowed": False,
            "selection_eligible": False,
            "production_mutation_allowed": False,
        }
        _write_json_atomic(self.state_path, self.state)

    def _record(self, event: str, **changes: Any) -> None:
        timestamp = now_iso()
        _append_journal(
            self.journal_path,
            {
                "schema_version": RECOVERY_VERSION,
                "at": timestamp,
                "event": event,
                **changes,
                "privacy": "hashes_counts_batch_ids_status_and_failure_classes_only",
            },
        )
        self.state.update(changes)
        self.state["updated_at"] = timestamp
        _write_json_atomic(self.state_path, self.state)

    def _resolve(self, raw: str) -> Path:
        path = Path(raw).expanduser()
        return path.resolve() if path.is_absolute() else (self.repo_root / path).resolve()

    def _verify_contract(self, contract: dict[str, Any], purpose: str) -> Path:
        path = self._resolve(str(contract.get("artifact_path") or ""))
        expected = contract.get("artifact_sha256")
        if not path.is_file() or not isinstance(expected, str) or _sha256_file(path) != expected:
            raise RecoveryError(f"frozen {purpose} artifact drift")
        return path

    def _preflight_and_inventory(self) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if self.spec.get("schema_version") != "pif_app_server_development_run_spec_v2":
            raise RecoveryError("recovery requires frozen run spec v2")
        if _sha256_file(self.run_spec_path) != (
            _read_json(
                self.matrix_root / "supervisor" / "state.json", "matrix supervisor state"
            ).get("run_spec_sha256")
        ):
            raise RecoveryError("matrix supervisor did not run the supplied frozen spec")
        self._verify_contract(self.spec["parent_run_spec"], "parent run spec")
        for label in ("manifest", "shared_reference_seed", "reference_noise"):
            self._verify_contract(self.spec[label], label)
        for label, contract in self.spec.get("frozen_artifacts", {}).items():
            self._verify_contract(contract, str(label))
        instruction_contract = self.spec.get("instruction_contract") or {}
        paths = []
        for source in instruction_contract.get("sources") or []:
            path = Path(str(source["path"])).expanduser()
            if (
                not path.is_file()
                or _sha256_file(path) != source.get("content_sha256")
                or path.stat().st_size != source.get("size_bytes")
            ):
                raise RecoveryError("frozen instruction content drift")
            paths.append(str(path))
        if _canonical_sha(paths) != instruction_contract.get("expected_path_set_sha256"):
            raise RecoveryError("frozen instruction path set drift")

        supervisor_root = self.matrix_root / "supervisor"
        supervisor_state_path = supervisor_root / "state.json"
        supervisor_journal_path = supervisor_root / "journal.jsonl"
        supervisor_state = _read_json(supervisor_state_path, "matrix supervisor state")
        arm_state = (supervisor_state.get("arms") or {}).get("batch_5_same_thread") or {}
        if (
            supervisor_state.get("status") != "stopped"
            or supervisor_state.get("stop_reason") != "stop_sentinel"
            or arm_state.get("status") != "interrupted"
            or arm_state.get("automatic_retry_prohibited") is not True
        ):
            raise RecoveryError("matrix supervisor state does not prove an operator interruption")
        events = []
        try:
            for line in supervisor_journal_path.read_text(encoding="utf-8").splitlines():
                item = json.loads(line)
                if isinstance(item, dict):
                    events.append(item)
        except (OSError, json.JSONDecodeError) as exc:
            raise RecoveryError("matrix supervisor journal is unreadable") from exc
        event_names = [item.get("event") for item in events]
        required_events = (
            "arm_launched",
            "active_arm_interrupt_forwarded",
            "arm_interrupted",
            "supervisor_stopped",
        )
        if not all(name in event_names for name in required_events):
            raise RecoveryError("matrix supervisor journal lacks interruption provenance")
        evidence_sha = _canonical_sha(
            {
                "state_sha256": _sha256_file(supervisor_state_path),
                "journal_sha256": _sha256_file(supervisor_journal_path),
                "events": required_events,
            }
        )
        mapping_path = self.arm_dir / "private-mapping.json"
        mapping = _read_json(mapping_path, "interrupted arm private mapping")
        if (
            mapping.get("schema_version") != "pif_app_server_core_arm_v3"
            or mapping.get("batch_size") != 5
            or mapping.get("thread_mode") != "same_thread"
            or mapping.get("manifest_sha256") != self.spec["manifest"]["artifact_sha256"]
        ):
            raise RecoveryError("interrupted arm private mapping drift")
        inventory = inspect_batch_inventory(mapping=mapping, arm_dir=self.arm_dir)
        in_progress = [item for item in inventory if item["status"] == "in_progress_requires_finalization"]
        if len(in_progress) > 1:
            raise RecoveryError("more than one in-progress sidecar cannot be recovered automatically")
        if in_progress:
            item = in_progress[0]
            raw = Path(item["raw_output_path"])
            normalized = Path(item["normalized_output_path"])
            if raw.exists() or normalized.exists():
                raise RecoveryError("in-progress sidecar has ambiguous output evidence")
            finalize_operator_interrupted_sidecar(
                Path(item["sidecar_path"]), interruption_evidence_sha256=evidence_sha
            )
            inventory = inspect_batch_inventory(mapping=mapping, arm_dir=self.arm_dir)
        statuses = Counter(item["status"] for item in inventory)
        if statuses["operator_interrupted_usage_unknown"] != 1:
            raise RecoveryError("recovery requires exactly one operator-interrupted batch")
        if statuses["completed_adopted"] < 1:
            raise RecoveryError("recovery found no completed batches to adopt")
        self._assert_no_live_core_process()
        return mapping, inventory

    def _assert_no_live_core_process(self) -> None:
        completed = subprocess.run(
            ["ps", "-ww", "-axo", "command="],
            check=True,
            capture_output=True,
            text=True,
        )
        for line in completed.stdout.splitlines():
            if (
                "-m research_factory efficiency-app-server-core-arm" in line
                and str(self.spec["manifest"]["artifact_path"]) in line
            ):
                raise RecoveryError("a legacy core-arm process is still live")

    def _write_plan(self, mapping: dict[str, Any], inventory: list[dict[str, Any]]) -> None:
        plan = {
            "schema_version": RECOVERY_PLAN_VERSION,
            "created_at": now_iso(),
            "run_spec_path": str(self.run_spec_path),
            "run_spec_sha256": _sha256_file(self.run_spec_path),
            "mapping_path": str(self.arm_dir / "private-mapping.json"),
            "mapping_sha256": _sha256_file(self.arm_dir / "private-mapping.json"),
            "not_before_epoch": self.not_before_epoch,
            "boundary_epoch": self.boundary_epoch,
            "batch_statuses": [
                {
                    "position": item["position"],
                    "batch_id": item["batch_id"],
                    "status": item["status"],
                    "segment_count": len(item["segment_ids"]),
                }
                for item in inventory
            ],
            "policy": {
                "rerun_completed_batch": False,
                "rerun_interrupted_batch": False,
                "run_unstarted_batches": False,
                "skip_all_remaining_interrupted_arm_batches": True,
                "interrupted_batch_intent_to_treat_failure": True,
                "interrupted_usage_accounting": "unknown_fail_closed",
                "selection_eligible": False,
            },
            "remaining_clean_arms_after_boundary": [
                "batch_8_new_thread",
                "batch_8_same_thread",
            ],
            "live_rate_limit_gate": {
                "method": "account/rateLimits/read",
                "managed_chatgpt_auth_required": True,
                "thread_or_turn_started": False,
                "maximum_primary_used_percent": self.maximum_primary_used_percent,
                "wait_and_recheck_if_not_clear": True,
            },
            "privacy": "batch_ids_counts_hashes_paths_and_status_only_no_prompt_or_output_text",
        }
        if self.plan_path.exists():
            existing = _read_json(self.plan_path, "recovery plan")
            comparable_existing = dict(existing)
            comparable_plan = dict(plan)
            comparable_existing.pop("created_at", None)
            comparable_plan.pop("created_at", None)
            if comparable_existing != comparable_plan:
                raise RecoveryError("recovery plan drift")
            return
        _write_immutable(self.plan_path, plan)

    def _write_intent_to_treat_provenance(
        self, mapping: dict[str, Any], inventory: list[dict[str, Any]]
    ) -> None:
        completed = [item for item in inventory if item["status"] == "completed_adopted"]
        cancelled = [
            item for item in inventory if item["status"] == "operator_interrupted_usage_unknown"
        ]
        unstarted = [item for item in inventory if item["status"] == "unstarted"]
        if len(completed) != 4 or len(cancelled) != 1 or len(unstarted) != 3:
            raise RecoveryError("ITT provenance requires the exact 4 completed, 1 cancelled, 3 unstarted split")
        measured = Counter()
        for item in completed:
            measured.update(item["usage"])
        payload = {
            "schema_version": "pif_app_server_arm_intent_to_treat_interruption_v1",
            "classification": "terminal_interrupted_accounting_incomplete",
            "created_at": now_iso(),
            "arm": {
                "batch_size": 5,
                "thread_mode": "same_thread",
                "concurrency": 1,
                "retry_count": 0,
                "model": self.spec["candidate"]["model"],
                "reasoning_effort": self.spec["candidate"]["reasoning_effort"],
            },
            "automatic_retry_prohibited": True,
            "selection_eligible": False,
            "semantic_quality_score": 0,
            "usage_status": "partial_unknown",
            "usage": None,
            "measured_partial_usage": {
                field: int(measured[field]) for field in _USAGE_FIELDS
            },
            "planned_calls": 8,
            "attempted_calls": 5,
            "completed_measured_calls": 4,
            "cancelled_unknown_usage_calls": 1,
            "not_started_calls": 3,
            "planned_segments": 32,
            "validated_segments": 16,
            "interrupted_batch_id": cancelled[0]["batch_id"],
            "not_started_batch_ids": [item["batch_id"] for item in unstarted],
            "artifacts": {
                "private_mapping_sha256": _sha256_file(
                    self.arm_dir / "private-mapping.json"
                ),
                "completed_sidecar_sha256s": sorted(
                    item["sidecar_sha256"] for item in completed
                ),
                "cancelled_sidecar_sha256": cancelled[0]["sidecar_sha256"],
                "supervisor_state_sha256": _sha256_file(
                    self.matrix_root / "supervisor" / "state.json"
                ),
                "supervisor_journal_sha256": _sha256_file(
                    self.matrix_root / "supervisor" / "journal.jsonl"
                ),
            },
            "recovery_policy": (
                "completed and cancelled original attempts are never rerun; the never-started "
                "batch-5 same-thread calls are permanently skipped and receive no model work"
            ),
            "production_database_mutation": False,
            "production_promotion": False,
            "privacy": "batch_ids_hashes_counts_usage_and_status_no_prompt_or_output_text",
        }
        if self.itt_provenance_path.exists():
            from .app_server_dev_selection import load_interrupted_arm_provenance

            manifest = _read_json(
                self._resolve(str(self.spec["manifest"]["artifact_path"])),
                "development manifest",
            )
            manifest_rows = [
                segment
                for episode in manifest.get("episodes") or []
                for segment in episode.get("segments") or []
            ]
            try:
                load_interrupted_arm_provenance(
                    provenance_path=self.itt_provenance_path,
                    manifest_rows=manifest_rows,
                )
            except Exception as exc:
                raise RecoveryError("existing ITT interruption provenance drift") from exc
            return
        _write_immutable(self.itt_provenance_path, payload)

    def _wait_for_boundary(self) -> None:
        while self.clock() < self.boundary_epoch:
            self._check_stop()
            remaining = max(0, int(self.boundary_epoch - self.clock()))
            self._record(
                "waiting_for_quota_reset_boundary",
                status="waiting_for_reset",
                seconds_remaining=remaining,
            )
            self.sleep(min(30.0, max(0.1, float(remaining))))
        self._record("quota_reset_boundary_reached", status="preflighting_clean_batch8")

    def _accept_live_rate_limit_probe(
        self, probe: dict[str, Any]
    ) -> Optional[dict[str, Any]]:
        if not isinstance(probe, dict):
            raise RecoveryError("live no-turn app-server rate-limit probe is malformed")
        if (
            probe.get("managed_chatgpt_auth_verified") is not True
            or probe.get("thread_started") is not False
            or probe.get("turn_started") is not False
        ):
            raise RecoveryError("rate-limit probe violated the no-turn managed-auth contract")
        cleared = probe.get("cleared_for_semantic_work") is True
        self._record(
            "live_rate_limit_probe",
            status=("rate_limit_cleared" if cleared else "waiting_for_live_rate_limit_clear"),
            primary_used_percent=probe.get("primary_used_percent"),
            primary_resets_at=probe.get("primary_resets_at"),
            rate_limit_reached_type=probe.get("rate_limit_reached_type"),
            managed_chatgpt_auth_verified=True,
            thread_started=False,
            turn_started=False,
        )
        if not cleared:
            return None
        artifact = {**probe, "observed_at": now_iso()}
        if self.rate_limit_clearance_path.exists():
            existing = _read_json(self.rate_limit_clearance_path, "rate-limit clearance")
            if existing.get("cleared_for_semantic_work") is not True:
                raise RecoveryError("existing rate-limit clearance is not clear")
            return existing
        _write_immutable(self.rate_limit_clearance_path, artifact)
        return artifact

    def _rate_limit_wait_seconds(self, probe: dict[str, Any]) -> float:
        now = int(self.clock())
        reset_at = probe.get("primary_resets_at")
        wait_seconds = self.rate_limit_recheck_seconds
        if isinstance(reset_at, int) and reset_at > now:
            wait_seconds = min(wait_seconds, max(30, reset_at - now + 30))
        return float(wait_seconds)

    def _wait_for_live_rate_limit_clear(self) -> dict[str, Any]:
        """Synchronous gate used before the clean-arm event loop starts."""

        while True:
            self._check_stop()
            try:
                probe = self.rate_limit_probe(
                    client_factory=self.client_factory,
                    maximum_primary_used_percent=self.maximum_primary_used_percent,
                )
            except Exception as exc:
                raise RecoveryError("live no-turn app-server rate-limit probe failed") from exc
            artifact = self._accept_live_rate_limit_probe(probe)
            if artifact is not None:
                return artifact
            self.sleep(self._rate_limit_wait_seconds(probe))

    async def _wait_for_live_rate_limit_clear_async(self) -> dict[str, Any]:
        """Await the no-turn capacity gate from inside clean-arm orchestration."""

        while True:
            self._check_stop()
            try:
                probe = await self.async_rate_limit_probe(
                    client_factory=self.client_factory,
                    maximum_primary_used_percent=self.maximum_primary_used_percent,
                )
            except Exception as exc:
                raise RecoveryError("live no-turn app-server rate-limit probe failed") from exc
            artifact = self._accept_live_rate_limit_probe(probe)
            if artifact is not None:
                return artifact
            await self.async_sleep(self._rate_limit_wait_seconds(probe))

    async def _recover_unstarted(
        self, mapping: dict[str, Any], inventory: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        del mapping, inventory
        raise RecoveryError(
            "batch-5/same is terminal; unstarted batches are intentionally never launched"
        )

    def _build_recovery_reports(
        self, mapping: dict[str, Any], inventory: list[dict[str, Any]]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        statuses = Counter(item["status"] for item in inventory)
        measured = Counter()
        measured_attempts = 0
        unknown_attempts = 0
        validated_segments = 0
        candidate_events = 0
        instruction_sets = Counter()
        batch_records = []
        for item in inventory:
            sidecar_path = Path(item["sidecar_path"])
            sidecar = _read_json(sidecar_path, "terminal recovery sidecar") if sidecar_path.exists() else None
            usage, usage_complete = (
                _terminal_sidecar_usage(sidecar) if sidecar else (None, False)
            )
            if usage_complete and usage:
                measured.update(usage)
                measured_attempts += 1
            elif sidecar:
                unknown_attempts += 1
            if sidecar and isinstance(sidecar.get("instruction_sources_sha256"), str):
                instruction_sets[
                    (
                        sidecar["instruction_sources_sha256"],
                        int(sidecar.get("instruction_sources_count") or 0),
                    )
                ] += 1
            normalized_path = Path(item["normalized_output_path"])
            if normalized_path.is_file():
                normalized = _read_json(normalized_path, "normalized recovered output")
                segments = normalized.get("segments") or []
                validated_segments += len(segments)
                candidate_events += sum(len(segment.get("events") or []) for segment in segments)
            batch_records.append(
                {
                    "position": item["position"],
                    "batch_id": item["batch_id"],
                    "status": item["status"],
                    "segment_count": len(item["segment_ids"]),
                    "usage_complete": usage_complete,
                    "sidecar_sha256": _sha256_file(sidecar_path) if sidecar_path.exists() else None,
                    "raw_output_sha256": (
                        _sha256_file(Path(item["raw_output_path"]))
                        if Path(item["raw_output_path"]).is_file()
                        else None
                    ),
                    "normalized_output_sha256": (
                        _sha256_file(normalized_path) if normalized_path.is_file() else None
                    ),
                }
            )
        report = {
            "schema_version": RECOVERY_REPORT_VERSION,
            "created_at": now_iso(),
            "plan_path": str(self.plan_path),
            "plan_sha256": _sha256_file(self.plan_path),
            "mapping_path": str(self.arm_dir / "private-mapping.json"),
            "mapping_sha256": _sha256_file(self.arm_dir / "private-mapping.json"),
            "batch_count": len(batch_records),
            "batch_status_counts": dict(sorted(statuses.items())),
            "batches": batch_records,
            "completed_or_interrupted_model_turns_rerun": 0,
            "interrupted_batch_policy": "intent_to_treat_failure_usage_unknown",
            "accounting_complete": unknown_attempts == 0,
            "usage_status": "complete" if unknown_attempts == 0 else "partial_unknown",
            "measured_partial_usage": {field: int(measured[field]) for field in _USAGE_FIELDS},
            "usage_unknown_attempts": unknown_attempts,
            "selection_eligible": False,
            "production_database_mutation": False,
            "production_promotion": False,
            "privacy": "batch_ids_hashes_counts_usage_and_status_no_prompt_or_output_text",
        }
        if self.report_path.exists():
            report = _read_json(self.report_path, "recovery report")
        else:
            _write_immutable(self.report_path, report)
        candidate = self.spec["candidate"]
        consolidated = {
            "schema_version": CONSOLIDATED_ARM_VERSION,
            "evaluation_role": "exploratory_interrupted_intent_to_treat_not_selection",
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
            "batch_size_ceiling": 5,
            "thread_mode": "same_thread_interrupted_recovery",
            "concurrency": 1,
            "retry_count": 0,
            "window_count": candidate["window_count"],
            "context_chars": candidate["context_chars"],
            "max_events_per_segment": candidate["event_cap"],
            "requested_segments": self.spec["manifest"]["segment_count"],
            "validated_segments": validated_segments,
            "requested_calls": len(mapping.get("batches") or []),
            "attempted_calls": sum(Path(item["sidecar_path"]).is_file() for item in inventory),
            "usage_status": "partial_unknown",
            "usage": None,
            "measured_partial_usage": {field: int(measured[field]) for field in _USAGE_FIELDS},
            "usage_measured_attempts": measured_attempts,
            "usage_unknown_attempts": unknown_attempts,
            "accounting_complete": False,
            "candidate_events": candidate_events,
            "failure_classes": {"operator_interrupted_usage_unknown": 1},
            "instruction_source_sets": [
                {
                    "instruction_sources_sha256": source_sha,
                    "instruction_sources_count": source_count,
                    "turn_count": count,
                }
                for (source_sha, source_count), count in sorted(instruction_sets.items())
            ],
            "recovery_report_path": str(self.report_path),
            "recovery_report_sha256": _sha256_file(self.report_path),
            "semantic_quality_status": "not_comparable_interrupted_intent_to_treat",
            "selection_eligible": False,
            "production_database_mutation": False,
            "production_promotion": False,
            "privacy": "sanitized_aggregates_hashes_counts_and_failure_classes_only",
        }
        if self.consolidated_path.exists():
            consolidated = _read_json(self.consolidated_path, "consolidated interrupted arm")
        else:
            _write_immutable(self.consolidated_path, consolidated)
        return report, consolidated

    async def _run_clean_batch8_arms(self) -> None:
        from .unattended_app_server_eval import _probe_instruction_sources_async

        candidate = self.spec["candidate"]
        manifest_path = self._resolve(str(self.spec["manifest"]["artifact_path"]))
        guideline_path = self._resolve(
            str(self.spec["frozen_artifacts"]["candidate_guideline"]["artifact_path"])
        )
        conn = _read_only_connection()
        try:
            for thread_mode in ("new_thread", "same_thread"):
                self._check_stop()
                # Recheck live capacity at the arm boundary.  The injected
                # capacity-gated client also probes immediately before every
                # semantic turn inside the arm.
                await self._wait_for_live_rate_limit_clear_async()
                output_dir = self.matrix_root / "batch-8" / thread_mode
                report_path = output_dir / "report.json"
                attempt_path = (
                    self.recovery_root
                    / "remaining-arm-attempts"
                    / f"batch_8_{thread_mode}.json"
                )
                if report_path.exists():
                    report = _read_json(report_path, "clean batch-8 arm report")
                    if (
                        report.get("accounting_complete") is not True
                        or report.get("usage_status") != "complete"
                        or report.get("usage_unknown_attempts") != 0
                        or report.get("instruction_source_sets")
                        != [
                            {
                                "instruction_sources_sha256": self.spec[
                                    "instruction_contract"
                                ]["expected_path_set_sha256"],
                                "instruction_sources_count": len(
                                    self.spec["instruction_contract"]["sources"]
                                ),
                                "thread_count": int(report.get("observed_thread_count") or 0),
                            }
                        ]
                    ):
                        raise RecoveryError("existing batch-8 arm report is not clean and complete")
                    continue
                if output_dir.exists() and any(output_dir.iterdir()):
                    raise RecoveryError("partial batch-8 arm cannot be rerun")
                clean_pre_turn_resume = bool(
                    attempt_path.exists()
                    and (not output_dir.exists() or not any(output_dir.iterdir()))
                )
                observed_source_sha, observed_source_count = (
                    await _probe_instruction_sources_async(
                        model=str(candidate["model"]), cwd=self.repo_root
                    )
                )
                if (
                    observed_source_sha
                    != self.spec["instruction_contract"]["expected_path_set_sha256"]
                    or observed_source_count
                    != len(self.spec["instruction_contract"]["sources"])
                ):
                    raise RecoveryError("batch-8 preflight instruction source drift")
                if not clean_pre_turn_resume:
                    _write_immutable(
                        attempt_path,
                        {
                            "schema_version": RECOVERY_OUTCOME_VERSION,
                            "arm": f"batch_8_{thread_mode}",
                            "manifest_sha256": self.spec["manifest"]["artifact_sha256"],
                            "retry_ordinal": 0,
                            "started_at": now_iso(),
                            "privacy": "hashes_configuration_and_policy_only",
                        },
                    )
                report = await self.full_arm_runner(
                    conn,
                    manifest_path=manifest_path,
                    output_dir=output_dir,
                    batch_size=8,
                    thread_mode=thread_mode,
                    model=str(candidate["model"]),
                    reasoning_effort=str(candidate["reasoning_effort"]),
                    concurrency=int(candidate["concurrency"]),
                    timeout_seconds=float(candidate["timeout_seconds"]),
                    window_count=int(candidate["window_count"]),
                    context_chars=int(candidate["context_chars"]),
                    max_events_per_segment=int(candidate["event_cap"]),
                    guideline_path=guideline_path,
                    client_factory=self.client_factory,
                )
                if (
                    report.get("accounting_complete") is not True
                    or report.get("usage_status") != "complete"
                    or report.get("usage_unknown_attempts") != 0
                    or len(report.get("instruction_source_sets") or []) != 1
                    or report["instruction_source_sets"][0].get(
                        "instruction_sources_sha256"
                    )
                    != self.spec["instruction_contract"]["expected_path_set_sha256"]
                ):
                    raise RecoveryError("new batch-8 arm did not complete clean accounting")
        finally:
            conn.close()

    def _build_recovered_matrix(self, consolidated: dict[str, Any]) -> dict[str, Any]:
        clean_paths = [
            self.matrix_root / "batch-3" / "new_thread" / "report.json",
            self.matrix_root / "batch-3" / "same_thread" / "report.json",
            self.matrix_root / "batch-5" / "new_thread" / "report.json",
            self.matrix_root / "batch-8" / "new_thread" / "report.json",
            self.matrix_root / "batch-8" / "same_thread" / "report.json",
        ]
        clean = []
        for path in clean_paths:
            report = _read_json(path, "clean recovered-matrix arm")
            if (
                report.get("schema_version") != "pif_app_server_core_arm_v3"
                or report.get("accounting_complete") is not True
                or report.get("usage_status") != "complete"
                or report.get("usage_unknown_attempts") != 0
                or report.get("validated_segments") != 32
            ):
                raise RecoveryError("recovered matrix contains a non-clean selectable arm")
            clean.append(
                {
                    "batch_size": report["batch_size_ceiling"],
                    "thread_mode": report["thread_mode"],
                    "report_path": str(path),
                    "report_sha256": _sha256_file(path),
                    "selection_eligible": True,
                }
            )
        matrix = {
            "schema_version": RECOVERED_MATRIX_VERSION,
            "created_at": now_iso(),
            "evaluation_role": "interrupted_development_recovery_not_acceptance",
            "clean_arm_count": len(clean),
            "clean_arms": clean,
            "interrupted_arm": {
                "batch_size": 5,
                "thread_mode": "same_thread",
                "report_path": str(self.consolidated_path),
                "report_sha256": _sha256_file(self.consolidated_path),
                "selection_eligible": False,
                "accounting_complete": False,
                "usage_status": "partial_unknown",
                "intent_to_treat_failure": True,
            },
            "interrupted_arm_intent_to_treat_provenance": {
                "path": str(self.itt_provenance_path),
                "sha256": _sha256_file(self.itt_provenance_path),
            },
            "clean_six_arm_matrix_achieved": False,
            "clean_six_arm_matrix_achievable_without_forbidden_rerun": False,
            "five_clean_arm_selection_eligible": True,
            "versioned_five_arm_selection_runner_compatible": True,
            "winner": None,
            "selection_eligible": True,
            "next_selection_requirement": (
                "run the versioned five-clean-arm shared-reference selection with the frozen ITT "
                "provenance; never synthesize or rerun batch-5 same-thread evidence"
            ),
            "production_database_mutation": False,
            "production_promotion": False,
            "privacy": "arm_configuration_report_paths_hashes_status_and_accounting_only",
        }
        if self.matrix_report_path.exists():
            return _read_json(self.matrix_report_path, "recovered matrix report")
        _write_immutable(self.matrix_report_path, matrix)
        return matrix

    def _install_signals(self) -> None:
        def handle(_signum: int, _frame: Any) -> None:
            self._stop_requested = True

        for signum in (signal.SIGINT, signal.SIGTERM):
            self._previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, handle)

    def _restore_signals(self) -> None:
        for signum, handler in self._previous_handlers.items():
            signal.signal(signum, handler)
        self._previous_handlers.clear()

    def _check_stop(self) -> None:
        if self.stop_path.exists():
            self._stop_requested = True
        if self._stop_requested:
            raise RecoveryStopped("recovery stop requested")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Freeze an operator-interrupted arm as nonselectable intent-to-treat evidence, skip "
            "all of its remaining batches, then run only clean batch-8 arms after both the "
            "reset boundary and a live no-turn managed-ChatGPT rate-limit clearance."
        )
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument(
        "--run-spec", default="work/app-server-development-v2/run-spec-v2.json"
    )
    parser.add_argument(
        "--matrix-root", default="work/app-server-development-v2/matrix-v1"
    )
    parser.add_argument("--not-before-epoch", type=int, required=True)
    parser.add_argument("--boundary-grace-seconds", type=int, default=30)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    recovery = InterruptedArmRecovery(
        repo_root=Path(args.repo_root),
        run_spec_path=Path(args.run_spec),
        matrix_root=Path(args.matrix_root),
        not_before_epoch=args.not_before_epoch,
        boundary_grace_seconds=args.boundary_grace_seconds,
    )
    try:
        result = recovery.run()
    except RecoveryStopped:
        print(json.dumps({"ok": False, "status": "stopped"}, sort_keys=True))
        return 130
    except (RecoveryError, OSError, sqlite3.Error, subprocess.SubprocessError):
        print(
            json.dumps({"ok": False, "status": "failed_closed"}, sort_keys=True),
            file=sys.stderr,
        )
        return 1
    print(
        json.dumps(
            {
                "ok": True,
                "status": result["status"],
                "recovery_report": str(recovery.report_path),
                "consolidated_arm_report": str(recovery.consolidated_path),
                "recovered_matrix_report": str(recovery.matrix_report_path),
                "clean_six_arm_matrix_achieved": False,
                "production_promotion": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
