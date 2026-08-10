from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import importlib
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Iterator, Protocol

from . import db as factory_db
from .daily_cycle import ensure_daily_schema, run_daily_cycle
from .orchestrator import pipeline_lock
from .pipeline_babysitter import (
    DEFAULT_DB_PATH,
    DEFAULT_EVALUATION_ROOT,
    DEFAULT_GOALS_DB_PATH,
    DEFAULT_STATE_PATH,
)
from .pipeline_watchdog import (
    DEFAULT_OBSERVATION_TIMEOUT_SECONDS,
    observe_with_deadline,
    recovery_message,
)
from .util import write_text_atomic


SCHEMA_VERSION = "pif_sdk_pipeline_controller_v2"
LEDGER_SCHEMA_VERSION = "pif_sdk_pipeline_controller_ledger_v1"
ACTIVATION_SCHEMA_VERSION = "pif_sdk_pipeline_controller_activation_v1"
CAPSULE_SCHEMA_VERSION = "pif_sdk_recovery_capsule_v1"
FRESH_THREAD_STRATEGY = "fresh_bounded"
LEGACY_RESUME_STRATEGY = "legacy_exact_resume"
THREAD_STRATEGIES = {FRESH_THREAD_STRATEGY, LEGACY_RESUME_STRATEGY}
TERMINAL_TURN_STATUSES = {"completed", "failed", "interrupted"}
DEFAULT_MAX_CAPSULE_BYTES = 6 * 1024

DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTROL_ROOT = (
    Path.home() / ".codex" / "memories" / "automation" / "pif-sdk-pipeline-controller"
)
DEFAULT_CHECKPOINT_PATH = DEFAULT_CONTROL_ROOT / "checkpoint.json"
DEFAULT_LEDGER_PATH = DEFAULT_CONTROL_ROOT / "ledger.jsonl"
DEFAULT_LOCK_PATH = DEFAULT_CONTROL_ROOT / "controller.lock"
DEFAULT_ACTIVATION_PATH = DEFAULT_CONTROL_ROOT / "activation.json"
DEFAULT_DAILY_MAX_ITEMS = 25
DEFAULT_DAILY_RUNTIME_SECONDS = 3_600
DEFAULT_DAILY_CONCURRENCY = 10
DEFAULT_DAILY_SOURCE_LIST = DEFAULT_PROJECT_ROOT / "config" / "sources.yaml"


class ControllerError(RuntimeError):
    """Base controller error."""


class ControllerAlreadyRunning(ControllerError):
    """Raised when launchd (or an operator) already owns the controller lease."""


class AuthBoundaryError(ControllerError):
    """Raised before a turn when the SDK is not using managed ChatGPT auth."""


@dataclass(frozen=True)
class ActivationStatus:
    active: bool
    reason: str
    expires_at: str | None = None


@dataclass(frozen=True)
class TurnOutcome:
    thread_id: str
    turn_id: str
    status: str
    final_response: str | None
    usage: dict[str, Any]
    started_at: int | None = None
    completed_at: int | None = None
    duration_ms: int | None = None


@dataclass(frozen=True)
class TurnReconciliation:
    state: str
    turn_id: str | None = None
    turn_status: str | None = None
    error_class: str | None = None


class TurnTransport(Protocol):
    def open(self) -> None: ...

    def close(self) -> None: ...

    def start_and_run(
        self,
        *,
        prompt: str,
        cwd: Path,
        on_thread_ready: Callable[[str], None] | None = None,
        on_turn_started: Callable[[str, str], None] | None = None,
    ) -> TurnOutcome: ...

    def resume_and_run(
        self,
        *,
        thread_id: str,
        prompt: str,
        cwd: Path,
        on_thread_ready: Callable[[str], None] | None = None,
        on_turn_started: Callable[[str, str], None] | None = None,
    ) -> TurnOutcome: ...

    def reconcile_turn(
        self,
        *,
        thread_id: str,
        turn_id: str | None,
        cwd: Path,
        discover_single_turn: bool = False,
    ) -> TurnReconciliation: ...


def _policy_model(stage: str) -> str:
    from .provider_policy import stage_policy

    return stage_policy(stage).model


def iso_now(now: dt.datetime | None = None) -> str:
    value = now or dt.datetime.now().astimezone()
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.replace(microsecond=0).isoformat()


def local_calendar_date(now: dt.datetime | None = None) -> str:
    value = now or dt.datetime.now().astimezone()
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone().date().isoformat()


def _successful_daily_receipt_for_date(
    conn,
    *,
    run_date: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT pif_daily_runs.id AS run_id,
               pif_daily_runs.receipt_json AS daily_receipt_json,
               pif_scale_gate_state_receipts.receipt_json AS scale_receipt_json
        FROM pif_scale_gate_state_receipts
        JOIN pif_daily_runs
          ON pif_daily_runs.id = pif_scale_gate_state_receipts.daily_run_id
        WHERE pif_scale_gate_state_receipts.run_date = ?
          AND pif_scale_gate_state_receipts.genuinely_successful = 1
        ORDER BY pif_scale_gate_state_receipts.created_at DESC,
                 pif_scale_gate_state_receipts.id DESC
        LIMIT 1
        """,
        (run_date,),
    ).fetchone()
    if not row:
        return None
    return {
        "run_id": str(row["run_id"]),
        "daily_receipt": json.loads(str(row["daily_receipt_json"])),
        "scale_receipt": json.loads(str(row["scale_receipt_json"])),
    }


def _gate_table(scale_receipt: dict[str, Any]) -> dict[str, bool]:
    gates = scale_receipt.get("gates")
    if not isinstance(gates, dict):
        return {}
    return {
        str(name): bool(value.get("passed"))
        for name, value in sorted(gates.items())
        if isinstance(value, dict)
    }


def daily_intent_path(project_root: Path, run_date: str) -> Path:
    return (
        project_root.expanduser().resolve()
        / "work"
        / "pif-ops"
        / "controller-daily-intent"
        / run_date
        / "intent.json"
    )


def _record_daily_intent(
    path: Path,
    *,
    run_date: str,
    status: str,
    configuration: dict[str, Any],
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    requested_at = iso_now()
    if path.exists():
        try:
            prior = json.loads(path.read_text(encoding="utf-8"))
            requested_at = str(prior.get("requested_at") or requested_at)
        except (OSError, json.JSONDecodeError):
            pass
    record = {
        "schema_version": "pif_controller_daily_intent_v1",
        "run_date": run_date,
        "status": status,
        "requested_at": requested_at,
        "updated_at": iso_now(),
        "configuration": configuration,
        "detail": detail or {},
    }
    record["content_sha256"] = json_sha256(record)
    write_text_atomic(path, json.dumps(record, indent=2, sort_keys=True) + "\n")
    return {**record, "path": str(path)}


def run_controller_daily_cycle(
    args: argparse.Namespace,
    *,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    run_date = local_calendar_date(now)
    configuration = {
        "run_date": run_date,
        "max_items": int(args.daily_max_items),
        "max_runtime_seconds": int(args.daily_runtime_seconds),
        "concurrency": int(args.daily_concurrency),
        "execute_ingestion": bool(args.execute_ingestion),
        "execute_extraction": bool(args.execute_extraction),
        "execute_outcomes": bool(args.execute_outcomes),
        "source_list": str(args.daily_source_list.expanduser().resolve()),
        # Model comes from the reviewed routing policy, not a hardcode
        # (durability plan Phase 2).
        "model": _policy_model("label_segment"),
        "label_pack": "ai_discourse_v3_1",
    }
    intent_path = daily_intent_path(args.project_root, run_date)
    _record_daily_intent(
        intent_path,
        run_date=run_date,
        status="requested",
        configuration=configuration,
    )
    with pipeline_lock(wait=False) as acquired:
        if not acquired:
            intent = _record_daily_intent(
                intent_path,
                run_date=run_date,
                status="deferred_pipeline_lock_busy",
                configuration=configuration,
                detail={"reason": "backfill_or_other_pipeline_work_holds_lock"},
            )
            return {
                "ok": True,
                "status": "deferred_pipeline_lock_busy",
                "deferred": True,
                "no_op": True,
                "genuinely_successful": False,
                "controller_daily_cycle": True,
                "configuration": configuration,
                "daily_intent": intent,
            }
        _record_daily_intent(
            intent_path,
            run_date=run_date,
            status="running",
            configuration=configuration,
        )
        conn = factory_db.connect(args.db.expanduser().resolve())
        try:
            ensure_daily_schema(conn)
            successful = _successful_daily_receipt_for_date(
                conn,
                run_date=run_date,
            )
            if successful:
                scale_receipt = successful["scale_receipt"]
                intent = _record_daily_intent(
                    intent_path,
                    run_date=run_date,
                    status="completed_genuinely_successful",
                    configuration=configuration,
                    detail={"run_id": successful["run_id"], "no_op": True},
                )
                return {
                    "ok": True,
                    "status": "no_op_genuinely_successful_today",
                    "no_op": True,
                    "controller_daily_cycle": True,
                    "configuration": configuration,
                    "run_id": successful["run_id"],
                    "genuinely_successful": True,
                    "gate_table": _gate_table(scale_receipt),
                    "receipt": successful["daily_receipt"],
                    "scale_receipt": scale_receipt,
                    "daily_intent": intent,
                }
            result = run_daily_cycle(
                conn,
                run_date=run_date,
                # A failed same-date receipt must remain immutable but must not
                # pin every later controller attempt to an idempotent replay of
                # that failure. The successful-receipt guard above remains the
                # per-date no-op authority.
                idempotency_key=f"pif-controller-daily-{run_date}-{uuid.uuid4().hex}",
                max_runtime_seconds=int(args.daily_runtime_seconds),
                max_items=int(args.daily_max_items),
                source_list=args.daily_source_list.expanduser().resolve(),
                execute_ingestion=bool(args.execute_ingestion),
                execute_extraction=bool(args.execute_extraction),
                extraction_concurrency=int(args.daily_concurrency),
                execute_outcomes=bool(args.execute_outcomes),
                lane="podcast",
                label_pack="ai_discourse_v3_1",
                model=_policy_model("label_segment"),
            )
            receipt = result.get("receipt") or {}
            scale_receipt = (
                receipt.get("scale_gate")
                if isinstance(receipt, dict)
                else {}
            ) or {}
            genuinely_successful = bool(scale_receipt.get("genuinely_successful"))
            intent = _record_daily_intent(
                intent_path,
                run_date=run_date,
                status=(
                    "completed_genuinely_successful"
                    if genuinely_successful
                    else "failed"
                ),
                configuration=configuration,
                detail={
                    "run_id": result.get("run_id") or receipt.get("run_id"),
                    "cycle_status": result.get("status"),
                    "genuinely_successful": genuinely_successful,
                },
            )
            return {
                **result,
                "controller_daily_cycle": True,
                "configuration": configuration,
                "no_op": False,
                "genuinely_successful": genuinely_successful,
                "gate_table": _gate_table(scale_receipt),
                "daily_intent": intent,
            }
        except Exception as exc:
            _record_daily_intent(
                intent_path,
                run_date=run_date,
                status="failed",
                configuration=configuration,
                detail={
                    "error_class": type(exc).__name__,
                    "message": str(exc)[:500],
                },
            )
            raise
        finally:
            conn.close()


def parse_iso(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def json_sha256(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def default_checkpoint(now: dt.datetime | None = None) -> dict[str, Any]:
    timestamp = iso_now(now)
    return {
        "schema_version": SCHEMA_VERSION,
        "created_at": timestamp,
        "updated_at": timestamp,
        "phase": "disabled",
        "generation": 0,
        "last_observation_sha256": None,
        "last_observation_at": None,
        "last_turn_observation_sha256": None,
        "thread_strategy": FRESH_THREAD_STRATEGY,
        "last_controller_thread_id": None,
        "last_turn_id": None,
        "last_turn_status": None,
        "last_turn_started_at": None,
        "last_turn_completed_at": None,
        "last_turn_usage": None,
        "last_response_sha256": None,
        "last_response_char_count": None,
        "next_action_at": None,
        "consecutive_failures": 0,
        "ambiguous_turn_count": 0,
        "pending_turn": None,
        "last_reconciliation": None,
        "terminal_reason": None,
    }


def load_checkpoint(path: Path = DEFAULT_CHECKPOINT_PATH) -> dict[str, Any]:
    if not path.exists():
        return default_checkpoint()
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION:
        raise ControllerError(f"unsupported controller checkpoint at {path}")
    return value


class ControllerStore:
    def __init__(self, *, checkpoint_path: Path, ledger_path: Path) -> None:
        self.checkpoint_path = checkpoint_path
        self.ledger_path = ledger_path

    def load(self) -> dict[str, Any]:
        return load_checkpoint(self.checkpoint_path)

    def save(self, checkpoint: dict[str, Any], now: dt.datetime | None = None) -> None:
        checkpoint["updated_at"] = iso_now(now)
        checkpoint["generation"] = int(checkpoint.get("generation", 0)) + 1
        write_json_atomic(self.checkpoint_path, checkpoint)

    def append(
        self,
        *,
        run_id: str,
        kind: str,
        phase: str,
        details: dict[str, Any] | None = None,
        now: dt.datetime | None = None,
    ) -> None:
        event = {
            "schema_version": LEDGER_SCHEMA_VERSION,
            "event_id": str(uuid.uuid4()),
            "recorded_at": iso_now(now),
            "controller_run_id": run_id,
            "kind": kind,
            "phase": phase,
            "details": details or {},
        }
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with self.ledger_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")
            handle.flush()
            os.fsync(handle.fileno())


@contextlib.contextmanager
def exclusive_controller_lock(path: Path = DEFAULT_LOCK_PATH) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ControllerAlreadyRunning(f"controller lease is already held: {path}") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def activation_status(
    *,
    allow_model_turns: bool,
    activation_path: Path,
    project_root: Path,
    now: dt.datetime | None = None,
) -> ActivationStatus:
    if not allow_model_turns:
        return ActivationStatus(False, "model_turn_flag_absent")
    if not activation_path.exists():
        return ActivationStatus(False, "activation_file_missing")
    try:
        value = json.loads(activation_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ActivationStatus(False, "activation_file_invalid")
    if not isinstance(value, dict) or value.get("schema_version") != ACTIVATION_SCHEMA_VERSION:
        return ActivationStatus(False, "activation_schema_invalid")
    if value.get("enabled") is not True:
        return ActivationStatus(False, "activation_disabled")
    configured_root = value.get("project_root")
    if not configured_root or Path(str(configured_root)).expanduser().resolve() != project_root.resolve():
        return ActivationStatus(False, "activation_project_mismatch")
    expires_at = parse_iso(value.get("expires_at"))
    if expires_at is None:
        return ActivationStatus(False, "activation_expiry_invalid")
    clock = now or dt.datetime.now().astimezone()
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=dt.timezone.utc)
    if expires_at <= clock.astimezone(expires_at.tzinfo):
        return ActivationStatus(False, "activation_expired", expires_at.isoformat())
    return ActivationStatus(True, "active", expires_at.isoformat())


def safe_observation(value: dict[str, Any]) -> dict[str, Any]:
    thread = value.get("thread") if isinstance(value.get("thread"), dict) else {}
    goal = value.get("goal") if isinstance(value.get("goal"), dict) else {}
    milestone = value.get("milestones") if isinstance(value.get("milestones"), dict) else {}
    process = thread.get("control_process") if isinstance(thread.get("control_process"), dict) else {}
    prior_exit = (
        value.get("prior_exit_receipt")
        if isinstance(value.get("prior_exit_receipt"), dict)
        else {}
    )
    return {
        "phase": value.get("phase"),
        "thread_id": value.get("thread_id"),
        "workflow_complete": bool(value.get("workflow_complete")),
        "turn_in_progress": bool(thread.get("turn_in_progress")),
        "session_turn_in_progress": bool(thread.get("session_turn_in_progress")),
        "control_process_alive": bool(process.get("alive")),
        "session_sha256": thread.get("recent_sha256"),
        "goal_status": goal.get("status"),
        "milestone_sha256": milestone.get("snapshot_sha256"),
        "queue_sha256": value.get("queue_sha256"),
        "prior_exit_status": prior_exit.get("status"),
        "prior_exit_code": prior_exit.get("exit_code"),
    }


def _bounded_text(value: Any, limit: int = 256) -> str | None:
    if value is None:
        return None
    text = str(value).replace("\x00", "").strip()
    if not text:
        return None
    return text[:limit]


def _artifact_reference(value: Any, project_root: Path) -> dict[str, str | None]:
    text = _bounded_text(value, 2048)
    if text is None:
        return {"relative_path": None, "path_sha256": None}
    path_sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        if ".." in candidate.parts:
            return {"relative_path": None, "path_sha256": path_sha}
        return {"relative_path": candidate.as_posix()[:512], "path_sha256": path_sha}
    try:
        relative = candidate.resolve().relative_to(project_root.resolve())
    except (OSError, ValueError):
        return {"relative_path": None, "path_sha256": path_sha}
    return {"relative_path": relative.as_posix()[:512], "path_sha256": path_sha}


def build_recovery_capsule(
    observation: dict[str, Any],
    checkpoint: dict[str, Any],
    *,
    recovery_id: str,
    project_root: Path,
    created_at: dt.datetime,
    max_bytes: int = DEFAULT_MAX_CAPSULE_BYTES,
) -> dict[str, Any]:
    """Build bounded continuity from sanitized control facts, never prior chat text."""
    safe = safe_observation(observation)
    milestones = (
        observation.get("milestones")
        if isinstance(observation.get("milestones"), dict)
        else {}
    )
    registered_thread = _bounded_text(safe.get("thread_id"), 256)
    capsule = {
        "schema_version": CAPSULE_SCHEMA_VERSION,
        "recovery_id": recovery_id,
        "created_at": iso_now(created_at),
        "pipeline": {
            "phase": _bounded_text(safe.get("phase"), 64),
            "workflow_complete": bool(safe.get("workflow_complete")),
            "historical_goal_status": _bounded_text(safe.get("goal_status"), 64),
            "registered_thread_id_sha256": (
                hashlib.sha256(registered_thread.encode("utf-8")).hexdigest()
                if registered_thread
                else None
            ),
            "session_sha256": _bounded_text(safe.get("session_sha256"), 128),
            "queue_sha256": _bounded_text(safe.get("queue_sha256"), 128),
        },
        "latest_immutable_milestone": {
            **_artifact_reference(milestones.get("newest_path"), project_root),
            "content_sha256": _bounded_text(safe.get("milestone_sha256"), 128),
        },
        "prior_control_exit": {
            "status": _bounded_text(safe.get("prior_exit_status"), 64),
            "exit_code": (
                safe.get("prior_exit_code")
                if isinstance(safe.get("prior_exit_code"), int)
                and not isinstance(safe.get("prior_exit_code"), bool)
                else None
            ),
        },
        "prior_controller_turn": {
            "thread_id_sha256": (
                hashlib.sha256(str(checkpoint["last_controller_thread_id"]).encode("utf-8")).hexdigest()
                if checkpoint.get("last_controller_thread_id")
                else None
            ),
            "turn_id_sha256": (
                hashlib.sha256(str(checkpoint["last_turn_id"]).encode("utf-8")).hexdigest()
                if checkpoint.get("last_turn_id")
                else None
            ),
            "status": _bounded_text(checkpoint.get("last_turn_status"), 64),
            "response_sha256": _bounded_text(checkpoint.get("last_response_sha256"), 128),
            "usage": numeric_usage(checkpoint.get("last_turn_usage")),
        },
        "constraints": [
            "Use only official persistent Codex app-server transport with managed ChatGPT auth.",
            "Keep production unchanged until every evaluation gate passes.",
            "Do not advance untouched holdout early.",
            "Never use API-key billing, raw session-token replay, embeddings, or semantic regex pruning.",
            "Preserve immutable predecessors and complete input, cached-input, output, and reasoning usage accounting.",
            "Treat a failed experiment as a checkpoint; execute one next safe bounded experiment when possible.",
        ],
    }
    encoded = json.dumps(capsule, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(encoded) > max_bytes:
        raise ControllerError(
            f"sanitized recovery capsule exceeded {max_bytes} bytes ({len(encoded)} bytes)"
        )
    return capsule


def fresh_recovery_prompt(capsule: dict[str, Any]) -> str:
    serialized = json.dumps(capsule, indent=2, sort_keys=True)
    return "\n".join(
        [
            "You are a fresh bounded controller thread for the Podcast Intelligence Factory.",
            "The JSON recovery capsule below is the complete continuity handoff; do not seek or reconstruct prior chat prose.",
            "Verify its referenced immutable artifact locally, perform the next concrete safe unit required by the pipeline, and persist a new immutable checkpoint.",
            "Do not stop after merely proposing an experiment. End this turn only after executing one bounded unit, reaching deterministic completion, or recording a genuinely external blocker.",
            "RECOVERY_CAPSULE_JSON",
            serialized,
            "END_RECOVERY_CAPSULE_JSON",
            "",
        ]
    )


def thread_is_active(observation: dict[str, Any]) -> bool:
    thread = observation.get("thread") if isinstance(observation.get("thread"), dict) else {}
    process = thread.get("control_process") if isinstance(thread.get("control_process"), dict) else {}
    return bool(
        thread.get("turn_in_progress")
        or thread.get("session_turn_in_progress")
        or process.get("alive")
    )


def numeric_usage(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if hasattr(value, "model_dump"):
        value = value.model_dump(exclude_none=True)
    if not isinstance(value, dict):
        return {}

    def numeric_tree(item: Any) -> Any:
        if isinstance(item, bool):
            return None
        if isinstance(item, int) or item is None:
            return item
        if isinstance(item, dict):
            nested = {
                str(key): kept
                for key, child in item.items()
                if (kept := numeric_tree(child)) is not None
            }
            return nested
        return None

    result: dict[str, Any] = {}
    for key, item in value.items():
        kept = numeric_tree(item)
        if kept is not None:
            result[str(key)] = kept
    return result


class OfficialCodexSDKTransport:
    """Lazy official-SDK transport with a hard managed-ChatGPT-auth boundary."""

    def __init__(
        self,
        *,
        project_root: Path,
        model: str = "gpt-5.4",
        reasoning_effort: str = "xhigh",
        codex_bin: str | None = None,
        module_loader: Callable[[str], ModuleType] = importlib.import_module,
    ) -> None:
        self.project_root = project_root.resolve()
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.codex_bin = codex_bin
        self.module_loader = module_loader
        self._module: ModuleType | None = None
        self._client: Any = None

    def open(self) -> None:
        if self._client is not None:
            return
        try:
            module = self.module_loader("openai_codex")
        except ModuleNotFoundError as exc:
            raise ControllerError(
                "openai-codex is not installed; install the pinned controller requirement first"
            ) from exc
        config_kwargs: dict[str, Any] = {"cwd": str(self.project_root)}
        if self.codex_bin:
            config_kwargs["codex_bin"] = self.codex_bin
        client = module.Codex(module.CodexConfig(**config_kwargs))
        client.__enter__()
        try:
            account_response = client.account(refresh_token=False)
            account = getattr(account_response, "account", None)
            account_value = getattr(account, "root", account)
            account_type = getattr(account_value, "type", None)
            if account_type != "chatgpt":
                raise AuthBoundaryError(
                    f"controller requires managed ChatGPT auth; observed account type {account_type!r}"
                )
        except BaseException:
            client.__exit__(*sys.exc_info())
            raise
        self._module = module
        self._client = client

    def close(self) -> None:
        if self._client is None:
            return
        client = self._client
        self._client = None
        self._module = None
        client.__exit__(None, None, None)

    def _run_thread(
        self,
        *,
        thread: Any,
        prompt: str,
        cwd: Path,
        on_thread_ready: Callable[[str], None] | None = None,
        on_turn_started: Callable[[str, str], None] | None = None,
    ) -> TurnOutcome:
        assert self._module is not None
        module = self._module
        approval_mode = module.ApprovalMode.deny_all
        sandbox = module.Sandbox.workspace_write
        turn = thread.turn(
            prompt,
            approval_mode=approval_mode,
            cwd=str(cwd.resolve()),
            effort=self.reasoning_effort,
            model=self.model,
            sandbox=sandbox,
        )
        if on_turn_started is not None:
            on_turn_started(str(thread.id), str(turn.id))
        result = turn.run()
        status = getattr(result.status, "value", result.status)
        return TurnOutcome(
            thread_id=str(thread.id),
            turn_id=str(result.id),
            status=str(status),
            final_response=result.final_response,
            usage=numeric_usage(result.usage),
            started_at=result.started_at,
            completed_at=result.completed_at,
            duration_ms=result.duration_ms,
        )

    def start_and_run(
        self,
        *,
        prompt: str,
        cwd: Path,
        on_thread_ready: Callable[[str], None] | None = None,
        on_turn_started: Callable[[str, str], None] | None = None,
    ) -> TurnOutcome:
        self.open()
        assert self._client is not None
        assert self._module is not None
        module = self._module
        thread = self._client.thread_start(
            approval_mode=module.ApprovalMode.deny_all,
            cwd=str(cwd.resolve()),
            ephemeral=False,
            model=self.model,
            sandbox=module.Sandbox.workspace_write,
        )
        if on_thread_ready is not None:
            on_thread_ready(str(thread.id))
        return self._run_thread(
            thread=thread,
            prompt=prompt,
            cwd=cwd,
            on_turn_started=on_turn_started,
        )

    def resume_and_run(
        self,
        *,
        thread_id: str,
        prompt: str,
        cwd: Path,
        on_thread_ready: Callable[[str], None] | None = None,
        on_turn_started: Callable[[str, str], None] | None = None,
    ) -> TurnOutcome:
        self.open()
        assert self._client is not None
        assert self._module is not None
        module = self._module
        thread = self._client.thread_resume(
            thread_id,
            approval_mode=module.ApprovalMode.deny_all,
            cwd=str(cwd.resolve()),
            model=self.model,
            sandbox=module.Sandbox.workspace_write,
        )
        if on_thread_ready is not None:
            on_thread_ready(str(thread.id))
        return self._run_thread(
            thread=thread,
            prompt=prompt,
            cwd=cwd,
            on_turn_started=on_turn_started,
        )

    def reconcile_turn(
        self,
        *,
        thread_id: str,
        turn_id: str | None,
        cwd: Path,
        discover_single_turn: bool = False,
    ) -> TurnReconciliation:
        self.open()
        assert self._client is not None
        assert self._module is not None
        module = self._module
        thread = self._client.thread_resume(
            thread_id,
            approval_mode=module.ApprovalMode.deny_all,
            cwd=str(cwd.resolve()),
            model=self.model,
            sandbox=module.Sandbox.workspace_write,
        )
        response = thread.read(include_turns=True)
        snapshot = getattr(response, "thread", None)
        snapshot = getattr(snapshot, "root", snapshot)
        turns = getattr(snapshot, "turns", None)
        if not isinstance(turns, (list, tuple)):
            return TurnReconciliation(state="unknown", error_class="turn_history_unavailable")
        candidates = list(turns)
        if turn_id is None:
            if not discover_single_turn:
                return TurnReconciliation(
                    state="unknown", error_class="turn_id_missing_discovery_forbidden"
                )
            if not candidates:
                return TurnReconciliation(state="not_started")
            if len(candidates) != 1:
                return TurnReconciliation(
                    state="unknown", error_class="ambiguous_discovered_turn_count"
                )
        for candidate in candidates:
            candidate = getattr(candidate, "root", candidate)
            candidate_id = str(getattr(candidate, "id", ""))
            if turn_id is not None and candidate_id != turn_id:
                continue
            raw_status = getattr(candidate, "status", None)
            status = str(getattr(raw_status, "value", raw_status))
            if status in TERMINAL_TURN_STATUSES:
                return TurnReconciliation(
                    state="terminal", turn_id=candidate_id, turn_status=status
                )
            if status == "inProgress":
                return TurnReconciliation(
                    state="active", turn_id=candidate_id, turn_status=status
                )
            return TurnReconciliation(
                state="unknown",
                turn_id=candidate_id,
                turn_status=status or None,
                error_class="unknown_turn_status",
            )
        return TurnReconciliation(state="unknown", error_class="turn_not_found")


class SDKPipelineController:
    def __init__(
        self,
        *,
        project_root: Path,
        store: ControllerStore,
        activation_path: Path,
        allow_model_turns: bool,
        observer: Callable[[], dict[str, Any]],
        transport_factory: Callable[[], TurnTransport],
        thread_strategy: str = FRESH_THREAD_STRATEGY,
        minimum_turn_interval_seconds: int = 60,
        clock: Callable[[], dt.datetime] = lambda: dt.datetime.now().astimezone(),
    ) -> None:
        self.project_root = project_root.resolve()
        self.store = store
        self.activation_path = activation_path
        self.allow_model_turns = allow_model_turns
        self.observer = observer
        self.transport_factory = transport_factory
        if thread_strategy not in THREAD_STRATEGIES:
            raise ValueError(f"unsupported controller thread strategy: {thread_strategy}")
        self.thread_strategy = thread_strategy
        self.minimum_turn_interval_seconds = max(1, minimum_turn_interval_seconds)
        self.clock = clock
        self.run_id = str(uuid.uuid4())
        self._transport: TurnTransport | None = None

    def close(self) -> None:
        if self._transport is not None:
            self._transport.close()
            self._transport = None

    def _transition(
        self,
        checkpoint: dict[str, Any],
        *,
        phase: str,
        kind: str,
        details: dict[str, Any] | None = None,
        now: dt.datetime,
    ) -> None:
        checkpoint["phase"] = phase
        self.store.append(
            run_id=self.run_id,
            kind=kind,
            phase=phase,
            details=details,
            now=now,
        )
        self.store.save(checkpoint, now)

    def _transport_client(self) -> TurnTransport:
        if self._transport is None:
            self._transport = self.transport_factory()
            self._transport.open()
        return self._transport

    def _persist_pending_receipt(
        self,
        checkpoint: dict[str, Any],
        *,
        kind: str,
        details: dict[str, Any],
        now: dt.datetime,
    ) -> None:
        """Checkpoint first so a ledger/checkpoint crash split cannot cause resend."""
        checkpoint["phase"] = "turn_running"
        self.store.save(checkpoint, now)
        self.store.append(
            run_id=self.run_id,
            kind=kind,
            phase="turn_running",
            details=details,
            now=now,
        )

    def _reconcile_pending_turn(
        self,
        checkpoint: dict[str, Any],
        *,
        activation: ActivationStatus,
        now: dt.datetime,
    ) -> dict[str, Any]:
        pending = checkpoint.get("pending_turn")
        if not isinstance(pending, dict):
            raise ControllerError("pending-turn reconciliation requested without pending state")
        if not activation.active:
            checkpoint["terminal_reason"] = "ambiguous_turn_requires_activation"
            self._transition(
                checkpoint,
                phase="reconciliation_required",
                kind="ambiguous_turn_preserved",
                details={"activation_reason": activation.reason},
                now=now,
            )
            return {
                "status": "ambiguous_turn_requires_activation",
                "phase": "reconciliation_required",
            }

        next_action_at = parse_iso(checkpoint.get("next_action_at"))
        if next_action_at is not None and next_action_at > now.astimezone(next_action_at.tzinfo):
            self._transition(
                checkpoint,
                phase="reconciliation_required",
                kind="reconciliation_backoff_wait",
                details={"next_action_at": next_action_at.isoformat()},
                now=now,
            )
            return {"status": "reconciliation_backoff", "phase": "reconciliation_required"}

        try:
            reconciliation = self._transport_client().reconcile_turn(
                thread_id=str(pending["thread_id"]),
                turn_id=(str(pending["turn_id"]) if pending.get("turn_id") else None),
                cwd=self.project_root,
                discover_single_turn=(
                    pending.get("thread_strategy") == FRESH_THREAD_STRATEGY
                ),
            )
        except AuthBoundaryError as exc:
            checkpoint["terminal_reason"] = "managed_chatgpt_auth_required"
            checkpoint["next_action_at"] = None
            self._transition(
                checkpoint,
                phase="reconciliation_required",
                kind="reconciliation_auth_blocked",
                details={"error_class": type(exc).__name__},
                now=self.clock(),
            )
            return {"status": "reconciliation_auth_blocked", "phase": "reconciliation_required"}
        except Exception as exc:
            failed_at = self.clock()
            checkpoint["terminal_reason"] = "ambiguous_turn_reconciliation_unavailable"
            checkpoint["next_action_at"] = iso_now(failed_at + dt.timedelta(seconds=60))
            checkpoint["last_reconciliation"] = {
                "state": "unknown",
                "error_class": type(exc).__name__,
                "checked_at": iso_now(failed_at),
            }
            self._transition(
                checkpoint,
                phase="reconciliation_required",
                kind="reconciliation_failed_closed",
                details={"error_class": type(exc).__name__},
                now=failed_at,
            )
            return {
                "status": "reconciliation_unavailable",
                "phase": "reconciliation_required",
            }

        checked_at = self.clock()
        checkpoint["last_reconciliation"] = {
            "state": reconciliation.state,
            "turn_id_sha256": (
                hashlib.sha256(reconciliation.turn_id.encode("utf-8")).hexdigest()
                if reconciliation.turn_id
                else None
            ),
            "turn_status": reconciliation.turn_status,
            "error_class": reconciliation.error_class,
            "checked_at": iso_now(checked_at),
        }
        if reconciliation.state == "terminal":
            checkpoint["pending_turn"] = None
            checkpoint["last_controller_thread_id"] = str(pending["thread_id"])
            checkpoint["last_turn_id"] = reconciliation.turn_id or pending.get("turn_id")
            checkpoint["last_turn_status"] = reconciliation.turn_status
            checkpoint["last_turn_completed_at"] = iso_now(checked_at)
            checkpoint["terminal_reason"] = None
            checkpoint["next_action_at"] = iso_now(
                checked_at + dt.timedelta(seconds=self.minimum_turn_interval_seconds)
            )
            self._transition(
                checkpoint,
                phase="observing",
                kind="ambiguous_turn_reconciled_terminal",
                details={"turn_status": reconciliation.turn_status},
                now=checked_at,
            )
            return {
                "status": "turn_reconciled_terminal",
                "phase": "observing",
                "turn_status": reconciliation.turn_status,
            }

        if reconciliation.state == "not_started":
            checkpoint["pending_turn"] = None
            checkpoint["terminal_reason"] = None
            checkpoint["next_action_at"] = iso_now(
                checked_at + dt.timedelta(seconds=self.minimum_turn_interval_seconds)
            )
            self._transition(
                checkpoint,
                phase="observing",
                kind="thread_ready_without_accepted_turn_reconciled",
                now=checked_at,
            )
            return {"status": "turn_not_started_reconciled", "phase": "observing"}

        if reconciliation.turn_id and not pending.get("turn_id"):
            pending["turn_id"] = reconciliation.turn_id
            pending["state"] = "discovered_during_reconciliation"

        checkpoint["terminal_reason"] = (
            "ambiguous_turn_still_active"
            if reconciliation.state == "active"
            else "ambiguous_turn_unresolved"
        )
        checkpoint["next_action_at"] = iso_now(checked_at + dt.timedelta(seconds=60))
        phase = "reconciling" if reconciliation.state == "active" else "reconciliation_required"
        self._transition(
            checkpoint,
            phase=phase,
            kind=(
                "ambiguous_turn_still_active"
                if reconciliation.state == "active"
                else "ambiguous_turn_unresolved"
            ),
            details={
                "turn_status": reconciliation.turn_status,
                "error_class": reconciliation.error_class,
            },
            now=checked_at,
        )
        return {
            "status": (
                "turn_still_active"
                if reconciliation.state == "active"
                else "turn_reconciliation_unknown"
            ),
            "phase": phase,
        }

    def tick(self) -> dict[str, Any]:
        now = self.clock()
        checkpoint = self.store.load()
        checkpoint["thread_strategy"] = self.thread_strategy
        try:
            observation = self.observer()
        except Exception as exc:
            failures = int(checkpoint.get("consecutive_failures", 0)) + 1
            checkpoint["consecutive_failures"] = failures
            checkpoint["terminal_reason"] = "bounded_observation_failed"
            checkpoint["next_action_at"] = iso_now(now + dt.timedelta(seconds=60))
            self._transition(
                checkpoint,
                phase="backoff",
                kind="bounded_observation_failed",
                details={"error_class": type(exc).__name__},
                now=now,
            )
            return {"status": "observation_failed", "phase": "backoff"}
        safe = safe_observation(observation)
        observation_sha = json_sha256(safe)
        checkpoint["last_observation_sha256"] = observation_sha
        checkpoint["last_observation_at"] = iso_now(now)
        activation = activation_status(
            allow_model_turns=self.allow_model_turns,
            activation_path=self.activation_path,
            project_root=self.project_root,
            now=now,
        )

        if safe["workflow_complete"]:
            checkpoint["terminal_reason"] = "pipeline_complete"
            self._transition(
                checkpoint,
                phase="complete",
                kind="pipeline_complete",
                details={"observation_sha256": observation_sha},
                now=now,
            )
            return {"status": "complete", "phase": "complete"}

        if isinstance(checkpoint.get("pending_turn"), dict):
            return self._reconcile_pending_turn(
                checkpoint,
                activation=activation,
                now=now,
            )

        if thread_is_active(observation):
            checkpoint["terminal_reason"] = None
            self._transition(
                checkpoint,
                phase="observing",
                kind="external_turn_active",
                details={"observation_sha256": observation_sha},
                now=now,
            )
            return {"status": "observing_active_turn", "phase": "observing"}

        if not activation.active:
            checkpoint["terminal_reason"] = None
            self._transition(
                checkpoint,
                phase="disabled",
                kind="model_turn_suppressed",
                details={
                    "activation_reason": activation.reason,
                    "observation_sha256": observation_sha,
                },
                now=now,
            )
            return {
                "status": "disabled",
                "phase": "disabled",
                "activation_reason": activation.reason,
            }

        next_action_at = parse_iso(checkpoint.get("next_action_at"))
        if next_action_at is not None and next_action_at > now.astimezone(next_action_at.tzinfo):
            self._transition(
                checkpoint,
                phase="backoff",
                kind="backoff_wait",
                details={"next_action_at": next_action_at.isoformat()},
                now=now,
            )
            return {"status": "backoff", "phase": "backoff"}

        registered_thread_id = safe.get("thread_id")
        if self.thread_strategy == LEGACY_RESUME_STRATEGY and not registered_thread_id:
            checkpoint["terminal_reason"] = "registered_thread_missing"
            self._transition(
                checkpoint,
                phase="blocked",
                kind="registered_thread_missing",
                now=now,
            )
            return {"status": "blocked", "phase": "blocked"}

        recovery_id = f"sdk-{now.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
        if self.thread_strategy == FRESH_THREAD_STRATEGY:
            capsule = build_recovery_capsule(
                observation,
                checkpoint,
                recovery_id=recovery_id,
                project_root=self.project_root,
                created_at=now,
            )
            prompt = fresh_recovery_prompt(capsule)
            handoff_sha = json_sha256(capsule)
        else:
            prompt = recovery_message(observation, recovery_id)
            handoff_sha = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        checkpoint["last_turn_observation_sha256"] = observation_sha
        checkpoint["last_turn_started_at"] = iso_now(now)
        checkpoint["terminal_reason"] = None
        self._transition(
            checkpoint,
            phase="turn_running",
            kind="turn_intent_recorded",
            details={
                "recovery_id": recovery_id,
                "thread_strategy": self.thread_strategy,
                "handoff_sha256": handoff_sha,
            },
            now=now,
        )

        try:
            transport = self._transport_client()

            def on_thread_ready(controller_thread_id: str) -> None:
                ready_at = self.clock()
                checkpoint["last_controller_thread_id"] = controller_thread_id
                checkpoint["pending_turn"] = {
                    "thread_id": controller_thread_id,
                    "turn_id": None,
                    "thread_strategy": self.thread_strategy,
                    "recovery_id": recovery_id,
                    "observation_sha256": observation_sha,
                    "accepted_at": None,
                    "thread_ready_at": iso_now(ready_at),
                    "state": "turn_acceptance_unknown",
                }
                self._persist_pending_receipt(
                    checkpoint,
                    kind="sdk_thread_ready",
                    details={
                        "thread_id_sha256": hashlib.sha256(
                            controller_thread_id.encode("utf-8")
                        ).hexdigest(),
                        "thread_strategy": self.thread_strategy,
                    },
                    now=ready_at,
                )

            def on_turn_started(controller_thread_id: str, turn_id: str) -> None:
                accepted_at = self.clock()
                checkpoint["last_controller_thread_id"] = controller_thread_id
                checkpoint["last_turn_id"] = turn_id
                checkpoint["pending_turn"] = {
                    "thread_id": controller_thread_id,
                    "turn_id": turn_id,
                    "thread_strategy": self.thread_strategy,
                    "recovery_id": recovery_id,
                    "observation_sha256": observation_sha,
                    "accepted_at": iso_now(accepted_at),
                    "state": "accepted_unconfirmed",
                }
                self._persist_pending_receipt(
                    checkpoint,
                    kind="sdk_turn_accepted",
                    details={
                        "thread_id_sha256": hashlib.sha256(
                            controller_thread_id.encode("utf-8")
                        ).hexdigest(),
                        "turn_id_sha256": hashlib.sha256(turn_id.encode("utf-8")).hexdigest(),
                        "thread_strategy": self.thread_strategy,
                    },
                    now=accepted_at,
                )

            if self.thread_strategy == FRESH_THREAD_STRATEGY:
                outcome = transport.start_and_run(
                    prompt=prompt,
                    cwd=self.project_root,
                    on_thread_ready=on_thread_ready,
                    on_turn_started=on_turn_started,
                )
            else:
                outcome = transport.resume_and_run(
                    thread_id=str(registered_thread_id),
                    prompt=prompt,
                    cwd=self.project_root,
                    on_thread_ready=on_thread_ready,
                    on_turn_started=on_turn_started,
                )
        except AuthBoundaryError as exc:
            checkpoint["consecutive_failures"] = int(checkpoint.get("consecutive_failures", 0)) + 1
            checkpoint["terminal_reason"] = "managed_chatgpt_auth_required"
            checkpoint["next_action_at"] = None
            self._transition(
                checkpoint,
                phase="blocked",
                kind="auth_boundary_blocked",
                details={"error_class": type(exc).__name__},
                now=self.clock(),
            )
            return {"status": "blocked_auth_boundary", "phase": "blocked"}
        except Exception as exc:
            failures = int(checkpoint.get("consecutive_failures", 0)) + 1
            failed_at = self.clock()
            checkpoint["consecutive_failures"] = failures
            if isinstance(checkpoint.get("pending_turn"), dict):
                checkpoint["ambiguous_turn_count"] = int(
                    checkpoint.get("ambiguous_turn_count", 0)
                ) + 1
                checkpoint["pending_turn"]["state"] = "outcome_ambiguous"
                checkpoint["pending_turn"]["error_class"] = type(exc).__name__
                checkpoint["terminal_reason"] = "ambiguous_turn_outcome"
                checkpoint["next_action_at"] = iso_now(failed_at + dt.timedelta(seconds=60))
                self._transition(
                    checkpoint,
                    phase="reconciliation_required",
                    kind="turn_outcome_ambiguous",
                    details={"error_class": type(exc).__name__},
                    now=failed_at,
                )
                return {
                    "status": "turn_outcome_ambiguous",
                    "phase": "reconciliation_required",
                }
            delay = min(3600, 60 * (2 ** min(failures - 1, 6)))
            checkpoint["terminal_reason"] = "transport_error"
            checkpoint["next_action_at"] = iso_now(failed_at + dt.timedelta(seconds=delay))
            self._transition(
                checkpoint,
                phase="backoff",
                kind="turn_failed",
                details={"error_class": type(exc).__name__, "retry_delay_seconds": delay},
                now=failed_at,
            )
            return {"status": "turn_failed", "phase": "backoff", "retry_delay_seconds": delay}

        finished_at = self.clock()
        pending = checkpoint.get("pending_turn")
        if not isinstance(pending, dict) or (
            str(pending.get("thread_id")) != outcome.thread_id
            or str(pending.get("turn_id")) != outcome.turn_id
        ):
            checkpoint["pending_turn"] = {
                "thread_id": outcome.thread_id,
                "turn_id": outcome.turn_id,
                "thread_strategy": self.thread_strategy,
                "recovery_id": recovery_id,
                "observation_sha256": observation_sha,
                "accepted_at": iso_now(finished_at),
                "state": "receipt_identity_mismatch",
            }
            checkpoint["terminal_reason"] = "turn_receipt_identity_mismatch"
            checkpoint["next_action_at"] = iso_now(finished_at + dt.timedelta(seconds=60))
            self._transition(
                checkpoint,
                phase="reconciliation_required",
                kind="turn_receipt_identity_mismatch",
                now=finished_at,
            )
            return {
                "status": "turn_receipt_identity_mismatch",
                "phase": "reconciliation_required",
            }
        response = outcome.final_response or ""
        checkpoint["pending_turn"] = None
        checkpoint["last_controller_thread_id"] = outcome.thread_id
        checkpoint["last_turn_id"] = outcome.turn_id
        checkpoint["last_turn_status"] = outcome.status
        checkpoint["last_turn_completed_at"] = iso_now(finished_at)
        checkpoint["last_turn_usage"] = outcome.usage
        checkpoint["last_response_sha256"] = hashlib.sha256(response.encode("utf-8")).hexdigest()
        checkpoint["last_response_char_count"] = len(response)
        checkpoint["consecutive_failures"] = 0
        checkpoint["terminal_reason"] = None
        checkpoint["next_action_at"] = iso_now(
            finished_at + dt.timedelta(seconds=self.minimum_turn_interval_seconds)
        )
        self._transition(
            checkpoint,
            phase="observing",
            kind="turn_completed",
            details={
                "turn_id": outcome.turn_id,
                "thread_id_sha256": hashlib.sha256(
                    outcome.thread_id.encode("utf-8")
                ).hexdigest(),
                "turn_status": outcome.status,
                "duration_ms": outcome.duration_ms,
                "usage": outcome.usage,
                "response_sha256": checkpoint["last_response_sha256"],
                "response_char_count": len(response),
            },
            now=finished_at,
        )
        return {
            "status": "turn_completed",
            "phase": "observing",
            "turn_id": outcome.turn_id,
            "thread_id": outcome.thread_id,
            "turn_status": outcome.status,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fail-closed launchd controller skeleton for the podcast pipeline."
    )
    parser.add_argument("--project-root", type=Path, default=DEFAULT_PROJECT_ROOT)
    parser.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT_PATH)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER_PATH)
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK_PATH)
    parser.add_argument("--activation-file", type=Path, default=DEFAULT_ACTIVATION_PATH)
    parser.add_argument("--pipeline-state", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--goals-db", type=Path, default=DEFAULT_GOALS_DB_PATH)
    parser.add_argument("--evaluation-root", type=Path, default=DEFAULT_EVALUATION_ROOT)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--model", default=None, help="Override the policy model (default: config/provider_policy.json)")
    parser.add_argument("--reasoning-effort", default="xhigh")
    parser.add_argument("--codex-bin", default=None)
    parser.add_argument("--allow-model-turns", action="store_true")
    parser.add_argument(
        "--thread-strategy",
        choices=sorted(THREAD_STRATEGIES),
        default=FRESH_THREAD_STRATEGY,
        help="Fresh bounded threads are the default; exact resume is migration-only.",
    )
    parser.add_argument(
        "--observation-timeout-seconds",
        type=int,
        default=DEFAULT_OBSERVATION_TIMEOUT_SECONDS,
    )
    parser.add_argument("--interval-seconds", type=int, default=30)
    parser.add_argument("--minimum-turn-interval-seconds", type=int, default=60)
    parser.add_argument(
        "--daily-max-items",
        type=int,
        default=DEFAULT_DAILY_MAX_ITEMS,
    )
    parser.add_argument(
        "--daily-runtime-seconds",
        type=int,
        default=DEFAULT_DAILY_RUNTIME_SECONDS,
    )
    parser.add_argument(
        "--daily-concurrency",
        type=int,
        default=DEFAULT_DAILY_CONCURRENCY,
    )
    parser.add_argument(
        "--daily-source-list",
        type=Path,
        default=DEFAULT_DAILY_SOURCE_LIST,
    )
    parser.add_argument(
        "--execute-ingestion",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--execute-extraction",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--execute-outcomes",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status")
    subparsers.add_parser("once")
    serve = subparsers.add_parser("serve")
    serve.add_argument(
        "--once",
        action="store_true",
        help="Run one controller daily-cycle decision and exit.",
    )
    return parser


def controller_from_args(args: argparse.Namespace) -> SDKPipelineController:
    project_root = args.project_root.expanduser().resolve()
    store = ControllerStore(
        checkpoint_path=args.checkpoint.expanduser(),
        ledger_path=args.ledger.expanduser(),
    )

    def observer() -> dict[str, Any]:
        return observe_with_deadline(
            args.pipeline_state.expanduser(),
            args.goals_db.expanduser(),
            args.evaluation_root.expanduser(),
            args.db.expanduser(),
            timeout_seconds=args.observation_timeout_seconds,
        )

    def transport_factory() -> TurnTransport:
        return OfficialCodexSDKTransport(
            project_root=project_root,
            model=args.model or _policy_model("label_segment"),
            reasoning_effort=args.reasoning_effort,
            codex_bin=args.codex_bin,
        )

    return SDKPipelineController(
        project_root=project_root,
        store=store,
        activation_path=args.activation_file.expanduser(),
        allow_model_turns=bool(args.allow_model_turns),
        observer=observer,
        transport_factory=transport_factory,
        thread_strategy=args.thread_strategy,
        minimum_turn_interval_seconds=args.minimum_turn_interval_seconds,
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "status":
        checkpoint = load_checkpoint(args.checkpoint.expanduser())
        activation = activation_status(
            allow_model_turns=bool(args.allow_model_turns),
            activation_path=args.activation_file.expanduser(),
            project_root=args.project_root.expanduser().resolve(),
        )
        print(
            json.dumps(
                {
                    "checkpoint": checkpoint,
                    "activation": {
                        "active": activation.active,
                        "reason": activation.reason,
                        "expires_at": activation.expires_at,
                    },
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "serve":
        try:
            with exclusive_controller_lock(args.lock.expanduser()):
                while True:
                    result = run_controller_daily_cycle(args)
                    print(json.dumps(result, indent=2, sort_keys=True), flush=True)
                    if args.once:
                        return 0 if result.get("ok") else 1
                    time.sleep(max(1, args.interval_seconds))
        except ControllerAlreadyRunning as exc:
            print(
                json.dumps({"status": "already_running", "error": str(exc)}),
                file=sys.stderr,
            )
            return 75

    controller = controller_from_args(args)
    try:
        with exclusive_controller_lock(args.lock.expanduser()):
            if args.command == "once":
                print(json.dumps(controller.tick(), indent=2, sort_keys=True))
                return 0
            while True:
                result = controller.tick()
                if result.get("status") == "complete":
                    return 0
                time.sleep(max(1, args.interval_seconds))
    except ControllerAlreadyRunning as exc:
        print(json.dumps({"status": "already_running", "error": str(exc)}), file=sys.stderr)
        return 75
    finally:
        controller.close()


if __name__ == "__main__":
    raise SystemExit(main())
