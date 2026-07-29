from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import fcntl
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

try:
    import tomllib
except ImportError:  # pragma: no cover - Python 3.10 compatibility on the host
    import tomli as tomllib  # type: ignore[no-redef]

from .app_server_goal_control import (
    DEFAULT_CODEX_BINARY,
    inspect_thread_goal,
    resume_blocked_thread_goal,
    resume_paused_thread_goal,
)
from .codex_app_server import AppServerError
from .labels import ValidationError, _validate_schema
from .util import now_iso, write_text_atomic


SCHEMA_VERSION = "pif_native_goal_supervisor_v5"
OPERATOR_HOLD_SCHEMA_VERSION = "pif_native_operator_hold_v1"
SEMANTIC_PLAN_SCHEMA_VERSION = "pif_evaluation_semantic_plan_v1"
SEMANTIC_STEP_RECEIPT_SCHEMA_VERSION = "pif_semantic_plan_step_receipt_v1"
END_TO_END_COMPLETION_SCHEMA_VERSION = "pif_end_to_end_completion_authorization_v1"
COMPLETION_DELIVERY_RECEIPT_SCHEMA_VERSION = (
    "pif_native_supervisor_completion_delivery_v1"
)
TARGET_THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
AUTOMATION_ID = "pif-evaluation-goal-resumer-v2"
AUTOMATION_NAME = "PIF evaluation goal resumer v2"
AUTOMATION_RRULE = "RRULE:FREQ=HOURLY;INTERVAL=4"
AUTOMATION_PROMPT_SHA256 = "942b7b04d7bc710725abc65542c95c0c4ee7987417e5dd202ca94f98e837d3f4"
PROJECT_ROOT = Path("/Users/kolbydayley/Documents/Codex/podcast-intelligence-factory")
CODEX_HOME = Path.home() / ".codex"
DEFAULT_AUTOMATION_TOML = CODEX_HOME / "automations" / AUTOMATION_ID / "automation.toml"
DEFAULT_CANONICAL_AUTOMATION_TOML = (
    CODEX_HOME / "lib" / "pif-evaluation-goal-resumer-v2.canonical.toml"
)
DEFAULT_AUTOMATIONS_DB = CODEX_HOME / "sqlite" / "codex-dev.db"
DEFAULT_ROLLOUT_PATH = (
    CODEX_HOME
    / "sessions"
    / "2026"
    / "07"
    / "10"
    / f"rollout-2026-07-10T12-52-20-{TARGET_THREAD_ID}.jsonl"
)
SUPERVISOR_ROOT = CODEX_HOME / "memories" / "automation" / "pif-native-goal-supervisor"
DEFAULT_LOCK_PATH = SUPERVISOR_ROOT / "supervisor.lock"
DEFAULT_RECEIPT_PATH = SUPERVISOR_ROOT / "events.jsonl"
DEFAULT_OPERATOR_HOLD_PATH = SUPERVISOR_ROOT / "operator-hold.json"
DEFAULT_SEMANTIC_PLAN_PATH = CODEX_HOME / "lib" / "pif-evaluation-semantic-plan-v1.json"
DEFAULT_COMPLETION_AUTHORIZATION_PATH = (
    PROJECT_ROOT / "work" / "pif-end-to-end-completion-authorization-v1.json"
)
DEFAULT_COMPLETION_DELIVERY_RECEIPT_PATH = (
    SUPERVISOR_ROOT / "completion-delivery-receipt.json"
)
DEFAULT_CODEX_CRON = CODEX_HOME / "bin" / "codex-cron"
DEFAULT_CODEX_OPS = CODEX_HOME / "bin" / "codex-ops"
DEFAULT_CODEX_OPS_CONFIG = (
    CODEX_HOME / "memories" / "automation" / "ops" / "config.json"
)
DEFAULT_CODEX_OPS_STATE = (
    CODEX_HOME / "memories" / "automation" / "ops" / "state.json"
)
DEFAULT_CODEX_OPS_EVENTS = (
    CODEX_HOME / "memories" / "automation" / "ops" / "events.jsonl"
)
EXPECTED_COMPLETION_TELEGRAM_REPO = Path(
    "/Users/kolbydayley/Documents/GitHub/clawdbot-railway"
)
EXPECTED_COMPLETION_TELEGRAM_SERVICE = "clawdbot-railway"
EXPECTED_COMPLETION_TELEGRAM_CHANNEL = "telegram"
EXPECTED_COMPLETION_TELEGRAM_ACCOUNT = "default"
EXPECTED_COMPLETION_TELEGRAM_IDENTITY = (
    Path.home() / ".ssh" / "railway_codex_agent_ed25519"
)
EXPECTED_COMPLETION_TELEGRAM_TARGET_SHA256 = (
    "49caaa271268a2cf47723663635baaf89b2bc7031d16727bdcf60c543b3438bc"
)
EXPECTED_COMPLETION_TELEGRAM_SOURCE_ALLOWLIST = ["pif-native-goal-supervisor"]
COMPLETION_NOTIFICATION_SUMMARY = "Podcast evaluation goal completed; babysitter stopped"
COMPLETION_NOTIFICATION_NEXT_STEP = (
    "Review the completed evaluation and extraction handoff receipts."
)
EXPECTED_TOML_KEYS = frozenset(
    {
        "version",
        "id",
        "kind",
        "name",
        "prompt",
        "status",
        "rrule",
        "target_thread_id",
        "created_at",
        "updated_at",
    }
)
SEMANTIC_PLAN_KEYS = frozenset(
    {"schema_version", "thread_id", "plan_epoch", "state", "step"}
)
SEMANTIC_STEP_KEYS = frozenset(
    {
        "step_id",
        "state",
        "max_model_calls",
        "max_total_tokens",
        "expected_receipt_path",
        "accepted_receipt_states",
        "directive_path",
        "directive_sha256",
    }
)
SEMANTIC_STATES = frozenset({"executable", "waiting", "complete"})
COMPLETION_CRITERIA = (
    "development_quality",
    "untouched_holdout",
    "production_app_server_cutover",
    "eligible_extraction_backlog_zero",
    "notification_readiness",
)
COMPLETION_AUTHORIZATION_KEYS = frozenset(
    {
        "schema_version",
        "thread_id",
        "state",
        "authorization_id",
        "authorized_at",
        "evidence",
    }
)
COMPLETION_EVIDENCE_DESCRIPTOR_KEYS = frozenset(
    {"path", "sha256", "size_bytes", "schema_version", "verifier"}
)
COMPLETION_SOURCE_CONTRACTS = {
    "development_quality": (
        "pif_semantic_plan_step_receipt_v1",
        "epoch5_direct_reference_verify_receipt_v1",
    ),
    "untouched_holdout": (
        "pif_pipeline_evaluation_receipt_v2",
        "pipeline_evaluation_receipt_verify_v2",
    ),
    "production_app_server_cutover": (
        "pif_production_app_server_cutover_receipt_v1",
        "production_app_server_cutover_verify_v1",
    ),
    "eligible_extraction_backlog_zero": (
        "pif_production_extraction_completion_manifest_v1",
        "pipeline_completion_audit_sqlite_v1",
    ),
    "notification_readiness": (
        "pif_completion_notification_readiness_receipt_v1",
        "codex_ops_telegram_readiness_verify_v1",
    ),
}


class SupervisorError(RuntimeError):
    pass


class SupervisorLockUnavailable(SupervisorError):
    pass


@dataclass(frozen=True)
class GoalState:
    before_status: str
    after_status: str
    action: str
    dispatch_allowed: bool
    complete: bool


@dataclass(frozen=True)
class WriterState:
    writer_count: int
    desktop_owned: bool
    writer_pids: tuple[int, ...]


@dataclass(frozen=True)
class RolloutState:
    classification: str
    lifecycle: str
    turn_id: str | None
    last_timestamp: str | None
    age_seconds: float | None
    file_size: int

    @property
    def fresh_open(self) -> bool:
        return self.classification == "fresh_open"


@dataclass(frozen=True)
class OperatorHoldState:
    classification: str
    marker_sha256: str | None
    hold_id_sha256: str | None

    @property
    def valid(self) -> bool:
        return self.classification == "valid"


@dataclass(frozen=True)
class CompletionAuthorization:
    sha256: str
    authorization_id: str
    authorized_at: str
    evidence_sha256: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class CompletionAuthorizationState:
    classification: str
    marker_sha256: str | None
    reason: str | None
    authorization: CompletionAuthorization | None

    @property
    def valid(self) -> bool:
        return self.classification == "valid" and self.authorization is not None


@dataclass(frozen=True)
class SemanticPlan:
    sha256: str
    plan_epoch: int
    state: str
    step_id: str
    step_state: str
    max_model_calls: int
    max_total_tokens: int
    expected_receipt_path: Path
    accepted_receipt_states: tuple[str, ...]
    directive_path: Path
    directive_sha256: str

    @property
    def waiting_reason(self) -> str | None:
        if self.state != "executable":
            return f"plan_{self.state}"
        if self.step_state != "executable":
            return f"step_{self.step_state}"
        if self.max_model_calls == 0:
            return "max_model_calls_exhausted"
        if self.max_total_tokens == 0:
            return "max_total_tokens_exhausted"
        return None


@dataclass(frozen=True)
class SemanticStepReceipt:
    state: str


@dataclass
class TriggerEvidence:
    task_started_turns: set[str] = field(default_factory=set)
    goal_context_turns: set[str] = field(default_factory=set)
    heartbeat_turns: set[str] = field(default_factory=set)
    dispatch_activity_turns: set[str] = field(default_factory=set)

    def verified_turn_id(self) -> str | None:
        verified = (
            self.task_started_turns
            & self.goal_context_turns
            & self.heartbeat_turns
            & self.dispatch_activity_turns
        )
        return sorted(verified)[-1] if verified else None


@dataclass(frozen=True)
class SupervisorConfig:
    thread_id: str = TARGET_THREAD_ID
    binary: Path = DEFAULT_CODEX_BINARY
    automation_toml: Path = DEFAULT_AUTOMATION_TOML
    canonical_automation_toml: Path = DEFAULT_CANONICAL_AUTOMATION_TOML
    automations_db: Path = DEFAULT_AUTOMATIONS_DB
    rollout_path: Path = DEFAULT_ROLLOUT_PATH
    lock_path: Path = DEFAULT_LOCK_PATH
    receipt_path: Path = DEFAULT_RECEIPT_PATH
    operator_hold_path: Path = DEFAULT_OPERATOR_HOLD_PATH
    semantic_plan_path: Path = DEFAULT_SEMANTIC_PLAN_PATH
    completion_authorization_path: Path = DEFAULT_COMPLETION_AUTHORIZATION_PATH
    completion_delivery_receipt_path: Path = DEFAULT_COMPLETION_DELIVERY_RECEIPT_PATH
    project_root: Path = PROJECT_ROOT
    fresh_activity_seconds: float = 30 * 60
    verify_timeout_seconds: float = 120.0
    verify_poll_seconds: float = 0.5
    rollout_tail_bytes: int = 64 * 1024 * 1024

    def validate(self) -> None:
        if self.thread_id != TARGET_THREAD_ID:
            raise ValueError("supervisor is pinned to the exact evaluation thread")
        if self.fresh_activity_seconds <= 0:
            raise ValueError("fresh activity threshold must be positive")
        if self.verify_timeout_seconds <= 0 or self.verify_poll_seconds <= 0:
            raise ValueError("verification timing must be positive")
        if self.rollout_tail_bytes < 64 * 1024:
            raise ValueError("rollout tail must be at least 64 KiB")
        if not self.project_root.expanduser().is_absolute():
            raise ValueError("project root must be absolute")
        completion_path = self.completion_authorization_path.expanduser().resolve()
        completion_root = self.project_root.expanduser().resolve() / "work"
        try:
            completion_path.relative_to(completion_root)
        except ValueError as exc:
            raise ValueError(
                "completion authorization must stay inside the project work directory"
            ) from exc
        if not self.completion_delivery_receipt_path.expanduser().is_absolute():
            raise ValueError("completion delivery receipt path must be absolute")


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise SupervisorError(f"semantic JSON contains duplicate key: {key}")
        payload[key] = value
    return payload


def _load_strict_json(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
    source = path.expanduser().resolve()
    if not source.is_file():
        raise SupervisorError(f"{label} is unavailable")
    try:
        raw = source.read_bytes()
        payload = json.loads(raw, object_pairs_hook=_unique_json_object)
    except SupervisorError:
        raise
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise SupervisorError(f"{label} is malformed") from exc
    if not isinstance(payload, dict):
        raise SupervisorError(f"{label} must be a JSON object")
    return payload, raw


def _absolute_path_within(value: Any, root: Path, *, label: str) -> Path:
    if not isinstance(value, str) or not value or not Path(value).is_absolute():
        raise SupervisorError(f"{label} must be an absolute path")
    resolved = Path(value).expanduser().resolve()
    resolved_root = root.expanduser().resolve()
    try:
        relative = resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise SupervisorError(f"{label} must stay inside {resolved_root}") from exc
    if not relative.parts:
        raise SupervisorError(f"{label} must name a file inside {resolved_root}")
    return resolved


def _nonnegative_int(value: Any, *, label: str, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SupervisorError(f"{label} must be an integer")
    if value < (1 if positive else 0):
        qualifier = "positive" if positive else "nonnegative"
        raise SupervisorError(f"{label} must be {qualifier}")
    return value


def _nonempty_ascii(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value or not value.isascii():
        raise SupervisorError(f"{label} must be nonempty ASCII")
    return value


def read_semantic_plan(
    path: Path,
    *,
    thread_id: str,
    project_root: Path = PROJECT_ROOT,
) -> SemanticPlan:
    """Read the exact checksum-bound semantic step that may authorize a resume."""

    payload, raw = _load_strict_json(path, label="semantic plan")
    if set(payload) != SEMANTIC_PLAN_KEYS:
        raise SupervisorError("semantic plan schema drifted")
    if payload.get("schema_version") != SEMANTIC_PLAN_SCHEMA_VERSION:
        raise SupervisorError("semantic plan schema version drifted")
    if payload.get("thread_id") != thread_id:
        raise SupervisorError("semantic plan targets a different thread")
    plan_epoch = _nonnegative_int(
        payload.get("plan_epoch"),
        label="semantic plan epoch",
        positive=True,
    )
    state = payload.get("state")
    if not isinstance(state, str) or state not in SEMANTIC_STATES:
        raise SupervisorError("semantic plan state is invalid")
    step = payload.get("step")
    if not isinstance(step, dict) or set(step) != SEMANTIC_STEP_KEYS:
        raise SupervisorError("semantic plan step schema drifted")
    step_state = step.get("state")
    if not isinstance(step_state, str) or step_state not in SEMANTIC_STATES:
        raise SupervisorError("semantic plan step state is invalid")
    accepted_states = step.get("accepted_receipt_states")
    if not isinstance(accepted_states, list) or not accepted_states:
        raise SupervisorError("semantic plan accepted receipt states must be nonempty")
    normalized_states = tuple(
        _nonempty_ascii(value, label="semantic plan accepted receipt state")
        for value in accepted_states
    )
    if len(set(normalized_states)) != len(normalized_states):
        raise SupervisorError("semantic plan accepted receipt states must be unique")
    directive_sha256 = step.get("directive_sha256")
    if (
        not isinstance(directive_sha256, str)
        or len(directive_sha256) != 64
        or any(character not in "0123456789abcdefABCDEF" for character in directive_sha256)
    ):
        raise SupervisorError("semantic plan directive sha256 is malformed")
    resolved_project = project_root.expanduser().resolve()
    return SemanticPlan(
        sha256=hashlib.sha256(raw).hexdigest(),
        plan_epoch=plan_epoch,
        state=str(state),
        step_id=_nonempty_ascii(step.get("step_id"), label="semantic plan step id"),
        step_state=str(step_state),
        max_model_calls=_nonnegative_int(
            step.get("max_model_calls"),
            label="semantic plan max model calls",
        ),
        max_total_tokens=_nonnegative_int(
            step.get("max_total_tokens"),
            label="semantic plan max total tokens",
        ),
        expected_receipt_path=_absolute_path_within(
            step.get("expected_receipt_path"),
            resolved_project / "work",
            label="semantic plan expected receipt path",
        ),
        accepted_receipt_states=normalized_states,
        directive_path=_absolute_path_within(
            step.get("directive_path"),
            resolved_project,
            label="semantic plan directive path",
        ),
        directive_sha256=directive_sha256.lower(),
    )


def validate_semantic_directive(plan: SemanticPlan) -> None:
    if not plan.directive_path.is_file():
        raise SupervisorError("semantic plan directive is unavailable")
    try:
        raw = plan.directive_path.read_bytes()
        actual = hashlib.sha256(raw).hexdigest()
    except OSError as exc:
        raise SupervisorError("semantic plan directive is unreadable") from exc
    if actual != plan.directive_sha256:
        raise SupervisorError("semantic plan directive checksum drifted")
    try:
        directive = json.loads(raw, object_pairs_hook=_unique_json_object)
    except SupervisorError:
        raise
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise SupervisorError("semantic plan directive is malformed") from exc
    if not isinstance(directive, dict):
        raise SupervisorError("semantic plan directive must be a JSON object")
    if directive.get("thread_id") != TARGET_THREAD_ID:
        raise SupervisorError("semantic plan directive targets a different thread")
    directive_epoch = _nonnegative_int(
        directive.get("plan_epoch"),
        label="semantic plan directive epoch",
        positive=True,
    )
    if directive_epoch != plan.plan_epoch:
        raise SupervisorError("semantic plan directive plan epoch drifted")
    if directive.get("step_id") != plan.step_id:
        raise SupervisorError("semantic plan directive step id drifted")
    directive_receipt = directive.get("expected_receipt_path")
    if (
        not isinstance(directive_receipt, str)
        or not Path(directive_receipt).is_absolute()
        or Path(directive_receipt).expanduser().resolve()
        != plan.expected_receipt_path
    ):
        raise SupervisorError("semantic plan directive receipt path drifted")


def read_semantic_step_receipt(plan: SemanticPlan) -> SemanticStepReceipt | None:
    path = plan.expected_receipt_path
    if not path.exists():
        return None
    payload, _raw = _load_strict_json(path, label="semantic step receipt")
    if payload.get("schema_version") != SEMANTIC_STEP_RECEIPT_SCHEMA_VERSION:
        raise SupervisorError("semantic step receipt schema version drifted")
    if payload.get("thread_id") != TARGET_THREAD_ID:
        raise SupervisorError("semantic step receipt targets a different thread")
    receipt_epoch = _nonnegative_int(
        payload.get("plan_epoch"),
        label="semantic step receipt epoch",
        positive=True,
    )
    if receipt_epoch != plan.plan_epoch:
        raise SupervisorError("semantic step receipt plan epoch drifted")
    if payload.get("step_id") != plan.step_id:
        raise SupervisorError("semantic step receipt step id drifted")
    state = payload.get("state")
    if state not in plan.accepted_receipt_states:
        raise SupervisorError("semantic step receipt state is not accepted")
    return SemanticStepReceipt(state=str(state))


def evaluate_semantic_plan(
    plan: SemanticPlan,
) -> tuple[str | None, SemanticStepReceipt | None]:
    waiting_reason = plan.waiting_reason
    if waiting_reason is not None:
        return waiting_reason, None
    receipt = read_semantic_step_receipt(plan)
    if receipt is not None:
        # A terminal step receipt is a safe continuation checkpoint, not the
        # completion of the thread's overall Goal.  The next app-native turn
        # must consume the receipt and advance to its declared branch without
        # replaying the completed semantic call.  Treating the receipt as a
        # no-op gate strands passed, rejected, and recovery-required steps.
        return None, receipt
    validate_semantic_directive(plan)
    return None, None


def _semantic_plan_receipt(
    plan: SemanticPlan,
    *,
    gate_reason: str | None = None,
    step_receipt: SemanticStepReceipt | None = None,
) -> dict[str, Any]:
    if gate_reason is not None:
        dispatch_mode = "recovery_or_successor_plan_only"
    elif step_receipt is not None:
        dispatch_mode = "receipt_branch_only"
    else:
        dispatch_mode = "current_semantic_step_authorized"
    return {
        "semantic_plan_sha256": plan.sha256,
        "semantic_plan_epoch": plan.plan_epoch,
        "semantic_plan_state": plan.state,
        "semantic_plan_step_id": plan.step_id,
        "semantic_plan_step_state": plan.step_state,
        "semantic_plan_directive_sha256": plan.directive_sha256,
        "semantic_plan_gate_reason": gate_reason,
        "semantic_step_receipt_state": (
            step_receipt.state if step_receipt is not None else None
        ),
        "semantic_dispatch_mode": dispatch_mode,
        "current_semantic_call_authorized": dispatch_mode
        == "current_semantic_step_authorized",
    }


def read_operator_hold_marker(path: Path, *, thread_id: str) -> OperatorHoldState:
    """Validate a privacy-safe operator hold that is independent of Goal status."""

    source = path.expanduser().resolve()
    if not source.exists():
        return OperatorHoldState("absent", None, None)
    try:
        raw = source.read_bytes()
        payload = json.loads(raw, object_pairs_hook=_unique_json_object)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError, SupervisorError):
        return OperatorHoldState("invalid", None, None)
    digest = hashlib.sha256(raw).hexdigest()
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "thread_id",
        "state",
        "authorized_by",
        "hold_id",
        "created_at",
        "reason_sha256",
    }:
        return OperatorHoldState("invalid", digest, None)
    hold_id = payload.get("hold_id")
    reason_sha256 = payload.get("reason_sha256")
    created_at = payload.get("created_at")
    try:
        parsed_hold_id = str(uuid.UUID(hold_id)) if isinstance(hold_id, str) else None
    except ValueError:
        parsed_hold_id = None
    valid = (
        payload.get("schema_version") == OPERATOR_HOLD_SCHEMA_VERSION
        and payload.get("thread_id") == thread_id
        and payload.get("state") == "active"
        and payload.get("authorized_by") == "kolby"
        and parsed_hold_id == hold_id
        and _parse_timestamp(created_at) is not None
        and isinstance(reason_sha256, str)
        and len(reason_sha256) == 64
        and all(character in "0123456789abcdef" for character in reason_sha256)
    )
    return OperatorHoldState(
        "valid" if valid else "invalid",
        digest,
        hashlib.sha256(hold_id.encode("ascii")).hexdigest()
        if valid and isinstance(hold_id, str)
        else None,
    )


def _sha256_value(value: Any, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise SupervisorError(f"{label} sha256 is malformed")
    return value


def _artifact_record(path: Path, root: Path, *, label: str) -> dict[str, Any]:
    resolved = _absolute_path_within(str(path), root, label=label)
    if not resolved.is_file() or resolved.is_symlink():
        raise SupervisorError(f"{label} is unavailable or symlinked")
    raw = resolved.read_bytes()
    return {
        "path": str(resolved),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
    }


def _verify_bound_records(
    records: Any,
    *,
    expected_roles: set[str],
    root: Path,
    label: str,
) -> dict[str, dict[str, Any]]:
    if not isinstance(records, dict) or set(records) != expected_roles:
        raise SupervisorError(f"{label} record roles drifted")
    verified: dict[str, dict[str, Any]] = {}
    for role in sorted(expected_roles):
        record = records.get(role)
        if not isinstance(record, dict) or set(record) != {
            "path",
            "sha256",
            "size_bytes",
        }:
            raise SupervisorError(f"{label} record schema drifted")
        actual = _artifact_record(Path(str(record.get("path"))), root, label=label)
        if record != actual:
            raise SupervisorError(f"{label} record binding drifted")
        verified[role] = actual
    return verified


def _run_completion_source_verifier(
    criterion: str,
    source_path: Path,
    project_root: Path,
) -> dict[str, Any]:
    """Invoke existing read-only receipt verifiers in a clean project Python."""

    common_tail = """
raw = source.read_bytes()
result = {
    'criterion': criterion,
    'passed': bool(passed),
    'source_sha256': hashlib.sha256(raw).hexdigest(),
    'source_size_bytes': len(raw),
}
print(json.dumps(result, sort_keys=True, separators=(',', ':')))
"""
    if criterion == "development_quality":
        body = """
from research_factory.app_server_candidate_expanded_cap_direct_reference import verify_receipt
receipt = verify_receipt(source.parent)
ratio = receipt.get('production_amortized_total_token_ratio')
macro = receipt.get('candidate_strict_full_field_macro_f1')
passed = (
    receipt.get('state') == 'passed'
    and receipt.get('development_quality_passed') is True
    and receipt.get('accounting_complete') is True
    and type(ratio) in (int, float) and 0 <= ratio <= 0.28
    and type(macro) in (int, float) and macro >= 0.97
    and receipt.get('candidate_noninferiority_delta', -1) >= 0
    and receipt.get('exact_evidence_rate') == 1
)
"""
    elif criterion == "untouched_holdout":
        body = """
from research_factory.pipeline_babysitter import verify_evaluation_receipt
verified = verify_evaluation_receipt(source, project / 'work' / 'app-server-development-v2')
ratio = verified.get('production_amortized_total_token_ratio')
passed = type(ratio) in (int, float) and 0 <= ratio <= 0.28
"""
    elif criterion == "eligible_extraction_backlog_zero":
        body = """
from research_factory.pipeline_babysitter import completion_audit
audit = completion_audit(project / 'data' / 'factory.sqlite', source)
passed = audit.get('pass') is True and all(
    type(value) is int and value == 0
    for value in (audit.get('blocking_counts') or {}).values()
)
"""
    else:
        raise SupervisorError("completion source has no external verifier")
    script = (
        "import hashlib,json,sys\n"
        "from pathlib import Path\n"
        "source=Path(sys.argv[1]).expanduser().resolve()\n"
        "project=Path(sys.argv[2]).expanduser().resolve()\n"
        f"criterion={criterion!r}\n"
        + body
        + common_tail
    )
    try:
        completed = subprocess.run(
            [sys.executable, "-c", script, str(source_path), str(project_root)],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SupervisorError("completion source verifier failed") from exc
    if completed.returncode != 0:
        raise SupervisorError("completion source verifier rejected its receipt")
    try:
        result = json.loads(completed.stdout)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise SupervisorError("completion source verifier output is malformed") from exc
    if not isinstance(result, dict):
        raise SupervisorError("completion source verifier output is malformed")
    return result


def _verify_production_cutover_source(
    source_path: Path,
    project_root: Path,
) -> dict[str, Any]:
    project_root = project_root.expanduser().resolve()
    source_path = source_path.expanduser().resolve()
    payload, raw = _load_strict_json(source_path, label="production cutover receipt")
    expected_keys = {
        "schema_version",
        "thread_id",
        "cutover_id",
        "frozen_configuration_sha256",
        "winner_system_id",
        "model",
        "reasoning_effort",
        "batch_size",
        "state",
        "transport",
        "auth_mode",
        "persistent_transport",
        "structured_output",
        "production_verified",
        "records",
    }
    if set(payload) != expected_keys or payload != {
        **payload,
        "schema_version": "pif_production_app_server_cutover_receipt_v1",
        "thread_id": TARGET_THREAD_ID,
        "state": "passed",
        "transport": "official_codex_app_server_stdio_managed_chatgpt_auth",
        "auth_mode": "chatgpt",
        "persistent_transport": True,
        "structured_output": True,
        "production_verified": True,
    }:
        raise SupervisorError("production cutover receipt contract drifted")
    cutover_id = payload.get("cutover_id")
    if (
        not isinstance(cutover_id, str)
        or not cutover_id
        or not cutover_id.isascii()
        or cutover_id != cutover_id.strip()
    ):
        raise SupervisorError("production cutover id is invalid")
    records = _verify_bound_records(
        payload.get("records"),
        expected_roles={
            "evaluation_receipt",
            "frozen_configuration",
            "production_runner",
            "runtime_config",
            "smoke_receipt",
            "smoke_output",
            "smoke_sidecar",
            "usage_telemetry",
            "winning_output_schema",
            "winning_prompt",
        },
        root=project_root,
        label="production cutover source",
    )
    evaluation, _ = _load_strict_json(
        Path(records["evaluation_receipt"]["path"]),
        label="production cutover evaluation receipt",
    )
    frozen_configuration, _ = _load_strict_json(
        Path(records["frozen_configuration"]["path"]),
        label="frozen production winner configuration",
    )
    winning_output_schema, _ = _load_strict_json(
        Path(records["winning_output_schema"]["path"]),
        label="winning production output schema",
    )
    try:
        winning_prompt_raw = Path(records["winning_prompt"]["path"]).read_bytes()
        winning_prompt = winning_prompt_raw.decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise SupervisorError("winning production prompt is malformed") from exc
    if not winning_prompt.strip():
        raise SupervisorError("winning production prompt is empty")
    canonical_output_schema = json.dumps(
        winning_output_schema,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    prompt_sha256 = hashlib.sha256(winning_prompt_raw).hexdigest()
    output_schema_sha256 = hashlib.sha256(canonical_output_schema).hexdigest()
    frozen_configuration_sha256 = records["frozen_configuration"]["sha256"]
    expected_frozen_configuration_keys = {
        "schema_version",
        "winner_system_id",
        "model",
        "reasoning_effort",
        "batch_size",
        "transport",
        "auth_mode",
        "persistent_transport",
        "structured_output",
        "winning_prompt",
        "prompt_sha256",
        "winning_output_schema",
        "output_schema_sha256",
    }
    winner_system_id = frozen_configuration.get("winner_system_id")
    model = frozen_configuration.get("model")
    reasoning_effort = frozen_configuration.get("reasoning_effort")
    batch_size = frozen_configuration.get("batch_size")
    if (
        set(frozen_configuration) != expected_frozen_configuration_keys
        or frozen_configuration.get("schema_version")
        != "pif_frozen_app_server_winner_configuration_v1"
        or not isinstance(winner_system_id, str)
        or not winner_system_id.strip()
        or not winner_system_id.isascii()
        or winner_system_id != winner_system_id.strip()
        or not isinstance(model, str)
        or not model.strip()
        or not model.isascii()
        or model != model.strip()
        or reasoning_effort not in {"low", "medium", "high", "xhigh"}
        or isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or batch_size not in {3, 5, 8}
        or frozen_configuration.get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or frozen_configuration.get("auth_mode") != "chatgpt"
        or frozen_configuration.get("persistent_transport") is not True
        or frozen_configuration.get("structured_output") is not True
        or frozen_configuration.get("winning_prompt") != records["winning_prompt"]
        or frozen_configuration.get("prompt_sha256") != prompt_sha256
        or frozen_configuration.get("winning_output_schema")
        != records["winning_output_schema"]
        or frozen_configuration.get("output_schema_sha256")
        != output_schema_sha256
        or payload.get("frozen_configuration_sha256")
        != frozen_configuration_sha256
        or payload.get("winner_system_id") != winner_system_id
        or payload.get("model") != model
        or payload.get("reasoning_effort") != reasoning_effort
        or payload.get("batch_size") != batch_size
    ):
        raise SupervisorError("frozen production winner configuration drifted")
    evaluation_frozen_sha256 = _sha256_value(
        evaluation.get("frozen_configuration_sha256"),
        label="evaluation frozen configuration",
    )
    if (
        evaluation.get("schema_version") != "pif_pipeline_evaluation_receipt_v2"
        or evaluation_frozen_sha256 != frozen_configuration_sha256
    ):
        raise SupervisorError("production cutover is not bound to the evaluation winner")
    evaluation_artifacts = evaluation.get("artifact_hashes")
    development_binding = (
        evaluation_artifacts.get("development_freeze")
        if isinstance(evaluation_artifacts, dict)
        else None
    )
    if (
        not isinstance(development_binding, dict)
        or set(development_binding) != {"path", "sha256"}
    ):
        raise SupervisorError("evaluation winner identity binding is unavailable")
    development_path = Path(str(development_binding.get("path")))
    evaluation_root = project_root / "work" / "app-server-development-v2"
    if not development_path.is_absolute():
        development_path = evaluation_root / development_path
    development_record = _artifact_record(
        development_path,
        evaluation_root,
        label="evaluation development winner",
    )
    if development_record["sha256"] != development_binding.get("sha256"):
        raise SupervisorError("evaluation winner identity binding drifted")
    development, _ = _load_strict_json(
        Path(development_record["path"]),
        label="evaluation development winner",
    )
    if (
        development.get("artifact_role") != "development_freeze"
        or development.get("development_frozen") is not True
        or development.get("selection_frozen") is not True
        or development.get("development_winner_frozen") is not True
        or development.get("frozen_configuration_sha256")
        != frozen_configuration_sha256
        or development.get("winner_system_id") != winner_system_id
    ):
        raise SupervisorError("production cutover winner identity drifted")
    runtime, _ = _load_strict_json(
        Path(records["runtime_config"]["path"]),
        label="production runtime config",
    )
    smoke, _ = _load_strict_json(
        Path(records["smoke_receipt"]["path"]),
        label="production smoke receipt",
    )
    usage, _ = _load_strict_json(
        Path(records["usage_telemetry"]["path"]),
        label="production usage telemetry",
    )
    runner_path = Path(records["production_runner"]["path"])
    try:
        runner_path.relative_to(project_root / "research_factory")
    except ValueError as exc:
        raise SupervisorError("production runner escapes research_factory") from exc
    if runner_path.suffix != ".py" or records["production_runner"]["size_bytes"] < 1:
        raise SupervisorError("production runner is invalid")
    expected_runtime_keys = {
        "schema_version",
        "thread_id",
        "cutover_id",
        "transport",
        "auth_mode",
        "persistent_transport",
        "structured_output",
        "production_runner",
        "evaluation_receipt",
        "frozen_configuration",
        "frozen_configuration_sha256",
        "winning_prompt",
        "winning_output_schema",
        "winner_system_id",
        "model",
        "reasoning_effort",
        "batch_size",
    }
    expected_smoke_keys = {
        "schema_version",
        "thread_id",
        "cutover_id",
        "state",
        "transport",
        "auth_mode",
        "runtime_config",
        "output",
        "sidecar",
        "usage_telemetry",
        "validated_output_count",
        "production_mutated",
        "frozen_configuration_sha256",
        "winner_system_id",
        "model",
        "reasoning_effort",
        "batch_size",
        "prompt_sha256",
        "output_schema_sha256",
    }
    expected_usage_keys = {
        "schema_version",
        "thread_id",
        "cutover_id",
        "transport",
        "auth_mode",
        "persistent_transport",
        "structured_output",
        "accounting_complete",
        "cache_telemetry_complete",
        "measured_turn_count",
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
        "wall_time_seconds",
        "turn_sidecars",
        "frozen_configuration_sha256",
        "winner_system_id",
        "model",
        "reasoning_effort",
        "batch_size",
    }
    if (
        set(runtime) != expected_runtime_keys
        or
        runtime.get("schema_version") != "pif_production_app_server_runtime_config_v1"
        or runtime.get("thread_id") != TARGET_THREAD_ID
        or runtime.get("cutover_id") != cutover_id
        or runtime.get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or runtime.get("auth_mode") != "chatgpt"
        or runtime.get("persistent_transport") is not True
        or runtime.get("structured_output") is not True
        or runtime.get("production_runner") != records["production_runner"]
        or runtime.get("evaluation_receipt") != records["evaluation_receipt"]
        or runtime.get("frozen_configuration") != records["frozen_configuration"]
        or runtime.get("frozen_configuration_sha256")
        != frozen_configuration_sha256
        or runtime.get("winning_prompt") != records["winning_prompt"]
        or runtime.get("winning_output_schema")
        != records["winning_output_schema"]
        or runtime.get("winner_system_id") != winner_system_id
        or runtime.get("model") != model
        or runtime.get("reasoning_effort") != reasoning_effort
        or runtime.get("batch_size") != batch_size
        or set(smoke) != expected_smoke_keys
        or smoke.get("schema_version") != "pif_production_app_server_smoke_receipt_v1"
        or smoke.get("thread_id") != TARGET_THREAD_ID
        or smoke.get("cutover_id") != cutover_id
        or smoke.get("state") != "passed"
        or smoke.get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or smoke.get("auth_mode") != "chatgpt"
        or smoke.get("runtime_config") != records["runtime_config"]
        or smoke.get("output") != records["smoke_output"]
        or smoke.get("sidecar") != records["smoke_sidecar"]
        or smoke.get("usage_telemetry") != records["usage_telemetry"]
        or isinstance(smoke.get("validated_output_count"), bool)
        or not isinstance(smoke.get("validated_output_count"), int)
        or smoke["validated_output_count"] < 1
        or smoke.get("production_mutated") is not True
        or smoke.get("frozen_configuration_sha256")
        != frozen_configuration_sha256
        or smoke.get("winner_system_id") != winner_system_id
        or smoke.get("model") != model
        or smoke.get("reasoning_effort") != reasoning_effort
        or smoke.get("batch_size") != batch_size
        or smoke.get("prompt_sha256") != prompt_sha256
        or smoke.get("output_schema_sha256") != output_schema_sha256
        or set(usage) != expected_usage_keys
        or usage.get("schema_version")
        != "pif_production_app_server_usage_telemetry_v1"
        or usage.get("thread_id") != TARGET_THREAD_ID
        or usage.get("cutover_id") != cutover_id
        or usage.get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or usage.get("auth_mode") != "chatgpt"
        or usage.get("persistent_transport") is not True
        or usage.get("structured_output") is not True
        or usage.get("accounting_complete") is not True
        or usage.get("cache_telemetry_complete") is not True
        or usage.get("frozen_configuration_sha256")
        != frozen_configuration_sha256
        or usage.get("winner_system_id") != winner_system_id
        or usage.get("model") != model
        or usage.get("reasoning_effort") != reasoning_effort
        or usage.get("batch_size") != batch_size
        or isinstance(usage.get("measured_turn_count"), bool)
        or not isinstance(usage.get("measured_turn_count"), int)
        or usage["measured_turn_count"] < 2
    ):
        raise SupervisorError("production cutover source records did not pass")
    usage_fields = (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )
    if any(
        isinstance(usage.get(field), bool)
        or not isinstance(usage.get(field), int)
        or usage[field] < 0
        for field in usage_fields
    ):
        raise SupervisorError("production usage token telemetry is invalid")
    wall_time = usage.get("wall_time_seconds")
    if (
        isinstance(wall_time, bool)
        or not isinstance(wall_time, (int, float))
        or not 0 < float(wall_time) < float("inf")
        or usage["cached_input_tokens"] > usage["input_tokens"]
        or usage["reasoning_output_tokens"] > usage["output_tokens"]
        or usage["total_tokens"]
        != usage["input_tokens"] + usage["output_tokens"]
    ):
        raise SupervisorError("production usage telemetry is inconsistent")
    sidecar_records = usage.get("turn_sidecars")
    if (
        not isinstance(sidecar_records, list)
        or len(sidecar_records) != usage.get("measured_turn_count")
        or not sidecar_records
    ):
        raise SupervisorError("production turn sidecar coverage is incomplete")
    aggregate = {field: 0 for field in usage_fields}
    aggregate_wall = 0.0
    seen_turns: set[tuple[str, str]] = set()
    seen_thread_ids: set[str] = set()
    seen_batch_sizes: set[int] = set()
    verified_sidecar_records: list[dict[str, Any]] = []
    for record in sidecar_records:
        if not isinstance(record, dict) or set(record) != {
            "path",
            "sha256",
            "size_bytes",
        }:
            raise SupervisorError("production turn sidecar binding drifted")
        actual = _artifact_record(
            Path(str(record.get("path"))),
            project_root / "work",
            label="production turn sidecar",
        )
        if actual != record:
            raise SupervisorError("production turn sidecar checksum drifted")
        verified_sidecar_records.append(actual)
        sidecar, _ = _load_strict_json(
            Path(actual["path"]),
            label="production app-server turn sidecar",
        )
        sidecar_usage = sidecar.get("usage")
        turn_identity = (sidecar.get("thread_id"), sidecar.get("turn_id"))
        if (
            sidecar.get("schema_version") != "pif_codex_app_server_turn_v2"
            or sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("transport") != "stdio"
            or sidecar.get("auth_type") != "chatgpt"
            or sidecar.get("thread_mode") != "same_thread"
            or sidecar.get("model") != model
            or sidecar.get("effort") != reasoning_effort
            or isinstance(sidecar.get("batch_size"), bool)
            or not isinstance(sidecar.get("batch_size"), int)
            or not 1 <= sidecar["batch_size"] <= batch_size
            or sidecar.get("base_instructions_sha256") != prompt_sha256
            or sidecar.get("base_instructions_bytes") != len(winning_prompt_raw)
            or not isinstance(sidecar.get("prompt_sha256"), str)
            or len(sidecar["prompt_sha256"]) != 64
            or any(
                character not in "0123456789abcdef"
                for character in sidecar["prompt_sha256"]
            )
            or isinstance(sidecar.get("prompt_bytes"), bool)
            or not isinstance(sidecar.get("prompt_bytes"), int)
            or sidecar["prompt_bytes"] < 1
            or sidecar.get("output_schema_sha256") != output_schema_sha256
            or sidecar.get("output_schema_bytes") != len(canonical_output_schema)
            or sidecar.get("usage_complete") is not True
            or sidecar.get("usage_status") != "measured"
            or not isinstance(turn_identity[0], str)
            or not turn_identity[0]
            or not isinstance(turn_identity[1], str)
            or not turn_identity[1]
            or turn_identity in seen_turns
            or not isinstance(sidecar_usage, dict)
        ):
            raise SupervisorError("production app-server sidecar did not pass")
        seen_turns.add(turn_identity)
        seen_thread_ids.add(turn_identity[0])
        seen_batch_sizes.add(sidecar["batch_size"])
        if any(
            isinstance(sidecar_usage.get(field), bool)
            or not isinstance(sidecar_usage.get(field), int)
            or sidecar_usage[field] < 0
            for field in usage_fields
        ):
            raise SupervisorError("production sidecar usage is invalid")
        if (
            sidecar_usage["cached_input_tokens"] > sidecar_usage["input_tokens"]
            or sidecar_usage["reasoning_output_tokens"]
            > sidecar_usage["output_tokens"]
            or sidecar_usage["total_tokens"]
            != sidecar_usage["input_tokens"] + sidecar_usage["output_tokens"]
        ):
            raise SupervisorError("production sidecar usage is inconsistent")
        sidecar_wall = sidecar.get("wall_elapsed_seconds")
        if (
            isinstance(sidecar_wall, bool)
            or not isinstance(sidecar_wall, (int, float))
            or float(sidecar_wall) <= 0
        ):
            raise SupervisorError("production sidecar wall time is invalid")
        output_path = Path(str(sidecar.get("output_path") or "")).expanduser().resolve()
        output_record = _artifact_record(
            output_path,
            project_root / "work",
            label="production structured output",
        )
        output_raw = output_path.read_bytes()
        accepted_output_hashes = {hashlib.sha256(output_raw).hexdigest()}
        if output_raw.endswith(b"\n"):
            accepted_output_hashes.add(hashlib.sha256(output_raw[:-1]).hexdigest())
        if sidecar.get("output_sha256") not in accepted_output_hashes:
            raise SupervisorError("production output lineage drifted")
        try:
            output_payload, _ = _load_strict_json(
                output_path,
                label="production structured output",
            )
            _validate_schema(winning_output_schema, output_payload, path="$")
        except (SupervisorError, ValidationError) as exc:
            raise SupervisorError(
                "production output did not validate against the frozen schema"
            ) from exc
        if (
            actual == records["smoke_sidecar"]
            and output_record != records["smoke_output"]
        ):
            raise SupervisorError("production smoke output lineage drifted")
        for field in usage_fields:
            aggregate[field] += sidecar_usage[field]
        aggregate_wall += float(sidecar_wall)
    if records["smoke_sidecar"] not in verified_sidecar_records:
        raise SupervisorError("production smoke sidecar is outside usage lineage")
    if len(seen_thread_ids) != 1:
        raise SupervisorError("production transport did not preserve one app-server thread")
    if batch_size not in seen_batch_sizes:
        raise SupervisorError("production telemetry never exercised the frozen batch size")
    if any(usage[field] != aggregate[field] for field in usage_fields) or abs(
        float(wall_time) - aggregate_wall
    ) > 1e-6:
        raise SupervisorError("production usage aggregation drifted")
    return {
        "criterion": "production_app_server_cutover",
        "passed": True,
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "source_size_bytes": len(raw),
    }


def _verify_notification_readiness_source(
    source_path: Path,
    project_root: Path,
) -> dict[str, Any]:
    payload, raw = _load_strict_json(source_path, label="notification readiness receipt")
    if set(payload) != {
        "schema_version",
        "thread_id",
        "state",
        "dedupe_key",
        "codex_ops",
        "telegram_config",
    }:
        raise SupervisorError("notification readiness receipt schema drifted")
    if (
        payload.get("schema_version")
        != "pif_completion_notification_readiness_receipt_v1"
        or payload.get("thread_id") != TARGET_THREAD_ID
        or payload.get("state") != "ready"
        or payload.get("dedupe_key") != "pif-evaluation-goal-complete"
    ):
        raise SupervisorError("notification readiness receipt did not pass")
    record = payload.get("codex_ops")
    if not isinstance(record, dict) or set(record) != {
        "path",
        "sha256",
        "size_bytes",
    }:
        raise SupervisorError("notification readiness codex-ops binding drifted")
    if Path(str(record.get("path"))).expanduser().resolve() != DEFAULT_CODEX_OPS.resolve():
        raise SupervisorError("notification readiness targets a different codex-ops")
    if _artifact_record(
        DEFAULT_CODEX_OPS,
        CODEX_HOME,
        label="notification readiness codex-ops",
    ) != record:
        raise SupervisorError("notification readiness codex-ops checksum drifted")
    if not os.access(DEFAULT_CODEX_OPS, os.X_OK):
        raise SupervisorError("notification readiness codex-ops is not executable")
    telegram_config_record = payload.get("telegram_config")
    if not isinstance(telegram_config_record, dict) or set(
        telegram_config_record
    ) != {"path", "sha256", "size_bytes"}:
        raise SupervisorError("notification readiness config binding drifted")
    if (
        Path(str(telegram_config_record.get("path"))).expanduser().resolve()
        != DEFAULT_CODEX_OPS_CONFIG.resolve()
        or _artifact_record(
            DEFAULT_CODEX_OPS_CONFIG,
            CODEX_HOME,
            label="notification readiness Telegram config",
        )
        != telegram_config_record
    ):
        raise SupervisorError("notification readiness Telegram config drifted")
    config, _ = _load_strict_json(
        DEFAULT_CODEX_OPS_CONFIG,
        label="notification readiness Telegram config",
    )
    telegram = config.get("telegram")
    if (
        not isinstance(telegram, dict)
        or telegram.get("enabled", True) is not True
        or telegram.get("transport") != "openclaw-railway"
        or Path(str(telegram.get("repo_path") or "")).expanduser().resolve()
        != EXPECTED_COMPLETION_TELEGRAM_REPO.resolve()
        or telegram.get("service") != EXPECTED_COMPLETION_TELEGRAM_SERVICE
        or telegram.get("channel") != EXPECTED_COMPLETION_TELEGRAM_CHANNEL
        or telegram.get("account_id") != EXPECTED_COMPLETION_TELEGRAM_ACCOUNT
        or Path(str(telegram.get("identity_file") or "")).expanduser().resolve()
        != EXPECTED_COMPLETION_TELEGRAM_IDENTITY.resolve()
        or not EXPECTED_COMPLETION_TELEGRAM_IDENTITY.is_file()
        or telegram.get("source_allowlist")
        != EXPECTED_COMPLETION_TELEGRAM_SOURCE_ALLOWLIST
        or hashlib.sha256(
            str(telegram.get("target") or "").encode("utf-8")
        ).hexdigest()
        != EXPECTED_COMPLETION_TELEGRAM_TARGET_SHA256
    ):
        raise SupervisorError("notification readiness Telegram is not configured")
    return {
        "criterion": "notification_readiness",
        "passed": True,
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "source_size_bytes": len(raw),
    }


def _verify_extraction_completion_manifest_contract(
    source_path: Path,
    project_root: Path,
) -> dict[str, dict[str, Any]]:
    payload, _raw = _load_strict_json(
        source_path,
        label="production extraction completion manifest",
    )
    flags = {
        "all_app_server_usage_accounted",
        "all_outputs_validated",
        "no_unresolved_audit_failures",
        "terminal_quarantine_audited",
    }
    if set(payload) != {
        "schema_version",
        "thread_id",
        "state",
        "records",
        *flags,
    }:
        raise SupervisorError("production extraction completion manifest drifted")
    if (
        payload.get("schema_version")
        != "pif_production_extraction_completion_manifest_v1"
        or payload.get("thread_id") != TARGET_THREAD_ID
        or payload.get("state") != "completed"
        or any(payload.get(flag) is not True for flag in flags)
    ):
        raise SupervisorError("production extraction completion manifest did not pass")
    return _verify_bound_records(
        payload.get("records"),
        expected_roles={"evaluation_receipt", "production_cutover_receipt"},
        root=project_root / "work",
        label="production extraction completion manifest",
    )


def _verify_completion_source(
    criterion: str,
    source_path: Path,
    project_root: Path,
) -> dict[str, Any]:
    if criterion in {
        "development_quality",
        "untouched_holdout",
        "eligible_extraction_backlog_zero",
    }:
        return _run_completion_source_verifier(criterion, source_path, project_root)
    if criterion == "production_app_server_cutover":
        return _verify_production_cutover_source(source_path, project_root)
    if criterion == "notification_readiness":
        return _verify_notification_readiness_source(source_path, project_root)
    raise SupervisorError("completion evidence criterion is unsupported")


def read_completion_authorization(
    path: Path,
    *,
    thread_id: str,
    project_root: Path = PROJECT_ROOT,
    source_verifier: Callable[[str, Path, Path], dict[str, Any]] = (
        _verify_completion_source
    ),
) -> CompletionAuthorization:
    """Validate one immutable, hash-bound end-to-end completion authorization."""

    resolved_work = project_root.expanduser().resolve() / "work"
    authorization_path = path.expanduser().resolve()
    try:
        authorization_path.relative_to(resolved_work)
    except ValueError as exc:
        raise SupervisorError(
            "completion authorization must stay inside the project work directory"
        ) from exc
    payload, raw = _load_strict_json(
        authorization_path,
        label="completion authorization",
    )
    if set(payload) != COMPLETION_AUTHORIZATION_KEYS:
        raise SupervisorError("completion authorization schema drifted")
    if payload.get("schema_version") != END_TO_END_COMPLETION_SCHEMA_VERSION:
        raise SupervisorError("completion authorization schema version drifted")
    if payload.get("thread_id") != thread_id:
        raise SupervisorError("completion authorization targets a different thread")
    if payload.get("state") != "authorized":
        raise SupervisorError("completion authorization is not authorized")
    authorization_id = payload.get("authorization_id")
    try:
        parsed_authorization_id = (
            str(uuid.UUID(authorization_id))
            if isinstance(authorization_id, str)
            else None
        )
    except ValueError:
        parsed_authorization_id = None
    if parsed_authorization_id != authorization_id:
        raise SupervisorError("completion authorization id is malformed")
    authorized_at = payload.get("authorized_at")
    if _parse_timestamp(authorized_at) is None:
        raise SupervisorError("completion authorization timestamp is malformed")
    evidence = payload.get("evidence")
    if not isinstance(evidence, dict) or set(evidence) != set(COMPLETION_CRITERIA):
        raise SupervisorError("completion authorization evidence set drifted")

    evidence_hashes: list[tuple[str, str]] = []
    evidence_paths: set[Path] = set()
    for criterion in COMPLETION_CRITERIA:
        descriptor = evidence.get(criterion)
        if (
            not isinstance(descriptor, dict)
            or set(descriptor) != COMPLETION_EVIDENCE_DESCRIPTOR_KEYS
        ):
            raise SupervisorError("completion evidence descriptor schema drifted")
        evidence_path = _absolute_path_within(
            descriptor.get("path"),
            resolved_work,
            label=f"{criterion} completion evidence path",
        )
        if evidence_path in evidence_paths:
            raise SupervisorError("completion evidence paths must be unique")
        evidence_paths.add(evidence_path)
        expected_sha256 = _sha256_value(
            descriptor.get("sha256"),
            label=f"{criterion} completion evidence",
        )
        expected_size = _nonnegative_int(
            descriptor.get("size_bytes"),
            label=f"{criterion} completion evidence size",
            positive=True,
        )
        expected_schema, expected_verifier = COMPLETION_SOURCE_CONTRACTS[criterion]
        if (
            descriptor.get("schema_version") != expected_schema
            or descriptor.get("verifier") != expected_verifier
        ):
            raise SupervisorError("completion evidence verifier contract drifted")
        evidence_payload, evidence_raw = _load_strict_json(
            evidence_path,
            label=f"{criterion} completion evidence",
        )
        actual_sha256 = hashlib.sha256(evidence_raw).hexdigest()
        if actual_sha256 != expected_sha256 or len(evidence_raw) != expected_size:
            raise SupervisorError("completion evidence checksum or size drifted")
        if evidence_payload.get("schema_version") != expected_schema:
            raise SupervisorError("completion evidence source schema drifted")
        if criterion == "production_app_server_cutover":
            cutover_records = evidence_payload.get("records")
            evaluation_record = (
                cutover_records.get("evaluation_receipt")
                if isinstance(cutover_records, dict)
                else None
            )
            holdout_descriptor = evidence["untouched_holdout"]
            expected_evaluation_record = {
                "path": str(
                    Path(str(holdout_descriptor["path"])).expanduser().resolve()
                ),
                "sha256": holdout_descriptor["sha256"],
                "size_bytes": holdout_descriptor["size_bytes"],
            }
            if evaluation_record != expected_evaluation_record:
                raise SupervisorError(
                    "production cutover evaluation lineage drifted"
                )
        if criterion == "eligible_extraction_backlog_zero":
            manifest_records = _verify_extraction_completion_manifest_contract(
                evidence_path,
                project_root.expanduser().resolve(),
            )
            upstream = {
                "evaluation_receipt": "untouched_holdout",
                "production_cutover_receipt": "production_app_server_cutover",
            }
            for role, upstream_criterion in upstream.items():
                upstream_descriptor = evidence[upstream_criterion]
                expected_record = {
                    "path": str(
                        Path(str(upstream_descriptor["path"])).expanduser().resolve()
                    ),
                    "sha256": upstream_descriptor["sha256"],
                    "size_bytes": upstream_descriptor["size_bytes"],
                }
                if manifest_records[role] != expected_record:
                    raise SupervisorError(
                        "extraction completion manifest upstream binding drifted"
                    )
        verified = source_verifier(
            criterion,
            evidence_path,
            project_root.expanduser().resolve(),
        )
        if verified != {
            "criterion": criterion,
            "passed": True,
            "source_sha256": actual_sha256,
            "source_size_bytes": len(evidence_raw),
        }:
            raise SupervisorError("completion source verifier did not pass exactly")
        evidence_hashes.append((criterion, actual_sha256))

    return CompletionAuthorization(
        sha256=hashlib.sha256(raw).hexdigest(),
        authorization_id=str(authorization_id),
        authorized_at=str(authorized_at),
        evidence_sha256=tuple(evidence_hashes),
    )


def inspect_completion_authorization(
    path: Path,
    *,
    thread_id: str,
    project_root: Path = PROJECT_ROOT,
    source_verifier: Callable[[str, Path, Path], dict[str, Any]] = (
        _verify_completion_source
    ),
) -> CompletionAuthorizationState:
    source = path.expanduser().resolve()
    if not source.exists():
        return CompletionAuthorizationState("absent", None, "absent", None)
    try:
        marker_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    except OSError:
        return CompletionAuthorizationState("invalid", None, "unreadable", None)
    try:
        authorization = read_completion_authorization(
            source,
            thread_id=thread_id,
            project_root=project_root,
            source_verifier=source_verifier,
        )
    except SupervisorError as exc:
        return CompletionAuthorizationState(
            "invalid",
            marker_sha256,
            str(exc),
            None,
        )
    return CompletionAuthorizationState(
        "valid",
        marker_sha256,
        None,
        authorization,
    )


class LifetimeLock:
    def __init__(self, path: Path):
        self.path = path.expanduser().resolve()
        self.handle: Any | None = None

    def __enter__(self) -> LifetimeLock:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.handle = self.path.open("a+", encoding="utf-8")
        os.chmod(self.path, 0o600)
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.handle.close()
            self.handle = None
            raise SupervisorLockUnavailable("another native supervisor owns this cycle") from exc
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(f"pid={os.getpid()} acquired_at={now_iso()}\n")
        self.handle.flush()
        os.fsync(self.handle.fileno())
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self.handle is None:
            return
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


def _parse_timestamp(value: Any) -> dt.datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def read_rollout_tail(
    path: Path,
    *,
    now: dt.datetime | None = None,
    fresh_activity_seconds: float = 30 * 60,
    max_bytes: int = 64 * 1024 * 1024,
) -> RolloutState:
    """Classify only the newest rollout lifecycle from a bounded file tail."""

    source = path.expanduser().resolve()
    if not source.is_file():
        raise SupervisorError("target rollout is unavailable")
    size = source.stat().st_size
    offset = max(0, size - max_bytes)
    with source.open("rb") as handle:
        handle.seek(offset)
        raw = handle.read()
    lines = raw.splitlines()
    if offset and lines:
        lines = lines[1:]

    last_timestamp: str | None = None
    lifecycle: str | None = None
    turn_id: str | None = None
    for raw_line in reversed(lines):
        try:
            record = json.loads(raw_line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if last_timestamp is None and isinstance(record.get("timestamp"), str):
            last_timestamp = record["timestamp"]
        if record.get("type") != "event_msg":
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        event_type = payload.get("type")
        if event_type not in {"task_started", "task_complete", "turn_aborted"}:
            continue
        lifecycle = str(event_type)
        turn_id = payload.get("turn_id") if isinstance(payload.get("turn_id"), str) else None
        break
    if lifecycle is None:
        return RolloutState("unknown", "not_found_in_bounded_tail", None, last_timestamp, None, size)

    current = now or dt.datetime.now(dt.timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=dt.timezone.utc)
    parsed = _parse_timestamp(last_timestamp)
    age = None if parsed is None else max(0.0, (current - parsed.astimezone(current.tzinfo)).total_seconds())
    if lifecycle == "task_started":
        classification = (
            "fresh_open"
            if age is not None and age <= fresh_activity_seconds
            else "stale_open"
        )
    else:
        classification = "idle_terminal"
    return RolloutState(classification, lifecycle, turn_id, last_timestamp, age, size)


def _message_text(payload: dict[str, Any]) -> str:
    content = payload.get("content")
    if not isinstance(content, list):
        return ""
    return "".join(
        str(item.get("text") or "")
        for item in content
        if isinstance(item, dict) and item.get("type") == "input_text"
    )


def _record_turn_id(payload: dict[str, Any]) -> str | None:
    metadata = payload.get("internal_chat_message_metadata_passthrough")
    if isinstance(metadata, dict) and isinstance(metadata.get("turn_id"), str):
        return metadata["turn_id"]
    return None


def update_trigger_evidence(record: dict[str, Any], evidence: TriggerEvidence) -> None:
    record_type = record.get("type")
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return
    if record_type == "event_msg" and payload.get("type") == "task_started":
        turn_id = payload.get("turn_id")
        if isinstance(turn_id, str):
            evidence.task_started_turns.add(turn_id)
        return
    if record_type != "response_item":
        return
    turn_id = _record_turn_id(payload)
    if turn_id is None:
        return
    if payload.get("type") == "message" and payload.get("role") == "user":
        text = _message_text(payload)
        if '<codex_internal_context source="goal">' in text:
            evidence.goal_context_turns.add(turn_id)
        if f"<automation_id>{AUTOMATION_ID}</automation_id>" in text:
            evidence.heartbeat_turns.add(turn_id)
        return
    if payload.get("type") in {"reasoning", "function_call", "custom_tool_call", "message"}:
        evidence.dispatch_activity_turns.add(turn_id)


def read_appended_records(path: Path, offset: int) -> tuple[list[dict[str, Any]], int]:
    source = path.expanduser().resolve()
    size = source.stat().st_size
    if size < offset:
        raise SupervisorError("target rollout was replaced during trigger verification")
    with source.open("rb") as handle:
        handle.seek(offset)
        raw = handle.read()
    records: list[dict[str, Any]] = []
    consumed = 0
    for line in raw.splitlines(keepends=True):
        if not line.endswith((b"\n", b"\r")):
            break
        consumed += len(line)
        try:
            value = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if isinstance(value, dict):
            records.append(value)
    return records, offset + consumed


def _validate_operator_automation_payload(
    payload: dict[str, Any],
    *,
    expected_status: str = "ACTIVE",
) -> dict[str, Any]:
    if set(payload) != EXPECTED_TOML_KEYS:
        raise SupervisorError("operator-owned automation TOML schema drifted")
    expected = {
        "version": 1,
        "id": AUTOMATION_ID,
        "kind": "heartbeat",
        "name": AUTOMATION_NAME,
        "status": expected_status,
        "rrule": AUTOMATION_RRULE,
        "target_thread_id": TARGET_THREAD_ID,
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise SupervisorError(f"operator-owned automation TOML drifted at {key}")
    prompt = payload.get("prompt")
    if not isinstance(prompt, str):
        raise SupervisorError("operator-owned automation prompt is malformed")
    if hashlib.sha256(prompt.encode("utf-8")).hexdigest() != AUTOMATION_PROMPT_SHA256:
        raise SupervisorError("operator-owned automation prompt drifted")
    for key in ("created_at", "updated_at"):
        if isinstance(payload.get(key), bool) or not isinstance(payload.get(key), int):
            raise SupervisorError(f"operator-owned automation {key} is malformed")
    return payload


def validate_operator_automation(path: Path) -> dict[str, Any]:
    source = path.expanduser().resolve()
    if not source.is_file():
        raise SupervisorError("operator-owned automation TOML is unavailable")
    payload = tomllib.loads(source.read_text(encoding="utf-8"))
    return _validate_operator_automation_payload(payload)


def restore_operator_automation(
    target_path: Path,
    canonical_path: Path,
) -> tuple[dict[str, Any], bool]:
    """Restore the target heartbeat from a checksum-bound canonical copy."""

    canonical_source = canonical_path.expanduser().resolve()
    if not canonical_source.is_file():
        raise SupervisorError("canonical operator automation is unavailable")
    canonical_text = canonical_source.read_text(encoding="utf-8")
    canonical = _validate_operator_automation_payload(tomllib.loads(canonical_text))
    target = target_path.expanduser().resolve()
    current_text = target.read_text(encoding="utf-8") if target.is_file() else None
    if current_text == canonical_text:
        return canonical, False
    write_text_atomic(target, canonical_text)
    os.chmod(target, 0o600)
    restored = validate_operator_automation(target)
    if restored != canonical:
        raise SupervisorError("operator-owned automation restoration did not verify")
    return canonical, True


def _pause_operator_automation_tomls(
    target_path: Path,
    canonical_path: Path,
    *,
    updated_at_ms: int,
) -> dict[str, Any]:
    """Persist PAUSED in both Desktop TOML authorities before shutdown.

    The Desktop database alone is not durable across app restart/reconciliation.
    Validate both exact heartbeat files first, then atomically replace only their
    status and timestamp lines while preserving the checksum-bound prompt.
    """

    sources: list[tuple[Path, str, dict[str, Any]]] = []
    for raw_path, label in (
        (target_path, "operator automation"),
        (canonical_path, "canonical operator automation"),
    ):
        path = raw_path.expanduser().resolve()
        if not path.is_file():
            raise SupervisorError(f"{label} TOML is unavailable")
        text = path.read_text(encoding="utf-8")
        try:
            payload = tomllib.loads(text)
        except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
            raise SupervisorError(f"{label} TOML is malformed") from exc
        status = payload.get("status")
        if status not in {"ACTIVE", "PAUSED"}:
            raise SupervisorError(f"{label} TOML has an invalid status")
        _validate_operator_automation_payload(payload, expected_status=status)
        sources.append((path, text, payload))

    rendered: list[tuple[Path, str, str]] = []
    for path, text, payload in sources:
        lines = text.splitlines(keepends=True)
        status_indexes = [
            index for index, line in enumerate(lines) if line.startswith("status = ")
        ]
        updated_indexes = [
            index
            for index, line in enumerate(lines)
            if line.startswith("updated_at = ")
        ]
        if len(status_indexes) != 1 or len(updated_indexes) != 1:
            raise SupervisorError("operator automation TOML rendering is ambiguous")
        status_index = status_indexes[0]
        updated_index = updated_indexes[0]
        status_newline = "\n" if lines[status_index].endswith("\n") else ""
        updated_newline = "\n" if lines[updated_index].endswith("\n") else ""
        lines[status_index] = f'status = "PAUSED"{status_newline}'
        lines[updated_index] = f"updated_at = {updated_at_ms}{updated_newline}"
        paused_text = "".join(lines)
        try:
            paused = tomllib.loads(paused_text)
        except tomllib.TOMLDecodeError as exc:  # pragma: no cover - fixed rendering
            raise SupervisorError("paused operator automation TOML is malformed") from exc
        _validate_operator_automation_payload(paused, expected_status="PAUSED")
        rendered.append((path, paused_text, str(payload["status"])))

    before_statuses: dict[str, str] = {}
    for path, paused_text, before_status in rendered:
        before_statuses[str(path)] = before_status
        if path.read_text(encoding="utf-8") != paused_text:
            write_text_atomic(path, paused_text)
            os.chmod(path, 0o600)
        persisted = tomllib.loads(path.read_text(encoding="utf-8"))
        _validate_operator_automation_payload(persisted, expected_status="PAUSED")
    return {
        "before_statuses": before_statuses,
        "after_status": "PAUSED",
        "already_paused": all(value == "PAUSED" for value in before_statuses.values()),
    }


def reconcile_native_automation_row(
    db_path: Path,
    automation: dict[str, Any],
    *,
    force_due: bool,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Restore one exact native heartbeat row, optionally making it due now."""

    database = db_path.expanduser().resolve()
    if not database.is_file():
        raise SupervisorError("Codex native automation database is unavailable")
    current_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    due_ms = current_ms - 1_000 if force_due else None
    connection = sqlite3.connect(database, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            """
            SELECT id, name, prompt, status, rrule, next_run_at, last_run_at
            FROM automations WHERE id=?
            """,
            (AUTOMATION_ID,),
        ).fetchone()
        if row is None:
            raise SupervisorError("native automation row is unavailable")
        before = {
            "name_matches": row["name"] == automation["name"],
            "prompt_matches": row["prompt"] == automation["prompt"],
            "status": row["status"],
            "rrule": row["rrule"],
            "next_run_at": row["next_run_at"],
            "last_run_at": row["last_run_at"],
        }
        updated = connection.execute(
            """
            UPDATE automations
            SET name=?, prompt=?, status='ACTIVE', rrule=?,
                next_run_at=CASE WHEN ? THEN ? ELSE next_run_at END,
                updated_at=?
            WHERE id=?
            """,
            (
                automation["name"],
                automation["prompt"],
                AUTOMATION_RRULE,
                1 if force_due else 0,
                due_ms,
                current_ms,
                AUTOMATION_ID,
            ),
        )
        if updated.rowcount != 1:
            raise SupervisorError("native automation transaction lost exact-row ownership")
        after = connection.execute(
            "SELECT name, prompt, status, rrule, next_run_at, last_run_at "
            "FROM automations WHERE id=?",
            (AUTOMATION_ID,),
        ).fetchone()
        if after is None:
            raise SupervisorError("native automation row disappeared during verification")
        if (
            after["name"] != automation["name"]
            or after["prompt"] != automation["prompt"]
            or after["status"] != "ACTIVE"
            or after["rrule"] != AUTOMATION_RRULE
            or (force_due and after["next_run_at"] != due_ms)
        ):
            raise SupervisorError("native automation due update did not verify")
        connection.commit()
        return {
            "before_name_matches": before["name_matches"],
            "before_prompt_matches": before["prompt_matches"],
            "before_status": before["status"],
            "before_rrule": before["rrule"],
            "before_next_run_at": before["next_run_at"],
            "before_last_run_at": before["last_run_at"],
            "after_status": after["status"],
            "after_rrule": after["rrule"],
            "after_next_run_at": after["next_run_at"],
            "after_last_run_at": after["last_run_at"],
            "forced_due": force_due,
        }
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def force_native_automation_due(
    db_path: Path,
    automation: dict[str, Any],
    *,
    now_ms: int | None = None,
) -> dict[str, Any]:
    return reconcile_native_automation_row(
        db_path,
        automation,
        force_due=True,
        now_ms=now_ms,
    )


def pause_native_automation_row(
    db_path: Path = DEFAULT_AUTOMATIONS_DB,
    *,
    automation_toml: Path = DEFAULT_AUTOMATION_TOML,
    canonical_automation_toml: Path = DEFAULT_CANONICAL_AUTOMATION_TOML,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Idempotently pause the exact Desktop heartbeat in TOML and SQLite."""

    database = db_path.expanduser().resolve()
    if not database.is_file():
        raise SupervisorError("Codex native automation database is unavailable")
    current_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    toml_pause = _pause_operator_automation_tomls(
        automation_toml,
        canonical_automation_toml,
        updated_at_ms=current_ms,
    )
    connection = sqlite3.connect(database, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT id,status,next_run_at FROM automations WHERE id=?",
            (AUTOMATION_ID,),
        ).fetchone()
        if row is None or row["id"] != AUTOMATION_ID:
            raise SupervisorError("native automation row is unavailable")
        before_status = row["status"]
        before_next_run_at = row["next_run_at"]
        updated = connection.execute(
            "UPDATE automations SET status='PAUSED',next_run_at=NULL,updated_at=? WHERE id=?",
            (current_ms, AUTOMATION_ID),
        )
        if updated.rowcount != 1:
            raise SupervisorError("native automation pause lost exact-row ownership")
        after = connection.execute(
            "SELECT status,next_run_at FROM automations WHERE id=?",
            (AUTOMATION_ID,),
        ).fetchone()
        if (
            after is None
            or after["status"] != "PAUSED"
            or after["next_run_at"] is not None
        ):
            raise SupervisorError("native automation pause did not verify")
        connection.commit()
        return {
            "before_status": before_status,
            "before_next_run_at": before_next_run_at,
            "after_status": after["status"],
            "after_next_run_at": after["next_run_at"],
            "already_paused": (
                before_status == "PAUSED" and toml_pause["already_paused"]
            ),
            "automation_tomls_paused": True,
        }
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def rollout_writer_state(path: Path) -> WriterState:
    source = path.expanduser().resolve()
    try:
        completed = subprocess.run(
            ["lsof", "-F0", "--", str(source)],
            capture_output=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SupervisorError("rollout writer probe failed") from exc
    if completed.returncode not in {0, 1}:
        raise SupervisorError("rollout writer probe failed")
    writers: set[int] = set()
    current_pid: int | None = None
    current_access: str | None = None
    current_name: str | None = None
    for token in completed.stdout.replace(b"\n", b"\0").split(b"\0"):
        if not token:
            continue
        prefix, value = chr(token[0]), token[1:].decode("utf-8", "replace")
        if prefix == "p":
            current_pid = int(value) if value.isdigit() else None
            current_access = None
            current_name = None
        elif prefix == "a":
            current_access = value
        elif prefix == "n":
            current_name = value
            if current_pid is not None and current_access in {"w", "u"} and current_name == str(source):
                writers.add(current_pid)
    desktop_owned = False
    if len(writers) == 1:
        pid = next(iter(writers))
        try:
            process = subprocess.run(
                ["ps", "-p", str(pid), "-o", "command="],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise SupervisorError("rollout writer identity probe failed") from exc
        command = process.stdout.strip()
        desktop_owned = (
            process.returncode == 0
            and command.startswith("/Applications/ChatGPT.app/Contents/Resources/codex ")
            and " app-server" in command
        )
    return WriterState(len(writers), desktop_owned, tuple(sorted(writers)))


def pretrigger_writer_state_is_safe(
    rollout: RolloutState,
    writers: WriterState,
) -> bool:
    """Validate rollout ownership before a native trigger.

    A completed/aborted Desktop turn normally closes its rollout descriptor, so
    an idle terminal may safely have zero writers. An open turn must still have
    exactly one Desktop-owned writer. Any foreign or multiple writer remains a
    hard failure. Post-trigger verification separately requires the new turn to
    acquire exactly one Desktop-owned writer.
    """

    if rollout.classification == "idle_terminal" and writers.writer_count == 0:
        return True
    return writers.writer_count == 1 and writers.desktop_owned


async def verify_native_trigger(
    config: SupervisorConfig,
    *,
    start_offset: int,
    writer_probe: Callable[[Path], WriterState] = rollout_writer_state,
) -> dict[str, Any]:
    deadline = time.monotonic() + config.verify_timeout_seconds
    offset = start_offset
    evidence = TriggerEvidence()
    while True:
        records, offset = read_appended_records(config.rollout_path, offset)
        for record in records:
            update_trigger_evidence(record, evidence)
        turn_id = evidence.verified_turn_id()
        if turn_id is not None:
            writers = writer_probe(config.rollout_path)
            if writers.writer_count == 1 and writers.desktop_owned:
                return {
                    "turn_id": turn_id,
                    "native_turn_started": True,
                    "dispatch_verified": True,
                    "goal_context": True,
                    "heartbeat_context": True,
                    "dispatch_activity_verified": True,
                    "semantic_progress_verified": False,
                    "writer_count": writers.writer_count,
                    "desktop_owned": True,
                    "end_offset": offset,
                }
        if time.monotonic() >= deadline:
            raise SupervisorError("native automation trigger was not fully verified")
        await asyncio.sleep(config.verify_poll_seconds)


GoalController = Callable[..., Awaitable[dict[str, Any]]]
CompletionController = Callable[..., dict[str, Any]]


def _validated_goal_state(
    result: dict[str, Any],
    *,
    thread_id: str,
    dispatch_actions: set[str],
) -> GoalState:
    if result.get("thread_id") != thread_id:
        raise SupervisorError("goal preflight returned a different thread")
    if result.get("auth_mode") != "chatgpt":
        raise SupervisorError("goal preflight did not verify managed ChatGPT auth")
    if not all(
        result.get(field) is True
        for field in (
            "objective_preserved",
            "token_budget_preserved",
            "accounting_preserved",
        )
    ):
        raise SupervisorError("goal preflight did not preserve the goal contract")
    before_status = result.get("before_status")
    after_status = result.get("after_status")
    action = result.get("action")
    if not all(isinstance(value, str) for value in (before_status, after_status, action)):
        raise SupervisorError("goal preflight returned malformed status")
    return GoalState(
        before_status=before_status,
        after_status=after_status,
        action=action,
        dispatch_allowed=after_status == "active" and action in dispatch_actions,
        complete=after_status == "complete" and action in {"complete", "inspected"},
    )


def _hold_receipt(state: OperatorHoldState) -> dict[str, Any]:
    return {
        "operator_hold_status": state.classification,
        "operator_hold_valid": state.valid,
        "operator_hold_marker_sha256": state.marker_sha256,
        "operator_hold_id_sha256": state.hold_id_sha256,
    }


def _completion_authorization_receipt(
    state: CompletionAuthorizationState,
) -> dict[str, Any]:
    authorization = state.authorization
    return {
        "completion_authorization_status": state.classification,
        "completion_authorization_valid": state.valid,
        "completion_authorization_marker_sha256": state.marker_sha256,
        "completion_authorization_reason": state.reason,
        "completion_authorization_id_sha256": (
            hashlib.sha256(authorization.authorization_id.encode("ascii")).hexdigest()
            if authorization is not None
            else None
        ),
        "completion_authorization_evidence_sha256": (
            dict(authorization.evidence_sha256)
            if authorization is not None
            else None
        ),
    }


def _run_completion_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SupervisorError("completion lifecycle command failed") from exc


def _completion_notification_details(
    authorization: CompletionAuthorization,
) -> str:
    return (
        "Development quality, untouched holdout, production cutover, and the "
        "eligible extraction backlog are verified complete. Both supervisors "
        f"are being stopped. Completion authorization sha256 {authorization.sha256}."
    )


def _reconcile_codex_ops_delivery(
    authorization: CompletionAuthorization,
    *,
    state_path: Path = DEFAULT_CODEX_OPS_STATE,
    events_path: Path = DEFAULT_CODEX_OPS_EVENTS,
) -> dict[str, Any] | None:
    """Recover the durable codex-ops delivery across a post-send process crash."""

    if not state_path.expanduser().resolve().is_file() or not events_path.expanduser().resolve().is_file():
        return None
    try:
        state, _ = _load_strict_json(state_path, label="codex-ops state")
    except SupervisorError:
        return None
    dedupe = state.get("dedupe")
    items = state.get("items")
    dedupe_row = (
        dedupe.get("pif-evaluation-goal-complete")
        if isinstance(dedupe, dict)
        else None
    )
    item_id = dedupe_row.get("item_id") if isinstance(dedupe_row, dict) else None
    item = items.get(item_id) if isinstance(items, dict) and isinstance(item_id, str) else None
    telegram = (
        (item.get("transport") or {}).get("telegram")
        if isinstance(item, dict)
        else None
    )
    expected_endpoint = {
        "transport": "openclaw-railway",
        "service": EXPECTED_COMPLETION_TELEGRAM_SERVICE,
        "channel": EXPECTED_COMPLETION_TELEGRAM_CHANNEL,
        "target_sha256": EXPECTED_COMPLETION_TELEGRAM_TARGET_SHA256,
        "account_id": EXPECTED_COMPLETION_TELEGRAM_ACCOUNT,
        "identity_file": str(EXPECTED_COMPLETION_TELEGRAM_IDENTITY.resolve()),
    }
    if (
        not isinstance(item, dict)
        or item.get("source") != "pif-native-goal-supervisor"
        or item.get("dedupe_key") != "pif-evaluation-goal-complete"
        or item.get("summary") != COMPLETION_NOTIFICATION_SUMMARY
        or item.get("severity") != "low"
        or item.get("details") != _completion_notification_details(authorization)
        or item.get("next_step") != COMPLETION_NOTIFICATION_NEXT_STEP
        or not isinstance(telegram, dict)
        or telegram.get("status") != "sent"
        or any(telegram.get(key) != value for key, value in expected_endpoint.items())
        or _parse_timestamp(telegram.get("updated_at")) is None
        or not isinstance(dedupe_row, dict)
        or _parse_timestamp(dedupe_row.get("last_notified_at")) is None
    ):
        return None
    matched_event: dict[str, Any] | None = None
    try:
        for line in events_path.expanduser().resolve().read_text(encoding="utf-8").splitlines():
            try:
                event = json.loads(line, object_pairs_hook=_unique_json_object)
            except (SupervisorError, json.JSONDecodeError, UnicodeDecodeError):
                continue
            if (
                isinstance(event, dict)
                and event.get("type") == "notify"
                and event.get("item_id") == item_id
                and event.get("dedupe_key") == "pif-evaluation-goal-complete"
                and event.get("source") == "pif-native-goal-supervisor"
                and event.get("severity") == "low"
                and event.get("summary") == COMPLETION_NOTIFICATION_SUMMARY
                and event.get("telegram_status") == "sent"
                and all(
                    event.get(f"telegram_{key}") == value
                    for key, value in expected_endpoint.items()
                )
                and _parse_timestamp(event.get("at")) is not None
            ):
                matched_event = event
    except OSError:
        return None
    if matched_event is None:
        return None
    evidence = {
        "item_id": item_id,
        "details_sha256": hashlib.sha256(item["details"].encode("utf-8")).hexdigest(),
        "last_notified_at": dedupe_row["last_notified_at"],
        "transport_updated_at": telegram["updated_at"],
        "event_at": matched_event["at"],
        "endpoint": expected_endpoint,
    }
    return {
        "schema_version": COMPLETION_DELIVERY_RECEIPT_SCHEMA_VERSION,
        "authorization_sha256": authorization.sha256,
        "authorization_id_sha256": hashlib.sha256(
            authorization.authorization_id.encode("ascii")
        ).hexdigest(),
        "source": "pif-native-goal-supervisor",
        "dedupe_key": "pif-evaluation-goal-complete",
        "item_id": item_id,
        "delivered_at": str(dedupe_row["last_notified_at"]),
        "telegram_status": "sent",
        "delivery_evidence_source": "codex_ops_state_and_event",
        "delivery_evidence_sha256": hashlib.sha256(
            json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    }


def _read_completion_delivery_receipt(
    path: Path,
    authorization: CompletionAuthorization,
) -> dict[str, Any] | None:
    source = path.expanduser().resolve()
    if not source.exists():
        return None
    payload, _raw = _load_strict_json(source, label="completion delivery receipt")
    if set(payload) != {
        "schema_version",
        "authorization_sha256",
        "authorization_id_sha256",
        "source",
        "dedupe_key",
        "item_id",
        "delivered_at",
        "telegram_status",
        "delivery_evidence_source",
        "delivery_evidence_sha256",
    }:
        raise SupervisorError("completion delivery receipt schema drifted")
    if (
        payload.get("schema_version") != COMPLETION_DELIVERY_RECEIPT_SCHEMA_VERSION
        or payload.get("authorization_sha256") != authorization.sha256
        or payload.get("authorization_id_sha256")
        != hashlib.sha256(authorization.authorization_id.encode("ascii")).hexdigest()
        or payload.get("source") != "pif-native-goal-supervisor"
        or payload.get("dedupe_key") != "pif-evaluation-goal-complete"
        or not isinstance(payload.get("item_id"), str)
        or not payload.get("item_id")
        or _parse_timestamp(payload.get("delivered_at")) is None
        or payload.get("telegram_status") != "sent"
        or payload.get("delivery_evidence_source")
        not in {"codex_ops_stdout", "codex_ops_state_and_event"}
    ):
        raise SupervisorError("completion delivery receipt binding drifted")
    _sha256_value(
        payload.get("delivery_evidence_sha256"),
        label="completion delivery evidence",
    )
    return payload


def _persist_completion_delivery_receipt(
    path: Path,
    payload: dict[str, Any],
    authorization: CompletionAuthorization,
) -> dict[str, Any]:
    destination = path.expanduser().resolve()
    if destination.exists():
        raise SupervisorError("completion delivery receipt already exists")
    write_text_atomic(
        destination,
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n",
    )
    os.chmod(destination, 0o600)
    verified = _read_completion_delivery_receipt(destination, authorization)
    if verified != payload:
        raise SupervisorError("completion delivery receipt write did not verify")
    return payload


def _verified_delivery_from_notification(
    notification: subprocess.CompletedProcess[str],
    authorization: CompletionAuthorization,
) -> dict[str, Any]:
    if notification.returncode != 0:
        raise SupervisorError(
            "completion notification failed; both supervisors remain enabled"
        )
    try:
        output = json.loads(
            notification.stdout,
            object_pairs_hook=_unique_json_object,
        )
    except (json.JSONDecodeError, UnicodeDecodeError, SupervisorError) as exc:
        raise SupervisorError(
            "completion notification output is invalid; both supervisors remain enabled"
        ) from exc
    item = output.get("item") if isinstance(output, dict) else None
    delivery = output.get("delivery") if isinstance(output, dict) else None
    if (
        not isinstance(item, dict)
        or not isinstance(delivery, dict)
        or item.get("source") != "pif-native-goal-supervisor"
        or item.get("dedupe_key") != "pif-evaluation-goal-complete"
        or item.get("summary") != COMPLETION_NOTIFICATION_SUMMARY
        or item.get("severity") != "low"
        or item.get("details") != _completion_notification_details(authorization)
        or item.get("next_step") != COMPLETION_NOTIFICATION_NEXT_STEP
        or not isinstance(item.get("id"), str)
        or not item.get("id")
        or delivery.get("ok") is not True
        or delivery.get("status") != "sent"
        or delivery.get("transport") != "openclaw-railway"
        or delivery.get("service") != EXPECTED_COMPLETION_TELEGRAM_SERVICE
        or delivery.get("channel") != EXPECTED_COMPLETION_TELEGRAM_CHANNEL
        or delivery.get("account_id") != EXPECTED_COMPLETION_TELEGRAM_ACCOUNT
        or delivery.get("target_sha256")
        != EXPECTED_COMPLETION_TELEGRAM_TARGET_SHA256
        or Path(str(delivery.get("identity_file") or "")).expanduser().resolve()
        != EXPECTED_COMPLETION_TELEGRAM_IDENTITY.resolve()
    ):
        raise SupervisorError(
            "Telegram delivery did not verify; both supervisors remain enabled"
        )
    return {
        "schema_version": COMPLETION_DELIVERY_RECEIPT_SCHEMA_VERSION,
        "authorization_sha256": authorization.sha256,
        "authorization_id_sha256": hashlib.sha256(
            authorization.authorization_id.encode("ascii")
        ).hexdigest(),
        "source": "pif-native-goal-supervisor",
        "dedupe_key": "pif-evaluation-goal-complete",
        "item_id": item["id"],
        "delivered_at": now_iso(),
        "telegram_status": "sent",
        "delivery_evidence_source": "codex_ops_stdout",
        "delivery_evidence_sha256": hashlib.sha256(
            notification.stdout.encode("utf-8")
        ).hexdigest(),
    }


def finalize_completed_supervision(
    authorization: CompletionAuthorization,
    *,
    automations_db: Path = DEFAULT_AUTOMATIONS_DB,
    automation_toml: Path = DEFAULT_AUTOMATION_TOML,
    canonical_automation_toml: Path = DEFAULT_CANONICAL_AUTOMATION_TOML,
    delivery_receipt_path: Path = DEFAULT_COMPLETION_DELIVERY_RECEIPT_PATH,
    codex_ops_state_path: Path = DEFAULT_CODEX_OPS_STATE,
    codex_ops_events_path: Path = DEFAULT_CODEX_OPS_EVENTS,
) -> dict[str, Any]:
    """Notify, pause Desktop automation, then disable the external supervisor.

    The fixed notification dedupe key makes a retry safe.  Notification happens
    first so a delivery failure leaves both recovery mechanisms enabled.  The
    Desktop row is paused before the external job; if external disable fails,
    that still-running supervisor can retry without another Desktop dispatch.
    """

    if not isinstance(authorization, CompletionAuthorization):
        raise SupervisorError("verified completion authorization is required")

    delivery_receipt = _read_completion_delivery_receipt(
        delivery_receipt_path,
        authorization,
    )
    delivery_receipt_reused = delivery_receipt is not None
    if delivery_receipt is None:
        reconciled = _reconcile_codex_ops_delivery(
            authorization,
            state_path=codex_ops_state_path,
            events_path=codex_ops_events_path,
        )
        if reconciled is not None:
            delivery_receipt = _persist_completion_delivery_receipt(
                delivery_receipt_path,
                reconciled,
                authorization,
            )
            delivery_receipt_reused = True
    if delivery_receipt is None:
        notification = _run_completion_command(
            [
                str(DEFAULT_CODEX_OPS),
                "notify",
                "--source",
                "pif-native-goal-supervisor",
                "--summary",
                COMPLETION_NOTIFICATION_SUMMARY,
                "--severity",
                "low",
                "--details",
                _completion_notification_details(authorization),
                "--next-step",
                COMPLETION_NOTIFICATION_NEXT_STEP,
                "--dedupe-key",
                "pif-evaluation-goal-complete",
                "--telegram-mode",
                "prefer",
                "--json",
            ],
        )
        delivery_payload = _verified_delivery_from_notification(
            notification,
            authorization,
        )
        delivery_receipt = _persist_completion_delivery_receipt(
            delivery_receipt_path,
            delivery_payload,
            authorization,
        )
    paused = pause_native_automation_row(
        automations_db,
        automation_toml=automation_toml,
        canonical_automation_toml=canonical_automation_toml,
    )
    disabled = _run_completion_command(
        [str(DEFAULT_CODEX_CRON), "disable", "pif-native-goal-supervisor"]
    )
    if disabled.returncode != 0:
        raise SupervisorError(
            "completion notification sent and Desktop automation paused but external supervisor disable failed"
        )
    return {
        "completion_notified": True,
        "completion_delivery_receipt_sha256": hashlib.sha256(
            json.dumps(
                delivery_receipt,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        ).hexdigest(),
        "completion_delivery_receipt_reused": delivery_receipt_reused,
        "desktop_native_automation_paused": True,
        "desktop_native_automation_already_paused": paused["already_paused"],
        "external_supervisor_disabled": True,
    }


def _base_receipt(run_id: str, config: SupervisorConfig) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "observed_at": now_iso(),
        "run_id": run_id,
        "thread_id": config.thread_id,
        "automation_id": AUTOMATION_ID,
        "transport": "desktop_native_automation",
        "privacy": "sanitized_status_ids_and_hashes_only",
        "native_turn_started": False,
        "dispatch_verified": False,
        "dispatch_activity_verified": False,
        "semantic_progress_verified": False,
    }


def _append_receipt(path: Path, receipt: dict[str, Any]) -> None:
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    line = json.dumps(receipt, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n"
    descriptor = os.open(destination, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        os.write(descriptor, line.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _rollout_receipt(state: RolloutState) -> dict[str, Any]:
    return {
        "rollout_classification": state.classification,
        "rollout_lifecycle": state.lifecycle,
        "rollout_turn_id": state.turn_id,
        "rollout_last_timestamp": state.last_timestamp,
        "rollout_age_seconds": state.age_seconds,
        "rollout_file_size": state.file_size,
    }


async def run_supervisor_cycle(
    config: SupervisorConfig = SupervisorConfig(),
    *,
    goal_controller: GoalController = resume_blocked_thread_goal,
    paused_goal_controller: GoalController = resume_paused_thread_goal,
    goal_reader: GoalController = inspect_thread_goal,
    completion_controller: CompletionController = finalize_completed_supervision,
    completion_source_verifier: Callable[
        [str, Path, Path], dict[str, Any]
    ] = _verify_completion_source,
    writer_probe: Callable[[Path], WriterState] = rollout_writer_state,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    """Trigger the exact Desktop-native heartbeat without becoming a thread writer."""

    config.validate()
    run_id = str(uuid.uuid4())
    base = _base_receipt(run_id, config)
    try:
        lock = LifetimeLock(config.lock_path)
        lock.__enter__()
    except SupervisorLockUnavailable:
        return {**base, "ok": True, "outcome": "lock_busy", "action": "none"}

    failure_stage = "rollout_preflight"
    failure_context: dict[str, Any] = {}
    try:
        rollout = read_rollout_tail(
            config.rollout_path,
            now=now,
            fresh_activity_seconds=config.fresh_activity_seconds,
            max_bytes=config.rollout_tail_bytes,
        )
        writers = writer_probe(config.rollout_path)
        observed = {
            **_rollout_receipt(rollout),
            "writer_count": writers.writer_count,
            "desktop_owned": writers.desktop_owned,
        }
        failure_context.update(observed)
        if rollout.classification == "unknown":
            raise SupervisorError("rollout lifecycle is unknown")
        if not pretrigger_writer_state_is_safe(rollout, writers):
            raise SupervisorError("rollout writer state is unsafe before trigger")

        failure_stage = "semantic_plan_preflight"
        semantic_plan = read_semantic_plan(
            config.semantic_plan_path,
            thread_id=config.thread_id,
            project_root=config.project_root,
        )
        semantic_fields = _semantic_plan_receipt(semantic_plan)
        base = {**base, **semantic_fields}
        failure_context.update(semantic_fields)
        semantic_gate_reason, semantic_step_receipt = evaluate_semantic_plan(
            semantic_plan
        )
        semantic_fields = _semantic_plan_receipt(
            semantic_plan,
            gate_reason=semantic_gate_reason,
            step_receipt=semantic_step_receipt,
        )
        base = {**base, **semantic_fields}
        failure_context.update(semantic_fields)

        failure_stage = "goal_preflight"
        operator_hold = read_operator_hold_marker(
            config.operator_hold_path,
            thread_id=config.thread_id,
        )
        if operator_hold.valid:
            goal_result = await goal_reader(config.thread_id, binary=config.binary)
            goal = _validated_goal_state(
                goal_result,
                thread_id=config.thread_id,
                dispatch_actions=set(),
            )
        else:
            goal_result = await goal_controller(config.thread_id, binary=config.binary)
            goal = _validated_goal_state(
                goal_result,
                thread_id=config.thread_id,
                dispatch_actions={"resumed", "already_active"},
            )

        accidental_pause_recovered = False
        pause_recovery_start_offset: int | None = None
        initial_goal_action = goal.action
        if operator_hold.valid:
            receipt = {
                **base,
                **observed,
                **_hold_receipt(operator_hold),
                "goal_before_status": goal.before_status,
                "goal_after_status": goal.after_status,
                "goal_action": goal.action,
                "observed_at": now_iso(),
                "ok": True,
                "outcome": "intentional_operator_hold_noop",
                "action": "none",
            }
            _append_receipt(config.receipt_path, receipt)
            return receipt

        if (
            not operator_hold.valid
            and goal.before_status == "paused"
            and goal.after_status == "paused"
            and goal.action == "not_resumable"
        ):
            pause_recovery_start_offset = config.rollout_path.stat().st_size
            paused_result = await paused_goal_controller(
                config.thread_id,
                binary=config.binary,
            )
            goal = _validated_goal_state(
                paused_result,
                thread_id=config.thread_id,
                dispatch_actions={"resumed_paused"},
            )
            if (
                goal.before_status != "paused"
                or goal.after_status != "active"
                or goal.action != "resumed_paused"
            ):
                raise SupervisorError("accidental pause recovery did not verify")
            accidental_pause_recovered = True

        goal_receipt = {
            "goal_before_status": goal.before_status,
            "goal_after_status": goal.after_status,
            "goal_action": goal.action,
            "initial_goal_action": initial_goal_action,
            "accidental_pause_recovered": accidental_pause_recovered,
            **_hold_receipt(operator_hold),
        }
        completion_authorization_state = CompletionAuthorizationState(
            "not_checked",
            None,
            None,
            None,
        )
        if goal.complete:
            failure_stage = "completion_authorization_preflight"
            completion_authorization_state = inspect_completion_authorization(
                config.completion_authorization_path,
                thread_id=config.thread_id,
                project_root=config.project_root,
                source_verifier=completion_source_verifier,
            )
        completion_fields = _completion_authorization_receipt(
            completion_authorization_state
        )
        base = {**base, **completion_fields}
        failure_context.update({**goal_receipt, **completion_fields})
        completion_recovery_required = (
            goal.complete and not completion_authorization_state.valid
        )
        if rollout.fresh_open and not accidental_pause_recovered:
            if not goal.dispatch_allowed and not goal.complete:
                receipt = {
                    **base,
                    **observed,
                    **goal_receipt,
                    "observed_at": now_iso(),
                    "ok": False,
                    "outcome": "fresh_open_goal_not_resumable",
                    "action": "goal_preflight_only",
                }
                _append_receipt(config.receipt_path, receipt)
                return receipt
            automation, automation_toml_repaired = restore_operator_automation(
                config.automation_toml,
                config.canonical_automation_toml,
            )
            automation_row = reconcile_native_automation_row(
                config.automations_db,
                automation,
                force_due=False,
            )
            automation_repaired = (
                automation_toml_repaired
                or not automation_row["before_name_matches"]
                or not automation_row["before_prompt_matches"]
                or automation_row["before_status"] != "ACTIVE"
                or automation_row["before_rrule"] != AUTOMATION_RRULE
            )
            receipt = {
                **base,
                **observed,
                **goal_receipt,
                "observed_at": now_iso(),
                "ok": writers.writer_count == 1 and writers.desktop_owned,
                "outcome": "fresh_open_noop",
                "action": "goal_preflight_only",
                "automation_config_repaired": automation_repaired,
                "automation_status": automation_row["after_status"],
                "automation_rrule": automation_row["after_rrule"],
            }
            _append_receipt(config.receipt_path, receipt)
            return receipt
        if goal.complete and completion_authorization_state.valid:
            authorization = completion_authorization_state.authorization
            if authorization is None:  # pragma: no cover - dataclass invariant
                raise SupervisorError(
                    "valid completion authorization disappeared before finalization"
                )
            completion_recheck = inspect_completion_authorization(
                config.completion_authorization_path,
                thread_id=config.thread_id,
                project_root=config.project_root,
                source_verifier=completion_source_verifier,
            )
            if (
                not completion_recheck.valid
                or completion_recheck.authorization != authorization
            ):
                raise SupervisorError(
                    "completion authorization drifted before finalization"
                )
            failure_stage = "completion_lifecycle"
            completion = completion_controller(
                authorization,
                automations_db=config.automations_db,
                automation_toml=config.automation_toml,
                canonical_automation_toml=config.canonical_automation_toml,
                delivery_receipt_path=config.completion_delivery_receipt_path,
            )
            receipt = {
                **base,
                **observed,
                **goal_receipt,
                "observed_at": now_iso(),
                "ok": True,
                "outcome": "end_to_end_completion_verified",
                "action": (
                    "completion_notified_desktop_automation_paused_"
                    "external_supervisor_disabled"
                ),
                **completion,
            }
            _append_receipt(config.receipt_path, receipt)
            return receipt
        if not goal.dispatch_allowed and not completion_recovery_required:
            receipt = {
                **base,
                **observed,
                **goal_receipt,
                "observed_at": now_iso(),
                "ok": False,
                "outcome": "goal_not_resumable",
                "action": "none",
            }
            _append_receipt(config.receipt_path, receipt)
            return receipt

        automation, automation_toml_repaired = restore_operator_automation(
            config.automation_toml,
            config.canonical_automation_toml,
        )
        pretrigger = read_rollout_tail(
            config.rollout_path,
            fresh_activity_seconds=config.fresh_activity_seconds,
            max_bytes=config.rollout_tail_bytes,
        )
        if pretrigger.fresh_open and not accidental_pause_recovered:
            race_writers = writer_probe(config.rollout_path)
            if race_writers.writer_count != 1 or not race_writers.desktop_owned:
                raise SupervisorError("Desktop lost sole rollout ownership during preflight")
            automation_row = reconcile_native_automation_row(
                config.automations_db,
                automation,
                force_due=False,
            )
            automation_repaired = (
                automation_toml_repaired
                or not automation_row["before_name_matches"]
                or not automation_row["before_prompt_matches"]
                or automation_row["before_status"] != "ACTIVE"
                or automation_row["before_rrule"] != AUTOMATION_RRULE
            )
            receipt = {
                **base,
                **_rollout_receipt(pretrigger),
                **goal_receipt,
                "observed_at": now_iso(),
                "ok": True,
                "outcome": "race_fresh_open_noop",
                "action": "goal_preflight_only",
                "writer_count": race_writers.writer_count,
                "desktop_owned": race_writers.desktop_owned,
                "automation_config_repaired": automation_repaired,
                "automation_status": automation_row["after_status"],
                "automation_rrule": automation_row["after_rrule"],
            }
            _append_receipt(config.receipt_path, receipt)
            return receipt
        start_offset = (
            pause_recovery_start_offset
            if pause_recovery_start_offset is not None
            else config.rollout_path.stat().st_size
        )
        failure_stage = "native_trigger_preflight"
        predispatch_rollout = read_rollout_tail(
            config.rollout_path,
            fresh_activity_seconds=config.fresh_activity_seconds,
            max_bytes=config.rollout_tail_bytes,
        )
        predispatch_writers = writer_probe(config.rollout_path)
        failure_context.update(
            {
                **_rollout_receipt(predispatch_rollout),
                "writer_count": predispatch_writers.writer_count,
                "desktop_owned": predispatch_writers.desktop_owned,
            }
        )
        if not pretrigger_writer_state_is_safe(
            predispatch_rollout,
            predispatch_writers,
        ):
            raise SupervisorError("rollout writer state became unsafe before dispatch")
        if base["current_semantic_call_authorized"]:
            validate_semantic_directive(semantic_plan)
        database = force_native_automation_due(config.automations_db, automation)
        automation_repaired = (
            automation_toml_repaired
            or not database["before_name_matches"]
            or not database["before_prompt_matches"]
            or database["before_status"] != "ACTIVE"
            or database["before_rrule"] != AUTOMATION_RRULE
        )
        failure_stage = "native_trigger_verification"
        try:
            verification = await verify_native_trigger(
                config,
                start_offset=start_offset,
                writer_probe=writer_probe,
            )
        except SupervisorError:
            if accidental_pause_recovered:
                unverified_outcome = "accidental_pause_recovery_trigger_unverified"
                unverified_action = (
                    "goal_resumed_paused_native_trigger_owner_recovery_required"
                )
            elif completion_recovery_required:
                unverified_outcome = (
                    "goal_complete_without_authorization_recovery_trigger_unverified"
                )
                unverified_action = (
                    "end_to_end_completion_recovery_owner_required"
                )
            elif semantic_gate_reason is not None:
                unverified_outcome = "semantic_plan_gate_recovery_trigger_unverified"
                unverified_action = "successor_plan_recovery_owner_required"
            else:
                unverified_outcome = "native_trigger_unverified"
                unverified_action = (
                    "native_automation_forced_due_owner_recovery_required"
                )
            receipt = {
                **base,
                **observed,
                **goal_receipt,
                "observed_at": now_iso(),
                "ok": False,
                "outcome": unverified_outcome,
                "action": unverified_action,
                "automation_before_status": database["before_status"],
                "automation_before_rrule": database["before_rrule"],
                "automation_after_status": database["after_status"],
                "automation_after_rrule": database["after_rrule"],
                "automation_last_run_at_before": database["before_last_run_at"],
                "automation_config_repaired": automation_repaired,
                "owner_recovery_policy": "retry_and_alert_no_second_writer",
            }
            _append_receipt(config.receipt_path, receipt)
            return receipt
        failure_stage = "semantic_progress_verification"
        new_step_receipt = read_semantic_step_receipt(semantic_plan)
        verification.pop("progress", None)
        verification.pop("task_started", None)
        verification["semantic_progress_verified"] = (
            semantic_step_receipt is None and new_step_receipt is not None
        )
        verification["semantic_step_receipt_state"] = (
            new_step_receipt.state if new_step_receipt is not None else None
        )
        if accidental_pause_recovered:
            verified_outcome = "accidental_pause_recovered_native_trigger_verified"
            verified_action = "goal_resumed_paused_and_native_automation_forced_due"
        elif completion_recovery_required:
            verified_outcome = (
                "goal_complete_without_authorization_recovery_trigger_verified"
            )
            verified_action = "end_to_end_completion_recovery_turn_forced_due"
        elif semantic_gate_reason is not None:
            verified_outcome = "semantic_plan_gate_recovery_trigger_verified"
            verified_action = "recovery_or_successor_plan_turn_forced_due"
        elif semantic_step_receipt is not None:
            verified_outcome = "semantic_step_receipt_successor_trigger_verified"
            verified_action = "successor_plan_turn_forced_due"
        else:
            verified_outcome = "native_trigger_verified"
            verified_action = "native_automation_forced_due"
        receipt = {
            **base,
            **observed,
            **goal_receipt,
            "observed_at": now_iso(),
            "ok": True,
            "outcome": verified_outcome,
            "action": verified_action,
            "automation_before_status": database["before_status"],
            "automation_before_rrule": database["before_rrule"],
            "automation_after_status": database["after_status"],
            "automation_after_rrule": database["after_rrule"],
            "automation_last_run_at_before": database["before_last_run_at"],
            "automation_config_repaired": automation_repaired,
            **verification,
        }
        _append_receipt(config.receipt_path, receipt)
        return receipt
    except asyncio.CancelledError:
        receipt = {
            **base,
            "observed_at": now_iso(),
            "ok": False,
            "outcome": "supervisor_cancelled",
            "action": "none",
        }
        _append_receipt(config.receipt_path, receipt)
        raise
    except (AppServerError, OSError, sqlite3.Error, SupervisorError, ValueError) as exc:
        receipt = {
            **base,
            **failure_context,
            "observed_at": now_iso(),
            "ok": False,
            "outcome": "supervisor_failed",
            "action": "none",
            "error_class": type(exc).__name__,
            "failure_stage": failure_stage,
        }
        _append_receipt(config.receipt_path, receipt)
        return receipt
    finally:
        lock.__exit__(None, None, None)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Preflight and trigger the exact Desktop-native PIF goal heartbeat."
    )
    parser.add_argument("--binary", default=str(DEFAULT_CODEX_BINARY))
    parser.add_argument("--automation-toml", default=str(DEFAULT_AUTOMATION_TOML))
    parser.add_argument(
        "--canonical-automation-toml",
        default=str(DEFAULT_CANONICAL_AUTOMATION_TOML),
    )
    parser.add_argument("--automations-db", default=str(DEFAULT_AUTOMATIONS_DB))
    parser.add_argument("--rollout-path", default=str(DEFAULT_ROLLOUT_PATH))
    parser.add_argument("--lock-path", default=str(DEFAULT_LOCK_PATH))
    parser.add_argument("--receipt-path", default=str(DEFAULT_RECEIPT_PATH))
    parser.add_argument("--operator-hold-path", default=str(DEFAULT_OPERATOR_HOLD_PATH))
    parser.add_argument("--semantic-plan-path", default=str(DEFAULT_SEMANTIC_PLAN_PATH))
    parser.add_argument(
        "--completion-authorization-path",
        default=str(DEFAULT_COMPLETION_AUTHORIZATION_PATH),
    )
    parser.add_argument(
        "--completion-delivery-receipt-path",
        default=str(DEFAULT_COMPLETION_DELIVERY_RECEIPT_PATH),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = asyncio.run(
        run_supervisor_cycle(
            SupervisorConfig(
                binary=Path(args.binary),
                automation_toml=Path(args.automation_toml),
                canonical_automation_toml=Path(args.canonical_automation_toml),
                automations_db=Path(args.automations_db),
                rollout_path=Path(args.rollout_path),
                lock_path=Path(args.lock_path),
                receipt_path=Path(args.receipt_path),
                operator_hold_path=Path(args.operator_hold_path),
                semantic_plan_path=Path(args.semantic_plan_path),
                completion_authorization_path=Path(
                    args.completion_authorization_path
                ),
                completion_delivery_receipt_path=Path(
                    args.completion_delivery_receipt_path
                ),
            )
        )
    )
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
