from __future__ import annotations

"""Checksum-bound one-turn runtime for the bounded-span architecture canary."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_bounded_span_episode_batch as adapter
from . import app_server_canonical_v31_epoch7_input_authority_runtime as epoch7
from . import app_server_canonical_v31_epoch9_split_authority_recovery as epoch9
from . import app_server_canonical_v31_epoch12_canonical_consistency_authority_recovery as epoch12
from . import codex_app_server


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch13-bounded-span-canary-v1"
)
PREDECESSOR_ROOT = epoch12.DEFAULT_ROOT
DIRECTIVE_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-epoch13-bounded-span-canary-v13.json"
)
PLAN_PATH = PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v13.json"
DIRECTIVE_SHA256 = "a2fbf0237f726840febe3038f7496fe25967c8dd9b55ae12560604ec1635ca29"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 13
STEP_ID = "canonical_v31_epoch13_bounded_span_canary_v13"
TURN_NAME = "epoch13_bounded_span_canary_6f0779f9233160dc06d14b36"
FAILED_PREDECESSOR_TURN = "authority12_canonical_labels_6f0779f9233160dc06d14b36"

CONTRACT_FILENAME = "runtime-contract.json"
RUNTIME_LOCK_FILENAME = "runtime-lock.json"
AUTHORIZATION_FILENAME = "operator-authorization.json"
RECEIPT_FILENAME = "plan-step-receipt.json"
TERMINAL_FILENAME = "terminal.json"
REJECTION_FILENAME = "semantic-rejection.json"
CONTRACT_VERSION = "pif_canonical_v31_bounded_span_canary_contract_v1"
RUNTIME_LOCK_VERSION = "pif_canonical_v31_bounded_span_canary_runtime_lock_v1"
AUTHORIZATION_VERSION = "pif_canonical_v31_bounded_span_canary_authorization_v1"
TURN_MANIFEST_VERSION = "pif_canonical_v31_bounded_span_canary_turn_manifest_v1"
ATTEMPT_VERSION = "pif_canonical_v31_bounded_span_canary_attempt_v1"
THREAD_BINDING_VERSION = "pif_canonical_v31_bounded_span_canary_thread_v1"
DISPATCH_VERSION = "pif_canonical_v31_bounded_span_canary_dispatch_v1"
REJECTION_VERSION = "pif_canonical_v31_bounded_span_canary_rejection_v1"
RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"

MODEL = adapter.MODEL
EFFORT = adapter.EFFORT
MAXIMUM_TOTAL_TOKENS = 90_000
MAXIMUM_WALL_SECONDS = 900
MINIMUM_REMAINING_RESERVE_PERCENT = 20
CAPACITY_SAFETY_MARGIN_PERCENT = 1
QUOTA_POINTS_PER_MILLION_TOKENS = 17
AUTHORIZATION_WINDOW_SECONDS = 14_400
AUTHORIZATION_STATEMENT = (
    "You have my full permission to continue. No need to seek out my approval anymore."
)
TRUSTED_CLIENT_FACTORY = adapter._client_factory  # noqa: SLF001

_FORBIDDEN_AUTH_ENVIRONMENT = (
    "OPENAI_API_KEY",
    "CODEX_API_KEY",
    "OPENAI_ACCESS_TOKEN",
    "OPENAI_AUTH_TOKEN",
    "CHATGPT_ACCESS_TOKEN",
    "OPENAI_SESSION_TOKEN",
    "CHATGPT_SESSION_TOKEN",
    "CODEX_SESSION_TOKEN",
)
_RECORD_CACHE: dict[tuple[str, int, int, int, int], dict[str, Any]] = {}


class BoundedSpanCanaryError(RuntimeError):
    pass


class BoundedSpanCanaryWaiting(BoundedSpanCanaryError):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise BoundedSpanCanaryError("current time must be timezone-aware")
    return current.astimezone(timezone.utc)


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    try:
        stat_result = resolved.stat()
    except OSError as exc:
        raise BoundedSpanCanaryError(f"required artifact is unavailable: {resolved}") from exc
    if not resolved.is_file() or resolved.is_symlink():
        raise BoundedSpanCanaryError(f"required artifact is not a regular file: {resolved}")
    identity = (
        str(resolved),
        int(stat_result.st_dev),
        int(stat_result.st_ino),
        int(stat_result.st_size),
        int(stat_result.st_mtime_ns),
    )
    cached = _RECORD_CACHE.get(identity)
    if cached is not None:
        return copy.deepcopy(cached)
    value = {
        "path": str(resolved),
        "sha256": _sha256_bytes(resolved.read_bytes()),
        "size_bytes": int(stat_result.st_size),
    }
    _RECORD_CACHE[identity] = value
    return copy.deepcopy(value)


def _verify_record(value: Any, *, label: str) -> Path:
    if not isinstance(value, Mapping) or set(value) != {"path", "sha256", "size_bytes"}:
        raise BoundedSpanCanaryError(f"{label} record is malformed")
    path_value = value.get("path")
    if not isinstance(path_value, str) or not path_value:
        raise BoundedSpanCanaryError(f"{label} path is malformed")
    path = Path(path_value).expanduser().resolve()
    if _record(path) != dict(value):
        raise BoundedSpanCanaryError(f"{label} record drifted")
    return path


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BoundedSpanCanaryError(f"{label} is unavailable or invalid") from exc
    if not isinstance(value, dict):
        raise BoundedSpanCanaryError(f"{label} must be an object")
    return value


def _write_json(path: Path, value: Any) -> dict[str, Any]:
    try:
        return epoch9._write_json(path, value)  # noqa: SLF001
    except epoch9.CanonicalV31Epoch9SplitRecoveryError as exc:
        raise BoundedSpanCanaryError(str(exc)) from exc


def _write_bytes(path: Path, value: bytes) -> dict[str, Any]:
    try:
        return epoch9._write_bytes(path, value)  # noqa: SLF001
    except epoch9.CanonicalV31Epoch9SplitRecoveryError as exc:
        raise BoundedSpanCanaryError(str(exc)) from exc


def _parse_timestamp(value: Any, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise BoundedSpanCanaryError(f"{label} is malformed")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BoundedSpanCanaryError(f"{label} is malformed") from exc
    if parsed.tzinfo is None:
        raise BoundedSpanCanaryError(f"{label} lacks timezone")
    return parsed.astimezone(timezone.utc)


def _zero_usage() -> dict[str, int]:
    return {field: 0 for field in adapter.USAGE_FIELDS}


def _verify_directive_and_plan() -> tuple[dict[str, Any], dict[str, Any]]:
    if _record(DIRECTIVE_PATH)["sha256"] != DIRECTIVE_SHA256:
        raise BoundedSpanCanaryError("epoch-13 directive drifted")
    directive = _load_object(DIRECTIVE_PATH, label="epoch-13 directive")
    plan = _load_object(PLAN_PATH, label="epoch-13 semantic plan")
    if (
        directive.get("schema_version")
        != "pif_evaluation_epoch13_bounded_span_canary_directive_v1"
        or directive.get("thread_id") != THREAD_ID
        or directive.get("plan_epoch") != PLAN_EPOCH
        or directive.get("step_id") != STEP_ID
        or directive.get("state") != "ready_for_direct_user_authorized_execute"
        or directive.get("expected_receipt_path") != str((DEFAULT_ROOT / RECEIPT_FILENAME).resolve())
    ):
        raise BoundedSpanCanaryError("epoch-13 directive fixed fields drifted")
    authorization = directive.get("authorization_contract")
    predecessor = directive.get("predecessor_contract")
    architecture = directive.get("architecture_contract")
    canary = directive.get("canary_contract")
    promotion = directive.get("promotion_contract")
    transport = directive.get("transport_contract")
    if (
        not isinstance(authorization, Mapping)
        or authorization.get("authority") != "direct_user_instruction"
        or authorization.get("authorized_by") != "kolby"
        or authorization.get("operator_authorization_statement") != AUTHORIZATION_STATEMENT
        or authorization.get("additional_interactive_approval_required") is not False
        or not isinstance(predecessor, Mapping)
        or predecessor.get("epoch12_root") != str(PREDECESSOR_ROOT.resolve())
        or predecessor.get("epoch12_receipt_sha256")
        != "e1187002e2d7f03b956875b73579696f00a423059b1dc000a4172227fe19ae23"
        or predecessor.get("epoch12_state") != "rejected"
        or predecessor.get("epoch12_semantic_model_call_count") != 3
        or predecessor.get("epoch12_total_tokens") != 168_529
        or predecessor.get("epoch12_completed_turn_replay_allowed") is not False
        or not isinstance(architecture, Mapping)
        or architecture.get("source_unit_max_characters") != adapter.SOURCE_UNIT_MAX_CHARS
        or architecture.get("selectable_evidence_span_max_characters")
        != adapter.CANONICAL_EVIDENCE_MAX_CHARS
        or architecture.get("every_selectable_span_prevalidated_within_canonical_limit")
        is not True
        or architecture.get("model_selects_one_opaque_evidence_span_id") is not True
        or architecture.get("deterministic_exact_span_to_offset_projection_only") is not True
        or any(
            architecture.get(field) is not False
            for field in (
                "deterministic_semantic_pruning",
                "deterministic_support_filtering",
                "deterministic_deduplication",
                "deterministic_relabeling",
                "semantic_regex_or_keyword_rules",
            )
        )
        or not isinstance(canary, Mapping)
        or canary.get("model") != MODEL
        or canary.get("effort") != EFFORT
        or canary.get("thread_mode") != "new_thread"
        or canary.get("batch_size") != 8
        or canary.get("new_model_call_cap") != 1
        or canary.get("semantic_retry_count") != 0
        or canary.get("measured_total_token_acceptance_ceiling") != MAXIMUM_TOTAL_TOKENS
        or canary.get("maximum_wall_seconds") != MAXIMUM_WALL_SECONDS
        or canary.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or canary.get("capacity_safety_margin_percent")
        != CAPACITY_SAFETY_MARGIN_PERCENT
        or canary.get("quota_points_per_million_tokens")
        != QUOTA_POINTS_PER_MILLION_TOKENS
        or canary.get("production_amortized_ratio_gate") != 0.28
        or not isinstance(promotion, Mapping)
        or promotion.get("winner_frozen") is not False
        or promotion.get("quality_authorized") is not False
        or promotion.get("holdout_authorized") is not False
        or promotion.get("production_mutation_allowed") is not False
        or not isinstance(transport, Mapping)
        or transport.get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or transport.get("managed_chatgpt_plan_type") != "pro"
        or transport.get("pinned_codex_cli_version") != "0.144.1"
        or transport.get("persistent_app_server_process_count") != 1
        or transport.get("api_key_billing_allowed") is not False
        or transport.get("raw_session_token_access_or_replay_allowed") is not False
        or transport.get("codex_exec_semantic_work_allowed") is not False
    ):
        raise BoundedSpanCanaryError("epoch-13 directive contract drifted")
    step = plan.get("step")
    if (
        set(plan) != {"schema_version", "thread_id", "plan_epoch", "state", "step"}
        or plan.get("schema_version") != "pif_evaluation_semantic_plan_v1"
        or plan.get("thread_id") != THREAD_ID
        or plan.get("plan_epoch") != PLAN_EPOCH
        or plan.get("state") != "executable"
        or not isinstance(step, Mapping)
        or set(step)
        != {
            "step_id", "state", "max_model_calls", "max_total_tokens",
            "expected_receipt_path", "accepted_receipt_states", "directive_path",
            "directive_sha256",
        }
        or step.get("step_id") != STEP_ID
        or step.get("state") != "executable"
        or step.get("max_model_calls") != 1
        or step.get("max_total_tokens") != MAXIMUM_TOTAL_TOKENS
        or step.get("expected_receipt_path")
        != str((DEFAULT_ROOT / RECEIPT_FILENAME).resolve())
        or step.get("accepted_receipt_states") != ["passed", "rejected", "waiting"]
        or step.get("directive_path") != str(DIRECTIVE_PATH.resolve())
        or step.get("directive_sha256") != DIRECTIVE_SHA256
    ):
        raise BoundedSpanCanaryError("epoch-13 semantic plan drifted")
    return directive, plan


def _predecessor_request_path() -> Path:
    return (
        PREDECESSOR_ROOT
        / "prepared-turns"
        / FAILED_PREDECESSOR_TURN
        / "request.private.json"
    )


def _predecessor_output_path() -> Path:
    return PREDECESSOR_ROOT / "turns" / FAILED_PREDECESSOR_TURN / "output.private.json"


def _validate_predecessor(*, full_verify: bool) -> dict[str, Any]:
    if full_verify:
        receipt = epoch12.verify_recovery_receipt(PREDECESSOR_ROOT)
    else:
        receipt = _load_object(PREDECESSOR_ROOT / RECEIPT_FILENAME, label="epoch-12 receipt")
    if (
        _record(PREDECESSOR_ROOT / RECEIPT_FILENAME)["sha256"]
        != "e1187002e2d7f03b956875b73579696f00a423059b1dc000a4172227fe19ae23"
        or receipt.get("state") != "rejected"
        or receipt.get("terminal_reason")
        != "epoch12_canonical_consistency_semantic_output_rejected"
        or receipt.get("new_semantic_model_call_count") != 3
        or receipt.get("new_measured_usage", {}).get("total_tokens") != 168_529
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("production_mutated") is not False
        or receipt.get("holdout_authorized") is not False
    ):
        raise BoundedSpanCanaryError("epoch-12 predecessor receipt drifted")
    rejection = _load_object(PREDECESSOR_ROOT / REJECTION_FILENAME, label="epoch-12 rejection")
    if (
        rejection.get("turn_id") != FAILED_PREDECESSOR_TURN
        or rejection.get("diagnostic_path")
        != "canonical source-unit output failed projection"
    ):
        raise BoundedSpanCanaryError("epoch-12 rejection diagnostic drifted")
    predecessor_request = _load_object(
        _predecessor_request_path(), label="epoch-12 failed prepared request"
    )
    predecessor_output = _load_object(
        _predecessor_output_path(), label="epoch-12 failed raw output"
    )
    try:
        epoch12.adapter.validate_and_project_output(
            predecessor_request, predecessor_output
        )
    except epoch12.adapter.CanonicalV31OutputError as exc:
        if str(exc) != (
            "$.discourse_events[4] projected evidence exceeds canonical "
            "maxLength=1000"
        ):
            raise BoundedSpanCanaryError(
                "epoch-12 offline rejection cause drifted"
            ) from exc
    else:
        raise BoundedSpanCanaryError("epoch-12 failed output unexpectedly validated")
    return receipt


def _episode_from_predecessor() -> tuple[dict[str, Any], dict[str, Any]]:
    predecessor_request = _load_object(
        _predecessor_request_path(), label="epoch-12 failed prepared request"
    )
    episode = copy.deepcopy(predecessor_request["episode_context"])
    episode["segments"] = [
        {
            key: copy.deepcopy(segment[key])
            for key in (
                "segment_id", "segment_text", "segment_quality", "density_stratum",
                "boundaries",
            )
        }
        for segment in predecessor_request["private_input"]["segments"]
    ]
    requests = adapter.prepare_episode_batches(
        episode, batch_size=8, thread_mode="new_thread"
    )
    if len(requests) != 1 or requests[0]["effective_batch_size"] != 6:
        raise BoundedSpanCanaryError("epoch-13 canary request cardinality drifted")
    return predecessor_request, requests[0]


def _paths(root: Path) -> dict[str, Path]:
    prepared = root / "prepared-turn"
    turn = root / "turn"
    return {
        "contract": root / CONTRACT_FILENAME,
        "lock": root / RUNTIME_LOCK_FILENAME,
        "authorization": root / AUTHORIZATION_FILENAME,
        "receipt": root / RECEIPT_FILENAME,
        "terminal": root / TERMINAL_FILENAME,
        "rejection": root / REJECTION_FILENAME,
        "request": prepared / "request.private.json",
        "prompt": prepared / "prompt.private.md",
        "base": prepared / "base-instructions.private.md",
        "schema": prepared / "schema.json",
        "manifest": prepared / "turn-manifest.json",
        "attempt": turn / "semantic-attempt.json",
        "initial_capacity": turn / "capacity" / "initial",
        "preturn_capacity": turn / "capacity" / "preturn",
        "thread": turn / "thread-start.json",
        "dispatch": turn / "semantic-dispatch.json",
        "sidecar": turn / "sidecar.json",
        "output": turn / "output.private.json",
        "labels": turn / "canonical-labels.private.json",
        "provenance": turn / "evidence-provenance.private.json",
        "fidelity": turn / "semantic-fidelity.json",
    }


def _turn_manifest(root: Path, request: Mapping[str, Any]) -> dict[str, Any]:
    paths = _paths(root)
    return {
        "schema_version": TURN_MANIFEST_VERSION,
        "turn_name": TURN_NAME,
        "stage": "bounded_span_canonical_labels",
        "ordinal": 0,
        "episode_id": request["episode_id"],
        "segment_ids": copy.deepcopy(request["segment_ids"]),
        "batch_id": request["batch_id"],
        "model": MODEL,
        "effort": EFFORT,
        "thread_mode": "new_thread",
        "batch_size": request["effective_batch_size"],
        "request": _record(paths["request"]),
        "prompt": _record(paths["prompt"]),
        "base_instructions": _record(paths["base"]),
        "output_schema": _record(paths["schema"]),
        "semantic_model_call_cap": 1,
        "semantic_retry_count": 0,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
    }


def _runtime_contract(root: Path) -> dict[str, Any]:
    paths = _paths(root)
    request = _load_object(paths["request"], label="epoch-13 request")
    adapter.validate_prepared_request(request)
    _predecessor_request, expected_request = _episode_from_predecessor()
    if request != expected_request:
        raise BoundedSpanCanaryError("epoch-13 request differs from the frozen canary sample")
    return {
        "schema_version": CONTRACT_VERSION,
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "output_root": str(root.resolve()),
        "directive": _record(DIRECTIVE_PATH),
        "semantic_plan": _record(PLAN_PATH),
        "predecessor": {
            "receipt": _record(PREDECESSOR_ROOT / RECEIPT_FILENAME),
            "terminal": _record(PREDECESSOR_ROOT / TERMINAL_FILENAME),
            "semantic_rejection": _record(PREDECESSOR_ROOT / REJECTION_FILENAME),
            "failed_request": _record(_predecessor_request_path()),
            "failed_raw_output": _record(_predecessor_output_path()),
            "runtime_lock": _record(PREDECESSOR_ROOT / RUNTIME_LOCK_FILENAME),
        },
        "adapter_binding": adapter.build_six_arm_matrix_binding(),
        "turns": [
            {
                "ordinal": 0,
                "turn_id": TURN_NAME,
                "episode_id": request["episode_id"],
                "turn_manifest": _record(paths["manifest"]),
            }
        ],
        "model": MODEL,
        "effort": EFFORT,
        "exact_turn_count": 1,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS,
        "maximum_wall_seconds_per_turn": MAXIMUM_WALL_SECONDS,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
        "capacity_safety_margin_percent": CAPACITY_SAFETY_MARGIN_PERCENT,
        "operator_wall_safety_margin_seconds": 60,
        "semantic_retry_count": 0,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "api_key_billing_allowed": False,
        "raw_session_token_replay_allowed": False,
        "codex_exec_semantic_work_allowed": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _runtime_lock(root: Path, contract: Mapping[str, Any]) -> dict[str, Any]:
    paths = _paths(root)
    modules = [
        Path(__file__),
        Path(adapter.__file__),
        Path(adapter.audited.__file__),
        Path(adapter.base.__file__),
        Path(epoch7.__file__),
        Path(epoch9.__file__),
        Path(epoch12.__file__),
        Path(codex_app_server.__file__),
    ]
    return {
        "schema_version": RUNTIME_LOCK_VERSION,
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "runtime_contract": _record(paths["contract"]),
        "directive": _record(DIRECTIVE_PATH),
        "semantic_plan": _record(PLAN_PATH),
        "python_modules": [_record(path.resolve()) for path in modules],
        "prepared_artifacts": {
            role: _record(paths[role])
            for role in ("request", "prompt", "base", "schema", "manifest")
        },
        "predecessor_artifacts": copy.deepcopy(contract["predecessor"]),
        "canonical_label_schema": _record(adapter.base.LABEL_SCHEMA_PATH),
        "protocol_schema": _record(codex_app_server.PROTOCOL_SCHEMA_PATH),
        "pinned_codex_cli": _record(adapter.PINNED_CODEX),
        "pinned_codex_cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
        "instruction_source_contract": adapter.expected_instruction_source_contract(),
        "context_control_overlay": adapter.verified_context_control_overlay(),
        "context_control_overlay_sha256": adapter.base.sha256_text(
            _canonical_json(adapter.verified_context_control_overlay())
        ),
    }


def prepare_canary(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    _verify_directive_and_plan()
    _validate_predecessor(full_verify=True)
    output_root = root.expanduser().resolve()
    if output_root.exists() and any(output_root.iterdir()):
        raise BoundedSpanCanaryError("epoch-13 output root is not empty")
    output_root.mkdir(parents=True, exist_ok=True)
    paths = _paths(output_root)
    _predecessor_request, request = _episode_from_predecessor()
    _write_json(paths["request"], request)
    _write_bytes(paths["prompt"], request["prompt"].encode("utf-8"))
    _write_bytes(paths["base"], request["base_instructions"].encode("utf-8"))
    _write_json(paths["schema"], request["output_schema"])
    _write_json(paths["manifest"], _turn_manifest(output_root, request))
    contract = _runtime_contract(output_root)
    _write_json(paths["contract"], contract)
    lock = _runtime_lock(output_root, contract)
    _write_json(paths["lock"], lock)
    return verify_runtime(output_root)


def verify_runtime(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    _verify_directive_and_plan()
    _validate_predecessor(full_verify=False)
    output_root = root.expanduser().resolve()
    paths = _paths(output_root)
    contract = _load_object(paths["contract"], label="epoch-13 runtime contract")
    expected_contract = _runtime_contract(output_root)
    if contract != expected_contract:
        raise BoundedSpanCanaryError("epoch-13 runtime contract drifted")
    observed_lock = _load_object(paths["lock"], label="epoch-13 runtime lock")
    expected_lock = _runtime_lock(output_root, contract)
    if observed_lock != expected_lock:
        raise BoundedSpanCanaryError("epoch-13 runtime lock drifted")
    request = _load_object(paths["request"], label="epoch-13 request")
    adapter.validate_prepared_request(request)
    return {
        "schema_version": "pif_canonical_v31_bounded_span_canary_runtime_status_v1",
        "state": "verified_zero_call_runtime",
        "runtime_contract": _record(paths["contract"]),
        "runtime_lock": _record(paths["lock"]),
        "request": _record(paths["request"]),
        "prompt_bytes": paths["prompt"].stat().st_size,
        "schema_bytes": paths["schema"].stat().st_size,
        "source_unit_count": sum(
            len(segment["units"])
            for segment in request["private_input"]["segments"]
        ),
        "evidence_span_count": sum(
            len(segment["evidence_spans"])
            for segment in request["private_input"]["segments"]
        ),
        "semantic_model_call_count": 0,
    }


def authorize_canary(
    *,
    root: Path = DEFAULT_ROOT,
    operator_authorization_id: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    if not isinstance(operator_authorization_id, str) or not operator_authorization_id.strip():
        raise BoundedSpanCanaryError("operator authorization id is required")
    output_root = root.expanduser().resolve()
    verify_runtime(output_root)
    paths = _paths(output_root)
    current = _now(now)
    payload = {
        "schema_version": AUTHORIZATION_VERSION,
        "state": "authorized_for_exactly_one_bounded_span_canary_turn",
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "operator_authorization_id": operator_authorization_id.strip(),
        "authorized_by": "kolby",
        "authority": "direct_user_instruction",
        "authorization_statement": AUTHORIZATION_STATEMENT,
        "authorization_statement_sha256": _sha256_bytes(
            AUTHORIZATION_STATEMENT.encode("utf-8")
        ),
        "created_at": current.isoformat(),
        "expires_at": (current + timedelta(seconds=AUTHORIZATION_WINDOW_SECONDS)).isoformat(),
        "runtime_contract": _record(paths["contract"]),
        "runtime_lock": _record(paths["lock"]),
        "semantic_model_call_cap": 1,
        "semantic_retry_count": 0,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    if paths["authorization"].exists():
        observed = _load_object(paths["authorization"], label="epoch-13 authorization")
        if observed != payload:
            raise BoundedSpanCanaryError("epoch-13 authorization already exists and differs")
    else:
        _write_json(paths["authorization"], payload)
    return verify_authorization(
        output_root,
        expected_authorization_id=operator_authorization_id,
        require_current=True,
        now=current,
    )


def verify_authorization(
    root: Path = DEFAULT_ROOT,
    *,
    expected_authorization_id: str | None,
    require_current: bool,
    now: datetime | None = None,
) -> dict[str, Any]:
    verify_runtime(root)
    output_root = root.expanduser().resolve()
    paths = _paths(output_root)
    value = _load_object(paths["authorization"], label="epoch-13 authorization")
    if (
        value.get("schema_version") != AUTHORIZATION_VERSION
        or value.get("state") != "authorized_for_exactly_one_bounded_span_canary_turn"
        or value.get("thread_id") != THREAD_ID
        or value.get("plan_epoch") != PLAN_EPOCH
        or value.get("step_id") != STEP_ID
        or value.get("authorized_by") != "kolby"
        or value.get("authority") != "direct_user_instruction"
        or value.get("authorization_statement") != AUTHORIZATION_STATEMENT
        or value.get("authorization_statement_sha256")
        != _sha256_bytes(AUTHORIZATION_STATEMENT.encode("utf-8"))
        or value.get("runtime_contract") != _record(paths["contract"])
        or value.get("runtime_lock") != _record(paths["lock"])
        or value.get("semantic_model_call_cap") != 1
        or value.get("semantic_retry_count") != 0
        or value.get("quality_authorized") is not False
        or value.get("holdout_authorized") is not False
        or value.get("production_mutation_allowed") is not False
    ):
        raise BoundedSpanCanaryError("epoch-13 authorization drifted")
    if expected_authorization_id is not None and value.get("operator_authorization_id") != expected_authorization_id:
        raise BoundedSpanCanaryError("epoch-13 authorization id drifted")
    created = _parse_timestamp(value.get("created_at"), label="authorization created_at")
    expires = _parse_timestamp(value.get("expires_at"), label="authorization expires_at")
    if expires <= created:
        raise BoundedSpanCanaryError("epoch-13 authorization window drifted")
    if (expires - created).total_seconds() != AUTHORIZATION_WINDOW_SECONDS:
        raise BoundedSpanCanaryError("epoch-13 authorization duration drifted")
    if require_current and not (created <= _now(now) < expires):
        raise BoundedSpanCanaryWaiting("epoch-13 authorization is not current")
    return value


def _reject_external_auth_material() -> None:
    present = sorted(name for name in _FORBIDDEN_AUTH_ENVIRONMENT if os.environ.get(name))
    if present:
        raise BoundedSpanCanaryError(
            "managed ChatGPT execution rejects API-key/raw-session auth: "
            + ", ".join(present)
        )


def _thread_payload(
    thread: Any,
    *,
    root: Path,
    request: Mapping[str, Any],
) -> dict[str, Any]:
    preflight = adapter._verify_started_thread(thread, request)  # noqa: SLF001
    if getattr(thread, "cwd", None) != str(PROJECT_ROOT.resolve()):
        raise BoundedSpanCanaryError("started thread cwd drifted")
    return {
        "schema_version": THREAD_BINDING_VERSION,
        "state": "ephemeral_thread_started_before_semantic_turn",
        "thread_id": thread.thread_id,
        "model": MODEL,
        "effort": EFFORT,
        "cwd": str(PROJECT_ROOT.resolve()),
        "ephemeral": True,
        "base_instructions_sha256": request["base_instructions_sha256"],
        "base_instructions_bytes": len(request["base_instructions"].encode("utf-8")),
        "instruction_sources_sha256": preflight["instruction_sources_sha256"],
        "instruction_sources_count": preflight["instruction_sources_count"],
        "context_control_overlay_sha256": request["context_control_overlay_sha256"],
        "runtime_contract": _record(_paths(root)["contract"]),
        "runtime_lock": _record(_paths(root)["lock"]),
        "operator_authorization": _record(_paths(root)["authorization"]),
        "turn_manifest": _record(_paths(root)["manifest"]),
    }


def _thread_from_payload(value: Mapping[str, Any], request: Mapping[str, Any]) -> Any:
    expected_sources = adapter.expected_instruction_source_contract()
    if (
        value.get("schema_version") != THREAD_BINDING_VERSION
        or value.get("state") != "ephemeral_thread_started_before_semantic_turn"
        or not isinstance(value.get("thread_id"), str)
        or not value.get("thread_id")
        or value.get("model") != MODEL
        or value.get("effort") != EFFORT
        or value.get("cwd") != str(PROJECT_ROOT.resolve())
        or value.get("ephemeral") is not True
        or value.get("base_instructions_sha256") != request["base_instructions_sha256"]
        or value.get("base_instructions_bytes")
        != len(request["base_instructions"].encode("utf-8"))
        or value.get("instruction_sources_sha256")
        != expected_sources["effective_instruction_sources_sha256"]
        or value.get("instruction_sources_count")
        != expected_sources["effective_instruction_sources_count"]
        or value.get("context_control_overlay_sha256")
        != request["context_control_overlay_sha256"]
    ):
        raise BoundedSpanCanaryError("epoch-13 thread binding drifted")
    return SimpleNamespace(
        thread_id=value["thread_id"],
        model=MODEL,
        cwd=str(PROJECT_ROOT.resolve()),
        ephemeral=True,
        base_instructions_sha256=value["base_instructions_sha256"],
        base_instructions_bytes=value["base_instructions_bytes"],
        instruction_sources_sha256=value["instruction_sources_sha256"],
        instruction_sources_count=value["instruction_sources_count"],
    )


def _verify_thread_records(root: Path, value: Mapping[str, Any]) -> None:
    paths = _paths(root)
    if (
        value.get("runtime_contract") != _record(paths["contract"])
        or value.get("runtime_lock") != _record(paths["lock"])
        or value.get("operator_authorization") != _record(paths["authorization"])
        or value.get("turn_manifest") != _record(paths["manifest"])
    ):
        raise BoundedSpanCanaryError("epoch-13 thread lineage records drifted")


def _attempt_payload(root: Path, authorization: Mapping[str, Any]) -> dict[str, Any]:
    paths = _paths(root)
    return {
        "schema_version": ATTEMPT_VERSION,
        "state": "declared_before_capacity_thread_or_turn",
        "turn_name": TURN_NAME,
        "runtime_contract": _record(paths["contract"]),
        "runtime_lock": _record(paths["lock"]),
        "operator_authorization": _record(paths["authorization"]),
        "turn_manifest": _record(paths["manifest"]),
        "operator_authorization_id": authorization["operator_authorization_id"],
        "model": MODEL,
        "effort": EFFORT,
        "semantic_model_call_cap": 1,
        "semantic_retry_count": 0,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
    }


def _capacity_request(
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    *,
    boundary: str,
) -> dict[str, Any]:
    turn = contract["turns"][0]
    return epoch7._capacity_request(  # noqa: SLF001
        root=root,
        contract=contract,
        authorization=authorization,
        turn=turn,
        boundary=boundary,
        completed_turn_count=0,
    )


def _verify_capacity_bundles(
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    *,
    historical: bool,
) -> None:
    paths = _paths(root)
    for boundary, role in (
        ("initial_before_thread", "initial_capacity"),
        ("preturn_before_turn", "preturn_capacity"),
    ):
        bundle = paths[role]
        if not bundle.exists():
            continue
        try:
            epoch7._verify_capacity_bundle(  # noqa: SLF001
                bundle,
                expected_request=_capacity_request(
                    root, contract, authorization, boundary=boundary
                ),
                contract=contract,
                authorization=authorization,
                historical=historical,
                require_available=False,
            )
        except epoch7.CanonicalV31Epoch7AuthorityRuntimeError as exc:
            raise BoundedSpanCanaryError(str(exc)) from exc


def _artifact_records(root: Path) -> dict[str, Any]:
    paths = _paths(root)
    roles = (
        "contract", "lock", "authorization", "request", "prompt", "base", "schema",
        "manifest", "attempt", "thread", "dispatch", "sidecar", "output", "labels",
        "provenance", "fidelity", "rejection",
    )
    records = {role: _record(paths[role]) for role in roles if paths[role].is_file()}
    for capacity_role in ("initial_capacity", "preturn_capacity"):
        bundle = paths[capacity_role]
        if bundle.is_dir():
            records[capacity_role] = {
                name: _record(bundle / filename)
                for name, filename in (
                    ("request", "request.json"),
                    ("provider_response", "provider-response.private.json"),
                    ("measurement", "measurement.json"),
                )
            }
    return records


def _derive_outcome(root: Path, *, write_projection: bool) -> dict[str, Any]:
    paths = _paths(root)
    contract = _load_object(paths["contract"], label="runtime contract")
    authorization = verify_authorization(
        root, expected_authorization_id=None, require_current=False
    )
    request = _load_object(paths["request"], label="prepared request")
    adapter.validate_prepared_request(request)
    _verify_capacity_bundles(root, contract, authorization, historical=True)
    if not paths["attempt"].is_file():
        raise BoundedSpanCanaryError("terminal cannot exist without a declared attempt")
    attempt = _load_object(paths["attempt"], label="semantic attempt")
    if attempt != _attempt_payload(root, authorization):
        raise BoundedSpanCanaryError("semantic attempt drifted")
    if not paths["dispatch"].is_file():
        reason = "epoch13_capacity_or_predispatch_waiting_no_semantic_call"
        if paths["initial_capacity"].is_dir():
            measurement = _load_object(
                paths["initial_capacity"] / "measurement.json",
                label="initial capacity measurement",
            )
            if measurement.get("capacity_available") is False:
                reason = "epoch13_capacity_waiting_zero_semantic_calls"
        return {
            "state": "waiting",
            "reason": reason,
            "semantic_model_call_count": 0,
            "unknown_usage_turn_count": 0,
            "usage": _zero_usage(),
            "wall_elapsed_seconds": 0.0,
            "failed_checks": [],
            "diagnostic": None,
            "thread_ids": [],
            "turn_ids": [],
        }
    dispatch = _load_object(paths["dispatch"], label="semantic dispatch")
    expected_capacity_records = {
        role: {
            name: _record(paths[role] / filename)
            for name, filename in (
                ("request", "request.json"),
                ("provider_response", "provider-response.private.json"),
                ("measurement", "measurement.json"),
            )
        }
        for role in ("initial_capacity", "preturn_capacity")
    }
    if (
        dispatch.get("schema_version") != DISPATCH_VERSION
        or dispatch.get("state") != "semantic_turn_dispatch_committed"
        or dispatch.get("turn_name") != TURN_NAME
        or dispatch.get("request") != _record(paths["request"])
        or dispatch.get("runtime_lock") != _record(paths["lock"])
        or dispatch.get("operator_authorization") != _record(paths["authorization"])
        or dispatch.get("thread") != _record(paths["thread"])
        or dispatch.get("initial_capacity")
        != expected_capacity_records["initial_capacity"]
        or dispatch.get("preturn_capacity")
        != expected_capacity_records["preturn_capacity"]
        or dispatch.get("semantic_retry_count") != 0
    ):
        raise BoundedSpanCanaryError("semantic dispatch marker drifted")
    if not paths["thread"].is_file():
        raise BoundedSpanCanaryError("semantic dispatch lacks thread binding")
    thread_payload = _load_object(paths["thread"], label="thread binding")
    thread = _thread_from_payload(thread_payload, request)
    _verify_thread_records(root, thread_payload)
    if not paths["sidecar"].is_file():
        return {
            "state": "waiting",
            "reason": "epoch13_interrupted_semantic_attempt_preserved_no_replay",
            "semantic_model_call_count": 1,
            "unknown_usage_turn_count": 1,
            "usage": _zero_usage(),
            "wall_elapsed_seconds": 0.0,
            "failed_checks": [],
            "diagnostic": None,
            "thread_ids": [thread.thread_id],
            "turn_ids": [],
        }
    if not paths["output"].is_file():
        sidecar = _load_object(paths["sidecar"], label="terminal sidecar")
        common_lineage_valid = bool(
            sidecar.get("schema_version")
            == codex_app_server.TURN_SIDECAR_SCHEMA_VERSION
            and sidecar.get("client_version")
            == codex_app_server.APP_SERVER_CLIENT_VERSION
            and sidecar.get("cli_version")
            == codex_app_server.PINNED_CODEX_CLI_VERSION
            and sidecar.get("protocol_schema_sha256")
            == adapter.base._sha256_file(codex_app_server.PROTOCOL_SCHEMA_PATH)  # noqa: SLF001
            and sidecar.get("transport") == "stdio"
            and sidecar.get("synthetic_debug_errors") is False
            and sidecar.get("auth_type") == "chatgpt"
            and sidecar.get("plan_type") == "pro"
            and sidecar.get("thread_id") == thread.thread_id
            and sidecar.get("model") == MODEL
            and sidecar.get("effort") == EFFORT
            and sidecar.get("thread_mode") == "new_thread"
            and sidecar.get("batch_size") == request["effective_batch_size"]
            and sidecar.get("prompt_sha256") == request["prompt_sha256"]
            and sidecar.get("prompt_bytes") == len(request["prompt"].encode("utf-8"))
            and sidecar.get("base_instructions_sha256")
            == request["base_instructions_sha256"]
            and sidecar.get("base_instructions_bytes")
            == len(request["base_instructions"].encode("utf-8"))
            and sidecar.get("output_schema_sha256")
            == request["output_schema_sha256"]
            and sidecar.get("output_schema_bytes")
            == len(_canonical_json(request["output_schema"]).encode("utf-8"))
            and sidecar.get("recovery_reran_model") is False
        )
        if not common_lineage_valid:
            raise BoundedSpanCanaryError("terminal sidecar lineage drifted")
        structured_rejection = sidecar.get("error_class") == "structured_output_invalid"
        usage = _zero_usage()
        unknown = 1
        if sidecar.get("usage_status") == "measured" and sidecar.get("usage_complete") is True:
            usage = adapter.base._usage_values(  # noqa: SLF001
                sidecar.get("thread_total_usage"), "thread_total_usage"
            )
            unknown = 0
        diagnostic = {
            "error_class": str(sidecar.get("error_class") or "incomplete_sidecar"),
            "diagnostic_path": (
                "structured_output_invalid"
                if structured_rejection
                else "semantic_output_absent_or_accounting_incomplete"
            ),
        }
        return {
            "state": "rejected" if structured_rejection else "waiting",
            "reason": (
                "epoch13_structured_output_rejected"
                if structured_rejection
                else "epoch13_interrupted_semantic_attempt_preserved_no_replay"
            ),
            "semantic_model_call_count": 1,
            "unknown_usage_turn_count": unknown,
            "usage": usage,
            "wall_elapsed_seconds": float(sidecar.get("wall_elapsed_seconds") or 0.0),
            "failed_checks": ["structured_output_validity"] if structured_rejection else [],
            "diagnostic": diagnostic,
            "thread_ids": [thread.thread_id],
            "turn_ids": [sidecar["turn_id"]] if isinstance(sidecar.get("turn_id"), str) else [],
        }
    try:
        telemetry = adapter.validate_turn_sidecar(
            request,
            paths["sidecar"],
            output_path=paths["output"],
            expected_thread=thread,
        )
    except adapter.CanonicalV31TelemetryError as exc:
        sidecar = _load_object(paths["sidecar"], label="incomplete sidecar")
        error_class = sidecar.get("error_class")
        state = "rejected" if error_class == "structured_output_invalid" else "waiting"
        reason = (
            "epoch13_structured_output_rejected"
            if state == "rejected"
            else "epoch13_incomplete_or_unknown_accounting_waiting_no_replay"
        )
        return {
            "state": state,
            "reason": reason,
            "semantic_model_call_count": 1,
            "unknown_usage_turn_count": 1,
            "usage": _zero_usage(),
            "wall_elapsed_seconds": float(sidecar.get("wall_elapsed_seconds") or 0.0),
            "failed_checks": ["structured_output_validity"] if state == "rejected" else [],
            "diagnostic": {
                "error_class": type(exc).__name__,
                "diagnostic_path": "structured_output_invalid" if state == "rejected" else "unknown_usage_or_incomplete_sidecar",
            },
            "thread_ids": [thread.thread_id],
            "turn_ids": [sidecar["turn_id"]] if isinstance(sidecar.get("turn_id"), str) else [],
        }
    raw_output = _load_object(paths["output"], label="raw semantic output")
    failed_checks: list[str] = []
    diagnostic: dict[str, Any] | None = None
    try:
        projected = adapter.validate_and_project_output(request, raw_output)
    except adapter.CanonicalV31OutputError as exc:
        projected = None
        failed_checks.append("semantic_output_validity")
        diagnostic = {
            "error_class": type(exc).__name__,
            "diagnostic_path": str(exc),
            "diagnostic_sha256": _sha256_bytes(str(exc).encode("utf-8")),
        }
    if telemetry["thread_total_usage"]["total_tokens"] > MAXIMUM_TOTAL_TOKENS:
        failed_checks.append("measured_total_token_acceptance_ceiling")
    if projected is not None:
        values = {
            "labels": projected["labels"],
            "provenance": projected["provenance"],
            "fidelity": projected["fidelity"],
        }
        for role, value in values.items():
            if write_projection and not paths[role].exists():
                _write_json(paths[role], value)
            if not paths[role].is_file() or _load_object(paths[role], label=role) != value:
                raise BoundedSpanCanaryError(f"{role} artifact drifted")
        if any(
            record["evidence_end"] - record["evidence_start"]
            > adapter.CANONICAL_EVIDENCE_MAX_CHARS
            for segment in projected["provenance"]["segments"]
            for collection in ("discourse_events", "concept_candidates")
            for record in segment[collection]
        ):
            raise BoundedSpanCanaryError("projected evidence span exceeded canonical bound")
    state = "passed" if not failed_checks else "rejected"
    reason = (
        "epoch13_bounded_span_canary_passed"
        if state == "passed"
        else (
            "epoch13_bounded_span_canary_cost_rejected"
            if failed_checks == ["measured_total_token_acceptance_ceiling"]
            else "epoch13_bounded_span_canary_semantic_output_rejected"
        )
    )
    outcome = {
        "state": state,
        "reason": reason,
        "semantic_model_call_count": 1,
        "unknown_usage_turn_count": 0,
        "usage": telemetry["thread_total_usage"],
        "wall_elapsed_seconds": telemetry["wall_elapsed_seconds"],
        "failed_checks": sorted(set(failed_checks)),
        "diagnostic": diagnostic,
        "thread_ids": [telemetry["thread_id"]],
        "turn_ids": [telemetry["turn_id"]],
    }
    expected_rejection = (
        {
            "schema_version": REJECTION_VERSION,
            "turn_name": TURN_NAME,
            **copy.deepcopy(diagnostic),
        }
        if diagnostic is not None
        else None
    )
    if paths["rejection"].exists():
        if expected_rejection is None or _load_object(
            paths["rejection"], label="semantic rejection"
        ) != expected_rejection:
            raise BoundedSpanCanaryError("semantic rejection artifact drifted")
    return outcome


def _receipt_payload(
    root: Path,
    *,
    created_at: str,
    outcome: Mapping[str, Any],
) -> dict[str, Any]:
    paths = _paths(root)
    return {
        "schema_version": RECEIPT_VERSION,
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "state": outcome["state"],
        "terminal_reason": outcome["reason"],
        "created_at": created_at,
        "output_root": str(root.resolve()),
        "new_semantic_model_call_count": outcome["semantic_model_call_count"],
        "semantic_retry_count": 0,
        "new_unknown_usage_turn_count": outcome["unknown_usage_turn_count"],
        "new_measured_usage": copy.deepcopy(outcome["usage"]),
        "new_wall_elapsed_seconds": outcome["wall_elapsed_seconds"],
        "failed_checks": copy.deepcopy(outcome["failed_checks"]),
        "diagnostic": copy.deepcopy(outcome["diagnostic"]),
        "thread_ids": copy.deepcopy(outcome["thread_ids"]),
        "semantic_turn_ids": copy.deepcopy(outcome["turn_ids"]),
        "runtime_contract": _record(paths["contract"]),
        "runtime_lock": _record(paths["lock"]),
        "operator_authorization": _record(paths["authorization"]),
        "directive": _record(DIRECTIVE_PATH),
        "semantic_plan": _record(PLAN_PATH),
        "artifact_records": _artifact_records(root),
        "architecture_class": "exact_structural_bounded_evidence_span_catalog",
        "selected_span_bound_characters": adapter.CANONICAL_EVIDENCE_MAX_CHARS,
        "source_unit_max_characters": adapter.SOURCE_UNIT_MAX_CHARS,
        "winner_frozen": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "next_authorized_action": (
            "freeze_fresh_six_arm_development_matrix"
            if outcome["state"] == "passed"
            else (
                "no_replay_bounded_recovery_only"
                if outcome["state"] == "waiting"
                else "reject_bounded_span_architecture_without_field_patch"
            )
        ),
    }


def _write_terminal(root: Path) -> dict[str, Any]:
    paths = _paths(root)
    outcome = _derive_outcome(root, write_projection=True)
    if (
        outcome["state"] == "rejected"
        and outcome["diagnostic"] is not None
        and not paths["rejection"].exists()
    ):
        _write_json(
            paths["rejection"],
            {
                "schema_version": REJECTION_VERSION,
                "turn_name": TURN_NAME,
                **copy.deepcopy(outcome["diagnostic"]),
            },
        )
    outcome = _derive_outcome(root, write_projection=True)
    created_at = _now().isoformat()
    payload = _receipt_payload(root, created_at=created_at, outcome=outcome)
    _write_json(paths["receipt"], payload)
    _write_json(paths["terminal"], payload)
    return verify_receipt(root)


def verify_receipt(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    paths = _paths(output_root)
    if not paths["receipt"].is_file() or not paths["terminal"].is_file():
        raise BoundedSpanCanaryError("epoch-13 terminal mirrors are incomplete")
    receipt = _load_object(paths["receipt"], label="epoch-13 receipt")
    terminal = _load_object(paths["terminal"], label="epoch-13 terminal")
    if receipt != terminal:
        raise BoundedSpanCanaryError("epoch-13 terminal mirrors differ")
    _parse_timestamp(receipt.get("created_at"), label="receipt created_at")
    outcome = _derive_outcome(output_root, write_projection=False)
    expected = _receipt_payload(
        output_root, created_at=receipt["created_at"], outcome=outcome
    )
    if receipt != expected:
        raise BoundedSpanCanaryError("epoch-13 receipt drifted")
    return receipt


async def execute_canary(
    *,
    root: Path = DEFAULT_ROOT,
    operator_authorization_id: str,
) -> dict[str, Any]:
    _reject_external_auth_material()
    output_root = root.expanduser().resolve()
    paths = _paths(output_root)
    if paths["receipt"].is_file() or paths["terminal"].is_file():
        return verify_receipt(output_root)
    verify_runtime(output_root)
    authorization = verify_authorization(
        output_root,
        expected_authorization_id=operator_authorization_id,
        require_current=True,
    )
    contract = _load_object(paths["contract"], label="runtime contract")
    request = _load_object(paths["request"], label="prepared request")
    if paths["attempt"].exists():
        return _write_terminal(output_root)
    paths["attempt"].parent.mkdir(parents=True, exist_ok=True)
    _write_json(paths["attempt"], _attempt_payload(output_root, authorization))
    if adapter._client_factory is not TRUSTED_CLIENT_FACTORY:  # noqa: SLF001
        raise BoundedSpanCanaryError("live canary client factory binding drifted")
    try:
        async with TRUSTED_CLIENT_FACTORY() as client:
            account = getattr(client, "account_summary", None)
            if (
                not isinstance(account, Mapping)
                or account.get("type") != "chatgpt"
                or account.get("plan_type") != "pro"
            ):
                raise BoundedSpanCanaryError("epoch-13 requires managed ChatGPT Pro auth")
            await epoch7._probe_and_publish(  # noqa: SLF001
                client=client,
                bundle_root=paths["initial_capacity"],
                request=_capacity_request(
                    output_root,
                    contract,
                    authorization,
                    boundary="initial_before_thread",
                ),
                contract=contract,
                authorization=authorization,
            )
            thread = await client.start_thread(
                model=MODEL,
                base_instructions=request["base_instructions"],
                cwd=PROJECT_ROOT,
                ephemeral=True,
            )
            _write_json(
                paths["thread"],
                _thread_payload(thread, root=output_root, request=request),
            )
            await epoch7._probe_and_publish(  # noqa: SLF001
                client=client,
                bundle_root=paths["preturn_capacity"],
                request=_capacity_request(
                    output_root,
                    contract,
                    authorization,
                    boundary="preturn_before_turn",
                ),
                contract=contract,
                authorization=authorization,
            )
            _write_json(
                paths["dispatch"],
                {
                    "schema_version": DISPATCH_VERSION,
                    "state": "semantic_turn_dispatch_committed",
                    "turn_name": TURN_NAME,
                    "request": _record(paths["request"]),
                    "runtime_lock": _record(paths["lock"]),
                    "operator_authorization": _record(paths["authorization"]),
                    "thread": _record(paths["thread"]),
                    "initial_capacity": {
                        name: _record(paths["initial_capacity"] / filename)
                        for name, filename in (
                            ("request", "request.json"),
                            ("provider_response", "provider-response.private.json"),
                            ("measurement", "measurement.json"),
                        )
                    },
                    "preturn_capacity": {
                        name: _record(paths["preturn_capacity"] / filename)
                        for name, filename in (
                            ("request", "request.json"),
                            ("provider_response", "provider-response.private.json"),
                            ("measurement", "measurement.json"),
                        )
                    },
                    "semantic_retry_count": 0,
                },
            )
            result = await client.run_structured_turn(
                thread=thread,
                effort=EFFORT,
                prompt=request["prompt"],
                output_schema=request["output_schema"],
                sidecar_path=paths["sidecar"],
                output_path=paths["output"],
                batch_size=request["effective_batch_size"],
                thread_mode="new_thread",
                timeout_seconds=float(MAXIMUM_WALL_SECONDS),
            )
            if result.status_ok is not True:
                raise BoundedSpanCanaryWaiting("semantic turn did not complete")
            telemetry = adapter.validate_turn_sidecar(
                request,
                paths["sidecar"],
                output_path=paths["output"],
                expected_thread=thread,
            )
            raw_output = _load_object(paths["output"], label="raw output")
            if (
                result.thread_id != telemetry["thread_id"]
                or result.turn_id != telemetry["turn_id"]
                or not isinstance(result.output, Mapping)
                or _canonical_json(result.output) != _canonical_json(raw_output)
            ):
                raise BoundedSpanCanaryError("in-memory semantic result lineage drifted")
    except (
        epoch7.CanonicalV31Epoch7AuthorityWaiting,
        BoundedSpanCanaryWaiting,
        codex_app_server.AppServerError,
        asyncio.TimeoutError,
    ):
        return _write_terminal(output_root)
    return _write_terminal(output_root)


def status_canary(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    paths = _paths(output_root)
    if paths["receipt"].is_file() and paths["terminal"].is_file():
        receipt = verify_receipt(output_root)
        return {
            "schema_version": "pif_canonical_v31_bounded_span_canary_status_v1",
            "state": receipt["state"],
            "reason": receipt["terminal_reason"],
            "semantic_model_call_count": receipt["new_semantic_model_call_count"],
        }
    runtime = verify_runtime(output_root)
    return {
        "schema_version": "pif_canonical_v31_bounded_span_canary_status_v1",
        "state": "authorized" if paths["authorization"].is_file() else "prepared",
        "reason": "ready_for_exactly_one_bounded_span_canary_turn",
        "semantic_model_call_count": int(paths["dispatch"].is_file()),
        "runtime_lock": runtime["runtime_lock"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("prepare")
    authorize = sub.add_parser("authorize")
    authorize.add_argument("--operator-authorization-id", required=True)
    execute = sub.add_parser("execute")
    execute.add_argument("--operator-authorization-id", required=True)
    sub.add_parser("verify-runtime")
    sub.add_parser("verify")
    sub.add_parser("status")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "prepare":
        value = prepare_canary(args.root)
    elif args.command == "authorize":
        value = authorize_canary(
            root=args.root,
            operator_authorization_id=args.operator_authorization_id,
        )
    elif args.command == "execute":
        value = asyncio.run(
            execute_canary(
                root=args.root,
                operator_authorization_id=args.operator_authorization_id,
            )
        )
    elif args.command == "verify-runtime":
        value = verify_runtime(args.root)
    elif args.command == "verify":
        value = verify_receipt(args.root)
    else:
        value = status_canary(args.root)
    print(_pretty_json(value), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__: Sequence[str] = (
    "DEFAULT_ROOT",
    "BoundedSpanCanaryError",
    "authorize_canary",
    "execute_canary",
    "prepare_canary",
    "status_canary",
    "verify_authorization",
    "verify_receipt",
    "verify_runtime",
)
