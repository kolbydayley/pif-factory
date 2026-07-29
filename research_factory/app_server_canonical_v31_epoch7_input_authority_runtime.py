from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import math
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_development_matrix_runtime as capacity_runtime
from . import app_server_canonical_v31_episode_batch as adapter
from . import app_server_canonical_v31_epoch7_input_authority_plan as authority_plan
from . import app_server_canonical_v31_epoch7_input_package as input_package
from . import app_server_expanded_cap_development_matrix as legacy_matrix
from . import codex_app_server
from .labels import ValidationError, validate_label_output


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_AUTHORITY_ROOT = authority_plan.DEFAULT_ROOT
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
    / "canonical-v31-epoch7-input-authority-runtime-v1"
)

CONTRACT_VERSION = "pif_canonical_v31_epoch7_input_authority_runtime_contract_v1"
RUNTIME_LOCK_VERSION = "pif_canonical_v31_epoch7_input_authority_execution_lock_v1"
PREAUTHORIZATION_VERSION = (
    "pif_canonical_v31_epoch7_input_authority_preauthorization_receipt_v1"
)
AUTHORIZATION_VERSION = "pif_canonical_v31_epoch7_input_authority_authorization_v1"
ATTEMPT_VERSION = "pif_canonical_v31_epoch7_input_authority_attempt_v1"
CAPACITY_REQUEST_VERSION = "pif_canonical_v31_epoch7_input_authority_capacity_request_v1"
CAPACITY_MEASUREMENT_VERSION = (
    "pif_canonical_v31_epoch7_input_authority_capacity_measurement_v1"
)
THREAD_BINDING_VERSION = "pif_canonical_v31_epoch7_input_authority_thread_binding_v1"
TURN_RESULT_VERSION = "pif_canonical_v31_epoch7_input_authority_turn_result_v1"
MERGED_OUTPUT_VERSION = "pif_canonical_v31_epoch7_merged_authority_output_v1"
EXECUTION_RECEIPT_VERSION = (
    "pif_canonical_v31_epoch7_input_authority_execution_receipt_v1"
)

CONTRACT_FILENAME = "runtime-contract.json"
RUNTIME_LOCK_FILENAME = "runtime-lock.json"
PREAUTHORIZATION_FILENAME = "preauthorization-receipt.json"
AUTHORIZATION_FILENAME = "operator-authorization.json"
MERGED_OUTPUT_FILENAME = "merged-authority-output.private.json"
EXECUTION_RECEIPT_FILENAME = "authority-execution-receipt.json"
TERMINAL_FILENAME = "terminal.json"
TURNS_DIRECTORY = "turns"

MAXIMUM_AUTHORIZATION_SECONDS = 4 * 60 * 60
MAXIMUM_SNAPSHOT_AGE_SECONDS = 120
AUTHORIZATION_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}")
USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
FORBIDDEN_AUTH_ENVIRONMENT = {
    "OPENAI_API_KEY",
    "OPENAI_ORG_ID",
    "OPENAI_PROJECT_ID",
    "CODEX_API_KEY",
    "CODEX_SESSION_TOKEN",
    "CHATGPT_SESSION_TOKEN",
}
TRUSTED_CLIENT_FACTORY = adapter._client_factory  # noqa: SLF001 - checksum-bound live surface
TRUSTED_CAPACITY_PARSER = capacity_runtime.parse_trusted_rate_limit_capacity
_HASH_CACHE: dict[tuple[str, int, int, int, int, int], str] = {}


class CanonicalV31Epoch7AuthorityRuntimeError(RuntimeError):
    pass


class CanonicalV31Epoch7AuthorityWaiting(CanonicalV31Epoch7AuthorityRuntimeError):
    pass


class CanonicalV31Epoch7AuthorityRejected(CanonicalV31Epoch7AuthorityRuntimeError):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _hash_file(path: Path) -> tuple[str, int]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file() or resolved.is_symlink():
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            f"required runtime artifact is absent or unsafe: {resolved}"
        )
    stat = resolved.stat()
    identity = (
        str(resolved),
        int(stat.st_dev),
        int(stat.st_ino),
        int(stat.st_size),
        int(stat.st_mtime_ns),
        int(stat.st_ctime_ns),
    )
    digest = _HASH_CACHE.get(identity)
    if digest is None:
        hasher = hashlib.sha256()
        with resolved.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        digest = hasher.hexdigest()
        _HASH_CACHE[identity] = digest
    return digest, int(stat.st_size)


def _parse_timestamp(value: Any, *, label: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise CanonicalV31Epoch7AuthorityRuntimeError(f"{label} is absent")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            f"{label} is not an ISO timestamp"
        ) from exc
    if parsed.tzinfo is None:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            f"{label} is not timezone-aware"
        )
    return parsed.astimezone(timezone.utc)


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise CanonicalV31Epoch7AuthorityRuntimeError("current time is not timezone-aware")
    return current.astimezone(timezone.utc)


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    digest, size = _hash_file(resolved)
    return {
        "path": str(resolved),
        "sha256": digest,
        "size_bytes": size,
    }


def _verify_record(value: Any, *, label: str) -> Path:
    if not isinstance(value, Mapping) or set(value) != {
        "path",
        "sha256",
        "size_bytes",
    }:
        raise CanonicalV31Epoch7AuthorityRuntimeError(f"{label} record is malformed")
    path_value = value.get("path")
    size_value = value.get("size_bytes")
    if (
        not isinstance(path_value, str)
        or not path_value
        or not _is_sha256(value.get("sha256"))
        or isinstance(size_value, bool)
        or not isinstance(size_value, int)
        or size_value < 0
    ):
        raise CanonicalV31Epoch7AuthorityRuntimeError(f"{label} record is malformed")
    path = Path(path_value).expanduser().resolve()
    if not path.is_file() or path.is_symlink():
        raise CanonicalV31Epoch7AuthorityRuntimeError(f"{label} is absent or unsafe")
    digest, size = _hash_file(path)
    if size != size_value or digest != value["sha256"]:
        raise CanonicalV31Epoch7AuthorityRuntimeError(f"{label} record drifted")
    return path


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalV31Epoch7AuthorityRuntimeError(f"{label} is unreadable") from exc
    if not isinstance(value, dict):
        raise CanonicalV31Epoch7AuthorityRuntimeError(f"{label} is not an object")
    return value


def _write_immutable_bytes(path: Path, raw: bytes) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(resolved, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        existing = resolved.read_bytes()
        if existing != raw:
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                f"immutable runtime artifact drifted: {resolved}"
            )
        return _record(resolved)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        resolved.unlink(missing_ok=True)
        raise
    return _record(resolved)


def _write_immutable_json(path: Path, value: Any) -> dict[str, Any]:
    return _write_immutable_bytes(path, _pretty_json(value).encode("ascii"))


def _runtime_lock() -> dict[str, Any]:
    adapter_binding = adapter.build_six_arm_matrix_binding()
    return {
        "schema_version": RUNTIME_LOCK_VERSION,
        "state": "frozen_zero_call_preauthorization",
        "authority_runtime_module": _record(Path(__file__)),
        "authority_plan_module": _record(Path(authority_plan.__file__)),
        "authority_input_package_module": _record(Path(input_package.__file__)),
        "trusted_capacity_module": _record(Path(capacity_runtime.__file__)),
        "canonical_adapter_module": _record(Path(adapter.__file__)),
        "official_app_server_transport_module": _record(Path(codex_app_server.__file__)),
        "legacy_manifest_verifier_module": _record(Path(legacy_matrix.__file__)),
        "pinned_official_codex_binary": _record(adapter.PINNED_CODEX),
        "pinned_protocol_schema": _record(codex_app_server.PROTOCOL_SCHEMA_PATH),
        "pinned_codex_cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
        "canonical_adapter_binding": copy.deepcopy(adapter_binding),
        "canonical_adapter_binding_sha256": _sha256_bytes(
            _canonical_json(adapter_binding).encode("ascii")
        ),
        "trusted_client_factory": "canonical_adapter._client_factory",
        "trusted_capacity_rpc": "account/rateLimits/read",
        "model": authority_plan.MODEL,
        "effort": authority_plan.EFFORT,
        "official_persistent_codex_app_server_only": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "semantic_retry_count": 0,
        "codex_exec_forbidden": True,
        "api_key_and_raw_session_token_forbidden": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _build_contract(authority_root: Path) -> dict[str, Any]:
    receipt = authority_plan.verify_authority_plan(authority_root)
    plan_path = _verify_record(receipt["authority_plan"], label="authority plan")
    plan = _load_object(plan_path, label="authority plan")
    capacity_path = _verify_record(receipt["capacity_policy"], label="authority capacity policy")
    capacity = _load_object(capacity_path, label="authority capacity policy")
    runtime_path = _verify_record(receipt["runtime_lock"], label="authority plan runtime lock")
    turn_rows: list[dict[str, Any]] = []
    for ordinal, row in enumerate(plan["turns"]):
        manifest_path = _verify_record(
            row["turn_manifest"], label=f"authority turn manifest {ordinal}"
        )
        manifest = _load_object(manifest_path, label=f"authority turn manifest {ordinal}")
        turn_rows.append(
            {
                "ordinal": ordinal,
                "turn_id": row["turn_id"],
                "episode_id": row["episode_id"],
                "invalid_reference_count": row["invalid_reference_count"],
                "turn_manifest": _record(manifest_path),
                "input": copy.deepcopy(manifest["input"]),
                "prompt": copy.deepcopy(manifest["prompt"]),
                "base_instructions": copy.deepcopy(manifest["base_instructions"]),
                "output_schema": copy.deepcopy(manifest["output_schema"]),
            }
        )
    if [row["ordinal"] for row in turn_rows] != list(range(authority_plan.EXACT_TURN_COUNT)):
        raise CanonicalV31Epoch7AuthorityRuntimeError("authority turn order drifted")
    return {
        "schema_version": CONTRACT_VERSION,
        "state": "frozen_zero_call_operator_authorization_required",
        "authority_plan_root": str(authority_root.expanduser().resolve()),
        "authority_plan_receipt": _record(
            authority_root.expanduser().resolve() / authority_plan.RECEIPT_FILENAME
        ),
        "authority_plan": _record(plan_path),
        "authority_capacity_policy": _record(capacity_path),
        "authority_plan_runtime_lock": _record(runtime_path),
        "turns": turn_rows,
        "exact_turn_count": authority_plan.EXACT_TURN_COUNT,
        "invalid_reference_count": 26,
        "preserved_valid_reference_count": 6,
        "model": authority_plan.MODEL,
        "effort": authority_plan.EFFORT,
        "maximum_total_tokens_per_turn": capacity["maximum_total_tokens_per_turn"],
        "phase_total_token_bound": capacity["phase_total_token_bound"],
        "quota_points_per_million_tokens": capacity["quota_points_per_million_tokens"],
        "projected_phase_quota_points": capacity["projected_phase_quota_points"],
        "minimum_remaining_reserve_percent": capacity[
            "minimum_remaining_reserve_percent"
        ],
        "capacity_safety_margin_percent": capacity["capacity_safety_margin_percent"],
        "maximum_wall_seconds_per_turn": capacity["maximum_wall_seconds_per_turn"],
        "phase_wall_seconds_ceiling": capacity["phase_wall_seconds_ceiling"],
        "operator_wall_safety_margin_seconds": capacity[
            "operator_wall_safety_margin_seconds"
        ],
        "maximum_rate_limit_snapshot_age_seconds": MAXIMUM_SNAPSHOT_AGE_SECONDS,
        "one_persistent_app_server_process_per_invocation": True,
        "fresh_ephemeral_thread_per_episode": True,
        "fresh_capacity_probe_before_thread_and_turn": True,
        "completed_prefix_adoptable_without_replay": True,
        "partial_attempt_replay_forbidden": True,
        "semantic_retry_count": 0,
        "operator_authorization_present": False,
        "executable": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def freeze_preauthorization(
    *,
    root: Path = DEFAULT_ROOT,
    authority_root: Path = DEFAULT_AUTHORITY_ROOT,
) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    if output_root.exists():
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "authority runtime root must be fresh and absent"
        )
    contract = _build_contract(authority_root.expanduser().resolve())
    lock = _runtime_lock()
    output_root.mkdir(parents=True, exist_ok=False)
    contract_record = _write_immutable_json(output_root / CONTRACT_FILENAME, contract)
    lock_record = _write_immutable_json(output_root / RUNTIME_LOCK_FILENAME, lock)
    receipt = {
        "schema_version": PREAUTHORIZATION_VERSION,
        "state": "passed_zero_call_preauthorization_only",
        "terminal_reason": "epoch7_authority_runtime_frozen_no_dispatch",
        "output_root": str(output_root),
        "runtime_contract": contract_record,
        "runtime_lock": lock_record,
        "authority_plan_receipt": copy.deepcopy(contract["authority_plan_receipt"]),
        "exact_turn_count": authority_plan.EXACT_TURN_COUNT,
        "semantic_model_call_count": 0,
        "semantic_retry_count": 0,
        "operator_authorization_present": False,
        "executable": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    _write_immutable_json(output_root / PREAUTHORIZATION_FILENAME, receipt)
    return verify_preauthorization(output_root)


def verify_preauthorization(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    if not output_root.is_dir() or output_root.is_symlink():
        raise CanonicalV31Epoch7AuthorityRuntimeError("authority runtime root is unavailable")
    allowed_root_entries = {
        CONTRACT_FILENAME,
        RUNTIME_LOCK_FILENAME,
        PREAUTHORIZATION_FILENAME,
        AUTHORIZATION_FILENAME,
        TURNS_DIRECTORY,
        MERGED_OUTPUT_FILENAME,
        EXECUTION_RECEIPT_FILENAME,
        TERMINAL_FILENAME,
    }
    unexpected = {path.name for path in output_root.iterdir()} - allowed_root_entries
    if unexpected:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "authority runtime root contains unexpected artifacts"
        )
    for filename in (CONTRACT_FILENAME, RUNTIME_LOCK_FILENAME, PREAUTHORIZATION_FILENAME):
        path = output_root / filename
        if not path.is_file() or path.is_symlink():
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "authority runtime preauthorization artifact set is incomplete"
            )
    contract = _load_object(output_root / CONTRACT_FILENAME, label="runtime contract")
    authority_root_value = contract.get("authority_plan_root")
    if not isinstance(authority_root_value, str) or not authority_root_value:
        raise CanonicalV31Epoch7AuthorityRuntimeError("authority plan root binding is absent")
    expected_contract = _build_contract(Path(authority_root_value))
    if contract != expected_contract:
        raise CanonicalV31Epoch7AuthorityRuntimeError("authority runtime contract drifted")
    lock = _load_object(output_root / RUNTIME_LOCK_FILENAME, label="runtime lock")
    if lock != _runtime_lock():
        raise CanonicalV31Epoch7AuthorityRuntimeError("authority runtime lock drifted")
    expected_receipt = {
        "schema_version": PREAUTHORIZATION_VERSION,
        "state": "passed_zero_call_preauthorization_only",
        "terminal_reason": "epoch7_authority_runtime_frozen_no_dispatch",
        "output_root": str(output_root),
        "runtime_contract": _record(output_root / CONTRACT_FILENAME),
        "runtime_lock": _record(output_root / RUNTIME_LOCK_FILENAME),
        "authority_plan_receipt": copy.deepcopy(contract["authority_plan_receipt"]),
        "exact_turn_count": authority_plan.EXACT_TURN_COUNT,
        "semantic_model_call_count": 0,
        "semantic_retry_count": 0,
        "operator_authorization_present": False,
        "executable": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    observed = _load_object(
        output_root / PREAUTHORIZATION_FILENAME, label="preauthorization receipt"
    )
    if observed != expected_receipt:
        raise CanonicalV31Epoch7AuthorityRuntimeError("preauthorization receipt drifted")
    return copy.deepcopy(observed)


def _authorization_payload(
    *,
    root: Path,
    operator_authorization_id: str,
    issued_at: datetime,
    expires_at: datetime,
) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    contract = _load_object(output_root / CONTRACT_FILENAME, label="runtime contract")
    if not AUTHORIZATION_ID_PATTERN.fullmatch(operator_authorization_id):
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "operator authorization ID is malformed"
        )
    issued = _now(issued_at)
    expires = _now(expires_at)
    if expires <= issued or (expires - issued).total_seconds() > MAXIMUM_AUTHORIZATION_SECONDS:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "operator authorization window is invalid or unbounded"
        )
    return {
        "schema_version": AUTHORIZATION_VERSION,
        "state": "authorized_for_one_exact_four_turn_authority_phase",
        "authorized_by": "kolby",
        "authority": "direct_user_instruction",
        "operator_authorization_id": operator_authorization_id,
        "issued_at": issued.isoformat(),
        "expires_at": expires.isoformat(),
        "authorized_scope": "one_epoch7_four_turn_context_and_reference_authority_phase",
        "runtime_contract": _record(output_root / CONTRACT_FILENAME),
        "runtime_lock": _record(output_root / RUNTIME_LOCK_FILENAME),
        "preauthorization_receipt": _record(
            output_root / PREAUTHORIZATION_FILENAME
        ),
        "authority_plan_receipt": copy.deepcopy(contract["authority_plan_receipt"]),
        "model": contract["model"],
        "effort": contract["effort"],
        "exact_model_call_cap": contract["exact_turn_count"],
        "exact_total_token_cap": contract["phase_total_token_bound"],
        "semantic_retry_count": 0,
        "official_persistent_codex_app_server_only": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "caller_supplied_capacity_admission_allowed": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def authorize_runtime(
    *,
    root: Path = DEFAULT_ROOT,
    operator_authorization_id: str,
    issued_at: datetime,
    expires_at: datetime,
) -> dict[str, Any]:
    verify_preauthorization(root)
    output_root = root.expanduser().resolve()
    payload = _authorization_payload(
        root=output_root,
        operator_authorization_id=operator_authorization_id,
        issued_at=issued_at,
        expires_at=expires_at,
    )
    _write_immutable_json(output_root / AUTHORIZATION_FILENAME, payload)
    return verify_authorization(output_root, now=issued_at)


def verify_authorization(
    root: Path = DEFAULT_ROOT,
    *,
    expected_authorization_id: str | None = None,
    now: datetime | None = None,
    require_current: bool = True,
) -> dict[str, Any]:
    verify_preauthorization(root)
    output_root = root.expanduser().resolve()
    path = output_root / AUTHORIZATION_FILENAME
    observed = _load_object(path, label="operator authorization")
    issued = _parse_timestamp(observed.get("issued_at"), label="authorization issued_at")
    expires = _parse_timestamp(observed.get("expires_at"), label="authorization expires_at")
    authorization_id = observed.get("operator_authorization_id")
    if not isinstance(authorization_id, str):
        raise CanonicalV31Epoch7AuthorityRuntimeError("authorization ID is absent")
    expected = _authorization_payload(
        root=output_root,
        operator_authorization_id=authorization_id,
        issued_at=issued,
        expires_at=expires,
    )
    if observed != expected:
        raise CanonicalV31Epoch7AuthorityRuntimeError("operator authorization drifted")
    if expected_authorization_id is not None and authorization_id != expected_authorization_id:
        raise CanonicalV31Epoch7AuthorityRuntimeError("operator authorization ID differs")
    current = _now(now)
    if require_current and not (issued <= current < expires):
        raise CanonicalV31Epoch7AuthorityWaiting("operator authorization is not current")
    return copy.deepcopy(observed)


def _turn_paths(root: Path, turn_id: str) -> dict[str, Path]:
    turn_root = root / TURNS_DIRECTORY / turn_id
    return {
        "root": turn_root,
        "attempt": turn_root / "semantic-attempt.json",
        "initial_capacity": turn_root / "capacity" / "initial",
        "thread": turn_root / "thread-start.json",
        "preturn_capacity": turn_root / "capacity" / "preturn",
        "sidecar": turn_root / "sidecar.json",
        "raw_output": turn_root / "output.private.json",
        "validated_output": turn_root / "validated-output.private.json",
        "result": turn_root / "turn-result.json",
    }


def _usage(value: Any, *, label: str) -> dict[str, int]:
    if not isinstance(value, Mapping) or set(value) != set(USAGE_FIELDS):
        raise CanonicalV31Epoch7AuthorityRuntimeError(f"{label} usage is malformed")
    result: dict[str, int] = {}
    for field in USAGE_FIELDS:
        item = value.get(field)
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise CanonicalV31Epoch7AuthorityRuntimeError(f"{label} usage is malformed")
        result[field] = item
    if (
        result["cached_input_tokens"] > result["input_tokens"]
        or result["reasoning_output_tokens"] > result["output_tokens"]
        or result["total_tokens"]
        != result["input_tokens"] + result["output_tokens"]
    ):
        raise CanonicalV31Epoch7AuthorityRuntimeError(f"{label} usage arithmetic drifted")
    return result


def _sum_usage(values: Sequence[Mapping[str, int]]) -> dict[str, int]:
    return {field: sum(int(value[field]) for value in values) for field in USAGE_FIELDS}


def _attempt_payload(
    *,
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    turn: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": ATTEMPT_VERSION,
        "state": "declared_before_capacity_thread_or_turn",
        "ordinal": turn["ordinal"],
        "turn_id": turn["turn_id"],
        "episode_id": turn["episode_id"],
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "runtime_lock": _record(root / RUNTIME_LOCK_FILENAME),
        "operator_authorization": _record(root / AUTHORIZATION_FILENAME),
        "authority_turn_manifest": copy.deepcopy(turn["turn_manifest"]),
        "model": contract["model"],
        "effort": contract["effort"],
        "semantic_model_call_cap": 1,
        "semantic_retry_count": 0,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "operator_authorization_id": authorization["operator_authorization_id"],
    }


def _capacity_request(
    *,
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    turn: Mapping[str, Any],
    boundary: str,
    completed_turn_count: int,
) -> dict[str, Any]:
    if boundary not in {"initial_before_thread", "preturn_before_turn"}:
        raise CanonicalV31Epoch7AuthorityRuntimeError("capacity boundary is invalid")
    remaining_turn_count = int(contract["exact_turn_count"]) - completed_turn_count
    remaining_tokens = remaining_turn_count * int(
        contract["maximum_total_tokens_per_turn"]
    )
    remaining_points = math.ceil(
        remaining_tokens * int(contract["quota_points_per_million_tokens"]) / 1_000_000
    )
    return {
        "schema_version": CAPACITY_REQUEST_VERSION,
        "state": "no_semantic_dispatch_at_probe_boundary",
        "boundary": boundary,
        "provider_method": "account/rateLimits/read",
        "ordinal": turn["ordinal"],
        "turn_id": turn["turn_id"],
        "episode_id": turn["episode_id"],
        "completed_turn_count": completed_turn_count,
        "remaining_turn_count": remaining_turn_count,
        "required_remaining_total_token_ceiling": remaining_tokens,
        "required_remaining_quota_points": remaining_points,
        "required_remaining_wall_seconds_ceiling": remaining_turn_count
        * int(contract["maximum_wall_seconds_per_turn"]),
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "runtime_lock": _record(root / RUNTIME_LOCK_FILENAME),
        "operator_authorization": _record(root / AUTHORIZATION_FILENAME),
        "authority_turn_manifest": copy.deepcopy(turn["turn_manifest"]),
        "operator_authorization_id": authorization["operator_authorization_id"],
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "capacity_is_estimate_not_reservation": True,
        "semantic_thread_started": boundary == "preturn_before_turn",
        "semantic_turn_started": False,
    }


def _capacity_measurement(
    response: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    measured_at: datetime,
) -> dict[str, Any]:
    try:
        parsed = TRUSTED_CAPACITY_PARSER(response)
    except capacity_runtime.CanonicalV31CapacityUnavailable as exc:
        raise CanonicalV31Epoch7AuthorityWaiting(
            "trusted managed capacity snapshot is unavailable"
        ) from exc
    current = _now(measured_at)
    expires = _parse_timestamp(
        authorization["expires_at"], label="authorization expires_at"
    )
    remaining = int(parsed["minimum_applicable_remaining_percent"])
    reserve = int(contract["minimum_remaining_reserve_percent"])
    safety = int(contract["capacity_safety_margin_percent"])
    usable_points = max(0, remaining - reserve - safety)
    required_points = int(request["required_remaining_quota_points"])
    wall_available = max(
        0.0,
        (expires - current).total_seconds()
        - float(contract["operator_wall_safety_margin_seconds"]),
    )
    required_wall = float(request["required_remaining_wall_seconds_ceiling"])
    available = bool(
        parsed["rate_limit_reached_type"] is None
        and usable_points >= required_points
        and wall_available >= required_wall
        and current < expires
    )
    identity = {
        "request_sha256": _sha256_bytes(_canonical_json(request).encode("ascii")),
        "provider_response_sha256": parsed["provider_response_sha256"],
        "measured_at": current.isoformat(),
    }
    return {
        "schema_version": CAPACITY_MEASUREMENT_VERSION,
        "measurement_id": _sha256_bytes(_canonical_json(identity).encode("ascii")),
        "state": (
            "cleared_before_semantic_boundary"
            if available
            else "not_cleared_before_semantic_boundary"
        ),
        "measured_at": current.isoformat(),
        "expires_at": min(
            expires, current + timedelta(seconds=MAXIMUM_SNAPSHOT_AGE_SECONDS)
        ).isoformat(),
        "boundary": request["boundary"],
        "turn_id": request["turn_id"],
        "episode_id": request["episode_id"],
        "capacity_available": available,
        "capacity_unknown": False,
        "minimum_applicable_remaining_percent": remaining,
        "minimum_remaining_reserve_percent": reserve,
        "capacity_safety_margin_percent": safety,
        "usable_quota_points_above_reserve": usable_points,
        "required_remaining_quota_points": required_points,
        "required_remaining_total_token_ceiling": request[
            "required_remaining_total_token_ceiling"
        ],
        "required_remaining_wall_seconds_ceiling": required_wall,
        "available_wall_seconds_before_authorization_expiry": wall_available,
        "rate_limit_reached_type": parsed["rate_limit_reached_type"],
        "provider_capacity": parsed,
        "provider_reports_tokens_remaining": False,
        "capacity_is_estimate_not_reservation": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "semantic_thread_started": request["semantic_thread_started"],
        "semantic_turn_started": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _publish_capacity_bundle(
    bundle_root: Path,
    *,
    request: Mapping[str, Any],
    response: Mapping[str, Any],
    measurement: Mapping[str, Any],
) -> dict[str, Any]:
    parent = bundle_root.parent
    parent.mkdir(parents=True, exist_ok=True)
    if bundle_root.exists():
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "capacity bundle already exists; probe replay is forbidden"
        )
    temporary = Path(tempfile.mkdtemp(prefix=f".{bundle_root.name}.", dir=parent))
    try:
        request_record = _write_immutable_json(temporary / "request.json", request)
        response_record = _write_immutable_json(
            temporary / "provider-response.private.json", response
        )
        measurement_record = _write_immutable_json(
            temporary / "measurement.json", measurement
        )
        directory_fd = os.open(temporary, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        os.rename(temporary, bundle_root)
        parent_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return {
        "request": _record(bundle_root / "request.json"),
        "provider_response": _record(bundle_root / "provider-response.private.json"),
        "measurement": _record(bundle_root / "measurement.json"),
    }


def _verify_capacity_bundle(
    bundle_root: Path,
    *,
    expected_request: Mapping[str, Any],
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    historical: bool,
    require_available: bool = True,
) -> dict[str, Any]:
    if not bundle_root.is_dir() or bundle_root.is_symlink() or {
        path.name for path in bundle_root.iterdir()
    } != {"request.json", "provider-response.private.json", "measurement.json"}:
        raise CanonicalV31Epoch7AuthorityRuntimeError("capacity bundle is incomplete")
    request = _load_object(bundle_root / "request.json", label="capacity request")
    response = _load_object(
        bundle_root / "provider-response.private.json", label="capacity provider response"
    )
    measurement = _load_object(
        bundle_root / "measurement.json", label="capacity measurement"
    )
    if request != expected_request:
        raise CanonicalV31Epoch7AuthorityRuntimeError("capacity request drifted")
    rebuilt = _capacity_measurement(
        response,
        request=request,
        contract=contract,
        authorization=authorization,
        measured_at=_parse_timestamp(measurement.get("measured_at"), label="measured_at"),
    )
    if measurement != rebuilt:
        raise CanonicalV31Epoch7AuthorityRuntimeError("capacity measurement drifted")
    if require_available and measurement.get("capacity_available") is not True:
        raise CanonicalV31Epoch7AuthorityWaiting("capacity did not clear the turn")
    if not historical and _now() >= _parse_timestamp(
        measurement["expires_at"], label="capacity expires_at"
    ):
        raise CanonicalV31Epoch7AuthorityWaiting("capacity measurement expired")
    return {
        "request": request,
        "response": response,
        "measurement": measurement,
        "records": {
            "request": _record(bundle_root / "request.json"),
            "provider_response": _record(
                bundle_root / "provider-response.private.json"
            ),
            "measurement": _record(bundle_root / "measurement.json"),
        },
    }


def _thread_binding(
    thread: Any,
    *,
    contract: Mapping[str, Any],
    turn: Mapping[str, Any],
) -> dict[str, Any]:
    manifest_path = _verify_record(turn["turn_manifest"], label="authority turn manifest")
    manifest = _load_object(manifest_path, label="authority turn manifest")
    base_path = _verify_record(manifest["base_instructions"], label="base instructions")
    expected_sources = adapter.expected_instruction_source_contract()
    if (
        getattr(thread, "model", None) != contract["model"]
        or getattr(thread, "ephemeral", None) is not True
        or getattr(thread, "cwd", None) != str(PROJECT_ROOT.resolve())
        or not isinstance(getattr(thread, "thread_id", None), str)
        or not thread.thread_id
        or getattr(thread, "base_instructions_sha256", None)
        != _sha256_bytes(base_path.read_bytes())
        or getattr(thread, "base_instructions_bytes", None) != base_path.stat().st_size
        or getattr(thread, "instruction_sources_sha256", None)
        != expected_sources["effective_instruction_sources_sha256"]
        or getattr(thread, "instruction_sources_count", None)
        != expected_sources["effective_instruction_sources_count"]
        or getattr(thread, "persisted_path_sha256", None) is not None
    ):
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "authority semantic thread contract drifted"
        )
    return {
        "schema_version": THREAD_BINDING_VERSION,
        "turn_id": turn["turn_id"],
        "episode_id": turn["episode_id"],
        "thread_id": thread.thread_id,
        "model": contract["model"],
        "effort": contract["effort"],
        "cwd": str(PROJECT_ROOT.resolve()),
        "ephemeral": True,
        "base_instructions_sha256": thread.base_instructions_sha256,
        "base_instructions_bytes": thread.base_instructions_bytes,
        "instruction_sources_sha256": thread.instruction_sources_sha256,
        "instruction_sources_count": thread.instruction_sources_count,
        "persisted_path_sha256": None,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _verify_thread_binding(
    value: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    turn: Mapping[str, Any],
) -> dict[str, Any]:
    manifest = _load_object(
        _verify_record(turn["turn_manifest"], label="authority turn manifest"),
        label="authority turn manifest",
    )
    base_path = _verify_record(manifest["base_instructions"], label="base instructions")
    expected_sources = adapter.expected_instruction_source_contract()
    expected_keys = {
        "schema_version",
        "turn_id",
        "episode_id",
        "thread_id",
        "model",
        "effort",
        "cwd",
        "ephemeral",
        "base_instructions_sha256",
        "base_instructions_bytes",
        "instruction_sources_sha256",
        "instruction_sources_count",
        "persisted_path_sha256",
        "quality_authorized",
        "holdout_authorized",
        "production_mutation_allowed",
    }
    if (
        set(value) != expected_keys
        or value.get("schema_version") != THREAD_BINDING_VERSION
        or value.get("turn_id") != turn["turn_id"]
        or value.get("episode_id") != turn["episode_id"]
        or value.get("model") != contract["model"]
        or value.get("effort") != contract["effort"]
        or value.get("cwd") != str(PROJECT_ROOT.resolve())
        or value.get("ephemeral") is not True
        or not isinstance(value.get("thread_id"), str)
        or not value["thread_id"]
        or value.get("base_instructions_sha256")
        != _sha256_bytes(base_path.read_bytes())
        or value.get("base_instructions_bytes") != base_path.stat().st_size
        or value.get("instruction_sources_sha256")
        != expected_sources["effective_instruction_sources_sha256"]
        or value.get("instruction_sources_count")
        != expected_sources["effective_instruction_sources_count"]
        or value.get("persisted_path_sha256") is not None
        or value.get("quality_authorized") is not False
        or value.get("holdout_authorized") is not False
        or value.get("production_mutation_allowed") is not False
    ):
        raise CanonicalV31Epoch7AuthorityRuntimeError("thread binding drifted")
    return copy.deepcopy(dict(value))


def _validate_completed_sidecar(
    *,
    sidecar_path: Path,
    output_path: Path,
    manifest: Mapping[str, Any],
    thread_binding: Mapping[str, Any],
) -> dict[str, Any]:
    sidecar = _load_object(sidecar_path, label="authority turn sidecar")
    prompt_path = _verify_record(manifest["prompt"], label="authority prompt")
    base_path = _verify_record(manifest["base_instructions"], label="base instructions")
    schema_path = _verify_record(manifest["output_schema"], label="output schema")
    schema = _load_object(schema_path, label="authority output schema")
    raw_text = output_path.read_text(encoding="utf-8")
    raw_message = raw_text[:-1] if raw_text.endswith("\n") else raw_text
    required = {
        "schema_version": codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
        "client_version": codex_app_server.APP_SERVER_CLIENT_VERSION,
        "cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
        "protocol_schema_sha256": _record(codex_app_server.PROTOCOL_SCHEMA_PATH)["sha256"],
        "transport": "stdio",
        "synthetic_debug_errors": False,
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "thread_id": thread_binding["thread_id"],
        "model": authority_plan.MODEL,
        "effort": authority_plan.EFFORT,
        "batch_size": 1,
        "thread_mode": "new_thread",
        "prompt_sha256": _sha256_bytes(prompt_path.read_bytes()),
        "prompt_bytes": prompt_path.stat().st_size,
        "base_instructions_sha256": _sha256_bytes(base_path.read_bytes()),
        "base_instructions_bytes": base_path.stat().st_size,
        "instruction_sources_sha256": thread_binding["instruction_sources_sha256"],
        "instruction_sources_count": thread_binding["instruction_sources_count"],
        "output_schema_sha256": _sha256_bytes(_canonical_json(schema).encode("utf-8")),
        "output_schema_bytes": len(_canonical_json(schema).encode("utf-8")),
        "state": "completed",
        "status": "completed",
        "error_class": None,
        "usage_status": "measured",
        "usage_complete": True,
        "recovery_reran_model": False,
        "output_sha256": _sha256_bytes(raw_message.encode("utf-8")),
    }
    for key, expected in required.items():
        if sidecar.get(key) != expected:
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                f"completed authority sidecar drifted at {key}"
            )
    try:
        _parse_timestamp(sidecar.get("started_at"), label="sidecar started_at")
        _parse_timestamp(sidecar.get("finished_at"), label="sidecar finished_at")
    except CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "completed authority sidecar timestamps drifted"
        ) from exc
    if (
        not isinstance(sidecar.get("turn_id"), str)
        or not sidecar["turn_id"]
        or not isinstance(sidecar.get("app_server_user_agent"), str)
        or not sidecar["app_server_user_agent"]
        or isinstance(sidecar.get("max_message_bytes"), bool)
        or not isinstance(sidecar.get("max_message_bytes"), int)
        or sidecar["max_message_bytes"] < 64 * 1024
        or not _is_sha256(sidecar.get("stderr_sha256"))
        or isinstance(sidecar.get("stderr_bytes"), bool)
        or not isinstance(sidecar.get("stderr_bytes"), int)
        or sidecar["stderr_bytes"] < 0
        or Path(str(sidecar.get("output_path") or "")).expanduser().resolve()
        != output_path.resolve()
        or not isinstance(sidecar.get("wall_elapsed_seconds"), (int, float))
        or isinstance(sidecar.get("wall_elapsed_seconds"), bool)
        or float(sidecar["wall_elapsed_seconds"]) < 0
    ):
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "completed authority sidecar lifecycle drifted"
        )
    usage = _usage(sidecar.get("usage"), label="authority turn")
    total = _usage(sidecar.get("thread_total_usage"), label="authority thread total")
    if usage != total:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "new-thread authority usage differs from thread total"
        )
    return {
        "sidecar": sidecar,
        "usage": usage,
        "thread_id": sidecar["thread_id"],
        "turn_id": sidecar["turn_id"],
        "wall_elapsed_seconds": float(sidecar["wall_elapsed_seconds"]),
        "raw_message": raw_message,
    }


def _verify_complete_turn(
    *,
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    turn: Mapping[str, Any],
    completed_turn_count: int,
    allow_finalize: bool,
) -> dict[str, Any]:
    paths = _turn_paths(root, str(turn["turn_id"]))
    expected_attempt = _attempt_payload(
        root=root, contract=contract, authorization=authorization, turn=turn
    )
    attempt = _load_object(paths["attempt"], label="authority semantic attempt")
    if attempt != expected_attempt:
        raise CanonicalV31Epoch7AuthorityRuntimeError("authority attempt drifted")
    initial_request = _capacity_request(
        root=root,
        contract=contract,
        authorization=authorization,
        turn=turn,
        boundary="initial_before_thread",
        completed_turn_count=completed_turn_count,
    )
    preturn_request = _capacity_request(
        root=root,
        contract=contract,
        authorization=authorization,
        turn=turn,
        boundary="preturn_before_turn",
        completed_turn_count=completed_turn_count,
    )
    initial = _verify_capacity_bundle(
        paths["initial_capacity"],
        expected_request=initial_request,
        contract=contract,
        authorization=authorization,
        historical=True,
    )
    preturn = _verify_capacity_bundle(
        paths["preturn_capacity"],
        expected_request=preturn_request,
        contract=contract,
        authorization=authorization,
        historical=True,
    )
    thread_binding = _verify_thread_binding(
        _load_object(paths["thread"], label="authority thread binding"),
        contract=contract,
        turn=turn,
    )
    manifest_path = _verify_record(turn["turn_manifest"], label="authority turn manifest")
    manifest = _load_object(manifest_path, label="authority turn manifest")
    input_path = _verify_record(manifest["input"], label="authority input")
    schema_path = _verify_record(manifest["output_schema"], label="authority output schema")
    if not paths["raw_output"].is_file() or not paths["sidecar"].is_file():
        raise CanonicalV31Epoch7AuthorityWaiting("completed authority turn evidence is partial")
    telemetry = _validate_completed_sidecar(
        sidecar_path=paths["sidecar"],
        output_path=paths["raw_output"],
        manifest=manifest,
        thread_binding=thread_binding,
    )
    try:
        raw_output = json.loads(telemetry["raw_message"])
    except json.JSONDecodeError as exc:
        raise CanonicalV31Epoch7AuthorityRejected(
            "authority turn output is not structured JSON"
        ) from exc
    turn_input = _load_object(input_path, label="authority input")
    output_schema = _load_object(schema_path, label="authority output schema")
    try:
        validated = authority_plan.validate_authority_output(
            raw_output, turn_input=turn_input, output_schema=output_schema
        )
    except authority_plan.CanonicalV31Epoch7InputAuthorityPlanError as exc:
        raise CanonicalV31Epoch7AuthorityRejected(
            "authority output failed its frozen semantic structure contract"
        ) from exc
    if paths["validated_output"].exists():
        observed_validated = _load_object(
            paths["validated_output"], label="validated authority output"
        )
        if observed_validated != validated:
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "validated authority output drifted"
            )
    elif allow_finalize:
        _write_immutable_json(paths["validated_output"], validated)
    else:
        raise CanonicalV31Epoch7AuthorityWaiting(
            "completed authority output was not deterministically finalized"
        )
    result = {
        "schema_version": TURN_RESULT_VERSION,
        "state": "completed_validated_no_retry",
        "ordinal": turn["ordinal"],
        "turn_id": turn["turn_id"],
        "episode_id": turn["episode_id"],
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "runtime_lock": _record(root / RUNTIME_LOCK_FILENAME),
        "operator_authorization": _record(root / AUTHORIZATION_FILENAME),
        "authority_turn_manifest": copy.deepcopy(turn["turn_manifest"]),
        "attempt": _record(paths["attempt"]),
        "initial_capacity": initial["records"],
        "thread_binding": _record(paths["thread"]),
        "preturn_capacity": preturn["records"],
        "sidecar": _record(paths["sidecar"]),
        "raw_output": _record(paths["raw_output"]),
        "validated_output": _record(paths["validated_output"]),
        "thread_id": telemetry["thread_id"],
        "semantic_turn_id": telemetry["turn_id"],
        "usage": telemetry["usage"],
        "wall_elapsed_seconds": telemetry["wall_elapsed_seconds"],
        "semantic_model_call_count": 1,
        "semantic_retry_count": 0,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    if paths["result"].exists():
        if _load_object(paths["result"], label="authority turn result") != result:
            raise CanonicalV31Epoch7AuthorityRuntimeError("authority turn result drifted")
    elif allow_finalize:
        _write_immutable_json(paths["result"], result)
    else:
        raise CanonicalV31Epoch7AuthorityWaiting("authority turn result is absent")
    return result


def _turn_state(paths: Mapping[str, Path]) -> str:
    root = paths["root"]
    if not root.exists():
        return "absent"
    if not root.is_dir() or root.is_symlink():
        return "partial"
    if paths["result"].is_file():
        return "complete"
    if paths["sidecar"].is_file() and paths["raw_output"].is_file():
        return "recoverable_completed"
    return "partial"


def _merge_episode_authority(
    *,
    turn_input: Mapping[str, Any],
    validated_output: Mapping[str, Any],
    reference_rows: Sequence[Mapping[str, Any]],
    reference_by_segment: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    episode_id = str(turn_input["episode_id"])
    repaired = {
        str(label["segment_id"]): copy.deepcopy(dict(label))
        for label in validated_output["repaired_reference_labels"]
    }
    expected_repaired_ids = list(turn_input["invalid_reference_segment_ids"])
    if list(repaired) != expected_repaired_ids:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "merged repaired reference membership drifted"
        )
    labels: list[dict[str, Any]] = []
    preserved_ids: list[str] = []
    repaired_ids: list[str] = []
    for row in reference_rows:
        if row.get("episode_id") != episode_id:
            continue
        segment_id = str(row["segment_id"])
        if row.get("canonical_v31_valid") is True:
            source = reference_by_segment.get(segment_id)
            label = source.get("golden_output") if isinstance(source, Mapping) else None
            if not isinstance(label, Mapping):
                raise CanonicalV31Epoch7AuthorityRuntimeError(
                    "preserved reference label is absent"
                )
            labels.append(copy.deepcopy(dict(label)))
            preserved_ids.append(segment_id)
        else:
            if segment_id not in repaired:
                raise CanonicalV31Epoch7AuthorityRuntimeError(
                    "repaired reference label is absent from merged authority"
                )
            labels.append(copy.deepcopy(repaired[segment_id]))
            repaired_ids.append(segment_id)
    if (
        repaired_ids != expected_repaired_ids
        or preserved_ids != list(turn_input["preserved_valid_reference_segment_ids"])
    ):
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "merged reference partition or order drifted"
        )
    context = copy.deepcopy(dict(turn_input["original_context"]))
    if "excluded_source_context" in context:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "authority merge would overwrite an existing context field"
        )
    context["excluded_source_context"] = copy.deepcopy(
        validated_output["excluded_source_context"]
    )
    return {
        "episode_id": episode_id,
        "episode_context": context,
        "episode_context_sha256": _sha256_bytes(
            _canonical_json(context).encode("ascii")
        ),
        "reference_labels": labels,
        "reference_labels_sha256": _sha256_bytes(
            _canonical_json(labels).encode("ascii")
        ),
        "repaired_reference_segment_ids": repaired_ids,
        "preserved_valid_reference_segment_ids": preserved_ids,
    }


def _build_merged_output(
    *,
    root: Path,
    contract: Mapping[str, Any],
    turn_results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    authority_plan_payload = _load_object(
        _verify_record(contract["authority_plan"], label="authority plan"),
        label="authority plan",
    )
    input_receipt = _load_object(
        _verify_record(
            authority_plan_payload["input_package_receipt"],
            label="input package receipt",
        ),
        label="input package receipt",
    )
    source_binding = _load_object(
        _verify_record(input_receipt["source_binding"], label="input source binding"),
        label="input source binding",
    )
    legacy_manifest_record = source_binding["legacy_manifest"]
    legacy_info = legacy_matrix.verify_development_manifest(
        Path(legacy_manifest_record["path"]),
        expected_sha256=legacy_manifest_record["sha256"],
    )
    results_by_turn = {str(row["turn_id"]): row for row in turn_results}
    episodes: list[dict[str, Any]] = []
    for turn in contract["turns"]:
        result = results_by_turn.get(str(turn["turn_id"]))
        if result is None:
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "authority merge is missing a completed turn"
            )
        manifest = _load_object(
            _verify_record(turn["turn_manifest"], label="authority turn manifest"),
            label="authority turn manifest",
        )
        turn_input = _load_object(
            _verify_record(manifest["input"], label="authority input"),
            label="authority input",
        )
        validated = _load_object(
            _verify_record(result["validated_output"], label="validated authority output"),
            label="validated authority output",
        )
        episode = _merge_episode_authority(
            turn_input=turn_input,
            validated_output=validated,
            reference_rows=source_binding["canonical_reference_validation"],
            reference_by_segment=legacy_info["reference_by_segment"],
        )
        repairs_by_id = {
            str(row["segment_id"]): row for row in turn_input["reference_repairs"]
        }
        source_text_by_id: dict[str, str] = {}
        for segment in turn_input["full_source_binding"]["segments"]:
            segment_id = str(segment["segment_id"])
            artifact_path = _verify_record(
                segment["artifact"], label=f"authority source segment {segment_id}"
            )
            source_text_by_id[segment_id] = artifact_path.read_text(encoding="utf-8")
        for label in episode["reference_labels"]:
            segment_id = str(label["segment_id"])
            if segment_id in repairs_by_id:
                segment_text = repairs_by_id[segment_id]["segment_text"]
            else:
                segment_text = source_text_by_id.get(segment_id)
            if not isinstance(segment_text, str):
                raise CanonicalV31Epoch7AuthorityRuntimeError(
                    "merged authority segment source is absent"
                )
            try:
                validate_label_output(
                    adapter.CANONICAL_LABEL_PACK, label, segment_text=segment_text
                )
            except ValidationError as exc:
                raise CanonicalV31Epoch7AuthorityRuntimeError(
                    "merged authority label failed canonical validation"
                ) from exc
        episodes.append(episode)
    return {
        "schema_version": MERGED_OUTPUT_VERSION,
        "state": "complete_development_authority_only",
        "authority_plan_receipt": copy.deepcopy(contract["authority_plan_receipt"]),
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "runtime_lock": _record(root / RUNTIME_LOCK_FILENAME),
        "operator_authorization": _record(root / AUTHORIZATION_FILENAME),
        "episode_count": len(episodes),
        "reference_label_count": sum(len(row["reference_labels"]) for row in episodes),
        "repaired_reference_count": sum(
            len(row["repaired_reference_segment_ids"]) for row in episodes
        ),
        "preserved_reference_count": sum(
            len(row["preserved_valid_reference_segment_ids"]) for row in episodes
        ),
        "episodes": episodes,
        "deterministic_semantic_mutation": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _validate_partial_sidecar(
    *,
    sidecar_path: Path,
    output_path: Path,
    manifest: Mapping[str, Any],
    thread_binding: Mapping[str, Any],
) -> dict[str, Any]:
    sidecar = _load_object(sidecar_path, label="partial authority sidecar")
    prompt_path = _verify_record(manifest["prompt"], label="authority prompt")
    base_path = _verify_record(manifest["base_instructions"], label="base instructions")
    schema = _load_object(
        _verify_record(manifest["output_schema"], label="output schema"),
        label="output schema",
    )
    fixed = {
        "schema_version": codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
        "client_version": codex_app_server.APP_SERVER_CLIENT_VERSION,
        "cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
        "protocol_schema_sha256": _record(codex_app_server.PROTOCOL_SCHEMA_PATH)["sha256"],
        "transport": "stdio",
        "synthetic_debug_errors": False,
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "thread_id": thread_binding["thread_id"],
        "model": authority_plan.MODEL,
        "effort": authority_plan.EFFORT,
        "batch_size": 1,
        "thread_mode": "new_thread",
        "prompt_sha256": _sha256_bytes(prompt_path.read_bytes()),
        "prompt_bytes": prompt_path.stat().st_size,
        "base_instructions_sha256": _sha256_bytes(base_path.read_bytes()),
        "base_instructions_bytes": base_path.stat().st_size,
        "instruction_sources_sha256": thread_binding["instruction_sources_sha256"],
        "instruction_sources_count": thread_binding["instruction_sources_count"],
        "output_schema_sha256": _sha256_bytes(_canonical_json(schema).encode("utf-8")),
        "output_schema_bytes": len(_canonical_json(schema).encode("utf-8")),
    }
    for key, expected in fixed.items():
        if sidecar.get(key) != expected:
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                f"partial authority sidecar drifted at {key}"
            )
    state = sidecar.get("state")
    if state not in {"started", "in_progress", "failed", "interrupted", "cancelled"}:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "partial authority sidecar state is invalid"
        )
    _parse_timestamp(sidecar.get("started_at"), label="partial sidecar started_at")
    if state not in {"started", "in_progress"}:
        _parse_timestamp(sidecar.get("finished_at"), label="partial sidecar finished_at")
        if sidecar.get("recovery_reran_model") is not False:
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "terminal partial sidecar recovery flag drifted"
            )
    elif sidecar.get("recovery_reran_model") not in {None, False}:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "preterminal partial sidecar recovery flag drifted"
        )
    turn_id = sidecar.get("turn_id")
    if state == "started":
        if turn_id is not None:
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "started sidecar unexpectedly has a turn ID"
            )
    elif not isinstance(turn_id, str) or not turn_id:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "dispatched partial sidecar lacks a turn ID"
        )
    if Path(str(sidecar.get("output_path") or "")).expanduser().resolve() != output_path.resolve():
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "partial authority sidecar output path drifted"
        )
    usage_value = sidecar.get("usage")
    if usage_value is None:
        if state in {"started", "in_progress"}:
            if sidecar.get("usage_complete") not in {None, False} or sidecar.get(
                "usage_status"
            ) not in {None, "unknown"}:
                raise CanonicalV31Epoch7AuthorityRuntimeError(
                    "preterminal authority usage state drifted"
                )
        elif sidecar.get("usage_complete") is not False or sidecar.get(
            "usage_status"
        ) != "unknown":
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "partial authority unknown-usage state drifted"
            )
        usage = None
    else:
        usage = _usage(usage_value, label="partial authority")
        total = _usage(sidecar.get("thread_total_usage"), label="partial thread total")
        if usage != total or sidecar.get("usage_complete") is not True or sidecar.get(
            "usage_status"
        ) != "measured":
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "partial authority measured usage drifted"
            )
    if output_path.exists():
        raw_text = output_path.read_text(encoding="utf-8")
        raw_message = raw_text[:-1] if raw_text.endswith("\n") else raw_text
        if sidecar.get("output_sha256") != _sha256_bytes(raw_message.encode("utf-8")):
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "partial authority output hash drifted"
            )
    return {
        "sidecar": sidecar,
        "usage": usage,
        "thread_id": sidecar["thread_id"],
        "turn_id": turn_id,
    }


def _validate_partial_turn(
    *,
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    turn: Mapping[str, Any],
    completed_turn_count: int,
) -> None:
    paths = _turn_paths(root, str(turn["turn_id"]))
    if not paths["root"].is_dir() or paths["root"].is_symlink():
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "partial authority turn root is invalid"
        )
    allowed = {
        paths["attempt"].name,
        paths["thread"].name,
        paths["sidecar"].name,
        paths["raw_output"].name,
        paths["validated_output"].name,
        "capacity",
    }
    if {path.name for path in paths["root"].iterdir()} - allowed:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "partial authority turn contains unexpected artifacts"
        )
    capacity_root = paths["root"] / "capacity"
    if capacity_root.exists():
        if (
            not capacity_root.is_dir()
            or capacity_root.is_symlink()
            or {path.name for path in capacity_root.iterdir()} - {"initial", "preturn"}
        ):
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "partial authority capacity artifact set drifted"
            )
    if not paths["attempt"].is_file():
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "partial authority turn lacks its immutable attempt"
        )
    expected_attempt = _attempt_payload(
        root=root, contract=contract, authorization=authorization, turn=turn
    )
    if _load_object(paths["attempt"], label="partial authority attempt") != expected_attempt:
        raise CanonicalV31Epoch7AuthorityRuntimeError("partial authority attempt drifted")

    initial_info = None
    if paths["initial_capacity"].exists():
        initial_info = _verify_capacity_bundle(
            paths["initial_capacity"],
            expected_request=_capacity_request(
                root=root,
                contract=contract,
                authorization=authorization,
                turn=turn,
                boundary="initial_before_thread",
                completed_turn_count=completed_turn_count,
            ),
            contract=contract,
            authorization=authorization,
            historical=True,
            require_available=False,
        )
    thread_binding = None
    if paths["thread"].exists():
        if initial_info is None or initial_info["measurement"]["capacity_available"] is not True:
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "partial semantic thread lacks cleared initial capacity"
            )
        thread_binding = _verify_thread_binding(
            _load_object(paths["thread"], label="partial thread binding"),
            contract=contract,
            turn=turn,
        )
    preturn_info = None
    if paths["preturn_capacity"].exists():
        if thread_binding is None:
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "preturn capacity exists without a bound semantic thread"
            )
        preturn_info = _verify_capacity_bundle(
            paths["preturn_capacity"],
            expected_request=_capacity_request(
                root=root,
                contract=contract,
                authorization=authorization,
                turn=turn,
                boundary="preturn_before_turn",
                completed_turn_count=completed_turn_count,
            ),
            contract=contract,
            authorization=authorization,
            historical=True,
            require_available=False,
        )
    if paths["sidecar"].exists():
        if (
            thread_binding is None
            or preturn_info is None
            or preturn_info["measurement"]["capacity_available"] is not True
        ):
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "partial semantic sidecar lacks cleared preturn lineage"
            )
        _validate_partial_sidecar(
            sidecar_path=paths["sidecar"],
            output_path=paths["raw_output"],
            manifest=_load_object(
                _verify_record(turn["turn_manifest"], label="authority turn manifest"),
                label="authority turn manifest",
            ),
            thread_binding=thread_binding,
        )
    elif paths["raw_output"].exists():
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "partial raw output exists without a sidecar"
        )
    if paths["validated_output"].exists():
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "partial turn claims a validated output without a result"
        )


def _terminal_accounting(root: Path, contract: Mapping[str, Any]) -> dict[str, Any]:
    usage_values: list[dict[str, int]] = []
    wall = 0.0
    call_count = 0
    thread_ids: list[str] = []
    semantic_turn_ids: list[str] = []
    for turn in contract["turns"]:
        paths = _turn_paths(root, str(turn["turn_id"]))
        if not paths["sidecar"].is_file():
            continue
        sidecar = _load_object(paths["sidecar"], label="partial authority sidecar")
        if isinstance(sidecar.get("thread_id"), str) and sidecar["thread_id"]:
            thread_ids.append(sidecar["thread_id"])
        if isinstance(sidecar.get("turn_id"), str) and sidecar["turn_id"]:
            semantic_turn_ids.append(sidecar["turn_id"])
            call_count += 1
        if isinstance(sidecar.get("usage"), Mapping):
            usage_values.append(_usage(sidecar["usage"], label="partial authority"))
        value = sidecar.get("wall_elapsed_seconds")
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
            wall += float(value)
    return {
        "semantic_model_call_count": call_count,
        "semantic_retry_count": 0,
        "measured_usage": _sum_usage(usage_values),
        "measured_usage_turn_count": len(usage_values),
        "unknown_usage_turn_count": max(0, call_count - len(usage_values)),
        "wall_elapsed_seconds": wall,
        "thread_ids": thread_ids,
        "semantic_turn_ids": semantic_turn_ids,
    }


def _write_terminal_receipt(
    *,
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    state: str,
    reason: str,
    turn_results: Sequence[Mapping[str, Any]],
    merged_output: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    accounting = _terminal_accounting(root, contract)
    usage = accounting["measured_usage"]
    failed_checks: list[str] = []
    if accounting["semantic_model_call_count"] > int(contract["exact_turn_count"]):
        failed_checks.append("semantic_model_call_cap")
    if usage["total_tokens"] > int(contract["phase_total_token_bound"]):
        failed_checks.append("phase_total_token_cap")
    receipt = {
        "schema_version": EXECUTION_RECEIPT_VERSION,
        "state": state,
        "terminal_reason": reason,
        "output_root": str(root),
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "runtime_lock": _record(root / RUNTIME_LOCK_FILENAME),
        "preauthorization_receipt": _record(root / PREAUTHORIZATION_FILENAME),
        "operator_authorization": _record(root / AUTHORIZATION_FILENAME),
        "authority_plan_receipt": copy.deepcopy(contract["authority_plan_receipt"]),
        "turn_results": [
            _record(_turn_paths(root, str(turn["turn_id"]))["result"])
            for turn in contract["turns"]
            if _turn_paths(root, str(turn["turn_id"]))["result"].is_file()
        ],
        "completed_validated_turn_count": len(turn_results),
        "exact_turn_count": int(contract["exact_turn_count"]),
        **accounting,
        "failed_checks": failed_checks,
        "merged_authority_output": (
            _record(root / MERGED_OUTPUT_FILENAME) if merged_output is not None else None
        ),
        "extraction_plan_rebuild_required": state == "passed",
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    raw = _pretty_json(receipt).encode("ascii")
    _write_immutable_bytes(root / EXECUTION_RECEIPT_FILENAME, raw)
    _write_immutable_bytes(root / TERMINAL_FILENAME, raw)
    return verify_execution_receipt(root)


def verify_execution_receipt(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    verify_preauthorization(root)
    output_root = root.expanduser().resolve()
    receipt_path = output_root / EXECUTION_RECEIPT_FILENAME
    terminal_path = output_root / TERMINAL_FILENAME
    mirror_repair_source: Path | None = None
    mirror_repair_target: Path | None = None
    if receipt_path.is_file() != terminal_path.is_file():
        mirror_repair_source = receipt_path if receipt_path.is_file() else terminal_path
        mirror_repair_target = terminal_path if receipt_path.is_file() else receipt_path
        receipt = _load_object(
            mirror_repair_source, label="single authority terminal mirror"
        )
        terminal = copy.deepcopy(receipt)
    else:
        receipt = _load_object(receipt_path, label="authority execution receipt")
        terminal = _load_object(terminal_path, label="authority terminal")
        if receipt != terminal or receipt_path.read_bytes() != terminal_path.read_bytes():
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "authority terminal mirrors drifted"
            )
    authorization = verify_authorization(output_root, require_current=False)
    contract = _load_object(output_root / CONTRACT_FILENAME, label="runtime contract")
    if (
        receipt.get("schema_version") != EXECUTION_RECEIPT_VERSION
        or receipt.get("output_root") != str(output_root)
        or receipt.get("runtime_contract") != _record(output_root / CONTRACT_FILENAME)
        or receipt.get("runtime_lock") != _record(output_root / RUNTIME_LOCK_FILENAME)
        or receipt.get("operator_authorization")
        != _record(output_root / AUTHORIZATION_FILENAME)
        or receipt.get("authority_plan_receipt") != contract["authority_plan_receipt"]
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("quality_authorized") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("production_mutated") is not False
        or receipt.get("state") not in {"passed", "waiting", "rejected"}
    ):
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "authority execution receipt contract drifted"
        )
    expected_receipt_keys = {
        "schema_version",
        "state",
        "terminal_reason",
        "output_root",
        "runtime_contract",
        "runtime_lock",
        "preauthorization_receipt",
        "operator_authorization",
        "authority_plan_receipt",
        "turn_results",
        "completed_validated_turn_count",
        "exact_turn_count",
        "semantic_model_call_count",
        "semantic_retry_count",
        "measured_usage",
        "measured_usage_turn_count",
        "unknown_usage_turn_count",
        "wall_elapsed_seconds",
        "thread_ids",
        "semantic_turn_ids",
        "failed_checks",
        "merged_authority_output",
        "extraction_plan_rebuild_required",
        "quality_authorized",
        "holdout_authorized",
        "production_mutated",
    }
    if (
        set(receipt) != expected_receipt_keys
        or receipt.get("preauthorization_receipt")
        != _record(output_root / PREAUTHORIZATION_FILENAME)
        or receipt.get("exact_turn_count") != int(contract["exact_turn_count"])
        or receipt.get("extraction_plan_rebuild_required")
        != (receipt["state"] == "passed")
    ):
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "authority execution receipt fields drifted"
        )
    completed: list[dict[str, Any]] = []
    for index, turn in enumerate(contract["turns"]):
        paths = _turn_paths(output_root, str(turn["turn_id"]))
        if not paths["result"].is_file():
            if paths["root"].exists():
                _validate_partial_turn(
                    root=output_root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                    completed_turn_count=index,
                )
            continue
        completed.append(
            _verify_complete_turn(
                root=output_root,
                contract=contract,
                authorization=authorization,
                turn=turn,
                completed_turn_count=index,
                allow_finalize=False,
            )
        )
    if receipt.get("completed_validated_turn_count") != len(completed):
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "authority receipt completed-turn count drifted"
        )
    expected_turn_result_records = [
        _record(_turn_paths(output_root, str(turn["turn_id"]))["result"])
        for turn in contract["turns"]
        if _turn_paths(output_root, str(turn["turn_id"]))["result"].is_file()
    ]
    if receipt.get("turn_results") != expected_turn_result_records:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "authority receipt turn-result lineage drifted"
        )
    accounting = _terminal_accounting(output_root, contract)
    for key, expected in accounting.items():
        if receipt.get(key) != expected:
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                f"authority receipt accounting drifted at {key}"
            )
    expected_failed_checks: list[str] = []
    if accounting["semantic_model_call_count"] > int(contract["exact_turn_count"]):
        expected_failed_checks.append("semantic_model_call_cap")
    if accounting["measured_usage"]["total_tokens"] > int(
        contract["phase_total_token_bound"]
    ):
        expected_failed_checks.append("phase_total_token_cap")
    if receipt.get("failed_checks") != expected_failed_checks:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "authority receipt failed-check accounting drifted"
        )
    if (
        len(accounting["thread_ids"]) != len(set(accounting["thread_ids"]))
        or len(accounting["semantic_turn_ids"])
        != len(set(accounting["semantic_turn_ids"]))
    ):
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "authority thread or semantic-turn identity was replayed"
        )
    if receipt["state"] == "passed":
        if (
            len(completed) != int(contract["exact_turn_count"])
            or accounting["semantic_model_call_count"] != int(contract["exact_turn_count"])
            or accounting["measured_usage_turn_count"] != int(contract["exact_turn_count"])
            or accounting["unknown_usage_turn_count"] != 0
            or expected_failed_checks
        ):
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "passed authority receipt lacks exact measured turn coverage"
            )
        merged_path = _verify_record(
            receipt.get("merged_authority_output"), label="merged authority output"
        )
        if merged_path != output_root / MERGED_OUTPUT_FILENAME:
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "merged authority output escaped its root"
            )
        rebuilt = _build_merged_output(
            root=output_root, contract=contract, turn_results=completed
        )
        if _load_object(merged_path, label="merged authority output") != rebuilt:
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "merged authority output drifted"
            )
    elif receipt.get("merged_authority_output") is not None:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "nonpassing authority receipt claims a merged output"
        )
    del authorization
    if mirror_repair_source is not None and mirror_repair_target is not None:
        _write_immutable_bytes(mirror_repair_target, mirror_repair_source.read_bytes())
        return verify_execution_receipt(output_root)
    return copy.deepcopy(receipt)


def _reject_external_auth_material() -> None:
    present = sorted(name for name in FORBIDDEN_AUTH_ENVIRONMENT if os.environ.get(name))
    if present:
        raise CanonicalV31Epoch7AuthorityRuntimeError(
            "managed ChatGPT execution rejects API-key/raw-session auth: "
            + ", ".join(present)
        )


async def _probe_and_publish(
    *,
    client: Any,
    bundle_root: Path,
    request: Mapping[str, Any],
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
) -> dict[str, Any]:
    response = await client._request("account/rateLimits/read", {})  # noqa: SLF001
    measured_at = _now()
    measurement = _capacity_measurement(
        response,
        request=request,
        contract=contract,
        authorization=authorization,
        measured_at=measured_at,
    )
    records = _publish_capacity_bundle(
        bundle_root, request=request, response=response, measurement=measurement
    )
    if measurement["capacity_available"] is not True:
        raise CanonicalV31Epoch7AuthorityWaiting(
            "managed capacity did not clear the semantic boundary"
        )
    return {"measurement": measurement, "records": records}


async def execute_authority_runtime(
    *,
    root: Path = DEFAULT_ROOT,
    operator_authorization_id: str,
) -> dict[str, Any]:
    _reject_external_auth_material()
    verify_preauthorization(root)
    output_root = root.expanduser().resolve()
    if (output_root / EXECUTION_RECEIPT_FILENAME).is_file() or (
        output_root / TERMINAL_FILENAME
    ).is_file():
        return verify_execution_receipt(output_root)
    authorization = verify_authorization(
        output_root,
        expected_authorization_id=operator_authorization_id,
        require_current=True,
    )
    contract = _load_object(output_root / CONTRACT_FILENAME, label="runtime contract")
    turns = contract["turns"]
    states = [_turn_state(_turn_paths(output_root, str(turn["turn_id"]))) for turn in turns]
    absent_seen = False
    for state in states:
        if state == "absent":
            absent_seen = True
        elif absent_seen:
            raise CanonicalV31Epoch7AuthorityRuntimeError(
                "authority turns are not a contiguous completed prefix"
            )
    completed: list[dict[str, Any]] = []
    for index, (turn, state) in enumerate(zip(turns, states)):
        if state in {"complete", "recoverable_completed"}:
            completed.append(
                _verify_complete_turn(
                    root=output_root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                    completed_turn_count=index,
                    allow_finalize=True,
                )
            )
        elif state == "partial":
            return _write_terminal_receipt(
                root=output_root,
                contract=contract,
                authorization=authorization,
                state="waiting",
                reason="epoch7_authority_partial_attempt_preserved_no_replay",
                turn_results=completed,
            )
    if len(completed) == len(turns):
        merged = _build_merged_output(
            root=output_root, contract=contract, turn_results=completed
        )
        _write_immutable_json(output_root / MERGED_OUTPUT_FILENAME, merged)
        return _write_terminal_receipt(
            root=output_root,
            contract=contract,
            authorization=authorization,
            state="passed",
            reason="epoch7_authority_outputs_validated_and_merged",
            turn_results=completed,
            merged_output=merged,
        )

    try:
        async with TRUSTED_CLIENT_FACTORY() as client:
            account = getattr(client, "account_summary", None)
            if (
                not isinstance(account, Mapping)
                or account.get("type") != "chatgpt"
                or account.get("plan_type") != "pro"
            ):
                raise CanonicalV31Epoch7AuthorityRuntimeError(
                    "authority runtime requires managed ChatGPT Pro auth"
                )
            for index in range(len(completed), len(turns)):
                verify_preauthorization(output_root)
                verify_authorization(
                    output_root,
                    expected_authorization_id=operator_authorization_id,
                    require_current=True,
                )
                turn = turns[index]
                paths = _turn_paths(output_root, str(turn["turn_id"]))
                paths["root"].mkdir(parents=True, exist_ok=False)
                attempt = _attempt_payload(
                    root=output_root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                )
                _write_immutable_json(paths["attempt"], attempt)
                initial_request = _capacity_request(
                    root=output_root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                    boundary="initial_before_thread",
                    completed_turn_count=index,
                )
                await _probe_and_publish(
                    client=client,
                    bundle_root=paths["initial_capacity"],
                    request=initial_request,
                    contract=contract,
                    authorization=authorization,
                )
                manifest = _load_object(
                    _verify_record(turn["turn_manifest"], label="authority turn manifest"),
                    label="authority turn manifest",
                )
                base_path = _verify_record(
                    manifest["base_instructions"], label="authority base instructions"
                )
                thread = await client.start_thread(
                    model=contract["model"],
                    base_instructions=base_path.read_text(encoding="ascii"),
                    cwd=PROJECT_ROOT,
                    ephemeral=True,
                )
                thread_binding = _thread_binding(thread, contract=contract, turn=turn)
                _write_immutable_json(paths["thread"], thread_binding)
                preturn_request = _capacity_request(
                    root=output_root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                    boundary="preturn_before_turn",
                    completed_turn_count=index,
                )
                await _probe_and_publish(
                    client=client,
                    bundle_root=paths["preturn_capacity"],
                    request=preturn_request,
                    contract=contract,
                    authorization=authorization,
                )
                prompt_path = _verify_record(manifest["prompt"], label="authority prompt")
                schema_path = _verify_record(
                    manifest["output_schema"], label="authority output schema"
                )
                result = await client.run_structured_turn(
                    thread=thread,
                    effort=contract["effort"],
                    prompt=prompt_path.read_text(encoding="ascii"),
                    output_schema=_load_object(schema_path, label="authority output schema"),
                    sidecar_path=paths["sidecar"],
                    output_path=paths["raw_output"],
                    batch_size=1,
                    thread_mode="new_thread",
                    timeout_seconds=float(contract["maximum_wall_seconds_per_turn"]),
                )
                if result.status_ok is not True:
                    raise CanonicalV31Epoch7AuthorityWaiting(
                        "authority semantic turn did not complete successfully"
                    )
                completed_result = _verify_complete_turn(
                    root=output_root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                    completed_turn_count=index,
                    allow_finalize=True,
                )
                raw_output = _load_object(
                    paths["raw_output"], label="authority raw output"
                )
                if (
                    result.thread_id != completed_result["thread_id"]
                    or result.turn_id != completed_result["semantic_turn_id"]
                    or not isinstance(result.output, Mapping)
                    or _canonical_json(result.output) != _canonical_json(raw_output)
                ):
                    raise CanonicalV31Epoch7AuthorityRuntimeError(
                        "in-memory authority result lineage drifted"
                    )
                completed.append(completed_result)
                usage = _sum_usage([row["usage"] for row in completed])
                if (
                    completed_result["usage"]["total_tokens"]
                    > int(contract["maximum_total_tokens_per_turn"])
                    or usage["total_tokens"] > int(contract["phase_total_token_bound"])
                ):
                    raise CanonicalV31Epoch7AuthorityRejected(
                        "authority measured token acceptance ceiling was exceeded"
                    )
    except (CanonicalV31Epoch7AuthorityRejected, codex_app_server.AppServerStructuredOutputError):
        return _write_terminal_receipt(
            root=output_root,
            contract=contract,
            authorization=authorization,
            state="rejected",
            reason="epoch7_authority_structural_or_cost_contract_rejected",
            turn_results=completed,
        )
    except (
        CanonicalV31Epoch7AuthorityWaiting,
        codex_app_server.AppServerError,
        asyncio.TimeoutError,
    ):
        return _write_terminal_receipt(
            root=output_root,
            contract=contract,
            authorization=authorization,
            state="waiting",
            reason="epoch7_authority_operational_or_capacity_waiting_no_replay",
            turn_results=completed,
        )

    merged = _build_merged_output(
        root=output_root, contract=contract, turn_results=completed
    )
    _write_immutable_json(output_root / MERGED_OUTPUT_FILENAME, merged)
    return _write_terminal_receipt(
        root=output_root,
        contract=contract,
        authorization=authorization,
        state="passed",
        reason="epoch7_authority_outputs_validated_and_merged",
        turn_results=completed,
        merged_output=merged,
    )


def status_runtime(root: Path = DEFAULT_ROOT, *, now: datetime | None = None) -> dict[str, Any]:
    verify_preauthorization(root)
    output_root = root.expanduser().resolve()
    if (output_root / EXECUTION_RECEIPT_FILENAME).is_file() or (
        output_root / TERMINAL_FILENAME
    ).is_file():
        receipt = verify_execution_receipt(output_root)
        return {
            "schema_version": "pif_canonical_v31_epoch7_authority_runtime_status_v1",
            "state": receipt["state"],
            "reason": receipt["terminal_reason"],
            "semantic_model_call_count": receipt["semantic_model_call_count"],
            "semantic_retry_count": 0,
        }
    authorization_path = output_root / AUTHORIZATION_FILENAME
    if not authorization_path.is_file():
        return {
            "schema_version": "pif_canonical_v31_epoch7_authority_runtime_status_v1",
            "state": "waiting",
            "reason": "explicit_kolby_authorization_required",
            "semantic_model_call_count": 0,
            "semantic_retry_count": 0,
        }
    try:
        verify_authorization(output_root, now=now, require_current=True)
    except CanonicalV31Epoch7AuthorityWaiting:
        return {
            "schema_version": "pif_canonical_v31_epoch7_authority_runtime_status_v1",
            "state": "waiting",
            "reason": "operator_authorization_not_current",
            "semantic_model_call_count": _terminal_accounting(
                output_root,
                _load_object(output_root / CONTRACT_FILENAME, label="runtime contract"),
            )["semantic_model_call_count"],
            "semantic_retry_count": 0,
        }
    contract = _load_object(output_root / CONTRACT_FILENAME, label="runtime contract")
    states = [
        _turn_state(_turn_paths(output_root, str(turn["turn_id"])))
        for turn in contract["turns"]
    ]
    reason = (
        "partial_attempt_requires_verified_waiting_terminal"
        if "partial" in states
        else "ready_for_exact_authorized_execute"
    )
    return {
        "schema_version": "pif_canonical_v31_epoch7_authority_runtime_status_v1",
        "state": "ready" if "partial" not in states else "waiting",
        "reason": reason,
        "turn_states": states,
        "semantic_model_call_count": _terminal_accounting(output_root, contract)[
            "semantic_model_call_count"
        ],
        "semantic_retry_count": 0,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=(
            "python3 -m research_factory."
            "app_server_canonical_v31_epoch7_input_authority_runtime"
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    freeze.add_argument("--authority-root", type=Path, default=DEFAULT_AUTHORITY_ROOT)
    status = commands.add_parser("status")
    status.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    authorize = commands.add_parser("authorize")
    authorize.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    authorize.add_argument("--authorized-by", required=True, choices=("kolby",))
    authorize.add_argument("--operator-authorization-id", required=True)
    authorize.add_argument("--issued-at", required=True)
    authorize.add_argument("--expires-at", required=True)
    execute = commands.add_parser("execute")
    execute.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    execute.add_argument("--operator-authorization-id", required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            result = freeze_preauthorization(
                root=args.root, authority_root=args.authority_root
            )
        elif args.command == "status":
            result = status_runtime(args.root)
        elif args.command == "authorize":
            result = authorize_runtime(
                root=args.root,
                operator_authorization_id=args.operator_authorization_id,
                issued_at=_parse_timestamp(args.issued_at, label="issued_at"),
                expires_at=_parse_timestamp(args.expires_at, label="expires_at"),
            )
        elif args.command == "execute":
            result = asyncio.run(
                execute_authority_runtime(
                    root=args.root,
                    operator_authorization_id=args.operator_authorization_id,
                )
            )
        else:
            result = verify_execution_receipt(args.root)
    except CanonicalV31Epoch7AuthorityRuntimeError as exc:
        print(_pretty_json({"ok": False, "error": str(exc)}), end="", file=sys.stderr)
        return 2
    print(_pretty_json({"ok": True, "result": result}), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
