from __future__ import annotations

"""Recover the presemantic v224 keyword-binding failure without replaying it."""

import argparse
import asyncio
import inspect
import json
import math
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_selection_v220_integrated_base_design as v220
from . import app_server_judge_v5_selection_v224_frozen_alignment_audit as v224
from . import codex_app_server
from .app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
)
from .app_server_judge_v5_calibration_v25_diagnostic import (
    PINNED_CODEX_0_144_1,
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .util import now_iso, sha256_text


V225_AUDIT_VERSION = "pif_app_server_judge_v5_4_selection_v225_audit_v1"
V225_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v225_spec_v1"
V225_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V225_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V225_RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_4_selection_v225_runtime_lock_v1"
)
V225_LAUNCH_VERSION = "pif_app_server_judge_v5_4_selection_v225_launch_v1"
V225_WINNER_VERSION = "pif_app_server_judge_v5_4_selection_v225_winner_v1"
V225_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v225_failure_v1"
V225_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v225_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v225_alignment_transport_recovery"
TURN_NAMES = v224.TURN_NAMES
MODEL = v224.MODEL
EFFORT = v224.EFFORT
MAXIMUM_TOTAL_TOKENS_PER_TURN = v224.MAXIMUM_TOTAL_TOKENS_PER_TURN
MINIMUM_REMAINING_RESERVE_PERCENT = v224.MINIMUM_REMAINING_RESERVE_PERCENT
TIMEOUT_SECONDS = v224.TIMEOUT_SECONDS
EXPECTED_V224_ERROR_SHA256 = (
    "9cf912677ea0a74d050ff1e254be7b4bfe7b7b3f6893c5389191e9ed1bad44f9"
)
EXPECTED_V224_ERROR_BYTES = 106
DEFAULT_OUTPUT_ROOT = (
    v220.PIPELINE_ROOT
    / "development-selection-v5_4-v225-alignment-transport-recovery"
).resolve()


class JudgeV5SelectionV225Error(RuntimeError):
    """The v224 presemantic recovery cannot be preserved or executed safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(
    left: Mapping[str, int], right: Mapping[str, int]
) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _v224_paths() -> dict[str, Path]:
    root = v224.DEFAULT_OUTPUT_ROOT
    base_stem = TURN_NAMES[0].replace("_", "-")
    canary_stem = TURN_NAMES[1].replace("_", "-")
    return {
        "design": root / "alignment-design.json",
        "instructions": root / "alignment-instructions.private.md",
        "pool": root / "alignment-pool.private.json",
        "spec": root / "attempt-spec.json",
        "capacity_audit": root / "capacity-policy-audit.json",
        "capacity_policy": root / "capacity-policy.json",
        "failure": root / "failure.json",
        "launch": root / "launch-receipt.json",
        "mapping": root / "origin-map.private.json",
        "runtime_lock": root / "runtime-lock.json",
        "receipts": root / "support-receipts-normalized.private.json",
        "terminal": root / "terminal.json",
        "capacity": root / "turns" / base_stem / "capacity.json",
        "base_input": root / f"{base_stem}-input.private.json",
        "base_prompt": root / f"{base_stem}-prompt.private.md",
        "base_schema": root / f"{base_stem}-schema.json",
        "canary_input": root / f"{canary_stem}-input.private.json",
        "canary_prompt": root / f"{canary_stem}-prompt.private.md",
        "canary_schema": root / f"{canary_stem}-schema.json",
    }


def _reproduce_v224_type_error() -> dict[str, Any]:
    signature = inspect.signature(
        codex_app_server.CodexAppServerClient.run_ephemeral_structured_turn
    )
    if "output_validator" in signature.parameters:
        raise JudgeV5SelectionV225Error(
            "v224 keyword failure no longer matches the pinned client signature"
        )
    try:
        codex_app_server.CodexAppServerClient.run_ephemeral_structured_turn(
            object(), output_validator=lambda value: []
        )
    except TypeError as exc:
        message = str(exc).encode("utf-8")
    else:  # pragma: no cover - guarded by the signature assertion
        raise JudgeV5SelectionV225Error("v224 keyword failure did not reproduce")
    return {
        "error_class": "TypeError",
        "error_message_sha256": sha256_text(message.decode("utf-8")),
        "error_message_bytes": len(message),
    }


def _validate_v224_presemantic_failure() -> dict[str, Any]:
    root = v224.DEFAULT_OUTPUT_ROOT
    paths = _v224_paths()
    actual = {path.resolve() for path in root.rglob("*") if path.is_file()}
    expected = {path.resolve() for path in paths.values()}
    if actual != expected:
        raise JudgeV5SelectionV225Error("v224 immutable file set drifted")
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV225Error("v224 immutable artifact drifted")
    v224.verify_runtime_lock(paths["runtime_lock"])
    values = {
        name: _load_json(path, f"v224 {name}")
        for name, path in paths.items()
        if path.suffix == ".json"
    }
    terminal = values["terminal"]
    failure = values["failure"]
    checkpoint = values["capacity"]
    reproduced = _reproduce_v224_type_error()
    base_turn_root = paths["capacity"].parent
    canary_turn_root = root / "turns" / TURN_NAMES[1].replace("_", "-")
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason")
        != "infrastructure_or_judge_attempt_failed"
        or terminal.get("terminal_classification")
        != "inactive_incomplete_recovery_required"
        or terminal.get("attempted_turn_count") != 1
        or terminal.get("measured_turn_count") != 0
        or terminal.get("unknown_usage_turn_count") != 1
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("error_class") != "TypeError"
        or failure.get("error_message_sha256") != EXPECTED_V224_ERROR_SHA256
        or failure.get("error_message_bytes") != EXPECTED_V224_ERROR_BYTES
        or reproduced.get("error_message_sha256") != EXPECTED_V224_ERROR_SHA256
        or reproduced.get("error_message_bytes") != EXPECTED_V224_ERROR_BYTES
        or checkpoint.get("cleared_for_semantic_turn") is not True
        or checkpoint.get("managed_chatgpt_auth_verified") is not True
        or checkpoint.get("rate_limit_reached_type") is not None
        or checkpoint.get("thread_started") is not False
        or checkpoint.get("turn_started") is not False
        or checkpoint.get("sidecar_started") is not False
        or (base_turn_root / "sidecar.json").exists()
        or (base_turn_root / "output.private.json").exists()
        or (base_turn_root / "alignment-normalized.private.json").exists()
        or canary_turn_root.exists()
    ):
        raise JudgeV5SelectionV225Error(
            "v224 presemantic failure contract drifted"
        )
    v223_source = v224._validate_v223_success()
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "failure": failure,
        "checkpoint": checkpoint,
        "reproduced": reproduced,
        "v223": v223_source,
        "bundle": v224._load_frozen_bundle(root),
    }


def _build_capacity_policy(
    root: Path, predecessor: Mapping[str, Any]
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(
        bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": V225_CAPACITY_AUDIT_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v224_terminal": predecessor["records"]["terminal"],
        "v224_capacity_checkpoint": predecessor["records"]["capacity"],
        "measured_basis": {
            "semantic_usage_predecessor": "v223",
            "predecessor_known_total_tokens_lower_bound": predecessor[
                "v223"
            ]["terminal"]["cumulative_known_usage_lower_bound"]["total_tokens"],
            "predecessor_unknown_usage_turn_count": predecessor["v223"][
                "terminal"
            ]["cumulative_unknown_usage_turn_count"],
            "v224_semantic_usage_resolution": "known_zero_presemantic",
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": (
                MINIMUM_REMAINING_RESERVE_PERCENT
            ),
        },
    }
    _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V225_CAPACITY_POLICY_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(TURN_NAMES),
        "minimum_remaining_reserve_percent": (
            MINIMUM_REMAINING_RESERVE_PERCENT
        ),
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_immutable(policy_path, policy)
    reserve.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    return v224._turn_paths(root, turn_name)


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            set(v224._expected_runtime_paths())
            | {
                Path(__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(v224.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
            },
            key=str,
        )
    )


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    predecessor = _validate_v224_presemantic_failure()
    lock = _load_json(path, "v225 runtime lock")
    expected_runtime = {str(item) for item in _expected_runtime_paths()}
    actual_runtime = {
        str(Path(record["path"]).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping) and isinstance(record.get("path"), str)
    }
    if (
        lock.get("schema_version") != V225_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or actual_runtime != expected_runtime
        or lock.get("semantic_turn_count") != len(TURN_NAMES)
        or lock.get("retry_count_per_turn") != 0
        or lock.get("v224_semantic_usage_resolution")
        != "known_zero_presemantic"
        or lock.get("v224_attempt")
        != list(predecessor["records"].values())
        or lock.get("extraction_replay_allowed") is not False
        or lock.get("holdout_authorized_before_score") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5SelectionV225Error("v225 runtime lock drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("v224_attempt") or []),
        lock.get("spec"),
        lock.get("forensic_audit"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("frozen_requests") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV225Error("v225 runtime lock record drifted")
    reserve.load_reserve_capacity_policy(
        Path(lock["capacity_policy"]["path"])
    )
    return lock


def freeze_v225(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {
            "root": root,
            "terminal": _load_json(terminal_path, "v225 terminal"),
        }
    if any(root.iterdir()):
        spec_path = root / "attempt-spec.json"
        lock_path = root / "runtime-lock.json"
        if not spec_path.exists() or not lock_path.exists():
            raise JudgeV5SelectionV225Error(
                "v225 root is nonempty without a complete presemantic freeze"
            )
        if (root / "launch-receipt.json").exists():
            raise JudgeV5SelectionV225Error(
                "v225 launch exists without terminal; replay is prohibited"
            )
        verify_runtime_lock(lock_path)
        predecessor = _validate_v224_presemantic_failure()
        return {
            "root": root,
            "predecessor": predecessor,
            "spec": _load_json(spec_path, "v225 spec"),
            "spec_path": spec_path,
            "runtime_lock": lock_path,
            "capacity_policy": root / "capacity-policy.json",
            "bundle": predecessor["bundle"],
        }
    predecessor = _validate_v224_presemantic_failure()
    forensic = {
        "schema_version": V225_AUDIT_VERSION,
        "created_at": now_iso(),
        "v224_terminal": predecessor["records"]["terminal"],
        "v224_failure": predecessor["records"]["failure"],
        "v224_capacity_checkpoint": predecessor["records"]["capacity"],
        "reproduced_error": predecessor["reproduced"],
        "semantic_thread_started": False,
        "semantic_turn_started": False,
        "semantic_sidecar_started": False,
        "semantic_output_created": False,
        "v224_semantic_usage_resolution": "known_zero_presemantic",
        "v224_terminal_rewritten": False,
        "v224_attempt_replayed": False,
        "recovery_change": "remove_unsupported_output_validator_keyword_only",
        "prompt_schema_or_instructions_changed": False,
        "production_mutated": False,
        "privacy": "sanitized_error_hash_size_lifecycle_and_records_only",
    }
    forensic_path = root / "v224-presemantic-failure-audit.json"
    _write_stable_time(forensic_path, forensic, "created_at")
    capacity_paths = _build_capacity_policy(root, predecessor)
    request_records = []
    for turn in predecessor["bundle"]["turns"]:
        request_records.extend(
            _record(turn["paths"][name]) for name in ("input", "prompt", "schema")
        )
    spec = {
        "schema_version": V225_SPEC_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "frozen_before_semantic_attempt",
        "recovery_of": predecessor["records"]["terminal"],
        "v224_semantic_usage_resolution": "known_zero_presemantic",
        "turn_plan": list(TURN_NAMES),
        "declared_turn_count": len(TURN_NAMES),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": TIMEOUT_SECONDS,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "retry_count_per_turn": 0,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "api_key_billing_allowed": False,
        "raw_session_token_access_allowed": False,
        "codex_exec_semantic_calls_allowed": False,
        "semantic_regex_or_keyword_pruning_allowed": False,
        "extraction_replay_allowed": False,
        "output_validation_timing": "after_structured_turn_return_before_adoption",
        "prompt_schema_or_instructions_changed": False,
        "repair_or_adjudication_turn_authorized": False,
        "holdout_authorized_before_score": False,
        "production_mutation_allowed": False,
        "forensic_audit": _record(forensic_path),
        "v224_attempt": predecessor["records"],
        "frozen_requests": request_records,
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    lock = {
        "schema_version": V225_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _expected_runtime_paths()],
        "v224_attempt": list(predecessor["records"].values()),
        "v224_semantic_usage_resolution": "known_zero_presemantic",
        "spec": _record(spec_path),
        "forensic_audit": _record(forensic_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "frozen_requests": request_records,
        "semantic_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "extraction_replay_allowed": False,
        "holdout_authorized_before_score": False,
        "production_mutation_allowed": False,
    }
    lock_path = root / "runtime-lock.json"
    _write_stable_time(lock_path, lock, "created_at")
    verify_runtime_lock(lock_path)
    return {
        "root": root,
        "predecessor": predecessor,
        "spec": spec,
        "spec_path": spec_path,
        "runtime_lock": lock_path,
        "capacity_policy": capacity_paths["policy"],
        "bundle": predecessor["bundle"],
    }


def _sidecar_accounting(root: Path) -> dict[str, Any]:
    usage = {field: 0 for field in USAGE_FIELDS}
    attempted = measured = unknown = 0
    sidecars = []
    for turn_name in TURN_NAMES:
        paths = _turn_paths(root, turn_name)
        if not paths["capacity"].exists() and not paths["sidecar"].exists():
            continue
        attempted += 1
        if not paths["sidecar"].exists():
            unknown += 1
            continue
        sidecar = _load_json(paths["sidecar"], f"v225 {turn_name} sidecar")
        sidecars.append(_record(paths["sidecar"]))
        try:
            turn_usage = _validate_usage(sidecar)
            if (
                sidecar.get("usage_status") != "measured"
                or sidecar.get("usage_complete") is not True
            ):
                raise JudgeV5SelectionV225Error("v225 usage is incomplete")
        except Exception:
            unknown += 1
            continue
        measured += 1
        usage = _sum_usage(usage, turn_usage)
    return {
        "attempted_turn_count": attempted,
        "measured_turn_count": measured,
        "unknown_usage_turn_count": unknown,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "usage": usage,
        "sidecars": sidecars,
    }


def _validate_measured_turn(root: Path, turn_name: str) -> dict[str, int]:
    paths = _turn_paths(root, turn_name)
    capacity_value = _load_json(paths["capacity"], f"v225 {turn_name} capacity")
    sidecar = _load_json(paths["sidecar"], f"v225 {turn_name} sidecar")
    usage = _validate_usage(sidecar)
    if (
        capacity_value.get("cleared_for_semantic_turn") is not True
        or capacity_value.get("managed_chatgpt_auth_verified") is not True
        or capacity_value.get("rate_limit_reached_type") is not None
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None
        or not paths["output"].is_file()
    ):
        raise JudgeV5SelectionV225Error(
            f"v225 measured turn contract drifted: {turn_name}"
        )
    return usage


def _write_failure(
    *, root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    accounting = _sidecar_accounting(root)
    message = str(exc).encode("utf-8", errors="replace")
    failure = {
        "schema_version": V225_FAILURE_VERSION,
        "failed_at": now_iso(),
        "error_class": type(exc).__name__,
        "error_message_sha256": sha256_text(
            message.decode("utf-8", errors="replace")
        ),
        "error_message_bytes": len(message),
        **accounting,
        "semantic_retry_allowed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "privacy": "error_class_hash_length_and_aggregate_usage_only",
    }
    failure_path = root / "failure.json"
    _write_stable_time(failure_path, failure, "failed_at")
    source_terminal = frozen["predecessor"]["v223"]["terminal"]
    known = _sum_usage(
        source_terminal["cumulative_known_usage_lower_bound"],
        accounting["usage"],
    )
    unknown = int(source_terminal["cumulative_unknown_usage_turn_count"]) + int(
        accounting["unknown_usage_turn_count"]
    )
    upper = int(
        source_terminal["cumulative_conservative_unknown_usage_upper_bound"]
    ) + int(accounting["unknown_usage_turn_count"]) * int(
        MAXIMUM_TOTAL_TOKENS_PER_TURN
    )
    terminal = {
        "schema_version": V225_TERMINAL_VERSION,
        "state": "failed",
        "terminal_at": now_iso(),
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "v224_semantic_usage_resolution": "known_zero_presemantic",
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "semantic_retry_allowed": False,
        "cumulative_known_usage_lower_bound": known,
        "cumulative_unknown_usage_turn_count": unknown,
        "cumulative_conservative_unknown_usage_upper_bound": upper,
        "failure": _record(failure_path),
        "spec": _record(frozen["spec_path"]),
        "runtime_lock": _record(frozen["runtime_lock"]),
        "launch_receipt": _record(root / "launch-receipt.json"),
        **{key: value for key, value in accounting.items() if key != "sidecars"},
        "sidecars": accounting["sidecars"],
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[
            str(PINNED_CODEX_0_144_1),
            "app-server",
            "--stdio",
            "--strict-config",
        ]
    )


def _default_client_factory(
    policy_path: Path,
) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path,
        inner_factory=_inner_factory,
    )


async def run_v225(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _default_client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v225 terminal")
    frozen = freeze_v225(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        raise JudgeV5SelectionV225Error(
            "v225 launch receipt exists; semantic replay is prohibited"
        )
    launch = {
        "schema_version": V225_LAUNCH_VERSION,
        "phase_id": PHASE_ID,
        "launched_at": now_iso(),
        "declared_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "runtime_lock": _record(frozen["runtime_lock"]),
        "capacity_policy": _record(frozen["capacity_policy"]),
        "managed_chatgpt_auth_only": True,
        "semantic_thread_or_turn_started_before_receipt": False,
        "v224_attempt_replayed": False,
        "extraction_replay_allowed": False,
        "holdout_authorized_before_score": False,
        "production_mutation_allowed": False,
    }
    _write_immutable(launch_path, launch)
    started = time.monotonic()
    try:
        normalized = []
        async with client_factory(frozen["capacity_policy"]) as client:
            for turn in frozen["bundle"]["turns"]:
                turn_name = turn["turn_name"]
                paths = _turn_paths(root, turn_name)
                paths["root"].mkdir(parents=True, exist_ok=True)
                result = await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
                    base_instructions=v130.alignment_instructions_v130(),
                    prompt=turn["prompt"],
                    output_schema=turn["schema"],
                    cwd=v220.PROJECT_ROOT,
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    batch_size=v224.EXPECTED_WITNESS_COUNT,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=paths["capacity"],
                )
                if result.status_ok is not True or not isinstance(
                    result.output, Mapping
                ):
                    raise JudgeV5SelectionV225Error(
                        f"v225 turn did not complete: {turn_name}"
                    )
                errors = v224.validate_alignment_output(
                    result.output, turn["value"]
                )
                if errors:
                    raise JudgeV5SelectionV225Error(
                        f"v225 structured alignment validation failed: {len(errors)}"
                    )
                _validate_measured_turn(root, turn_name)
                projected = v224.judge.normalize_neutral_alignment_output(
                    result.output, turn["value"]
                )
                _write_immutable(paths["normalized"], projected)
                normalized.append(projected)
        accounting = _sidecar_accounting(root)
        if (
            accounting["attempted_turn_count"] != len(TURN_NAMES)
            or accounting["measured_turn_count"] != len(TURN_NAMES)
            or accounting["unknown_usage_turn_count"] != 0
            or accounting["accounting_complete"] is not True
        ):
            raise JudgeV5SelectionV225Error("v225 accounting is incomplete")
        observed_token_ratio = float(
            frozen["predecessor"]["v223"]["v222"]["gate"][
                "observed_production_amortized_total_token_ratio"
            ]
        )
        score = v224.score_alignment(
            base=normalized[0],
            canary=normalized[1],
            mapping=frozen["bundle"]["mapping"],
            observed_token_ratio=observed_token_ratio,
        )
        score_path = root / "alignment-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        winner_path = root / "development-winner.json"
        winner_record = None
        if passed:
            winner = {
                "schema_version": V225_WINNER_VERSION,
                "frozen_at": now_iso(),
                "system_id": "fresh_integrated_base_v220_v225_frozen",
                "configuration": {
                    "model": v220.MODEL,
                    "reasoning_effort": v220.EFFORT,
                    "max_events_per_segment": v220.MAX_EVENTS_PER_SEGMENT,
                    "design": _record(
                        v220.DEFAULT_OUTPUT_ROOT / "integrated-base-design.json"
                    ),
                    "attempt_spec": _record(
                        v220.DEFAULT_OUTPUT_ROOT / "attempt-spec.json"
                    ),
                    "instructions": _record(
                        v220.DEFAULT_OUTPUT_ROOT
                        / "integrated-core-instructions.private.md"
                    ),
                },
                "development_evidence": {
                    "v223": frozen["predecessor"]["v223"]["records"],
                    "v224_presemantic_failure": frozen["predecessor"][
                        "records"
                    ],
                    "v225_score": _record(score_path),
                },
                "development_quality_passed": True,
                "production_amortized_total_token_ratio": observed_token_ratio,
                "production_amortized_token_target_passed": True,
                "untouched_holdout_authorized": True,
                "overall_evaluation_complete": False,
                "production_mutated": False,
                "privacy": (
                    "hashes_counts_metrics_and_configuration_"
                    "no_source_or_event_text"
                ),
            }
            _write_stable_time(winner_path, winner, "frozen_at")
            winner_record = _record(winner_path)
        source_terminal = frozen["predecessor"]["v223"]["terminal"]
        cumulative = _sum_usage(
            source_terminal["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        terminal = {
            "schema_version": V225_TERMINAL_VERSION,
            "state": "completed" if passed else "development_strategy_not_accepted",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v225_alignment_passed_development_winner_frozen_holdout_authorized"
                if passed
                else "v225_alignment_quality_or_permutation_gate_not_passed"
            ),
            "terminal_classification": "inactive_incomplete_recovery_required",
            "overall_evaluation_complete": False,
            "v224_semantic_usage_resolution": "known_zero_presemantic",
            "development_quality_passed": passed,
            "development_winner_frozen": passed,
            "holdout_authorized": passed,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "semantic_retry_allowed": False,
            "usage_status": accounting["usage_status"],
            "accounting_complete": accounting["accounting_complete"],
            "usage": accounting["usage"],
            "attempted_turn_count": accounting["attempted_turn_count"],
            "measured_turn_count": accounting["measured_turn_count"],
            "unknown_usage_turn_count": accounting["unknown_usage_turn_count"],
            "cumulative_known_usage_lower_bound": cumulative,
            "cumulative_unknown_usage_turn_count": source_terminal[
                "cumulative_unknown_usage_turn_count"
            ],
            "cumulative_conservative_unknown_usage_upper_bound": source_terminal[
                "cumulative_conservative_unknown_usage_upper_bound"
            ],
            "wall_seconds": round(time.monotonic() - started, 6),
            "score": _record(score_path),
            "development_winner": winner_record,
            "sidecars": accounting["sidecars"],
            "spec": _record(frozen["spec_path"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "launch_receipt": _record(launch_path),
            "required_next_artifact_path": str(
                root.parent
                / (
                    "untouched-holdout-v5_4-v226-frozen-winner-execution"
                    if passed
                    else "development-selection-v5_4-v226-alignment-nonacceptance"
                )
                / "terminal.json"
            ),
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except BaseException as exc:
        if terminal_path.exists():
            return _load_json(terminal_path, "v225 terminal")
        return _write_failure(root=root, frozen=frozen, exc=exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run v225 alignment transport recovery"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v225(
            output_dir=Path(args.output_dir),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "development_winner_frozen": terminal[
                    "development_winner_frozen"
                ],
                "holdout_authorized": terminal["holdout_authorized"],
                "usage_status": terminal["usage_status"],
                "production_mutated": terminal["production_mutated"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
