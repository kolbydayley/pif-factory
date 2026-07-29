from __future__ import annotations

"""One additive schema-only recovery for the typed event-set canary."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import app_server_configured_experiment as configured
from . import app_server_typed_event_set_experiment as typed
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .app_server_runtime_verifier import ContentHashCache, normalize_record
from .util import now_iso


CONFIG_VERSION = "pif_typed_event_set_schema_recovery_config_v1"
LOCK_VERSION = "pif_typed_event_set_schema_recovery_runtime_lock_v1"
TERMINAL_VERSION = "pif_typed_event_set_schema_recovery_terminal_v1"
ARCHITECTURE_ID = typed.ARCHITECTURE_ID
TURN_NAME = typed.TURN_NAME
MODEL = typed.MODEL
EFFORT = typed.EFFORT
MAX_TOTAL_TOKENS = typed.MAX_TOTAL_TOKENS
PROJECT_ROOT = typed.PROJECT_ROOT
PIPELINE_ROOT = typed.PIPELINE_ROOT
PINNED_CODEX = typed.PINNED_CODEX
PREDECESSOR_ROOT = typed.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-canary-schema-compiled-typed-event-set-luna-high-schema-compat-recovery-v2-2026-07-18"
).resolve()
UNSUPPORTED_PROVIDER_KEYWORDS = frozenset({"minLength", "uniqueItems"})
OFFICIAL_SCHEMA_GUIDE = "https://developers.openai.com/api/docs/guides/structured-outputs"
LINEAGE_KEYS = (
    "predecessor_terminal",
    "predecessor_runtime_lock",
    "predecessor_runtime_closure",
    "predecessor_capacity",
    "predecessor_sidecar",
    "predecessor_config",
    "predecessor_schema",
    "architecture_decision",
    "source_input",
    "source_prompt",
    "source_base",
    "projection_schema",
    "schema_compatibility_diagnostic",
)


class TypedEventSetSchemaRecoveryError(RuntimeError):
    pass


class TypedEventSetSchemaRecoveryStop(TypedEventSetSchemaRecoveryError):
    pass


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TypedEventSetSchemaRecoveryError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    configured._write_immutable(path, value)  # noqa: SLF001


def _write_private_text(path: Path, value: str) -> None:
    configured._write_private_text(path, value)  # noqa: SLF001


def _record(path: Path, *, cache: ContentHashCache | None = None) -> dict[str, Any]:
    return (cache or ContentHashCache()).record(path)


def _verify_record(record: Mapping[str, Any], *, cache: ContentHashCache) -> None:
    if not cache.verify_record(record):
        raise TypedEventSetSchemaRecoveryError("frozen recovery artifact drifted")


def _turn_paths(root: Path) -> dict[str, Path]:
    return typed._turn_paths(root)  # noqa: SLF001


def _predecessor_paths() -> dict[str, Path]:
    turn = _turn_paths(PREDECESSOR_ROOT)
    return {
        "predecessor_terminal": PREDECESSOR_ROOT / "terminal.json",
        "predecessor_runtime_lock": PREDECESSOR_ROOT / "runtime-lock.json",
        "predecessor_runtime_closure": PREDECESSOR_ROOT / "runtime-lock-closure.json",
        "predecessor_capacity": turn["capacity"],
        "predecessor_sidecar": turn["sidecar"],
        "predecessor_config": PREDECESSOR_ROOT / "experiment-config.json",
        "predecessor_schema": turn["schema"],
        "architecture_decision": PREDECESSOR_ROOT / "architecture-decision.json",
        "source_input": turn["input"],
        "source_prompt": turn["prompt"],
        "source_base": turn["base"],
        "projection_schema": turn["projection_schema"],
    }


def _runtime_files() -> tuple[Path, ...]:
    return tuple(sorted({Path(__file__).resolve(), *typed._runtime_files()}, key=str))  # noqa: SLF001


def _count_key(value: Any, key: str) -> int:
    if isinstance(value, dict):
        return int(key in value) + sum(_count_key(item, key) for item in value.values())
    if isinstance(value, list):
        return sum(_count_key(item, key) for item in value)
    return 0


def _drop_provider_unsupported_keywords(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _drop_provider_unsupported_keywords(item)
            for key, item in value.items()
            if key not in UNSUPPORTED_PROVIDER_KEYWORDS
        }
    if isinstance(value, list):
        return [_drop_provider_unsupported_keywords(item) for item in value]
    return copy.deepcopy(value)


def build_provider_compatible_schema(schema: Mapping[str, Any]) -> dict[str, Any]:
    result = _drop_provider_unsupported_keywords(dict(schema))
    if any(_count_key(result, key) for key in UNSUPPORTED_PROVIDER_KEYWORDS):
        raise TypedEventSetSchemaRecoveryError("provider keyword removal failed")
    return result


def _schema_profile(schema: Mapping[str, Any]) -> dict[str, int]:
    profile = {
        "object_property_count": 0,
        "object_count": 0,
        "enum_schema_count": 0,
        "enum_value_count": 0,
        "enum_string_char_count": 0,
        "property_name_char_count": 0,
        "min_length_keyword_count": 0,
        "unique_items_keyword_count": 0,
    }

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            properties = value.get("properties")
            if value.get("type") == "object":
                profile["object_count"] += 1
            if isinstance(properties, dict):
                profile["object_property_count"] += len(properties)
                profile["property_name_char_count"] += sum(len(str(key)) for key in properties)
            enum = value.get("enum")
            if isinstance(enum, list):
                profile["enum_schema_count"] += 1
                profile["enum_value_count"] += len(enum)
                profile["enum_string_char_count"] += sum(
                    len(item) for item in enum if isinstance(item, str)
                )
            profile["min_length_keyword_count"] += int("minLength" in value)
            profile["unique_items_keyword_count"] += int("uniqueItems" in value)
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(schema)
    return profile


def _validate_predecessor(paths: Mapping[str, Path]) -> dict[str, Any]:
    typed.verify_frozen(PREDECESSOR_ROOT)
    terminal = _load_json(paths["predecessor_terminal"], "predecessor terminal")
    sidecar = _load_json(paths["predecessor_sidecar"], "predecessor sidecar")
    capacity = _load_json(paths["predecessor_capacity"], "predecessor capacity")
    if (
        terminal.get("terminal_reason") != "infrastructure_or_extraction_attempt_failed"
        or terminal.get("usage_status") != "unknown"
        or terminal.get("accounting_complete") is not False
        or terminal.get("semantic_attempt_count") != 1
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or sidecar.get("state") != "failed"
        or sidecar.get("status") != "failed"
        or sidecar.get("error_class") != "turn_failed"
        or sidecar.get("usage_status") != "unknown"
        or sidecar.get("usage_complete") is not False
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("rate_limit_reached_type") is not None
        or _turn_paths(PREDECESSOR_ROOT)["output"].exists()
    ):
        raise TypedEventSetSchemaRecoveryError("predecessor infrastructure evidence drifted")
    schema = _load_json(paths["predecessor_schema"], "predecessor schema")
    profile = _schema_profile(schema)
    if profile["min_length_keyword_count"] != 25:
        raise TypedEventSetSchemaRecoveryError("predecessor schema diagnosis drifted")
    if profile["unique_items_keyword_count"] != 8:
        raise TypedEventSetSchemaRecoveryError("predecessor schema diagnosis drifted")
    return {"terminal": terminal, "sidecar": sidecar, "capacity": capacity, "schema": schema}


def prepare_recovery(root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    root = root.expanduser().resolve()
    paths = _predecessor_paths()
    evidence = _validate_predecessor(paths)
    cache = ContentHashCache()
    records = {key: _record(path, cache=cache) for key, path in paths.items()}
    compatible = build_provider_compatible_schema(evidence["schema"])
    diagnostic_path = root / "schema-compatibility-diagnostic.json"
    configured._write_stable_time(  # noqa: SLF001
        diagnostic_path,
        {
            "schema_version": "pif_typed_event_set_schema_compatibility_diagnostic_v1",
            "created_at": now_iso(),
            "state": "one_serialization_only_recovery_authorized",
            "classification": "infrastructure_schema_subset_failure",
            "official_schema_guide": OFFICIAL_SCHEMA_GUIDE,
            "documented_supported_string_constraints": ["format", "pattern"],
            "documented_global_limits": {
                "maximum_object_properties": 5000,
                "maximum_enum_values": 1000,
                "maximum_schema_name_enum_const_characters": 120000,
            },
            "failed_schema_profile": _schema_profile(evidence["schema"]),
            "recovery_schema_profile": _schema_profile(compatible),
            "isolated_provider_subset_delta": {
                "removed_keywords": sorted(UNSUPPORTED_PROVIDER_KEYWORDS)
            },
            "semantic_request_invariants": {
                "input_bytes_identical": True,
                "prompt_bytes_identical": True,
                "base_instructions_bytes_identical": True,
                "model": MODEL,
                "effort": EFFORT,
                "retry_count": 0,
                "maximum_total_tokens": MAX_TOTAL_TOKENS,
            },
            "predecessor_usage_status": "unknown",
            "predecessor_attempt_count": 1,
            "predecessor_retry_count": 0,
            "on_any_recovery_failure": "freeze without retry; reject this architecture",
            "holdout_authorized": False,
            "production_mutated": False,
            "predecessor_records": records,
        },
        "created_at",
    )
    records["schema_compatibility_diagnostic"] = _record(diagnostic_path, cache=cache)
    config = {
        "schema_version": CONFIG_VERSION,
        "experiment_id": "typed_event_set_luna_high_schema_compat_recovery_v2_2026_07_18",
        "architecture_id": ARCHITECTURE_ID,
        "recovery_version": 2,
        "output_root": str(root),
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_total_tokens": MAX_TOTAL_TOKENS,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "semantic_request_changed": False,
        "schema_serialization_only_delta": True,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "minimum_remaining_reserve_percent": typed.MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": typed.QUOTA_POINTS_PER_MILLION_TOKENS,
        "production_amortized_context_tokens": typed.PRODUCTION_CONTEXT_TOKENS,
        "production_scale": typed.PRODUCTION_SCALE,
        "baseline_total_tokens": typed.BASELINE_TOTAL_TOKENS,
        **records,
    }
    config_path = root / "experiment-config.json"
    _write_immutable(config_path, config)
    return config_path


def _validate_config(config_path: Path) -> dict[str, Any]:
    value = _load_json(config_path, "recovery config")
    expected = {
        "schema_version": CONFIG_VERSION,
        "architecture_id": ARCHITECTURE_ID,
        "recovery_version": 2,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_total_tokens": MAX_TOTAL_TOKENS,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "semantic_request_changed": False,
        "schema_serialization_only_delta": True,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "minimum_remaining_reserve_percent": typed.MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": typed.QUOTA_POINTS_PER_MILLION_TOKENS,
        "production_amortized_context_tokens": typed.PRODUCTION_CONTEXT_TOKENS,
        "production_scale": typed.PRODUCTION_SCALE,
        "baseline_total_tokens": typed.BASELINE_TOTAL_TOKENS,
    }
    if not isinstance(value, dict) or any(value.get(key) != item for key, item in expected.items()):
        raise TypedEventSetSchemaRecoveryError("recovery config contract drifted")
    if Path(str(value.get("output_root") or "")).expanduser().resolve() != config_path.parent.resolve():
        raise TypedEventSetSchemaRecoveryError("recovery root drifted")
    for key in LINEAGE_KEYS:
        normalize_record(value.get(key) or {})
    return value


def _capacity_policy(root: Path, config: Mapping[str, Any]) -> dict[str, Path]:
    audit = root / "capacity-policy-audit.json"
    policy = root / "capacity-policy.json"
    projected_points = math.ceil(
        MAX_TOTAL_TOKENS * typed.QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    configured._write_stable_time(  # noqa: SLF001
        audit,
        {
            "schema_version": configured.CAPACITY_AUDIT_VERSION,
            "phase_id": str(config["experiment_id"]),
            "created_at": now_iso(),
            "production_mutation_performed": False,
            "measured_basis": {
                "declared_turn_count": 1,
                "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
                "phase_total_token_bound": MAX_TOTAL_TOKENS,
                "projected_phase_quota_points": projected_points,
                "minimum_remaining_reserve_percent": typed.MINIMUM_REMAINING_RESERVE_PERCENT,
            },
        },
        "created_at",
    )
    configured._write_stable_time(  # noqa: SLF001
        policy,
        {
            "schema_version": configured.CAPACITY_POLICY_VERSION,
            "phase_id": str(config["experiment_id"]),
            "created_at": now_iso(),
            "managed_chatgpt_auth_only": True,
            "official_persistent_codex_app_server_only": True,
            "retry_count_per_turn": 0,
            "production_mutation_allowed": False,
            "rate_limit_reached_type_must_be_null": True,
            "unknown_usage_hard_stop": True,
            "ordered_turn_names": [TURN_NAME],
            "minimum_remaining_reserve_percent": typed.MINIMUM_REMAINING_RESERVE_PERCENT,
            "quota_points_per_million_tokens": typed.QUOTA_POINTS_PER_MILLION_TOKENS,
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
            "phase_total_token_bound": MAX_TOTAL_TOKENS,
            "projected_phase_quota_points": projected_points,
            "semantic_output_root": str(root),
            "audit": _record(audit),
        },
        "created_at",
    )
    configured.reserve_module.load_reserve_capacity_policy(policy)
    return {"audit": audit, "policy": policy}


def freeze_recovery(config_path: Path) -> dict[str, Any]:
    config_path = config_path.expanduser().resolve()
    config = _validate_config(config_path)
    root = config_path.parent
    if (root / "runtime-lock.json").exists():
        return verify_frozen(root)
    cache = ContentHashCache()
    for key in LINEAGE_KEYS:
        _verify_record(config[key], cache=cache)
    turn = _turn_paths(root)
    for name, config_key in (
        ("input", "source_input"),
        ("prompt", "source_prompt"),
        ("base", "source_base"),
        ("projection_schema", "projection_schema"),
    ):
        source = Path(config[config_key]["path"])
        if name == "projection_schema":
            _write_immutable(turn[name], _load_json(source, config_key))
        else:
            _write_private_text(turn[name], source.read_text(encoding="utf-8"))
        if _record(turn[name], cache=cache)["sha256"] != config[config_key]["sha256"]:
            raise TypedEventSetSchemaRecoveryError("semantic request bytes changed")
    predecessor_schema = _load_json(Path(config["predecessor_schema"]["path"]), "predecessor schema")
    _write_immutable(turn["schema"], build_provider_compatible_schema(predecessor_schema))
    capacity = _capacity_policy(root, config)
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "experiment_id": config["experiment_id"],
        "architecture_id": ARCHITECTURE_ID,
        "recovery_version": 2,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_total_tokens": MAX_TOTAL_TOKENS,
        "managed_chatgpt_auth_only": True,
        "semantic_request_changed": False,
        "schema_serialization_only_delta": True,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "pinned_codex_cli": _record(PINNED_CODEX, cache=cache),
        "runtime_files": [_record(path, cache=cache) for path in _runtime_files()],
        "config": _record(config_path, cache=cache),
        "direct_lineage": [copy.deepcopy(config[key]) for key in LINEAGE_KEYS],
        "capacity_audit": _record(capacity["audit"], cache=cache),
        "capacity_policy": _record(capacity["policy"], cache=cache),
        "frozen_request": [
            _record(turn[name], cache=cache)
            for name in ("input", "prompt", "base", "schema", "projection_schema")
        ],
    }
    lock_path = root / "runtime-lock.json"
    configured._write_stable_time(lock_path, lock, "frozen_at")  # noqa: SLF001
    _write_immutable(
        root / "runtime-lock-closure.json",
        typed._direct_runtime_receipt(lock_path, cache=ContentHashCache()),  # noqa: SLF001
    )
    return verify_frozen(root)


def verify_frozen(root: Path) -> dict[str, Any]:
    root = root.expanduser().resolve()
    config = _validate_config(root / "experiment-config.json")
    manifest = typed._verify_direct_runtime_lock(  # noqa: SLF001
        root / "runtime-lock.json",
        root / "runtime-lock-closure.json",
        required_fields={
            "schema_version": LOCK_VERSION,
            "experiment_id": config["experiment_id"],
            "architecture_id": ARCHITECTURE_ID,
            "recovery_version": 2,
            "model": MODEL,
            "effort": EFFORT,
            "declared_turn_count": 1,
            "retry_count": 0,
            "semantic_request_changed": False,
            "schema_serialization_only_delta": True,
            "production_mutation_allowed": False,
        },
        required_record_paths=_runtime_files(),
    )
    if manifest.get("pinned_codex_cli") != _record(PINNED_CODEX):
        raise TypedEventSetSchemaRecoveryError("pinned Codex binary drifted")
    configured.reserve_module.load_reserve_capacity_policy(root / "capacity-policy.json")
    cache = ContentHashCache()
    for key in LINEAGE_KEYS:
        _verify_record(config[key], cache=cache)
    turn = _turn_paths(root)
    expected_schema = build_provider_compatible_schema(
        _load_json(Path(config["predecessor_schema"]["path"]), "predecessor schema")
    )
    if _load_json(turn["schema"], "recovery schema") != expected_schema:
        raise TypedEventSetSchemaRecoveryError("recovery schema drifted")
    for name, config_key in (
        ("input", "source_input"),
        ("prompt", "source_prompt"),
        ("base", "source_base"),
        ("projection_schema", "projection_schema"),
    ):
        if _record(turn[name], cache=cache)["sha256"] != config[config_key]["sha256"]:
            raise TypedEventSetSchemaRecoveryError("frozen semantic request drifted")
    launch = root / "launch-receipt.json"
    if launch.exists():
        value = _load_json(launch, "launch receipt")
        for key in ("runtime_lock", "runtime_closure", "config"):
            _verify_record(value.get(key) or {}, cache=cache)
        if value.get("semantic_attempt_count") != 1 or value.get("retry_count") != 0:
            raise TypedEventSetSchemaRecoveryError("launch receipt drifted")
    return {
        "root": root,
        "config": config,
        "runtime_lock": root / "runtime-lock.json",
        "runtime_closure": root / "runtime-lock-closure.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": turn,
        "source": _load_json(Path(config["source_input"]["path"]), "source input"),
    }


def _inner_factory() -> typed.codex_app_server.CodexAppServerClient:
    return typed.codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(root: Path, frozen: Mapping[str, Any], exc: BaseException) -> dict[str, Any]:
    turn = frozen["turn"]
    measured: dict[str, int] | None = None
    current_unknown = 0
    if turn["sidecar"].exists():
        try:
            measured = typed._usage(turn["sidecar"])  # noqa: SLF001
        except Exception:
            current_unknown = 1
    elif turn["capacity"].exists() or turn["output"].exists():
        current_unknown = 1
    semantic = isinstance(
        exc,
        (
            typed.TypedEventSetOutputError,
            typed.TypedEventSetArchitectureStop,
            TypedEventSetSchemaRecoveryStop,
        ),
    )
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "typed_event_set_schema_recovery_structural_or_cost_gate_not_passed"
            if semantic
            else "infrastructure_or_extraction_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": int(measured is not None) + current_unknown,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if current_unknown else "complete",
        "accounting_complete": current_unknown == 0,
        "usage": measured or {field: 0 for field in configured.USAGE_FIELDS},
        "unknown_usage_attempt_count": current_unknown,
        "predecessor_unknown_usage_attempt_count": 1,
        "architecture_strategy_rejected": semantic,
        "support_alignment_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "exact_next_action": (
            "reject this architecture; no further schema or field repair"
            if semantic
            else "preserve this failed recovery without retry"
        ),
    }
    configured._write_stable_time(root / "terminal.json", terminal, "terminal_at")  # noqa: SLF001
    return terminal


async def run_recovery(
    config_path: Path,
    *,
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = config_path.expanduser().resolve().parent
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "terminal")
    frozen = freeze_recovery(config_path)
    try:
        verify_frozen(root)
        launch = root / "launch-receipt.json"
        if not launch.exists():
            configured._write_stable_time(  # noqa: SLF001
                launch,
                {
                    "schema_version": "pif_typed_event_set_schema_recovery_launch_v1",
                    "launched_at": now_iso(),
                    "semantic_attempt_count": 1,
                    "retry_count": 0,
                    "managed_chatgpt_auth_only": True,
                    "semantic_request_changed": False,
                    "schema_serialization_only_delta": True,
                    "holdout_authorized": False,
                    "production_mutation_allowed": False,
                    "runtime_lock": _record(frozen["runtime_lock"]),
                    "runtime_closure": _record(frozen["runtime_closure"]),
                    "config": _record(config_path),
                },
                "launched_at",
            )
        verify_frozen(root)
        turn = frozen["turn"]
        if not turn["sidecar"].exists() and (turn["capacity"].exists() or turn["output"].exists()):
            raise TypedEventSetSchemaRecoveryError("attempt artifact exists without sidecar")
        async with client_factory(frozen["capacity_policy"]) as client:
            if not turn["sidecar"].exists():
                await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
                    base_instructions=turn["base"].read_text(encoding="utf-8"),
                    prompt=turn["prompt"].read_text(encoding="utf-8"),
                    output_schema=_load_json(turn["schema"], "recovery schema"),
                    cwd=PROJECT_ROOT,
                    sidecar_path=turn["sidecar"],
                    output_path=turn["output"],
                    batch_size=2,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=turn["capacity"],
                )
        usage = typed._usage(turn["sidecar"])  # noqa: SLF001
        if usage["total_tokens"] > MAX_TOTAL_TOKENS:
            raise TypedEventSetSchemaRecoveryStop("turn exceeded frozen token ceiling")
        normalized, provenance, diagnostics = typed.validate_and_project_output(
            _load_json(turn["output"], "recovery output"), frozen
        )
        _write_immutable(root / "normalized-output.private.json", normalized)
        _write_immutable(root / "evidence-provenance.private.json", provenance)
        _write_immutable(root / "diagnostics.private.json", diagnostics)
        gate = typed._gate(usage, diagnostics)  # noqa: SLF001
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise TypedEventSetSchemaRecoveryStop("structural or production-cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "typed_event_set_schema_recovery_structural_cost_passed",
            "terminal_reason": "typed_event_set_schema_recovery_passed_support_alignment_required",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "unknown_usage_attempt_count": 0,
            "predecessor_unknown_usage_attempt_count": 1,
            "production_amortized_total_token_ratio": gate[
                "production_amortized_total_token_ratio"
            ],
            "support_alignment_authorized": True,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "gate": _record(gate_path),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "sidecar": _record(turn["sidecar"]),
            "normalized_output": _record(root / "normalized-output.private.json"),
            "evidence_provenance": _record(root / "evidence-provenance.private.json"),
            "exact_next_action": "run the unchanged source-support and neutral two-permutation evaluator",
        }
        configured._write_stable_time(root / "terminal.json", terminal, "terminal_at")  # noqa: SLF001
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "freeze", "verify", "run"))
    parser.add_argument("--root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--config", type=Path)
    args = parser.parse_args(argv)
    if args.command == "prepare":
        path = prepare_recovery(args.root)
        print(json.dumps({"config": str(path), "prepared": True}, sort_keys=True))
        return 0
    config = args.config or (args.root / "experiment-config.json")
    if args.command == "freeze":
        frozen = freeze_recovery(config)
        print(json.dumps({"root": str(frozen["root"]), "frozen": True}, sort_keys=True))
        return 0
    if args.command == "verify":
        frozen = verify_frozen(config.expanduser().resolve().parent)
        print(json.dumps({"root": str(frozen["root"]), "verified": True}, sort_keys=True))
        return 0
    terminal = asyncio.run(run_recovery(config))
    print(json.dumps(terminal, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
    return 0 if terminal.get("support_alignment_authorized") else 2


if __name__ == "__main__":
    raise SystemExit(main())
