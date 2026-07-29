from __future__ import annotations

"""Prepare and, only with direct override, run one adopted-core global join."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import app_server_capacity as capacity_module
from . import app_server_capacity_reserve as reserve_module
from . import app_server_configured_experiment as configured
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .app_server_runtime_verifier import (
    ContentHashCache,
    RuntimeVerificationError,
    closure_receipt,
    normalize_record,
    verify_runtime_lock,
)
from .labels import ValidationError, _validate_schema
from .util import now_iso


CONFIG_VERSION = "pif_app_server_adoption_join_config_v1"
LOCK_VERSION = "pif_app_server_adoption_join_runtime_lock_v1"
PREPARED_VERSION = "pif_app_server_adoption_join_prepared_v1"
AUTHORIZATION_VERSION = "pif_post_v277_operator_override_v1"
ACTIVATION_VERSION = "pif_app_server_adoption_join_activation_v1"
TERMINAL_VERSION = "pif_app_server_adoption_join_terminal_v1"
CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PINNED_CODEX = configured.PINNED_CODEX
USAGE_FIELDS = configured.USAGE_FIELDS
TURN_NAME = "adoption_only_episode_global_owner_join"


class AdoptionJoinError(RuntimeError):
    """The immutable adoption-only join cannot proceed safely."""


class AdoptionAuthorizationRequired(AdoptionJoinError):
    """No direct post-v277 operator override has been frozen."""


class AdoptionJoinOutputContractError(AdoptionJoinError):
    """A measured join output violated the frozen contract."""


class AdoptionJoinArchitectureStop(AdoptionJoinError):
    """The measured join failed a frozen structural or cost gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdoptionJoinError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise AdoptionJoinError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise AdoptionJoinError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _record(path: Path, *, cache: ContentHashCache | None = None) -> dict[str, Any]:
    return (cache or ContentHashCache()).record(path)


def _verify_record(record: Mapping[str, Any], *, cache: ContentHashCache) -> None:
    if not cache.verify_record(record):
        raise AdoptionJoinError("frozen artifact record drifted")


def _turn_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns" / TURN_NAME.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _validate_config(config_path: Path) -> dict[str, Any]:
    config = _load_json(config_path, "adoption config")
    if (
        not isinstance(config, dict)
        or config.get("schema_version") != CONFIG_VERSION
        or config.get("architecture_id")
        != "adopt_measured_compact_cores_then_episode_global_llm_owner_join"
        or config.get("model") != "gpt-5.6-sol"
        or config.get("effort") != "low"
        or config.get("turn_name") != TURN_NAME
        or config.get("semantic_launch_authorized") is not False
        or config.get("managed_chatgpt_auth_only") is not True
        or config.get("official_persistent_codex_app_server_only") is not True
        or config.get("retry_count") != 0
        or config.get("adopted_core_tokens") != 30253
        or config.get("join_total_tokens_maximum") != 40000
        or config.get("combined_total_tokens_maximum") != 70253
        or config.get("minimum_remaining_reserve_percent") != 20
        or config.get("quota_points_per_million_tokens") != 17
        or config.get("production_mutation_allowed") is not False
        or config.get("holdout_authorized") is not False
    ):
        raise AdoptionJoinError("adoption config contract drifted")
    output_root = Path(str(config.get("output_root") or "")).expanduser().resolve()
    if not output_root.is_absolute() or output_root != config_path.parent.resolve():
        raise AdoptionJoinError("adoption output root drifted")
    original_join_root = Path(str(config.get("original_join_root") or "")).expanduser().resolve()
    if not original_join_root.is_absolute():
        raise AdoptionJoinError("original join root is malformed")
    for key in (
        "source_input",
        "core_schema",
        "core_output",
        "core_sidecar",
        "core_capacity",
        "predecessor_terminal",
        "predecessor_runtime_lock",
        "predecessor_runtime_closure",
        "convergence_blocker",
        "join_base",
        "join_schema",
    ):
        normalize_record(config.get(key) or {})
    return config


def _assert_original_join_never_started(config: Mapping[str, Any]) -> None:
    root = Path(config["original_join_root"]).expanduser().resolve()
    forbidden = (
        root / "capacity.json",
        root / "sidecar.json",
        root / "output.private.json",
        root / "input.private.json",
        root / "prompt.private.md",
    )
    if any(path.exists() for path in forbidden):
        raise AdoptionJoinError("original failed canary join artifacts appeared")


def _verify_predecessor(config: Mapping[str, Any]) -> None:
    cache = ContentHashCache()
    for key in (
        "source_input",
        "core_schema",
        "core_output",
        "core_sidecar",
        "core_capacity",
        "predecessor_terminal",
        "predecessor_runtime_lock",
        "predecessor_runtime_closure",
        "convergence_blocker",
        "join_base",
        "join_schema",
    ):
        _verify_record(config[key], cache=cache)
    closure = _load_json(
        Path(config["predecessor_runtime_closure"]["path"]),
        "predecessor runtime closure",
    )
    try:
        verify_runtime_lock(
            Path(config["predecessor_runtime_lock"]["path"]),
            cache=cache,
            expected_manifest_record=closure.get("manifest"),
            expected_closure_digest=closure.get("closure_digest"),
        )
    except RuntimeVerificationError as exc:
        raise AdoptionJoinError("predecessor runtime closure drifted") from exc
    terminal = _load_json(
        Path(config["predecessor_terminal"]["path"]), "predecessor terminal"
    )
    sidecar = _load_json(Path(config["core_sidecar"]["path"]), "core sidecar")
    capacity = _load_json(Path(config["core_capacity"]["path"]), "core capacity")
    usage = sidecar.get("usage") if isinstance(sidecar, dict) else None
    if (
        terminal.get("terminal_reason")
        != "configured_canary_structural_or_cost_gate_not_passed"
        or terminal.get("error_class") != "ConfiguredArchitectureStop"
        or terminal.get("semantic_attempt_count") != 1
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("accounting_complete") is not True
        or terminal.get("production_mutated") is not False
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != "gpt-5.6-sol"
        or sidecar.get("effort") != "low"
        or not isinstance(usage, dict)
        or usage.get("total_tokens") != config["adopted_core_tokens"]
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("rate_limit_reached_type") is not None
    ):
        raise AdoptionJoinError("measured predecessor contract drifted")
    _assert_original_join_never_started(config)


def _adopt_core_packet(config: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    source = _load_json(Path(config["source_input"]["path"]), "source input")
    schema = _load_json(Path(config["core_schema"]["path"]), "core schema")
    output = _load_json(Path(config["core_output"]["path"]), "core output")
    try:
        _validate_schema(schema, output, path="$")
    except (ValidationError, TypeError, ValueError) as exc:
        raise AdoptionJoinError("adopted core output schema drifted") from exc
    segment_ids = [str(row["segment_id"]) for row in source["segments"]]
    rows = list(output.get("segments") or [])
    if output.get("episode_id") != source["episode_id"] or [
        row.get("segment_id") for row in rows
    ] != segment_ids:
        raise AdoptionJoinError("adopted core episode or segment order drifted")
    source_by_id = {str(row["segment_id"]): row for row in source["segments"]}
    seen = set()
    packet_rows = []
    counts: dict[str, int] = {}
    for segment_position, row in enumerate(rows):
        segment_id = str(row["segment_id"])
        source_segment = source_by_id[segment_id]
        expected_units = [str(unit["unit_id"]) for unit in source_segment["units"]]
        if list(row["covered_unit_ids"]) != expected_units:
            raise AdoptionJoinError("adopted core unit coverage drifted")
        events = list(row["events"])
        if (row["status"] == "coded") != bool(events):
            raise AdoptionJoinError("adopted core status drifted")
        unit_index = {unit_id: index for index, unit_id in enumerate(expected_units)}
        prior_start = -1
        packet_events = []
        for event_position, raw_event in enumerate(events):
            event = copy.deepcopy(dict(raw_event))
            start_id = str(event["evidence_start_unit_id"])
            end_id = str(event["evidence_end_unit_id"])
            evidence, _start, _end, _window = configured._evidence(
                source_segment, start_id, end_id
            )
            if unit_index[start_id] < prior_start:
                raise AdoptionJoinError("adopted core event order drifted")
            prior_start = unit_index[start_id]
            metric = str(event.get("metric_raw_text") or "")
            if metric and metric not in evidence:
                raise AdoptionJoinError("adopted core metric drifted")
            identity = _canonical_json(event)
            if identity in seen:
                raise AdoptionJoinError("adopted core exact identity duplicate")
            seen.add(identity)
            packet_events.append(
                {"core_id": f"S{segment_position}C{event_position:03d}", **event}
            )
        packet_rows.append({"segment_id": segment_id, "events": packet_events})
        counts[segment_id] = len(events)
    if list(counts.values()) != [24, 1]:
        raise AdoptionJoinError("adopted core event counts drifted")
    return (
        {
            "schema_version": configured.CORE_SCHEMA_VERSION,
            "episode_id": source["episode_id"],
            "segments": packet_rows,
        },
        {
            "schema_version": PREPARED_VERSION,
            "source_unit_count": sum(len(row["units"]) for row in source["segments"]),
            "covered_source_unit_count": sum(len(row["covered_unit_ids"]) for row in rows),
            "segment_event_counts": counts,
            "core_event_count": sum(counts.values()),
            "exact_evidence_validated": True,
            "metric_grounding_error_count": 0,
            "exact_identity_duplicate_count": 0,
        },
    )


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(configured.__file__).resolve(),
                Path(capacity_module.__file__).resolve(),
                Path(reserve_module.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
                codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
            },
            key=str,
        )
    )


def _capacity_policy(root: Path, config: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    maximum = int(config["join_total_tokens_maximum"])
    projected = math.ceil(
        maximum * int(config["quota_points_per_million_tokens"]) / 1_000_000
    )
    _write_stable_time(
        audit_path,
        {
            "schema_version": CAPACITY_AUDIT_VERSION,
            "phase_id": config["experiment_id"],
            "created_at": now_iso(),
            "production_mutation_performed": False,
            "measured_basis": {
                "declared_turn_count": 1,
                "maximum_total_tokens_per_turn": maximum,
                "phase_total_token_bound": maximum,
                "projected_phase_quota_points": projected,
                "minimum_remaining_reserve_percent": config[
                    "minimum_remaining_reserve_percent"
                ],
            },
        },
        "created_at",
    )
    _write_stable_time(
        policy_path,
        {
            "schema_version": CAPACITY_POLICY_VERSION,
            "phase_id": config["experiment_id"],
            "created_at": now_iso(),
            "managed_chatgpt_auth_only": True,
            "official_persistent_codex_app_server_only": True,
            "retry_count_per_turn": 0,
            "production_mutation_allowed": False,
            "rate_limit_reached_type_must_be_null": True,
            "unknown_usage_hard_stop": True,
            "ordered_turn_names": [TURN_NAME],
            "minimum_remaining_reserve_percent": config[
                "minimum_remaining_reserve_percent"
            ],
            "quota_points_per_million_tokens": config[
                "quota_points_per_million_tokens"
            ],
            "maximum_total_tokens_per_turn": maximum,
            "phase_total_token_bound": maximum,
            "projected_phase_quota_points": projected,
            "semantic_output_root": str(root),
            "audit": _record(audit_path),
        },
        "created_at",
    )
    reserve_module.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def freeze_prepared(config_path: Path) -> dict[str, Any]:
    config_path = config_path.expanduser().resolve()
    config = _validate_config(config_path)
    root = config_path.parent
    if (root / "runtime-lock.json").is_file():
        return verify_prepared(root)
    _verify_predecessor(config)
    core_packet, diagnostics = _adopt_core_packet(config)
    source = _load_json(Path(config["source_input"]["path"]), "source input")
    turn = _turn_paths(root)
    base = Path(config["join_base"]["path"]).read_text(encoding="utf-8")
    schema = _load_json(Path(config["join_schema"]["path"]), "join schema")
    join_input = {
        "schema_version": configured.JOIN_SCHEMA_VERSION,
        "source": source,
        "compact_event_cores": core_packet,
        "privacy": "private immutable source and adopted LLM event cores",
    }
    prompt = configured._join_prompt(source, core_packet)
    _write_immutable(turn["input"], join_input)
    _write_private_text(turn["prompt"], prompt)
    _write_private_text(turn["base"], base)
    _write_immutable(turn["schema"], schema)
    _write_immutable(root / "adopted-core-packet.private.json", core_packet)
    _write_immutable(root / "adopted-core-validation.json", diagnostics)
    capacity_paths = _capacity_policy(root, config)
    prepared_path = root / "prepared-receipt.json"
    _write_stable_time(
        prepared_path,
        {
            "schema_version": PREPARED_VERSION,
            "prepared_at": now_iso(),
            "state": "prepared_waiting_for_direct_operator_override",
            "semantic_launch_authorized": False,
            "semantic_attempt_started": False,
            "adopted_core_turn_replayed": False,
            "declared_new_semantic_turn_count": 1,
            "retry_count": 0,
            "adopted_core_tokens": config["adopted_core_tokens"],
            "join_total_tokens_maximum": config["join_total_tokens_maximum"],
            "combined_total_tokens_maximum": config[
                "combined_total_tokens_maximum"
            ],
            "projected_production_amortized_total_token_ratio_maximum": 0.269053,
            "core_validation": _record(root / "adopted-core-validation.json"),
            "core_packet": _record(root / "adopted-core-packet.private.json"),
            "join_request": [
                _record(turn[name]) for name in ("input", "prompt", "base", "schema")
            ],
            "production_mutated": False,
            "holdout_authorized": False,
        },
        "prepared_at",
    )
    cache = ContentHashCache()
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "experiment_id": config["experiment_id"],
        "architecture_id": config["architecture_id"],
        "declared_turn_count": 1,
        "retry_count": 0,
        "semantic_launch_authorized": False,
        "operator_override_required": True,
        "adopted_core_turn_replayed": False,
        "combined_total_tokens_maximum": config["combined_total_tokens_maximum"],
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "pinned_codex_cli": _record(PINNED_CODEX, cache=cache),
        "runtime_files": [_record(path, cache=cache) for path in _runtime_files()],
        "config": _record(config_path, cache=cache),
        "direct_lineage": [
            copy.deepcopy(config[key])
            for key in (
                "source_input",
                "core_schema",
                "core_output",
                "core_sidecar",
                "core_capacity",
                "predecessor_terminal",
                "predecessor_runtime_lock",
                "predecessor_runtime_closure",
                "convergence_blocker",
                "join_base",
                "join_schema",
            )
        ],
        "prepared_receipt": _record(prepared_path, cache=cache),
        "capacity_audit": _record(capacity_paths["audit"], cache=cache),
        "capacity_policy": _record(capacity_paths["policy"], cache=cache),
        "request": [_record(turn[name], cache=cache) for name in ("input", "prompt", "base", "schema")],
    }
    lock_path = root / "runtime-lock.json"
    _write_stable_time(lock_path, lock, "frozen_at")
    _write_immutable(
        root / "runtime-lock-closure.json",
        closure_receipt(lock_path, cache=ContentHashCache()),
    )
    return verify_prepared(root)


def verify_prepared(root: Path) -> dict[str, Any]:
    root = root.expanduser().resolve()
    config_path = root / "experiment-config.json"
    config = _validate_config(config_path)
    _verify_predecessor(config)
    closure_path = root / "runtime-lock-closure.json"
    closure = _load_json(closure_path, "runtime closure")
    try:
        result = verify_runtime_lock(
            root / "runtime-lock.json",
            cache=ContentHashCache(),
            expected_manifest_record=closure.get("manifest"),
            expected_closure_digest=closure.get("closure_digest"),
            required_fields={
                "schema_version": LOCK_VERSION,
                "experiment_id": config["experiment_id"],
                "declared_turn_count": 1,
                "retry_count": 0,
                "semantic_launch_authorized": False,
                "operator_override_required": True,
                "production_mutation_allowed": False,
            },
            required_record_paths=_runtime_files(),
        )
    except RuntimeVerificationError as exc:
        raise AdoptionJoinError("adoption runtime lock verification failed") from exc
    if result.manifest.get("pinned_codex_cli") != _record(PINNED_CODEX):
        raise AdoptionJoinError("pinned Codex binary drifted")
    reserve_module.load_reserve_capacity_policy(root / "capacity-policy.json")
    turn = _turn_paths(root)
    launch_path = root / "launch-receipt.json"
    if not launch_path.exists() and any(
        turn[name].exists() for name in ("capacity", "sidecar", "output")
    ):
        raise AdoptionJoinError("semantic artifact appeared before launch authorization")
    return {
        "root": root,
        "config": config,
        "runtime_lock": root / "runtime-lock.json",
        "runtime_closure": closure_path,
        "prepared_receipt": root / "prepared-receipt.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": turn,
        "source": _load_json(Path(config["source_input"]["path"]), "source input"),
        "core_packet": _load_json(root / "adopted-core-packet.private.json", "core packet"),
    }


def _load_operator_authorization(frozen: Mapping[str, Any]) -> dict[str, Any]:
    root = frozen["root"]
    path = root / "operator-authorization.json"
    if not path.is_file():
        raise AdoptionAuthorizationRequired(
            "direct post-v277 operator override is required before semantic launch"
        )
    value = _load_json(path, "operator authorization")
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != AUTHORIZATION_VERSION
        or value.get("authority") != "direct_user_instruction"
        or value.get("scope") != "one_adoption_only_episode_global_owner_join"
        or value.get("semantic_launch_authorized") is not True
        or value.get("retry_count") != 0
        or value.get("adopted_core_turn_replayed") is not False
        or value.get("production_mutation_allowed") is not False
        or value.get("holdout_authorized") is not False
        or value.get("runtime_lock") != _record(frozen["runtime_lock"])
        or value.get("convergence_blocker")
        != frozen["config"]["convergence_blocker"]
    ):
        raise AdoptionAuthorizationRequired("operator override receipt contract drifted")
    return value


def _usage(path: Path, config: Mapping[str, Any]) -> dict[str, int]:
    sidecar = _load_json(path, "join sidecar")
    values = sidecar.get("usage")
    if not isinstance(values, Mapping):
        raise AdoptionJoinError("join usage is absent")
    usage = {}
    for field in USAGE_FIELDS:
        value = values.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise AdoptionJoinError("join usage is incomplete")
        usage[field] = value
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != config["model"]
        or sidecar.get("effort") != config["effort"]
        or sidecar.get("error_class") is not None
        or usage["cached_input_tokens"] > usage["input_tokens"]
        or usage["reasoning_output_tokens"] > usage["output_tokens"]
        or usage["total_tokens"] != usage["input_tokens"] + usage["output_tokens"]
        or usage["total_tokens"] > config["join_total_tokens_maximum"]
    ):
        raise AdoptionJoinError("join sidecar accounting contract failed")
    return usage


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(
    frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    root = frozen["root"]
    turn = frozen["turn"]
    config = frozen["config"]
    attempted = int(turn["capacity"].exists() or turn["sidecar"].exists())
    unknown = attempted
    usage = {field: 0 for field in USAGE_FIELDS}
    if turn["sidecar"].is_file():
        try:
            usage = _usage(turn["sidecar"], config)
            unknown = 0
        except Exception:
            unknown = 1
    semantic = isinstance(
        exc, (AdoptionJoinOutputContractError, AdoptionJoinArchitectureStop)
    )
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "adoption_join_structural_or_cost_gate_not_passed"
            if semantic
            else "infrastructure_or_judge_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": attempted,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "new_join_usage": usage,
        "adopted_core_usage_tokens": config["adopted_core_tokens"],
        "unknown_usage_attempt_count": unknown,
        "support_alignment_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "exact_next_action": "audit this immutable adoption-only attempt; no retry or successor field patch",
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


def _gate(
    frozen: Mapping[str, Any], usage: Mapping[str, int], diagnostics: Mapping[str, Any]
) -> dict[str, Any]:
    config = frozen["config"]
    rows = diagnostics["diagnostics"]
    counts = {str(row["segment_id"]): int(row["event_count"]) for row in rows}
    source = frozen["source"]
    dense_id = str(source["segments"][0]["segment_id"])
    residual_id = str(source["segments"][1]["segment_id"])
    combined = config["adopted_core_tokens"] + usage["total_tokens"]
    production_total = 600538 + combined * 30
    ratio = production_total / 10065426
    checks = {
        "one_join_turn_measured": True,
        "adopted_core_turn_not_replayed": True,
        "join_tokens_lte_40000": usage["total_tokens"] <= 40000,
        "combined_tokens_lte_70253": combined <= 70253,
        "all_core_events_accounted_exactly_once": diagnostics[
            "accounted_core_event_count"
        ]
        == diagnostics["core_event_count"],
        "dense_event_count_gte_27": counts.get(dense_id, -1) >= 27,
        "nominal_no_signal_event_count_lte_1": 0
        <= counts.get(residual_id, -1)
        <= 1,
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "exact_identity_duplicates_0": True,
        "event_cap_violations_0": True,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": "pif_adoption_join_structural_gate_v1",
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "adopted_core_tokens": config["adopted_core_tokens"],
        "new_join_usage": dict(usage),
        "combined_extraction_tokens": combined,
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "dense_event_count": counts.get(dense_id, -1),
        "candidate_only_nominal_no_signal_event_count": counts.get(residual_id, -1),
        "support_alignment_authorized": not failed,
        "holdout_authorized": False,
        "production_mutated": False,
    }


async def run_authorized(
    config_path: Path,
    *,
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = config_path.expanduser().resolve().parent
    if (root / "terminal.json").is_file():
        return _load_json(root / "terminal.json", "terminal")
    frozen = freeze_prepared(config_path)
    authorization = _load_operator_authorization(frozen)
    try:
        verify_prepared(root)
        activation_path = root / "authorization-activation-receipt.json"
        _write_stable_time(
            activation_path,
            {
                "schema_version": ACTIVATION_VERSION,
                "activated_at": now_iso(),
                "operator_authorization": _record(root / "operator-authorization.json"),
                "runtime_lock": _record(frozen["runtime_lock"]),
                "runtime_closure": _record(frozen["runtime_closure"]),
                "prepared_receipt": _record(frozen["prepared_receipt"]),
                "declared_semantic_turn_count": 1,
                "retry_count": 0,
                "production_mutation_allowed": False,
                "holdout_authorized": False,
            },
            "activated_at",
        )
        launch_path = root / "launch-receipt.json"
        _write_stable_time(
            launch_path,
            {
                "schema_version": "pif_app_server_adoption_join_launch_v1",
                "launched_at": now_iso(),
                "activation": _record(activation_path),
                "runtime_lock": _record(frozen["runtime_lock"]),
                "declared_semantic_turn_count": 1,
                "retry_count": 0,
                "managed_chatgpt_auth_only": True,
                "production_mutation_allowed": False,
                "holdout_authorized": False,
            },
            "launched_at",
        )
        turn = frozen["turn"]
        if any(turn[name].exists() for name in ("capacity", "sidecar", "output")):
            raise AdoptionJoinError("adoption join attempt already has semantic artifacts")
        async with client_factory(frozen["capacity_policy"]) as client:
            await client.run_ephemeral_structured_turn(
                model=frozen["config"]["model"],
                effort=frozen["config"]["effort"],
                base_instructions=turn["base"].read_text(encoding="utf-8"),
                prompt=turn["prompt"].read_text(encoding="utf-8"),
                output_schema=_load_json(turn["schema"], "join schema"),
                cwd=PROJECT_ROOT,
                sidecar_path=turn["sidecar"],
                output_path=turn["output"],
                batch_size=2,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=turn["capacity"],
            )
        usage = _usage(turn["sidecar"], frozen["config"])
        output = _load_json(turn["output"], "join output")
        validator_frozen = {
            "source": frozen["source"],
            "turns": [{}, {"schema": turn["schema"]}],
        }
        try:
            normalized, provenance, diagnostics = configured.validate_join_output(
                output, frozen["core_packet"], validator_frozen
            )
        except configured.ConfiguredOutputContractError as exc:
            raise AdoptionJoinOutputContractError(str(exc)) from exc
        _write_immutable(root / "normalized-output.private.json", normalized)
        _write_immutable(root / "evidence-provenance.private.json", provenance)
        _write_immutable(root / "diagnostics.private.json", diagnostics)
        gate = _gate(frozen, usage, diagnostics)
        _write_immutable(root / "architecture-structural-gate.json", gate)
        if not gate["passed"]:
            raise AdoptionJoinArchitectureStop("adoption join structural gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "adoption_join_structural_cost_passed",
            "terminal_reason": "adoption_join_structural_cost_gate_passed_support_alignment_required",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "new_join_usage": dict(usage),
            "adopted_core_usage_tokens": frozen["config"]["adopted_core_tokens"],
            "combined_extraction_tokens": gate["combined_extraction_tokens"],
            "production_amortized_total_token_ratio": gate[
                "production_amortized_total_token_ratio"
            ],
            "support_alignment_authorized": True,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "runtime_lock": _record(frozen["runtime_lock"]),
            "operator_authorization": _record(root / "operator-authorization.json"),
            "gate": _record(root / "architecture-structural-gate.json"),
            "normalized_output": _record(root / "normalized-output.private.json"),
            "exact_next_action": "run unchanged frozen side-free support and two-permutation alignment; do not create a field-repair successor",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return _failure_terminal(frozen, exc)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "verify", "run"))
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.command == "freeze":
        frozen = freeze_prepared(args.config)
        print(_canonical_json({"state": "prepared", "root": str(frozen["root"])}))
        return 0
    if args.command == "verify":
        frozen = verify_prepared(args.config.expanduser().resolve().parent)
        print(_canonical_json({"state": "verified", "root": str(frozen["root"])}))
        return 0
    try:
        terminal = asyncio.run(run_authorized(args.config))
    except AdoptionAuthorizationRequired as exc:
        print(
            _canonical_json(
                {
                    "state": "waiting_for_direct_operator_override",
                    "semantic_attempt_started": False,
                    "error_class": type(exc).__name__,
                    "error_message_sha256": hashlib.sha256(str(exc).encode()).hexdigest(),
                }
            )
        )
        return 3
    print(_canonical_json(terminal))
    return 0 if terminal.get("terminal_reason", "").endswith("support_alignment_required") else 2


if __name__ == "__main__":
    raise SystemExit(main())
