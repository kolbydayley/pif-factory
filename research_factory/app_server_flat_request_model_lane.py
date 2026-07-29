from __future__ import annotations

"""Run one immutable flat-schema extraction request through a frontier model lane."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import app_server_configured_experiment as configured
from . import codex_app_server
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .app_server_runtime_verifier import (
    ContentHashCache,
    RuntimeVerificationError,
    closure_receipt,
    normalize_record,
    verify_runtime_lock,
)
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


CONFIG_VERSION = "pif_flat_request_model_lane_config_v1"
LOCK_VERSION = "pif_flat_request_model_lane_runtime_lock_v1"
TERMINAL_VERSION = "pif_flat_request_model_lane_terminal_v1"
ARCHITECTURE_ID = "single_pass_flat_total_field_gpt55_high"
TURN_NAME = "flat_total_field_frontier_lane"
MODEL = "gpt-5.5"
EFFORT = "high"
MAX_TOTAL_TOKENS = 70_000
BASELINE_TOTAL_TOKENS = 10_065_426
PRODUCTION_CONTEXT_TOKENS = 600_538
PRODUCTION_SCALE = 30
MINIMUM_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PINNED_CODEX = configured.PINNED_CODEX
USAGE_FIELDS = configured.USAGE_FIELDS
IDENTITY_FIELDS = configured.IDENTITY_FIELDS
LINEAGE_KEYS = (
    "request_input",
    "request_prompt",
    "request_base",
    "request_schema",
    "v239_runtime_lock",
    "v239_terminal",
    "phase_checkpoint",
    "delta_futility_audit",
    "evaluator_protocol_lock",
    "evaluator_protocol_receipt",
    "judge_calibration_terminal",
    "architecture_decision",
)


class FlatRequestModelLaneError(RuntimeError):
    pass


class FlatRequestOutputError(FlatRequestModelLaneError):
    pass


class FlatRequestArchitectureStop(FlatRequestModelLaneError):
    pass


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FlatRequestModelLaneError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    configured._write_immutable(path, value)  # noqa: SLF001


def _write_private_text(path: Path, value: str) -> None:
    configured._write_private_text(path, value)  # noqa: SLF001


def _record(path: Path, *, cache: ContentHashCache | None = None) -> dict[str, Any]:
    return (cache or ContentHashCache()).record(path)


def _verify_record(record: Mapping[str, Any], *, cache: ContentHashCache) -> None:
    if not cache.verify_record(record):
        raise FlatRequestModelLaneError("frozen direct-lineage artifact drifted")


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
    value = _load_json(config_path, "experiment config")
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != CONFIG_VERSION
        or value.get("architecture_id") != ARCHITECTURE_ID
        or value.get("model") != MODEL
        or value.get("effort") != EFFORT
        or value.get("declared_turn_count") != 1
        or value.get("retry_count") != 0
        or value.get("maximum_total_tokens") != MAX_TOTAL_TOKENS
        or value.get("managed_chatgpt_auth_only") is not True
        or value.get("official_persistent_codex_app_server_only") is not True
        or value.get("semantic_regex_or_keyword_filtering") is not False
        or value.get("production_mutation_allowed") is not False
        or value.get("holdout_authorized") is not False
        or value.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or value.get("quota_points_per_million_tokens")
        != QUOTA_POINTS_PER_MILLION_TOKENS
        or value.get("production_amortized_context_tokens")
        != PRODUCTION_CONTEXT_TOKENS
        or value.get("production_scale") != PRODUCTION_SCALE
        or value.get("baseline_total_tokens") != BASELINE_TOTAL_TOKENS
    ):
        raise FlatRequestModelLaneError("experiment config contract drifted")
    if Path(str(value.get("output_root") or "")).expanduser().resolve() != config_path.parent.resolve():
        raise FlatRequestModelLaneError("experiment output root drifted")
    for key in LINEAGE_KEYS:
        normalize_record(value.get(key) or {})
    projected_ratio = (
        PRODUCTION_CONTEXT_TOKENS + MAX_TOTAL_TOKENS * PRODUCTION_SCALE
    ) / BASELINE_TOTAL_TOKENS
    if projected_ratio > 0.28:
        raise FlatRequestModelLaneError("frozen token ceiling exceeds production target")
    return value


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {Path(__file__).resolve(), *configured._runtime_files()},  # noqa: SLF001
            key=str,
        )
    )


def _capacity_policy(root: Path, config: Mapping[str, Any]) -> dict[str, Path]:
    audit = root / "capacity-policy-audit.json"
    policy = root / "capacity-policy.json"
    projected_points = math.ceil(
        MAX_TOTAL_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
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
                "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
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
            "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
            "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
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


def freeze_experiment(config_path: Path) -> dict[str, Any]:
    config_path = config_path.expanduser().resolve()
    config = _validate_config(config_path)
    root = config_path.parent
    if (root / "runtime-lock.json").is_file():
        return verify_frozen(root)
    cache = ContentHashCache()
    for key in LINEAGE_KEYS:
        _verify_record(config[key], cache=cache)
    turn = _turn_paths(root)
    for destination, source_key in (
        ("input", "request_input"),
        ("prompt", "request_prompt"),
        ("base", "request_base"),
        ("schema", "request_schema"),
    ):
        source = Path(str(config[source_key]["path"]))
        _write_private_text(turn[destination], source.read_text(encoding="utf-8"))
        if _record(turn[destination], cache=cache)["sha256"] != config[source_key]["sha256"]:
            raise FlatRequestModelLaneError("frozen request bytes changed during copy")
    capacity = _capacity_policy(root, config)
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "experiment_id": config["experiment_id"],
        "architecture_id": ARCHITECTURE_ID,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_total_tokens": MAX_TOTAL_TOKENS,
        "managed_chatgpt_auth_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "pinned_codex_cli": _record(PINNED_CODEX, cache=cache),
        "runtime_files": [_record(path, cache=cache) for path in _runtime_files()],
        "config": _record(config_path, cache=cache),
        "direct_lineage": [copy.deepcopy(config[key]) for key in LINEAGE_KEYS],
        "capacity_audit": _record(capacity["audit"], cache=cache),
        "capacity_policy": _record(capacity["policy"], cache=cache),
        "frozen_request": [
            _record(turn[name], cache=cache)
            for name in ("input", "prompt", "base", "schema")
        ],
    }
    lock_path = root / "runtime-lock.json"
    configured._write_stable_time(lock_path, lock, "frozen_at")  # noqa: SLF001
    _write_immutable(
        root / "runtime-lock-closure.json",
        closure_receipt(lock_path, cache=ContentHashCache()),
    )
    return verify_frozen(root)


def verify_frozen(root: Path) -> dict[str, Any]:
    root = root.expanduser().resolve()
    config = _validate_config(root / "experiment-config.json")
    receipt = _load_json(root / "runtime-lock-closure.json", "runtime closure")
    try:
        result = verify_runtime_lock(
            root / "runtime-lock.json",
            cache=ContentHashCache(),
            expected_manifest_record=receipt.get("manifest"),
            expected_closure_digest=receipt.get("closure_digest"),
            required_fields={
                "schema_version": LOCK_VERSION,
                "experiment_id": config["experiment_id"],
                "architecture_id": ARCHITECTURE_ID,
                "model": MODEL,
                "effort": EFFORT,
                "declared_turn_count": 1,
                "retry_count": 0,
                "production_mutation_allowed": False,
            },
            required_record_paths=_runtime_files(),
        )
    except RuntimeVerificationError as exc:
        raise FlatRequestModelLaneError("runtime lock verification failed") from exc
    if result.manifest.get("pinned_codex_cli") != _record(PINNED_CODEX):
        raise FlatRequestModelLaneError("pinned Codex binary drifted")
    configured.reserve_module.load_reserve_capacity_policy(root / "capacity-policy.json")
    cache = ContentHashCache()
    for key in LINEAGE_KEYS:
        _verify_record(config[key], cache=cache)
    launch = root / "launch-receipt.json"
    if launch.exists():
        value = _load_json(launch, "launch receipt")
        for key in ("runtime_lock", "runtime_closure", "config"):
            _verify_record(value.get(key) or {}, cache=cache)
        if value.get("semantic_attempt_count") != 1 or value.get("retry_count") != 0:
            raise FlatRequestModelLaneError("launch receipt contract drifted")
    return {
        "root": root,
        "config": config,
        "runtime_lock": root / "runtime-lock.json",
        "runtime_closure": root / "runtime-lock-closure.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": _turn_paths(root),
        "source": _load_json(Path(config["request_input"]["path"]), "source input"),
    }


def validate_and_project_output(
    output: Mapping[str, Any], frozen: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    turn = frozen["turn"]
    schema = _load_json(turn["schema"], "structured output schema")
    try:
        _validate_schema(schema, output, path="$")
    except (ValidationError, TypeError, ValueError) as exc:
        raise FlatRequestOutputError("structured output schema failed") from exc
    source = frozen["source"]
    segment_ids = [str(row["segment_id"]) for row in source["segments"]]
    rows = list(output.get("segments") or [])
    if output.get("episode_id") != source.get("episode_id") or [
        row.get("segment_id") for row in rows
    ] != segment_ids:
        raise FlatRequestOutputError("episode or segment order drifted")
    source_by_id = {str(row["segment_id"]): row for row in source["segments"]}
    normalized_rows: list[dict[str, Any]] = []
    provenance_rows: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        segment_id = str(row["segment_id"])
        source_row = source_by_id[segment_id]
        units = list(source_row["units"])
        expected_ids = [str(unit["unit_id"]) for unit in units]
        receipts = list(row.get("unit_receipts") or [])
        coverage = row.get("coverage_audit") or {}
        if [receipt.get("unit_id") for receipt in receipts] != expected_ids:
            raise FlatRequestOutputError("source unit receipt order drifted")
        if coverage.get("all_source_units_reviewed") is not True or coverage.get("unresolved_count") != 0:
            raise FlatRequestOutputError("source unit coverage audit failed")
        if any(receipt.get("unresolved_count") != 0 for receipt in receipts):
            raise FlatRequestOutputError("source unit receipt is unresolved")
        receipt_counts = {
            str(receipt["unit_id"]): int(receipt["eligible_event_count"])
            for receipt in receipts
        }
        raw_events = list(row.get("events") or [])
        if sum(receipt_counts.values()) != len(raw_events):
            raise FlatRequestOutputError("source unit event total drifted")
        if (row.get("status") == "coded") != bool(raw_events) or len(raw_events) > 32:
            raise FlatRequestOutputError("segment status or event cap drifted")
        unit_index = {unit_id: index for index, unit_id in enumerate(expected_ids)}
        start_counts = {unit_id: 0 for unit_id in expected_ids}
        prior_start = -1
        projected_events: list[dict[str, Any]] = []
        for event_index, raw_event in enumerate(raw_events):
            event = copy.deepcopy(dict(raw_event))
            start_id = str(event.pop("evidence_start_unit_id"))
            end_id = str(event.pop("evidence_end_unit_id"))
            try:
                evidence, start_char, end_char, window_id = configured._evidence(  # noqa: SLF001
                    source_row, start_id, end_id
                )
            except configured.ConfiguredOutputContractError as exc:
                raise FlatRequestOutputError(str(exc)) from exc
            if unit_index[start_id] < prior_start:
                raise FlatRequestOutputError("event source order drifted")
            prior_start = unit_index[start_id]
            metric_values = [
                str(event.get(field) or "")
                for field in (
                    "metric_value",
                    "metric_unit",
                    "metric_comparator",
                    "metric_raw_text",
                )
            ]
            if any(value and value not in evidence for value in metric_values):
                raise FlatRequestOutputError("metric literal is not in exact evidence")
            if any(metric_values) == (event.get("metric_direction") == "not_applicable"):
                raise FlatRequestOutputError("metric direction applicability drifted")
            identity = json.dumps(
                {field: event.get(field) for field in IDENTITY_FIELDS},
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            if identity in seen:
                raise FlatRequestOutputError("exact event identity duplicate")
            seen.add(identity)
            start_counts[start_id] += 1
            event["window_id"] = window_id
            event["evidence"] = evidence
            projected_events.append(event)
            provenance_rows.append(
                {
                    "segment_id": segment_id,
                    "event_index": event_index,
                    "evidence_start_unit_id": start_id,
                    "evidence_end_unit_id": end_id,
                    "start_char": start_char,
                    "end_char": end_char,
                    "window_id": window_id,
                    "evidence_sha256": sha256_text(evidence),
                }
            )
        if start_counts != receipt_counts:
            raise FlatRequestOutputError("source unit event ownership drifted")
        normalized_rows.append(
            {
                "segment_id": segment_id,
                "status": row["status"],
                "segment_source_context": row["segment_source_context"],
                "no_signal_reason": row["no_signal_reason"],
                "events": projected_events,
            }
        )
        diagnostics.append(
            {
                "segment_id": segment_id,
                "density_stratum": source_row["density_stratum"],
                "source_unit_count": len(units),
                "reviewed_source_unit_count": len(receipts),
                "event_count": len(projected_events),
                "unresolved_count": 0,
            }
        )
    return (
        {"episode_id": source["episode_id"], "segments": normalized_rows},
        {
            "schema_version": CONFIG_VERSION,
            "episode_id": source["episode_id"],
            "events": provenance_rows,
        },
        {
            "schema_version": CONFIG_VERSION,
            "diagnostics": diagnostics,
            "deterministic_projection_only": True,
        },
    )


def _usage_from_sidecar(path: Path) -> dict[str, int]:
    sidecar = _load_json(path, "turn sidecar")
    values = sidecar.get("usage")
    if not isinstance(values, Mapping):
        raise FlatRequestModelLaneError("turn usage is absent")
    usage: dict[str, int] = {}
    for field in USAGE_FIELDS:
        value = values.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise FlatRequestModelLaneError("turn usage is incomplete")
        usage[field] = value
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None
        or usage["cached_input_tokens"] > usage["input_tokens"]
        or usage["reasoning_output_tokens"] > usage["output_tokens"]
        or usage["total_tokens"] != usage["input_tokens"] + usage["output_tokens"]
    ):
        raise FlatRequestModelLaneError("measured sidecar contract failed")
    return usage


def _gate(usage: Mapping[str, int], diagnostics: Mapping[str, Any]) -> dict[str, Any]:
    rows = list(diagnostics["diagnostics"])
    total = PRODUCTION_CONTEXT_TOKENS + int(usage["total_tokens"]) * PRODUCTION_SCALE
    ratio = total / BASELINE_TOTAL_TOKENS
    checks = {
        "one_turn_measured": True,
        "total_tokens_lte_70000": int(usage["total_tokens"]) <= MAX_TOTAL_TOKENS,
        "all_source_units_reviewed": all(
            int(row["source_unit_count"]) == int(row["reviewed_source_unit_count"])
            for row in rows
        ),
        "unresolved_count_0": all(int(row["unresolved_count"]) == 0 for row in rows),
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "exact_identity_duplicates_0": True,
        "event_cap_violations_0": True,
        "semantic_event_count_proxy_not_used": True,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": "pif_flat_request_model_lane_structural_gate_v1",
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "usage": dict(usage),
        "production_amortized_total_tokens": total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "segment_event_counts": {
            str(row["segment_id"]): int(row["event_count"]) for row in rows
        },
        "support_alignment_authorized": not failed,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    turn = frozen["turn"]
    measured: dict[str, int] | None = None
    unknown = 0
    if turn["sidecar"].exists():
        try:
            measured = _usage_from_sidecar(turn["sidecar"])
        except Exception:
            unknown = 1
    elif turn["capacity"].exists() or turn["output"].exists():
        unknown = 1
    semantic = isinstance(exc, (FlatRequestOutputError, FlatRequestArchitectureStop))
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "flat_request_model_lane_structural_or_cost_gate_not_passed"
            if semantic
            else "infrastructure_or_extraction_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": int(measured is not None) + unknown,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "usage": measured or {field: 0 for field in USAGE_FIELDS},
        "unknown_usage_attempt_count": unknown,
        "architecture_strategy_rejected": semantic,
        "support_alignment_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "exact_next_action": (
            "freeze this model/representation lane as rejected; do not repair or retry it"
            if semantic
            else "audit the immutable attempt; do not retry this root"
        ),
    }
    configured._write_stable_time(root / "terminal.json", terminal, "terminal_at")  # noqa: SLF001
    return terminal


async def run_experiment(
    config_path: Path,
    *,
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = config_path.expanduser().resolve().parent
    if (root / "terminal.json").is_file():
        return _load_json(root / "terminal.json", "terminal")
    frozen = freeze_experiment(config_path)
    try:
        verify_frozen(root)
        launch = root / "launch-receipt.json"
        if not launch.exists():
            configured._write_stable_time(  # noqa: SLF001
                launch,
                {
                    "schema_version": "pif_flat_request_model_lane_launch_v1",
                    "launched_at": now_iso(),
                    "semantic_attempt_count": 1,
                    "retry_count": 0,
                    "managed_chatgpt_auth_only": True,
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
        if not turn["sidecar"].exists() and (
            turn["capacity"].exists() or turn["output"].exists()
        ):
            raise FlatRequestModelLaneError("attempt artifact exists without sidecar")
        async with client_factory(frozen["capacity_policy"]) as client:
            if not turn["sidecar"].exists():
                await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
                    base_instructions=turn["base"].read_text(encoding="utf-8"),
                    prompt=turn["prompt"].read_text(encoding="utf-8"),
                    output_schema=_load_json(turn["schema"], "structured output schema"),
                    cwd=PROJECT_ROOT,
                    sidecar_path=turn["sidecar"],
                    output_path=turn["output"],
                    batch_size=2,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=turn["capacity"],
                )
        usage = _usage_from_sidecar(turn["sidecar"])
        if usage["total_tokens"] > MAX_TOTAL_TOKENS:
            raise FlatRequestArchitectureStop("turn exceeded frozen token ceiling")
        output = _load_json(turn["output"], "structured output")
        normalized, provenance, diagnostics = validate_and_project_output(output, frozen)
        _write_immutable(root / "normalized-output.private.json", normalized)
        _write_immutable(root / "evidence-provenance.private.json", provenance)
        _write_immutable(root / "diagnostics.private.json", diagnostics)
        gate = _gate(usage, diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise FlatRequestArchitectureStop("structural or production-cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "flat_request_model_lane_structural_cost_passed",
            "terminal_reason": "flat_request_model_lane_structural_cost_passed_support_alignment_required",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
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
            "exact_next_action": "run the frozen source-support and neutral alignment evaluator",
        }
        configured._write_stable_time(root / "terminal.json", terminal, "terminal_at")  # noqa: SLF001
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "verify", "run"))
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.command == "freeze":
        frozen = freeze_experiment(args.config)
        print(json.dumps({"root": str(frozen["root"]), "frozen": True}, sort_keys=True))
        return 0
    if args.command == "verify":
        frozen = verify_frozen(args.config.expanduser().resolve().parent)
        print(json.dumps({"root": str(frozen["root"]), "verified": True}, sort_keys=True))
        return 0
    terminal = asyncio.run(run_experiment(args.config))
    print(json.dumps(terminal, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
    return 0 if terminal.get("support_alignment_authorized") else 2


if __name__ == "__main__":
    raise SystemExit(main())
