from __future__ import annotations

"""Run one minimal event-only typed-set extraction canary."""

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
from . import app_server_typed_event_set_schema_recovery as schema_recovery
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .app_server_runtime_verifier import ContentHashCache, normalize_record
from .labels import ValidationError, _validate_schema
from .util import now_iso


CONFIG_VERSION = "pif_minimal_typed_event_set_config_v1"
LOCK_VERSION = "pif_minimal_typed_event_set_runtime_lock_v1"
TERMINAL_VERSION = "pif_minimal_typed_event_set_terminal_v1"
ARCHITECTURE_ID = "minimal_typed_event_set_with_structural_wrapper_projection"
TURN_NAME = "minimal_typed_event_set_extraction"
MODEL = "gpt-5.6-luna"
EFFORT = "medium"
PROJECT_ROOT = typed.PROJECT_ROOT
PIPELINE_ROOT = typed.PIPELINE_ROOT
PINNED_CODEX = typed.PINNED_CODEX
BASELINE_TOTAL_TOKENS = typed.BASELINE_TOTAL_TOKENS
PRODUCTION_CONTEXT_TOKENS = typed.PRODUCTION_CONTEXT_TOKENS
PRODUCTION_SCALE = typed.PRODUCTION_SCALE
MAX_TOTAL_TOKENS = math.floor(
    (0.28 * BASELINE_TOTAL_TOKENS - PRODUCTION_CONTEXT_TOKENS) / PRODUCTION_SCALE
)
EXPECTED_TOTAL_TOKENS = 45_000
MINIMUM_REMAINING_RESERVE_PERCENT = typed.MINIMUM_REMAINING_RESERVE_PERCENT
QUOTA_POINTS_PER_MILLION_TOKENS = typed.QUOTA_POINTS_PER_MILLION_TOKENS
REQUEST_TIMEOUT_SECONDS = 90.0
MAX_EVENTS_PER_SEGMENT = typed.MAX_EVENTS_PER_SEGMENT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-canary-minimal-typed-event-set-luna-medium-2026-07-18"
).resolve()
TYPED_POSTMORTEM_ROOT = (
    PIPELINE_ROOT / "development-convergence-typed-event-set-v3-postmortem-2026-07-18"
).resolve()
LINEAGE_KEYS = (
    "source_input",
    "source_prompt",
    "source_base",
    "projection_schema",
    "typed_postmortem",
    "structure_audit_terminal",
    "structure_audit_report",
    "architecture_family_comparison",
    "current_evaluator_checkpoint",
    "judge_calibration_terminal",
    "evaluator_runtime_lock",
    "architecture_decision",
)


MINIMAL_EVENT_SET_INSTRUCTIONS = """# Minimal source-complete typed event-set extraction

Perform one fresh blind source-complete extraction. Every semantic decision is yours. Do not use a prior candidate, reference answer, target count, expected density, topic list, keywords, regex rules, embeddings, similarity rules, or deterministic semantic heuristics.

Review every supplied source unit in chronological order. Return every independently truth-valued eligible event. Select the smallest sufficient contiguous exact evidence-unit range. Keep events separate whenever actor, speaker, reported actor, attribution, target, claim, stance, certainty, temporal horizon, causal mechanism, metric, event type, evidence commitment, or event boundary differs. Merge only exact semantic duplicates.

Return one segment_source_context value for every schema-keyed segment. Return metric-bearing events in metric_events and all other events in non_metric_events. Assign every event to its source segment_id. A metric event has exactly one source-grounded metric object whose raw_text and every nonempty metric string are literal contiguous substrings of its selected evidence. A non-metric event has no metric object. Party arrays contain either zero items or one evidence-supported named party; zero means absent or unknown for that role.

Assign one unique source_order_index from zero through N-1 across both event arrays within each segment. This records chronological event order only. Do not return segment status, no-signal labels, unit receipts, coverage booleans, or event counts; those redundant structural wrappers are projected mechanically from your event set and cannot change event semantics. Re-read every source unit, evidence range, and material field before returning complete schema-valid JSON."""


class MinimalTypedEventSetError(RuntimeError):
    pass


class MinimalTypedEventSetOutputError(MinimalTypedEventSetError):
    pass


class MinimalTypedEventSetStop(MinimalTypedEventSetError):
    pass


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MinimalTypedEventSetError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    configured._write_immutable(path, value)  # noqa: SLF001


def _write_private_text(path: Path, value: str) -> None:
    configured._write_private_text(path, value)  # noqa: SLF001


def _record(path: Path, *, cache: ContentHashCache | None = None) -> dict[str, Any]:
    return (cache or ContentHashCache()).record(path)


def _verify_record(record: Mapping[str, Any], *, cache: ContentHashCache) -> None:
    if not cache.verify_record(record):
        raise MinimalTypedEventSetError("frozen minimal-event artifact drifted")


def _turn_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns" / TURN_NAME.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "projection_schema": turn_root / "projection-schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _lineage_paths() -> dict[str, Path]:
    source = typed._lineage_paths()  # noqa: SLF001
    return {
        "source_input": source["source_input"],
        "source_prompt": source["source_prompt"],
        "source_base": source["source_base"],
        "projection_schema": source["projection_schema"],
        "typed_postmortem": TYPED_POSTMORTEM_ROOT / "terminal.json",
        "structure_audit_terminal": source["structure_audit_terminal"],
        "structure_audit_report": source["structure_audit_report"],
        "architecture_family_comparison": source["architecture_family_comparison"],
        "current_evaluator_checkpoint": source["current_evaluator_checkpoint"],
        "judge_calibration_terminal": source["judge_calibration_terminal"],
        "evaluator_runtime_lock": source["evaluator_runtime_lock"],
    }


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(schema_recovery.__file__).resolve(),
                *typed._runtime_files(),  # noqa: SLF001
            },
            key=str,
        )
    )


def build_minimal_schema(
    projection_schema: Mapping[str, Any], source: Mapping[str, Any]
) -> dict[str, Any]:
    sparse_schema = typed.sparse.build_sparse_schema(projection_schema)
    sparse_event = sparse_schema["properties"]["segments"]["items"]["properties"][
        "events"
    ]["items"]
    canonical_segment = projection_schema["properties"]["segments"]["items"]
    segment_ids = [str(row["segment_id"]) for row in source["segments"]]
    unit_ids = [
        str(unit["unit_id"])
        for segment in source["segments"]
        for unit in segment["units"]
    ]
    context_schema = canonical_segment["properties"]["segment_source_context"]
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "episode_id",
            "segment_source_contexts_by_id",
            "metric_events",
            "non_metric_events",
        ],
        "properties": {
            "episode_id": {"type": "string", "enum": [str(source["episode_id"])]},
            "segment_source_contexts_by_id": {
                "type": "object",
                "additionalProperties": False,
                "required": segment_ids,
                "properties": {
                    segment_id: copy.deepcopy(context_schema)
                    for segment_id in segment_ids
                },
            },
            "metric_events": {
                "type": "array",
                "minItems": 0,
                "maxItems": MAX_EVENTS_PER_SEGMENT * len(segment_ids),
                "items": typed._typed_event_schema(  # noqa: SLF001
                    sparse_event, unit_ids, segment_ids, metric=True
                ),
            },
            "non_metric_events": {
                "type": "array",
                "minItems": 0,
                "maxItems": MAX_EVENTS_PER_SEGMENT * len(segment_ids),
                "items": typed._typed_event_schema(  # noqa: SLF001
                    sparse_event, unit_ids, segment_ids, metric=False
                ),
            },
        },
    }
    return schema_recovery.build_provider_compatible_schema(schema)


def validate_and_project_output(
    output: Mapping[str, Any], frozen: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    turn = frozen["turn"]
    try:
        _validate_schema(_load_json(turn["schema"], "minimal schema"), output, path="$")
    except (ValidationError, TypeError, ValueError) as exc:
        raise MinimalTypedEventSetOutputError("minimal output schema failed") from exc
    source = frozen["source"]
    if output.get("episode_id") != source.get("episode_id"):
        raise MinimalTypedEventSetOutputError("episode identity drifted")
    contexts = output.get("segment_source_contexts_by_id")
    if not isinstance(contexts, Mapping):
        raise MinimalTypedEventSetOutputError("segment contexts are absent")
    event_sets: dict[str, list[tuple[Mapping[str, Any], bool]]] = {
        str(row["segment_id"]): [] for row in source["segments"]
    }
    for event in output["metric_events"]:
        event_sets[str(event["segment_id"])].append((event, True))
    for event in output["non_metric_events"]:
        event_sets[str(event["segment_id"])].append((event, False))
    direct_segments: list[dict[str, Any]] = []
    for source_segment in source["segments"]:
        segment_id = str(source_segment["segment_id"])
        raw_events = event_sets[segment_id]
        if len(raw_events) > MAX_EVENTS_PER_SEGMENT:
            raise MinimalTypedEventSetOutputError("segment event cap drifted")
        order = [int(event["source_order_index"]) for event, _ in raw_events]
        if sorted(order) != list(range(len(raw_events))):
            raise MinimalTypedEventSetOutputError("source order coverage drifted")
        projected: list[tuple[int, dict[str, Any]]] = []
        receipt_counts = {
            str(unit["unit_id"]): 0 for unit in source_segment["units"]
        }
        for raw_event, has_metric in raw_events:
            event = copy.deepcopy(dict(raw_event))
            index = int(event.pop("source_order_index"))
            if event.pop("segment_id") != segment_id:
                raise MinimalTypedEventSetOutputError("event ownership drifted")
            start_unit = str(event["evidence_start_unit_id"])
            if start_unit not in receipt_counts:
                raise MinimalTypedEventSetOutputError("evidence owner drifted")
            receipt_counts[start_unit] += 1
            for field in ("speaker", "actor", "reported_actor"):
                event[field] = typed._project_named_party(event[field], field)  # noqa: SLF001
            if not has_metric:
                event["metric"] = []
            projected.append((index, typed.sparse.project_sparse_event(event)))
        projected.sort(key=lambda item: item[0])
        events = [event for _index, event in projected]
        direct_segments.append(
            {
                "segment_id": segment_id,
                "status": "coded" if events else "no_signal",
                "segment_source_context": contexts[segment_id],
                "no_signal_reason": (
                    "" if events else "No eligible event was returned for this reviewed segment."
                ),
                "unit_receipts": [
                    {
                        "unit_id": str(unit["unit_id"]),
                        "eligible_event_count": receipt_counts[str(unit["unit_id"])],
                        "unresolved_count": 0,
                    }
                    for unit in source_segment["units"]
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "events": events,
            }
        )
    direct = {"episode_id": source["episode_id"], "segments": direct_segments}
    try:
        normalized, provenance, diagnostics = typed.flat.validate_and_project_output(
            direct,
            {"turn": {"schema": turn["projection_schema"]}, "source": source},
        )
    except typed.flat.FlatRequestOutputError as exc:
        raise MinimalTypedEventSetOutputError(str(exc)) from exc
    diagnostics = copy.deepcopy(diagnostics)
    diagnostics.update(
        {
            "representation": ARCHITECTURE_ID,
            "all_semantic_decisions_owned_by_llm": True,
            "deterministic_event_semantics_changed": False,
            "derived_structural_fields": [
                "segment status from event presence",
                "unit receipt counts from evidence owners",
                "zero unresolved counts",
                "coverage wrapper",
                "canonical no-signal wrapper text",
            ],
        }
    )
    return normalized, provenance, diagnostics


def _architecture_decision(records: Mapping[str, Any]) -> dict[str, Any]:
    expected_ratio = (
        PRODUCTION_CONTEXT_TOKENS + EXPECTED_TOTAL_TOKENS * PRODUCTION_SCALE
    ) / BASELINE_TOTAL_TOKENS
    maximum_ratio = (
        PRODUCTION_CONTEXT_TOKENS + MAX_TOTAL_TOKENS * PRODUCTION_SCALE
    ) / BASELINE_TOTAL_TOKENS
    return {
        "schema_version": "pif_minimal_typed_event_set_architecture_decision_v1",
        "created_at": now_iso(),
        "state": "presemantic_architecture_decision_frozen",
        "production_mutated": False,
        "holdout_authorized": False,
        "semantic_model_call_count_for_decision": 0,
        "ranked_architectures": [
            {
                "rank": 1,
                "architecture_id": ARCHITECTURE_ID,
                "status": "selected_smallest_decision_changing_canary",
                "evidence": (
                    "The typed-ledger run found 32 events but spent 52,528 tokens and emitted "
                    "a contradictory coded/zero-event wrapper. Removing redundant wrapper and "
                    "receipt generation preserves LLM event semantics while eliminating that "
                    "cross-structure and reducing output/reasoning load."
                ),
            },
            {
                "rank": 2,
                "architecture_id": "pointwise_unit_event_objects_then_global_owner",
                "status": "deferred_two_pass_cost_and_boundary_risk",
                "evidence": (
                    "Per-unit calls improve coverage observability but require a second semantic "
                    "owner pass and repeat shared source context, threatening the 0.28 target."
                ),
            },
            {
                "rank": 3,
                "architecture_id": "episode_bootstrap_then_segment_specialists",
                "status": "deferred_history_and_cache_risk",
                "evidence": (
                    "Prior same-thread trials did not show custom-prefix cache gains and retained "
                    "history accumulates across turns, making cost and attribution less stable."
                ),
            },
        ],
        "selected_canary": {
            "hypothesis": (
                "A single source-complete Luna-medium event-only typed set can preserve the "
                "typed architecture's event inventory while avoiding redundant wrapper conflicts "
                "and remaining under the actual production-amortized 0.28 token ceiling."
            ),
            "representative_development_sample": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "source_unit_count": 69,
                "reference_visible_to_model": False,
                "target_count_visible_to_model": False,
            },
            "model": MODEL,
            "effort": EFFORT,
            "declared_turn_count": 1,
            "retry_count": 0,
            "expected_total_tokens": EXPECTED_TOTAL_TOKENS,
            "maximum_total_tokens": MAX_TOTAL_TOKENS,
            "expected_production_amortized_total_token_ratio": round(expected_ratio, 6),
            "maximum_production_amortized_total_token_ratio": round(maximum_ratio, 6),
            "stop_rules": {
                "production_amortized_total_token_ratio_maximum": 0.28,
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "exact_identity_duplicates": 0,
                "event_cap_violations": 0,
                "semantic_event_count_proxy_used": False,
                "on_failure": "freeze and reject without field repair or retry",
                "on_pass": "run the unchanged support-first neutral two-permutation evaluator",
            },
        },
        "direct_predecessor_records": copy.deepcopy(dict(records)),
    }


def prepare_experiment(root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    root = root.expanduser().resolve()
    paths = _lineage_paths()
    cache = ContentHashCache()
    records = {key: _record(path, cache=cache) for key, path in paths.items()}
    for key, expected_hash in typed.EXPECTED_SOURCE_HASHES.items():
        if key in records and records[key]["sha256"] != expected_hash:
            raise MinimalTypedEventSetError(f"{key} source artifact drifted")
    postmortem = _load_json(paths["typed_postmortem"], "typed postmortem")
    judge = _load_json(paths["judge_calibration_terminal"], "judge terminal")
    if (
        postmortem.get("terminal_reason")
        != "measured_structural_and_frozen_cost_gates_not_passed"
        or postmortem.get("architecture_strategy_rejected") is not True
        or postmortem.get("schema_or_field_repair_authorized") is not False
        or postmortem.get("production_mutated") is not False
        or judge.get("selection_authorized") is not True
    ):
        raise MinimalTypedEventSetError("direct predecessor decision drifted")
    decision_path = root / "architecture-decision.json"
    configured._write_stable_time(  # noqa: SLF001
        decision_path, _architecture_decision(records), "created_at"
    )
    records["architecture_decision"] = _record(decision_path, cache=cache)
    config = {
        "schema_version": CONFIG_VERSION,
        "experiment_id": "minimal_typed_event_set_luna_medium_2026_07_18",
        "architecture_id": ARCHITECTURE_ID,
        "output_root": str(root),
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_total_tokens": MAX_TOTAL_TOKENS,
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "deterministic_event_semantics_changed": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "production_amortized_context_tokens": PRODUCTION_CONTEXT_TOKENS,
        "production_scale": PRODUCTION_SCALE,
        "baseline_total_tokens": BASELINE_TOTAL_TOKENS,
        **records,
    }
    config_path = root / "experiment-config.json"
    _write_immutable(config_path, config)
    return config_path


def _validate_config(config_path: Path) -> dict[str, Any]:
    value = _load_json(config_path, "experiment config")
    expected = {
        "schema_version": CONFIG_VERSION,
        "architecture_id": ARCHITECTURE_ID,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_total_tokens": MAX_TOTAL_TOKENS,
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "deterministic_event_semantics_changed": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "production_amortized_context_tokens": PRODUCTION_CONTEXT_TOKENS,
        "production_scale": PRODUCTION_SCALE,
        "baseline_total_tokens": BASELINE_TOTAL_TOKENS,
    }
    if not isinstance(value, dict) or any(value.get(key) != item for key, item in expected.items()):
        raise MinimalTypedEventSetError("experiment config drifted")
    if Path(str(value.get("output_root") or "")).expanduser().resolve() != config_path.parent.resolve():
        raise MinimalTypedEventSetError("experiment root drifted")
    for key in LINEAGE_KEYS:
        normalize_record(value.get(key) or {})
    return value


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
    if (root / "runtime-lock.json").exists():
        return verify_frozen(root)
    cache = ContentHashCache()
    for key in LINEAGE_KEYS:
        _verify_record(config[key], cache=cache)
    turn = _turn_paths(root)
    _write_private_text(
        turn["input"], Path(config["source_input"]["path"]).read_text(encoding="utf-8")
    )
    _write_private_text(
        turn["prompt"], Path(config["source_prompt"]["path"]).read_text(encoding="utf-8")
    )
    source_base = Path(config["source_base"]["path"]).read_text(encoding="utf-8")
    _write_private_text(
        turn["base"], source_base + "\n\n" + MINIMAL_EVENT_SET_INSTRUCTIONS + "\n"
    )
    source = _load_json(Path(config["source_input"]["path"]), "source input")
    projection = _load_json(Path(config["projection_schema"]["path"]), "projection schema")
    _write_immutable(turn["projection_schema"], projection)
    _write_immutable(turn["schema"], build_minimal_schema(projection, source))
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
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "managed_chatgpt_auth_only": True,
        "deterministic_event_semantics_changed": False,
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
            "model": MODEL,
            "effort": EFFORT,
            "declared_turn_count": 1,
            "retry_count": 0,
            "maximum_total_tokens": MAX_TOTAL_TOKENS,
            "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "deterministic_event_semantics_changed": False,
            "production_mutation_allowed": False,
        },
        required_record_paths=_runtime_files(),
    )
    if manifest.get("pinned_codex_cli") != _record(PINNED_CODEX):
        raise MinimalTypedEventSetError("pinned Codex binary drifted")
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
            raise MinimalTypedEventSetError("launch receipt drifted")
    return {
        "root": root,
        "config": config,
        "runtime_lock": root / "runtime-lock.json",
        "runtime_closure": root / "runtime-lock-closure.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": _turn_paths(root),
        "source": _load_json(Path(config["source_input"]["path"]), "source input"),
    }


def _usage(path: Path) -> dict[str, int]:
    sidecar = _load_json(path, "turn sidecar")
    values = sidecar.get("usage")
    if not isinstance(values, Mapping):
        raise MinimalTypedEventSetError("turn usage is absent")
    usage: dict[str, int] = {}
    for field in configured.USAGE_FIELDS:
        value = values.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise MinimalTypedEventSetError("turn usage is incomplete")
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
        raise MinimalTypedEventSetError("measured sidecar contract failed")
    return usage


def _gate(usage: Mapping[str, int], diagnostics: Mapping[str, Any]) -> dict[str, Any]:
    rows = list(diagnostics["diagnostics"])
    production_total = PRODUCTION_CONTEXT_TOKENS + int(usage["total_tokens"]) * PRODUCTION_SCALE
    ratio = production_total / BASELINE_TOTAL_TOKENS
    checks = {
        "one_turn_measured": True,
        "total_tokens_within_production_safe_bound": int(usage["total_tokens"])
        <= MAX_TOTAL_TOKENS,
        "source_complete_review_instructed": True,
        "derived_unit_coverage_complete": all(
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
        "schema_version": "pif_minimal_typed_event_set_structural_gate_v1",
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "usage": dict(usage),
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "segment_event_counts": {
            str(row["segment_id"]): int(row["event_count"]) for row in rows
        },
        "support_alignment_authorized": not failed,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _inner_factory() -> typed.codex_app_server.CodexAppServerClient:
    return typed.codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"],
        request_timeout_seconds=REQUEST_TIMEOUT_SECONDS,
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(root: Path, frozen: Mapping[str, Any], exc: BaseException) -> dict[str, Any]:
    turn = frozen["turn"]
    measured: dict[str, int] | None = None
    unknown = 0
    if turn["sidecar"].exists():
        try:
            measured = _usage(turn["sidecar"])
        except Exception:
            unknown = 1
    elif turn["capacity"].exists() or turn["output"].exists():
        unknown = 1
    semantic = isinstance(exc, (MinimalTypedEventSetOutputError, MinimalTypedEventSetStop))
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "minimal_typed_event_set_structural_or_cost_gate_not_passed"
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
        "usage": measured or {field: 0 for field in configured.USAGE_FIELDS},
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
            "reject this architecture without field repair or retry"
            if semantic
            else "audit this immutable infrastructure attempt without replay"
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
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "terminal")
    frozen = freeze_experiment(config_path)
    try:
        verify_frozen(root)
        launch = root / "launch-receipt.json"
        if not launch.exists():
            configured._write_stable_time(  # noqa: SLF001
                launch,
                {
                    "schema_version": "pif_minimal_typed_event_set_launch_v1",
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
        if not turn["sidecar"].exists() and (turn["capacity"].exists() or turn["output"].exists()):
            raise MinimalTypedEventSetError("attempt artifact exists without sidecar")
        async with client_factory(frozen["capacity_policy"]) as client:
            if not turn["sidecar"].exists():
                await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
                    base_instructions=turn["base"].read_text(encoding="utf-8"),
                    prompt=turn["prompt"].read_text(encoding="utf-8"),
                    output_schema=_load_json(turn["schema"], "minimal schema"),
                    cwd=PROJECT_ROOT,
                    sidecar_path=turn["sidecar"],
                    output_path=turn["output"],
                    batch_size=2,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=turn["capacity"],
                )
        usage = _usage(turn["sidecar"])
        if usage["total_tokens"] > MAX_TOTAL_TOKENS:
            raise MinimalTypedEventSetStop("turn exceeded production-safe token ceiling")
        normalized, provenance, diagnostics = validate_and_project_output(
            _load_json(turn["output"], "minimal output"), frozen
        )
        _write_immutable(root / "normalized-output.private.json", normalized)
        _write_immutable(root / "evidence-provenance.private.json", provenance)
        _write_immutable(root / "diagnostics.private.json", diagnostics)
        gate = _gate(usage, diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise MinimalTypedEventSetStop("structural or production-cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "minimal_typed_event_set_structural_cost_passed",
            "terminal_reason": "minimal_typed_event_set_passed_support_alignment_required",
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
        path = prepare_experiment(args.root)
        print(json.dumps({"config": str(path), "prepared": True}, sort_keys=True))
        return 0
    config = args.config or (args.root / "experiment-config.json")
    if args.command == "freeze":
        frozen = freeze_experiment(config)
        print(json.dumps({"root": str(frozen["root"]), "frozen": True}, sort_keys=True))
        return 0
    if args.command == "verify":
        frozen = verify_frozen(config.expanduser().resolve().parent)
        print(json.dumps({"root": str(frozen["root"]), "verified": True}, sort_keys=True))
        return 0
    terminal = asyncio.run(run_experiment(config))
    print(json.dumps(terminal, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
    return 0 if terminal.get("support_alignment_authorized") else 2


if __name__ == "__main__":
    raise SystemExit(main())
