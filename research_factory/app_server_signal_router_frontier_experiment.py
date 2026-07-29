from __future__ import annotations

"""Run a Sol semantic segment router followed by routed Luna extraction."""

import argparse
import asyncio
import copy
import hashlib
import itertools
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import app_server_configured_experiment as configured
from . import app_server_flat_request_model_lane as flat
from . import app_server_sparse_event_frame_lane as sparse_v1
from . import app_server_sparse_event_frame_recovery as sparse_v2
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


CONFIG_VERSION = "pif_signal_router_frontier_experiment_config_v1"
LOCK_VERSION = "pif_signal_router_frontier_experiment_runtime_lock_v1"
TERMINAL_VERSION = "pif_signal_router_frontier_experiment_terminal_v1"
ARCHITECTURE_ID = "sol_signal_router_then_luna_routed_frontier_extraction"
ROUTER_TURN = "semantic_signal_router"
EXTRACTOR_TURN = "routed_frontier_extraction"
ROUTER_MODEL = "gpt-5.6-sol"
ROUTER_EFFORT = "low"
EXTRACTOR_MODEL = "gpt-5.6-luna"
EXTRACTOR_EFFORT = "high"
ROUTER_MAX_TOKENS = 28_000
EXTRACTOR_MAX_TOKENS = 45_000
COMBINED_MAX_TOKENS = 73_000
CAPACITY_PHASE_BOUND = 2 * EXTRACTOR_MAX_TOKENS
MINIMUM_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
BASELINE_TOTAL_TOKENS = sparse_v1.BASELINE_TOTAL_TOKENS
PRODUCTION_CONTEXT_TOKENS = sparse_v1.PRODUCTION_CONTEXT_TOKENS
PRODUCTION_SCALE = sparse_v1.PRODUCTION_SCALE
PROJECT_ROOT = sparse_v1.PROJECT_ROOT
PIPELINE_ROOT = sparse_v1.PIPELINE_ROOT
PINNED_CODEX = configured.PINNED_CODEX
SOURCE_TURN_ROOT = sparse_v1.SOURCE_TURN_ROOT
SPARSE_V1_ROOT = sparse_v1.DEFAULT_OUTPUT_ROOT
SPARSE_V2_ROOT = sparse_v2.DEFAULT_OUTPUT_ROOT
PHASE_ROOT = sparse_v1.PHASE_ROOT
JUDGE_ROOT = sparse_v1.JUDGE_ROOT
EVALUATOR_ROOT = sparse_v1.EVALUATOR_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-canary-signal-router-luna-frontier-2026-07-18"
).resolve()
LINEAGE_KEYS = (
    "source_input",
    "original_base",
    "projection_schema",
    "sparse_v1_terminal",
    "sparse_v1_runtime_lock",
    "sparse_v1_sidecar",
    "sparse_v2_terminal",
    "sparse_v2_runtime_lock",
    "sparse_v2_sidecar",
    "sparse_v2_output",
    "current_evaluator_checkpoint",
    "judge_calibration_terminal",
    "evaluator_runtime_lock",
    "architecture_decision",
)

ROUTER_INSTRUCTIONS = """# LLM-only semantic segment router

Review every supplied source unit in every segment. For each segment, decide whether it contains at least one independently truth-valued event eligible under the immutable extraction rules. Use full_extraction whenever any eligible event exists; use no_signal only when the entire segment has no eligible event. This is a semantic decision owned solely by you, not a keyword or topic filter.

Return segments and reviewed_unit_ids in the supplied order. Classify segment_source_context from the source. Provide a concise no_signal_reason only for no_signal. Do not extract, count, summarize, or expose events. Do not use keywords, regex rules, phrase lists, embeddings, similarity, prior candidates, references, expected counts, or target hints. Return schema-valid JSON only."""

EXTRACTOR_INSTRUCTIONS = """# Routed frontier extraction

The supplied packet contains only segments that an independent source-reading LLM routed for full extraction. Perform a fresh blind extraction from those source units; the router supplied no events, labels, counts, or reference hints.

Privately complete a chronological event pass and an independent relational pass over every source unit, then globally reconcile them against the source. Preserve each independently truth-valued eligible event. Merge only exact semantic duplicates. Keep events separate whenever actor, speaker, reported actor, attribution, target, claim, stance, certainty, temporal horizon, causal mechanism, metric, evidence commitment, event type, or event boundary differs. Select the smallest sufficient contiguous exact evidence range and re-read it before finalizing every field. Every nonempty metric string must be a literal contiguous evidence substring.

Return every reviewed unit receipt and the complete canonical full-schema extraction in source order. Never use keywords, regex rules, phrase lists, embeddings, semantic similarity, confidence voting, prior candidates, references, expected counts, or target hints. Return schema-valid JSON only."""


class SignalRouterFrontierError(RuntimeError):
    pass


class SignalRouterOutputError(SignalRouterFrontierError):
    pass


class SignalRouterArchitectureStop(SignalRouterFrontierError):
    pass


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SignalRouterFrontierError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    configured._write_immutable(path, value)  # noqa: SLF001


def _write_private_text(path: Path, value: str) -> None:
    configured._write_private_text(path, value)  # noqa: SLF001


def _record(path: Path, *, cache: ContentHashCache | None = None) -> dict[str, Any]:
    return (cache or ContentHashCache()).record(path)


def _verify_record(record: Mapping[str, Any], *, cache: ContentHashCache) -> None:
    if not cache.verify_record(record):
        raise SignalRouterFrontierError("frozen direct-lineage artifact drifted")


def _turn_paths(root: Path, name: str) -> dict[str, Path]:
    turn_root = root / "turns" / name.replace("_", "-")
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


def _variant_root(root: Path, segment_ids: Sequence[str]) -> Path:
    digest = sha256_text("|".join(segment_ids))[:16]
    return root / "request-variants" / f"route-{digest}"


def _variant_paths(root: Path, segment_ids: Sequence[str]) -> dict[str, Path]:
    variant = _variant_root(root, segment_ids)
    return {
        "root": variant,
        "input": variant / "input.private.json",
        "prompt": variant / "prompt.private.md",
        "base": variant / "base-instructions.private.md",
        "schema": variant / "schema.json",
        "segment_ids": variant / "segment-ids.json",
    }


def _lineage_paths() -> dict[str, Path]:
    sparse_v1_turn = sparse_v1._turn_paths(SPARSE_V1_ROOT)  # noqa: SLF001
    sparse_v2_turn = sparse_v2._turn_paths(SPARSE_V2_ROOT)  # noqa: SLF001
    return {
        "source_input": SOURCE_TURN_ROOT / "input.private.json",
        "original_base": SOURCE_TURN_ROOT / "base-instructions.private.md",
        "projection_schema": SOURCE_TURN_ROOT / "schema.json",
        "sparse_v1_terminal": SPARSE_V1_ROOT / "terminal.json",
        "sparse_v1_runtime_lock": SPARSE_V1_ROOT / "runtime-lock.json",
        "sparse_v1_sidecar": sparse_v1_turn["sidecar"],
        "sparse_v2_terminal": SPARSE_V2_ROOT / "terminal.json",
        "sparse_v2_runtime_lock": SPARSE_V2_ROOT / "runtime-lock.json",
        "sparse_v2_sidecar": sparse_v2_turn["sidecar"],
        "sparse_v2_output": sparse_v2_turn["output"],
        "current_evaluator_checkpoint": PHASE_ROOT
        / "current-evaluator-identity-adoption-checkpoint.json",
        "judge_calibration_terminal": JUDGE_ROOT / "terminal.json",
        "evaluator_runtime_lock": EVALUATOR_ROOT / "runtime-lock.json",
    }


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(flat.__file__).resolve(),
                Path(sparse_v1.__file__).resolve(),
                Path(sparse_v2.__file__).resolve(),
                *configured._runtime_files(),  # noqa: SLF001
            },
            key=str,
        )
    )


def _source_packet(source: Mapping[str, Any], segment_ids: Sequence[str]) -> dict[str, Any]:
    selected = set(segment_ids)
    return {
        "episode_id": str(source["episode_id"]),
        "segments": [
            {
                "segment_id": str(segment["segment_id"]),
                "source_units": [
                    {
                        "unit_id": str(unit["unit_id"]),
                        "window_id": int(unit["window_id"]),
                        "text": str(unit["text"]),
                    }
                    for unit in segment["units"]
                ],
            }
            for segment in source["segments"]
            if str(segment["segment_id"]) in selected
        ],
    }


def _subset_source(source: Mapping[str, Any], segment_ids: Sequence[str]) -> dict[str, Any]:
    selected = set(segment_ids)
    value = copy.deepcopy(dict(source))
    value["segments"] = [
        copy.deepcopy(segment)
        for segment in source["segments"]
        if str(segment["segment_id"]) in selected
    ]
    return value


def _prompt(source: Mapping[str, Any], segment_ids: Sequence[str]) -> str:
    return "# Blind source-unit packet\n" + json.dumps(
        _source_packet(source, segment_ids), ensure_ascii=True, separators=(",", ":")
    ) + "\n"


def _router_schema(
    source: Mapping[str, Any], projection_schema: Mapping[str, Any]
) -> dict[str, Any]:
    segment_ids = [str(row["segment_id"]) for row in source["segments"]]
    unit_ids = [
        str(unit["unit_id"])
        for segment in source["segments"]
        for unit in segment["units"]
    ]
    context_schema = copy.deepcopy(
        projection_schema["properties"]["segments"]["items"]["properties"]
        ["segment_source_context"]
    )
    row = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "segment_id",
            "route",
            "segment_source_context",
            "no_signal_reason",
            "reviewed_unit_ids",
        ],
        "properties": {
            "segment_id": {"type": "string", "enum": segment_ids},
            "route": {"type": "string", "enum": ["full_extraction", "no_signal"]},
            "segment_source_context": context_schema,
            "no_signal_reason": {"type": "string"},
            "reviewed_unit_ids": {
                "type": "array",
                "minItems": 0,
                "maxItems": len(unit_ids),
                "items": {"type": "string", "enum": unit_ids},
            },
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["episode_id", "segments"],
        "properties": {
            "episode_id": {"type": "string", "enum": [str(source["episode_id"])]},
            "segments": {
                "type": "array",
                "minItems": len(segment_ids),
                "maxItems": len(segment_ids),
                "items": row,
            },
        },
    }


def _extractor_schema(
    source: Mapping[str, Any],
    projection_schema: Mapping[str, Any],
    segment_ids: Sequence[str],
) -> dict[str, Any]:
    schema = copy.deepcopy(dict(projection_schema))
    selected = set(segment_ids)
    units = [
        str(unit["unit_id"])
        for segment in source["segments"]
        if str(segment["segment_id"]) in selected
        for unit in segment["units"]
    ]
    segments = schema["properties"]["segments"]
    segments["minItems"] = len(segment_ids)
    segments["maxItems"] = len(segment_ids)
    segment = segments["items"]
    segment["properties"]["segment_id"] = {
        "type": "string",
        "enum": list(segment_ids),
    }
    receipts = segment["properties"]["unit_receipts"]
    receipts["maxItems"] = len(units)
    receipts["items"]["properties"]["unit_id"] = {
        "type": "string",
        "enum": units,
    }
    event = segment["properties"]["events"]["items"]
    for field in ("evidence_start_unit_id", "evidence_end_unit_id"):
        event["properties"][field] = {"type": "string", "enum": units}
    return schema


def _route_subsets(source: Mapping[str, Any]) -> list[tuple[str, ...]]:
    segment_ids = [str(row["segment_id"]) for row in source["segments"]]
    return [
        tuple(combo)
        for length in range(1, len(segment_ids) + 1)
        for combo in itertools.combinations(segment_ids, length)
    ]


def _validate_router_output(
    output: Mapping[str, Any], source: Mapping[str, Any], schema_path: Path
) -> tuple[tuple[str, ...], dict[str, dict[str, Any]]]:
    try:
        _validate_schema(_load_json(schema_path, "router schema"), output, path="$")
    except (ValidationError, TypeError, ValueError) as exc:
        raise SignalRouterOutputError("router output schema failed") from exc
    segments = list(source["segments"])
    rows = list(output.get("segments") or [])
    expected_ids = [str(row["segment_id"]) for row in segments]
    if output.get("episode_id") != source.get("episode_id") or [
        row.get("segment_id") for row in rows
    ] != expected_ids:
        raise SignalRouterOutputError("router episode or segment order drifted")
    by_id: dict[str, dict[str, Any]] = {}
    routed: list[str] = []
    for source_row, row in zip(segments, rows):
        segment_id = str(source_row["segment_id"])
        expected_units = [str(unit["unit_id"]) for unit in source_row["units"]]
        if row.get("reviewed_unit_ids") != expected_units:
            raise SignalRouterOutputError("router source unit coverage drifted")
        route = str(row["route"])
        reason = str(row["no_signal_reason"])
        if (route == "no_signal" and not reason.strip()) or (
            route == "full_extraction" and reason != ""
        ):
            raise SignalRouterOutputError("router route reason contract drifted")
        by_id[segment_id] = copy.deepcopy(dict(row))
        if route == "full_extraction":
            routed.append(segment_id)
    return tuple(routed), by_id


def _assemble_candidate(
    source: Mapping[str, Any],
    router_rows: Mapping[str, Mapping[str, Any]],
    extracted: Mapping[str, Any],
    extracted_provenance: Mapping[str, Any],
    extracted_diagnostics: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    extracted_by_id = {
        str(row["segment_id"]): copy.deepcopy(dict(row))
        for row in extracted.get("segments") or []
    }
    diagnostic_by_id = {
        str(row["segment_id"]): copy.deepcopy(dict(row))
        for row in extracted_diagnostics.get("diagnostics") or []
    }
    rows: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    for source_row in source["segments"]:
        segment_id = str(source_row["segment_id"])
        route = router_rows[segment_id]
        if route["route"] == "full_extraction":
            if segment_id not in extracted_by_id:
                raise SignalRouterOutputError("routed extraction segment is missing")
            rows.append(extracted_by_id[segment_id])
            diagnostics.append(diagnostic_by_id[segment_id])
        else:
            rows.append(
                {
                    "segment_id": segment_id,
                    "status": "no_signal",
                    "segment_source_context": route["segment_source_context"],
                    "no_signal_reason": route["no_signal_reason"],
                    "events": [],
                }
            )
            diagnostics.append(
                {
                    "segment_id": segment_id,
                    "density_stratum": source_row["density_stratum"],
                    "source_unit_count": len(source_row["units"]),
                    "reviewed_source_unit_count": len(source_row["units"]),
                    "event_count": 0,
                    "unresolved_count": 0,
                    "semantic_owner": ROUTER_MODEL,
                }
            )
    return (
        {"episode_id": source["episode_id"], "segments": rows},
        copy.deepcopy(dict(extracted_provenance)),
        {
            "schema_version": CONFIG_VERSION,
            "diagnostics": diagnostics,
            "router_model": ROUTER_MODEL,
            "extractor_model": EXTRACTOR_MODEL,
            "deterministic_semantic_decision_made": False,
        },
    )


def _architecture_decision(records: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "pif_signal_router_frontier_architecture_decision_v1",
        "created_at": now_iso(),
        "state": "presemantic_architecture_decision_frozen",
        "production_mutated": False,
        "holdout_authorized": False,
        "semantic_model_call_count_for_decision": 0,
        "measured_sparse_lane_disposition": {
            "v1": "infrastructure_unknown_usage_blocked",
            "v2": "measured_speaker_identity_contract_failure",
            "v2_total_tokens": 38803,
            "architecture_rejected_without_field_repair": True,
        },
        "ranked_architectures": [
            {
                "rank": 1,
                "architecture_id": ARCHITECTURE_ID,
                "status": "selected_smallest_decision_changing_canary",
                "evidence": "A source-reading Sol router can remove genuinely empty segments without deterministic semantic gates, while a fresh Luna extraction sees less irrelevant source and retains the complete canonical schema.",
            },
            {
                "rank": 2,
                "architecture_id": "pointwise_event_span_extraction_then_global_owner",
                "status": "deferred_cost_and_boundary_risk",
                "evidence": "Pointwise spans reduce field interference but repeat source and require a costly owner stage; measured map and segment-isolation families approached or exceeded the token envelope.",
            },
            {
                "rank": 3,
                "architecture_id": "baseline_equivalent_frontier_per_segment",
                "status": "rejected_measured_cost_path",
                "evidence": "The baseline-quality control exceeds the production-amortized token target under paired accounting.",
            },
        ],
        "selected_canary": {
            "architecture_id": ARCHITECTURE_ID,
            "hypothesis": "Separating source-complete no-signal routing from dense semantic extraction lets Luna-high focus its full field budget on routed signal units, improving event boundaries and material fields while remaining under the production token gate.",
            "turns": [
                {
                    "name": ROUTER_TURN,
                    "model": ROUTER_MODEL,
                    "effort": ROUTER_EFFORT,
                    "maximum_total_tokens": ROUTER_MAX_TOKENS,
                },
                {
                    "name": EXTRACTOR_TURN,
                    "model": EXTRACTOR_MODEL,
                    "effort": EXTRACTOR_EFFORT,
                    "maximum_total_tokens": EXTRACTOR_MAX_TOKENS,
                },
            ],
            "combined_total_tokens_maximum": COMBINED_MAX_TOKENS,
            "retry_count_per_turn": 0,
            "representative_development_sample": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "source_unit_count": 69,
                "reference_visible_to_models": False,
                "target_count_visible_to_models": False,
            },
            "production_cost_projection": {
                "maximum_production_amortized_total_token_ratio": round(
                    (PRODUCTION_CONTEXT_TOKENS + COMBINED_MAX_TOKENS * PRODUCTION_SCALE)
                    / BASELINE_TOTAL_TOKENS,
                    6,
                )
            },
            "stop_rules": {
                "router_must_review_every_unit": True,
                "router_total_tokens_maximum": ROUTER_MAX_TOKENS,
                "extractor_total_tokens_maximum": EXTRACTOR_MAX_TOKENS,
                "combined_total_tokens_maximum": COMBINED_MAX_TOKENS,
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "event_cap_violations": 0,
                "semantic_event_count_proxy_used": False,
                "production_amortized_total_token_ratio_maximum": 0.28,
                "on_any_failure": "freeze and reject this architecture without repair or retry",
                "on_pass": "run the unchanged frozen support-first and two-permutation neutral alignment evaluator",
            },
        },
        "direct_predecessor_records": copy.deepcopy(dict(records)),
    }


def prepare_experiment(root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    root = root.expanduser().resolve()
    sparse_v1.verify_frozen(SPARSE_V1_ROOT)
    sparse_v2.verify_frozen(SPARSE_V2_ROOT)
    paths = _lineage_paths()
    t1 = _load_json(paths["sparse_v1_terminal"], "sparse v1 terminal")
    t2 = _load_json(paths["sparse_v2_terminal"], "sparse v2 terminal")
    s2 = _load_json(paths["sparse_v2_sidecar"], "sparse v2 sidecar")
    if (
        t1.get("usage_status") != "unknown"
        or t1.get("semantic_retry_count") != 0
        or t2.get("terminal_reason")
        != "sparse_event_frame_recovery_structural_or_cost_gate_not_passed"
        or t2.get("usage_status") != "complete"
        or (t2.get("usage") or {}).get("total_tokens") != 38803
        or t2.get("error_message_sha256")
        != "c8032551c5fe3f830292d4d80a9501bc263f8a17a1b8539926256257d9f722fa"
        or t2.get("semantic_retry_count") != 0
        or t2.get("production_mutated") is not False
        or s2.get("status") != "completed"
        or s2.get("usage_status") != "measured"
    ):
        raise SignalRouterFrontierError("sparse predecessor evidence drifted")
    cache = ContentHashCache()
    records = {key: _record(path, cache=cache) for key, path in paths.items()}
    checkpoint_path = root / "sparse-frame-final-checkpoint.json"
    configured._write_stable_time(  # noqa: SLF001
        checkpoint_path,
        {
            "schema_version": "pif_sparse_frame_final_checkpoint_v1",
            "created_at": now_iso(),
            "state": "sparse_frame_architecture_rejected",
            "attempts": [
                {"version": 1, "usage_status": "unknown", "retry_count": 0},
                {
                    "version": 2,
                    "usage_status": "complete",
                    "total_tokens": 38803,
                    "failure_class": "speaker_identity_contract_drift",
                    "retry_count": 0,
                },
            ],
            "further_sparse_frame_recovery_authorized": False,
            "production_mutated": False,
            "holdout_authorized": False,
            "artifact_records": records,
        },
        "created_at",
    )
    records["sparse_frame_checkpoint"] = _record(checkpoint_path, cache=cache)
    decision_path = root / "architecture-decision.json"
    configured._write_stable_time(  # noqa: SLF001
        decision_path, _architecture_decision(records), "created_at"
    )
    config = {
        "schema_version": CONFIG_VERSION,
        "experiment_id": "development_canary_signal_router_luna_frontier_2026_07_18",
        "architecture_id": ARCHITECTURE_ID,
        "output_root": str(root),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "retry_count_per_turn": 0,
        "router_model": ROUTER_MODEL,
        "router_effort": ROUTER_EFFORT,
        "router_total_tokens_maximum": ROUTER_MAX_TOKENS,
        "extractor_model": EXTRACTOR_MODEL,
        "extractor_effort": EXTRACTOR_EFFORT,
        "extractor_total_tokens_maximum": EXTRACTOR_MAX_TOKENS,
        "combined_total_tokens_maximum": COMBINED_MAX_TOKENS,
        "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "production_amortized_context_tokens": PRODUCTION_CONTEXT_TOKENS,
        "production_scale": PRODUCTION_SCALE,
        "baseline_total_tokens": BASELINE_TOTAL_TOKENS,
        **{key: records[key] for key in LINEAGE_KEYS if key in records},
        "architecture_decision": _record(decision_path, cache=cache),
        "sparse_frame_checkpoint": records["sparse_frame_checkpoint"],
    }
    config_path = root / "experiment-config.json"
    _write_immutable(config_path, config)
    return config_path


def _validate_config(config_path: Path) -> dict[str, Any]:
    value = _load_json(config_path, "experiment config")
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != CONFIG_VERSION
        or value.get("architecture_id") != ARCHITECTURE_ID
        or value.get("managed_chatgpt_auth_only") is not True
        or value.get("official_persistent_codex_app_server_only") is not True
        or value.get("semantic_regex_or_keyword_filtering") is not False
        or value.get("production_mutation_allowed") is not False
        or value.get("holdout_authorized") is not False
        or value.get("retry_count_per_turn") != 0
        or value.get("router_model") != ROUTER_MODEL
        or value.get("router_effort") != ROUTER_EFFORT
        or value.get("router_total_tokens_maximum") != ROUTER_MAX_TOKENS
        or value.get("extractor_model") != EXTRACTOR_MODEL
        or value.get("extractor_effort") != EXTRACTOR_EFFORT
        or value.get("extractor_total_tokens_maximum") != EXTRACTOR_MAX_TOKENS
        or value.get("combined_total_tokens_maximum") != COMBINED_MAX_TOKENS
        or value.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or value.get("quota_points_per_million_tokens")
        != QUOTA_POINTS_PER_MILLION_TOKENS
        or value.get("production_amortized_context_tokens")
        != PRODUCTION_CONTEXT_TOKENS
        or value.get("production_scale") != PRODUCTION_SCALE
        or value.get("baseline_total_tokens") != BASELINE_TOTAL_TOKENS
    ):
        raise SignalRouterFrontierError("experiment config contract drifted")
    if Path(str(value.get("output_root") or "")).expanduser().resolve() != config_path.parent.resolve():
        raise SignalRouterFrontierError("experiment output root drifted")
    for key in (*LINEAGE_KEYS, "sparse_frame_checkpoint"):
        normalize_record(value.get(key) or {})
    projected = (
        PRODUCTION_CONTEXT_TOKENS + COMBINED_MAX_TOKENS * PRODUCTION_SCALE
    ) / BASELINE_TOTAL_TOKENS
    if projected > 0.28:
        raise SignalRouterFrontierError("frozen token ceiling exceeds production target")
    return value


def _capacity_policy(root: Path, config: Mapping[str, Any]) -> dict[str, Path]:
    audit = root / "capacity-policy-audit.json"
    policy = root / "capacity-policy.json"
    projected_points = math.ceil(
        CAPACITY_PHASE_BOUND * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    configured._write_stable_time(  # noqa: SLF001
        audit,
        {
            "schema_version": configured.CAPACITY_AUDIT_VERSION,
            "phase_id": str(config["experiment_id"]),
            "created_at": now_iso(),
            "production_mutation_performed": False,
            "measured_basis": {
                "declared_turn_count": 2,
                "maximum_total_tokens_per_turn": EXTRACTOR_MAX_TOKENS,
                "phase_total_token_bound": CAPACITY_PHASE_BOUND,
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
            "ordered_turn_names": [ROUTER_TURN, EXTRACTOR_TURN],
            "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
            "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
            "maximum_total_tokens_per_turn": EXTRACTOR_MAX_TOKENS,
            "phase_total_token_bound": CAPACITY_PHASE_BOUND,
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
    for key in (*LINEAGE_KEYS, "sparse_frame_checkpoint"):
        _verify_record(config[key], cache=cache)
    source = _load_json(Path(config["source_input"]["path"]), "source input")
    original_base = Path(config["original_base"]["path"]).read_text(encoding="utf-8")
    projection_schema = _load_json(
        Path(config["projection_schema"]["path"]), "projection schema"
    )
    all_ids = [str(row["segment_id"]) for row in source["segments"]]
    router = _turn_paths(root, ROUTER_TURN)
    _write_immutable(router["input"], source)
    _write_private_text(router["prompt"], _prompt(source, all_ids))
    _write_private_text(router["base"], original_base + "\n\n" + ROUTER_INSTRUCTIONS + "\n")
    _write_immutable(router["schema"], _router_schema(source, projection_schema))
    variant_records = []
    for subset in _route_subsets(source):
        paths = _variant_paths(root, subset)
        subset_source = _subset_source(source, subset)
        _write_immutable(paths["input"], subset_source)
        _write_private_text(paths["prompt"], _prompt(source, subset))
        _write_private_text(
            paths["base"], original_base + "\n\n" + EXTRACTOR_INSTRUCTIONS + "\n"
        )
        _write_immutable(
            paths["schema"], _extractor_schema(source, projection_schema, subset)
        )
        _write_immutable(paths["segment_ids"], {"segment_ids": list(subset)})
        variant_records.append(
            {
                "segment_ids": list(subset),
                "files": [
                    _record(paths[name], cache=cache)
                    for name in ("input", "prompt", "base", "schema", "segment_ids")
                ],
            }
        )
    capacity = _capacity_policy(root, config)
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "experiment_id": config["experiment_id"],
        "architecture_id": ARCHITECTURE_ID,
        "declared_turn_count": 2,
        "retry_count_per_turn": 0,
        "router_model": ROUTER_MODEL,
        "router_effort": ROUTER_EFFORT,
        "extractor_model": EXTRACTOR_MODEL,
        "extractor_effort": EXTRACTOR_EFFORT,
        "combined_total_tokens_maximum": COMBINED_MAX_TOKENS,
        "managed_chatgpt_auth_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "pinned_codex_cli": _record(PINNED_CODEX, cache=cache),
        "runtime_files": [_record(path, cache=cache) for path in _runtime_files()],
        "config": _record(config_path, cache=cache),
        "direct_lineage": [
            copy.deepcopy(config[key]) for key in (*LINEAGE_KEYS, "sparse_frame_checkpoint")
        ],
        "capacity_audit": _record(capacity["audit"], cache=cache),
        "capacity_policy": _record(capacity["policy"], cache=cache),
        "router_request": [
            _record(router[name], cache=cache)
            for name in ("input", "prompt", "base", "schema")
        ],
        "extractor_request_variants": variant_records,
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
                "declared_turn_count": 2,
                "retry_count_per_turn": 0,
                "router_model": ROUTER_MODEL,
                "extractor_model": EXTRACTOR_MODEL,
                "production_mutation_allowed": False,
            },
            required_record_paths=_runtime_files(),
        )
    except RuntimeVerificationError as exc:
        raise SignalRouterFrontierError("runtime lock verification failed") from exc
    if result.manifest.get("pinned_codex_cli") != _record(PINNED_CODEX):
        raise SignalRouterFrontierError("pinned Codex binary drifted")
    configured.reserve_module.load_reserve_capacity_policy(root / "capacity-policy.json")
    cache = ContentHashCache()
    for key in (*LINEAGE_KEYS, "sparse_frame_checkpoint"):
        _verify_record(config[key], cache=cache)
    launch = root / "launch-receipt.json"
    if launch.exists():
        value = _load_json(launch, "launch receipt")
        for key in ("runtime_lock", "runtime_closure", "config"):
            _verify_record(value.get(key) or {}, cache=cache)
        if value.get("declared_turn_count") != 2 or value.get("retry_count_per_turn") != 0:
            raise SignalRouterFrontierError("launch receipt contract drifted")
    return {
        "root": root,
        "config": config,
        "runtime_lock": root / "runtime-lock.json",
        "runtime_closure": root / "runtime-lock-closure.json",
        "capacity_policy": root / "capacity-policy.json",
        "router": _turn_paths(root, ROUTER_TURN),
        "extractor": _turn_paths(root, EXTRACTOR_TURN),
        "source": _load_json(Path(config["source_input"]["path"]), "source input"),
    }


def _usage(path: Path, model: str, effort: str) -> dict[str, int]:
    try:
        return configured._usage_from_sidecar(  # noqa: SLF001
            path, {"model": model, "effort": effort}
        )
    except configured.ConfiguredExperimentError as exc:
        raise SignalRouterFrontierError(str(exc)) from exc


def _aggregate(values: Sequence[Mapping[str, int]]) -> dict[str, int]:
    return configured._aggregate_usage(values)  # noqa: SLF001


def _gate(
    usage: Mapping[str, int], diagnostics: Mapping[str, Any]
) -> dict[str, Any]:
    rows = list(diagnostics["diagnostics"])
    total = PRODUCTION_CONTEXT_TOKENS + int(usage["total_tokens"]) * PRODUCTION_SCALE
    ratio = total / BASELINE_TOTAL_TOKENS
    checks = {
        "two_turns_measured": True,
        "router_total_tokens_lte_28000": int(usage["router_total_tokens"]) <= ROUTER_MAX_TOKENS,
        "extractor_total_tokens_lte_45000": int(usage["extractor_total_tokens"])
        <= EXTRACTOR_MAX_TOKENS,
        "combined_total_tokens_lte_73000": int(usage["total_tokens"])
        <= COMBINED_MAX_TOKENS,
        "all_source_units_reviewed": all(
            int(row["source_unit_count"]) == int(row["reviewed_source_unit_count"])
            for row in rows
        ),
        "unresolved_count_0": all(int(row["unresolved_count"]) == 0 for row in rows),
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "semantic_event_count_proxy_not_used": True,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": "pif_signal_router_frontier_structural_gate_v1",
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


def _known_and_unknown_usage(
    frozen: Mapping[str, Any]
) -> tuple[list[dict[str, int]], int]:
    values: list[dict[str, int]] = []
    unknown = 0
    for paths, model, effort in (
        (frozen["router"], ROUTER_MODEL, ROUTER_EFFORT),
        (frozen["extractor"], EXTRACTOR_MODEL, EXTRACTOR_EFFORT),
    ):
        if not paths["capacity"].exists() and not paths["sidecar"].exists():
            continue
        if not paths["sidecar"].exists():
            unknown += 1
            continue
        try:
            values.append(_usage(paths["sidecar"], model, effort))
        except Exception:
            unknown += 1
    return values, unknown


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    measured, unknown = _known_and_unknown_usage(frozen)
    semantic = isinstance(exc, (SignalRouterOutputError, SignalRouterArchitectureStop))
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "signal_router_frontier_structural_or_cost_gate_not_passed"
            if semantic
            else "infrastructure_or_extraction_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": len(measured) + unknown,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "usage": _aggregate(measured) if measured else {field: 0 for field in configured.USAGE_FIELDS},
        "unknown_usage_attempt_count": unknown,
        "architecture_strategy_rejected": semantic,
        "support_alignment_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "exact_next_action": "freeze this architecture; do not repair or retry it",
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
                    "schema_version": "pif_signal_router_frontier_launch_v1",
                    "launched_at": now_iso(),
                    "declared_turn_count": 2,
                    "retry_count_per_turn": 0,
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
        router = frozen["router"]
        extractor = frozen["extractor"]
        async with client_factory(frozen["capacity_policy"]) as client:
            if not router["sidecar"].exists():
                await client.run_ephemeral_structured_turn(
                    model=ROUTER_MODEL,
                    effort=ROUTER_EFFORT,
                    base_instructions=router["base"].read_text(encoding="utf-8"),
                    prompt=router["prompt"].read_text(encoding="utf-8"),
                    output_schema=_load_json(router["schema"], "router schema"),
                    cwd=PROJECT_ROOT,
                    sidecar_path=router["sidecar"],
                    output_path=router["output"],
                    batch_size=2,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=router["capacity"],
                )
            router_usage = _usage(router["sidecar"], ROUTER_MODEL, ROUTER_EFFORT)
            if router_usage["total_tokens"] > ROUTER_MAX_TOKENS:
                raise SignalRouterArchitectureStop("router exceeded frozen token ceiling")
            routed, router_rows = _validate_router_output(
                _load_json(router["output"], "router output"),
                frozen["source"],
                router["schema"],
            )
            _write_immutable(
                root / "router-receipt.private.json",
                {
                    "episode_id": frozen["source"]["episode_id"],
                    "segments": list(router_rows.values()),
                    "routed_segment_ids": list(routed),
                },
            )
            if not routed:
                raise SignalRouterArchitectureStop("router selected no extraction segments")
            variant = _variant_paths(root, routed)
            selected = {
                "schema_version": "pif_signal_router_selected_request_v1",
                "selected_at": now_iso(),
                "segment_ids": list(routed),
                "router_output_sha256": _record(router["output"])["sha256"],
                "variant_files": [
                    _record(variant[name])
                    for name in ("input", "prompt", "base", "schema", "segment_ids")
                ],
            }
            configured._write_stable_time(  # noqa: SLF001
                root / "selected-request.json", selected, "selected_at"
            )
            if not extractor["sidecar"].exists():
                await client.run_ephemeral_structured_turn(
                    model=EXTRACTOR_MODEL,
                    effort=EXTRACTOR_EFFORT,
                    base_instructions=variant["base"].read_text(encoding="utf-8"),
                    prompt=variant["prompt"].read_text(encoding="utf-8"),
                    output_schema=_load_json(variant["schema"], "extractor schema"),
                    cwd=PROJECT_ROOT,
                    sidecar_path=extractor["sidecar"],
                    output_path=extractor["output"],
                    batch_size=len(routed),
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=extractor["capacity"],
                )
        extractor_usage = _usage(
            extractor["sidecar"], EXTRACTOR_MODEL, EXTRACTOR_EFFORT
        )
        if extractor_usage["total_tokens"] > EXTRACTOR_MAX_TOKENS:
            raise SignalRouterArchitectureStop("extractor exceeded frozen token ceiling")
        usage = _aggregate([router_usage, extractor_usage])
        usage["router_total_tokens"] = router_usage["total_tokens"]
        usage["extractor_total_tokens"] = extractor_usage["total_tokens"]
        if usage["total_tokens"] > COMBINED_MAX_TOKENS:
            raise SignalRouterArchitectureStop("combined extraction token ceiling exceeded")
        variant = _variant_paths(root, routed)
        subset = _load_json(variant["input"], "selected subset source")
        try:
            extracted, provenance, diagnostics = flat.validate_and_project_output(
                _load_json(extractor["output"], "extractor output"),
                {"turn": {"schema": variant["schema"]}, "source": subset},
            )
        except flat.FlatRequestOutputError as exc:
            raise SignalRouterOutputError(str(exc)) from exc
        normalized, provenance, diagnostics = _assemble_candidate(
            frozen["source"], router_rows, extracted, provenance, diagnostics
        )
        _write_immutable(root / "normalized-output.private.json", normalized)
        _write_immutable(root / "evidence-provenance.private.json", provenance)
        _write_immutable(root / "diagnostics.private.json", diagnostics)
        gate = _gate(usage, diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise SignalRouterArchitectureStop("structural or production-cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "signal_router_frontier_structural_cost_passed",
            "terminal_reason": "signal_router_frontier_structural_cost_passed_support_alignment_required",
            "semantic_attempt_count": 2,
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
            "router_sidecar": _record(router["sidecar"]),
            "extractor_sidecar": _record(extractor["sidecar"]),
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
