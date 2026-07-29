from __future__ import annotations

"""Run one source-complete extraction into schema-compiled typed event sets."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import app_server_configured_experiment as configured
from . import app_server_flat_request_model_lane as flat
from . import app_server_sparse_event_frame_lane as sparse
from . import codex_app_server
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .app_server_runtime_verifier import (
    ContentHashCache,
    manifest_records,
    normalize_record,
)
from .labels import ValidationError, _validate_schema
from .util import now_iso


CONFIG_VERSION = "pif_typed_event_set_experiment_config_v1"
LOCK_VERSION = "pif_typed_event_set_experiment_runtime_lock_v1"
TERMINAL_VERSION = "pif_typed_event_set_experiment_terminal_v1"
ARCHITECTURE_ID = "schema_compiled_typed_event_sets_with_required_unit_ledger"
TURN_NAME = "typed_event_set_extraction"
MODEL = "gpt-5.6-luna"
EFFORT = "high"
MAX_TOTAL_TOKENS = 45_000
MINIMUM_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
MAX_EVENTS_PER_SEGMENT = 32
BASELINE_TOTAL_TOKENS = sparse.BASELINE_TOTAL_TOKENS
PRODUCTION_CONTEXT_TOKENS = sparse.PRODUCTION_CONTEXT_TOKENS
PRODUCTION_SCALE = sparse.PRODUCTION_SCALE
PROJECT_ROOT = sparse.PROJECT_ROOT
PIPELINE_ROOT = sparse.PIPELINE_ROOT
SOURCE_TURN_ROOT = sparse.SOURCE_TURN_ROOT
PHASE_ROOT = sparse.PHASE_ROOT
JUDGE_ROOT = sparse.JUDGE_ROOT
EVALUATOR_ROOT = sparse.EVALUATOR_ROOT
PINNED_CODEX = configured.PINNED_CODEX
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-canary-schema-compiled-typed-event-set-luna-high-2026-07-18"
).resolve()
SPARSE_V2_ROOT = (
    PIPELINE_ROOT
    / "development-canary-sparse-event-frame-luna-high-schema-compat-recovery-v2-2026-07-18"
).resolve()
ROUTER_ROOT = (
    PIPELINE_ROOT / "development-canary-signal-router-luna-frontier-2026-07-18"
).resolve()
STRUCTURE_AUDIT_ROOT = (
    PIPELINE_ROOT / "development-convergence-structural-audit-2026-07-18"
).resolve()
CONVERGENCE_ROOT = (
    PIPELINE_ROOT / "development-convergence-blocker-2026-07-18"
).resolve()
LINEAGE_KEYS = (
    "source_input",
    "source_prompt",
    "source_base",
    "projection_schema",
    "sparse_v2_terminal",
    "sparse_v2_runtime_lock",
    "sparse_v2_runtime_closure",
    "sparse_v2_sidecar",
    "router_terminal",
    "router_runtime_lock",
    "router_runtime_closure",
    "router_sidecar",
    "router_extractor_sidecar",
    "structure_audit_terminal",
    "structure_audit_report",
    "convergence_terminal",
    "architecture_family_comparison",
    "current_evaluator_checkpoint",
    "judge_calibration_terminal",
    "evaluator_runtime_lock",
    "architecture_decision",
)
EXPECTED_SOURCE_HASHES = sparse.EXPECTED_SOURCE_HASHES

TYPED_EVENT_SET_INSTRUCTIONS = """# Schema-compiled typed event-set extraction

Perform one fresh blind source-complete extraction. Every semantic decision is yours. Do not use a prior candidate, reference answer, target count, topic list, expected density, keywords, regex rules, embeddings, similarity rules, or deterministic semantic heuristics.

Review every supplied source unit in chronological order. For every segment, fill the schema-required receipt object for every unit ID. The eligible_event_count for a unit equals the number of returned events whose evidence_start_unit_id is that unit. Return zero unresolved units.

Identify every independently truth-valued eligible event and select its smallest sufficient contiguous exact evidence-unit range. Keep distinct events separate whenever their actor, speaker, reported actor, attribution, target, claim, stance, certainty, temporal horizon, causal mechanism, metric, event type, evidence commitment, or event boundary differs. Merge only exact semantic duplicates.

Return metric-bearing events in the top-level metric_events array and all other events in the top-level non_metric_events array. Assign every event to its source segment_id. A metric event must contain one source-grounded metric object whose raw_text and every nonempty metric string are literal contiguous substrings of its selected evidence. A non-metric event has no metric object. Party arrays contain either zero items or one evidence-supported named party; zero means the canonical absent/unknown value for that role.

Assign one unique source_order_index from zero through N-1 across both event arrays in each segment. This index records the final chronological event order only; it is not a confidence or importance score. Re-read every evidence range and every material field before returning complete schema-valid JSON."""


class TypedEventSetError(RuntimeError):
    pass


class TypedEventSetOutputError(TypedEventSetError):
    pass


class TypedEventSetArchitectureStop(TypedEventSetError):
    pass


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TypedEventSetError(f"cannot read {label}") from exc


def _record(path: Path, *, cache: ContentHashCache | None = None) -> dict[str, Any]:
    return (cache or ContentHashCache()).record(path)


def _verify_record(record: Mapping[str, Any], *, cache: ContentHashCache) -> None:
    if not cache.verify_record(record):
        raise TypedEventSetError("frozen typed-event-set artifact drifted")


def _write_immutable(path: Path, value: Any) -> None:
    configured._write_immutable(path, value)  # noqa: SLF001


def _write_private_text(path: Path, value: str) -> None:
    configured._write_private_text(path, value)  # noqa: SLF001


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
    sparse_v2_sidecar = next((SPARSE_V2_ROOT / "turns").glob("*/sidecar.json"))
    return {
        "source_input": SOURCE_TURN_ROOT / "input.private.json",
        "source_prompt": SOURCE_TURN_ROOT / "prompt.private.md",
        "source_base": SOURCE_TURN_ROOT / "base-instructions.private.md",
        "projection_schema": SOURCE_TURN_ROOT / "schema.json",
        "sparse_v2_terminal": SPARSE_V2_ROOT / "terminal.json",
        "sparse_v2_runtime_lock": SPARSE_V2_ROOT / "runtime-lock.json",
        "sparse_v2_runtime_closure": SPARSE_V2_ROOT / "runtime-lock-closure.json",
        "sparse_v2_sidecar": sparse_v2_sidecar,
        "router_terminal": ROUTER_ROOT / "terminal.json",
        "router_runtime_lock": ROUTER_ROOT / "runtime-lock.json",
        "router_runtime_closure": ROUTER_ROOT / "runtime-lock-closure.json",
        "router_sidecar": ROUTER_ROOT / "turns/semantic-signal-router/sidecar.json",
        "router_extractor_sidecar": ROUTER_ROOT
        / "turns/routed-frontier-extraction/sidecar.json",
        "structure_audit_terminal": STRUCTURE_AUDIT_ROOT / "terminal.json",
        "structure_audit_report": STRUCTURE_AUDIT_ROOT / "event-structure-audit.json",
        "convergence_terminal": CONVERGENCE_ROOT / "terminal.json",
        "architecture_family_comparison": PHASE_ROOT
        / "architecture-family-comparison.json",
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
                Path(sparse.__file__).resolve(),
                Path(flat.__file__).resolve(),
                *configured._runtime_files(),  # noqa: SLF001
            },
            key=str,
        )
    )


def _direct_runtime_receipt(
    lock_path: Path, *, cache: ContentHashCache | None = None
) -> dict[str, Any]:
    cache = cache or ContentHashCache()
    manifest_record = cache.record(lock_path)
    manifest = _load_json(lock_path, "runtime lock")
    records = manifest_records(manifest)
    for record in records:
        if not cache.verify_record(record):
            raise TypedEventSetError("direct runtime record drifted")
    payload = {
        "manifest": manifest_record,
        "records": sorted(records, key=lambda row: (row["path"], row["sha256"])),
    }
    digest = hashlib.sha256(
        json.dumps(
            payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    return {
        "schema_version": "pif_direct_successor_runtime_receipt_v1",
        "manifest": manifest_record,
        "direct_records_digest": digest,
        "direct_record_count": len(records),
        "recursive_predecessor_rehash_performed": False,
    }


def _verify_direct_runtime_lock(
    lock_path: Path,
    receipt_path: Path,
    *,
    required_fields: Mapping[str, Any],
    required_record_paths: Sequence[Path],
) -> dict[str, Any]:
    receipt = _load_json(receipt_path, "direct runtime receipt")
    if (
        receipt.get("schema_version") != "pif_direct_successor_runtime_receipt_v1"
        or receipt.get("recursive_predecessor_rehash_performed") is not False
    ):
        raise TypedEventSetError("direct runtime receipt drifted")
    cache = ContentHashCache()
    current = _direct_runtime_receipt(lock_path, cache=cache)
    if current != receipt:
        raise TypedEventSetError("direct runtime receipt no longer matches")
    manifest = _load_json(lock_path, "runtime lock")
    for key, expected in required_fields.items():
        if manifest.get(key) != expected:
            raise TypedEventSetError(f"runtime lock field drifted: {key}")
    actual_paths = {record["path"] for record in manifest_records(manifest)}
    required_paths = {
        str(path.expanduser().resolve()) for path in required_record_paths
    }
    if not required_paths.issubset(actual_paths):
        raise TypedEventSetError("runtime lock direct file coverage drifted")
    return manifest


def _named_party_schema(source: Mapping[str, Any]) -> dict[str, Any]:
    item = copy.deepcopy(dict(source["items"]))
    name = copy.deepcopy(dict(item["properties"]["name"]))
    name["minLength"] = max(1, int(name.get("minLength", 0)))
    party_type = copy.deepcopy(dict(item["properties"]["type"]))
    if "enum" in party_type:
        party_type["enum"] = [
            value for value in party_type["enum"] if value not in {"none", "unknown"}
        ]
    return {
        "type": "array",
        "minItems": 0,
        "maxItems": 1,
        "items": {
            "type": "object",
            "additionalProperties": False,
            "required": ["name", "type"],
            "properties": {"name": name, "type": party_type},
        },
    }


def _typed_event_schema(
    sparse_event: Mapping[str, Any],
    unit_ids: Sequence[str],
    segment_ids: Sequence[str],
    *,
    metric: bool,
) -> dict[str, Any]:
    event = copy.deepcopy(dict(sparse_event))
    props = event["properties"]
    for field in ("speaker", "actor", "reported_actor"):
        props[field] = _named_party_schema(props[field])
    for field in ("evidence_start_unit_id", "evidence_end_unit_id"):
        props[field] = {"type": "string", "enum": list(unit_ids)}
    props["source_order_index"] = {
        "type": "integer",
        "minimum": 0,
        "maximum": MAX_EVENTS_PER_SEGMENT - 1,
    }
    props["segment_id"] = {"type": "string", "enum": list(segment_ids)}
    if metric:
        props["metric"]["minItems"] = 1
        props["metric"]["maxItems"] = 1
    else:
        props.pop("metric")
    required = [name for name in event["required"] if metric or name != "metric"]
    required.append("source_order_index")
    required.append("segment_id")
    event["required"] = required
    return event


def build_typed_schema(
    projection_schema: Mapping[str, Any], source: Mapping[str, Any]
) -> dict[str, Any]:
    sparse_schema = sparse.build_sparse_schema(projection_schema)
    sparse_segment = sparse_schema["properties"]["segments"]["items"]
    sparse_event = sparse_segment["properties"]["events"]["items"]
    canonical_segment = projection_schema["properties"]["segments"]["items"]
    receipt_item = canonical_segment["properties"]["unit_receipts"]["items"]
    receipt_value = {
        "type": "object",
        "additionalProperties": False,
        "required": ["eligible_event_count", "unresolved_count"],
        "properties": {
            "eligible_event_count": copy.deepcopy(
                receipt_item["properties"]["eligible_event_count"]
            ),
            "unresolved_count": {"type": "integer", "enum": [0]},
        },
    }
    segment_properties: dict[str, Any] = {}
    all_unit_ids = [
        str(unit["unit_id"])
        for source_segment in source["segments"]
        for unit in source_segment["units"]
    ]
    segment_ids = [str(row["segment_id"]) for row in source["segments"]]
    for source_segment in source["segments"]:
        segment_id = str(source_segment["segment_id"])
        unit_ids = [str(unit["unit_id"]) for unit in source_segment["units"]]
        segment_properties[segment_id] = {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "status",
                "segment_source_context",
                "no_signal_reason",
                "coverage_audit",
                "unit_receipts_by_id",
            ],
            "properties": {
                "status": copy.deepcopy(canonical_segment["properties"]["status"]),
                "segment_source_context": copy.deepcopy(
                    canonical_segment["properties"]["segment_source_context"]
                ),
                "no_signal_reason": copy.deepcopy(
                    canonical_segment["properties"]["no_signal_reason"]
                ),
                "coverage_audit": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["all_source_units_reviewed", "unresolved_count"],
                    "properties": {
                        "all_source_units_reviewed": {
                            "type": "boolean",
                            "enum": [True],
                        },
                        "unresolved_count": {"type": "integer", "enum": [0]},
                    },
                },
                "unit_receipts_by_id": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": unit_ids,
                    "properties": {
                        unit_id: copy.deepcopy(receipt_value) for unit_id in unit_ids
                    },
                },
            },
        }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "episode_id",
            "segments_by_id",
            "metric_events",
            "non_metric_events",
        ],
        "properties": {
            "episode_id": {"type": "string", "enum": [str(source["episode_id"])]},
            "segments_by_id": {
                "type": "object",
                "additionalProperties": False,
                "required": segment_ids,
                "properties": segment_properties,
            },
            "metric_events": {
                "type": "array",
                "minItems": 0,
                "maxItems": MAX_EVENTS_PER_SEGMENT * len(segment_ids),
                "items": _typed_event_schema(
                    sparse_event, all_unit_ids, segment_ids, metric=True
                ),
            },
            "non_metric_events": {
                "type": "array",
                "minItems": 0,
                "maxItems": MAX_EVENTS_PER_SEGMENT * len(segment_ids),
                "items": _typed_event_schema(
                    sparse_event, all_unit_ids, segment_ids, metric=False
                ),
            },
        },
    }


def _project_named_party(value: Any, field: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) > 1:
        raise TypedEventSetOutputError(f"{field} cardinality drifted")
    if not value:
        return []
    item = value[0]
    if not isinstance(item, Mapping):
        raise TypedEventSetOutputError(f"{field} item drifted")
    return [
        {
            "identity_kind": "named",
            "name": str(item["name"]),
            "type": str(item["type"]),
        }
    ]


def validate_and_project_output(
    output: Mapping[str, Any], frozen: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    turn = frozen["turn"]
    try:
        _validate_schema(_load_json(turn["schema"], "typed schema"), output, path="$")
    except (ValidationError, TypeError, ValueError) as exc:
        raise TypedEventSetOutputError("typed output schema failed") from exc
    source = frozen["source"]
    if output.get("episode_id") != source.get("episode_id"):
        raise TypedEventSetOutputError("episode identity drifted")
    by_id = output.get("segments_by_id")
    if not isinstance(by_id, Mapping):
        raise TypedEventSetOutputError("segment map is absent")
    metric_by_segment: dict[str, list[Mapping[str, Any]]] = {
        str(row["segment_id"]): [] for row in source["segments"]
    }
    nonmetric_by_segment: dict[str, list[Mapping[str, Any]]] = {
        str(row["segment_id"]): [] for row in source["segments"]
    }
    for event in output["metric_events"]:
        metric_by_segment[str(event["segment_id"])].append(event)
    for event in output["non_metric_events"]:
        nonmetric_by_segment[str(event["segment_id"])].append(event)
    direct_segments: list[dict[str, Any]] = []
    for source_segment in source["segments"]:
        segment_id = str(source_segment["segment_id"])
        row = by_id[segment_id]
        unit_ids = [str(unit["unit_id"]) for unit in source_segment["units"]]
        receipts = row["unit_receipts_by_id"]
        metric_events = metric_by_segment[segment_id]
        non_metric_events = nonmetric_by_segment[segment_id]
        raw_events = [
            (event, True) for event in metric_events
        ] + [(event, False) for event in non_metric_events]
        if len(raw_events) > MAX_EVENTS_PER_SEGMENT:
            raise TypedEventSetOutputError("segment event cap drifted")
        order = [int(event["source_order_index"]) for event, _ in raw_events]
        if sorted(order) != list(range(len(raw_events))):
            raise TypedEventSetOutputError("source order index coverage drifted")
        projected_events: list[tuple[int, dict[str, Any]]] = []
        for raw_event, has_metric in raw_events:
            event = copy.deepcopy(dict(raw_event))
            source_order_index = int(event.pop("source_order_index"))
            if event.pop("segment_id") != segment_id:
                raise TypedEventSetOutputError("event segment ownership drifted")
            for field in ("speaker", "actor", "reported_actor"):
                event[field] = _project_named_party(event[field], field)
            if not has_metric:
                event["metric"] = []
            projected_events.append(
                (source_order_index, sparse.project_sparse_event(event))
            )
        projected_events.sort(key=lambda item: item[0])
        events = [event for _index, event in projected_events]
        if (row["status"] == "coded") != bool(events):
            raise TypedEventSetOutputError("segment status and events drifted")
        if (row["status"] == "coded" and row["no_signal_reason"] != "") or (
            row["status"] == "no_signal" and not str(row["no_signal_reason"]).strip()
        ):
            raise TypedEventSetOutputError("segment no-signal reason drifted")
        direct_segments.append(
            {
                "segment_id": segment_id,
                "status": row["status"],
                "segment_source_context": row["segment_source_context"],
                "no_signal_reason": row["no_signal_reason"],
                "unit_receipts": [
                    {
                        "unit_id": unit_id,
                        "eligible_event_count": int(
                            receipts[unit_id]["eligible_event_count"]
                        ),
                        "unresolved_count": int(receipts[unit_id]["unresolved_count"]),
                    }
                    for unit_id in unit_ids
                ],
                "coverage_audit": copy.deepcopy(row["coverage_audit"]),
                "events": events,
            }
        )
    direct = {"episode_id": source["episode_id"], "segments": direct_segments}
    try:
        normalized, provenance, diagnostics = flat.validate_and_project_output(
            direct,
            {"turn": {"schema": turn["projection_schema"]}, "source": source},
        )
    except flat.FlatRequestOutputError as exc:
        raise TypedEventSetOutputError(str(exc)) from exc
    diagnostics = copy.deepcopy(diagnostics)
    diagnostics["representation"] = ARCHITECTURE_ID
    diagnostics["all_semantic_decisions_owned_by_llm"] = True
    diagnostics["deterministic_projection_only"] = True
    return normalized, provenance, diagnostics


def _architecture_decision(records: Mapping[str, Any]) -> dict[str, Any]:
    projected_ratio = (
        PRODUCTION_CONTEXT_TOKENS + MAX_TOTAL_TOKENS * PRODUCTION_SCALE
    ) / BASELINE_TOTAL_TOKENS
    return {
        "schema_version": "pif_typed_event_set_architecture_decision_v1",
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
                    "The prior Luna output had valid evidence ranges and identities but failed "
                    "six structural contracts. Compiling segment IDs, unit receipts, party "
                    "cardinality, and metric applicability into the output type removes those "
                    "invalid states without a semantic rule or prior-candidate repair."
                ),
            },
            {
                "rank": 2,
                "architecture_id": "pointwise_unit_event_objects_then_global_owner",
                "status": "deferred_boundary_and_cost_risk",
                "evidence": (
                    "Per-unit extraction guarantees coverage but obscures cross-unit event "
                    "boundaries and requires a second semantic owner pass."
                ),
            },
            {
                "rank": 3,
                "architecture_id": "episode_bootstrap_then_segment_specialists",
                "status": "deferred_raw_token_and_history_risk",
                "evidence": (
                    "Thread reuse may reduce transmitted prompt bytes but measured raw context "
                    "accumulation and specialist reconciliation threaten the 0.28 gate."
                ),
            },
        ],
        "selected_canary": {
            "architecture_id": ARCHITECTURE_ID,
            "hypothesis": (
                "One source-complete Luna-high pass over a schema-compiled typed event set can "
                "retain frontier semantic quality while making receipt coverage, named-party "
                "identity, and metric applicability structurally coherent."
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
            "maximum_total_tokens": MAX_TOTAL_TOKENS,
            "production_cost_projection": {
                "maximum_production_amortized_total_token_ratio": round(
                    projected_ratio, 6
                )
            },
            "stop_rules": {
                "total_tokens_maximum": MAX_TOTAL_TOKENS,
                "all_source_units_reviewed": True,
                "unresolved_count": 0,
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "exact_identity_duplicates": 0,
                "event_cap_violations": 0,
                "semantic_event_count_proxy_used": False,
                "production_amortized_total_token_ratio_maximum": 0.28,
                "on_failure": "freeze and reject without repair or retry",
                "on_pass": (
                    "run the unchanged frozen support-first and neutral two-permutation evaluator"
                ),
            },
        },
        "direct_predecessor_records": copy.deepcopy(dict(records)),
    }


def prepare_experiment(root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    root = root.expanduser().resolve()
    paths = _lineage_paths()
    cache = ContentHashCache()
    records = {key: _record(path, cache=cache) for key, path in paths.items()}
    for key, expected_hash in EXPECTED_SOURCE_HASHES.items():
        if records[key]["sha256"] != expected_hash:
            raise TypedEventSetError(f"{key} source artifact drifted")
    sparse_terminal = _load_json(paths["sparse_v2_terminal"], "sparse v2 terminal")
    router_terminal = _load_json(paths["router_terminal"], "router terminal")
    audit_terminal = _load_json(
        paths["structure_audit_terminal"], "structure audit terminal"
    )
    judge_terminal = _load_json(
        paths["judge_calibration_terminal"], "judge terminal"
    )
    if (
        sparse_terminal.get("usage_status") != "complete"
        or (sparse_terminal.get("usage") or {}).get("total_tokens") != 38803
        or sparse_terminal.get("semantic_retry_count") != 0
        or router_terminal.get("usage_status") != "complete"
        or (router_terminal.get("usage") or {}).get("total_tokens") != 68522
        or router_terminal.get("semantic_retry_count") != 0
        or audit_terminal.get("terminal_reason")
        != "development_candidate_structural_gate_not_passed"
        or audit_terminal.get("semantic_model_call_count") != 0
        or audit_terminal.get("production_mutated") is not False
        or judge_terminal.get("selection_authorized") is not True
    ):
        raise TypedEventSetError("direct predecessor state drifted")
    decision_path = root / "architecture-decision.json"
    configured._write_stable_time(  # noqa: SLF001
        decision_path, _architecture_decision(records), "created_at"
    )
    records["architecture_decision"] = _record(decision_path, cache=cache)
    config = {
        "schema_version": CONFIG_VERSION,
        "experiment_id": "development_canary_typed_event_set_luna_high_2026_07_18",
        "architecture_id": ARCHITECTURE_ID,
        "output_root": str(root),
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_total_tokens": MAX_TOTAL_TOKENS,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
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
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "production_amortized_context_tokens": PRODUCTION_CONTEXT_TOKENS,
        "production_scale": PRODUCTION_SCALE,
        "baseline_total_tokens": BASELINE_TOTAL_TOKENS,
    }
    if not isinstance(value, dict) or any(value.get(key) != item for key, item in expected.items()):
        raise TypedEventSetError("experiment config contract drifted")
    if Path(str(value.get("output_root") or "")).expanduser().resolve() != config_path.parent.resolve():
        raise TypedEventSetError("experiment output root drifted")
    for key in LINEAGE_KEYS:
        normalize_record(value.get(key) or {})
    projected = (
        PRODUCTION_CONTEXT_TOKENS + MAX_TOTAL_TOKENS * PRODUCTION_SCALE
    ) / BASELINE_TOTAL_TOKENS
    if projected > 0.28:
        raise TypedEventSetError("frozen token ceiling exceeds production target")
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
        turn["base"], source_base + "\n\n" + TYPED_EVENT_SET_INSTRUCTIONS + "\n"
    )
    source = _load_json(Path(config["source_input"]["path"]), "source input")
    projection_schema = _load_json(
        Path(config["projection_schema"]["path"]), "projection schema"
    )
    _write_immutable(turn["projection_schema"], projection_schema)
    _write_immutable(turn["schema"], build_typed_schema(projection_schema, source))
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
        _direct_runtime_receipt(lock_path, cache=ContentHashCache()),
    )
    return verify_frozen(root)


def verify_frozen(root: Path) -> dict[str, Any]:
    root = root.expanduser().resolve()
    config = _validate_config(root / "experiment-config.json")
    manifest = _verify_direct_runtime_lock(
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
            "production_mutation_allowed": False,
        },
        required_record_paths=_runtime_files(),
    )
    if manifest.get("pinned_codex_cli") != _record(PINNED_CODEX):
        raise TypedEventSetError("pinned Codex binary drifted")
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
            raise TypedEventSetError("launch receipt contract drifted")
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
        raise TypedEventSetError("turn usage is absent")
    usage: dict[str, int] = {}
    for field in configured.USAGE_FIELDS:
        value = values.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise TypedEventSetError("turn usage is incomplete")
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
        raise TypedEventSetError("measured sidecar contract failed")
    return usage


def _gate(usage: Mapping[str, int], diagnostics: Mapping[str, Any]) -> dict[str, Any]:
    rows = list(diagnostics["diagnostics"])
    production_total = PRODUCTION_CONTEXT_TOKENS + int(usage["total_tokens"]) * PRODUCTION_SCALE
    ratio = production_total / BASELINE_TOTAL_TOKENS
    checks = {
        "one_turn_measured": True,
        "total_tokens_lte_45000": int(usage["total_tokens"]) <= MAX_TOTAL_TOKENS,
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
        "schema_version": "pif_typed_event_set_structural_gate_v1",
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
            measured = _usage(turn["sidecar"])
        except Exception:
            unknown = 1
    elif turn["capacity"].exists() or turn["output"].exists():
        unknown = 1
    semantic = isinstance(exc, (TypedEventSetOutputError, TypedEventSetArchitectureStop))
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "typed_event_set_structural_or_cost_gate_not_passed"
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
            "freeze and reject this typed-event-set architecture without repair or retry"
            if semantic
            else "audit the immutable infrastructure attempt; do not retry this root"
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
                    "schema_version": "pif_typed_event_set_launch_v1",
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
            raise TypedEventSetError("attempt artifact exists without sidecar")
        async with client_factory(frozen["capacity_policy"]) as client:
            if not turn["sidecar"].exists():
                await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
                    base_instructions=turn["base"].read_text(encoding="utf-8"),
                    prompt=turn["prompt"].read_text(encoding="utf-8"),
                    output_schema=_load_json(turn["schema"], "typed structured schema"),
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
            raise TypedEventSetArchitectureStop("turn exceeded frozen token ceiling")
        normalized, provenance, diagnostics = validate_and_project_output(
            _load_json(turn["output"], "typed structured output"), frozen
        )
        _write_immutable(root / "normalized-output.private.json", normalized)
        _write_immutable(root / "evidence-provenance.private.json", provenance)
        _write_immutable(root / "diagnostics.private.json", diagnostics)
        gate = _gate(usage, diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise TypedEventSetArchitectureStop("structural or production-cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "typed_event_set_structural_cost_passed",
            "terminal_reason": (
                "typed_event_set_structural_cost_passed_support_alignment_required"
            ),
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
            "exact_next_action": (
                "run the unchanged source-support and neutral two-permutation evaluator"
            ),
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
