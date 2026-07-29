from __future__ import annotations

"""Realize the immutable v243 semantic core through source-indexed enrichment."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_judge_v5_selection_v243_core_enrichment as v243
from . import app_server_judge_v5_selection_v247_alignment_transport_recovery as v247
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v248_source_span_enrichment_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v248_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v248_terminal_v1"
PHASE_ID = "development_selection_v5_4_v248_source_span_enrichment"
MODEL = v243.MODEL
EFFORT = v243.EFFORT
NEW_TURN_MAX_TOKENS = 42_000
ADOPTED_CORE_TOKENS = 30_083
MAX_COMBINED_TOKENS = ADOPTED_CORE_TOKENS + NEW_TURN_MAX_TOKENS
MAX_PROMPT_BYTES = 120_000
MAX_BASE_BYTES = 14_000
MAX_SCHEMA_BYTES = 60_000
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
USAGE_FIELDS = v243.USAGE_FIELDS
PROJECT_ROOT = v243.PROJECT_ROOT
PIPELINE_ROOT = v243.PIPELINE_ROOT
V243_ROOT = v243.DEFAULT_OUTPUT_ROOT
V247_ROOT = v247.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v248-source-span-enrichment"
).resolve()
PINNED_CODEX_0_144_1 = v243.PINNED_CODEX_0_144_1
SPAN_FIELDS = (
    "metric_value_span",
    "metric_unit_span",
    "metric_comparator_span",
    "metric_raw_text_span",
)
SPAN_ARRAY_FIELDS = (
    "model_name_spans",
    "product_name_spans",
    "organization_spans",
    "people_spans",
)
DIRECT_FIELDS = (
    "event_subtype",
    "claim_type",
    "actor_type",
    "speaker_role",
    "reported_actor_type",
    "metric_direction",
    "signal_reason",
    "confidence",
)


class V248SourceSpanEnrichmentError(RuntimeError):
    """The source-span architecture cannot proceed or be adopted safely."""


class V248OutputContractError(V248SourceSpanEnrichmentError):
    """The LLM output violates the frozen source-span contract."""


class V248ArchitectureStop(V248SourceSpanEnrichmentError):
    """The measured architecture failed a predeclared semantic or cost gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V248SourceSpanEnrichmentError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V248SourceSpanEnrichmentError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V248SourceSpanEnrichmentError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    data = resolved.read_bytes()
    return {
        "path": str(resolved),
        "sha256": hashlib.sha256(data).hexdigest(),
        "size_bytes": len(data),
    }


def _verify_record(record: Mapping[str, Any]) -> bool:
    try:
        return _record(Path(str(record["path"]))) == dict(record)
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _v243_paths() -> dict[str, Path]:
    spec = _load_json(V243_ROOT / "attempt-spec.json", "v243 spec")
    core_root = V243_ROOT / "turns" / str(spec["core_turn_name"]).replace("_", "-")
    return {
        "terminal": V243_ROOT / "terminal.json",
        "runtime_lock": V243_ROOT / "runtime-lock.json",
        "design": V243_ROOT / "architecture-design.json",
        "core_gate": V243_ROOT / "core-structural-gate.json",
        "core_output": V243_ROOT / "core-output.private.json",
        "core_input": core_root / "input.private.json",
        "core_sidecar": core_root / "sidecar.json",
        "core_model_output": core_root / "output.private.json",
        "completed_audit": PIPELINE_ROOT / "v243-completed-output-audit-2026-07-17/report.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _v243_paths()
    v243.verify_runtime_lock(paths["runtime_lock"])
    v247.verify_runtime_lock(V247_ROOT / "runtime-lock.json")
    terminal = _load_json(paths["terminal"], "v243 terminal")
    core_gate = _load_json(paths["core_gate"], "v243 core gate")
    core_sidecar = _load_json(paths["core_sidecar"], "v243 core sidecar")
    audit = _load_json(paths["completed_audit"], "v243 completed audit")
    v247_terminal = _load_json(V247_ROOT / "terminal.json", "v247 terminal")
    v247_score = _load_json(V247_ROOT / "alignment-score.json", "v247 score")
    if (
        terminal.get("terminal_reason")
        != "v243_core_enrichment_structural_quality_or_cost_gate_not_passed"
        or terminal.get("production_mutated") is not False
        or core_gate.get("passed") is not True
        or core_gate.get("dense_event_count") != 27
        or core_gate.get("candidate_only_nominal_no_signal_event_count") != 1
        or core_sidecar.get("state") != "completed"
        or core_sidecar.get("usage_status") != "measured"
        or core_sidecar.get("usage_complete") is not True
        or (core_sidecar.get("usage") or {}).get("total_tokens") != ADOPTED_CORE_TOKENS
        or audit.get("corrected_failure_class")
        != "architecture_semantic_exact_literal_gate_not_passed"
        or audit.get("metric_literal_violation_event_count") != 1
        or audit.get("metric_literal_violation_field_count") != 1
        or audit.get("metric_applicability_violation_count") != 0
        or audit.get("production_amortized_total_token_ratio") != 0.244812
        or v247_terminal.get("terminal_reason")
        != "v247_alignment_quality_or_permutation_gate_not_passed"
        or v247_terminal.get("usage_status") != "complete"
        or v247_terminal.get("accounting_complete") is not True
        or v247_score.get("metrics", {}).get("development_strict_full_field_macro_f1")
        != 0.680723
        or v247_score.get("passed") is not False
    ):
        raise V248SourceSpanEnrichmentError("v248 architecture lineage drifted")
    packet = v243._load_frozen(V243_ROOT)["packet"]
    core = _load_json(paths["core_output"], "v243 canonical core")
    return {
        "paths": paths,
        "packet": packet,
        "core": core,
        "core_usage": {field: int(core_sidecar["usage"][field]) for field in USAGE_FIELDS},
        "records": {
            **{f"v243_{name}": _record(path) for name, path in paths.items()},
            "v247_terminal": _record(V247_ROOT / "terminal.json"),
            "v247_score": _record(V247_ROOT / "alignment-score.json"),
            "v247_runtime_lock": _record(V247_ROOT / "runtime-lock.json"),
        },
    }


SOURCE_SPAN_INSTRUCTIONS = """You are the source-indexed metadata realization stage for a topic-general podcast event pipeline. A prior blind Sol core pass has frozen event identity, count, order, evidence boundaries, event type, speaker, actor, reported actor, attribution context, target, claim, stance, certainty, temporal horizon, causal mechanism, and counterclaim. You cannot add, delete, merge, split, relabel, or override a core field.

For every core_event_id, read only its supplied exact evidence units and fill every requested metadata field from meaning. Never use keywords, regex, phrase lists, fixed topics, a reference answer, or target counts.

All metric strings and entity names are selected only through source spans. A source span contains unit_id plus zero-based Unicode code-point start inclusive and end exclusive offsets into that unit's exact text. Use {unit_id:"none",start:0,end:0} when a scalar metric span is absent. Every non-none span must be nonempty and inside one supplied evidence unit for that event. For entity arrays, return zero or more nonempty source spans. Do not reproduce source text in the output; deterministic code will slice your chosen spans exactly.

Set metric_direction=not_applicable if and only if all four metric spans are none. If any metric span is present, choose the meaning-correct non-not_applicable direction, including unknown when direction is not stated. Return every event exactly once in the supplied order and schema-valid JSON only."""


def _span_schema(unit_ids: Sequence[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["unit_id", "start", "end"],
        "properties": {
            "unit_id": {"type": "string", "enum": ["none", *unit_ids]},
            "start": {"type": "integer", "minimum": 0},
            "end": {"type": "integer", "minimum": 0},
        },
    }


def _output_schema(packet: Mapping[str, Any], event_ids: Sequence[str]) -> dict[str, Any]:
    source_event = packet["source_schema"]["properties"]["segments"]["items"][
        "properties"
    ]["events"]["items"]
    unit_ids = [
        str(unit["unit_id"])
        for segment in packet["private_input"]["segments"]
        for unit in segment["units"]
    ]
    span = _span_schema(unit_ids)
    properties = {
        "core_event_id": {"type": "string", "enum": list(event_ids)},
        **{
            field: copy.deepcopy(source_event["properties"][field])
            for field in DIRECT_FIELDS
        },
        **{field: copy.deepcopy(span) for field in SPAN_FIELDS},
        **{
            field: {
                "type": "array",
                "maxItems": 20,
                "items": copy.deepcopy(span),
            }
            for field in SPAN_ARRAY_FIELDS
        },
    }
    event = {
        "type": "object",
        "additionalProperties": False,
        "required": ["core_event_id", *DIRECT_FIELDS, *SPAN_FIELDS, *SPAN_ARRAY_FIELDS],
        "properties": properties,
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["episode_id", "events"],
        "properties": {
            "episode_id": {"type": "string", "enum": [packet["episode_id"]]},
            "events": {
                "type": "array",
                "minItems": len(event_ids),
                "maxItems": len(event_ids),
                "items": event,
            },
        },
    }


def prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    packet = lineage["packet"]
    core = lineage["core"]
    source_by_segment = {
        str(row["segment_id"]): row for row in packet["private_input"]["segments"]
    }
    events = []
    event_ids = []
    for segment in core["segments"]:
        source = source_by_segment[str(segment["segment_id"])]
        units = list(source["units"])
        index = {str(unit["unit_id"]): position for position, unit in enumerate(units)}
        for core_event in segment["events"]:
            start = index[str(core_event["evidence_start_unit_id"])]
            end = index[str(core_event["evidence_end_unit_id"])]
            event_ids.append(str(core_event["event_id"]))
            events.append(
                {
                    "segment_id": segment["segment_id"],
                    "core_event": core_event,
                    "exact_evidence_units": [
                        {
                            "unit_id": unit["unit_id"],
                            "text": unit["text"],
                            "unicode_code_point_length": len(unit["text"]),
                        }
                        for unit in units[start : end + 1]
                    ],
                }
            )
    prompt_packet = {"episode_id": packet["episode_id"], "events": events}
    prompt = "# Frozen core events with exact source units\n" + json.dumps(
        prompt_packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"
    base = (
        SOURCE_SPAN_INSTRUCTIONS
        + "\n\n# Immutable episode context\n"
        + json.dumps(packet["context"], ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )
    schema = _output_schema(packet, event_ids)
    sizes = {
        "prompt_bytes": len(prompt.encode("utf-8")),
        "base_bytes": len(base.encode("utf-8")),
        "schema_bytes": len(_canonical_json(schema).encode("utf-8")),
    }
    if (
        sizes["prompt_bytes"] > MAX_PROMPT_BYTES
        or sizes["base_bytes"] > MAX_BASE_BYTES
        or sizes["schema_bytes"] > MAX_SCHEMA_BYTES
    ):
        raise V248SourceSpanEnrichmentError("v248 request size cap exceeded")
    return {
        "turn_name": "v248_source_span_enrichment_" + sha256_text(packet["episode_id"])[:20],
        "episode_id": packet["episode_id"],
        "event_ids": event_ids,
        "private_input": prompt_packet,
        "prompt": prompt,
        "base": base,
        "schema": schema,
        "packet": packet,
        "core": core,
        **sizes,
    }


def _slice_span(
    span: Mapping[str, Any], *, unit_by_id: Mapping[str, Mapping[str, Any]], allowed_ids: set[str]
) -> tuple[str, dict[str, Any]]:
    unit_id = str(span.get("unit_id"))
    start = span.get("start")
    end = span.get("end")
    if unit_id == "none":
        if start != 0 or end != 0:
            raise V248OutputContractError("none span has nonzero offsets")
        return "", {"unit_id": "none", "start": 0, "end": 0, "text_sha256": None}
    if (
        unit_id not in allowed_ids
        or unit_id not in unit_by_id
        or isinstance(start, bool)
        or isinstance(end, bool)
        or not isinstance(start, int)
        or not isinstance(end, int)
    ):
        raise V248OutputContractError("source span escapes event evidence")
    text = str(unit_by_id[unit_id]["text"])
    if start < 0 or end <= start or end > len(text):
        raise V248OutputContractError("source span offsets are invalid")
    value = text[start:end]
    return value, {
        "unit_id": unit_id,
        "start": start,
        "end": end,
        "text_sha256": sha256_text(value),
    }


def project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    try:
        _validate_schema(turn["schema"], output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V248OutputContractError("source-span output schema failed") from exc
    if output.get("episode_id") != turn["episode_id"]:
        raise V248OutputContractError("source-span episode id drifted")
    rows = list(output.get("events") or [])
    if [row.get("core_event_id") for row in rows] != list(turn["event_ids"]):
        raise V248OutputContractError("source-span event order or coverage drifted")
    packet = turn["packet"]
    core = turn["core"]
    source_by_segment = {
        str(row["segment_id"]): row for row in packet["private_input"]["segments"]
    }
    row_by_id = {str(row["core_event_id"]): row for row in rows}
    enriched_rows = []
    span_receipts = []
    for segment in core["segments"]:
        source = source_by_segment[str(segment["segment_id"])]
        units = list(source["units"])
        unit_by_id = {str(unit["unit_id"]): unit for unit in units}
        unit_index = {str(unit["unit_id"]): index for index, unit in enumerate(units)}
        for core_event in segment["events"]:
            event_id = str(core_event["event_id"])
            row = row_by_id[event_id]
            start = unit_index[str(core_event["evidence_start_unit_id"])]
            end = unit_index[str(core_event["evidence_end_unit_id"])]
            allowed = {str(unit["unit_id"]) for unit in units[start : end + 1]}
            projected = {field: copy.deepcopy(row[field]) for field in DIRECT_FIELDS}
            receipt = {"core_event_id": event_id, "scalar_spans": {}, "array_spans": {}}
            scalar_map = {
                "metric_value_span": "metric_value",
                "metric_unit_span": "metric_unit",
                "metric_comparator_span": "metric_comparator",
                "metric_raw_text_span": "metric_raw_text",
            }
            for span_field, target_field in scalar_map.items():
                value, span_receipt = _slice_span(
                    row[span_field], unit_by_id=unit_by_id, allowed_ids=allowed
                )
                projected[target_field] = value
                receipt["scalar_spans"][target_field] = span_receipt
            array_map = {
                "model_name_spans": "model_names",
                "product_name_spans": "product_names",
                "organization_spans": "organizations",
                "people_spans": "people",
            }
            for span_field, target_field in array_map.items():
                values = []
                receipts = []
                for span in row[span_field]:
                    value, span_receipt = _slice_span(
                        span, unit_by_id=unit_by_id, allowed_ids=allowed
                    )
                    if not value:
                        raise V248OutputContractError("entity array contains none span")
                    values.append(value)
                    receipts.append(span_receipt)
                if len(values) != len(set(values)):
                    raise V248OutputContractError("entity array contains duplicate exact spans")
                projected[target_field] = values
                receipt["array_spans"][target_field] = receipts
            metric_empty = all(
                not projected[field]
                for field in ("metric_value", "metric_unit", "metric_comparator", "metric_raw_text")
            )
            if (projected["metric_direction"] == "not_applicable") != metric_empty:
                raise V248OutputContractError("metric direction and source-span applicability conflict")
            enriched_rows.append({"core_event_id": event_id, **projected})
            span_receipts.append(receipt)
    legacy_turn = v243._prepare_enrichment_turn(packet, core)
    legacy_turn["packet"] = packet
    normalized, provenance, diagnostics = v243._final_projection(
        {"episode_id": turn["episode_id"], "events": enriched_rows}, core, legacy_turn
    )
    return (
        normalized,
        provenance,
        diagnostics,
        {
            "schema_version": SCHEMA_VERSION,
            "events": span_receipts,
            "all_literal_values_projected_from_llm_selected_exact_source_spans": True,
            "semantic_selection_performed_by_deterministic_code": False,
            "privacy": "private opaque ids offsets and text hashes no source text",
        },
    )


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
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


def _request_records(paths: Mapping[str, Path]) -> list[dict[str, Any]]:
    return [_record(paths[name]) for name in ("input", "prompt", "base", "schema")]


def _capacity_policy(root: Path, turn_name: str) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(NEW_TURN_MAX_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_new_turn_count": 1,
            "adopted_measured_core_tokens": ADOPTED_CORE_TOKENS,
            "maximum_new_turn_tokens": NEW_TURN_MAX_TOKENS,
            "maximum_combined_production_tokens": MAX_COMBINED_TOKENS,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": "pif_app_server_capacity_policy_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [turn_name],
        "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": NEW_TURN_MAX_TOKENS,
        "phase_total_token_bound": NEW_TURN_MAX_TOKENS,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    reserve.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                *v243._runtime_files(),
                *v247._runtime_files(),
                Path(__file__).resolve(),
                Path(v243.__file__).resolve(),
                Path(v247.__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
                codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
            },
            key=str,
        )
    )


def _production_ratio(new_tokens: int) -> tuple[int, float]:
    combined = ADOPTED_CORE_TOKENS + new_tokens
    production_total = v243.PRODUCTION_AMORTIZED_CONTEXT_TOKENS + combined * v243.PRODUCTION_SCALE
    return production_total, production_total / v243.BASELINE_END_TO_END_TOKENS


def _final_gate(
    *, usage: Mapping[str, int], diagnostics: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    new_tokens = int(usage["total_tokens"])
    combined = ADOPTED_CORE_TOKENS + new_tokens
    production_total, ratio = _production_ratio(new_tokens)
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense_count = int((by_id.get(v243.DENSE_SEGMENT_ID) or {}).get("event_count", -1))
    residual_count = int((by_id.get(v243.NO_SIGNAL_SEGMENT_ID) or {}).get("event_count", -1))
    checks = {
        "adopted_core_gate_passed": True,
        "both_segments_validated": set(by_id)
        == {v243.DENSE_SEGMENT_ID, v243.NO_SIGNAL_SEGMENT_ID},
        "dense_event_count_27": dense_count == 27,
        "nominal_no_signal_event_count_1": residual_count == 1,
        "all_source_units_reviewed": all(
            int(row["source_unit_count"]) == int(row["reviewed_source_unit_count"])
            for row in diagnostics
        ),
        "unresolved_count_0": all(int(row["unresolved_count"]) == 0 for row in diagnostics),
        "all_literal_values_projected_from_exact_source_spans": True,
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "new_turn_tokens_lte_42000": new_tokens <= NEW_TURN_MAX_TOKENS,
        "combined_tokens_lte_72083": combined <= MAX_COMBINED_TOKENS,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": SCHEMA_VERSION,
        "phase_id": PHASE_ID,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "adopted_core_usage": {
            "total_tokens": ADOPTED_CORE_TOKENS,
            "source": _record(_v243_paths()["core_sidecar"]),
        },
        "new_turn_usage": dict(usage),
        "combined_tokens": combined,
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "dense_event_count": dense_count,
        "candidate_only_nominal_no_signal_event_count": residual_count,
        "residual_support_audit_required": residual_count > 0,
        "diagnostics": list(diagnostics),
        "support_alignment_authorized": not failed,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v248 runtime lock")
    root = path.parent.resolve()
    spec = _load_json(root / "attempt-spec.json", "v248 spec")
    paths = _turn_paths(root, spec["turn_name"])
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_new_turn_count") != 1
        or lock.get("adopted_predecessor_turn_count") != 1
        or lock.get("retry_count") != 0
        or lock.get("new_turn_max_tokens") != NEW_TURN_MAX_TOKENS
        or lock.get("max_combined_tokens") != MAX_COMBINED_TOKENS
        or lock.get("semantic_regex_or_keyword_filtering") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(value) for value in _runtime_files()}
        or {row["path"] for row in lock.get("request") or []}
        != {row["path"] for row in _request_records(paths)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
    ):
        raise V248SourceSpanEnrichmentError("v248 runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("authorization"),
        lock.get("ranking"),
        lock.get("design"),
        lock.get("spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        lock.get("adopted_core_receipt"),
        *(lock.get("request") or []),
    ]
    if any(not _verify_record(row or {}) for row in records):
        raise V248SourceSpanEnrichmentError("v248 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V248SourceSpanEnrichmentError("v248 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v248 spec")
    paths = _turn_paths(root, spec["turn_name"])
    lineage = _validate_lineage()
    turn = prepare_turn(lineage)
    return {
        "root": root,
        "spec_path": spec_path,
        "spec": spec,
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "lineage": lineage,
        "turn": {
            **turn,
            "private_input": _load_json(paths["input"], "v248 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v248 schema"),
            "paths": paths,
        },
    }


def freeze_v248(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v248 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V248SourceSpanEnrichmentError("unfinished v248 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    turn = prepare_turn(lineage)
    paths = _turn_paths(root, turn["turn_name"])
    paths["root"].mkdir(parents=True, exist_ok=True)
    _write_immutable(paths["input"], turn["private_input"])
    _write_private_text(paths["prompt"], turn["prompt"])
    _write_private_text(paths["base"], turn["base"])
    _write_immutable(paths["schema"], turn["schema"])
    capacity_paths = _capacity_policy(root, turn["turn_name"])
    authorization_path = root / "authorization.json"
    _write_stable_time(
        authorization_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "authority": "direct_operator_architecture_steering_2026_07_17",
            "scope": "one bounded source-span enrichment turn over immutable v243 core",
            "semantic_attempt_count": 1,
            "retry_count": 0,
            "isolated_field_patch": False,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    ranking_path = root / "architecture-ranking.json"
    _write_stable_time(
        ranking_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "selected_architecture_id": "frozen_core_then_source_span_full_schema_enrichment",
            "architectures": [
                {"rank": 1, "id": "frozen_core_then_source_span_full_schema_enrichment"},
                {"rank": 2, "id": "one_pass_full_schema_with_explicit_applicability_states"},
                {"rank": 3, "id": "source_graph_then_global_full_schema_canonicalizer"},
            ],
            "measured_basis": {
                "v243_core_tokens": ADOPTED_CORE_TOKENS,
                "v243_dense_events": 27,
                "v243_prior_combined_tokens": 62_120,
                "v243_prior_ratio": 0.244812,
                "v243_literal_violation_events": 1,
                "v239_tokens": 39_004,
                "v239_metric_applicability_violations": 4,
                "v247_v244_macro_f1": 0.680723,
                "v247_permutation_exact": False,
            },
            "on_failure": "freeze and reject v248; choose the next distinct architecture without a field patch",
        },
        "created_at",
    )
    projected_total, projected_ratio = _production_ratio(NEW_TURN_MAX_TOKENS)
    design_path = root / "architecture-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "architecture_id": "frozen_core_then_source_span_full_schema_enrichment",
            "hypothesis": (
                "reusing the measured full-semantic 27-event core while making the LLM select all "
                "literal metadata through exact source offsets will retain semantic coverage and remove "
                "the free-form literal failure within the 28 percent production token target"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "core_semantic_turn_reused_not_replayed": True,
                "new_semantic_turn_count": 1,
                "reference_visible_to_model": False,
                "target_count_visible_to_model": False,
            },
            "production_cost_projection": {
                "adopted_measured_core_tokens": ADOPTED_CORE_TOKENS,
                "new_turn_hard_max": NEW_TURN_MAX_TOKENS,
                "combined_hard_max": MAX_COMBINED_TOKENS,
                "production_amortized_context_tokens": v243.PRODUCTION_AMORTIZED_CONTEXT_TOKENS,
                "production_scale": v243.PRODUCTION_SCALE,
                "projected_production_amortized_total_tokens": projected_total,
                "projected_production_amortized_total_token_ratio": round(projected_ratio, 6),
                "required_ratio_max": 0.28,
            },
            "predeclared_stop_rules": {
                "retry_count": 0,
                "dense_event_count": 27,
                "nominal_no_signal_event_count": 1,
                "all_literal_fields_from_llm_selected_exact_source_spans": True,
                "metric_grounding_error_events": 0,
                "event_cap_violations": 0,
                "exact_identity_duplicates": 0,
                "new_turn_tokens_max": NEW_TURN_MAX_TOKENS,
                "production_amortized_total_token_ratio_max": 0.28,
            },
            "semantic_regex_or_keyword_filtering": False,
            "deterministic_semantic_decisions": False,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    adopted_core_path = root / "adopted-core-receipt.json"
    _write_stable_time(
        adopted_core_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "core_output": _record(_v243_paths()["core_output"]),
            "core_sidecar": _record(_v243_paths()["core_sidecar"]),
            "core_gate": _record(_v243_paths()["core_gate"]),
            "measured_core_tokens": ADOPTED_CORE_TOKENS,
            "core_replayed": False,
            "production_mutated": False,
        },
        "created_at",
    )
    spec_path = root / "attempt-spec.json"
    _write_stable_time(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "state": "frozen_before_one_turn_source_span_enrichment_canary",
            "turn_name": turn["turn_name"],
            "event_count": len(turn["event_ids"]),
            "model": MODEL,
            "effort": EFFORT,
            "declared_new_turn_count": 1,
            "adopted_predecessor_turn_count": 1,
            "retry_count": 0,
            "new_turn_max_tokens": NEW_TURN_MAX_TOKENS,
            "max_combined_tokens": MAX_COMBINED_TOKENS,
            "semantic_regex_or_keyword_filtering": False,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    lock_path = root / "runtime-lock.json"
    lock = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "frozen_at": now_iso(),
        "phase_id": PHASE_ID,
        "model": MODEL,
        "effort": EFFORT,
        "declared_new_turn_count": 1,
        "adopted_predecessor_turn_count": 1,
        "retry_count": 0,
        "new_turn_max_tokens": NEW_TURN_MAX_TOKENS,
        "max_combined_tokens": MAX_COMBINED_TOKENS,
        "semantic_regex_or_keyword_filtering": False,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
        "authorization": _record(authorization_path),
        "ranking": _record(ranking_path),
        "design": _record(design_path),
        "spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "adopted_core_receipt": _record(adopted_core_path),
        "request": _request_records(paths),
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "frozen_at")
    verify_runtime_lock(lock_path)
    return _load_frozen(root)


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    values = sidecar.get("usage") or {}
    try:
        usage = {field: int(values[field]) for field in USAGE_FIELDS}
    except (KeyError, TypeError, ValueError) as exc:
        raise V248SourceSpanEnrichmentError("v248 sidecar usage incomplete") from exc
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None
        or usage["total_tokens"] > NEW_TURN_MAX_TOKENS
    ):
        raise V248SourceSpanEnrichmentError("v248 sidecar accounting or auth failed")
    return usage


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(root: Path, frozen: Mapping[str, Any], exc: BaseException) -> dict[str, Any]:
    paths = frozen["turn"]["paths"]
    attempted = int(paths["capacity"].exists())
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = attempted
    sidecar_record = None
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        try:
            sidecar = _load_json(paths["sidecar"], "v248 sidecar")
            values = sidecar.get("usage") or {}
            usage = {field: int(values[field]) for field in USAGE_FIELDS}
            unknown = int(
                sidecar.get("usage_complete") is not True
                or sidecar.get("usage_status") != "measured"
            )
        except Exception:
            unknown = 1
    semantic = isinstance(exc, (V248OutputContractError, V248ArchitectureStop))
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v248_source_span_enrichment_structural_quality_or_cost_gate_not_passed"
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
        "new_turn_usage": usage,
        "adopted_core_usage": frozen["lineage"]["core_usage"],
        "unknown_usage_attempt_count": unknown,
        "sidecar": sidecar_record,
        "architecture_strategy_rejected": semantic,
        "next_distinct_architecture_authorized": semantic,
        "isolated_field_repair_authorized": False,
        "support_alignment_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "exact_next_action": (
            "advance to another distinct architecture"
            if semantic
            else "audit immutable v248 infrastructure attempt; no retry"
        ),
    }
    gate_path = root / "architecture-structural-gate.json"
    if gate_path.is_file():
        terminal["gate"] = _record(gate_path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v248(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v248 terminal")
    frozen = freeze_v248(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root, frozen, V248SourceSpanEnrichmentError("launch exists; replay prohibited")
        )
    _write_immutable(
        root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_new_turn_count": 1,
            "adopted_predecessor_turn_count": 1,
            "retry_count": 0,
            "model": MODEL,
            "effort": EFFORT,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "managed_chatgpt_auth_only": True,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
    )
    started = time.monotonic()
    paths = frozen["turn"]["paths"]
    try:
        async with client_factory(frozen["capacity_policy"]) as client:
            result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=frozen["turn"]["base"],
                prompt=frozen["turn"]["prompt"],
                output_schema=frozen["turn"]["schema"],
                cwd=PROJECT_ROOT,
                sidecar_path=paths["sidecar"],
                output_path=paths["output"],
                batch_size=len(frozen["turn"]["event_ids"]),
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=paths["capacity"],
            )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise V248SourceSpanEnrichmentError("v248 source-span turn did not complete")
        usage = _usage(_load_json(paths["sidecar"], "v248 sidecar"))
        normalized, provenance, diagnostics, span_receipts = project_output(
            result.output, frozen["turn"]
        )
        normalized_path = root / "normalized-output.private.json"
        provenance_path = root / "evidence-provenance.private.json"
        diagnostics_path = root / "diagnostics.private.json"
        span_receipts_path = root / "source-span-receipts.private.json"
        _write_immutable(normalized_path, normalized)
        _write_immutable(provenance_path, provenance)
        _write_immutable(diagnostics_path, {"segments": diagnostics})
        _write_immutable(span_receipts_path, span_receipts)
        gate = _final_gate(usage=usage, diagnostics=diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise V248ArchitectureStop("v248 structural quality or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v248_architecture_structural_gate_passed",
            "terminal_reason": "v248_source_span_enrichment_structural_cost_gate_passed",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "new_turn_usage": usage,
            "adopted_core_usage": frozen["lineage"]["core_usage"],
            "combined_tokens": gate["combined_tokens"],
            "production_amortized_total_token_ratio": gate[
                "production_amortized_total_token_ratio"
            ],
            "dense_event_count": gate["dense_event_count"],
            "candidate_only_nominal_no_signal_event_count": gate[
                "candidate_only_nominal_no_signal_event_count"
            ],
            "residual_support_audit_required": gate["residual_support_audit_required"],
            "support_alignment_authorized": True,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "wall_seconds": round(time.monotonic() - started, 6),
            "gate": _record(gate_path),
            "normalized_output": _record(normalized_path),
            "evidence_provenance": _record(provenance_path),
            "source_span_receipts": _record(span_receipts_path),
            "sidecar": _record(paths["sidecar"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": "run frozen source-support audit before any alignment or holdout",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v248 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v248 source-span enrichment")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v248(output_dir=Path(args.output_dir))
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "event_count": frozen["spec"]["event_count"],
            "projected_ratio": round(_production_ratio(NEW_TURN_MAX_TOKENS)[1], 6),
        }
    else:
        terminal = asyncio.run(
            run_v248(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "new_turn_tokens": (terminal.get("new_turn_usage") or {}).get("total_tokens"),
            "combined_tokens": terminal.get("combined_tokens"),
            "production_amortized_total_token_ratio": terminal.get(
                "production_amortized_total_token_ratio"
            ),
            "support_alignment_authorized": terminal.get("support_alignment_authorized", False),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
