from __future__ import annotations

"""Run the bounded v244 source-indexed event-graph architecture canary."""

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
from . import app_server_judge_v5_selection_v233_blind_unit_sweep as v233
from . import app_server_judge_v5_selection_v234_two_pass_blind_inventory as v234
from . import app_server_judge_v5_selection_v239_frontier_long_horizon as v239
from . import app_server_judge_v5_selection_v242_set_coded_reducer as v242
from . import app_server_judge_v5_selection_v243_core_enrichment as v243
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v244_source_indexed_graph_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v244_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v244_terminal_v1"
PHASE_ID = "development_selection_v5_4_v244_source_indexed_graph"
MODEL = "gpt-5.6-sol"
EFFORT = "high"
MAX_TOTAL_TOKENS = 45_000
MAX_EVENTS_PER_SEGMENT = 32
MAX_PROMPT_BYTES = 100_000
MAX_BASE_BYTES = 14_000
MAX_SCHEMA_BYTES = 60_000
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
BASELINE_END_TO_END_TOKENS = v233.BASELINE_END_TO_END_TOKENS
PRODUCTION_AMORTIZED_CONTEXT_TOKENS = v233.PRODUCTION_AMORTIZED_CONTEXT_TOKENS
PRODUCTION_SCALE = v233.PRODUCTION_SCALE
DENSE_SEGMENT_ID = v233.DENSE_SEGMENT_ID
NO_SIGNAL_SEGMENT_ID = v233.NO_SIGNAL_SEGMENT_ID
MIN_DENSE_EVENTS = v233.MIN_DENSE_EVENTS_FOR_FROZEN_SOURCE_FLOOR
USAGE_FIELDS = v233.USAGE_FIELDS
METRIC_LITERAL_FIELDS = (
    "metric_value",
    "metric_unit",
    "metric_comparator",
    "metric_raw_text",
)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
V239_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v239-frontier-long-horizon"
).resolve()
V243_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v243-core-enrichment"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v244-source-indexed-graph"
).resolve()
PINNED_CODEX_0_144_1 = v233.PINNED_CODEX_0_144_1


class V244SourceGraphError(RuntimeError):
    """The v244 architecture cannot proceed safely."""


class V244OutputContractError(V244SourceGraphError):
    """A completed graph output violated the frozen projection contract."""


class V244ArchitectureStop(V244SourceGraphError):
    """The v244 architecture did not clear its predeclared gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V244SourceGraphError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V244SourceGraphError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V244SourceGraphError(f"frozen {path.name} drifted")
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


def _lineage_paths() -> dict[str, Path]:
    source = v242._lineage_paths()
    return {
        "source_input": source["source_input"],
        "source_base": source["source_base"],
        "source_schema": source["source_schema"],
        "v239_terminal": V239_ROOT / "terminal.json",
        "v239_sidecar": next(V239_ROOT.glob("turns/*/sidecar.json")),
        "v239_audit": PIPELINE_ROOT / "v239-completed-output-audit-2026-07-17/report.json",
        "v243_terminal": V243_ROOT / "terminal.json",
        "v243_runtime_lock": V243_ROOT / "runtime-lock.json",
        "v243_core_sidecar": next(V243_ROOT.glob("turns/v243-core-*/sidecar.json")),
        "v243_enrichment_sidecar": next(
            V243_ROOT.glob("turns/v243-enrichment-*/sidecar.json")
        ),
        "v243_audit": PIPELINE_ROOT / "v243-completed-output-audit-2026-07-17/report.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    v239.verify_runtime_lock(V239_ROOT / "runtime-lock.json")
    v243.verify_runtime_lock(paths["v243_runtime_lock"])
    v239_terminal = _load_json(paths["v239_terminal"], "v239 terminal")
    v239_sidecar = _load_json(paths["v239_sidecar"], "v239 sidecar")
    v239_audit = _load_json(paths["v239_audit"], "v239 audit")
    v243_terminal = _load_json(paths["v243_terminal"], "v243 terminal")
    v243_core = _load_json(paths["v243_core_sidecar"], "v243 core sidecar")
    v243_enrichment = _load_json(
        paths["v243_enrichment_sidecar"], "v243 enrichment sidecar"
    )
    v243_audit = _load_json(paths["v243_audit"], "v243 audit")
    v239_counts = v239_audit.get("sanitized_structural_counts") or []
    if (
        v239_terminal.get("terminal_reason")
        != "v239_frontier_long_horizon_structural_or_count_gate_not_passed"
        or (v239_sidecar.get("usage") or {}).get("total_tokens") != 39_004
        or [row.get("event_count") for row in v239_counts] != [31, 1]
        or sum(int(row.get("exact_nonempty_evidence_count", 0)) for row in v239_counts)
        != 32
        or v243_terminal.get("terminal_reason")
        != "v243_core_enrichment_structural_quality_or_cost_gate_not_passed"
        or (v243_core.get("usage") or {}).get("total_tokens") != 30_083
        or (v243_enrichment.get("usage") or {}).get("total_tokens") != 32_037
        or v243_audit.get("metric_literal_violation_event_count") != 1
        or v243_audit.get("combined_tokens") != 62_120
        or v243_audit.get("next_distinct_architecture_authorized") is not True
    ):
        raise V244SourceGraphError("source-indexed graph evidence lineage drifted")
    packet = v234._source_packet(v234._validate_lineage())
    return {"paths": paths, "records": records, "packet": packet}


def _event_ids(position: int) -> list[str]:
    return [f"S{position}G{index:03d}" for index in range(MAX_EVENTS_PER_SEGMENT)]


def _output_schema(packet: Mapping[str, Any]) -> dict[str, Any]:
    source_segment = packet["source_schema"]["properties"]["segments"]["items"]
    all_unit_ids = list(packet["all_unit_ids"])
    all_event_ids = [value for position in range(2) for value in _event_ids(position)]
    event = v233._event_schema(packet["source_schema"], all_unit_ids)
    for name in METRIC_LITERAL_FIELDS:
        event["properties"].pop(name)
        event["required"].remove(name)
    event["properties"]["event_id"] = {
        "type": "string",
        "enum": all_event_ids,
    }
    event["properties"]["metric_literal_spans"] = {
        "type": "array",
        "minItems": 0,
        "maxItems": len(METRIC_LITERAL_FIELDS),
        "items": {
            "type": "object",
            "additionalProperties": False,
            "required": ["field", "unit_id", "exact_text"],
            "properties": {
                "field": {"type": "string", "enum": list(METRIC_LITERAL_FIELDS)},
                "unit_id": {"type": "string", "enum": all_unit_ids},
                "exact_text": {"type": "string", "minLength": 1, "maxLength": 500},
            },
        },
    }
    event["required"].extend(["event_id", "metric_literal_spans"])
    segment = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "segment_id",
            "status",
            "segment_source_context",
            "no_signal_reason",
            "coverage_audit",
            "events",
        ],
        "properties": {
            "segment_id": {"type": "string", "enum": list(packet["segment_ids"])},
            "status": copy.deepcopy(source_segment["properties"]["status"]),
            "segment_source_context": copy.deepcopy(
                source_segment["properties"]["segment_source_context"]
            ),
            "no_signal_reason": copy.deepcopy(
                source_segment["properties"]["no_signal_reason"]
            ),
            "coverage_audit": {
                "type": "object",
                "additionalProperties": False,
                "required": ["all_source_units_reviewed", "unresolved_count"],
                "properties": {
                    "all_source_units_reviewed": {"type": "boolean"},
                    "unresolved_count": {"type": "integer", "minimum": 0},
                },
            },
            "events": {
                "type": "array",
                "minItems": 0,
                "maxItems": MAX_EVENTS_PER_SEGMENT,
                "items": event,
            },
        },
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["episode_id", "segments"],
        "properties": {
            "episode_id": {"type": "string", "enum": [packet["episode_id"]]},
            "segments": {
                "type": "array",
                "minItems": 2,
                "maxItems": 2,
                "items": segment,
            },
        },
    }


BASE_INSTRUCTIONS = """You are a source-indexed event-graph extractor for a topic-general podcast research pipeline. Read every supplied source unit. You cannot see a reference, target count, density label, prior extraction, expected topic, or downstream answer. Never use keywords, regex, phrase lists, fixed topics, or event counts to decide meaning.

Extract every distinct, explicitly source-supported research event. Split independently truth-valued premises, mechanisms, capabilities, constraints, comparisons, outcomes, alternatives, frames, uncertainties, counterclaims, product or market signals, risks, adoption signals, term uses, and stance positions. Preserve distinct lenses when actor, reported actor, speaker, target, mechanism, stance, certainty, temporal horizon, attribution, or claim differs. Exclude greetings, logistics, acknowledgements, unasserted questions, bare mentions, and unsupported inference.

Each event is a semantic node linked to exact source units. Select the smallest contiguous evidence-unit range supporting every material field. Evidence must stay inside one fixed source window and one segment. Assign event IDs sequentially in source order as S0G000, S0G001, ... for the first segment and S1G000, S1G001, ... for the second.

Do not copy metric literals into ordinary event fields. For each nonempty canonical metric field, create exactly one metric_literal_spans row whose field names metric_value, metric_unit, metric_comparator, or metric_raw_text; unit_id selects a source unit inside the event evidence range; exact_text is an exact contiguous substring of that unit. Include metric_raw_text whenever any metric span exists. Use metric_direction=not_applicable if and only if metric_literal_spans is empty. The LLM alone decides whether a metric exists, its direction, every selected literal, and every evidence boundary. Deterministic code only verifies and projects your exact source references.

Set coverage_audit.all_source_units_reviewed=true and unresolved_count=0 only after reviewing all units. Use coded exactly when events is nonempty. Return schema-valid JSON only."""


def _prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    packet = lineage["packet"]
    prompt_packet = {
        "episode_id": packet["episode_id"],
        "segments": packet["prompt_segments"],
    }
    prompt = "# Blind source-indexed event graph packet\n" + json.dumps(
        prompt_packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"
    base = (
        BASE_INSTRUCTIONS
        + "\n\n# Immutable episode context\n"
        + json.dumps(packet["context"], ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )
    schema = _output_schema(packet)
    values = {
        "prompt_bytes": len(prompt.encode("utf-8")),
        "base_bytes": len(base.encode("utf-8")),
        "schema_bytes": len(_canonical_json(schema).encode("utf-8")),
    }
    if (
        values["prompt_bytes"] > MAX_PROMPT_BYTES
        or values["base_bytes"] > MAX_BASE_BYTES
        or values["schema_bytes"] > MAX_SCHEMA_BYTES
    ):
        raise V244SourceGraphError("v244 request size cap exceeded")
    return {
        **packet,
        "turn_name": "v244_source_graph_" + sha256_text(packet["episode_id"])[:20],
        "private_input": packet["private_input"],
        "prompt": prompt,
        "base": base,
        "schema": schema,
        **values,
    }


def _project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    try:
        _validate_schema(turn["schema"], output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V244OutputContractError("source graph output schema failed") from exc
    if output.get("episode_id") != turn["episode_id"]:
        raise V244OutputContractError("source graph episode id drifted")
    rows = list(output.get("segments") or [])
    if [row.get("segment_id") for row in rows] != list(turn["segment_ids"]):
        raise V244OutputContractError("source graph segment order or coverage drifted")
    source_by_id = {
        str(row["segment_id"]): row for row in turn["private_input"]["segments"]
    }
    projection_rows = []
    diagnostics = []
    span_receipts = []
    for position, row in enumerate(rows):
        segment_id = str(row["segment_id"])
        source = source_by_id[segment_id]
        units = list(source["units"])
        unit_index = {str(unit["unit_id"]): index for index, unit in enumerate(units)}
        unit_by_id = {str(unit["unit_id"]): unit for unit in units}
        audit = row.get("coverage_audit") or {}
        if (
            audit.get("all_source_units_reviewed") is not True
            or audit.get("unresolved_count") != 0
        ):
            raise V244OutputContractError("source graph coverage audit failed")
        raw_events = list(row["events"])
        if (row["status"] == "coded") != bool(raw_events):
            raise V244OutputContractError("source graph status does not match events")
        events = []
        start_counts = {str(unit["unit_id"]): 0 for unit in units}
        prior_start = -1
        for event_position, raw_event in enumerate(raw_events):
            event = dict(raw_event)
            event_id = str(event.pop("event_id"))
            if event_id != f"S{position}G{event_position:03d}":
                raise V244OutputContractError("source graph event id order drifted")
            spans = list(event.pop("metric_literal_spans"))
            start_id = str(event["evidence_start_unit_id"])
            end_id = str(event["evidence_end_unit_id"])
            if start_id not in unit_index or end_id not in unit_index:
                raise V244OutputContractError("source graph evidence belongs to another segment")
            start = unit_index[start_id]
            end = unit_index[end_id]
            if start < prior_start or end < start:
                raise V244OutputContractError("source graph evidence order drifted")
            if unit_by_id[start_id]["window_id"] != unit_by_id[end_id]["window_id"]:
                raise V244OutputContractError("source graph evidence crosses a fixed source window")
            prior_start = start
            metric_values = {name: "" for name in METRIC_LITERAL_FIELDS}
            event_span_receipts = []
            for span in spans:
                field = str(span["field"])
                unit_id = str(span["unit_id"])
                exact_text = str(span["exact_text"])
                if metric_values[field]:
                    raise V244OutputContractError("source graph metric field span duplicate")
                if unit_id not in unit_index or not (start <= unit_index[unit_id] <= end):
                    raise V244OutputContractError("source graph metric span is outside event evidence")
                if exact_text not in str(unit_by_id[unit_id]["text"]):
                    raise V244OutputContractError("source graph metric span is not exact")
                metric_values[field] = exact_text
                event_span_receipts.append(
                    {"field": field, "unit_id": unit_id, "exact_text_sha256": hashlib.sha256(exact_text.encode("utf-8")).hexdigest()}
                )
            if spans and not metric_values["metric_raw_text"]:
                raise V244OutputContractError("source graph metric spans omit raw text")
            if bool(spans) == (event.get("metric_direction") == "not_applicable"):
                raise V244OutputContractError("source graph metric direction applicability drifted")
            event.update(metric_values)
            events.append(event)
            start_counts[start_id] += 1
            span_receipts.append({"event_id": event_id, "spans": event_span_receipts})
        projection_rows.append(
            {
                "segment_id": segment_id,
                "status": row["status"],
                "segment_source_context": row["segment_source_context"],
                "no_signal_reason": row["no_signal_reason"],
                "unit_receipts": [
                    {
                        "unit_id": unit["unit_id"],
                        "eligible_event_count": start_counts[str(unit["unit_id"])],
                        "unresolved_count": 0,
                    }
                    for unit in units
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "events": events,
            }
        )
        diagnostics.append(
            {
                "segment_id": segment_id,
                "density_stratum": source["density_stratum"],
                "source_unit_count": len(units),
                "reviewed_source_unit_count": len(units),
                "event_count": len(events),
                "metric_span_count": sum(len(item["spans"]) for item in span_receipts if item["event_id"].startswith(f"S{position}G")),
                "unresolved_count": 0,
            }
        )
    all_unit_ids = [
        str(unit["unit_id"])
        for segment in turn["private_input"]["segments"]
        for unit in segment["units"]
    ]
    full_schema = v233._output_schema(
        episode_id=turn["episode_id"],
        segment_ids=turn["segment_ids"],
        source_schema=turn["source_schema"],
        unit_ids=all_unit_ids,
        max_unit_count=max(len(row["units"]) for row in turn["private_input"]["segments"]),
    )
    projection_turn = dict(turn)
    projection_turn["schema"] = full_schema
    try:
        normalized, provenance, _ = v233._project_output(
            {"episode_id": turn["episode_id"], "segments": projection_rows},
            projection_turn,
        )
    except v233.V233OutputContractError as exc:
        raise V244OutputContractError(str(exc)) from exc
    receipts = {
        "schema_version": SCHEMA_VERSION,
        "episode_id": turn["episode_id"],
        "events": span_receipts,
    }
    return normalized, provenance, diagnostics, receipts


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    values = sidecar.get("usage") or {}
    try:
        usage = {field: int(values[field]) for field in USAGE_FIELDS}
    except (KeyError, TypeError, ValueError) as exc:
        raise V244SourceGraphError("v244 sidecar usage incomplete") from exc
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None
        or usage["total_tokens"] > MAX_TOTAL_TOKENS
    ):
        raise V244SourceGraphError("v244 sidecar accounting or managed-auth contract failed")
    return usage


def _gate(*, usage: Mapping[str, int], diagnostics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    production_total = PRODUCTION_AMORTIZED_CONTEXT_TOKENS + int(usage["total_tokens"]) * PRODUCTION_SCALE
    ratio = production_total / BASELINE_END_TO_END_TOKENS
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense_count = int((by_id.get(DENSE_SEGMENT_ID) or {}).get("event_count", -1))
    residual_count = int((by_id.get(NO_SIGNAL_SEGMENT_ID) or {}).get("event_count", -1))
    checks = {
        "both_segments_validated": set(by_id) == {DENSE_SEGMENT_ID, NO_SIGNAL_SEGMENT_ID},
        "all_source_units_reviewed": all(
            int(row["source_unit_count"]) == int(row["reviewed_source_unit_count"])
            for row in diagnostics
        ),
        "unresolved_count_0": all(int(row["unresolved_count"]) == 0 for row in diagnostics),
        "dense_event_count_gte_24": dense_count >= MIN_DENSE_EVENTS,
        "exact_evidence_rate_1": True,
        "metric_source_reference_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "turn_tokens_lte_45000": int(usage["total_tokens"]) <= MAX_TOTAL_TOKENS,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": SCHEMA_VERSION,
        "phase_id": PHASE_ID,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "usage": dict(usage),
        "diagnostics": list(diagnostics),
        "dense_event_count": dense_count,
        "candidate_only_nominal_no_signal_event_count": residual_count,
        "residual_support_audit_required": residual_count > 0,
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "support_alignment_authorized": not failed,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


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
        "normalized": turn_root / "normalized-output.private.json",
        "provenance": turn_root / "evidence-provenance.private.json",
        "span_receipts": turn_root / "metric-span-receipts.private.json",
        "diagnostics": turn_root / "diagnostics.private.json",
    }


def _capacity_policy(root: Path, turn_name: str) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(MAX_TOTAL_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
            "phase_total_token_bound": MAX_TOTAL_TOKENS,
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
        "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
        "phase_total_token_bound": MAX_TOTAL_TOKENS,
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
                *v239._runtime_files(),
                *v243._runtime_files(),
                Path(__file__).resolve(),
                Path(v243.__file__).resolve(),
                Path(v239.__file__).resolve(),
                Path(v234.__file__).resolve(),
                Path(v233.__file__).resolve(),
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


def _request_records(root: Path, turn_name: str) -> list[dict[str, Any]]:
    paths = _turn_paths(root, turn_name)
    return [_record(paths[name]) for name in ("input", "prompt", "base", "schema")]


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v244 runtime lock")
    root = path.parent.resolve()
    turn_name = str(lock.get("turn_name") or "")
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("retry_count") != 0
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(value) for value in _runtime_files()}
        or {row["path"] for row in lock.get("frozen_request") or []}
        != {row["path"] for row in _request_records(root, turn_name)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V244SourceGraphError("v244 runtime lock contract drifted")
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
        *(lock.get("frozen_request") or []),
    ]
    if any(not _verify_record(row or {}) for row in records):
        raise V244SourceGraphError("v244 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V244SourceGraphError("v244 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v244 spec")
    paths = _turn_paths(root, spec["turn_name"])
    lineage = _validate_lineage()
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "design_path": root / "architecture-design.json",
        "turn": {
            "turn_name": spec["turn_name"],
            "episode_id": spec["episode_id"],
            "segment_ids": spec["segment_ids"],
            "private_input": _load_json(paths["input"], "v244 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v244 schema"),
            "source_schema": lineage["packet"]["source_schema"],
            "paths": paths,
        },
    }


def freeze_v244(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v244 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V244SourceGraphError("unfinished v244 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    turn = _prepare_turn(lineage)
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
            "scope": "one bounded source-indexed event-graph canary",
            "isolated_field_repair": False,
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
            "selected_architecture_id": "single_pass_source_indexed_event_graph",
            "architectures": [
                {"rank": 1, "id": "single_pass_source_indexed_event_graph"},
                {"rank": 2, "id": "independent_boundary_then_event_local_realization"},
                {"rank": 3, "id": "overlapping_window_maps_with_owner_projection"},
            ],
            "measured_basis": {
                "v239_high_dense_events": 31,
                "v239_high_tokens": 39_004,
                "v243_core_dense_events": 27,
                "v243_combined_tokens": 62_120,
                "v243_metric_literal_violation_events": 1,
            },
            "on_failure": "freeze and reject source-indexed graph; choose next distinct architecture",
        },
        "created_at",
    )
    projected_total = PRODUCTION_AMORTIZED_CONTEXT_TOKENS + MAX_TOTAL_TOKENS * PRODUCTION_SCALE
    design_path = root / "architecture-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "architecture_id": "single_pass_source_indexed_event_graph",
            "hypothesis": (
                "one frontier full-source semantic pass using exact source references for every literal "
                "field will preserve v239 recall while eliminating copied-literal drift and reducer cost"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "all_source_units_seen_by_llm": True,
                "reference_visible_to_model": False,
                "target_count_visible_to_model": False,
            },
            "production_cost_projection": {
                "turn_hard_max": MAX_TOTAL_TOKENS,
                "production_amortized_context_tokens": PRODUCTION_AMORTIZED_CONTEXT_TOKENS,
                "production_scale": PRODUCTION_SCALE,
                "projected_production_amortized_total_tokens": projected_total,
                "projected_production_amortized_total_token_ratio": round(
                    projected_total / BASELINE_END_TO_END_TOKENS, 6
                ),
                "required_ratio_max": 0.28,
            },
            "predeclared_stop_rules": {
                "retry_count": 0,
                "dense_event_count_minimum": MIN_DENSE_EVENTS,
                "exact_evidence_rate": 1.0,
                "metric_source_reference_errors": 0,
                "event_cap_violations": 0,
                "exact_identity_duplicates": 0,
                "production_amortized_total_token_ratio_max": 0.28,
            },
            "holdout_authorized": False,
            "production_mutation_allowed": False,
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
            "state": "frozen_before_one_turn_source_indexed_graph_canary",
            "declared_turn_count": 1,
            "turn_name": turn["turn_name"],
            "episode_id": turn["episode_id"],
            "segment_ids": turn["segment_ids"],
            "model": MODEL,
            "effort": EFFORT,
            "retry_count": 0,
            "maximum_total_tokens": MAX_TOTAL_TOKENS,
            "isolated_field_repair": False,
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
        "turn_name": turn["turn_name"],
        "declared_turn_count": 1,
        "retry_count": 0,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
        "authorization": _record(authorization_path),
        "ranking": _record(ranking_path),
        "design": _record(design_path),
        "spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "frozen_request": _request_records(root, turn["turn_name"]),
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "frozen_at")
    verify_runtime_lock(lock_path)
    return _load_frozen(root)


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    paths = frozen["turn"]["paths"]
    attempted = int(paths["capacity"].exists())
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = attempted
    sidecar_record = None
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        try:
            sidecar = _load_json(paths["sidecar"], "v244 sidecar")
            values = sidecar.get("usage") or {}
            usage = {field: int(values[field]) for field in USAGE_FIELDS}
            unknown = int(
                sidecar.get("usage_complete") is not True
                or sidecar.get("usage_status") != "measured"
            )
        except Exception:
            unknown = 1
    measured_cost_stop = (
        type(exc).__name__ == "ReserveCapacityError"
        and unknown == 0
        and int(usage["total_tokens"]) > MAX_TOTAL_TOKENS
    )
    semantic = isinstance(exc, (V244OutputContractError, V244ArchitectureStop)) or measured_cost_stop
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v244_source_indexed_graph_structural_quality_or_cost_gate_not_passed"
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
        "usage": usage,
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
            else "audit immutable infrastructure attempt; no retry"
        ),
    }
    gate_path = root / "architecture-structural-gate.json"
    if gate_path.is_file():
        terminal["gate"] = _record(gate_path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v244(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v244 terminal")
    frozen = freeze_v244(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(root, frozen, V244SourceGraphError("launch exists; replay prohibited"))
    _write_immutable(
        root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "turn_name": frozen["turn"]["turn_name"],
            "declared_turn_count": 1,
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
                batch_size=2,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=paths["capacity"],
            )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise V244SourceGraphError("source-indexed graph turn did not complete")
        usage = _usage(_load_json(paths["sidecar"], "v244 sidecar"))
        normalized, provenance, diagnostics, span_receipts = _project_output(
            result.output, frozen["turn"]
        )
        _write_immutable(paths["normalized"], normalized)
        _write_immutable(paths["provenance"], provenance)
        _write_immutable(paths["span_receipts"], span_receipts)
        _write_immutable(paths["diagnostics"], {"segments": diagnostics})
        gate = _gate(usage=usage, diagnostics=diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise V244ArchitectureStop("v244 structural quality or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v244_architecture_structural_gate_passed",
            "terminal_reason": "v244_source_indexed_graph_structural_cost_gate_passed",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
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
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "sidecar": _record(paths["sidecar"]),
            "exact_next_action": "run frozen side-free support and neutral alignment",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v244 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v244 source-indexed graph canary")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v244(output_dir=Path(args.output_dir))
        design = _load_json(frozen["design_path"], "v244 design")
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "projected_ratio": design["production_cost_projection"][
                "projected_production_amortized_total_token_ratio"
            ],
        }
    else:
        terminal = asyncio.run(
            run_v244(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "total_tokens": (terminal.get("usage") or {}).get("total_tokens"),
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
