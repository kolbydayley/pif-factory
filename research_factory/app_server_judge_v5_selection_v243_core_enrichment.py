from __future__ import annotations

"""Run the bounded v243 core-first extraction plus restricted enrichment canary."""

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
from . import app_server_judge_v5_selection_v242_set_coded_reducer as v242
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v243_core_enrichment_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v243_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v243_terminal_v1"
PHASE_ID = "development_selection_v5_4_v243_core_enrichment"
MODEL = "gpt-5.6-sol"
EFFORT = "low"
CORE_MAX_TOKENS = 34_000
ENRICHMENT_MAX_TOKENS = 39_904
MAX_COMBINED_TOKENS = 73_904
CAPACITY_PHASE_TOKEN_BOUND = 2 * ENRICHMENT_MAX_TOKENS
MAX_EVENTS_PER_SEGMENT = 32
MAX_PROMPT_BYTES = 100_000
MAX_BASE_BYTES = 12_000
MAX_SCHEMA_BYTES = 50_000
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
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
V234_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v234-two-pass-blind-inventory-realization"
).resolve()
V235_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v235-support-critic-realization"
).resolve()
V242_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v242-set-coded-reducer"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v243-core-enrichment"
).resolve()
PINNED_CODEX_0_144_1 = v233.PINNED_CODEX_0_144_1

CORE_FIELDS = (
    "event_type",
    "actor_name",
    "speaker_name",
    "reported_actor_name",
    "source_context_kind",
    "target_concept",
    "claim_text",
    "stance",
    "certainty",
    "temporal_horizon",
    "causal_mechanism",
    "counterclaim",
    "evidence_start_unit_id",
    "evidence_end_unit_id",
)
ENRICHMENT_FIELDS = (
    "event_subtype",
    "claim_type",
    "actor_type",
    "speaker_role",
    "reported_actor_type",
    "metric_value",
    "metric_unit",
    "metric_comparator",
    "metric_direction",
    "metric_raw_text",
    "signal_reason",
    "model_names",
    "product_names",
    "organizations",
    "people",
    "confidence",
)


class V243CoreEnrichmentError(RuntimeError):
    """The v243 architecture cannot proceed safely."""


class V243OutputContractError(V243CoreEnrichmentError):
    """A completed v243 output violated its frozen semantic boundary."""


class V243ArchitectureStop(V243CoreEnrichmentError):
    """The v243 architecture did not clear a predeclared gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V243CoreEnrichmentError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V243CoreEnrichmentError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V243CoreEnrichmentError(f"frozen {path.name} drifted")
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
        "v234_terminal": V234_ROOT / "terminal.json",
        "v234_gate": V234_ROOT / "inventory-structural-gate.json",
        "v234_sidecar": next(V234_ROOT.glob("turns/*/sidecar.json")),
        "v235_audit": PIPELINE_ROOT / "v235-completed-output-audit-2026-07-17/report.json",
        "v242_terminal": V242_ROOT / "terminal.json",
        "v242_runtime_lock": V242_ROOT / "runtime-lock.json",
        "v242_sidecar": next(V242_ROOT.glob("turns/*/sidecar.json")),
        "v242_audit": PIPELINE_ROOT / "v242-completed-output-audit-2026-07-17/report.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    v242.verify_runtime_lock(paths["v242_runtime_lock"])
    v234_terminal = _load_json(paths["v234_terminal"], "v234 terminal")
    v234_gate = _load_json(paths["v234_gate"], "v234 gate")
    v234_sidecar = _load_json(paths["v234_sidecar"], "v234 sidecar")
    v235_audit = _load_json(paths["v235_audit"], "v235 audit")
    v242_terminal = _load_json(paths["v242_terminal"], "v242 terminal")
    v242_sidecar = _load_json(paths["v242_sidecar"], "v242 sidecar")
    v242_audit = _load_json(paths["v242_audit"], "v242 audit")
    if (
        v234_terminal.get("terminal_reason")
        != "v234_two_pass_architecture_structural_or_count_gate_not_passed"
        or v234_gate.get("dense_proposition_count") != 32
        or v234_gate.get("no_signal_proposition_count") != 1
        or (v234_sidecar.get("usage") or {}).get("total_tokens") != 29_362
        or v235_audit.get("combined_total_tokens") != 67_569
        or v235_audit.get("metric_applicability_violation_count") != 5
        or v242_terminal.get("terminal_reason")
        != "v242_set_coded_reducer_structural_quality_or_cost_gate_not_passed"
        or (v242_sidecar.get("usage") or {}).get("total_tokens") != 36_655
        or v242_audit.get("structural_quality_proxy_passed") is not True
        or v242_audit.get("production_amortized_total_token_ratio") != 0.285165
        or v242_audit.get("next_distinct_architecture_authorized") is not True
    ):
        raise V243CoreEnrichmentError("architecture evidence lineage drifted")
    packet = v234._source_packet(v234._validate_lineage())
    return {"paths": paths, "records": records, "packet": packet}


def _event_ids(segment_position: int) -> list[str]:
    return [f"S{segment_position}E{index:03d}" for index in range(MAX_EVENTS_PER_SEGMENT)]


def _core_schema(packet: Mapping[str, Any]) -> dict[str, Any]:
    source_segment = packet["source_schema"]["properties"]["segments"]["items"]
    source_event = source_segment["properties"]["events"]["items"]
    all_unit_ids = list(packet["all_unit_ids"])
    all_event_ids = [value for position in range(2) for value in _event_ids(position)]
    event = {
        "type": "object",
        "additionalProperties": False,
        "required": ["event_id", *CORE_FIELDS],
        "properties": {
            "event_id": {"type": "string", "enum": all_event_ids},
            **{
                name: copy.deepcopy(source_event["properties"][name])
                for name in CORE_FIELDS
                if name not in {"evidence_start_unit_id", "evidence_end_unit_id"}
            },
        },
    }
    event["properties"]["evidence_start_unit_id"] = {
        "type": "string",
        "enum": all_unit_ids,
    }
    event["properties"]["evidence_end_unit_id"] = {
        "type": "string",
        "enum": all_unit_ids,
    }
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


CORE_INSTRUCTIONS = """You are the core semantic extraction stage for a topic-general podcast event pipeline. Read every supplied source unit. You cannot see a reference, target count, density label, prior event map, expected topic, or downstream answer. Never use keywords, regex, phrase lists, fixed topics, or counts to decide meaning.

Extract every distinct, explicitly source-supported research event. Split truth-conditionally independent premises, mechanisms, capabilities, constraints, comparisons, outcomes, alternatives, frames, uncertainties, counterclaims, product or market signals, risks, adoption signals, term uses, and stance positions. Preserve distinct lenses when they assert different actors, targets, mechanisms, time horizons, stances, or claims. Exclude greetings, logistics, acknowledgements, unasserted questions, bare mentions, and unsupported inference.

This pass owns event discovery, event boundary, evidence unit IDs, event type, speaker, actor, reported actor, attribution context, target, claim, stance, certainty, temporal horizon, causal mechanism, and counterclaim. A later restricted pass can only fill fields absent from this schema and cannot delete, relabel, merge, split, or override your core events.

Assign event IDs sequentially in source order as S0E000, S0E001, ... for the first segment and S1E000, S1E001, ... for the second. Select the smallest self-contained contiguous unit range supporting every material core field, including adjacent attribution or coreference only when needed. Evidence must stay inside one source window and one segment. Set coverage_audit.all_source_units_reviewed=true and unresolved_count=0 only after reviewing all units. Use coded exactly when events is nonempty. Return schema-valid JSON only."""


def _prepare_core_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    packet = lineage["packet"]
    prompt_packet = {
        "episode_id": packet["episode_id"],
        "segments": packet["prompt_segments"],
    }
    prompt = "# Blind source-unit core extraction packet\n" + json.dumps(
        prompt_packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"
    base = (
        CORE_INSTRUCTIONS
        + "\n\n# Immutable episode context\n"
        + json.dumps(packet["context"], ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )
    schema = _core_schema(packet)
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
        raise V243CoreEnrichmentError("v243 core request size cap exceeded")
    return {
        **packet,
        "turn_name": "v243_core_" + sha256_text(packet["episode_id"])[:20],
        "private_input": {
            "schema_version": SCHEMA_VERSION,
            "episode_id": packet["episode_id"],
            "segments": packet["private_input"]["segments"],
            "privacy": "private source units and context",
        },
        "prompt": prompt,
        "base": base,
        "schema": schema,
        **values,
    }


def _core_projection(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    try:
        _validate_schema(turn["schema"], output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V243OutputContractError("core output schema failed") from exc
    if output.get("episode_id") != turn["episode_id"]:
        raise V243OutputContractError("core episode id drifted")
    rows = list(output.get("segments") or [])
    if [row.get("segment_id") for row in rows] != list(turn["segment_ids"]):
        raise V243OutputContractError("core segment order or coverage drifted")
    source_by_id = {
        str(row["segment_id"]): row for row in turn["private_input"]["segments"]
    }
    seen_ids: set[str] = set()
    identities: set[str] = set()
    diagnostics = []
    canonical_rows = []
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
            raise V243OutputContractError("core coverage audit failed")
        events = [dict(event) for event in row["events"]]
        if (row["status"] == "coded") != bool(events):
            raise V243OutputContractError("core status does not match events")
        expected_prefix = f"S{position}E"
        previous_start = -1
        for index, event in enumerate(events):
            event_id = str(event["event_id"])
            if event_id != f"{expected_prefix}{index:03d}" or event_id in seen_ids:
                raise V243OutputContractError("core event id order or uniqueness drifted")
            seen_ids.add(event_id)
            start = str(event["evidence_start_unit_id"])
            end = str(event["evidence_end_unit_id"])
            if start not in unit_index or end not in unit_index:
                raise V243OutputContractError("core evidence belongs to another segment")
            start_index = unit_index[start]
            end_index = unit_index[end]
            if start_index < previous_start or end_index < start_index:
                raise V243OutputContractError("core evidence order drifted")
            if unit_by_id[start]["window_id"] != unit_by_id[end]["window_id"]:
                raise V243OutputContractError("core evidence crosses a fixed source window")
            previous_start = start_index
            identity = _canonical_json({key: event[key] for key in CORE_FIELDS})
            if identity in identities:
                raise V243OutputContractError("core exact identity duplicate")
            identities.add(identity)
        canonical_rows.append(copy.deepcopy(row))
        diagnostics.append(
            {
                "segment_id": segment_id,
                "density_stratum": source["density_stratum"],
                "source_unit_count": len(units),
                "reviewed_source_unit_count": len(units),
                "event_count": len(events),
                "unresolved_count": 0,
            }
        )
    return {"episode_id": turn["episode_id"], "segments": canonical_rows}, diagnostics


def _core_gate(*, usage: Mapping[str, int], diagnostics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
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
        "exact_evidence_unit_ranges_valid": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "core_turn_tokens_lte_34000": int(usage["total_tokens"]) <= CORE_MAX_TOKENS,
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
        "enrichment_authorized": not failed,
        "holdout_authorized": False,
        "production_mutated": False,
    }


ENRICHMENT_INSTRUCTIONS = """You are the restricted enrichment stage for a topic-general podcast event pipeline. A prior blind LLM core pass has frozen event identity, count, order, evidence boundaries, event type, speaker, actor, reported actor, attribution context, target, claim, stance, certainty, temporal horizon, causal mechanism, and counterclaim. You cannot delete, add, merge, split, relabel, or override any core field.

For every supplied core_event_id, read its exact evidence units and core fields, then fill only the requested absent fields. Every output row must correspond to exactly one core event in input order. Make all enrichment decisions from meaning; never use keywords, regex, phrase lists, fixed topics, or counts.

Every nonempty metric_value, metric_unit, metric_comparator, and metric_raw_text must be a literal contiguous substring of the exact evidence. Set metric_direction=not_applicable if and only if all four metric strings are empty. Entity arrays contain only entities explicitly supported by the evidence. Return schema-valid JSON only."""


def _enrichment_schema(packet: Mapping[str, Any], event_ids: Sequence[str]) -> dict[str, Any]:
    source_event = packet["source_schema"]["properties"]["segments"]["items"][
        "properties"
    ]["events"]["items"]
    event = {
        "type": "object",
        "additionalProperties": False,
        "required": ["core_event_id", *ENRICHMENT_FIELDS],
        "properties": {
            "core_event_id": {"type": "string", "enum": list(event_ids)},
            **{
                name: copy.deepcopy(source_event["properties"][name])
                for name in ENRICHMENT_FIELDS
            },
        },
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


def _prepare_enrichment_turn(
    packet: Mapping[str, Any], core: Mapping[str, Any]
) -> dict[str, Any]:
    source_by_id = {
        str(row["segment_id"]): row for row in packet["private_input"]["segments"]
    }
    prompt_events = []
    event_ids = []
    for segment in core["segments"]:
        source = source_by_id[str(segment["segment_id"])]
        units = list(source["units"])
        unit_index = {str(unit["unit_id"]): index for index, unit in enumerate(units)}
        for event in segment["events"]:
            start = unit_index[str(event["evidence_start_unit_id"])]
            end = unit_index[str(event["evidence_end_unit_id"])]
            event_ids.append(str(event["event_id"]))
            prompt_events.append(
                {
                    "segment_id": segment["segment_id"],
                    "core_event": event,
                    "exact_evidence_units": [
                        {
                            "unit_id": unit["unit_id"],
                            "text": unit["text"],
                        }
                        for unit in units[start : end + 1]
                    ],
                }
            )
    prompt_packet = {"episode_id": packet["episode_id"], "events": prompt_events}
    prompt = "# Frozen core-event enrichment packet\n" + json.dumps(
        prompt_packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"
    base = (
        ENRICHMENT_INSTRUCTIONS
        + "\n\n# Immutable episode context\n"
        + json.dumps(packet["context"], ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )
    schema = _enrichment_schema(packet, event_ids)
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
        raise V243CoreEnrichmentError("v243 enrichment request size cap exceeded")
    return {
        "turn_name": "v243_enrichment_" + sha256_text(packet["episode_id"])[:20],
        "episode_id": packet["episode_id"],
        "event_ids": event_ids,
        "private_input": prompt_packet,
        "prompt": prompt,
        "base": base,
        "schema": schema,
        **values,
    }


def _final_projection(
    enrichment: Mapping[str, Any], core: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    try:
        _validate_schema(turn["schema"], enrichment, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V243OutputContractError("enrichment output schema failed") from exc
    if enrichment.get("episode_id") != turn["episode_id"]:
        raise V243OutputContractError("enrichment episode id drifted")
    rows = list(enrichment.get("events") or [])
    if [row.get("core_event_id") for row in rows] != list(turn["event_ids"]):
        raise V243OutputContractError("enrichment event order or coverage drifted")
    enrichment_by_id = {str(row["core_event_id"]): row for row in rows}
    packet = turn["packet"]
    source_by_id = {
        str(row["segment_id"]): row for row in packet["private_input"]["segments"]
    }
    projection_rows = []
    diagnostics = []
    for segment in core["segments"]:
        segment_id = str(segment["segment_id"])
        source = source_by_id[segment_id]
        units = list(source["units"])
        receipt_counts = {str(unit["unit_id"]): 0 for unit in units}
        events = []
        for core_event in segment["events"]:
            event_id = str(core_event["event_id"])
            enriched = enrichment_by_id[event_id]
            event = {name: copy.deepcopy(core_event[name]) for name in CORE_FIELDS}
            event.update(
                {
                    name: copy.deepcopy(enriched[name])
                    for name in ENRICHMENT_FIELDS
                }
            )
            events.append(event)
            receipt_counts[str(event["evidence_start_unit_id"])] += 1
        projection_rows.append(
            {
                "segment_id": segment_id,
                "status": segment["status"],
                "segment_source_context": segment["segment_source_context"],
                "no_signal_reason": segment["no_signal_reason"],
                "unit_receipts": [
                    {
                        "unit_id": unit["unit_id"],
                        "eligible_event_count": receipt_counts[str(unit["unit_id"])],
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
                "unresolved_count": 0,
            }
        )
    all_unit_ids = [
        str(unit["unit_id"])
        for segment in packet["private_input"]["segments"]
        for unit in segment["units"]
    ]
    full_schema = v233._output_schema(
        episode_id=packet["episode_id"],
        segment_ids=packet["segment_ids"],
        source_schema=packet["source_schema"],
        unit_ids=all_unit_ids,
        max_unit_count=max(len(row["units"]) for row in packet["private_input"]["segments"]),
    )
    projection_turn = {
        "episode_id": packet["episode_id"],
        "segment_ids": packet["segment_ids"],
        "private_input": packet["private_input"],
        "schema": full_schema,
    }
    try:
        normalized, provenance, _ = v233._project_output(
            {"episode_id": packet["episode_id"], "segments": projection_rows},
            projection_turn,
        )
    except v233.V233OutputContractError as exc:
        raise V243OutputContractError(str(exc)) from exc
    return normalized, provenance, diagnostics


def _usage(sidecar: Mapping[str, Any], *, max_tokens: int) -> dict[str, int]:
    values = sidecar.get("usage") or {}
    try:
        usage = {field: int(values[field]) for field in USAGE_FIELDS}
    except (KeyError, TypeError, ValueError) as exc:
        raise V243CoreEnrichmentError("sidecar usage incomplete") from exc
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None
        or usage["total_tokens"] > max_tokens
    ):
        raise V243CoreEnrichmentError("sidecar accounting or managed-auth contract failed")
    return usage


def _final_gate(
    *, core_usage: Mapping[str, int], enrichment_usage: Mapping[str, int],
    diagnostics: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    combined = int(core_usage["total_tokens"]) + int(enrichment_usage["total_tokens"])
    production_total = PRODUCTION_AMORTIZED_CONTEXT_TOKENS + combined * PRODUCTION_SCALE
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
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "core_turn_tokens_lte_34000": int(core_usage["total_tokens"]) <= CORE_MAX_TOKENS,
        "enrichment_turn_tokens_lte_39904": int(enrichment_usage["total_tokens"])
        <= ENRICHMENT_MAX_TOKENS,
        "combined_tokens_lte_73904": combined <= MAX_COMBINED_TOKENS,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": SCHEMA_VERSION,
        "phase_id": PHASE_ID,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "core_usage": dict(core_usage),
        "enrichment_usage": dict(enrichment_usage),
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


def _write_turn_request(paths: Mapping[str, Path], turn: Mapping[str, Any]) -> None:
    paths["root"].mkdir(parents=True, exist_ok=True)
    _write_immutable(paths["input"], turn["private_input"])
    _write_private_text(paths["prompt"], turn["prompt"])
    _write_private_text(paths["base"], turn["base"])
    _write_immutable(paths["schema"], turn["schema"])


def _request_records(paths: Mapping[str, Path]) -> list[dict[str, Any]]:
    return [_record(paths[name]) for name in ("input", "prompt", "base", "schema")]


def _capacity_policy(root: Path, turn_names: Sequence[str]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        CAPACITY_PHASE_TOKEN_BOUND * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_turn_count": 2,
            "core_total_token_bound": CORE_MAX_TOKENS,
            "enrichment_total_token_bound": ENRICHMENT_MAX_TOKENS,
            "architecture_combined_token_bound": MAX_COMBINED_TOKENS,
            "capacity_phase_token_bound": CAPACITY_PHASE_TOKEN_BOUND,
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
        "ordered_turn_names": list(turn_names),
        "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": ENRICHMENT_MAX_TOKENS,
        "phase_total_token_bound": CAPACITY_PHASE_TOKEN_BOUND,
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
                *v234._runtime_files(),
                *v242._runtime_files(),
                Path(__file__).resolve(),
                Path(v242.__file__).resolve(),
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


def _enrichment_template() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "instructions": ENRICHMENT_INSTRUCTIONS,
        "model": MODEL,
        "effort": EFFORT,
        "fields": list(ENRICHMENT_FIELDS),
        "core_fields_immutable": list(CORE_FIELDS),
        "maximum_total_tokens": ENRICHMENT_MAX_TOKENS,
    }


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v243 runtime lock")
    root = path.parent.resolve()
    spec = _load_json(root / "attempt-spec.json", "v243 spec")
    core_paths = _turn_paths(root, spec["core_turn_name"])
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 2
        or lock.get("retry_count") != 0
        or lock.get("phase_total_token_bound") != MAX_COMBINED_TOKENS
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(value) for value in _runtime_files()}
        or {row["path"] for row in lock.get("core_request") or []}
        != {row["path"] for row in _request_records(core_paths)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V243CoreEnrichmentError("v243 runtime lock contract drifted")
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
        lock.get("enrichment_template"),
        *(lock.get("core_request") or []),
    ]
    if any(not _verify_record(row or {}) for row in records):
        raise V243CoreEnrichmentError("v243 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V243CoreEnrichmentError("v243 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v243 spec")
    core_paths = _turn_paths(root, spec["core_turn_name"])
    lineage = _validate_lineage()
    packet = lineage["packet"]
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "design_path": root / "architecture-design.json",
        "packet": packet,
        "core": {
            "turn_name": spec["core_turn_name"],
            "episode_id": spec["episode_id"],
            "segment_ids": spec["segment_ids"],
            "private_input": _load_json(core_paths["input"], "v243 core input"),
            "prompt": core_paths["prompt"].read_text(encoding="utf-8"),
            "base": core_paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(core_paths["schema"], "v243 core schema"),
            "paths": core_paths,
        },
    }


def freeze_v243(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v243 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V243CoreEnrichmentError("unfinished v243 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    core = _prepare_core_turn(lineage)
    core_paths = _turn_paths(root, core["turn_name"])
    _write_turn_request(core_paths, core)
    enrichment_turn_name = "v243_enrichment_" + sha256_text(core["episode_id"])[:20]
    capacity_paths = _capacity_policy(root, [core["turn_name"], enrichment_turn_name])
    authorization_path = root / "authorization.json"
    _write_stable_time(
        authorization_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "authority": "direct_operator_architecture_steering_2026_07_17",
            "scope": "one bounded two-turn core-first extraction and restricted enrichment canary",
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
            "selected_architecture_id": "core_source_pointer_extraction_then_restricted_enrichment",
            "architectures": [
                {"rank": 1, "id": "core_source_pointer_extraction_then_restricted_enrichment"},
                {"rank": 2, "id": "independent_boundary_then_full_semantic_realization"},
                {"rank": 3, "id": "overlapping_window_maps_with_owner_projection"},
            ],
            "measured_basis": {
                "v234_core_like_inventory_tokens": 29_362,
                "v234_dense_propositions": 32,
                "v235_combined_tokens": 67_569,
                "v242_structural_quality_proxy_passed": True,
                "v242_ratio": 0.285165,
            },
            "on_failure": "freeze and reject v243; select the next distinct architecture without a field patch",
        },
        "created_at",
    )
    projected_total = PRODUCTION_AMORTIZED_CONTEXT_TOKENS + MAX_COMBINED_TOKENS * PRODUCTION_SCALE
    design_path = root / "architecture-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "architecture_id": "core_source_pointer_extraction_then_restricted_enrichment",
            "hypothesis": (
                "separating full-source event identity and exact source pointers from non-overriding "
                "metadata enrichment will preserve dense recall and exactness while keeping the complete "
                "production path at or below 28 percent of baseline tokens"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "all_source_units_seen_by_core_llm": True,
                "reference_visible_to_model": False,
                "target_count_visible_to_model": False,
            },
            "production_cost_projection": {
                "core_turn_hard_max": CORE_MAX_TOKENS,
                "enrichment_turn_hard_max": ENRICHMENT_MAX_TOKENS,
                "combined_hard_max": MAX_COMBINED_TOKENS,
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
                "core_dense_event_count_minimum": MIN_DENSE_EVENTS,
                "core_exact_evidence_unit_ranges": True,
                "enrichment_cannot_override_core": True,
                "final_exact_evidence_rate": 1.0,
                "final_metric_grounding_error_events": 0,
                "event_cap_violations": 0,
                "exact_identity_duplicates": 0,
                "production_amortized_total_token_ratio_max": 0.28,
            },
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    template_path = root / "enrichment-template.json"
    _write_immutable(template_path, _enrichment_template())
    spec_path = root / "attempt-spec.json"
    _write_stable_time(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "state": "frozen_before_conditional_two_turn_core_enrichment_canary",
            "declared_turn_count": 2,
            "core_turn_name": core["turn_name"],
            "enrichment_turn_name": enrichment_turn_name,
            "episode_id": core["episode_id"],
            "segment_ids": core["segment_ids"],
            "model": MODEL,
            "effort": EFFORT,
            "retry_count": 0,
            "core_max_tokens": CORE_MAX_TOKENS,
            "enrichment_max_tokens": ENRICHMENT_MAX_TOKENS,
            "combined_max_tokens": MAX_COMBINED_TOKENS,
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
        "declared_turn_count": 2,
        "retry_count": 0,
        "phase_total_token_bound": MAX_COMBINED_TOKENS,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
        "authorization": _record(authorization_path),
        "ranking": _record(ranking_path),
        "design": _record(design_path),
        "spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "enrichment_template": _record(template_path),
        "core_request": _request_records(core_paths),
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "frozen_at")
    verify_runtime_lock(lock_path)
    return _load_frozen(root)


def _freeze_enrichment_request(
    root: Path, frozen: Mapping[str, Any], core: Mapping[str, Any]
) -> dict[str, Any]:
    turn = _prepare_enrichment_turn(frozen["packet"], core)
    paths = _turn_paths(root, turn["turn_name"])
    _write_turn_request(paths, turn)
    lock_path = root / "enrichment-request-lock.json"
    lock = {
        "schema_version": SCHEMA_VERSION,
        "frozen_at": now_iso(),
        "turn_name": turn["turn_name"],
        "core_output": _record(frozen["core"]["paths"]["output"]),
        "core_gate": _record(root / "core-structural-gate.json"),
        "runtime_lock": _record(frozen["runtime_lock"]),
        "enrichment_template": _record(root / "enrichment-template.json"),
        "request": _request_records(paths),
        "retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "frozen_at")
    records = [
        lock["core_output"],
        lock["core_gate"],
        lock["runtime_lock"],
        lock["enrichment_template"],
        *lock["request"],
    ]
    if any(not _verify_record(row) for row in records):
        raise V243CoreEnrichmentError("enrichment request lock drifted")
    return {**turn, "paths": paths, "request_lock": lock_path, "packet": frozen["packet"]}


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _sidecar_usage_or_unknown(path: Path) -> tuple[dict[str, int], bool]:
    zeros = {field: 0 for field in USAGE_FIELDS}
    if not path.is_file():
        return zeros, True
    try:
        sidecar = _load_json(path, "v243 sidecar")
        values = sidecar.get("usage") or {}
        usage = {field: int(values[field]) for field in USAGE_FIELDS}
        complete = sidecar.get("usage_complete") is True and sidecar.get("usage_status") == "measured"
        return usage, not complete
    except Exception:
        return zeros, True


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    core_paths = frozen["core"]["paths"]
    enrichment_paths = _turn_paths(root, frozen["spec"]["enrichment_turn_name"])
    attempted_paths = [paths for paths in (core_paths, enrichment_paths) if paths["capacity"].exists()]
    usages = []
    unknown = 0
    sidecars = []
    for paths in attempted_paths:
        usage, is_unknown = _sidecar_usage_or_unknown(paths["sidecar"])
        usages.append(usage)
        unknown += int(is_unknown)
        if paths["sidecar"].is_file():
            sidecars.append(_record(paths["sidecar"]))
    combined = None if unknown else sum(item["total_tokens"] for item in usages)
    semantic = isinstance(exc, (V243OutputContractError, V243ArchitectureStop))
    if (
        type(exc).__name__ == "ReserveCapacityError"
        and unknown == 0
        and combined is not None
        and combined > MAX_COMBINED_TOKENS
    ):
        semantic = True
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v243_core_enrichment_structural_quality_or_cost_gate_not_passed"
            if semantic
            else "infrastructure_or_judge_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": len(attempted_paths),
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "turn_usages": usages,
        "combined_tokens": combined,
        "unknown_usage_attempt_count": unknown,
        "sidecars": sidecars,
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
    for name in ("core-structural-gate.json", "architecture-structural-gate.json"):
        path = root / name
        if path.is_file():
            terminal[name.removesuffix(".json").replace("-", "_")] = _record(path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v243(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v243 terminal")
    frozen = freeze_v243(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(root, frozen, V243CoreEnrichmentError("launch exists; replay prohibited"))
    _write_immutable(
        root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_turn_count": 2,
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
    try:
        async with client_factory(frozen["capacity_policy"]) as client:
            core_result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=frozen["core"]["base"],
                prompt=frozen["core"]["prompt"],
                output_schema=frozen["core"]["schema"],
                cwd=PROJECT_ROOT,
                sidecar_path=frozen["core"]["paths"]["sidecar"],
                output_path=frozen["core"]["paths"]["output"],
                batch_size=2,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=frozen["core"]["paths"]["capacity"],
            )
            if core_result.status_ok is not True or not isinstance(core_result.output, Mapping):
                raise V243CoreEnrichmentError("core turn did not complete")
            core_usage = _usage(
                _load_json(frozen["core"]["paths"]["sidecar"], "v243 core sidecar"),
                max_tokens=CORE_MAX_TOKENS,
            )
            core, core_diagnostics = _core_projection(core_result.output, frozen["core"])
            _write_immutable(root / "core-output.private.json", core)
            _write_immutable(root / "core-diagnostics.private.json", {"segments": core_diagnostics})
            core_gate = _core_gate(usage=core_usage, diagnostics=core_diagnostics)
            _write_immutable(root / "core-structural-gate.json", core_gate)
            if not core_gate["passed"]:
                raise V243ArchitectureStop("v243 core structural or cost gate failed")
            enrichment = _freeze_enrichment_request(root, frozen, core)
            enrichment_result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=enrichment["base"],
                prompt=enrichment["prompt"],
                output_schema=enrichment["schema"],
                cwd=PROJECT_ROOT,
                sidecar_path=enrichment["paths"]["sidecar"],
                output_path=enrichment["paths"]["output"],
                batch_size=len(enrichment["event_ids"]),
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=enrichment["paths"]["capacity"],
            )
        if enrichment_result.status_ok is not True or not isinstance(enrichment_result.output, Mapping):
            raise V243CoreEnrichmentError("enrichment turn did not complete")
        enrichment_usage = _usage(
            _load_json(enrichment["paths"]["sidecar"], "v243 enrichment sidecar"),
            max_tokens=ENRICHMENT_MAX_TOKENS,
        )
        normalized, provenance, diagnostics = _final_projection(
            enrichment_result.output, core, enrichment
        )
        _write_immutable(root / "normalized-output.private.json", normalized)
        _write_immutable(root / "evidence-provenance.private.json", provenance)
        _write_immutable(root / "diagnostics.private.json", {"segments": diagnostics})
        gate = _final_gate(
            core_usage=core_usage,
            enrichment_usage=enrichment_usage,
            diagnostics=diagnostics,
        )
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise V243ArchitectureStop("v243 final structural quality or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v243_architecture_structural_gate_passed",
            "terminal_reason": "v243_core_enrichment_structural_cost_gate_passed",
            "semantic_attempt_count": 2,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "core_usage": core_usage,
            "enrichment_usage": enrichment_usage,
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
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "core_sidecar": _record(frozen["core"]["paths"]["sidecar"]),
            "enrichment_sidecar": _record(enrichment["paths"]["sidecar"]),
            "exact_next_action": "run frozen side-free support and neutral alignment",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v243 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v243 core plus enrichment canary")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v243(output_dir=Path(args.output_dir))
        design = _load_json(frozen["design_path"], "v243 design")
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "projected_ratio": design["production_cost_projection"][
                "projected_production_amortized_total_token_ratio"
            ],
        }
    else:
        terminal = asyncio.run(
            run_v243(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
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
