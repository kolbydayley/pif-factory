from __future__ import annotations

"""Run the bounded v236 Sol-core then Mini-metadata architecture canary."""

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
from . import app_server_judge_v5_selection_v232_window_ledger as v232
from . import app_server_judge_v5_selection_v233_blind_unit_sweep as v233
from . import app_server_judge_v5_selection_v234_two_pass_blind_inventory as v234
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v236_core_metadata_v1"
RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_selection_v236_core_metadata_runtime_lock_v1"
)
CONTINUATION_LOCK_VERSION = (
    "pif_app_server_judge_v5_selection_v236_metadata_continuation_lock_v1"
)
TERMINAL_VERSION = (
    "pif_app_server_judge_v5_selection_v236_core_metadata_terminal_v1"
)
PHASE_ID = "development_selection_v5_4_v236_core_then_metadata"
CORE_MODEL = "gpt-5.6-sol"
CORE_EFFORT = "low"
ENRICHMENT_MODEL = "gpt-5.4-mini"
ENRICHMENT_EFFORT = "low"
MAX_TOTAL_TOKENS_PER_TURN = 35_000
PHASE_TOTAL_TOKEN_BOUND = 70_000
MAX_EVENTS_PER_SEGMENT = 32
MAX_PROMPT_BYTES = 90_000
MAX_SCHEMA_BYTES = 40_000
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
BASELINE_END_TO_END_TOKENS = 10_065_426
PRODUCTION_AMORTIZED_CONTEXT_TOKENS = 600_538
PRODUCTION_SCALE = 30
DENSE_SEGMENT_ID = v234.DENSE_SEGMENT_ID
NO_SIGNAL_SEGMENT_ID = v234.NO_SIGNAL_SEGMENT_ID
MIN_DENSE_EVENTS = v234.MIN_DENSE_PROPOSITIONS
USAGE_FIELDS = v234.USAGE_FIELDS
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
SOURCE_TURN_ROOT = v234.SOURCE_TURN_ROOT
V235_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v235-support-critic-realization"
).resolve()
V235_TURN_ROOT = (
    V235_ROOT / "turns/v235-support-critic-d7c914bc1cee2c432b36"
).resolve()
V235_AUDIT = (
    PIPELINE_ROOT / "v235-completed-output-audit-2026-07-17/report.json"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v236-core-then-metadata"
).resolve()
PINNED_CODEX_0_144_1 = v234.PINNED_CODEX_0_144_1
EXPECTED_LINEAGE_HASHES = {
    "source_input": "71433895667434bedfa9f1b5fd57d7c3400d5f8a94e64f4e2c6efc78af8a8332",
    "source_base": "6b66e1ca9d00cf6558b84020b9879a3b86644b449f7498daea9a2c9837b4b0fc",
    "source_schema": "bd6b128cba67cc77700cc063778406c120107fde2cdadfa435d93eafe824db00",
    "v235_terminal": "b6ba9ca5bbe3870b766d42b8789b74da8a95528876853e045c043dabedb54002",
    "v235_sidecar": "3a2628e7a3616b8e9c30afe043783ccd5aa7ac52f2905ed363073e8e00cf19bc",
    "v235_audit": "92d178855d4709e81c1d1b027efef5240025e16656035f5bcf6e0068fd07966b",
}
ENRICHMENT_FIELDS = (
    "signal_reason",
    "model_names",
    "product_names",
    "organizations",
    "people",
    "confidence",
)


class V236CoreMetadataError(RuntimeError):
    """The v236 core/metadata architecture cannot proceed safely."""


class V236OutputContractError(V236CoreMetadataError):
    """A completed semantic output violated the frozen structural contract."""


class V236ArchitectureStop(V236CoreMetadataError):
    """The architecture cannot reach the frozen development gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V236CoreMetadataError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V236CoreMetadataError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V236CoreMetadataError(f"frozen {path.name} drifted")
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
    return {
        "source_input": SOURCE_TURN_ROOT / "input.private.json",
        "source_base": SOURCE_TURN_ROOT / "base-instructions.private.md",
        "source_schema": SOURCE_TURN_ROOT / "schema.json",
        "v235_terminal": V235_ROOT / "terminal.json",
        "v235_sidecar": V235_TURN_ROOT / "sidecar.json",
        "v235_audit": V235_AUDIT,
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V236CoreMetadataError(f"frozen lineage {name} drifted")
    terminal = _load_json(paths["v235_terminal"], "v235 terminal")
    sidecar = _load_json(paths["v235_sidecar"], "v235 sidecar")
    audit = _load_json(paths["v235_audit"], "v235 audit")
    if (
        terminal.get("state") != "inactive_incomplete_recovery_required"
        or terminal.get("production_mutated") is not False
        or terminal.get("holdout_authorized") is not False
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or audit.get("corrected_failure_class")
        != "architecture_quality_and_predeclared_cost_allocation_not_passed"
        or audit.get("next_distinct_architecture_authorized") is not True
        or audit.get("no_signal_kept_event_count") != 1
        or audit.get("metric_applicability_violation_count") != 5
        or audit.get("production_mutated") is not False
    ):
        raise V236CoreMetadataError("v235 predecessor state drifted")
    return {"paths": paths, "records": records}


def _source_packet(lineage: Mapping[str, Any]) -> dict[str, Any]:
    paths = lineage["paths"]
    source = _load_json(paths["source_input"], "source input")
    source_schema = _load_json(paths["source_schema"], "source schema")
    context = v232._shared_context(paths["source_base"])
    episode_id = str(source["episode_id"])
    if context.get("episode_id") != episode_id:
        raise V236CoreMetadataError("episode context id drifted")
    private_segments = []
    prompt_segments = []
    all_unit_ids = []
    segment_ids = []
    for position, row in enumerate(source["normalization_segments"]):
        segment_id = str(row["segment_id"])
        segment_ids.append(segment_id)
        text = str(row["segment_text"])
        boundaries = list(row["boundaries"])
        units = v232.source_units(text, boundaries, segment_position=position)
        all_unit_ids.extend(str(unit["unit_id"]) for unit in units)
        prompt_segments.append(
            {
                "segment_id": segment_id,
                "source_units": [
                    {
                        "unit_id": unit["unit_id"],
                        "window_id": unit["window_id"],
                        "text": unit["text"],
                    }
                    for unit in units
                ],
            }
        )
        private_segments.append(
            {
                "segment_id": segment_id,
                "segment_text": text,
                "boundaries": boundaries,
                "units": units,
                "density_stratum": row["density_stratum"],
            }
        )
    if segment_ids != [DENSE_SEGMENT_ID, NO_SIGNAL_SEGMENT_ID]:
        raise V236CoreMetadataError("canary segment membership drifted")
    return {
        "episode_id": episode_id,
        "segment_ids": segment_ids,
        "all_unit_ids": all_unit_ids,
        "source_schema": source_schema,
        "context": context,
        "private_input": {
            "schema_version": SCHEMA_VERSION,
            "episode_id": episode_id,
            "segments": private_segments,
            "privacy": "private source units and context",
        },
        "prompt_segments": prompt_segments,
    }


def _core_event_schema(
    source_schema: Mapping[str, Any], unit_ids: Sequence[str]
) -> dict[str, Any]:
    event = copy.deepcopy(
        source_schema["properties"]["segments"]["items"]["properties"][
            "events"
        ]["items"]
    )
    for field in (*ENRICHMENT_FIELDS, "window_id", "evidence"):
        event["properties"].pop(field, None)
    event["required"] = [
        field
        for field in event["required"]
        if field not in {*ENRICHMENT_FIELDS, "window_id", "evidence"}
    ]
    event["properties"]["core_event_id"] = {
        "type": "string",
        "pattern": "^S[01]E[0-9]{3}$",
    }
    unit_schema = {"type": "string", "enum": list(unit_ids)}
    event["properties"]["evidence_start_unit_id"] = copy.deepcopy(unit_schema)
    event["properties"]["evidence_end_unit_id"] = copy.deepcopy(unit_schema)
    event["required"].extend(
        ["core_event_id", "evidence_start_unit_id", "evidence_end_unit_id"]
    )
    return event


def _core_schema(packet: Mapping[str, Any]) -> dict[str, Any]:
    source_segment = packet["source_schema"]["properties"]["segments"]["items"]
    receipt = {
        "type": "object",
        "additionalProperties": False,
        "required": ["unit_id", "eligible_event_count", "unresolved_count"],
        "properties": {
            "unit_id": {"type": "string", "enum": list(packet["all_unit_ids"])},
            "eligible_event_count": {
                "type": "integer",
                "minimum": 0,
                "maximum": MAX_EVENTS_PER_SEGMENT,
            },
            "unresolved_count": {
                "type": "integer",
                "minimum": 0,
                "maximum": MAX_EVENTS_PER_SEGMENT,
            },
        },
    }
    segment = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "segment_id",
            "status",
            "segment_source_context",
            "no_signal_reason",
            "unit_receipts",
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
            "unit_receipts": {
                "type": "array",
                "minItems": 1,
                "maxItems": max(
                    len(row["units"])
                    for row in packet["private_input"]["segments"]
                ),
                "items": receipt,
            },
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
                "items": _core_event_schema(
                    packet["source_schema"], packet["all_unit_ids"]
                ),
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


CORE_INSTRUCTIONS = """You are the Sol-owned core semantic extractor for a private, topic-general podcast research corpus. This is a blind standalone pass with no existing events, reference answer, density label, target count, expected topic, or downstream quality hint. Read every source unit and make every semantic decision from meaning; never use keyword, regex, phrase, fixed-topic, or event-count rules.

Extract every distinct, explicitly source-supported, research-useful atomic proposition. Split independently truth-conditional premises, mechanisms, capabilities, constraints, comparisons, outcomes, alternatives, frames, uncertainties, counterclaims, product signals, market signals, risks, adoption signals, term uses, and stance positions. Do not inventory greetings, logistics, acknowledgements, unasserted questions, or bare mentions without a research-relevant relationship, identity, position, or claim. Do not invent an event to populate a receipt.

Own every core semantic field requested by the schema: discovery, boundaries, evidence, actor, speaker, reported actor, source context, claim, type, stance, certainty, time horizon, causal mechanism, counterclaim, and exact metric semantics. A later smaller model may fill only noncritical rationale, entity-list metadata, and numeric confidence; it cannot delete, relabel, or override your core event.

Return one unit_receipt per source unit in exact input order. eligible_event_count is the number of events whose evidence begins at that unit. unresolved_count must be zero. Receipt counts must sum to the event count. coverage_audit.all_source_units_reviewed must be true and unresolved_count zero.

Assign core_event_id sequentially in source order: S0E000, S0E001, ... for the first segment and S1E000, S1E001, ... for the second. Select the smallest self-contained contiguous evidence-unit range supporting every material core field, including adjacent attribution or coreference only when needed. The range must fit one fixed source window and one segment. Return only evidence unit IDs; deterministic code projects exact text and offsets.

Every nonempty metric_value, metric_unit, metric_comparator, and metric_raw_text must be a literal contiguous substring of the selected evidence. Use metric_direction=not_applicable if and only if all metric strings are empty. Use coded if and only if events are returned; otherwise no_signal. Keep segment and event order equal to input. Return schema-valid JSON only."""


ENRICHMENT_INSTRUCTIONS = """You fill noncritical metadata for a frozen set of core podcast events. The Sol core extractor has already fixed every event's identity, evidence, actor, claim, type, stance, attribution, mechanism, time horizon, and metric semantics. You may not add, drop, merge, split, relabel, reorder, or override any core event or core field.

Return exactly one metadata row for every core_event_id in exact input order. signal_reason briefly explains why the frozen core event matters without adding a new assertion. model_names, product_names, organizations, and people list only entities explicitly present in the event or its exact evidence; use empty arrays when absent. confidence reflects the clarity of the frozen event and evidence, not a license to change it. Return schema-valid JSON only."""


def _core_turn_name(episode_id: str) -> str:
    return "v236_sol_core_" + sha256_text(episode_id)[:20]


def _metadata_turn_name(episode_id: str) -> str:
    return "v236_mini_metadata_" + sha256_text(episode_id)[:20]


def _prepare_core_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    packet = _source_packet(lineage)
    prompt = "# Blind source-unit core packet\n" + json.dumps(
        {"episode_id": packet["episode_id"], "segments": packet["prompt_segments"]},
        ensure_ascii=True,
        separators=(",", ":"),
    ) + "\n"
    base = (
        CORE_INSTRUCTIONS
        + "\n\n# Immutable episode context\n"
        + json.dumps(packet["context"], ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )
    schema = _core_schema(packet)
    prompt_bytes = len(prompt.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if prompt_bytes > MAX_PROMPT_BYTES or schema_bytes > MAX_SCHEMA_BYTES:
        raise V236CoreMetadataError("v236 core request exceeds frozen byte cap")
    return {
        **packet,
        "turn_name": _core_turn_name(packet["episode_id"]),
        "prompt": prompt,
        "base": base,
        "schema": schema,
        "prompt_bytes": prompt_bytes,
        "base_bytes": len(base.encode("utf-8")),
        "schema_bytes": schema_bytes,
    }


def _metadata_schema(
    source_schema: Mapping[str, Any], core_ids: Sequence[str], episode_id: str,
    segment_ids: Sequence[str],
) -> dict[str, Any]:
    source_event = source_schema["properties"]["segments"]["items"]["properties"][
        "events"
    ]["items"]
    metadata = {
        "type": "object",
        "additionalProperties": False,
        "required": ["core_event_id", *ENRICHMENT_FIELDS],
        "properties": {
            "core_event_id": {"type": "string", "enum": list(core_ids)},
            **{
                field: copy.deepcopy(source_event["properties"][field])
                for field in ENRICHMENT_FIELDS
            },
        },
    }
    segment = {
        "type": "object",
        "additionalProperties": False,
        "required": ["segment_id", "events"],
        "properties": {
            "segment_id": {"type": "string", "enum": list(segment_ids)},
            "events": {
                "type": "array",
                "minItems": 0,
                "maxItems": MAX_EVENTS_PER_SEGMENT,
                "items": metadata,
            },
        },
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["episode_id", "segments"],
        "properties": {
            "episode_id": {"type": "string", "enum": [episode_id]},
            "segments": {
                "type": "array",
                "minItems": 2,
                "maxItems": 2,
                "items": segment,
            },
        },
    }


def _prepare_metadata_turn(
    *, packet: Mapping[str, Any], core: Mapping[str, Any]
) -> dict[str, Any]:
    segments = []
    core_ids = []
    for row in core["segments"]:
        events = []
        for event in row["events"]:
            core_ids.append(str(event["core_event_id"]))
            events.append(dict(event))
        segments.append({"segment_id": row["segment_id"], "core_events": events})
    prompt = "# Frozen core-event metadata packet\n" + json.dumps(
        {"episode_id": packet["episode_id"], "segments": segments},
        ensure_ascii=True,
        separators=(",", ":"),
    ) + "\n"
    schema = _metadata_schema(
        packet["source_schema"], core_ids, packet["episode_id"], packet["segment_ids"]
    )
    prompt_bytes = len(prompt.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if prompt_bytes > MAX_PROMPT_BYTES or schema_bytes > MAX_SCHEMA_BYTES:
        raise V236CoreMetadataError("v236 metadata request exceeds frozen byte cap")
    return {
        "turn_name": _metadata_turn_name(packet["episode_id"]),
        "episode_id": packet["episode_id"],
        "segment_ids": packet["segment_ids"],
        "private_input": {
            "schema_version": SCHEMA_VERSION,
            "episode_id": packet["episode_id"],
            "segments": segments,
            "privacy": "private frozen core events and exact evidence",
        },
        "prompt": prompt,
        "base": ENRICHMENT_INSTRUCTIONS + "\n",
        "schema": schema,
        "prompt_bytes": prompt_bytes,
        "base_bytes": len((ENRICHMENT_INSTRUCTIONS + "\n").encode("utf-8")),
        "schema_bytes": schema_bytes,
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
        "diagnostics": turn_root / "diagnostics.private.json",
    }


def _capacity_policy(root: Path, turn_names: Sequence[str]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        PHASE_TOTAL_TOKEN_BOUND * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_turn_count": 2,
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": PHASE_TOTAL_TOKEN_BOUND,
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
        "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": PHASE_TOTAL_TOKEN_BOUND,
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
                Path(__file__).resolve(),
                Path(v234.__file__).resolve(),
                Path(v233.__file__).resolve(),
                Path(v232.__file__).resolve(),
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


def _ranking() -> list[dict[str, Any]]:
    return [
        {
            "rank": 1,
            "architecture_id": "sol_core_then_mini_noncritical_metadata",
            "mechanism": (
                "Sol owns a compact truth-conditional event core and exact evidence; "
                "Mini fills only noncritical rationale entity lists and confidence"
            ),
            "measured_basis": (
                "v234 recovered 32 propositions while v235 full-schema critic used "
                "10067 output tokens and introduced metric inconsistencies"
            ),
        },
        {
            "rank": 2,
            "architecture_id": "exact_coverage_mask_gap_plus_reconciliation",
            "mechanism": "base-span mask then independent gap extraction and LLM merge",
            "measured_basis": "additive base and reconciliation cost has limited headroom",
        },
        {
            "rank": 3,
            "architecture_id": "independent_specialist_ensemble_consolidation",
            "mechanism": "multiple semantic lenses followed by an LLM-owned merge",
            "measured_basis": "repeated transcript input and prior lens overproduction",
        },
    ]


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v236 runtime lock")
    root = path.parent.resolve()
    core_name = str(lock.get("core_turn_name") or "")
    metadata_name = str(lock.get("metadata_turn_name") or "")
    actual_runtime = {
        str(Path(row["path"]).expanduser().resolve())
        for row in lock.get("runtime_files") or []
    }
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("core_model") != CORE_MODEL
        or lock.get("enrichment_model") != ENRICHMENT_MODEL
        or lock.get("declared_turn_count") != 2
        or lock.get("retry_count_per_turn") != 0
        or actual_runtime != {str(path) for path in _runtime_files()}
        or {row["path"] for row in lock.get("core_request") or []}
        != {row["path"] for row in _request_records(root, core_name)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or not core_name
        or not metadata_name
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V236CoreMetadataError("v236 runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("authorization"), lock.get("ranking"), lock.get("design"),
        lock.get("spec"), lock.get("capacity_audit"), lock.get("capacity_policy"),
        *(lock.get("core_request") or []),
    ]
    if any(not _verify_record(row or {}) for row in records):
        raise V236CoreMetadataError("v236 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V236CoreMetadataError("v236 lineage set drifted")
    policy = reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    if policy["ordered_turn_names"] != [core_name, metadata_name]:
        raise V236CoreMetadataError("v236 capacity order drifted")
    return lock


def verify_continuation_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v236 continuation lock")
    root = path.parent.resolve()
    initial = verify_runtime_lock(root / "runtime-lock.json")
    metadata_name = initial["metadata_turn_name"]
    if (
        lock.get("schema_version") != CONTINUATION_LOCK_VERSION
        or lock.get("initial_runtime_lock") != _record(root / "runtime-lock.json")
        or lock.get("core_gate_passed") is not True
        or lock.get("metadata_turn_name") != metadata_name
        or {row["path"] for row in lock.get("metadata_request") or []}
        != {row["path"] for row in _request_records(root, metadata_name)}
        or lock.get("retry_count") != 0
        or lock.get("holdout_authorized") is not False
    ):
        raise V236CoreMetadataError("v236 continuation lock contract drifted")
    records = [
        lock.get("initial_runtime_lock"), lock.get("core_receipt"),
        *(lock.get("core_artifacts") or []), *(lock.get("metadata_request") or []),
        lock.get("capacity_policy"),
    ]
    if any(not _verify_record(row or {}) for row in records):
        raise V236CoreMetadataError("v236 continuation record drifted")
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v236 spec")
    core_paths = _turn_paths(root, spec["core_turn_name"])
    result = {
        "root": root, "spec": spec, "spec_path": spec_path,
        "design_path": root / "architecture-design.json",
        "ranking_path": root / "architecture-ranking.json",
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "core": {
            "turn_name": spec["core_turn_name"], "episode_id": spec["episode_id"],
            "segment_ids": spec["segment_ids"], "paths": core_paths,
            "private_input": _load_json(core_paths["input"], "v236 core input"),
            "prompt": core_paths["prompt"].read_text(encoding="utf-8"),
            "base": core_paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(core_paths["schema"], "v236 core schema"),
        },
        "metadata_turn_name": spec["metadata_turn_name"],
    }
    metadata_paths = _turn_paths(root, spec["metadata_turn_name"])
    if metadata_paths["schema"].is_file():
        result["metadata"] = {
            "turn_name": spec["metadata_turn_name"], "episode_id": spec["episode_id"],
            "segment_ids": spec["segment_ids"], "paths": metadata_paths,
            "private_input": _load_json(metadata_paths["input"], "v236 metadata input"),
            "prompt": metadata_paths["prompt"].read_text(encoding="utf-8"),
            "base": metadata_paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(metadata_paths["schema"], "v236 metadata schema"),
        }
    return result


def freeze_v236(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v236 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V236CoreMetadataError("unfinished v236 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    core = _prepare_core_turn(lineage)
    metadata_name = _metadata_turn_name(core["episode_id"])
    paths = _turn_paths(root, core["turn_name"])
    paths["root"].mkdir(parents=True, exist_ok=True)
    _write_immutable(paths["input"], core["private_input"])
    _write_private_text(paths["prompt"], core["prompt"])
    _write_private_text(paths["base"], core["base"])
    _write_immutable(paths["schema"], core["schema"])
    capacity_paths = _capacity_policy(root, [core["turn_name"], metadata_name])
    authorization = {
        "schema_version": SCHEMA_VERSION, "created_at": now_iso(),
        "authority": "direct_operator_steering_2026_07_17",
        "scope": "bounded distinct Sol-core then Mini-metadata architecture",
        "holdout_authorized": False, "production_mutation_allowed": False,
    }
    authorization_path = root / "authorization.json"
    _write_stable_time(authorization_path, authorization, "created_at")
    ranking = {
        "schema_version": SCHEMA_VERSION, "created_at": now_iso(),
        "selected_architecture_id": "sol_core_then_mini_noncritical_metadata",
        "architectures": _ranking(),
        "on_failure": "freeze v236 and select another distinct architecture",
    }
    ranking_path = root / "architecture-ranking.json"
    _write_stable_time(ranking_path, ranking, "created_at")
    projected_total = PRODUCTION_AMORTIZED_CONTEXT_TOKENS + PHASE_TOTAL_TOKEN_BOUND * PRODUCTION_SCALE
    design = {
        "schema_version": SCHEMA_VERSION, "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "architecture_id": "sol_core_then_mini_noncritical_metadata",
        "hypothesis": (
            "removing noncritical rationale entity-list and confidence generation from "
            "the Sol discovery pass will recover at least 24 grounded dense events while "
            "preserving core semantics; Mini can then enrich one-to-one without deletion"
        ),
        "representative_canary": {
            "episode_count": 1, "segment_count": 2, "dense_count": 1,
            "nominal_no_signal_count": 1, "source_count": 1,
            "existing_reference_density_visible_to_models": False,
        },
        "production_cost_projection": {
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": PHASE_TOTAL_TOKEN_BOUND,
            "projected_production_amortized_total_tokens": projected_total,
            "projected_production_amortized_total_token_ratio": round(
                projected_total / BASELINE_END_TO_END_TOKENS, 6
            ),
            "required_ratio_max": 0.28,
        },
        "predeclared_stop_rules": {
            "retry_count_per_turn": 0, "all_source_units_reviewed": True,
            "unresolved_count": 0, "dense_core_event_count_minimum": MIN_DENSE_EVENTS,
            "exact_evidence_rate": 1.0, "metric_grounding_error_events": 0,
            "event_cap_violations": 0, "exact_identity_duplicates": 0,
            "metadata_exactly_one_per_core_event": True,
            "combined_tokens_max": PHASE_TOTAL_TOKEN_BOUND,
            "production_amortized_total_token_ratio_max": 0.28,
            "candidate_only_nominal_no_signal_events": (
                "require frozen side-free source support adjudication; do not auto-delete"
            ),
            "support_alignment_required_after_structural_pass": True,
        },
        "semantic_boundary": {
            "sol_owned": [
                "event discovery eligibility boundaries evidence actor claim source context",
                "event type stance certainty attribution mechanism time horizon metrics",
            ],
            "mini_owned": [
                "signal reason", "entity lists", "numeric confidence",
            ],
            "mini_forbidden": ["delete", "relabel", "override core semantics"],
            "deterministic_only": [
                "schema IDs coverage ordering", "exact evidence and offsets",
                "literal metric grounding", "exact identity duplicates",
                "one-to-one merge caps lifecycle provenance accounting",
            ],
        },
        "holdout_authorized": False, "production_mutation_allowed": False,
    }
    design_path = root / "architecture-design.json"
    _write_stable_time(design_path, design, "created_at")
    core_request = _request_records(root, core["turn_name"])
    spec = {
        "schema_version": SCHEMA_VERSION, "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "frozen_before_conditional_two_turn_core_metadata_canary",
        "declared_turn_count": 2, "core_turn_name": core["turn_name"],
        "metadata_turn_name": metadata_name, "episode_id": core["episode_id"],
        "segment_ids": core["segment_ids"], "core_model": CORE_MODEL,
        "core_effort": CORE_EFFORT, "enrichment_model": ENRICHMENT_MODEL,
        "enrichment_effort": ENRICHMENT_EFFORT, "retry_count_per_turn": 0,
        "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": PHASE_TOTAL_TOKEN_BOUND,
        "core_prompt_bytes": core["prompt_bytes"],
        "core_base_bytes": core["base_bytes"], "core_schema_bytes": core["schema_bytes"],
        "metadata_conditional_on_core_gate": True,
        "holdout_authorized": False, "production_mutation_allowed": False,
        "authorization": _record(authorization_path), "ranking": _record(ranking_path),
        "design": _record(design_path), "direct_lineage": lineage["records"],
        "core_request": core_request, "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    lock = {
        "schema_version": RUNTIME_LOCK_VERSION, "created_at": now_iso(),
        "phase_id": PHASE_ID, "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
        "authorization": _record(authorization_path), "ranking": _record(ranking_path),
        "design": _record(design_path), "spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "core_request": core_request, "core_turn_name": core["turn_name"],
        "metadata_turn_name": metadata_name, "core_model": CORE_MODEL,
        "enrichment_model": ENRICHMENT_MODEL, "declared_turn_count": 2,
        "retry_count_per_turn": 0, "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(root / "runtime-lock.json", lock, "created_at")
    verify_runtime_lock(root / "runtime-lock.json")
    return _load_frozen(root)


def _validate_output(schema: Mapping[str, Any], output: Mapping[str, Any], label: str) -> None:
    try:
        _validate_schema(schema, output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V236OutputContractError(
            f"{label} structured output validation failed: {type(exc).__name__}"
        ) from exc


def _project_core(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    _validate_output(turn["schema"], output, "core")
    if output.get("episode_id") != turn["episode_id"]:
        raise V236OutputContractError("core episode id drifted")
    rows = output.get("segments") or []
    if [row.get("segment_id") for row in rows] != list(turn["segment_ids"]):
        raise V236OutputContractError("core segment order drifted")
    source_by_id = {str(row["segment_id"]): row for row in turn["private_input"]["segments"]}
    normalized_rows = []
    provenance_rows = []
    diagnostics = []
    seen = set()
    for position, row in enumerate(rows):
        segment_id = str(row["segment_id"])
        source = source_by_id[segment_id]
        units = list(source["units"])
        expected_units = [str(unit["unit_id"]) for unit in units]
        receipts = list(row.get("unit_receipts") or [])
        if [item.get("unit_id") for item in receipts] != expected_units:
            raise V236OutputContractError("core unit receipt order drifted")
        audit = row.get("coverage_audit") or {}
        if audit.get("all_source_units_reviewed") is not True or audit.get("unresolved_count") != 0:
            raise V236OutputContractError("core coverage audit failed")
        receipt_counts = {}
        for receipt in receipts:
            count = receipt.get("eligible_event_count")
            if isinstance(count, bool) or not isinstance(count, int) or count < 0 or receipt.get("unresolved_count") != 0:
                raise V236OutputContractError("core receipt count invalid")
            receipt_counts[str(receipt["unit_id"])] = count
        events = list(row.get("events") or [])
        expected_ids = [f"S{position}E{index:03d}" for index in range(len(events))]
        if [event.get("core_event_id") for event in events] != expected_ids:
            raise V236OutputContractError("core event id order drifted")
        if sum(receipt_counts.values()) != len(events):
            raise V236OutputContractError("core receipt total drifted")
        if (row.get("status") == "coded") != bool(events):
            raise V236OutputContractError("core status drifted")
        start_counts = {unit_id: 0 for unit_id in expected_units}
        prior_start = -1
        projected_events = []
        for event_index, raw in enumerate(events):
            event = dict(raw)
            start_id = str(event.pop("evidence_start_unit_id"))
            end_id = str(event.pop("evidence_end_unit_id"))
            projection = v234._project_unit_range(source=source, start_id=start_id, end_id=end_id)
            if projection["start_index"] < prior_start:
                raise V236OutputContractError("core evidence order drifted")
            prior_start = int(projection["start_index"])
            metric_values = [str(event.get(field) or "") for field in (
                "metric_value", "metric_unit", "metric_comparator", "metric_raw_text"
            )]
            if any(value and value not in projection["evidence"] for value in metric_values):
                raise V236OutputContractError("core metric literal is not in evidence")
            if any(metric_values) == (event.get("metric_direction") == "not_applicable"):
                raise V236OutputContractError("core metric applicability drifted")
            identity = _canonical_json({field: event.get(field) for field in event if field != "core_event_id"})
            if identity in seen:
                raise V236OutputContractError("core exact identity duplicate")
            seen.add(identity)
            event["window_id"] = projection["window_id"]
            event["evidence"] = projection["evidence"]
            projected_events.append(event)
            start_counts[start_id] += 1
            provenance_rows.append({
                "segment_id": segment_id, "event_index": event_index,
                "core_event_id": event["core_event_id"],
                "evidence_start_unit_id": start_id, "evidence_end_unit_id": end_id,
                "start_char": projection["start_char"], "end_char": projection["end_char"],
                "window_id": projection["window_id"],
                "evidence_sha256": sha256_text(projection["evidence"]),
            })
        if start_counts != receipt_counts:
            raise V236OutputContractError("core receipt ownership drifted")
        normalized_rows.append({
            "segment_id": segment_id, "status": row["status"],
            "segment_source_context": row["segment_source_context"],
            "no_signal_reason": row["no_signal_reason"], "events": projected_events,
        })
        diagnostics.append({
            "segment_id": segment_id, "density_stratum": source["density_stratum"],
            "source_unit_count": len(units), "reviewed_source_unit_count": len(receipts),
            "core_event_count": len(events), "unresolved_count": 0,
        })
    return (
        {"episode_id": turn["episode_id"], "segments": normalized_rows},
        {"schema_version": SCHEMA_VERSION, "episode_id": turn["episode_id"], "events": provenance_rows},
        diagnostics,
    )


def _core_gate(diagnostics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense = int(by_id.get(DENSE_SEGMENT_ID, {}).get("core_event_count", -1))
    residual = int(by_id.get(NO_SIGNAL_SEGMENT_ID, {}).get("core_event_count", -1))
    checks = {
        "both_segments_validated": set(by_id) == {DENSE_SEGMENT_ID, NO_SIGNAL_SEGMENT_ID},
        "all_source_units_reviewed": all(row["source_unit_count"] == row["reviewed_source_unit_count"] for row in diagnostics),
        "unresolved_count_0": all(row["unresolved_count"] == 0 for row in diagnostics),
        "dense_core_event_count_gte_24": dense >= MIN_DENSE_EVENTS,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": SCHEMA_VERSION, "phase_id": PHASE_ID,
        "passed": not failed, "checks": checks, "failed_checks": failed,
        "diagnostics": list(diagnostics), "dense_core_event_count": dense,
        "candidate_only_nominal_no_signal_event_count": residual,
        "residual_support_audit_required": residual > 0,
        "metadata_authorized": not failed, "holdout_authorized": False,
        "production_mutated": False,
    }


def _usage(sidecar: Mapping[str, Any], model: str, effort: str) -> dict[str, int]:
    usage = sidecar.get("usage")
    if not isinstance(usage, Mapping):
        raise V236CoreMetadataError("turn usage absent")
    result = {}
    for field in USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise V236CoreMetadataError("turn usage incomplete")
        result[field] = value
    if (
        sidecar.get("state") != "completed" or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured" or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt" or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != model or sidecar.get("effort") != effort
        or sidecar.get("error_class") is not None
        or result["total_tokens"] > MAX_TOTAL_TOKENS_PER_TURN
    ):
        raise V236CoreMetadataError("measured sidecar contract failed")
    return result


def _freeze_metadata(
    *, frozen: Mapping[str, Any], core: Mapping[str, Any], core_gate_path: Path,
) -> dict[str, Any]:
    root = Path(frozen["root"])
    if _load_json(core_gate_path, "core gate").get("passed") is not True:
        raise V236ArchitectureStop("core gate did not authorize metadata")
    packet = _source_packet(_validate_lineage())
    metadata = _prepare_metadata_turn(packet=packet, core=core)
    if metadata["turn_name"] != frozen["metadata_turn_name"]:
        raise V236CoreMetadataError("metadata turn name drifted")
    paths = _turn_paths(root, metadata["turn_name"])
    paths["root"].mkdir(parents=True, exist_ok=True)
    _write_immutable(paths["input"], metadata["private_input"])
    _write_private_text(paths["prompt"], metadata["prompt"])
    _write_private_text(paths["base"], metadata["base"])
    _write_immutable(paths["schema"], metadata["schema"])
    core_paths = frozen["core"]["paths"]
    receipt = {
        "schema_version": SCHEMA_VERSION, "created_at": now_iso(),
        "state": "core_gate_passed_metadata_request_frozen",
        "core_gate": _record(core_gate_path), "core_sidecar": _record(core_paths["sidecar"]),
        "core_output": _record(core_paths["output"]),
        "core_normalized": _record(core_paths["normalized"]),
        "metadata_turn_name": metadata["turn_name"],
        "holdout_authorized": False, "production_mutated": False,
    }
    receipt_path = root / "core-phase-receipt.json"
    _write_stable_time(receipt_path, receipt, "created_at")
    core_artifacts = [_record(core_paths[name]) for name in (
        "capacity", "sidecar", "output", "normalized", "provenance", "diagnostics"
    )] + [_record(core_gate_path)]
    lock = {
        "schema_version": CONTINUATION_LOCK_VERSION, "created_at": now_iso(),
        "phase_id": PHASE_ID, "initial_runtime_lock": _record(frozen["runtime_lock"]),
        "core_receipt": _record(receipt_path), "core_artifacts": core_artifacts,
        "metadata_request": _request_records(root, metadata["turn_name"]),
        "capacity_policy": _record(frozen["capacity_policy"]),
        "metadata_turn_name": metadata["turn_name"], "retry_count": 0,
        "core_gate_passed": True, "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    lock_path = root / "metadata-continuation-lock.json"
    _write_stable_time(lock_path, lock, "created_at")
    verify_continuation_lock(lock_path)
    refreshed = _load_frozen(root)
    refreshed["continuation_lock"] = lock_path
    return refreshed


def _merge_metadata(
    *, output: Mapping[str, Any], turn: Mapping[str, Any], core: Mapping[str, Any],
    source_schema: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    _validate_output(turn["schema"], output, "metadata")
    if output.get("episode_id") != turn["episode_id"]:
        raise V236OutputContractError("metadata episode id drifted")
    rows = output.get("segments") or []
    if [row.get("segment_id") for row in rows] != list(turn["segment_ids"]):
        raise V236OutputContractError("metadata segment order drifted")
    core_by_id = {str(row["segment_id"]): row for row in core["segments"]}
    source_event_schema = source_schema["properties"]["segments"]["items"]["properties"]["events"]["items"]
    normalized_rows = []
    diagnostics = []
    for row in rows:
        segment_id = str(row["segment_id"])
        core_row = core_by_id[segment_id]
        core_events = list(core_row["events"])
        metadata_events = list(row.get("events") or [])
        expected_ids = [event["core_event_id"] for event in core_events]
        if [event.get("core_event_id") for event in metadata_events] != expected_ids:
            raise V236OutputContractError("metadata core event coverage drifted")
        merged = []
        for index, (core_event, metadata) in enumerate(zip(core_events, metadata_events)):
            event = dict(core_event)
            event.pop("core_event_id", None)
            for field in ENRICHMENT_FIELDS:
                event[field] = metadata[field]
            try:
                _validate_schema(source_event_schema, event, path=f"$.events[{index}]")
            except (ValidationError, ValueError, TypeError) as exc:
                raise V236OutputContractError("merged full event schema failed") from exc
            merged.append(event)
        normalized_rows.append({
            "segment_id": segment_id, "status": core_row["status"],
            "segment_source_context": core_row["segment_source_context"],
            "no_signal_reason": core_row["no_signal_reason"], "events": merged,
        })
        diagnostics.append({
            "segment_id": segment_id, "core_event_count": len(core_events),
            "metadata_event_count": len(metadata_events), "merged_event_count": len(merged),
        })
    return {"episode_id": turn["episode_id"], "segments": normalized_rows}, diagnostics


def _final_gate(
    *, core_usage: Mapping[str, int], metadata_usage: Mapping[str, int],
    core_gate: Mapping[str, Any], diagnostics: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    combined = {field: int(core_usage[field]) + int(metadata_usage[field]) for field in USAGE_FIELDS}
    production_total = PRODUCTION_AMORTIZED_CONTEXT_TOKENS + combined["total_tokens"] * PRODUCTION_SCALE
    ratio = production_total / BASELINE_END_TO_END_TOKENS
    checks = {
        "metadata_exactly_one_per_core_event": all(row["core_event_count"] == row["metadata_event_count"] == row["merged_event_count"] for row in diagnostics),
        "exact_evidence_rate_1": True, "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True, "exact_identity_duplicates_0": True,
        "combined_tokens_lte_70000": combined["total_tokens"] <= PHASE_TOTAL_TOKEN_BOUND,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    residual = int(core_gate["candidate_only_nominal_no_signal_event_count"])
    return {
        "schema_version": SCHEMA_VERSION, "phase_id": PHASE_ID,
        "passed": not failed, "checks": checks, "failed_checks": failed,
        "core_usage": dict(core_usage), "metadata_usage": dict(metadata_usage),
        "combined_usage": combined, "diagnostics": list(diagnostics),
        "candidate_only_nominal_no_signal_event_count": residual,
        "residual_support_audit_required": residual > 0,
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "support_alignment_authorized": not failed,
        "development_winner_frozen": False, "holdout_authorized": False,
        "production_mutated": False,
    }


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(command=[
        str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"
    ])


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(policy_path=policy_path, inner_factory=_inner_factory)


def _failure_terminal(root: Path, frozen: Mapping[str, Any], exc: BaseException) -> dict[str, Any]:
    refreshed = _load_frozen(root)
    usages = []
    sidecars = []
    attempted = 0
    unknown = 0
    for phase, model, effort in (("core", CORE_MODEL, CORE_EFFORT), ("metadata", ENRICHMENT_MODEL, ENRICHMENT_EFFORT)):
        turn = refreshed.get(phase)
        if not isinstance(turn, Mapping):
            continue
        paths = turn["paths"]
        attempted += int(paths["capacity"].exists())
        if paths["sidecar"].is_file():
            sidecars.append(_record(paths["sidecar"]))
            try:
                usages.append(_usage(_load_json(paths["sidecar"], f"{phase} sidecar"), model, effort))
            except V236CoreMetadataError:
                unknown += 1
        elif paths["capacity"].is_file():
            unknown += 1
    usage = {field: sum(row[field] for row in usages) for field in USAGE_FIELDS} if usages else {field: 0 for field in USAGE_FIELDS}
    semantic_failure = isinstance(exc, (V236OutputContractError, V236ArchitectureStop))
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION, "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": "v236_core_metadata_structural_or_count_gate_not_passed" if semantic_failure else "infrastructure_or_judge_attempt_failed",
        "error_class": type(exc).__name__, "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message), "semantic_attempt_count": attempted,
        "semantic_retry_count": 0, "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0, "usage": usage,
        "unknown_usage_attempt_count": unknown, "sidecars": sidecars,
        "architecture_strategy_rejected": semantic_failure,
        "next_distinct_architecture_authorized": semantic_failure,
        "isolated_field_repair_authorized": False, "support_alignment_authorized": False,
        "development_winner_frozen": False, "holdout_authorized": False,
        "production_mutated": False, "overall_goal_complete": False,
        "goal_status_required": "active", "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "exact_next_action": "advance to a distinct architecture without field repair" if semantic_failure else "audit immutable infrastructure attempt; no retry",
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v236(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v236 terminal")
    frozen = freeze_v236(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(root, frozen, V236CoreMetadataError("launch exists; replay prohibited"))
    _write_immutable(root / "launch-receipt.json", {
        "schema_version": SCHEMA_VERSION, "launched_at": now_iso(),
        "phase_id": PHASE_ID, "declared_turn_count": 2,
        "core_turn_name": frozen["core"]["turn_name"],
        "metadata_turn_name": frozen["metadata_turn_name"],
        "retry_count_per_turn": 0, "core_model": CORE_MODEL,
        "enrichment_model": ENRICHMENT_MODEL, "managed_chatgpt_auth_only": True,
        "official_persistent_app_server": True,
        "runtime_lock": _record(frozen["runtime_lock"]),
        "holdout_authorized": False, "production_mutation_allowed": False,
    })
    started = time.monotonic()
    core_paths = frozen["core"]["paths"]
    try:
        async with client_factory(frozen["capacity_policy"]) as client:
            core_result = await client.run_ephemeral_structured_turn(
                model=CORE_MODEL, effort=CORE_EFFORT,
                base_instructions=frozen["core"]["base"], prompt=frozen["core"]["prompt"],
                output_schema=frozen["core"]["schema"], cwd=PROJECT_ROOT,
                sidecar_path=core_paths["sidecar"], output_path=core_paths["output"],
                batch_size=2, thread_mode="new_thread", timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=core_paths["capacity"],
            )
            if core_result.status_ok is not True or not isinstance(core_result.output, Mapping):
                raise V236CoreMetadataError("core turn did not complete")
            core_usage = _usage(_load_json(core_paths["sidecar"], "core sidecar"), CORE_MODEL, CORE_EFFORT)
            core, provenance, diagnostics = _project_core(core_result.output, frozen["core"])
            _write_immutable(core_paths["normalized"], core)
            _write_immutable(core_paths["provenance"], provenance)
            _write_immutable(core_paths["diagnostics"], diagnostics)
            core_gate = _core_gate(diagnostics)
            core_gate_path = root / "core-structural-gate.json"
            _write_immutable(core_gate_path, core_gate)
            if not core_gate["passed"]:
                raise V236ArchitectureStop("v236 core gate failed")
            frozen = _freeze_metadata(frozen=frozen, core=core, core_gate_path=core_gate_path)
            verify_continuation_lock(frozen["continuation_lock"])
            metadata = frozen["metadata"]
            metadata_paths = metadata["paths"]
            metadata_result = await client.run_ephemeral_structured_turn(
                model=ENRICHMENT_MODEL, effort=ENRICHMENT_EFFORT,
                base_instructions=metadata["base"], prompt=metadata["prompt"],
                output_schema=metadata["schema"], cwd=PROJECT_ROOT,
                sidecar_path=metadata_paths["sidecar"], output_path=metadata_paths["output"],
                batch_size=2, thread_mode="new_thread", timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=metadata_paths["capacity"],
            )
        if metadata_result.status_ok is not True or not isinstance(metadata_result.output, Mapping):
            raise V236CoreMetadataError("metadata turn did not complete")
        metadata_usage = _usage(_load_json(metadata_paths["sidecar"], "metadata sidecar"), ENRICHMENT_MODEL, ENRICHMENT_EFFORT)
        packet = _source_packet(_validate_lineage())
        normalized, metadata_diagnostics = _merge_metadata(
            output=metadata_result.output, turn=metadata, core=core,
            source_schema=packet["source_schema"],
        )
        _write_immutable(metadata_paths["normalized"], normalized)
        _write_immutable(metadata_paths["diagnostics"], metadata_diagnostics)
        gate = _final_gate(core_usage=core_usage, metadata_usage=metadata_usage,
                           core_gate=core_gate, diagnostics=metadata_diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise V236ArchitectureStop("v236 final structural or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION, "terminal_at": now_iso(),
            "state": "v236_architecture_structural_gate_passed",
            "terminal_reason": "v236_core_metadata_structural_cost_gate_passed",
            "semantic_attempt_count": 2, "semantic_retry_count": 0,
            "usage_status": "complete", "accounting_complete": True,
            "usage": gate["combined_usage"],
            "production_amortized_total_token_ratio": gate["production_amortized_total_token_ratio"],
            "candidate_only_nominal_no_signal_event_count": gate["candidate_only_nominal_no_signal_event_count"],
            "residual_support_audit_required": gate["residual_support_audit_required"],
            "support_alignment_authorized": True, "development_winner_frozen": False,
            "holdout_authorized": False, "production_mutated": False,
            "overall_goal_complete": False, "goal_status_required": "active",
            "wall_seconds": round(time.monotonic() - started, 6),
            "core_gate": _record(root / "core-structural-gate.json"),
            "gate": _record(gate_path), "runtime_lock": _record(frozen["runtime_lock"]),
            "continuation_lock": _record(frozen["continuation_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "sidecars": [_record(core_paths["sidecar"]), _record(metadata_paths["sidecar"])],
            "exact_next_action": "run frozen v174 side-free support and neutral alignment; candidate-only residuals are not automatic false positives",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v236 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v236 core then metadata canary")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v236(output_dir=Path(args.output_dir))
        design = _load_json(frozen["design_path"], "v236 design")
        result = {
            "state": frozen["spec"]["state"], "root": str(frozen["root"]),
            "declared_turn_count": 2,
            "projected_ratio": design["production_cost_projection"]["projected_production_amortized_total_token_ratio"],
        }
    else:
        terminal = asyncio.run(run_v236(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
        result = {
            "state": terminal["state"], "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "total_tokens": (terminal.get("usage") or {}).get("total_tokens"),
            "support_alignment_authorized": terminal.get("support_alignment_authorized", False),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
