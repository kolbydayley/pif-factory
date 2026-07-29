from __future__ import annotations

"""Run the sole architecture-level post-v231 coverage-ledger diagnostic."""

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
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v232_window_ledger_v1"
RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_selection_v232_window_ledger_runtime_lock_v1"
)
TERMINAL_VERSION = (
    "pif_app_server_judge_v5_selection_v232_window_ledger_terminal_v1"
)
PHASE_ID = "development_selection_v5_4_v232_window_ledger_architecture"
MODEL = "gpt-5.6-sol"
EFFORT = "low"
MAX_TOTAL_TOKENS_PER_TURN = 44_980
MAX_DIAGNOSTIC_TOKENS = 77_250
MAX_EVENTS_PER_SEGMENT = 32
MAX_PROMPT_BYTES_PER_TURN = 90_000
MAX_SCHEMA_BYTES_PER_TURN = 40_000
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
BASELINE_END_TO_END_TOKENS = 10_065_426
BASE_PRODUCTION_AMORTIZED_TOKENS = 1_650_538
PRODUCTION_SEGMENT_SCOPE = 60
DEVELOPMENT_SEGMENT_SCOPE = 4
PRODUCTION_SCALE = PRODUCTION_SEGMENT_SCOPE // DEVELOPMENT_SEGMENT_SCOPE
USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
V220_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v220-fresh-integrated-base-design"
).resolve()
V227_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v227-independent-completeness-strategy"
).resolve()
V231_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v231-incremental-nonacceptance"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v232-window-ledger-architecture"
).resolve()
PINNED_CODEX_0_144_1 = (
    Path.home()
    / ".codex/packages/standalone/releases/0.144.1-aarch64-apple-darwin"
    / "bin/codex"
).resolve()

V227_TURN_DIRECTORIES = (
    "v227-luna-gap-67e7287720c7a19cb555243d",
    "v227-luna-gap-ba5e86bd4ce77f079d3379fb",
)
EXPECTED_LINEAGE_HASHES = {
    "v231_terminal": "552e758f73ca2abb322062be211136b3031957cf30aa41da392a5c62a3ee3bfe",
    "v231_gate": "490883a74eadec64c59e07a121c0df7b1cf3cddb8af387deb8651d04c1b41b3e",
    "v220_design": "1358fd2da7af12d62e61fdebec847321783dae847bdf21368951ae9b701276bc",
    "v220_runtime_lock": "4c7460472ab1ca2a111037157b42c1f32f2d8db30f5e7ba7678f4971903c2b65",
    "v220_terminal": "8cc7cc28c5e31d7d01587222c52dc4340637a92443fce61d422452e34816bd46",
    "browser_input": "959e3da0ca6a36f4b35f8024cabc0852f9da39f5a588bc7be1777835dc6ffde1",
    "browser_base": "5d0b0a88f425e6f5a51bf96c277eac133b2f73d255dac1b3bf174e14e31e4d41",
    "browser_schema": "2aada4f82b107957783e2b74eb35fce554e753a19988284c75dea9cfbc72a319",
    "node_input": "71433895667434bedfa9f1b5fd57d7c3400d5f8a94e64f4e2c6efc78af8a8332",
    "node_base": "6b66e1ca9d00cf6558b84020b9879a3b86644b449f7498daea9a2c9837b4b0fc",
    "node_schema": "bd6b128cba67cc77700cc063778406c120107fde2cdadfa435d93eafe824db00",
}

# These are frozen count ceilings, not labels shown to the model. If either
# dense case falls below its value, one-to-one alignment cannot reach the
# existing 0.97 macro and per-source floors even under perfect future judging.
DENSE_CASES = {
    "seg_afbf7ba7ce69db15f02bd14e": {
        "prior_represented": 10,
        "reference_count": 23,
        "minimum_new_for_possible_pass": 11,
    },
    "seg_c84d5569778c65202b572971": {
        "prior_represented": 6,
        "reference_count": 27,
        "minimum_new_for_possible_pass": 18,
    },
}
NO_SIGNAL_SEGMENT_IDS = frozenset(
    {
        "seg_a444b8d67229f3c6f4d76cc7",
        "seg_5d6fb274a5d25880b9db3006",
    }
)
IDENTITY_FIELDS = (
    "event_type",
    "event_subtype",
    "claim_type",
    "actor_name",
    "actor_type",
    "speaker_name",
    "speaker_role",
    "reported_actor_name",
    "reported_actor_type",
    "source_context_kind",
    "target_concept",
    "claim_text",
    "stance",
    "certainty",
    "temporal_horizon",
    "causal_mechanism",
    "counterclaim",
    "metric_value",
    "metric_unit",
    "metric_comparator",
    "metric_direction",
    "metric_raw_text",
)
COMPACT_EXISTING_FIELDS = (*IDENTITY_FIELDS, "evidence")


class V232WindowLedgerError(RuntimeError):
    """The v232 architecture diagnostic cannot proceed safely."""


class V232OutputContractError(V232WindowLedgerError):
    """A completed semantic output failed the predeclared structural contract."""


class V232ArchitectureStop(V232WindowLedgerError):
    """An observed case makes the frozen quality ceiling unattainable."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V232WindowLedgerError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(
        value, ensure_ascii=True, sort_keys=True, indent=2
    ) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V232WindowLedgerError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V232WindowLedgerError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        prior = _load_json(path, f"existing {path.name}")
        value[key] = prior.get(key)
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


def _source_artifact_paths() -> dict[str, Path]:
    browser = V227_ROOT / "turns" / V227_TURN_DIRECTORIES[0]
    node = V227_ROOT / "turns" / V227_TURN_DIRECTORIES[1]
    return {
        "v231_terminal": V231_ROOT / "terminal.json",
        "v231_gate": V231_ROOT / "alignment-ceiling-gate.json",
        "v220_design": V220_ROOT / "integrated-base-design.json",
        "v220_runtime_lock": V220_ROOT / "runtime-lock.json",
        "v220_terminal": V220_ROOT / "terminal.json",
        "browser_input": browser / "input.private.json",
        "browser_base": browser / "base-instructions.private.md",
        "browser_schema": browser / "schema.json",
        "node_input": node / "input.private.json",
        "node_base": node / "base-instructions.private.md",
        "node_schema": node / "schema.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _source_artifact_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V232WindowLedgerError(f"frozen lineage {name} drifted")
    v231_terminal = _load_json(paths["v231_terminal"], "v231 terminal")
    v231_gate = _load_json(paths["v231_gate"], "v231 gate")
    v220_design = _load_json(paths["v220_design"], "v220 design")
    v220_terminal = _load_json(paths["v220_terminal"], "v220 terminal")
    cost = v220_design.get("cost_bound") or {}
    if (
        v231_terminal.get("terminal_reason")
        != "v231_incremental_strategy_best_case_quality_ceiling_not_passed"
        or v231_terminal.get("architecture_level_redesign_authorized") is not True
        or v231_gate.get("architecture_level_redesign_authorized") is not True
        or v231_gate.get("further_isolated_field_repair_authorized") is not False
        or v231_gate.get("holdout_authorized") is not False
        or v231_gate.get("production_mutated") is not False
        or cost.get("base_production_amortized_total_tokens")
        != BASE_PRODUCTION_AMORTIZED_TOKENS
        or v220_terminal.get("production_mutated") is not False
        or v220_terminal.get("holdout_authorized") is not False
    ):
        raise V232WindowLedgerError("architecture redesign authorization drifted")
    return {"paths": paths, "records": records, "v220_design": v220_design}


def _shared_context(base_path: Path) -> dict[str, Any]:
    marker = "# Shared episode title, context, and extraction guidance\n"
    text = base_path.read_text(encoding="utf-8")
    if text.count(marker) != 1:
        raise V232WindowLedgerError("shared episode context marker drifted")
    try:
        value = json.loads(text.split(marker, 1)[1].strip())
    except json.JSONDecodeError as exc:
        raise V232WindowLedgerError("shared episode context is malformed") from exc
    speakers = []
    for row in value.get("speaker_map") or []:
        speakers.append(
            {
                "name": row.get("name", ""),
                "aliases": row.get("aliases") or [],
                "speaker_labels": row.get("speaker_labels") or [],
                "role": row.get("role", ""),
            }
        )
    return {
        "episode_id": value.get("episode_id"),
        "source_name": value.get("source_name"),
        "episode_title": value.get("episode_title"),
        "context_summary": value.get("context_summary"),
        "speaker_map": speakers,
        "extraction_guidance": value.get("extraction_guidance"),
    }


def _owner_window(start: int, boundaries: Sequence[Mapping[str, Any]]) -> int:
    matches = [
        int(row["window_id"])
        for row in boundaries
        if int(row["owner_start"]) <= start <= int(row["owner_end"])
    ]
    if len(matches) != 1:
        raise V232WindowLedgerError("source unit owner window is ambiguous")
    return matches[0]


def source_units(
    segment_text: str,
    boundaries: Sequence[Mapping[str, Any]],
    *,
    segment_position: int,
) -> list[dict[str, Any]]:
    units = []
    cursor = 0
    for raw in segment_text.splitlines(keepends=True):
        content = raw.rstrip("\r\n")
        if content.strip():
            start = cursor
            end = start + len(content)
            units.append(
                {
                    "unit_id": f"S{segment_position}U{len(units):03d}",
                    "start_char": start,
                    "end_char": end,
                    "window_id": _owner_window(start, boundaries),
                    "text": content,
                }
            )
        cursor += len(raw)
    if cursor < len(segment_text):
        raise V232WindowLedgerError("source unit projection truncated text")
    if not units:
        raise V232WindowLedgerError("segment has no nonempty source unit")
    if len({row["unit_id"] for row in units}) != len(units):
        raise V232WindowLedgerError("source unit ids are not unique")
    return units


def _compact_existing_event(event: Mapping[str, Any], index: int) -> dict[str, Any]:
    result: dict[str, Any] = {"existing_event_id": f"E{index:03d}"}
    for field in COMPACT_EXISTING_FIELDS:
        value = event.get(field)
        if value not in (None, "", [], {}):
            result[field] = value
    return result


def _event_schema(source_schema: Mapping[str, Any], unit_ids: Sequence[str]) -> dict[str, Any]:
    source = source_schema["properties"]["segments"]["items"]["properties"][
        "events"
    ]["items"]
    event = copy.deepcopy(source)
    properties = event["properties"]
    properties.pop("window_id", None)
    properties.pop("evidence", None)
    event["required"] = [
        field
        for field in event["required"]
        if field not in {"window_id", "evidence"}
    ]
    unit_schema = {
        "type": "string",
        "enum": list(unit_ids),
        "description": "Opaque exact source unit id from this episode packet.",
    }
    properties["evidence_start_unit_id"] = copy.deepcopy(unit_schema)
    properties["evidence_end_unit_id"] = copy.deepcopy(unit_schema)
    event["required"].extend(
        ["evidence_start_unit_id", "evidence_end_unit_id"]
    )
    return event


def _output_schema(
    *,
    episode_id: str,
    segment_ids: Sequence[str],
    source_schema: Mapping[str, Any],
    unit_ids: Sequence[str],
) -> dict[str, Any]:
    source_segment = source_schema["properties"]["segments"]["items"]
    receipt = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "window_id",
            "eligible_atomic_claim_count",
            "already_represented_count",
            "new_event_count",
            "unresolved_count",
        ],
        "properties": {
            "window_id": {"type": "integer", "enum": [0, 1, 2, 3]},
            "eligible_atomic_claim_count": {"type": "integer", "minimum": 0},
            "already_represented_count": {"type": "integer", "minimum": 0},
            "new_event_count": {"type": "integer", "minimum": 0},
            "unresolved_count": {"type": "integer", "minimum": 0},
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
            "window_receipts",
            "events",
        ],
        "properties": {
            "segment_id": {"type": "string", "enum": list(segment_ids)},
            "status": copy.deepcopy(source_segment["properties"]["status"]),
            "segment_source_context": copy.deepcopy(
                source_segment["properties"]["segment_source_context"]
            ),
            "no_signal_reason": copy.deepcopy(
                source_segment["properties"]["no_signal_reason"]
            ),
            "window_receipts": {
                "type": "array",
                "minItems": 4,
                "maxItems": 4,
                "items": receipt,
            },
            "events": {
                "type": "array",
                "minItems": 0,
                "maxItems": MAX_EVENTS_PER_SEGMENT,
                "items": _event_schema(source_schema, unit_ids),
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
                "minItems": len(segment_ids),
                "maxItems": len(segment_ids),
                "items": segment,
            },
        },
    }


BASE_INSTRUCTIONS = """You are a systematic, topic-general semantic event reader for a private podcast research corpus. Read every source unit. All semantic extraction decisions must come from understanding the language; never use keyword, regex, phrase, event-count, or fixed-topic rules.

For each segment, process source_windows in order. First construct an independent internal inventory of every distinct research-useful atomic proposition in each window without using the existing event ledger to set an expected density. Split independently meaningful premises, mechanisms, capabilities, constraints, comparisons, outcomes, alternatives, frames, uncertainties, counterclaims, and product or market signals. Keep inseparable parts together only when needed to express one truth-conditional claim.

Only after that independent inventory, compare each atomic proposition with existing_event_ledger. Mark it already represented only when the existing event matches all material semantics: actor, speaker, reported actor, attribution, target, stance, certainty, temporal horizon, metric, negation, causal mechanism, event boundary, and event type. A harmless paraphrase is represented. Any material truth-conditional difference is a new event. Existing events are fallible coverage records, not hints about what the source should contain. Never rewrite or delete them.

Return only new events. For every eligible atomic proposition, record exactly one disposition in its owner window: already represented, new, or unresolved. eligible_atomic_claim_count must equal the sum of those three counts. unresolved_count must be zero after your final audit. new_event_count must equal the number of returned events whose evidence starts in that owner window.

For evidence, select the smallest self-contained contiguous range of opaque source unit IDs that supports every material claim field. Return only evidence_start_unit_id and evidence_end_unit_id; do not copy evidence text and do not invent IDs. The range may cross an adjacent owner boundary only when required for a complete proposition. Deterministic code will project the exact source substring and owner window.

Populate every requested semantic field. speaker is who says the words; actor is whose position or action is represented; reported_actor is a quoted or reported source. claim_text must be a concise complete proposition naming the relevant actor and target. Every nonempty metric_value, metric_unit, metric_comparator, and metric_raw_text must be a literal contiguous substring of the selected evidence range. Use metric_direction=not_applicable when all metric strings are empty.

Event families include term_usage, frame_usage, stance_position, forecast, causal_mechanism, capability_claim, product_signal, market_signal, risk_signal, counterclaim, uncertainty, adoption_signal, actor_mention, and entity_reference. Classify by the proposition's main predicate and evidential commitment, not merely its topic.

Use coded if and only if this pass returns at least one new event. Otherwise use no_signal with events=[] and explain that no unrepresented eligible event remains. Classify segment_source_context from the source itself. Keep segment order equal to input order, preserve the per-segment remaining_event_slots ceiling, and return schema-valid JSON only."""


def _turn_name(episode_id: str) -> str:
    return "v232_window_ledger_" + sha256_text(episode_id)[:20]


def _prepare_turn(
    *,
    input_path: Path,
    base_path: Path,
    schema_path: Path,
) -> dict[str, Any]:
    source = _load_json(input_path, "v227 private input")
    source_schema = _load_json(schema_path, "v227 output schema")
    episode_id = str(source["episode_id"])
    context = _shared_context(base_path)
    if context.get("episode_id") != episode_id:
        raise V232WindowLedgerError("episode context id drifted")
    private_segments = []
    prompt_segments = []
    event_ledger = []
    all_unit_ids = []
    segment_ids = []
    for position, row in enumerate(source["normalization_segments"]):
        segment_id = str(row["segment_id"])
        segment_ids.append(segment_id)
        text = str(row["segment_text"])
        boundaries = list(row["boundaries"])
        if [int(item["window_id"]) for item in boundaries] != [0, 1, 2, 3]:
            raise V232WindowLedgerError("owner window coverage drifted")
        units = source_units(text, boundaries, segment_position=position)
        all_unit_ids.extend(str(unit["unit_id"]) for unit in units)
        windows = []
        for window_id in range(4):
            windows.append(
                {
                    "window_id": window_id,
                    "source_units": [
                        {"unit_id": unit["unit_id"], "text": unit["text"]}
                        for unit in units
                        if int(unit["window_id"]) == window_id
                    ],
                }
            )
        existing = [dict(event) for event in row.get("existing_events") or []]
        remaining = MAX_EVENTS_PER_SEGMENT - len(existing)
        if remaining < 0:
            raise V232WindowLedgerError("v220 base exceeded final event cap")
        prompt_segments.append(
            {
                "segment_id": segment_id,
                "remaining_event_slots": remaining,
                "source_windows": windows,
            }
        )
        event_ledger.append(
            {
                "segment_id": segment_id,
                "existing_events": [
                    _compact_existing_event(event, index)
                    for index, event in enumerate(existing)
                ],
            }
        )
        private_segments.append(
            {
                "segment_id": segment_id,
                "segment_text": text,
                "boundaries": boundaries,
                "units": units,
                "existing_events": existing,
                "density_stratum": row["density_stratum"],
            }
        )
    if set(segment_ids) - (set(DENSE_CASES) | set(NO_SIGNAL_SEGMENT_IDS)):
        raise V232WindowLedgerError("development segment membership drifted")
    packet = {
        "episode_id": episode_id,
        "segments": prompt_segments,
        "existing_event_ledger": event_ledger,
    }
    prompt = (
        "# Source windows first; existing coverage ledger follows\n"
        + json.dumps(packet, ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )
    base = (
        BASE_INSTRUCTIONS
        + "\n\n# Compact immutable shared episode context\n"
        + json.dumps(context, ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )
    schema = _output_schema(
        episode_id=episode_id,
        segment_ids=segment_ids,
        source_schema=source_schema,
        unit_ids=all_unit_ids,
    )
    if len(prompt.encode()) > MAX_PROMPT_BYTES_PER_TURN:
        raise V232WindowLedgerError("v232 prompt exceeds frozen byte cap")
    if len(_canonical_json(schema).encode()) > MAX_SCHEMA_BYTES_PER_TURN:
        raise V232WindowLedgerError("v232 schema exceeds frozen byte cap")
    return {
        "turn_name": _turn_name(episode_id),
        "episode_id": episode_id,
        "segment_ids": segment_ids,
        "private_input": {
            "schema_version": SCHEMA_VERSION,
            "episode_id": episode_id,
            "segments": private_segments,
            "privacy": "private_source_units_context_and_existing_events",
        },
        "prompt": prompt,
        "base": base,
        "schema": schema,
        "prompt_bytes": len(prompt.encode()),
        "base_bytes": len(base.encode()),
        "schema_bytes": len(_canonical_json(schema).encode()),
    }


def _prepare_turns(lineage: Mapping[str, Any]) -> list[dict[str, Any]]:
    paths = lineage["paths"]
    turns = [
        _prepare_turn(
            input_path=paths["browser_input"],
            base_path=paths["browser_base"],
            schema_path=paths["browser_schema"],
        ),
        _prepare_turn(
            input_path=paths["node_input"],
            base_path=paths["node_base"],
            schema_path=paths["node_schema"],
        ),
    ]
    turns.sort(key=lambda row: str(row["episode_id"]))
    if (
        len(turns) != 2
        or len({row["turn_name"] for row in turns}) != 2
        or sum(len(row["segment_ids"]) for row in turns) != 4
    ):
        raise V232WindowLedgerError("representative sample coverage drifted")
    return turns


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
    phase_bound = len(turn_names) * MAX_TOTAL_TOKENS_PER_TURN
    projected_points = math.ceil(
        phase_bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": phase_bound,
            "projected_phase_quota_points": projected_points,
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
        "phase_total_token_bound": phase_bound,
        "projected_phase_quota_points": projected_points,
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


def _request_records(root: Path, turn_names: Sequence[str]) -> list[dict[str, Any]]:
    records = []
    for turn_name in turn_names:
        paths = _turn_paths(root, turn_name)
        records.extend(
            _record(paths[name]) for name in ("input", "prompt", "base", "schema")
        )
    return records


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v232 runtime lock")
    root = path.parent.resolve()
    expected_runtime = {str(item) for item in _runtime_files()}
    actual_runtime = {
        str(Path(row["path"]).expanduser().resolve())
        for row in lock.get("runtime_files") or []
    }
    turn_names = list(lock.get("ordered_turn_names") or [])
    expected_request_paths = {
        row["path"] for row in _request_records(root, turn_names)
    }
    actual_request_paths = {
        row.get("path") for row in lock.get("frozen_request") or []
    }
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 2
        or lock.get("retry_count") != 0
        or len(turn_names) != 2
        or actual_runtime != expected_runtime
        or actual_request_paths != expected_request_paths
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V232WindowLedgerError("v232 runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("architecture_design"),
        lock.get("attempt_spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("frozen_request") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V232WindowLedgerError("v232 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        record["path"] for record in lineage["records"].values()
    }:
        raise V232WindowLedgerError("v232 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v232 attempt spec")
    turns = []
    for row in spec["turns"]:
        paths = _turn_paths(root, str(row["turn_name"]))
        turns.append(
            {
                **row,
                "paths": paths,
                "private_input": _load_json(paths["input"], "v232 input"),
                "prompt": paths["prompt"].read_text(encoding="utf-8"),
                "base": paths["base"].read_text(encoding="utf-8"),
                "schema": _load_json(paths["schema"], "v232 schema"),
            }
        )
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "design_path": root / "architecture-design.json",
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "turns": turns,
    }


def freeze_v232(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {
            "root": root,
            "terminal": _load_json(root / "terminal.json", "v232 terminal"),
        }
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V232WindowLedgerError("unfinished v232 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    turns = _prepare_turns(lineage)
    turn_names = [str(row["turn_name"]) for row in turns]
    for turn in turns:
        paths = _turn_paths(root, turn["turn_name"])
        paths["root"].mkdir(parents=True, exist_ok=True)
        _write_immutable(paths["input"], turn["private_input"])
        _write_private_text(paths["prompt"], turn["prompt"])
        _write_private_text(paths["base"], turn["base"])
        _write_immutable(paths["schema"], turn["schema"])
    capacity_paths = _capacity_policy(root, turn_names)
    design = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "strategy_class": "sole_architecture_level_redesign_after_v231",
        "hypothesis": (
            "independent owner-window atomic inventory before comparison prevents "
            "existing-event anchoring, while opaque source-unit selection removes "
            "model-copied evidence failures"
        ),
        "representative_sample": {
            "episode_count": 2,
            "segment_count": 4,
            "dense_count": 2,
            "no_signal_count": 2,
            "source_count": 2,
            "reference_or_density_visible_to_model": False,
        },
        "semantic_boundary": {
            "llm_owned": [
                "atomic proposition inventory",
                "eligibility",
                "represented versus new",
                "all event semantics",
                "source unit evidence selection",
            ],
            "deterministic_only": [
                "schema and enum validation",
                "opaque unit offset projection",
                "owner window assignment",
                "literal metric grounding",
                "exact identity duplicate detection",
                "caps provenance lifecycle and accounting",
            ],
        },
        "cost_projection": {
            "base_production_amortized_tokens": BASE_PRODUCTION_AMORTIZED_TOKENS,
            "diagnostic_token_hard_max": MAX_DIAGNOSTIC_TOKENS,
            "production_scale": PRODUCTION_SCALE,
            "projected_total_at_hard_max": (
                BASE_PRODUCTION_AMORTIZED_TOKENS
                + MAX_DIAGNOSTIC_TOKENS * PRODUCTION_SCALE
            ),
            "projected_ratio_at_hard_max": round(
                (
                    BASE_PRODUCTION_AMORTIZED_TOKENS
                    + MAX_DIAGNOSTIC_TOKENS * PRODUCTION_SCALE
                )
                / BASELINE_END_TO_END_TOKENS,
                6,
            ),
            "required_ratio_max": 0.28,
        },
        "predeclared_stop_rules": {
            "retry_count": 0,
            "per_turn_total_tokens_max": MAX_TOTAL_TOKENS_PER_TURN,
            "combined_diagnostic_tokens_max": MAX_DIAGNOSTIC_TOKENS,
            "exact_evidence_rate": 1.0,
            "metric_grounding_errors": 0,
            "event_cap_violations": 0,
            "exact_identity_duplicates": 0,
            "no_signal_positive_segments": 0,
            "unresolved_atomic_claims": 0,
            "minimum_new_events_by_dense_segment": {
                segment_id: row["minimum_new_for_possible_pass"]
                for segment_id, row in DENSE_CASES.items()
            },
            "early_futility_stop": (
                "stop after an episode when its dense minimum or no-signal zero "
                "condition fails because the frozen per-source floor is then impossible"
            ),
            "on_failure": (
                "reject the incremental and sole architecture redesign; no isolated "
                "field repair or successor architecture is locally authorized"
            ),
        },
        "support_alignment_required_after_structural_pass": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    design_path = root / "architecture-design.json"
    _write_stable_time(design_path, design, "created_at")
    request_records = _request_records(root, turn_names)
    spec = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "frozen_before_two_turn_architecture_diagnostic",
        "declared_turn_count": 2,
        "ordered_turn_names": turn_names,
        "retry_count": 0,
        "model": MODEL,
        "effort": EFFORT,
        "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
        "maximum_combined_diagnostic_tokens": MAX_DIAGNOSTIC_TOKENS,
        "turns": [
            {
                "turn_name": turn["turn_name"],
                "episode_id": turn["episode_id"],
                "segment_ids": turn["segment_ids"],
                "prompt_bytes": turn["prompt_bytes"],
                "base_instructions_bytes": turn["base_bytes"],
                "schema_bytes": turn["schema_bytes"],
            }
            for turn in turns
        ],
        "v220_extraction_replayed": False,
        "v227_calls_replayed": False,
        "reference_visible_to_model": False,
        "density_visible_to_model": False,
        "support_alignment_quality_measured": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "architecture_design": _record(design_path),
        "direct_lineage": lineage["records"],
        "frozen_request": request_records,
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "privacy": "private sources prompts outputs; sanitized terminal and gate",
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    lock = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
        "architecture_design": _record(design_path),
        "attempt_spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "frozen_request": request_records,
        "ordered_turn_names": turn_names,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 2,
        "retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(root / "runtime-lock.json", lock, "created_at")
    verify_runtime_lock(root / "runtime-lock.json")
    return _load_frozen(root)


def _event_identity(event: Mapping[str, Any]) -> str:
    return _canonical_json({field: event.get(field) for field in IDENTITY_FIELDS})


def _project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    try:
        _validate_schema(turn["schema"], output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V232OutputContractError(
            f"structured output validation failed: {type(exc).__name__}"
        ) from exc
    if output.get("episode_id") != turn["episode_id"]:
        raise V232OutputContractError("episode id drifted")
    rows = output.get("segments") or []
    if [row.get("segment_id") for row in rows] != list(turn["segment_ids"]):
        raise V232OutputContractError("segment order or coverage drifted")
    prepared = {
        str(row["segment_id"]): row
        for row in turn["private_input"]["segments"]
    }
    normalized_rows = []
    provenance_rows = []
    diagnostics = []
    for row in rows:
        segment_id = str(row["segment_id"])
        source = prepared[segment_id]
        events = list(row.get("events") or [])
        if (row.get("status") == "coded") != bool(events):
            raise V232OutputContractError("coded status does not match events")
        if not events and row.get("status") != "no_signal":
            raise V232OutputContractError("empty coverage output must be no_signal")
        receipts = list(row.get("window_receipts") or [])
        if [receipt.get("window_id") for receipt in receipts] != [0, 1, 2, 3]:
            raise V232OutputContractError("window receipt order drifted")
        for receipt in receipts:
            values = [
                receipt.get("eligible_atomic_claim_count"),
                receipt.get("already_represented_count"),
                receipt.get("new_event_count"),
                receipt.get("unresolved_count"),
            ]
            if any(isinstance(value, bool) or not isinstance(value, int) for value in values):
                raise V232OutputContractError("window receipt count is not an integer")
            if values[0] != values[1] + values[2] + values[3]:
                raise V232OutputContractError("window ledger arithmetic failed")
            if values[3] != 0:
                raise V232OutputContractError("window ledger retained unresolved claims")
        unit_rows = list(source["units"])
        unit_index = {
            str(unit["unit_id"]): index for index, unit in enumerate(unit_rows)
        }
        text = str(source["segment_text"])
        boundaries = list(source["boundaries"])
        seen = {_event_identity(event) for event in source["existing_events"]}
        new_by_window = {window_id: 0 for window_id in range(4)}
        projected_events = []
        for event_index, raw_event in enumerate(events):
            event = dict(raw_event)
            start_id = str(event.pop("evidence_start_unit_id"))
            end_id = str(event.pop("evidence_end_unit_id"))
            if start_id not in unit_index or end_id not in unit_index:
                raise V232OutputContractError("evidence unit belongs to another segment")
            start_index = unit_index[start_id]
            end_index = unit_index[end_id]
            if start_index > end_index:
                raise V232OutputContractError("evidence unit range is reversed")
            start_char = int(unit_rows[start_index]["start_char"])
            end_char = int(unit_rows[end_index]["end_char"])
            evidence = text[start_char:end_char]
            if not evidence:
                raise V232OutputContractError("projected evidence is empty")
            if not any(
                int(boundary["extract_start"]) <= start_char
                and end_char <= int(boundary["extract_end"])
                for boundary in boundaries
            ):
                raise V232OutputContractError(
                    "projected evidence is outside every fixed evidence window"
                )
            window_id = _owner_window(start_char, boundaries)
            metric_fields = (
                "metric_value",
                "metric_unit",
                "metric_comparator",
                "metric_raw_text",
            )
            metric_values = [str(event.get(field) or "") for field in metric_fields]
            if any(value and value not in evidence for value in metric_values):
                raise V232OutputContractError("metric literal is not in evidence")
            if any(metric_values) == (event.get("metric_direction") == "not_applicable"):
                raise V232OutputContractError("metric direction applicability drifted")
            identity = _event_identity(event)
            if identity in seen:
                raise V232OutputContractError("exact existing or new identity duplicate")
            seen.add(identity)
            event["window_id"] = window_id
            event["evidence"] = evidence
            projected_events.append(event)
            new_by_window[window_id] += 1
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
        if len(source["existing_events"]) + len(projected_events) > MAX_EVENTS_PER_SEGMENT:
            raise V232OutputContractError("combined event cap exceeded")
        if sum(int(receipt["new_event_count"]) for receipt in receipts) != len(events):
            raise V232OutputContractError("window receipt new-event total drifted")
        for receipt in receipts:
            window_id = int(receipt["window_id"])
            if int(receipt["new_event_count"]) != new_by_window[window_id]:
                raise V232OutputContractError("window receipt ownership drifted")
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
                "density_stratum": source["density_stratum"],
                "existing_event_count": len(source["existing_events"]),
                "new_event_count": len(projected_events),
                "combined_event_count": len(source["existing_events"])
                + len(projected_events),
                "unresolved_count": sum(
                    int(receipt["unresolved_count"]) for receipt in receipts
                ),
                "eligible_atomic_claim_count": sum(
                    int(receipt["eligible_atomic_claim_count"])
                    for receipt in receipts
                ),
                "already_represented_count": sum(
                    int(receipt["already_represented_count"])
                    for receipt in receipts
                ),
            }
        )
    normalized = {"episode_id": turn["episode_id"], "segments": normalized_rows}
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "episode_id": turn["episode_id"],
        "events": provenance_rows,
    }
    return normalized, provenance, diagnostics


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    usage = sidecar.get("usage")
    if not isinstance(usage, Mapping):
        raise V232WindowLedgerError("turn usage is absent")
    result = {}
    for field in USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise V232WindowLedgerError("turn usage is incomplete")
        result[field] = value
    return result


def _validate_measured_sidecar(path: Path) -> dict[str, int]:
    sidecar = _load_json(path, "v232 sidecar")
    usage = _usage(sidecar)
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
        or usage["total_tokens"] > MAX_TOTAL_TOKENS_PER_TURN
    ):
        raise V232WindowLedgerError("measured turn sidecar contract failed")
    return usage


def _add_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _zero_usage() -> dict[str, int]:
    return {field: 0 for field in USAGE_FIELDS}


def _episode_futility_check(diagnostics: Sequence[Mapping[str, Any]]) -> None:
    for row in diagnostics:
        segment_id = str(row["segment_id"])
        count = int(row["new_event_count"])
        if segment_id in NO_SIGNAL_SEGMENT_IDS and count != 0:
            raise V232ArchitectureStop("no-signal segment produced a new event")
        if segment_id in DENSE_CASES and count < int(
            DENSE_CASES[segment_id]["minimum_new_for_possible_pass"]
        ):
            raise V232ArchitectureStop("dense count cannot reach frozen quality floor")


def _count_ceiling(diagnostics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_segment = {str(row["segment_id"]): row for row in diagnostics}
    case_rows = []
    dense_f1 = []
    for segment_id, basis in sorted(DENSE_CASES.items()):
        new_count = int(by_segment[segment_id]["new_event_count"])
        represented = int(basis["prior_represented"]) + new_count
        reference_count = int(basis["reference_count"])
        best_f1 = 2 * represented / (reference_count + represented)
        dense_f1.append(best_f1)
        case_rows.append(
            {
                "segment_id": segment_id,
                "prior_represented": basis["prior_represented"],
                "reference_count": reference_count,
                "new_event_count": new_count,
                "represented_ceiling": represented,
                "semantic_f1_ceiling": round(best_f1, 6),
            }
        )
    macro = (2.0 + sum(dense_f1)) / 4.0
    source_macros = [(1.0 + value) / 2.0 for value in dense_f1]
    return {
        "cases": case_rows,
        "best_case_macro_f1": round(macro, 6),
        "minimum_best_case_source_macro_f1": round(min(source_macros), 6),
        "passes_frozen_0_97_floor": macro >= 0.97 and min(source_macros) >= 0.97,
    }


def _build_gate(
    *,
    usage: Mapping[str, int],
    diagnostics: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    count_ceiling = _count_ceiling(diagnostics)
    production_total = (
        BASE_PRODUCTION_AMORTIZED_TOKENS
        + int(usage["total_tokens"]) * PRODUCTION_SCALE
    )
    production_ratio = production_total / BASELINE_END_TO_END_TOKENS
    by_segment = {str(row["segment_id"]): row for row in diagnostics}
    checks = {
        "all_4_segments_validated": len(by_segment) == 4,
        "all_turn_usage_measured": True,
        "combined_diagnostic_tokens_lte_77250": int(usage["total_tokens"])
        <= MAX_DIAGNOSTIC_TOKENS,
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "unresolved_atomic_claims_0": all(
            int(row["unresolved_count"]) == 0 for row in diagnostics
        ),
        "no_signal_candidate_positive_segments_0": all(
            int(by_segment[segment_id]["new_event_count"]) == 0
            for segment_id in NO_SIGNAL_SEGMENT_IDS
        ),
        "count_based_quality_ceiling_gte_0_97": count_ceiling[
            "passes_frozen_0_97_floor"
        ],
        "production_amortized_total_token_ratio_lte_0_28": production_ratio
        <= 0.28,
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
        "count_based_quality_ceiling": count_ceiling,
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(production_ratio, 6),
        "semantic_support_alignment_quality_measured": False,
        "support_alignment_authorized": not failed,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "privacy": "sanitized ids counts metrics and hashes only",
    }


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[
            str(PINNED_CODEX_0_144_1),
            "app-server",
            "--stdio",
            "--strict-config",
        ]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(
    root: Path,
    frozen: Mapping[str, Any],
    exc: BaseException,
) -> dict[str, Any]:
    measured = _zero_usage()
    attempted = 0
    unknown_attempts = 0
    sidecars = []
    for turn in frozen["turns"]:
        paths = turn["paths"]
        if paths["capacity"].exists():
            attempted += 1
        if paths["sidecar"].is_file():
            sidecars.append(_record(paths["sidecar"]))
            try:
                measured = _add_usage(measured, _usage(_load_json(paths["sidecar"], "sidecar")))
            except V232WindowLedgerError:
                unknown_attempts += 1
        elif paths["capacity"].exists():
            unknown_attempts += 1
    semantic_failure = isinstance(
        exc, (V232OutputContractError, V232ArchitectureStop)
    )
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v232_architecture_structural_or_count_stop_rule_not_passed"
            if semantic_failure
            else "infrastructure_or_judge_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": attempted,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown_attempts else "complete",
        "accounting_complete": unknown_attempts == 0,
        "usage": measured,
        "unknown_usage_attempt_count": unknown_attempts,
        "sidecars": sidecars,
        "architecture_strategy_rejected": semantic_failure,
        "further_isolated_field_repair_authorized": False,
        "further_architecture_redesign_authorized": False,
        "support_alignment_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "exact_next_action": (
            "operator approval is required because the sole architecture redesign failed"
            if semantic_failure
            else "audit the immutable infrastructure attempt; no silent retry is allowed"
        ),
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v232(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v232 terminal")
    frozen = freeze_v232(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        return _failure_terminal(
            root,
            frozen,
            V232WindowLedgerError("launch receipt exists; replay prohibited"),
        )
    _write_immutable(
        launch_path,
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_turn_count": 2,
            "ordered_turn_names": [row["turn_name"] for row in frozen["turns"]],
            "retry_count": 0,
            "model": MODEL,
            "effort": EFFORT,
            "managed_chatgpt_auth_only": True,
            "official_persistent_app_server": True,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
    )
    started = time.monotonic()
    combined_usage = _zero_usage()
    all_diagnostics: list[dict[str, Any]] = []
    try:
        async with client_factory(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                paths = turn["paths"]
                result = await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
                    base_instructions=turn["base"],
                    prompt=turn["prompt"],
                    output_schema=turn["schema"],
                    cwd=PROJECT_ROOT,
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    batch_size=2,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=paths["capacity"],
                )
                if result.status_ok is not True or not isinstance(result.output, Mapping):
                    raise V232WindowLedgerError("coverage-ledger turn did not complete")
                usage = _validate_measured_sidecar(paths["sidecar"])
                combined_usage = _add_usage(combined_usage, usage)
                if combined_usage["total_tokens"] > MAX_DIAGNOSTIC_TOKENS:
                    raise V232ArchitectureStop("combined diagnostic token cap exceeded")
                normalized, provenance, diagnostics = _project_output(
                    result.output, turn
                )
                _write_immutable(paths["normalized"], normalized)
                _write_immutable(paths["provenance"], provenance)
                _write_immutable(paths["diagnostics"], diagnostics)
                all_diagnostics.extend(diagnostics)
                _episode_futility_check(diagnostics)
        gate = _build_gate(usage=combined_usage, diagnostics=all_diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise V232ArchitectureStop("final architecture structural gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v232_architecture_structural_gate_passed",
            "terminal_reason": "v232_window_ledger_structural_cost_gate_passed",
            "semantic_attempt_count": 2,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": combined_usage,
            "production_amortized_total_token_ratio": gate[
                "production_amortized_total_token_ratio"
            ],
            "structural_cost_gate_passed": True,
            "support_alignment_quality_measured": False,
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
            "sidecars": [
                _record(turn["paths"]["sidecar"]) for turn in frozen["turns"]
            ],
            "exact_next_action": (
                "run the already-frozen side-free support then alignment gate over "
                "the v232 additions without changing the extractor prompt"
            ),
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v232 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v232 window-ledger diagnostic")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v232(output_dir=Path(args.output_dir))
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "turn_count": len(frozen["turns"]),
            "prompt_bytes": [row["prompt_bytes"] for row in frozen["spec"]["turns"]],
            "schema_bytes": [row["schema_bytes"] for row in frozen["spec"]["turns"]],
        }
    else:
        terminal = asyncio.run(
            run_v232(
                output_dir=Path(args.output_dir),
                timeout_seconds=args.timeout_seconds,
            )
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "total_tokens": (terminal.get("usage") or {}).get("total_tokens"),
            "support_alignment_authorized": terminal.get(
                "support_alignment_authorized", False
            ),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
