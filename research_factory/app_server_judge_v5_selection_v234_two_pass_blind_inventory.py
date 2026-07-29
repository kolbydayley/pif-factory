from __future__ import annotations

"""Run the bounded v234 blind inventory-then-realization architecture canary."""

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
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = (
    "pif_app_server_judge_v5_selection_v234_two_pass_blind_inventory_v1"
)
RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_selection_v234_two_pass_runtime_lock_v1"
)
CONTINUATION_LOCK_VERSION = (
    "pif_app_server_judge_v5_selection_v234_realization_continuation_lock_v1"
)
TERMINAL_VERSION = (
    "pif_app_server_judge_v5_selection_v234_two_pass_terminal_v1"
)
PHASE_ID = "development_selection_v5_4_v234_two_pass_blind_inventory"
MODEL = "gpt-5.6-sol"
EFFORT = "low"
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
PRODUCTION_SEGMENT_SCOPE = 60
CANARY_SEGMENT_SCOPE = 2
PRODUCTION_SCALE = PRODUCTION_SEGMENT_SCOPE // CANARY_SEGMENT_SCOPE
DENSE_SEGMENT_ID = v233.DENSE_SEGMENT_ID
NO_SIGNAL_SEGMENT_ID = v233.NO_SIGNAL_SEGMENT_ID
DENSE_REFERENCE_COUNT = v233.DENSE_REFERENCE_COUNT
MIN_DENSE_PROPOSITIONS = v233.MIN_DENSE_EVENTS_FOR_FROZEN_SOURCE_FLOOR
USAGE_FIELDS = v233.USAGE_FIELDS
IDENTITY_FIELDS = v232.IDENTITY_FIELDS
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
V227_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v227-independent-completeness-strategy"
).resolve()
V233_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v233-blind-unit-sweep"
).resolve()
SOURCE_TURN_ROOT = (
    V227_ROOT / "turns/v227-luna-gap-ba5e86bd4ce77f079d3379fb"
).resolve()
V233_TURN_ROOT = (
    V233_ROOT
    / "turns/v233-blind-unit-sweep-d7c914bc1cee2c432b36"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v234-two-pass-blind-inventory-realization"
).resolve()
PINNED_CODEX_0_144_1 = (
    Path.home()
    / ".codex/packages/standalone/releases/0.144.1-aarch64-apple-darwin"
    / "bin/codex"
).resolve()
EXPECTED_LINEAGE_HASHES = {
    "source_input": "71433895667434bedfa9f1b5fd57d7c3400d5f8a94e64f4e2c6efc78af8a8332",
    "source_base": "6b66e1ca9d00cf6558b84020b9879a3b86644b449f7498daea9a2c9837b4b0fc",
    "source_schema": "bd6b128cba67cc77700cc063778406c120107fde2cdadfa435d93eafe824db00",
    "v233_terminal": "2584bfa238032e584d948ad4d97ca63834ad09b4c1c4d73cedf936fff19a24bf",
    "v233_gate": "89f5f6aa99c7bcc332986ace4929e143c1522df0e5a8931b7eb474176faabf51",
    "v233_sidecar": "b4e0cf388ea26f20866c2de79c9ae7fbea62c1443eb562b1de1cefa529d6c151",
    "v233_ranking": "a61ea9a40a5f7120e9186266e1629c19bb7e0afecdd739d0e89a46de8f24cbff",
}


class V234TwoPassError(RuntimeError):
    """The v234 two-pass architecture cannot proceed safely."""


class V234OutputContractError(V234TwoPassError):
    """A completed semantic output violated the frozen structural contract."""


class V234ArchitectureStop(V234TwoPassError):
    """The architecture cannot reach the frozen development gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V234TwoPassError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V234TwoPassError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V234TwoPassError(f"frozen {path.name} drifted")
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


def _lineage_paths() -> dict[str, Path]:
    return {
        "source_input": SOURCE_TURN_ROOT / "input.private.json",
        "source_base": SOURCE_TURN_ROOT / "base-instructions.private.md",
        "source_schema": SOURCE_TURN_ROOT / "schema.json",
        "v233_terminal": V233_ROOT / "terminal.json",
        "v233_gate": V233_ROOT / "architecture-structural-gate.json",
        "v233_sidecar": V233_TURN_ROOT / "sidecar.json",
        "v233_ranking": V233_ROOT / "architecture-ranking.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V234TwoPassError(f"frozen lineage {name} drifted")
    terminal = _load_json(paths["v233_terminal"], "v233 terminal")
    gate = _load_json(paths["v233_gate"], "v233 gate")
    sidecar = _load_json(paths["v233_sidecar"], "v233 sidecar")
    ranking = _load_json(paths["v233_ranking"], "v233 ranking")
    if (
        terminal.get("terminal_reason")
        != "v233_blind_unit_sweep_structural_or_count_gate_not_passed"
        or terminal.get("next_distinct_architecture_authorized") is not True
        or terminal.get("isolated_field_repair_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("holdout_authorized") is not False
        or gate.get("passed") is not False
        or gate.get("failed_checks")
        != ["no_signal_event_count_0", "dense_event_count_gte_24"]
        or gate.get("production_amortized_total_token_ratio") != 0.157367
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or (sidecar.get("usage") or {}).get("total_tokens") != 32_781
        or ranking.get("selected_architecture_id")
        != "blind_per_unit_standalone_sweep"
    ):
        raise V234TwoPassError("v233 predecessor state drifted")
    return {"paths": paths, "records": records}


def _source_packet(lineage: Mapping[str, Any]) -> dict[str, Any]:
    paths = lineage["paths"]
    source = _load_json(paths["source_input"], "v227 source input")
    source_schema = _load_json(paths["source_schema"], "v227 source schema")
    context = v232._shared_context(paths["source_base"])
    episode_id = str(source["episode_id"])
    if context.get("episode_id") != episode_id:
        raise V234TwoPassError("episode context id drifted")
    private_segments = []
    prompt_segments = []
    segment_ids = []
    all_unit_ids = []
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
        raise V234TwoPassError("canary segment membership drifted")
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


def _proposition_ids(segment_position: int) -> list[str]:
    return [
        f"S{segment_position}P{index:03d}"
        for index in range(MAX_EVENTS_PER_SEGMENT)
    ]


def _inventory_schema(packet: Mapping[str, Any]) -> dict[str, Any]:
    source_event = packet["source_schema"]["properties"]["segments"]["items"][
        "properties"
    ]["events"]["items"]
    unit_ids = list(packet["all_unit_ids"])
    proposition_ids = [
        item for position in range(2) for item in _proposition_ids(position)
    ]
    receipt = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "unit_id",
            "eligible_proposition_count",
            "unresolved_count",
        ],
        "properties": {
            "unit_id": {"type": "string", "enum": unit_ids},
            "eligible_proposition_count": {
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
    proposition = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "proposition_id",
            "evidence_start_unit_id",
            "evidence_end_unit_id",
            "event_type",
            "claim_text",
        ],
        "properties": {
            "proposition_id": {"type": "string", "enum": proposition_ids},
            "evidence_start_unit_id": {"type": "string", "enum": unit_ids},
            "evidence_end_unit_id": {"type": "string", "enum": unit_ids},
            "event_type": copy.deepcopy(source_event["properties"]["event_type"]),
            "claim_text": copy.deepcopy(source_event["properties"]["claim_text"]),
        },
    }
    segment = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "segment_id",
            "unit_receipts",
            "coverage_audit",
            "propositions",
        ],
        "properties": {
            "segment_id": {"type": "string", "enum": list(packet["segment_ids"])},
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
            "propositions": {
                "type": "array",
                "minItems": 0,
                "maxItems": MAX_EVENTS_PER_SEGMENT,
                "items": proposition,
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


INVENTORY_INSTRUCTIONS = """You are the recall inventory stage of a topic-general semantic event extractor for a private podcast research corpus. This is a blind standalone pass. You have no existing events, reference answer, density label, target count, expected topic, or downstream event output. Read every source unit and make every semantic decision from the language itself; never use keyword, regex, phrase, fixed-topic, or event-count rules.

Your only job is to enumerate every distinct, explicitly source-supported, research-useful atomic proposition before any full event schema is populated. Inventory independently meaningful premises, mechanisms, capabilities, constraints, comparisons, outcomes, alternatives, frames, uncertainties, counterclaims, product signals, market signals, risks, adoption signals, term uses, and stance positions. Split propositions when either can be true or false independently. Keep inseparable material together only when required for one truth-conditional claim.

Do not invent a proposition merely to populate a source unit. Do not inventory greetings, logistics, conversational acknowledgements, unasserted questions, or a bare mention without a research-relevant relationship, identity, position, or claim. A hypothetical becomes a proposition only when the speaker asserts a conclusion, premise, forecast, mechanism, constraint, or uncertainty about it. Apply these semantic distinctions from meaning, not words or patterns.

For each segment, return one unit_receipt for every source unit in exact input order. eligible_proposition_count is the number of returned propositions whose evidence begins at that unit. unresolved_count must be zero. The receipt counts must sum to the returned proposition count. coverage_audit.all_source_units_reviewed must be true and its unresolved_count must be zero.

Assign proposition IDs sequentially in source order: S0P000, S0P001, ... for the first segment and S1P000, S1P001, ... for the second. Select the smallest self-contained contiguous evidence-unit range supporting the full proposition, including adjacent attribution or coreference only when needed. Ranges must stay inside one fixed source window and one segment. claim_text must be a concise complete proposition. event_type is the best preliminary event family from the supplied schema. Order propositions by evidence start and source order. Return schema-valid JSON only."""


REALIZATION_INSTRUCTIONS = """You are the full-schema realization stage of a topic-general semantic event extractor for a private podcast research corpus. A separate blind LLM inventory pass has already frozen the proposition ledger. You have no existing events, reference answer, density label, target count, or expected topic.

Realize exactly one complete event for every frozen proposition_id, in the exact supplied proposition order. Do not add, drop, merge, split, or skip a proposition. The frozen inventory controls proposition coverage; you make every semantic field decision from the proposition and exact source units. You may refine the preliminary event_type when the proposition's main predicate and evidential commitment require it.

Select the smallest self-contained contiguous evidence-unit range that supports every material output field, including adjacent attribution or coreference only when needed. The range must stay inside one fixed source window and one segment. Return only evidence_start_unit_id and evidence_end_unit_id; deterministic code projects exact evidence and offsets.

Populate every requested semantic field. speaker is who says the words; actor is whose position or action is represented; reported_actor is a quoted or reported source. claim_text is a concise complete proposition naming the relevant actor and target. Every nonempty metric_value, metric_unit, metric_comparator, and metric_raw_text must be a literal contiguous substring of the selected evidence. Use metric_direction=not_applicable when all metric strings are empty.

Classify event_type by the proposition's main predicate and evidential commitment, not its topic. Use coded if and only if at least one frozen proposition exists for the segment. Otherwise use no_signal with events=[] and explain why the segment has no inventoried proposition. Classify segment_source_context from the source. Keep segment and event order equal to the input. Return schema-valid JSON only."""


def _inventory_turn_name(episode_id: str) -> str:
    return "v234_blind_inventory_" + sha256_text(episode_id)[:20]


def _realization_turn_name(episode_id: str) -> str:
    return "v234_blind_realization_" + sha256_text(episode_id)[:20]


def _prepare_inventory_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    packet = _source_packet(lineage)
    prompt_packet = {
        "episode_id": packet["episode_id"],
        "segments": packet["prompt_segments"],
    }
    prompt = "# Blind source-unit inventory packet\n" + json.dumps(
        prompt_packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"
    base = (
        INVENTORY_INSTRUCTIONS
        + "\n\n# Immutable episode context\n"
        + json.dumps(packet["context"], ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )
    schema = _inventory_schema(packet)
    prompt_bytes = len(prompt.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if prompt_bytes > MAX_PROMPT_BYTES:
        raise V234TwoPassError("inventory prompt exceeds frozen byte cap")
    if schema_bytes > MAX_SCHEMA_BYTES:
        raise V234TwoPassError("inventory schema exceeds frozen byte cap")
    return {
        **packet,
        "turn_name": _inventory_turn_name(packet["episode_id"]),
        "prompt": prompt,
        "base": base,
        "schema": schema,
        "prompt_bytes": prompt_bytes,
        "base_bytes": len(base.encode("utf-8")),
        "schema_bytes": schema_bytes,
    }


def _realization_schema(
    packet: Mapping[str, Any], inventory: Mapping[str, Any]
) -> dict[str, Any]:
    source_segment = packet["source_schema"]["properties"]["segments"]["items"]
    proposition_ids = [
        proposition["proposition_id"]
        for row in inventory["segments"]
        for proposition in row["propositions"]
    ]
    event = v232._event_schema(packet["source_schema"], packet["all_unit_ids"])
    event["properties"]["proposition_id"] = {
        "type": "string",
        "enum": proposition_ids,
    }
    event["required"].append("proposition_id")
    segment = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "segment_id",
            "status",
            "segment_source_context",
            "no_signal_reason",
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


def _prepare_realization_turn(
    packet: Mapping[str, Any], inventory: Mapping[str, Any]
) -> dict[str, Any]:
    inventory_by_id = {
        str(row["segment_id"]): row for row in inventory["segments"]
    }
    segments = []
    for source in packet["prompt_segments"]:
        segment_id = str(source["segment_id"])
        row = inventory_by_id[segment_id]
        segments.append(
            {
                "segment_id": segment_id,
                "source_units": source["source_units"],
                "frozen_propositions": row["propositions"],
            }
        )
    prompt_packet = {"episode_id": packet["episode_id"], "segments": segments}
    prompt = "# Frozen inventory realization packet\n" + json.dumps(
        prompt_packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"
    base = (
        REALIZATION_INSTRUCTIONS
        + "\n\n# Immutable episode context\n"
        + json.dumps(packet["context"], ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )
    schema = _realization_schema(packet, inventory)
    prompt_bytes = len(prompt.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if prompt_bytes > MAX_PROMPT_BYTES:
        raise V234TwoPassError("realization prompt exceeds frozen byte cap")
    if schema_bytes > MAX_SCHEMA_BYTES:
        raise V234TwoPassError("realization schema exceeds frozen byte cap")
    return {
        "turn_name": _realization_turn_name(packet["episode_id"]),
        "episode_id": packet["episode_id"],
        "segment_ids": packet["segment_ids"],
        "private_input": {
            "schema_version": SCHEMA_VERSION,
            "episode_id": packet["episode_id"],
            "segments": segments,
            "privacy": "private source units inventory and context",
        },
        "prompt": prompt,
        "base": base,
        "schema": schema,
        "prompt_bytes": prompt_bytes,
        "base_bytes": len(base.encode("utf-8")),
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
    projected_points = math.ceil(
        PHASE_TOTAL_TOKEN_BOUND
        * QUOTA_POINTS_PER_MILLION_TOKENS
        / 1_000_000
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
        "phase_total_token_bound": PHASE_TOTAL_TOKEN_BOUND,
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


def _architecture_ranking() -> list[dict[str, Any]]:
    ranking = copy.deepcopy(v233._architecture_ranking())
    ranking[0]["measured_result"] = {
        "state": "rejected",
        "dense_event_count": 23,
        "no_signal_event_count": 1,
        "total_tokens": 32_781,
        "production_amortized_total_token_ratio": 0.157367,
    }
    return ranking


def _projected_ratio(tokens: int) -> float:
    total = PRODUCTION_AMORTIZED_CONTEXT_TOKENS + tokens * PRODUCTION_SCALE
    return total / BASELINE_END_TO_END_TOKENS


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v234 runtime lock")
    root = path.parent.resolve()
    expected_runtime = {str(item) for item in _runtime_files()}
    actual_runtime = {
        str(Path(row["path"]).expanduser().resolve())
        for row in lock.get("runtime_files") or []
    }
    inventory_name = str(lock.get("inventory_turn_name") or "")
    realization_name = str(lock.get("realization_turn_name") or "")
    expected_request = {
        row["path"] for row in _request_records(root, inventory_name)
    }
    actual_request = {
        row.get("path") for row in lock.get("inventory_request") or []
    }
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 2
        or lock.get("retry_count_per_turn") != 0
        or not inventory_name
        or not realization_name
        or actual_runtime != expected_runtime
        or actual_request != expected_request
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V234TwoPassError("v234 runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("authorization"),
        lock.get("architecture_ranking"),
        lock.get("architecture_design"),
        lock.get("attempt_spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("inventory_request") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V234TwoPassError("v234 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        record["path"] for record in lineage["records"].values()
    }:
        raise V234TwoPassError("v234 direct lineage set drifted")
    policy = reserve.load_reserve_capacity_policy(
        Path(lock["capacity_policy"]["path"])
    )
    if policy["ordered_turn_names"] != [inventory_name, realization_name]:
        raise V234TwoPassError("v234 capacity turn order drifted")
    return lock


def verify_continuation_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v234 continuation lock")
    root = path.parent.resolve()
    initial_lock = verify_runtime_lock(root / "runtime-lock.json")
    realization_name = str(lock.get("realization_turn_name") or "")
    expected_request = {
        row["path"] for row in _request_records(root, realization_name)
    }
    actual_request = {
        row.get("path") for row in lock.get("realization_request") or []
    }
    if (
        lock.get("schema_version") != CONTINUATION_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("initial_runtime_lock") != _record(root / "runtime-lock.json")
        or realization_name != initial_lock["realization_turn_name"]
        or actual_request != expected_request
        or lock.get("retry_count") != 0
        or lock.get("inventory_gate_passed") is not True
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V234TwoPassError("v234 continuation lock contract drifted")
    records = [
        lock.get("initial_runtime_lock"),
        lock.get("inventory_phase_receipt"),
        *(lock.get("inventory_artifacts") or []),
        *(lock.get("realization_request") or []),
        lock.get("capacity_policy"),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V234TwoPassError("v234 continuation lock record drifted")
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v234 attempt spec")
    inventory_name = str(spec["inventory_turn_name"])
    realization_name = str(spec["realization_turn_name"])
    inventory_paths = _turn_paths(root, inventory_name)
    result = {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "design_path": root / "architecture-design.json",
        "ranking_path": root / "architecture-ranking.json",
        "authorization_path": root / "authorization.json",
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "inventory": {
            "turn_name": inventory_name,
            "episode_id": spec["episode_id"],
            "segment_ids": spec["segment_ids"],
            "paths": inventory_paths,
            "private_input": _load_json(inventory_paths["input"], "v234 inventory input"),
            "prompt": inventory_paths["prompt"].read_text(encoding="utf-8"),
            "base": inventory_paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(inventory_paths["schema"], "v234 inventory schema"),
        },
        "realization_turn_name": realization_name,
    }
    realization_paths = _turn_paths(root, realization_name)
    if realization_paths["schema"].is_file():
        result["realization"] = {
            "turn_name": realization_name,
            "episode_id": spec["episode_id"],
            "segment_ids": spec["segment_ids"],
            "paths": realization_paths,
            "private_input": _load_json(realization_paths["input"], "v234 realization input"),
            "prompt": realization_paths["prompt"].read_text(encoding="utf-8"),
            "base": realization_paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(realization_paths["schema"], "v234 realization schema"),
        }
    return result


def freeze_v234(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v234 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V234TwoPassError("unfinished v234 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    inventory = _prepare_inventory_turn(lineage)
    realization_name = _realization_turn_name(inventory["episode_id"])
    inventory_paths = _turn_paths(root, inventory["turn_name"])
    inventory_paths["root"].mkdir(parents=True, exist_ok=True)
    _write_immutable(inventory_paths["input"], inventory["private_input"])
    _write_private_text(inventory_paths["prompt"], inventory["prompt"])
    _write_private_text(inventory_paths["base"], inventory["base"])
    _write_immutable(inventory_paths["schema"], inventory["schema"])
    capacity_paths = _capacity_policy(
        root, [inventory["turn_name"], realization_name]
    )
    authorization = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "authority": "direct_operator_steering_2026_07_17",
        "scope": "additional bounded distinct architecture experiments one at a time",
        "managed_chatgpt_app_server_only": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    authorization_path = root / "authorization.json"
    _write_stable_time(authorization_path, authorization, "created_at")
    ranking = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "selected_architecture_id": "two_pass_blind_inventory_then_realization",
        "architectures": _architecture_ranking(),
        "selection_basis": (
            "v233 exhaustively reviewed every source unit but remained one dense event "
            "below the frozen floor and emitted one no-signal event; v234 separates "
            "recall inventory from full-schema realization rather than patching a field"
        ),
        "on_failure": (
            "freeze v234 and select ranked architecture 3; do not create an isolated "
            "field, schema, exactness, or validator repair"
        ),
    }
    ranking_path = root / "architecture-ranking.json"
    _write_stable_time(ranking_path, ranking, "created_at")
    design = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "architecture_id": "two_pass_blind_inventory_then_realization",
        "hypothesis": (
            "freezing an exhaustive minimal proposition ledger before any full-schema "
            "generation will reduce schema-induced omissions while the separate "
            "realization pass preserves one-to-one semantic coverage"
        ),
        "representative_canary": {
            "episode_count": 1,
            "segment_count": 2,
            "dense_count": 1,
            "no_signal_count": 1,
            "source_count": 1,
            "reuses_exact_v233_failed_source": True,
            "existing_reference_density_visible_to_either_model": False,
        },
        "production_cost_projection": {
            "episode_context_tokens": PRODUCTION_AMORTIZED_CONTEXT_TOKENS,
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
            "conditional_turn_count": 2,
            "phase_total_token_bound": PHASE_TOTAL_TOKEN_BOUND,
            "production_scale": PRODUCTION_SCALE,
            "projected_production_amortized_total_tokens": (
                PRODUCTION_AMORTIZED_CONTEXT_TOKENS
                + PHASE_TOTAL_TOKEN_BOUND * PRODUCTION_SCALE
            ),
            "projected_production_amortized_total_token_ratio": round(
                _projected_ratio(PHASE_TOTAL_TOKEN_BOUND), 6
            ),
            "required_ratio_max": 0.28,
        },
        "predeclared_stop_rules": {
            "retry_count_per_turn": 0,
            "inventory_turn_tokens_max": MAX_TOTAL_TOKENS_PER_TURN,
            "realization_turn_tokens_max": MAX_TOTAL_TOKENS_PER_TURN,
            "combined_tokens_max": PHASE_TOTAL_TOKEN_BOUND,
            "all_source_units_reviewed": True,
            "inventory_unresolved_count": 0,
            "inventory_no_signal_proposition_count": 0,
            "inventory_dense_proposition_count_minimum": MIN_DENSE_PROPOSITIONS,
            "realization_exactly_one_event_per_proposition": True,
            "exact_evidence_rate": 1.0,
            "metric_grounding_error_events": 0,
            "event_cap_violations": 0,
            "exact_identity_duplicates": 0,
            "production_amortized_total_token_ratio_max": 0.28,
            "support_alignment_required_after_structural_pass": True,
            "on_failure": "reject v234 and advance to ranked architecture 3",
        },
        "semantic_boundary": {
            "llm_owned": [
                "inventory proposition eligibility boundaries evidence and preliminary type",
                "full event realization and every semantic field",
                "realization evidence selection",
            ],
            "deterministic_only": [
                "schema IDs coverage order and one-to-one proposition projection",
                "exact evidence and offset projection",
                "literal metric grounding",
                "exact identity duplicate detection",
                "caps provenance lifecycle and accounting",
            ],
        },
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    design_path = root / "architecture-design.json"
    _write_stable_time(design_path, design, "created_at")
    inventory_records = _request_records(root, inventory["turn_name"])
    spec = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "frozen_before_conditional_two_turn_architecture_canary",
        "declared_turn_count": 2,
        "inventory_turn_name": inventory["turn_name"],
        "realization_turn_name": realization_name,
        "episode_id": inventory["episode_id"],
        "segment_ids": inventory["segment_ids"],
        "model": MODEL,
        "effort": EFFORT,
        "retry_count_per_turn": 0,
        "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": PHASE_TOTAL_TOKEN_BOUND,
        "inventory_prompt_bytes": inventory["prompt_bytes"],
        "inventory_base_instructions_bytes": inventory["base_bytes"],
        "inventory_schema_bytes": inventory["schema_bytes"],
        "realization_is_conditional_on_inventory_gate": True,
        "existing_events_visible_to_model": False,
        "reference_visible_to_model": False,
        "density_visible_to_model": False,
        "support_alignment_quality_measured": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "authorization": _record(authorization_path),
        "architecture_ranking": _record(ranking_path),
        "architecture_design": _record(design_path),
        "direct_lineage": lineage["records"],
        "inventory_request": inventory_records,
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "privacy": "private source prompts outputs; sanitized terminal and gates",
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
        "authorization": _record(authorization_path),
        "architecture_ranking": _record(ranking_path),
        "architecture_design": _record(design_path),
        "attempt_spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "inventory_request": inventory_records,
        "inventory_turn_name": inventory["turn_name"],
        "realization_turn_name": realization_name,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 2,
        "retry_count_per_turn": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(root / "runtime-lock.json", lock, "created_at")
    verify_runtime_lock(root / "runtime-lock.json")
    return _load_frozen(root)


def _validate_schema_output(
    schema: Mapping[str, Any], output: Mapping[str, Any], label: str
) -> None:
    try:
        _validate_schema(schema, output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V234OutputContractError(
            f"{label} structured output validation failed: {type(exc).__name__}"
        ) from exc


def _project_unit_range(
    *, source: Mapping[str, Any], start_id: str, end_id: str
) -> dict[str, Any]:
    units = list(source["units"])
    unit_index = {str(unit["unit_id"]): index for index, unit in enumerate(units)}
    if start_id not in unit_index or end_id not in unit_index:
        raise V234OutputContractError("evidence unit belongs to another segment")
    start_index = unit_index[start_id]
    end_index = unit_index[end_id]
    if start_index > end_index:
        raise V234OutputContractError("evidence unit range is reversed")
    start_char = int(units[start_index]["start_char"])
    end_char = int(units[end_index]["end_char"])
    text = str(source["segment_text"])
    evidence = text[start_char:end_char]
    if not evidence:
        raise V234OutputContractError("projected evidence is empty")
    boundaries = list(source["boundaries"])
    if not any(
        int(boundary["extract_start"]) <= start_char
        and end_char <= int(boundary["extract_end"])
        for boundary in boundaries
    ):
        raise V234OutputContractError(
            "projected evidence is outside every fixed evidence window"
        )
    return {
        "start_index": start_index,
        "end_index": end_index,
        "start_char": start_char,
        "end_char": end_char,
        "window_id": v232._owner_window(start_char, boundaries),
        "evidence": evidence,
    }


def _project_inventory(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    _validate_schema_output(turn["schema"], output, "inventory")
    if output.get("episode_id") != turn["episode_id"]:
        raise V234OutputContractError("inventory episode id drifted")
    rows = output.get("segments") or []
    if [row.get("segment_id") for row in rows] != list(turn["segment_ids"]):
        raise V234OutputContractError("inventory segment order or coverage drifted")
    source_by_id = {
        str(row["segment_id"]): row for row in turn["private_input"]["segments"]
    }
    normalized_rows = []
    provenance_rows = []
    diagnostics = []
    seen_exact = set()
    for segment_position, row in enumerate(rows):
        segment_id = str(row["segment_id"])
        source = source_by_id[segment_id]
        units = list(source["units"])
        expected_unit_ids = [str(unit["unit_id"]) for unit in units]
        receipts = list(row.get("unit_receipts") or [])
        if [receipt.get("unit_id") for receipt in receipts] != expected_unit_ids:
            raise V234OutputContractError("inventory unit receipt order or coverage drifted")
        audit = row.get("coverage_audit") or {}
        if (
            audit.get("all_source_units_reviewed") is not True
            or audit.get("unresolved_count") != 0
        ):
            raise V234OutputContractError("inventory source coverage audit failed")
        receipt_counts: dict[str, int] = {}
        for receipt in receipts:
            eligible = receipt.get("eligible_proposition_count")
            unresolved = receipt.get("unresolved_count")
            if (
                isinstance(eligible, bool)
                or not isinstance(eligible, int)
                or eligible < 0
                or unresolved != 0
            ):
                raise V234OutputContractError("inventory receipt count is invalid")
            receipt_counts[str(receipt["unit_id"])] = eligible
        propositions = list(row.get("propositions") or [])
        expected_ids = _proposition_ids(segment_position)[: len(propositions)]
        if [item.get("proposition_id") for item in propositions] != expected_ids:
            raise V234OutputContractError("inventory proposition id order drifted")
        if sum(receipt_counts.values()) != len(propositions):
            raise V234OutputContractError("inventory receipt total drifted")
        start_counts = {unit_id: 0 for unit_id in expected_unit_ids}
        prior_start = -1
        normalized_propositions = []
        for proposition in propositions:
            value = dict(proposition)
            start_id = str(value["evidence_start_unit_id"])
            end_id = str(value["evidence_end_unit_id"])
            projection = _project_unit_range(
                source=source, start_id=start_id, end_id=end_id
            )
            if projection["start_index"] < prior_start:
                raise V234OutputContractError("inventory evidence order drifted")
            prior_start = int(projection["start_index"])
            identity = _canonical_json(
                {
                    "event_type": value.get("event_type"),
                    "claim_text": value.get("claim_text"),
                    "evidence_start_unit_id": start_id,
                    "evidence_end_unit_id": end_id,
                }
            )
            if identity in seen_exact:
                raise V234OutputContractError("inventory exact proposition duplicate")
            seen_exact.add(identity)
            start_counts[start_id] += 1
            normalized_propositions.append(value)
            provenance_rows.append(
                {
                    "segment_id": segment_id,
                    "proposition_id": value["proposition_id"],
                    "evidence_start_unit_id": start_id,
                    "evidence_end_unit_id": end_id,
                    "start_char": projection["start_char"],
                    "end_char": projection["end_char"],
                    "window_id": projection["window_id"],
                    "evidence_sha256": sha256_text(projection["evidence"]),
                }
            )
        if start_counts != receipt_counts:
            raise V234OutputContractError("inventory receipt ownership drifted")
        normalized_rows.append(
            {"segment_id": segment_id, "propositions": normalized_propositions}
        )
        diagnostics.append(
            {
                "segment_id": segment_id,
                "density_stratum": source["density_stratum"],
                "source_unit_count": len(units),
                "reviewed_source_unit_count": len(receipts),
                "proposition_count": len(propositions),
                "unresolved_count": 0,
            }
        )
    normalized = {"episode_id": turn["episode_id"], "segments": normalized_rows}
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "episode_id": turn["episode_id"],
        "propositions": provenance_rows,
    }
    return normalized, provenance, diagnostics


def _build_inventory_gate(diagnostics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_segment = {str(row["segment_id"]): row for row in diagnostics}
    dense_count = int(
        by_segment.get(DENSE_SEGMENT_ID, {}).get("proposition_count", -1)
    )
    no_signal_count = int(
        by_segment.get(NO_SIGNAL_SEGMENT_ID, {}).get("proposition_count", -1)
    )
    checks = {
        "both_segments_validated": set(by_segment)
        == {DENSE_SEGMENT_ID, NO_SIGNAL_SEGMENT_ID},
        "all_source_units_reviewed": all(
            int(row["source_unit_count"])
            == int(row["reviewed_source_unit_count"])
            for row in diagnostics
        ),
        "unresolved_count_0": all(
            int(row["unresolved_count"]) == 0 for row in diagnostics
        ),
        "no_signal_proposition_count_0": no_signal_count == 0,
        "dense_proposition_count_gte_24": dense_count >= MIN_DENSE_PROPOSITIONS,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": SCHEMA_VERSION,
        "phase_id": PHASE_ID,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "diagnostics": list(diagnostics),
        "dense_proposition_count": dense_count,
        "no_signal_proposition_count": no_signal_count,
        "realization_authorized": not failed,
        "support_alignment_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _event_identity(event: Mapping[str, Any]) -> str:
    return _canonical_json({field: event.get(field) for field in IDENTITY_FIELDS})


def _project_realization(
    output: Mapping[str, Any],
    turn: Mapping[str, Any],
    source_input: Mapping[str, Any],
    inventory: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    _validate_schema_output(turn["schema"], output, "realization")
    if output.get("episode_id") != turn["episode_id"]:
        raise V234OutputContractError("realization episode id drifted")
    rows = output.get("segments") or []
    if [row.get("segment_id") for row in rows] != list(turn["segment_ids"]):
        raise V234OutputContractError("realization segment order or coverage drifted")
    source_by_id = {
        str(row["segment_id"]): row for row in source_input["segments"]
    }
    inventory_by_id = {
        str(row["segment_id"]): row for row in inventory["segments"]
    }
    normalized_rows = []
    provenance_rows = []
    diagnostics = []
    seen_global = set()
    for row in rows:
        segment_id = str(row["segment_id"])
        source = source_by_id[segment_id]
        propositions = inventory_by_id[segment_id]["propositions"]
        expected_ids = [item["proposition_id"] for item in propositions]
        events = list(row.get("events") or [])
        if [event.get("proposition_id") for event in events] != expected_ids:
            raise V234OutputContractError(
                "realization proposition coverage or order drifted"
            )
        if (row.get("status") == "coded") != bool(events):
            raise V234OutputContractError("realization coded status does not match events")
        if not events and row.get("status") != "no_signal":
            raise V234OutputContractError("empty realization must be no_signal")
        if len(events) > MAX_EVENTS_PER_SEGMENT:
            raise V234OutputContractError("realization event cap exceeded")
        prior_start = -1
        projected_events = []
        for event_index, raw_event in enumerate(events):
            event = dict(raw_event)
            proposition_id = str(event.pop("proposition_id"))
            start_id = str(event.pop("evidence_start_unit_id"))
            end_id = str(event.pop("evidence_end_unit_id"))
            projection = _project_unit_range(
                source=source, start_id=start_id, end_id=end_id
            )
            if projection["start_index"] < prior_start:
                raise V234OutputContractError("realization evidence order drifted")
            prior_start = int(projection["start_index"])
            metric_fields = (
                "metric_value",
                "metric_unit",
                "metric_comparator",
                "metric_raw_text",
            )
            metric_values = [str(event.get(field) or "") for field in metric_fields]
            if any(
                value and value not in projection["evidence"]
                for value in metric_values
            ):
                raise V234OutputContractError("metric literal is not in evidence")
            if any(metric_values) == (
                event.get("metric_direction") == "not_applicable"
            ):
                raise V234OutputContractError("metric direction applicability drifted")
            identity = _event_identity(event)
            if identity in seen_global:
                raise V234OutputContractError("exact event identity duplicate")
            seen_global.add(identity)
            event["window_id"] = projection["window_id"]
            event["evidence"] = projection["evidence"]
            projected_events.append(event)
            provenance_rows.append(
                {
                    "segment_id": segment_id,
                    "event_index": event_index,
                    "proposition_id": proposition_id,
                    "evidence_start_unit_id": start_id,
                    "evidence_end_unit_id": end_id,
                    "start_char": projection["start_char"],
                    "end_char": projection["end_char"],
                    "window_id": projection["window_id"],
                    "evidence_sha256": sha256_text(projection["evidence"]),
                }
            )
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
                "inventory_proposition_count": len(propositions),
                "realized_event_count": len(projected_events),
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
        raise V234TwoPassError("turn usage is absent")
    result = {}
    for field in USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise V234TwoPassError("turn usage is incomplete")
        result[field] = value
    return result


def _validate_measured_sidecar(path: Path, *, turn_name: str) -> dict[str, int]:
    sidecar = _load_json(path, f"{turn_name} sidecar")
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
        raise V234TwoPassError(f"{turn_name} measured sidecar contract failed")
    return usage


def _combine_usage(usages: Sequence[Mapping[str, int]]) -> dict[str, int]:
    return {field: sum(int(row[field]) for row in usages) for field in USAGE_FIELDS}


def _zero_usage() -> dict[str, int]:
    return {field: 0 for field in USAGE_FIELDS}


def _build_structural_gate(
    *,
    inventory_usage: Mapping[str, int],
    realization_usage: Mapping[str, int],
    diagnostics: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    combined = _combine_usage([inventory_usage, realization_usage])
    production_total = (
        PRODUCTION_AMORTIZED_CONTEXT_TOKENS
        + combined["total_tokens"] * PRODUCTION_SCALE
    )
    production_ratio = production_total / BASELINE_END_TO_END_TOKENS
    checks = {
        "both_segments_validated": {str(row["segment_id"]) for row in diagnostics}
        == {DENSE_SEGMENT_ID, NO_SIGNAL_SEGMENT_ID},
        "exactly_one_event_per_inventory_proposition": all(
            int(row["inventory_proposition_count"])
            == int(row["realized_event_count"])
            for row in diagnostics
        ),
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "inventory_turn_tokens_lte_35000": inventory_usage["total_tokens"]
        <= MAX_TOTAL_TOKENS_PER_TURN,
        "realization_turn_tokens_lte_35000": realization_usage["total_tokens"]
        <= MAX_TOTAL_TOKENS_PER_TURN,
        "combined_tokens_lte_70000": combined["total_tokens"]
        <= PHASE_TOTAL_TOKEN_BOUND,
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
        "inventory_usage": dict(inventory_usage),
        "realization_usage": dict(realization_usage),
        "combined_usage": combined,
        "diagnostics": list(diagnostics),
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(production_ratio, 6),
        "support_alignment_quality_measured": False,
        "support_alignment_authorized": not failed,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _freeze_realization_continuation(
    *,
    frozen: Mapping[str, Any],
    inventory: Mapping[str, Any],
    inventory_gate_path: Path,
) -> dict[str, Any]:
    root = Path(frozen["root"])
    inventory_gate = _load_json(inventory_gate_path, "v234 inventory gate")
    if inventory_gate.get("passed") is not True:
        raise V234ArchitectureStop("inventory gate did not authorize realization")
    lineage = _validate_lineage()
    packet = _source_packet(lineage)
    realization = _prepare_realization_turn(packet, inventory)
    if realization["turn_name"] != frozen["realization_turn_name"]:
        raise V234TwoPassError("realization turn name drifted")
    paths = _turn_paths(root, realization["turn_name"])
    paths["root"].mkdir(parents=True, exist_ok=True)
    _write_immutable(paths["input"], realization["private_input"])
    _write_private_text(paths["prompt"], realization["prompt"])
    _write_private_text(paths["base"], realization["base"])
    _write_immutable(paths["schema"], realization["schema"])
    inventory_paths = frozen["inventory"]["paths"]
    inventory_usage = _validate_measured_sidecar(
        inventory_paths["sidecar"], turn_name=frozen["inventory"]["turn_name"]
    )
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "state": "inventory_gate_passed_realization_request_frozen",
        "inventory_gate": _record(inventory_gate_path),
        "inventory_usage": inventory_usage,
        "inventory_output": _record(inventory_paths["output"]),
        "inventory_normalized": _record(inventory_paths["normalized"]),
        "inventory_provenance": _record(inventory_paths["provenance"]),
        "inventory_sidecar": _record(inventory_paths["sidecar"]),
        "inventory_capacity": _record(inventory_paths["capacity"]),
        "realization_turn_name": realization["turn_name"],
        "realization_prompt_bytes": realization["prompt_bytes"],
        "realization_base_instructions_bytes": realization["base_bytes"],
        "realization_schema_bytes": realization["schema_bytes"],
        "holdout_authorized": False,
        "production_mutated": False,
    }
    receipt_path = root / "inventory-phase-receipt.json"
    _write_stable_time(receipt_path, receipt, "created_at")
    inventory_artifacts = [
        _record(inventory_paths[name])
        for name in (
            "capacity",
            "sidecar",
            "output",
            "normalized",
            "provenance",
            "diagnostics",
        )
    ] + [_record(inventory_gate_path)]
    lock = {
        "schema_version": CONTINUATION_LOCK_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "initial_runtime_lock": _record(frozen["runtime_lock"]),
        "inventory_phase_receipt": _record(receipt_path),
        "inventory_artifacts": inventory_artifacts,
        "realization_request": _request_records(root, realization["turn_name"]),
        "capacity_policy": _record(frozen["capacity_policy"]),
        "realization_turn_name": realization["turn_name"],
        "retry_count": 0,
        "inventory_gate_passed": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    lock_path = root / "realization-continuation-lock.json"
    _write_stable_time(lock_path, lock, "created_at")
    verify_continuation_lock(lock_path)
    refreshed = _load_frozen(root)
    refreshed["continuation_lock"] = lock_path
    return refreshed


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


def _collect_attempt_usage(frozen: Mapping[str, Any]) -> dict[str, Any]:
    usages = []
    sidecars = []
    attempted = 0
    unknown = 0
    for phase in ("inventory", "realization"):
        turn = frozen.get(phase)
        if not isinstance(turn, Mapping):
            continue
        paths = turn["paths"]
        if paths["capacity"].is_file():
            attempted += 1
        if paths["sidecar"].is_file():
            sidecars.append(_record(paths["sidecar"]))
            try:
                usages.append(_usage(_load_json(paths["sidecar"], f"{phase} sidecar")))
            except V234TwoPassError:
                unknown += 1
        elif paths["capacity"].is_file():
            unknown += 1
    return {
        "attempted": attempted,
        "unknown": unknown,
        "usage": _combine_usage(usages) if usages else _zero_usage(),
        "sidecars": sidecars,
    }


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    refreshed = _load_frozen(root)
    accounting = _collect_attempt_usage(refreshed)
    semantic_failure = isinstance(
        exc, (V234OutputContractError, V234ArchitectureStop)
    )
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v234_two_pass_architecture_structural_or_count_gate_not_passed"
            if semantic_failure
            else "infrastructure_or_judge_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": accounting["attempted"],
        "semantic_retry_count": 0,
        "usage_status": "unknown" if accounting["unknown"] else "complete",
        "accounting_complete": accounting["unknown"] == 0,
        "usage": accounting["usage"],
        "unknown_usage_attempt_count": accounting["unknown"],
        "sidecars": accounting["sidecars"],
        "architecture_strategy_rejected": semantic_failure,
        "isolated_field_repair_authorized": False,
        "next_distinct_architecture_authorized": semantic_failure,
        "support_alignment_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "architecture_ranking": _record(frozen["ranking_path"]),
        "exact_next_action": (
            "freeze v234 and advance to ranked architecture 3 without a field patch"
            if semantic_failure
            else "audit the immutable infrastructure attempt; no silent retry"
        ),
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v234(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v234 terminal")
    frozen = freeze_v234(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        return _failure_terminal(
            root, frozen, V234TwoPassError("launch receipt exists; replay prohibited")
        )
    _write_immutable(
        launch_path,
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_turn_count": 2,
            "conditional_realization": True,
            "inventory_turn_name": frozen["inventory"]["turn_name"],
            "realization_turn_name": frozen["realization_turn_name"],
            "retry_count_per_turn": 0,
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
    inventory_paths = frozen["inventory"]["paths"]
    try:
        async with client_factory(frozen["capacity_policy"]) as client:
            inventory_result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=frozen["inventory"]["base"],
                prompt=frozen["inventory"]["prompt"],
                output_schema=frozen["inventory"]["schema"],
                cwd=PROJECT_ROOT,
                sidecar_path=inventory_paths["sidecar"],
                output_path=inventory_paths["output"],
                batch_size=2,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=inventory_paths["capacity"],
            )
            if (
                inventory_result.status_ok is not True
                or not isinstance(inventory_result.output, Mapping)
            ):
                raise V234TwoPassError("blind inventory turn did not complete")
            inventory_usage = _validate_measured_sidecar(
                inventory_paths["sidecar"],
                turn_name=frozen["inventory"]["turn_name"],
            )
            inventory, inventory_provenance, inventory_diagnostics = _project_inventory(
                inventory_result.output, frozen["inventory"]
            )
            _write_immutable(inventory_paths["normalized"], inventory)
            _write_immutable(inventory_paths["provenance"], inventory_provenance)
            _write_immutable(inventory_paths["diagnostics"], inventory_diagnostics)
            inventory_gate = _build_inventory_gate(inventory_diagnostics)
            inventory_gate_path = root / "inventory-structural-gate.json"
            _write_immutable(inventory_gate_path, inventory_gate)
            if not inventory_gate["passed"]:
                raise V234ArchitectureStop("v234 inventory gate failed")
            frozen = _freeze_realization_continuation(
                frozen=frozen,
                inventory=inventory,
                inventory_gate_path=inventory_gate_path,
            )
            verify_continuation_lock(frozen["continuation_lock"])
            realization = frozen["realization"]
            realization_paths = realization["paths"]
            realization_result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=realization["base"],
                prompt=realization["prompt"],
                output_schema=realization["schema"],
                cwd=PROJECT_ROOT,
                sidecar_path=realization_paths["sidecar"],
                output_path=realization_paths["output"],
                batch_size=2,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=realization_paths["capacity"],
            )
        if (
            realization_result.status_ok is not True
            or not isinstance(realization_result.output, Mapping)
        ):
            raise V234TwoPassError("blind realization turn did not complete")
        realization_usage = _validate_measured_sidecar(
            realization_paths["sidecar"], turn_name=realization["turn_name"]
        )
        normalized, provenance, diagnostics = _project_realization(
            realization_result.output,
            realization,
            frozen["inventory"]["private_input"],
            inventory,
        )
        _write_immutable(realization_paths["normalized"], normalized)
        _write_immutable(realization_paths["provenance"], provenance)
        _write_immutable(realization_paths["diagnostics"], diagnostics)
        gate = _build_structural_gate(
            inventory_usage=inventory_usage,
            realization_usage=realization_usage,
            diagnostics=diagnostics,
        )
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise V234ArchitectureStop("v234 realization structural or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v234_architecture_structural_gate_passed",
            "terminal_reason": "v234_two_pass_structural_cost_gate_passed",
            "semantic_attempt_count": 2,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": gate["combined_usage"],
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
            "inventory_gate": _record(root / "inventory-structural-gate.json"),
            "gate": _record(gate_path),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "continuation_lock": _record(frozen["continuation_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "architecture_ranking": _record(frozen["ranking_path"]),
            "sidecars": [
                _record(inventory_paths["sidecar"]),
                _record(realization_paths["sidecar"]),
            ],
            "exact_next_action": (
                "run the frozen side-free support and neutral alignment canary over "
                "the v234 events without changing the extractor"
            ),
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v234 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run v234 blind inventory-then-realization canary"
    )
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v234(output_dir=Path(args.output_dir))
        design = _load_json(frozen["design_path"], "v234 design")
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "declared_turn_count": frozen["spec"]["declared_turn_count"],
            "inventory_prompt_bytes": frozen["spec"]["inventory_prompt_bytes"],
            "inventory_schema_bytes": frozen["spec"]["inventory_schema_bytes"],
            "projected_ratio": design["production_cost_projection"][
                "projected_production_amortized_total_token_ratio"
            ],
        }
    else:
        terminal = asyncio.run(
            run_v234(
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
