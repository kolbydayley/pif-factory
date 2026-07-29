from __future__ import annotations

"""Run the bounded v233 blind source-unit extraction architecture canary."""

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
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v233_blind_unit_sweep_v1"
RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_selection_v233_blind_unit_sweep_runtime_lock_v1"
)
TERMINAL_VERSION = (
    "pif_app_server_judge_v5_selection_v233_blind_unit_sweep_terminal_v1"
)
PHASE_ID = "development_selection_v5_4_v233_blind_unit_sweep"
MODEL = "gpt-5.6-sol"
EFFORT = "low"
MAX_TOTAL_TOKENS = 60_000
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
DENSE_SEGMENT_ID = "seg_c84d5569778c65202b572971"
NO_SIGNAL_SEGMENT_ID = "seg_5d6fb274a5d25880b9db3006"
DENSE_REFERENCE_COUNT = 27
MIN_DENSE_EVENTS_FOR_FROZEN_SOURCE_FLOOR = 24
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
V227_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v227-independent-completeness-strategy"
).resolve()
V232_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v232-window-ledger-architecture"
).resolve()
SOURCE_TURN_ROOT = (
    V227_ROOT / "turns/v227-luna-gap-ba5e86bd4ce77f079d3379fb"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v233-blind-unit-sweep"
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
    "v232_terminal": "6de30d79232dbd457cd8889065803ce57a7673b9ff71c59d43e904473dfcac91",
    "v232_futility_audit": "c48f71ebe0f3797bd033a93988aaa7b4cb65f6e62218977952a9946d7eeb204f",
}
IDENTITY_FIELDS = v232.IDENTITY_FIELDS


class V233BlindUnitSweepError(RuntimeError):
    """The v233 architecture canary cannot proceed safely."""


class V233OutputContractError(V233BlindUnitSweepError):
    """A completed output violated the frozen structural contract."""


class V233ArchitectureStop(V233BlindUnitSweepError):
    """The canary cannot reach the frozen development gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V233BlindUnitSweepError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V233BlindUnitSweepError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V233BlindUnitSweepError(f"frozen {path.name} drifted")
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


def _source_paths() -> dict[str, Path]:
    return {
        "source_input": SOURCE_TURN_ROOT / "input.private.json",
        "source_base": SOURCE_TURN_ROOT / "base-instructions.private.md",
        "source_schema": SOURCE_TURN_ROOT / "schema.json",
        "v232_terminal": V232_ROOT / "terminal.json",
        "v232_futility_audit": (
            PIPELINE_ROOT
            / "v232-futility-validity-audit-2026-07-17/report.json"
        ),
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _source_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V233BlindUnitSweepError(f"frozen lineage {name} drifted")
    terminal = _load_json(paths["v232_terminal"], "v232 terminal")
    audit = _load_json(paths["v232_futility_audit"], "v232 futility audit")
    if (
        terminal.get("terminal_reason")
        != "v232_architecture_structural_or_count_stop_rule_not_passed"
        or terminal.get("production_mutated") is not False
        or terminal.get("holdout_authorized") is not False
        or audit.get("production_mutated") is not False
        or (audit.get("futility_decision") or {}).get(
            "one_to_one_count_ceiling_is_valid"
        )
        is not True
    ):
        raise V233BlindUnitSweepError("v232 predecessor state drifted")
    return {"paths": paths, "records": records}


def _event_schema(
    source_schema: Mapping[str, Any], unit_ids: Sequence[str]
) -> dict[str, Any]:
    return v232._event_schema(source_schema, unit_ids)


def _output_schema(
    *,
    episode_id: str,
    segment_ids: Sequence[str],
    source_schema: Mapping[str, Any],
    unit_ids: Sequence[str],
    max_unit_count: int,
) -> dict[str, Any]:
    source_segment = source_schema["properties"]["segments"]["items"]
    receipt = {
        "type": "object",
        "additionalProperties": False,
        "required": ["unit_id", "eligible_event_count", "unresolved_count"],
        "properties": {
            "unit_id": {"type": "string", "enum": list(unit_ids)},
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
            "segment_id": {"type": "string", "enum": list(segment_ids)},
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
                "maxItems": max_unit_count,
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


BASE_INSTRUCTIONS = """You are a systematic, topic-general semantic event extractor for a private podcast research corpus. This is a blind standalone extraction pass. You have no existing events, reference answer, density label, target event count, or expected topic. Read every source unit and make every semantic decision from the language itself; never use keyword, regex, phrase, fixed-topic, or event-count rules.

For each segment, review every source unit exactly once and return unit_receipts in the exact input order. eligible_event_count is the number of distinct returned events whose evidence begins at that unit. unresolved_count must be zero. The sum of eligible_event_count across unit_receipts must equal the number of returned events. coverage_audit.all_source_units_reviewed must be true and coverage_audit.unresolved_count must be zero.

Extract every distinct research-useful atomic proposition. Split independently meaningful premises, mechanisms, capabilities, constraints, comparisons, outcomes, alternatives, frames, uncertainties, counterclaims, product signals, market signals, risks, adoption signals, term uses, and stance positions. Keep inseparable parts together only when needed for one truth-conditional claim. Do not collapse two propositions because they share a topic, actor, or evidence unit. Do not invent an event merely to populate a unit receipt.

For evidence, select the smallest self-contained contiguous range of opaque source unit IDs that supports every material event field. The range must fit inside one fixed source window, may include adjacent units when needed for attribution or coreference, and must not cross segments. Return only evidence_start_unit_id and evidence_end_unit_id; never copy evidence text or invent IDs. Deterministic code projects the exact source substring and offsets.

Populate every requested semantic field. speaker is who says the words; actor is whose position or action is represented; reported_actor is a quoted or reported source. claim_text is a concise complete proposition naming the relevant actor and target. Every nonempty metric_value, metric_unit, metric_comparator, and metric_raw_text must be a literal contiguous substring of the selected evidence. Use metric_direction=not_applicable when all metric strings are empty.

Event families include term_usage, frame_usage, stance_position, forecast, causal_mechanism, capability_claim, product_signal, market_signal, risk_signal, counterclaim, uncertainty, adoption_signal, actor_mention, and entity_reference. Classify by the proposition's main predicate and evidential commitment, not merely its topic.

Order events by evidence_start_unit_id and then by their order in the source. Use coded if and only if at least one event is returned. Otherwise use no_signal with events=[] and explain why the source contains no eligible research event. Classify segment_source_context from the source. Keep segment order equal to input order, enforce the per-segment 32-event cap, and return schema-valid JSON only."""


def _turn_name(episode_id: str) -> str:
    return "v233_blind_unit_sweep_" + sha256_text(episode_id)[:20]


def _prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    paths = lineage["paths"]
    source = _load_json(paths["source_input"], "v227 source input")
    source_schema = _load_json(paths["source_schema"], "v227 source schema")
    context = v232._shared_context(paths["source_base"])
    episode_id = str(source["episode_id"])
    if context.get("episode_id") != episode_id:
        raise V233BlindUnitSweepError("episode context id drifted")
    private_segments = []
    prompt_segments = []
    all_unit_ids = []
    segment_ids = []
    for position, row in enumerate(source["normalization_segments"]):
        segment_id = str(row["segment_id"])
        segment_ids.append(segment_id)
        text = str(row["segment_text"])
        boundaries = list(row["boundaries"])
        units = v232.source_units(
            text, boundaries, segment_position=position
        )
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
        raise V233BlindUnitSweepError("canary segment membership drifted")
    packet = {"episode_id": episode_id, "segments": prompt_segments}
    prompt = "# Blind source-unit packet\n" + json.dumps(
        packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"
    base = BASE_INSTRUCTIONS + "\n\n# Immutable episode context\n" + json.dumps(
        context, ensure_ascii=True, separators=(",", ":")
    ) + "\n"
    schema = _output_schema(
        episode_id=episode_id,
        segment_ids=segment_ids,
        source_schema=source_schema,
        unit_ids=all_unit_ids,
        max_unit_count=max(len(row["units"]) for row in private_segments),
    )
    prompt_bytes = len(prompt.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if prompt_bytes > MAX_PROMPT_BYTES:
        raise V233BlindUnitSweepError("v233 prompt exceeds frozen byte cap")
    if schema_bytes > MAX_SCHEMA_BYTES:
        raise V233BlindUnitSweepError("v233 schema exceeds frozen byte cap")
    return {
        "turn_name": _turn_name(episode_id),
        "episode_id": episode_id,
        "segment_ids": segment_ids,
        "private_input": {
            "schema_version": SCHEMA_VERSION,
            "episode_id": episode_id,
            "segments": private_segments,
            "privacy": "private source units and context",
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


def _capacity_policy(root: Path, turn_name: str) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected_points = math.ceil(
        MAX_TOTAL_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
            "phase_total_token_bound": MAX_TOTAL_TOKENS,
            "projected_phase_quota_points": projected_points,
            "minimum_remaining_reserve_percent": (
                MIN_REMAINING_RESERVE_PERCENT
            ),
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
    return [
        {
            "rank": 1,
            "architecture_id": "blind_per_unit_standalone_sweep",
            "distinctive_mechanism": (
                "one mandatory LLM receipt per opaque source unit and a standalone "
                "event set with no existing-event or reference anchoring"
            ),
            "evidence_based_rationale": (
                "v232 found 41 eligible atomic claims but marked 32 represented; "
                "removing the existing ledger directly tests anchoring while preserving "
                "exact evidence projection"
            ),
            "cost_risk": "low",
            "quality_risk": "moderate_under_extraction_if_unit_receipts_are_shallow",
        },
        {
            "rank": 2,
            "architecture_id": "two_pass_blind_inventory_then_realization",
            "distinctive_mechanism": (
                "a first LLM turn freezes exhaustive propositions before a separate "
                "LLM turn realizes the full event schema"
            ),
            "evidence_based_rationale": (
                "separates recall from schema burden if rank 1 remains sparse, but "
                "adds an inventory bottleneck and a second production turn"
            ),
            "cost_risk": "moderate",
            "quality_risk": "inventory_omissions_cannot_be_recovered_downstream",
        },
        {
            "rank": 3,
            "architecture_id": "exact_coverage_mask_gap_extraction_and_llm_reconciliation",
            "distinctive_mechanism": (
                "hide existing event semantics, expose only exact covered spans, extract "
                "from uncovered source, then use an LLM to reconcile semantic duplicates"
            ),
            "evidence_based_rationale": (
                "targets omissions without semantic anchoring, but additive base cost has "
                "less token headroom and exact span masks cannot prove semantic coverage"
            ),
            "cost_risk": "moderate_high",
            "quality_risk": "duplicate_and_paraphrase_reconciliation",
        },
        {
            "rank": 4,
            "architecture_id": "independent_specialist_ensemble_with_llm_consolidation",
            "distinctive_mechanism": (
                "multiple topic-general proposition lenses followed by an LLM-owned merge"
            ),
            "evidence_based_rationale": (
                "could raise recall, but prior partition experiments overproduced lenses "
                "and repeated transcript tokens"
            ),
            "cost_risk": "high",
            "quality_risk": "overproduction_and_boundary_consolidation",
        },
    ]


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v233 runtime lock")
    root = path.parent.resolve()
    expected_runtime = {str(item) for item in _runtime_files()}
    actual_runtime = {
        str(Path(row["path"]).expanduser().resolve())
        for row in lock.get("runtime_files") or []
    }
    turn_name = str(lock.get("turn_name") or "")
    expected_request_paths = {
        row["path"] for row in _request_records(root, turn_name)
    }
    actual_request_paths = {
        row.get("path") for row in lock.get("frozen_request") or []
    }
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("retry_count") != 0
        or not turn_name
        or actual_runtime != expected_runtime
        or actual_request_paths != expected_request_paths
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V233BlindUnitSweepError("v233 runtime lock contract drifted")
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
        *(lock.get("frozen_request") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V233BlindUnitSweepError("v233 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        record["path"] for record in lineage["records"].values()
    }:
        raise V233BlindUnitSweepError("v233 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v233 attempt spec")
    turn_name = str(spec["turn_name"])
    paths = _turn_paths(root, turn_name)
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "design_path": root / "architecture-design.json",
        "ranking_path": root / "architecture-ranking.json",
        "authorization_path": root / "authorization.json",
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": {
            "turn_name": turn_name,
            "episode_id": spec["episode_id"],
            "segment_ids": spec["segment_ids"],
            "paths": paths,
            "private_input": _load_json(paths["input"], "v233 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v233 schema"),
        },
    }


def freeze_v233(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {
            "root": root,
            "terminal": _load_json(root / "terminal.json", "v233 terminal"),
        }
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V233BlindUnitSweepError("unfinished v233 root is not replayable")
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
    authorization = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "authority": "direct_operator_steering_2026_07_17",
        "scope": (
            "additional bounded architecture-level experiments one at a time; "
            "no isolated field patches"
        ),
        "managed_chatgpt_app_server_only": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    authorization_path = root / "authorization.json"
    _write_stable_time(authorization_path, authorization, "created_at")
    ranking = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "selected_architecture_id": "blind_per_unit_standalone_sweep",
        "architectures": _architecture_ranking(),
        "selection_basis": (
            "v232 same-turn existing-event comparison overmatched 32 of 41 eligible "
            "claims while returning only 9 new events; the selected architecture "
            "removes that anchor at the lowest projected production cost"
        ),
        "on_failure": (
            "freeze v233 and select the next genuinely distinct architecture; do not "
            "create an isolated field or validator repair"
        ),
    }
    ranking_path = root / "architecture-ranking.json"
    _write_stable_time(ranking_path, ranking, "created_at")
    projected_total = (
        PRODUCTION_AMORTIZED_CONTEXT_TOKENS
        + MAX_TOTAL_TOKENS * PRODUCTION_SCALE
    )
    design = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "architecture_id": "blind_per_unit_standalone_sweep",
        "hypothesis": (
            "removing all existing-event semantics and requiring one LLM receipt per "
            "opaque source unit will prevent represented-event anchoring and recover "
            "enough grounded atomic events to reach the frozen source floor"
        ),
        "representative_canary": {
            "episode_count": 1,
            "segment_count": 2,
            "dense_count": 1,
            "no_signal_count": 1,
            "source_count": 1,
            "reuses_exact_v232_failed_source": True,
            "reference_existing_events_and_density_visible_to_model": False,
        },
        "production_cost_projection": {
            "episode_context_tokens": PRODUCTION_AMORTIZED_CONTEXT_TOKENS,
            "canary_turn_token_hard_max": MAX_TOTAL_TOKENS,
            "production_scale": PRODUCTION_SCALE,
            "projected_production_amortized_total_tokens": projected_total,
            "projected_production_amortized_total_token_ratio": round(
                projected_total / BASELINE_END_TO_END_TOKENS, 6
            ),
            "required_ratio_max": 0.28,
        },
        "predeclared_stop_rules": {
            "retry_count": 0,
            "total_tokens_max": MAX_TOTAL_TOKENS,
            "all_source_units_reviewed": True,
            "unresolved_count": 0,
            "exact_evidence_rate": 1.0,
            "metric_grounding_error_events": 0,
            "event_cap_violations": 0,
            "exact_identity_duplicates": 0,
            "no_signal_event_count": 0,
            "dense_event_count_minimum_for_possible_frozen_source_floor": (
                MIN_DENSE_EVENTS_FOR_FROZEN_SOURCE_FLOOR
            ),
            "support_alignment_required_after_structural_pass": True,
            "on_failure": "reject v233 and advance to ranked architecture 2",
        },
        "semantic_boundary": {
            "llm_owned": [
                "unit-level eligibility and atomic event count",
                "event discovery and boundaries",
                "all event semantics",
                "source-unit evidence selection",
            ],
            "deterministic_only": [
                "schema enum and unit coverage validation",
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
    request_records = _request_records(root, turn["turn_name"])
    spec = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "frozen_before_one_turn_architecture_canary",
        "declared_turn_count": 1,
        "turn_name": turn["turn_name"],
        "episode_id": turn["episode_id"],
        "segment_ids": turn["segment_ids"],
        "model": MODEL,
        "effort": EFFORT,
        "retry_count": 0,
        "maximum_total_tokens": MAX_TOTAL_TOKENS,
        "prompt_bytes": turn["prompt_bytes"],
        "base_instructions_bytes": turn["base_bytes"],
        "schema_bytes": turn["schema_bytes"],
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
        "frozen_request": request_records,
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "privacy": "private source prompts outputs; sanitized terminal and gate",
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
        "frozen_request": request_records,
        "turn_name": turn["turn_name"],
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
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
        raise V233OutputContractError(
            f"structured output validation failed: {type(exc).__name__}"
        ) from exc
    if output.get("episode_id") != turn["episode_id"]:
        raise V233OutputContractError("episode id drifted")
    rows = output.get("segments") or []
    if [row.get("segment_id") for row in rows] != list(turn["segment_ids"]):
        raise V233OutputContractError("segment order or coverage drifted")
    source_by_id = {
        str(row["segment_id"]): row
        for row in turn["private_input"]["segments"]
    }
    normalized_rows = []
    provenance_rows = []
    diagnostics = []
    seen_global = set()
    for row in rows:
        segment_id = str(row["segment_id"])
        source = source_by_id[segment_id]
        units = list(source["units"])
        expected_unit_ids = [str(unit["unit_id"]) for unit in units]
        receipts = list(row.get("unit_receipts") or [])
        if [receipt.get("unit_id") for receipt in receipts] != expected_unit_ids:
            raise V233OutputContractError("unit receipt order or coverage drifted")
        coverage_audit = row.get("coverage_audit") or {}
        if (
            coverage_audit.get("all_source_units_reviewed") is not True
            or coverage_audit.get("unresolved_count") != 0
        ):
            raise V233OutputContractError("source unit coverage audit failed")
        receipt_counts = {}
        for receipt in receipts:
            eligible = receipt.get("eligible_event_count")
            unresolved = receipt.get("unresolved_count")
            if (
                isinstance(eligible, bool)
                or not isinstance(eligible, int)
                or eligible < 0
                or unresolved != 0
            ):
                raise V233OutputContractError("unit receipt count is invalid")
            receipt_counts[str(receipt["unit_id"])] = eligible
        events = list(row.get("events") or [])
        if sum(receipt_counts.values()) != len(events):
            raise V233OutputContractError("unit receipt event total drifted")
        if (row.get("status") == "coded") != bool(events):
            raise V233OutputContractError("coded status does not match events")
        if not events and row.get("status") != "no_signal":
            raise V233OutputContractError("empty extraction must be no_signal")
        if len(events) > MAX_EVENTS_PER_SEGMENT:
            raise V233OutputContractError("event cap exceeded")
        unit_index = {
            str(unit["unit_id"]): index for index, unit in enumerate(units)
        }
        start_counts = {unit_id: 0 for unit_id in expected_unit_ids}
        prior_start_index = -1
        projected_events = []
        text = str(source["segment_text"])
        boundaries = list(source["boundaries"])
        for event_index, raw_event in enumerate(events):
            event = dict(raw_event)
            start_id = str(event.pop("evidence_start_unit_id"))
            end_id = str(event.pop("evidence_end_unit_id"))
            if start_id not in unit_index or end_id not in unit_index:
                raise V233OutputContractError(
                    "evidence unit belongs to another segment"
                )
            start_index = unit_index[start_id]
            end_index = unit_index[end_id]
            if start_index > end_index:
                raise V233OutputContractError("evidence unit range is reversed")
            if start_index < prior_start_index:
                raise V233OutputContractError("event evidence order drifted")
            prior_start_index = start_index
            start_char = int(units[start_index]["start_char"])
            end_char = int(units[end_index]["end_char"])
            evidence = text[start_char:end_char]
            if not evidence:
                raise V233OutputContractError("projected evidence is empty")
            if not any(
                int(boundary["extract_start"]) <= start_char
                and end_char <= int(boundary["extract_end"])
                for boundary in boundaries
            ):
                raise V233OutputContractError(
                    "projected evidence is outside every fixed evidence window"
                )
            window_id = v232._owner_window(start_char, boundaries)
            metric_fields = (
                "metric_value",
                "metric_unit",
                "metric_comparator",
                "metric_raw_text",
            )
            metric_values = [str(event.get(field) or "") for field in metric_fields]
            if any(value and value not in evidence for value in metric_values):
                raise V233OutputContractError("metric literal is not in evidence")
            if any(metric_values) == (
                event.get("metric_direction") == "not_applicable"
            ):
                raise V233OutputContractError(
                    "metric direction applicability drifted"
                )
            identity = _event_identity(event)
            if identity in seen_global:
                raise V233OutputContractError("exact event identity duplicate")
            seen_global.add(identity)
            event["window_id"] = window_id
            event["evidence"] = evidence
            projected_events.append(event)
            start_counts[start_id] += 1
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
            raise V233OutputContractError("unit receipt ownership drifted")
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
                "source_unit_count": len(units),
                "reviewed_source_unit_count": len(receipts),
                "event_count": len(projected_events),
                "unresolved_count": 0,
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
        raise V233BlindUnitSweepError("turn usage is absent")
    result = {}
    for field in USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise V233BlindUnitSweepError("turn usage is incomplete")
        result[field] = value
    return result


def _validate_measured_sidecar(path: Path) -> dict[str, int]:
    sidecar = _load_json(path, "v233 sidecar")
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
        or usage["total_tokens"] > MAX_TOTAL_TOKENS
    ):
        raise V233BlindUnitSweepError("measured turn sidecar contract failed")
    return usage


def _zero_usage() -> dict[str, int]:
    return {field: 0 for field in USAGE_FIELDS}


def _build_gate(
    *, usage: Mapping[str, int], diagnostics: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    by_segment = {str(row["segment_id"]): row for row in diagnostics}
    dense_count = int(by_segment.get(DENSE_SEGMENT_ID, {}).get("event_count", -1))
    no_signal_count = int(
        by_segment.get(NO_SIGNAL_SEGMENT_ID, {}).get("event_count", -1)
    )
    represented_ceiling = min(dense_count, DENSE_REFERENCE_COUNT)
    dense_f1_ceiling = (
        2 * represented_ceiling / (dense_count + DENSE_REFERENCE_COUNT)
        if dense_count >= 0
        else 0.0
    )
    source_macro_ceiling = (1.0 + dense_f1_ceiling) / 2.0
    production_total = (
        PRODUCTION_AMORTIZED_CONTEXT_TOKENS
        + int(usage["total_tokens"]) * PRODUCTION_SCALE
    )
    production_ratio = production_total / BASELINE_END_TO_END_TOKENS
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
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "no_signal_event_count_0": no_signal_count == 0,
        "dense_event_count_gte_24": dense_count
        >= MIN_DENSE_EVENTS_FOR_FROZEN_SOURCE_FLOOR,
        "total_tokens_lte_60000": int(usage["total_tokens"])
        <= MAX_TOTAL_TOKENS,
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
        "dense_count_based_f1_ceiling": round(dense_f1_ceiling, 6),
        "source_macro_count_based_f1_ceiling": round(source_macro_ceiling, 6),
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(production_ratio, 6),
        "support_alignment_quality_measured": False,
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
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    paths = frozen["turn"]["paths"]
    attempted = int(paths["capacity"].exists())
    usage = _zero_usage()
    unknown_usage = attempted
    sidecar_record = None
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        try:
            usage = _usage(_load_json(paths["sidecar"], "v233 sidecar"))
            unknown_usage = 0
        except V233BlindUnitSweepError:
            unknown_usage = 1
    semantic_failure = isinstance(
        exc, (V233OutputContractError, V233ArchitectureStop)
    )
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v233_blind_unit_sweep_structural_or_count_gate_not_passed"
            if semantic_failure
            else "infrastructure_or_judge_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": attempted,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown_usage else "complete",
        "accounting_complete": unknown_usage == 0,
        "usage": usage,
        "unknown_usage_attempt_count": unknown_usage,
        "sidecar": sidecar_record,
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
            "freeze v233 and advance to ranked architecture 2 without a field patch"
            if semantic_failure
            else "audit the immutable infrastructure attempt; no silent retry"
        ),
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v233(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v233 terminal")
    frozen = freeze_v233(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        return _failure_terminal(
            root,
            frozen,
            V233BlindUnitSweepError("launch receipt exists; replay prohibited"),
        )
    _write_immutable(
        launch_path,
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_turn_count": 1,
            "turn_name": frozen["turn"]["turn_name"],
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
            raise V233BlindUnitSweepError("blind unit sweep did not complete")
        usage = _validate_measured_sidecar(paths["sidecar"])
        normalized, provenance, diagnostics = _project_output(
            result.output, frozen["turn"]
        )
        _write_immutable(paths["normalized"], normalized)
        _write_immutable(paths["provenance"], provenance)
        _write_immutable(paths["diagnostics"], diagnostics)
        gate = _build_gate(usage=usage, diagnostics=diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise V233ArchitectureStop("v233 structural or count gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v233_architecture_structural_gate_passed",
            "terminal_reason": "v233_blind_unit_sweep_structural_cost_gate_passed",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
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
            "architecture_ranking": _record(frozen["ranking_path"]),
            "sidecar": _record(paths["sidecar"]),
            "exact_next_action": (
                "run the frozen side-free support and neutral alignment canary over "
                "the v233 standalone events without changing the extractor"
            ),
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v233 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v233 blind unit sweep canary")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v233(output_dir=Path(args.output_dir))
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "turn_count": frozen["spec"]["declared_turn_count"],
            "prompt_bytes": frozen["spec"]["prompt_bytes"],
            "schema_bytes": frozen["spec"]["schema_bytes"],
            "projected_ratio": _load_json(
                frozen["design_path"], "v233 design"
            )["production_cost_projection"][
                "projected_production_amortized_total_token_ratio"
            ],
        }
    else:
        terminal = asyncio.run(
            run_v233(
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
