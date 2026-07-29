from __future__ import annotations

"""Run the bounded v238 single-turn specialist-ensemble canary."""

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
from . import app_server_judge_v5_selection_v237_opaque_coverage_gap as v237
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v238_specialist_ensemble_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v238_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v238_terminal_v1"
PHASE_ID = "development_selection_v5_4_v238_specialist_ensemble"
MODEL = "gpt-5.6-sol"
EFFORT = "low"
MAX_TOTAL_TOKENS = 45_000
MAX_EVENTS_PER_SEGMENT = 32
MAX_CANDIDATES_PER_SEGMENT = 64
MAX_PROMPT_BYTES = 90_000
MAX_BASE_BYTES = 20_000
MAX_SCHEMA_BYTES = 60_000
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
BASELINE_END_TO_END_TOKENS = 10_065_426
PRODUCTION_AMORTIZED_CONTEXT_TOKENS = 600_538
PRODUCTION_SCALE = 30
DENSE_SEGMENT_ID = v233.DENSE_SEGMENT_ID
NO_SIGNAL_SEGMENT_ID = v233.NO_SIGNAL_SEGMENT_ID
MIN_DENSE_EVENTS = v233.MIN_DENSE_EVENTS_FOR_FROZEN_SOURCE_FLOOR
USAGE_FIELDS = v233.USAGE_FIELDS
SPECIALISTS = ("relations", "attribution", "market_operations")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
SOURCE_TURN_ROOT = v233.SOURCE_TURN_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v238-specialist-ensemble"
).resolve()
PINNED_CODEX_0_144_1 = v233.PINNED_CODEX_0_144_1


EXPECTED_LINEAGE_HASHES = {
    "source_input": "71433895667434bedfa9f1b5fd57d7c3400d5f8a94e64f4e2c6efc78af8a8332",
    "source_base": "6b66e1ca9d00cf6558b84020b9879a3b86644b449f7498daea9a2c9837b4b0fc",
    "source_schema": "bd6b128cba67cc77700cc063778406c120107fde2cdadfa435d93eafe824db00",
    "v233_terminal": "2584bfa238032e584d948ad4d97ca63834ad09b4c1c4d73cedf936fff19a24bf",
    "v234_terminal": "35d610922d38b1564fc9d927ad7e8b49e1592ccaeea78d66a8ddb76b7522806c",
    "v235_audit": "92d178855d4709e81c1d1b027efef5240025e16656035f5bcf6e0068fd07966b",
    "v236_terminal": "5d6d0a77a0c55992a437735bbc311c1daa279febcd43b03a731ef55a8443b603",
    "v237_terminal": "2e443c3c979501396924b307508efd87f7f953f6e253adb644fab1fa99384072",
    "v237_gate": "5b187086a2c2a29713cf22935cc8cdb0affebc7dc5e327bcd89e22c2ed3ed529",
    "v237_sidecar": "6fe47a39cb9f1b0acb317e57279e54b0a9f0c16565d8baf7073554ea10240e0b",
}


class V238SpecialistEnsembleError(RuntimeError):
    """The v238 specialist ensemble cannot proceed safely."""


class V238OutputContractError(V238SpecialistEnsembleError):
    """A completed v238 output violated the frozen contract."""


class V238ArchitectureStop(V238SpecialistEnsembleError):
    """The v238 architecture did not clear its predeclared gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V238SpecialistEnsembleError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V238SpecialistEnsembleError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V238SpecialistEnsembleError(f"frozen {path.name} drifted")
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
        "v233_terminal": PIPELINE_ROOT / "development-selection-v5_4-v233-blind-unit-sweep/terminal.json",
        "v234_terminal": PIPELINE_ROOT / "development-selection-v5_4-v234-two-pass-blind-inventory-realization/terminal.json",
        "v235_audit": PIPELINE_ROOT / "v235-completed-output-audit-2026-07-17/report.json",
        "v236_terminal": PIPELINE_ROOT / "development-selection-v5_4-v236-core-then-metadata/terminal.json",
        "v237_terminal": PIPELINE_ROOT / "development-selection-v5_4-v237-opaque-coverage-gap/terminal.json",
        "v237_gate": PIPELINE_ROOT / "development-selection-v5_4-v237-opaque-coverage-gap/architecture-structural-gate.json",
        "v237_sidecar": PIPELINE_ROOT / "development-selection-v5_4-v237-opaque-coverage-gap/turns/v237-opaque-gap-d7c914bc1cee2c432b36/sidecar.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V238SpecialistEnsembleError(f"frozen lineage {name} drifted")
    expected_reasons = {
        "v233_terminal": "v233_blind_unit_sweep_structural_or_count_gate_not_passed",
        "v234_terminal": "v234_two_pass_architecture_structural_or_count_gate_not_passed",
        "v236_terminal": "v236_core_metadata_structural_or_count_gate_not_passed",
        "v237_terminal": "v237_opaque_gap_structural_or_count_gate_not_passed",
    }
    for name, reason in expected_reasons.items():
        terminal = _load_json(paths[name], name)
        if (
            terminal.get("terminal_reason") != reason
            or terminal.get("next_distinct_architecture_authorized") is not True
            or terminal.get("production_mutated") is not False
        ):
            raise V238SpecialistEnsembleError(f"{name} state drifted")
    audit = _load_json(paths["v235_audit"], "v235 audit")
    gate = _load_json(paths["v237_gate"], "v237 gate")
    sidecar = _load_json(paths["v237_sidecar"], "v237 sidecar")
    if (
        audit.get("corrected_failure_class")
        != "architecture_quality_and_predeclared_cost_allocation_not_passed"
        or audit.get("next_distinct_architecture_authorized") is not True
        or gate.get("failed_checks") != ["dense_final_event_count_gte_24"]
        or gate.get("dense_final_event_count") != 18
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
    ):
        raise V238SpecialistEnsembleError("measured predecessor evidence drifted")
    v233._validate_lineage()
    return {"paths": paths, "records": records}


def _candidate_schema(unit_ids: Sequence[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "candidate_id",
            "specialist",
            "evidence_start_unit_id",
            "evidence_end_unit_id",
            "proposition",
            "disposition",
            "final_event_id",
        ],
        "properties": {
            "candidate_id": {"type": "string", "minLength": 1},
            "specialist": {"type": "string", "enum": list(SPECIALISTS)},
            "evidence_start_unit_id": {"type": "string", "enum": list(unit_ids)},
            "evidence_end_unit_id": {"type": "string", "enum": list(unit_ids)},
            "proposition": {"type": "string"},
            "disposition": {
                "type": "string",
                "enum": ["emit", "merge", "reject_non_event"],
            },
            "final_event_id": {"type": "string"},
        },
    }


def _output_schema(
    *, episode_id: str, segment_ids: Sequence[str], source_schema: Mapping[str, Any],
    unit_ids: Sequence[str], max_unit_count: int,
) -> dict[str, Any]:
    source_segment = source_schema["properties"]["segments"]["items"]
    event = v233._event_schema(source_schema, unit_ids)
    event["properties"]["event_id"] = {"type": "string", "minLength": 1}
    event["properties"]["source_candidate_ids"] = {
        "type": "array",
        "minItems": 1,
        "maxItems": MAX_CANDIDATES_PER_SEGMENT,
        "items": {"type": "string", "minLength": 1},
    }
    event["required"] = [*event["required"], "event_id", "source_candidate_ids"]
    receipt = {
        "type": "object",
        "additionalProperties": False,
        "required": ["unit_id", "specialists_reviewed", "candidate_count", "unresolved_count"],
        "properties": {
            "unit_id": {"type": "string", "enum": list(unit_ids)},
            "specialists_reviewed": {
                "type": "array",
                "minItems": len(SPECIALISTS),
                "maxItems": len(SPECIALISTS),
                "items": {"type": "string", "enum": list(SPECIALISTS)},
            },
            "candidate_count": {"type": "integer", "minimum": 0, "maximum": MAX_CANDIDATES_PER_SEGMENT},
            "unresolved_count": {"type": "integer", "minimum": 0, "maximum": MAX_CANDIDATES_PER_SEGMENT},
        },
    }
    segment = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "segment_id", "status", "segment_source_context", "no_signal_reason",
            "unit_receipts", "candidates", "events",
        ],
        "properties": {
            "segment_id": {"type": "string", "enum": list(segment_ids)},
            "status": copy.deepcopy(source_segment["properties"]["status"]),
            "segment_source_context": copy.deepcopy(source_segment["properties"]["segment_source_context"]),
            "no_signal_reason": copy.deepcopy(source_segment["properties"]["no_signal_reason"]),
            "unit_receipts": {
                "type": "array", "minItems": 1, "maxItems": max_unit_count, "items": receipt,
            },
            "candidates": {
                "type": "array", "minItems": 0, "maxItems": MAX_CANDIDATES_PER_SEGMENT,
                "items": _candidate_schema(unit_ids),
            },
            "events": {
                "type": "array", "minItems": 0, "maxItems": MAX_EVENTS_PER_SEGMENT, "items": event,
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
                "type": "array", "minItems": len(segment_ids), "maxItems": len(segment_ids), "items": segment,
            },
        },
    }


BASE_INSTRUCTIONS = """You are a topic-general three-specialist extraction ensemble operating inside one structured turn. You cannot see any reference answer, prior event, density label, target count, expected topic, or quality hint. Read every source unit using three independent semantic lenses, then perform one global semantic consolidation. Never use keywords, regex, fixed topic rules, or counts to decide meaning.

The relations specialist inventories independently truth-conditional identities, relationships, mechanisms, capabilities, constraints, comparisons, outcomes, definitions, and alternatives. The attribution specialist inventories speaker and reported-actor claims, stance, certainty, temporal scope, negation, counterclaims, causal framing, and source attribution. The market_operations specialist inventories product and market signals, adoption, operational implications, risks, incentives, competition, and implementation claims. These lenses are topic-neutral and may overlap.

First produce compact candidates from all three lenses. Every candidate selects the smallest contiguous exact source-unit range supporting its proposition. Then consolidate candidates globally: emit a standalone event, merge semantically duplicate or complementary candidates into one event, or reject material that is not an eligible research event. The LLM alone makes every eligibility, merge, boundary, actor, attribution, stance, and field decision. Every non-rejected candidate must name exactly one final_event_id, and every final event must list all source_candidate_ids that support it.

Split distinct truth-conditional propositions. Preserve legitimately distinct multi-lens events. Do not emit greetings, logistics, acknowledgements, unasserted questions, or bare mentions. Return one receipt per source unit in exact order, with specialists_reviewed exactly [\"relations\",\"attribution\",\"market_operations\"], candidate_count equal to candidates beginning at that unit, and unresolved_count=0.

For final events, select the smallest contiguous evidence-unit range supporting every material field and populate the full schema. Every nonempty metric string must be a literal substring of selected evidence; metric_direction=not_applicable if and only if all metric strings are empty. Order candidates by source evidence then specialist order. Order final events by source evidence. Use coded if and only if at least one final event is returned; otherwise use no_signal. Enforce the 32-event cap and return schema-valid JSON only."""


def _prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    blind = v233._prepare_turn(v233._validate_lineage())
    source_schema = _load_json(lineage["paths"]["source_schema"], "source schema")
    context = v232._shared_context(lineage["paths"]["source_base"])
    all_unit_ids = [
        str(unit["unit_id"])
        for segment in blind["private_input"]["segments"]
        for unit in segment["units"]
    ]
    packet = {
        "episode_id": blind["episode_id"],
        "segments": [
            {
                "segment_id": segment["segment_id"],
                "source_units": [
                    {
                        "unit_id": unit["unit_id"],
                        "window_id": unit["window_id"],
                        "text": unit["text"],
                    }
                    for unit in segment["units"]
                ],
            }
            for segment in blind["private_input"]["segments"]
        ],
    }
    prompt = "# Blind specialist-ensemble source packet\n" + json.dumps(
        packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"
    base = BASE_INSTRUCTIONS + "\n\n# Immutable episode context\n" + json.dumps(
        context, ensure_ascii=True, separators=(",", ":")
    ) + "\n"
    schema = _output_schema(
        episode_id=blind["episode_id"],
        segment_ids=blind["segment_ids"],
        source_schema=source_schema,
        unit_ids=all_unit_ids,
        max_unit_count=max(len(row["units"]) for row in blind["private_input"]["segments"]),
    )
    prompt_bytes = len(prompt.encode("utf-8"))
    base_bytes = len(base.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if (
        prompt_bytes > MAX_PROMPT_BYTES
        or base_bytes > MAX_BASE_BYTES
        or schema_bytes > MAX_SCHEMA_BYTES
    ):
        raise V238SpecialistEnsembleError("v238 request size cap exceeded")
    return {
        "turn_name": "v238_specialist_ensemble_" + sha256_text(blind["episode_id"])[:20],
        "episode_id": blind["episode_id"],
        "segment_ids": blind["segment_ids"],
        "private_input": blind["private_input"],
        "prompt": prompt,
        "base": base,
        "schema": schema,
        "source_schema": source_schema,
        "prompt_bytes": prompt_bytes,
        "base_bytes": base_bytes,
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
        "ledger": turn_root / "specialist-ledger.private.json",
        "diagnostics": turn_root / "diagnostics.private.json",
    }


def _project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    try:
        _validate_schema(turn["schema"], output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V238OutputContractError("specialist output schema failed") from exc
    if output.get("episode_id") != turn["episode_id"]:
        raise V238OutputContractError("episode id drifted")
    rows = list(output.get("segments") or [])
    if [row.get("segment_id") for row in rows] != list(turn["segment_ids"]):
        raise V238OutputContractError("segment order or coverage drifted")
    source_by_id = {
        str(row["segment_id"]): row for row in turn["private_input"]["segments"]
    }
    projection_rows = []
    ledger_rows = []
    ensemble_diagnostics = []
    for row in rows:
        segment_id = str(row["segment_id"])
        source = source_by_id[segment_id]
        units = list(source["units"])
        unit_ids = [str(unit["unit_id"]) for unit in units]
        unit_index = {unit_id: index for index, unit_id in enumerate(unit_ids)}
        receipts = list(row["unit_receipts"])
        if [receipt.get("unit_id") for receipt in receipts] != unit_ids:
            raise V238OutputContractError("unit receipt order or coverage drifted")
        receipt_counts = {}
        for receipt in receipts:
            if (
                list(receipt.get("specialists_reviewed") or []) != list(SPECIALISTS)
                or receipt.get("unresolved_count") != 0
            ):
                raise V238OutputContractError("specialist receipt review drifted")
            receipt_counts[str(receipt["unit_id"])] = int(receipt["candidate_count"])
        candidates = list(row["candidates"])
        candidate_by_id = {}
        start_counts = {unit_id: 0 for unit_id in unit_ids}
        prior_candidate_order = (-1, -1)
        specialist_index = {name: index for index, name in enumerate(SPECIALISTS)}
        for candidate in candidates:
            candidate_id = str(candidate["candidate_id"])
            if candidate_id in candidate_by_id:
                raise V238OutputContractError("candidate id duplicate")
            start_id = str(candidate["evidence_start_unit_id"])
            end_id = str(candidate["evidence_end_unit_id"])
            if start_id not in unit_index or end_id not in unit_index:
                raise V238OutputContractError("candidate unit belongs to another segment")
            if unit_index[start_id] > unit_index[end_id]:
                raise V238OutputContractError("candidate evidence range reversed")
            order = (unit_index[start_id], specialist_index[str(candidate["specialist"])])
            if order < prior_candidate_order:
                raise V238OutputContractError("candidate source order drifted")
            prior_candidate_order = order
            start_counts[start_id] += 1
            candidate_by_id[candidate_id] = candidate
        if start_counts != receipt_counts:
            raise V238OutputContractError("candidate receipt ownership drifted")
        raw_events = list(row["events"])
        if (row["status"] == "coded") != bool(raw_events):
            raise V238OutputContractError("coded status does not match final events")
        event_by_id = {}
        active_candidate_ids = set()
        projection_events = []
        event_receipt_counts = {unit_id: 0 for unit_id in unit_ids}
        for raw_event in raw_events:
            event = dict(raw_event)
            event_id = str(event.pop("event_id"))
            source_candidate_ids = list(event.pop("source_candidate_ids"))
            if event_id in event_by_id:
                raise V238OutputContractError("final event id duplicate")
            if len(source_candidate_ids) != len(set(source_candidate_ids)):
                raise V238OutputContractError("event candidate mapping duplicate")
            for candidate_id in source_candidate_ids:
                candidate = candidate_by_id.get(str(candidate_id))
                if candidate is None or candidate["disposition"] == "reject_non_event":
                    raise V238OutputContractError("event maps an absent or rejected candidate")
                if candidate["final_event_id"] != event_id:
                    raise V238OutputContractError("candidate final event mapping drifted")
                if candidate_id in active_candidate_ids:
                    raise V238OutputContractError("candidate maps to multiple final events")
                active_candidate_ids.add(str(candidate_id))
            event_by_id[event_id] = raw_event
            event_receipt_counts[str(event["evidence_start_unit_id"])] += 1
            projection_events.append(event)
        for candidate_id, candidate in candidate_by_id.items():
            disposition = str(candidate["disposition"])
            final_event_id = str(candidate["final_event_id"])
            if disposition == "reject_non_event":
                if final_event_id:
                    raise V238OutputContractError("rejected candidate names a final event")
            elif not final_event_id or candidate_id not in active_candidate_ids:
                raise V238OutputContractError("active candidate is not accounted")
        projection_rows.append(
            {
                "segment_id": segment_id,
                "status": row["status"],
                "segment_source_context": row["segment_source_context"],
                "no_signal_reason": row["no_signal_reason"],
                "unit_receipts": [
                    {
                        "unit_id": unit_id,
                        "eligible_event_count": event_receipt_counts[unit_id],
                        "unresolved_count": 0,
                    }
                    for unit_id in unit_ids
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "events": projection_events,
            }
        )
        ledger_rows.append({"segment_id": segment_id, "candidates": candidates})
        ensemble_diagnostics.append(
            {
                "segment_id": segment_id,
                "density_stratum": source["density_stratum"],
                "candidate_count": len(candidates),
                "rejected_candidate_count": sum(
                    candidate["disposition"] == "reject_non_event" for candidate in candidates
                ),
                "merged_candidate_count": sum(
                    candidate["disposition"] == "merge" for candidate in candidates
                ),
                "event_count": len(raw_events),
                "reviewed_source_unit_count": len(receipts),
                "source_unit_count": len(units),
                "unresolved_count": 0,
            }
        )
    all_unit_ids = [
        str(unit["unit_id"])
        for segment in turn["private_input"]["segments"]
        for unit in segment["units"]
    ]
    projection_schema = v233._output_schema(
        episode_id=turn["episode_id"],
        segment_ids=turn["segment_ids"],
        source_schema=turn["source_schema"],
        unit_ids=all_unit_ids,
        max_unit_count=max(len(row["units"]) for row in turn["private_input"]["segments"]),
    )
    projection_turn = dict(turn)
    projection_turn["schema"] = projection_schema
    try:
        normalized, provenance, _ = v233._project_output(
            {"episode_id": turn["episode_id"], "segments": projection_rows},
            projection_turn,
        )
    except v233.V233OutputContractError as exc:
        raise V238OutputContractError(str(exc)) from exc
    ledger = {"schema_version": SCHEMA_VERSION, "episode_id": turn["episode_id"], "segments": ledger_rows}
    return normalized, provenance, ledger, ensemble_diagnostics


def _capacity_policy(root: Path, turn_name: str) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
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
            "production_amortized_context_tokens": PRODUCTION_AMORTIZED_CONTEXT_TOKENS,
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
                Path(__file__).resolve(),
                Path(v237.__file__).resolve(),
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


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v238 runtime lock")
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
        != {str(runtime_path) for runtime_path in _runtime_files()}
        or {row["path"] for row in lock.get("frozen_request") or []}
        != {row["path"] for row in _request_records(root, turn_name)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V238SpecialistEnsembleError("v238 runtime lock contract drifted")
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
        raise V238SpecialistEnsembleError("v238 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V238SpecialistEnsembleError("v238 lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v238 spec")
    paths = _turn_paths(root, spec["turn_name"])
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "design_path": root / "architecture-design.json",
        "ranking_path": root / "architecture-ranking.json",
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": {
            "turn_name": spec["turn_name"],
            "episode_id": spec["episode_id"],
            "segment_ids": spec["segment_ids"],
            "paths": paths,
            "private_input": _load_json(paths["input"], "v238 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v238 schema"),
            "source_schema": _load_json(SOURCE_TURN_ROOT / "schema.json", "source schema"),
        },
    }


def freeze_v238(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v238 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V238SpecialistEnsembleError("unfinished v238 root is not replayable")
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
            "authority": "direct_operator_steering_2026_07_17",
            "scope": "one bounded specialist-ensemble architecture canary",
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
            "selected_architecture_id": "single_turn_three_specialist_llm_consolidation",
            "architectures": [
                {
                    "rank": 1,
                    "id": "single_turn_three_specialist_llm_consolidation",
                    "reason": "tests whether v234 inventory recall and v233 realization fidelity can coexist without a second prefill",
                },
                {
                    "rank": 2,
                    "id": "frontier_long_horizon_full_schema_single_pass",
                    "reason": "one blind semantic pass with more reasoning but no specialist ledger",
                },
                {
                    "rank": 3,
                    "id": "episode_map_reduce_with_llm_global_consolidator",
                    "reason": "independent source maps followed by a separate semantic consolidation turn",
                },
            ],
            "on_failure": "freeze and advance to a distinct architecture; no per-field patch",
        },
        "created_at",
    )
    projected_total = (
        PRODUCTION_AMORTIZED_CONTEXT_TOKENS
        + MAX_TOTAL_TOKENS * PRODUCTION_SCALE
    )
    design_path = root / "architecture-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "architecture_id": "single_turn_three_specialist_llm_consolidation",
            "hypothesis": (
                "three independent topic-neutral candidate ledgers followed by LLM-owned global consolidation "
                "will preserve the 32-proposition inventory recall observed in v234 while producing at least "
                "24 exact full-schema dense events in one source prefill"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "density_mix": ["dense", "nominal_no_signal"],
                "reference_visible_to_model": False,
                "prior_events_visible_to_model": False,
                "target_count_visible_to_model": False,
            },
            "production_cost_projection": {
                "production_amortized_context_tokens": PRODUCTION_AMORTIZED_CONTEXT_TOKENS,
                "turn_hard_max": MAX_TOTAL_TOKENS,
                "production_scale": PRODUCTION_SCALE,
                "projected_production_amortized_total_tokens": projected_total,
                "projected_production_amortized_total_token_ratio": round(
                    projected_total / BASELINE_END_TO_END_TOKENS, 6
                ),
                "required_ratio_max": 0.28,
            },
            "predeclared_stop_rules": {
                "retry_count": 0,
                "all_source_units_reviewed_by_all_specialists": True,
                "unresolved_count": 0,
                "all_candidates_accounted_by_llm_disposition": True,
                "dense_event_count_minimum": MIN_DENSE_EVENTS,
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "event_cap_violations": 0,
                "exact_identity_duplicates": 0,
                "candidate_only_residuals": "frozen side-free support audit; no automatic false positive",
                "production_amortized_total_token_ratio_max": 0.28,
            },
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    request = _request_records(root, turn["turn_name"])
    spec_path = root / "attempt-spec.json"
    _write_stable_time(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "state": "frozen_before_one_turn_specialist_ensemble_canary",
            "declared_turn_count": 1,
            "turn_name": turn["turn_name"],
            "episode_id": turn["episode_id"],
            "segment_ids": turn["segment_ids"],
            "model": MODEL,
            "effort": EFFORT,
            "retry_count": 0,
            "maximum_total_tokens": MAX_TOTAL_TOKENS,
            "specialists": list(SPECIALISTS),
            "reference_visible_to_model": False,
            "prior_events_visible_to_model": False,
            "density_visible_to_model": False,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
            "authorization": _record(authorization_path),
            "ranking": _record(ranking_path),
            "design": _record(design_path),
            "direct_lineage": lineage["records"],
            "frozen_request": request,
            "capacity_audit": _record(capacity_paths["audit"]),
            "capacity_policy": _record(capacity_paths["policy"]),
        },
        "created_at",
    )
    lock = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(runtime_path) for runtime_path in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
        "authorization": _record(authorization_path),
        "ranking": _record(ranking_path),
        "design": _record(design_path),
        "spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "frozen_request": request,
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


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    usage = sidecar.get("usage")
    if not isinstance(usage, Mapping):
        raise V238SpecialistEnsembleError("turn usage absent")
    result = {}
    for field in USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise V238SpecialistEnsembleError("turn usage incomplete")
        result[field] = value
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
        or result["total_tokens"] > MAX_TOTAL_TOKENS
    ):
        raise V238SpecialistEnsembleError("measured sidecar contract failed")
    return result


def _gate(*, usage: Mapping[str, int], diagnostics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense_count = int(by_id.get(DENSE_SEGMENT_ID, {}).get("event_count", -1))
    residual_count = int(by_id.get(NO_SIGNAL_SEGMENT_ID, {}).get("event_count", -1))
    production_total = (
        PRODUCTION_AMORTIZED_CONTEXT_TOKENS
        + int(usage["total_tokens"]) * PRODUCTION_SCALE
    )
    ratio = production_total / BASELINE_END_TO_END_TOKENS
    checks = {
        "both_segments_validated": set(by_id) == {DENSE_SEGMENT_ID, NO_SIGNAL_SEGMENT_ID},
        "all_units_reviewed_by_all_specialists": all(
            int(row["reviewed_source_unit_count"]) == int(row["source_unit_count"])
            for row in diagnostics
        ),
        "unresolved_count_0": all(int(row["unresolved_count"]) == 0 for row in diagnostics),
        "candidate_accounting_valid": True,
        "dense_event_count_gte_24": dense_count >= MIN_DENSE_EVENTS,
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "total_tokens_lte_45000": int(usage["total_tokens"]) <= MAX_TOTAL_TOKENS,
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
            raw = _load_json(paths["sidecar"], "v238 sidecar")
            values = raw.get("usage") or {}
            usage = {field: int(values[field]) for field in USAGE_FIELDS}
            unknown = 0
        except Exception:
            unknown = 1
    semantic = isinstance(exc, (V238OutputContractError, V238ArchitectureStop))
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v238_specialist_ensemble_structural_or_count_gate_not_passed"
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


async def run_v238(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v238 terminal")
    frozen = freeze_v238(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root, frozen, V238SpecialistEnsembleError("launch exists; replay prohibited")
        )
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
            "managed_chatgpt_auth_only": True,
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
            raise V238SpecialistEnsembleError("specialist turn did not complete")
        usage = _usage(_load_json(paths["sidecar"], "v238 sidecar"))
        normalized, provenance, ledger, diagnostics = _project_output(
            result.output, frozen["turn"]
        )
        _write_immutable(paths["normalized"], normalized)
        _write_immutable(paths["provenance"], provenance)
        _write_immutable(paths["ledger"], ledger)
        _write_immutable(paths["diagnostics"], {"segments": diagnostics})
        gate = _gate(usage=usage, diagnostics=diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise V238ArchitectureStop("v238 structural or count gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v238_architecture_structural_gate_passed",
            "terminal_reason": "v238_specialist_ensemble_structural_cost_gate_passed",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "production_amortized_total_token_ratio": gate[
                "production_amortized_total_token_ratio"
            ],
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
            return _load_json(root / "terminal.json", "v238 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v238 specialist-ensemble canary")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v238(output_dir=Path(args.output_dir))
        design = _load_json(frozen["design_path"], "v238 design")
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "projected_ratio": design["production_cost_projection"][
                "projected_production_amortized_total_token_ratio"
            ],
        }
    else:
        terminal = asyncio.run(
            run_v238(
                output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds
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
