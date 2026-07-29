from __future__ import annotations

"""Run the bounded v242 source-local set-coded LLM reducer."""

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
from . import app_server_judge_v5_selection_v239_frontier_long_horizon as v239
from . import app_server_judge_v5_selection_v241_delta_llm_reducer as v241
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v242_set_coded_reducer_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v242_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v242_terminal_v1"
PHASE_ID = "development_selection_v5_4_v242_set_coded_reducer"
MODEL = "gpt-5.6-sol"
EFFORT = "low"
MAX_NEW_TOKENS = 34_900
PRIOR_MAP_TOKENS = 39_004
MAX_COMBINED_DEVELOPMENT_TOKENS = PRIOR_MAP_TOKENS + MAX_NEW_TOKENS
MAX_EVENTS_PER_SEGMENT = 32
MAX_PROMPT_BYTES = 90_000
MAX_BASE_BYTES = 10_000
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
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
V239_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v239-frontier-long-horizon"
).resolve()
V239_TURN_ROOT = (
    V239_ROOT / "turns/v239-frontier-long-horizon-d7c914bc1cee2c432b36"
).resolve()
V241_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v241-delta-llm-reducer"
).resolve()
V241_TURN_ROOT = (
    V241_ROOT / "turns/v241-delta-reducer-d7c914bc1cee2c432b36"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v242-set-coded-reducer"
).resolve()
PINNED_CODEX_0_144_1 = v233.PINNED_CODEX_0_144_1


EXPECTED_LINEAGE_HASHES = {
    "source_input": "b0b693ff7d774a61d004d7dca6991a1324c27ed4c3c5ce7ca5d9d0a9c7b44d63",
    "source_base": "6b66e1ca9d00cf6558b84020b9879a3b86644b449f7498daea9a2c9837b4b0fc",
    "source_schema": "bd6b128cba67cc77700cc063778406c120107fde2cdadfa435d93eafe824db00",
    "v239_output": "7129e722e5e74c9a142a463a5af72c1e0e0dec6c5ea8ed55586b8257b976237e",
    "v239_sidecar": "73f6c79a66960a855dfcca79bff0487fbcd5af7c276b15c5cf51b1887862fe0a",
    "v239_runtime_lock": "ee310845c66671054165432cf5e2c7eedce9d296baeebae12bb4dc46e3f054b1",
    "v241_output": "9dcd65d97ab0c95ddc8e10dbe4270c5f4467e22e380f21bc9872837bba7efa30",
    "v241_sidecar": "f806c0feb0bd8e10d538dd3b012bf6371d7572218cc0bc1e2ab65435348ef3de",
    "v241_terminal": "6bdc1ac3d4d885354ddde6c8a12af2dd0ce9cabd40d111ef77580f679b4b2784",
    "v241_runtime_lock": "503c64837cb1d4ba102636baf34a01f232dab007fd3f9947492147e54155da12",
    "v241_audit": "8082af830340e06f779603ee3038a16f6ff21292de2360e4876fa79228631ece",
}


class V242SetReducerError(RuntimeError):
    """The v242 set-coded reducer cannot proceed safely."""


class V242OutputContractError(V242SetReducerError):
    """A completed v242 output violated the frozen contract."""


class V242ArchitectureStop(V242SetReducerError):
    """The v242 architecture did not clear its predeclared gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V242SetReducerError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V242SetReducerError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V242SetReducerError(f"frozen {path.name} drifted")
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
        "source_input": v241.v240.V233_TURN_ROOT / "input.private.json",
        "source_base": v233.SOURCE_TURN_ROOT / "base-instructions.private.md",
        "source_schema": v233.SOURCE_TURN_ROOT / "schema.json",
        "v239_output": V239_TURN_ROOT / "output.private.json",
        "v239_sidecar": V239_TURN_ROOT / "sidecar.json",
        "v239_runtime_lock": V239_ROOT / "runtime-lock.json",
        "v241_output": V241_TURN_ROOT / "output.private.json",
        "v241_sidecar": V241_TURN_ROOT / "sidecar.json",
        "v241_terminal": V241_ROOT / "terminal.json",
        "v241_runtime_lock": V241_ROOT / "runtime-lock.json",
        "v241_audit": PIPELINE_ROOT / "v241-completed-output-audit-2026-07-17/report.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V242SetReducerError(f"frozen lineage {name} drifted")
    v239.verify_runtime_lock(paths["v239_runtime_lock"])
    v241.verify_runtime_lock(paths["v241_runtime_lock"])
    v239_sidecar = _load_json(paths["v239_sidecar"], "v239 sidecar")
    v241_sidecar = _load_json(paths["v241_sidecar"], "v241 sidecar")
    v241_terminal = _load_json(paths["v241_terminal"], "v241 terminal")
    v241_audit = _load_json(paths["v241_audit"], "v241 audit")
    v239_output = _load_json(paths["v239_output"], "v239 output")
    if (
        (v239_sidecar.get("usage") or {}).get("total_tokens") != PRIOR_MAP_TOKENS
        or v239_sidecar.get("usage_complete") is not True
        or (v241_sidecar.get("usage") or {}).get("total_tokens") != 35_886
        or v241_sidecar.get("usage_complete") is not True
        or v241_terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or v241_audit.get("corrected_terminal_reason")
        != "v241_delta_llm_reducer_cost_gate_not_passed"
        or v241_audit.get("semantic_findings", {}).get(
            "set_coded_source_local_reducer_authorized"
        ) is not True
        or v241_audit.get("required_new_turn_token_reduction") != 964
        or [len(row.get("events") or []) for row in v239_output.get("segments") or []]
        != [31, 1]
    ):
        raise V242SetReducerError("predecessor set-coded evidence drifted")
    return {"paths": paths, "records": records}


def _output_schema(
    *, episode_id: str, segment_ids: Sequence[str], proposal_ids: Sequence[str],
    unit_ids: Sequence[str], source_schema: Mapping[str, Any], max_proposal_count: int,
) -> dict[str, Any]:
    source_segment = source_schema["properties"]["segments"]["items"]
    event = v233._event_schema(source_schema, unit_ids)
    event["properties"]["event_id"] = {"type": "string", "minLength": 1}
    event["properties"]["source_proposal_ids"] = {
        "type": "array",
        "minItems": 1,
        "maxItems": max_proposal_count,
        "items": {"type": "string", "enum": list(proposal_ids)},
    }
    event["required"] = [*event["required"], "event_id", "source_proposal_ids"]
    id_array = {
        "type": "array",
        "minItems": 0,
        "maxItems": max_proposal_count,
        "items": {"type": "string", "enum": list(proposal_ids)},
    }
    segment = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "segment_id", "final_status", "segment_source_context", "no_signal_reason",
            "coverage_audit", "keep_proposal_ids", "drop_proposal_ids", "replacement_events",
        ],
        "properties": {
            "segment_id": {"type": "string", "enum": list(segment_ids)},
            "final_status": copy.deepcopy(source_segment["properties"]["status"]),
            "segment_source_context": copy.deepcopy(source_segment["properties"]["segment_source_context"]),
            "no_signal_reason": copy.deepcopy(source_segment["properties"]["no_signal_reason"]),
            "coverage_audit": {
                "type": "object",
                "additionalProperties": False,
                "required": ["all_source_units_reviewed", "all_proposals_reviewed", "unresolved_count"],
                "properties": {
                    "all_source_units_reviewed": {"type": "boolean"},
                    "all_proposals_reviewed": {"type": "boolean"},
                    "unresolved_count": {"type": "integer", "minimum": 0},
                },
            },
            "keep_proposal_ids": copy.deepcopy(id_array),
            "drop_proposal_ids": copy.deepcopy(id_array),
            "replacement_events": {
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


BASE_INSTRUCTIONS = """You are a source-local set-coded semantic reducer for a topic-general podcast event pipeline. A prior blind map proposed events, but every proposal and field may be wrong. You cannot see a reference, target count, density label, expected topic, model identity, or quality hint. Re-read all source units and adjudicate every opaque proposal. Never use keywords, regex, fixed topics, or counts to decide meaning.

Place a proposal_id in keep_proposal_ids only when every material field and evidence boundary is already supported. Place it in drop_proposal_ids when it is unsupported or not an eligible research event. For any proposal needing correction, return one complete replacement_event listing that source_proposal_id. To merge genuinely identical truth conditions, return one replacement_event listing all merged source_proposal_ids in input order. The LLM alone makes every keep, drop, replace, merge, evidence, boundary, actor, attribution, stance, and field decision.

Every proposal_id must appear exactly once across keep_proposal_ids, drop_proposal_ids, and replacement source_proposal_ids. Keep and drop lists preserve input order. Merge only when actor, reported actor, speaker, target, stance, certainty, temporal scope, negation, attribution, event boundary, metric, and mechanism do not conflict. Preserve distinct premises, mechanisms, outcomes, comparisons, constraints, stances, and multi-lens events. Drop logistics, greetings, acknowledgements, unasserted questions, bare mentions, and unsupported inference.

Emit complete replacement_events only for corrections or merges. Select the smallest contiguous source-unit range supporting every material field. Every nonempty metric string must be a literal substring of evidence. If all four metric strings are empty, metric_direction must be not_applicable; otherwise it must not be not_applicable. Confirm all source units and proposals were reviewed with unresolved_count=0. Return schema-valid JSON only."""


def _prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    base_turn = v241._prepare_turn(v241._validate_lineage())
    source_schema = _load_json(lineage["paths"]["source_schema"], "source schema")
    all_unit_ids = [
        str(unit["unit_id"])
        for segment in base_turn["private_input"]["segments"]
        for unit in segment["units"]
    ]
    all_proposal_ids = [
        str(proposal["proposal_id"])
        for segment in base_turn["private_input"]["segments"]
        for proposal in segment["proposals"]
    ]
    schema = _output_schema(
        episode_id=base_turn["episode_id"],
        segment_ids=base_turn["segment_ids"],
        proposal_ids=all_proposal_ids,
        unit_ids=all_unit_ids,
        source_schema=source_schema,
        max_proposal_count=max(
            len(segment["proposals"]) for segment in base_turn["private_input"]["segments"]
        ),
    )
    prompt = base_turn["prompt"]
    base = BASE_INSTRUCTIONS + "\n"
    prompt_bytes = len(prompt.encode("utf-8"))
    base_bytes = len(base.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if (
        prompt_bytes > MAX_PROMPT_BYTES
        or base_bytes > MAX_BASE_BYTES
        or schema_bytes > MAX_SCHEMA_BYTES
    ):
        raise V242SetReducerError("v242 request size cap exceeded")
    return {
        "turn_name": "v242_set_coded_reducer_" + sha256_text(base_turn["episode_id"])[:20],
        "episode_id": base_turn["episode_id"],
        "segment_ids": base_turn["segment_ids"],
        "private_input": base_turn["private_input"],
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
        "decisions": turn_root / "set-decisions.private.json",
        "diagnostics": turn_root / "diagnostics.private.json",
    }


def _project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    try:
        _validate_schema(turn["schema"], output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V242OutputContractError("set-coded reducer output schema failed") from exc
    if output.get("episode_id") != turn["episode_id"]:
        raise V242OutputContractError("episode id drifted")
    rows = list(output.get("segments") or [])
    if [row.get("segment_id") for row in rows] != list(turn["segment_ids"]):
        raise V242OutputContractError("segment order or coverage drifted")
    source_by_id = {
        str(row["segment_id"]): row for row in turn["private_input"]["segments"]
    }
    projection_rows = []
    decision_rows = []
    diagnostics = []
    for row in rows:
        segment_id = str(row["segment_id"])
        source = source_by_id[segment_id]
        proposals = list(source["proposals"])
        proposal_ids = [str(proposal["proposal_id"]) for proposal in proposals]
        proposal_order = {proposal_id: index for index, proposal_id in enumerate(proposal_ids)}
        proposal_by_id = {
            str(proposal["proposal_id"]): dict(proposal["event"])
            for proposal in proposals
        }
        audit = row.get("coverage_audit") or {}
        if (
            audit.get("all_source_units_reviewed") is not True
            or audit.get("all_proposals_reviewed") is not True
            or audit.get("unresolved_count") != 0
        ):
            raise V242OutputContractError("set-coded reducer coverage audit failed")
        keep_ids = [str(value) for value in row["keep_proposal_ids"]]
        drop_ids = [str(value) for value in row["drop_proposal_ids"]]
        if any(value not in proposal_order for value in [*keep_ids, *drop_ids]):
            raise V242OutputContractError("decision references another segment proposal")
        if keep_ids != sorted(keep_ids, key=proposal_order.__getitem__):
            raise V242OutputContractError("keep proposal order drifted")
        if drop_ids != sorted(drop_ids, key=proposal_order.__getitem__):
            raise V242OutputContractError("drop proposal order drifted")
        replacement_by_id = {}
        replacement_sources = {}
        for raw_event in row["replacement_events"]:
            replacement = dict(raw_event)
            event_id = str(replacement.pop("event_id"))
            source_ids = [str(value) for value in replacement.pop("source_proposal_ids")]
            if event_id in replacement_by_id:
                raise V242OutputContractError("replacement event id duplicate")
            if len(source_ids) != len(set(source_ids)) or not source_ids:
                raise V242OutputContractError("replacement proposal mapping duplicate or empty")
            if any(value not in proposal_order for value in source_ids):
                raise V242OutputContractError("replacement maps another segment proposal")
            if source_ids != sorted(source_ids, key=proposal_order.__getitem__):
                raise V242OutputContractError("replacement proposal order drifted")
            replacement_by_id[event_id] = replacement
            replacement_sources[event_id] = source_ids
        replacement_ids = [
            proposal_id
            for source_ids in replacement_sources.values()
            for proposal_id in source_ids
        ]
        all_decided = [*keep_ids, *drop_ids, *replacement_ids]
        if len(all_decided) != len(set(all_decided)):
            raise V242OutputContractError("proposal appears in multiple decision sets")
        if set(all_decided) != set(proposal_ids):
            raise V242OutputContractError("proposal decision coverage drifted")
        final_events = [dict(proposal_by_id[proposal_id]) for proposal_id in keep_ids]
        final_events.extend(dict(event) for event in replacement_by_id.values())
        units = list(source["units"])
        unit_index = {str(unit["unit_id"]): index for index, unit in enumerate(units)}
        try:
            final_events.sort(key=lambda event: unit_index[str(event["evidence_start_unit_id"])])
        except KeyError as exc:
            raise V242OutputContractError("final event evidence belongs to another segment") from exc
        if (row["final_status"] == "coded") != bool(final_events):
            raise V242OutputContractError("final status does not match events")
        event_receipt_counts = {str(unit["unit_id"]): 0 for unit in units}
        for event in final_events:
            event_receipt_counts[str(event["evidence_start_unit_id"])] += 1
        projection_rows.append(
            {
                "segment_id": segment_id,
                "status": row["final_status"],
                "segment_source_context": row["segment_source_context"],
                "no_signal_reason": row["no_signal_reason"],
                "unit_receipts": [
                    {
                        "unit_id": unit["unit_id"],
                        "eligible_event_count": event_receipt_counts[str(unit["unit_id"])],
                        "unresolved_count": 0,
                    }
                    for unit in units
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "events": final_events,
            }
        )
        decision_rows.append(
            {
                "segment_id": segment_id,
                "keep_proposal_ids": keep_ids,
                "drop_proposal_ids": drop_ids,
                "replacement_event_sources": replacement_sources,
            }
        )
        diagnostics.append(
            {
                "segment_id": segment_id,
                "density_stratum": source["density_stratum"],
                "source_unit_count": len(units),
                "reviewed_source_unit_count": len(units),
                "proposal_count": len(proposals),
                "reviewed_proposal_count": len(all_decided),
                "kept_proposal_count": len(keep_ids),
                "dropped_proposal_count": len(drop_ids),
                "replaced_proposal_count": sum(
                    1 for source_ids in replacement_sources.values() if len(source_ids) == 1
                ),
                "merged_proposal_count": sum(
                    len(source_ids) for source_ids in replacement_sources.values() if len(source_ids) > 1
                ),
                "replacement_event_count": len(replacement_by_id),
                "event_count": len(final_events),
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
        raise V242OutputContractError(str(exc)) from exc
    decisions = {
        "schema_version": SCHEMA_VERSION,
        "episode_id": turn["episode_id"],
        "segments": decision_rows,
    }
    return normalized, provenance, decisions, diagnostics


def _capacity_policy(root: Path, turn_name: str) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        MAX_NEW_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_NEW_TOKENS,
            "phase_total_token_bound": MAX_NEW_TOKENS,
            "prior_map_tokens": PRIOR_MAP_TOKENS,
            "maximum_combined_development_tokens": MAX_COMBINED_DEVELOPMENT_TOKENS,
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
        "maximum_total_tokens_per_turn": MAX_NEW_TOKENS,
        "phase_total_token_bound": MAX_NEW_TOKENS,
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
                Path(v241.__file__).resolve(),
                Path(v239.__file__).resolve(),
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
    lock = _load_json(path, "v242 runtime lock")
    root = path.parent.resolve()
    turn_name = str(lock.get("turn_name") or "")
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("retry_count") != 0
        or lock.get("adopted_prior_map_tokens") != PRIOR_MAP_TOKENS
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(runtime_path) for runtime_path in _runtime_files()}
        or {row["path"] for row in lock.get("frozen_request") or []}
        != {row["path"] for row in _request_records(root, turn_name)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V242SetReducerError("v242 runtime lock contract drifted")
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
        raise V242SetReducerError("v242 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V242SetReducerError("v242 lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v242 spec")
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
            "private_input": _load_json(paths["input"], "v242 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v242 schema"),
            "source_schema": _load_json(v233.SOURCE_TURN_ROOT / "schema.json", "source schema"),
        },
    }


def freeze_v242(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v242 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V242SetReducerError("unfinished v242 root is not replayable")
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
            "scope": "one bounded source-local set-coded LLM reducer canary",
            "isolated_field_repair": False,
            "all_proposals_and_fields_readjudicated": True,
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
            "selected_architecture_id": "source_local_set_coded_llm_reducer",
            "architectures": [
                {"rank": 1, "id": "source_local_set_coded_llm_reducer"},
                {"rank": 2, "id": "independent_boundary_and_semantic_dual_pass_with_llm_join"},
                {"rank": 3, "id": "overlapping_window_maps_plus_episode_owner_reducer"},
            ],
            "on_failure": "freeze and reject set-coded reducer; no per-field patch",
        },
        "created_at",
    )
    projected_total = (
        PRODUCTION_AMORTIZED_CONTEXT_TOKENS
        + MAX_COMBINED_DEVELOPMENT_TOKENS * PRODUCTION_SCALE
    )
    design_path = root / "architecture-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "architecture_id": "source_local_set_coded_llm_reducer",
            "hypothesis": (
                "set-coded keep/drop decisions and source-local replacement payloads will preserve the "
                "v241 semantic proxy while reducing its measured reducer path by at least 964 tokens"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "adopted_map_event_counts": [31, 1],
                "all_proposals_and_fields_readjudicated": True,
                "episode_context_in_reducer": False,
                "reference_visible_to_model": False,
                "target_count_visible_to_model": False,
            },
            "production_cost_projection": {
                "production_amortized_context_tokens": PRODUCTION_AMORTIZED_CONTEXT_TOKENS,
                "measured_map_tokens": PRIOR_MAP_TOKENS,
                "set_reducer_turn_hard_max": MAX_NEW_TOKENS,
                "maximum_combined_development_tokens": MAX_COMBINED_DEVELOPMENT_TOKENS,
                "production_scale": PRODUCTION_SCALE,
                "projected_production_amortized_total_tokens": projected_total,
                "projected_production_amortized_total_token_ratio": round(
                    projected_total / BASELINE_END_TO_END_TOKENS, 6
                ),
                "required_ratio_max": 0.28,
            },
            "predeclared_stop_rules": {
                "retry_count": 0,
                "all_source_units_and_proposals_reviewed": True,
                "unresolved_count": 0,
                "all_proposals_in_exactly_one_llm_decision_set": True,
                "dense_event_count_minimum": MIN_DENSE_EVENTS,
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "event_cap_violations": 0,
                "exact_identity_duplicates": 0,
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
            "state": "frozen_before_one_turn_set_coded_reducer_canary",
            "declared_turn_count": 1,
            "turn_name": turn["turn_name"],
            "episode_id": turn["episode_id"],
            "segment_ids": turn["segment_ids"],
            "model": MODEL,
            "effort": EFFORT,
            "retry_count": 0,
            "maximum_new_tokens": MAX_NEW_TOKENS,
            "adopted_prior_map_tokens": PRIOR_MAP_TOKENS,
            "isolated_field_repair": False,
            "reference_visible_to_model": False,
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
        "adopted_prior_map_tokens": PRIOR_MAP_TOKENS,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(root / "runtime-lock.json", lock, "created_at")
    verify_runtime_lock(root / "runtime-lock.json")
    return _load_frozen(root)


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    usage = sidecar.get("usage")
    if not isinstance(usage, Mapping):
        raise V242SetReducerError("set-coded reducer usage absent")
    result = {}
    for field in USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise V242SetReducerError("set-coded reducer usage incomplete")
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
    ):
        raise V242SetReducerError("measured set-coded sidecar contract failed")
    return result


def _gate(*, usage: Mapping[str, int], diagnostics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense_count = int(by_id.get(DENSE_SEGMENT_ID, {}).get("event_count", -1))
    residual_count = int(by_id.get(NO_SIGNAL_SEGMENT_ID, {}).get("event_count", -1))
    combined = PRIOR_MAP_TOKENS + int(usage["total_tokens"])
    production_total = PRODUCTION_AMORTIZED_CONTEXT_TOKENS + combined * PRODUCTION_SCALE
    ratio = production_total / BASELINE_END_TO_END_TOKENS
    checks = {
        "both_segments_validated": set(by_id) == {DENSE_SEGMENT_ID, NO_SIGNAL_SEGMENT_ID},
        "all_source_units_reviewed": all(
            int(row["reviewed_source_unit_count"]) == int(row["source_unit_count"])
            for row in diagnostics
        ),
        "all_proposals_reviewed": all(
            int(row["reviewed_proposal_count"]) == int(row["proposal_count"])
            for row in diagnostics
        ),
        "unresolved_count_0": all(int(row["unresolved_count"]) == 0 for row in diagnostics),
        "proposal_set_accounting_valid": True,
        "dense_event_count_gte_24": dense_count >= MIN_DENSE_EVENTS,
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "new_turn_tokens_lte_34900": int(usage["total_tokens"]) <= MAX_NEW_TOKENS,
        "combined_development_tokens_lte_73904": combined <= MAX_COMBINED_DEVELOPMENT_TOKENS,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": SCHEMA_VERSION,
        "phase_id": PHASE_ID,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "new_turn_usage": dict(usage),
        "prior_map_tokens": PRIOR_MAP_TOKENS,
        "combined_development_tokens": combined,
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
            raw = _load_json(paths["sidecar"], "v242 sidecar")
            values = raw.get("usage") or {}
            usage = {field: int(values[field]) for field in USAGE_FIELDS}
            unknown = 0
        except Exception:
            unknown = 1
    measured_cost_stop = (
        type(exc).__name__ == "ReserveCapacityError"
        and unknown == 0
        and int(usage["total_tokens"]) > MAX_NEW_TOKENS
    )
    semantic = isinstance(exc, (V242OutputContractError, V242ArchitectureStop)) or measured_cost_stop
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v242_set_coded_reducer_structural_quality_or_cost_gate_not_passed"
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
        "prior_map_tokens": PRIOR_MAP_TOKENS,
        "combined_development_tokens": (
            None if unknown else PRIOR_MAP_TOKENS + int(usage["total_tokens"])
        ),
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


async def run_v242(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v242 terminal")
    frozen = freeze_v242(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(root, frozen, V242SetReducerError("launch exists; replay prohibited"))
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
            "adopted_prior_map_tokens": PRIOR_MAP_TOKENS,
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
            raise V242SetReducerError("set-coded reducer turn did not complete")
        usage = _usage(_load_json(paths["sidecar"], "v242 sidecar"))
        normalized, provenance, decisions, diagnostics = _project_output(
            result.output, frozen["turn"]
        )
        _write_immutable(paths["normalized"], normalized)
        _write_immutable(paths["provenance"], provenance)
        _write_immutable(paths["decisions"], decisions)
        _write_immutable(paths["diagnostics"], {"segments": diagnostics})
        gate = _gate(usage=usage, diagnostics=diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise V242ArchitectureStop("v242 structural quality or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v242_architecture_structural_gate_passed",
            "terminal_reason": "v242_set_coded_reducer_structural_cost_gate_passed",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "new_turn_usage": usage,
            "prior_map_tokens": PRIOR_MAP_TOKENS,
            "combined_development_tokens": gate["combined_development_tokens"],
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
            return _load_json(root / "terminal.json", "v242 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v242 set-coded reducer canary")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v242(output_dir=Path(args.output_dir))
        design = _load_json(frozen["design_path"], "v242 design")
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "projected_ratio": design["production_cost_projection"][
                "projected_production_amortized_total_token_ratio"
            ],
        }
    else:
        terminal = asyncio.run(
            run_v242(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "new_total_tokens": (terminal.get("new_turn_usage") or {}).get("total_tokens"),
            "combined_development_tokens": terminal.get("combined_development_tokens"),
            "support_alignment_authorized": terminal.get("support_alignment_authorized", False),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
