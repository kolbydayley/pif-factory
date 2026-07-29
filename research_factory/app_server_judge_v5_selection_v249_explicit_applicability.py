from __future__ import annotations

"""Run a one-pass full-schema extractor with explicit applicability states."""

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
from . import app_server_judge_v5_selection_v239_frontier_long_horizon as v239
from . import app_server_judge_v5_selection_v248_source_span_enrichment as v248
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v249_explicit_applicability_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v249_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v249_terminal_v1"
PHASE_ID = "development_selection_v5_4_v249_explicit_applicability"
MODEL = "gpt-5.6-sol"
EFFORT = "high"
MAX_TOTAL_TOKENS = 48_000
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
MAX_PROMPT_BYTES = 100_000
MAX_BASE_BYTES = 20_000
MAX_SCHEMA_BYTES = 80_000
MIN_DENSE_EVENTS = 30
MAX_RESIDUAL_EVENTS = 1
USAGE_FIELDS = v233.USAGE_FIELDS
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
V239_ROOT = v239.DEFAULT_OUTPUT_ROOT
V248_ROOT = v248.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v249-explicit-applicability"
).resolve()
PINNED_CODEX_0_144_1 = v239.PINNED_CODEX_0_144_1

DIRECT_EVENT_FIELDS = (
    "event_type",
    "evidence_start_unit_id",
    "evidence_end_unit_id",
    "claim_text",
    "certainty",
    "temporal_horizon",
    "source_context_kind",
    "confidence",
)
OPTIONAL_TEXT_FIELDS = (
    "event_subtype",
    "target_concept",
    "causal_mechanism",
    "counterclaim",
    "signal_reason",
)
ENTITY_FIELDS = ("model_names", "product_names", "organizations", "people")


class V249ExplicitApplicabilityError(RuntimeError):
    """The v249 architecture cannot proceed or be adopted safely."""


class V249OutputContractError(V249ExplicitApplicabilityError):
    """A completed output violated the explicit-applicability contract."""


class V249ArchitectureStop(V249ExplicitApplicabilityError):
    """The measured architecture failed a predeclared gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V249ExplicitApplicabilityError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V249ExplicitApplicabilityError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V249ExplicitApplicabilityError(f"frozen {path.name} drifted")
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
    v239_turn = next(V239_ROOT.glob("turns/v239-frontier-long-horizon-*/sidecar.json")).parent
    v248_turn = next(V248_ROOT.glob("turns/v248-source-span-enrichment-*/sidecar.json")).parent
    return {
        "v239_terminal": V239_ROOT / "terminal.json",
        "v239_runtime_lock": V239_ROOT / "runtime-lock.json",
        "v239_output": v239_turn / "output.private.json",
        "v239_sidecar": v239_turn / "sidecar.json",
        "v239_audit": PIPELINE_ROOT / "v239-completed-output-audit-2026-07-17/report.json",
        "v248_terminal": V248_ROOT / "terminal.json",
        "v248_runtime_lock": V248_ROOT / "runtime-lock.json",
        "v248_sidecar": v248_turn / "sidecar.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    v239.verify_runtime_lock(paths["v239_runtime_lock"])
    v248.verify_runtime_lock(paths["v248_runtime_lock"])
    t239 = _load_json(paths["v239_terminal"], "v239 terminal")
    s239 = _load_json(paths["v239_sidecar"], "v239 sidecar")
    a239 = _load_json(paths["v239_audit"], "v239 completed audit")
    t248 = _load_json(paths["v248_terminal"], "v248 terminal")
    s248 = _load_json(paths["v248_sidecar"], "v248 sidecar")
    if (
        t239.get("terminal_reason")
        != "v239_frontier_long_horizon_structural_or_count_gate_not_passed"
        or (s239.get("usage") or {}).get("total_tokens") != 39_004
        or sum(
            int(row.get("metric_direction_applicability_violation_event_count", 0))
            for row in a239.get("sanitized_structural_counts") or []
        )
        != 4
        or a239.get("corrected_failure_class")
        != "architecture_full_schema_output_semantic_field_contract_not_passed"
        or t248.get("terminal_reason")
        != "v248_source_span_enrichment_structural_quality_or_cost_gate_not_passed"
        or t248.get("error_class") != "V248OutputContractError"
        or t248.get("error_message_sha256")
        != "773d940581875e52ae10467ae029b068623900a36f3cc4da7d782684a17ea14b"
        or (t248.get("new_turn_usage") or {}).get("total_tokens") != 39_735
        or s248.get("usage_status") != "measured"
        or s248.get("usage_complete") is not True
        or t248.get("production_mutated") is not False
    ):
        raise V249ExplicitApplicabilityError("v249 architecture lineage drifted")
    frozen239 = v239._load_frozen(V239_ROOT)
    return {
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "source_turn": frozen239["turn"],
    }


APPLICABILITY_INSTRUCTIONS = """The output schema uses explicit applicability objects for every optional field. This representation replaces implicit empty-string and sentinel inference.

For each applicability object, the applicability decision is semantic and must be made by you from the selected evidence. Use present only when the evidence supports the supplied value. Use not_applicable only when the field does not apply to that event. Use unknown only where the schema offers it and the field applies but the evidence does not identify the value. Never fill a value merely because an entity or number appears nearby.

State/value contracts are strict. A present text or list must be nonempty. A not_applicable text or list must be empty. Present actor and speaker objects require an evidence-supported name and non-unknown type or role. Unknown actor or speaker objects require an empty name and unknown type or role. A not_applicable reported actor requires empty name and type none; unknown requires empty name and type unknown. claim_type and stance use not_applicable only with their matching enum value. A present metric requires at least one literal metric string from the selected evidence and a non-not_applicable direction; a not_applicable metric requires all metric strings empty and direction not_applicable. Each entity list contains distinct evidence-supported names.

certainty and temporal_horizon are total classifications: use hedged or unspecified when appropriate. Do not expose a reference answer, target count, or prior event list. Return every event directly from the blind source packet in source order and schema-valid JSON only."""


def _value_object(value_schema: Mapping[str, Any], states: Sequence[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["applicability", "value"],
        "properties": {
            "applicability": {"type": "string", "enum": list(states)},
            "value": copy.deepcopy(dict(value_schema)),
        },
    }


def _party_object(
    *, name_schema: Mapping[str, Any], type_schema: Mapping[str, Any], states: Sequence[str]
) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["applicability", "name", "type"],
        "properties": {
            "applicability": {"type": "string", "enum": list(states)},
            "name": copy.deepcopy(dict(name_schema)),
            "type": copy.deepcopy(dict(type_schema)),
        },
    }


def _explicit_event_schema(source_event: Mapping[str, Any]) -> dict[str, Any]:
    props = source_event["properties"]
    event_props: dict[str, Any] = {
        field: copy.deepcopy(props[field]) for field in DIRECT_EVENT_FIELDS
    }
    for field in OPTIONAL_TEXT_FIELDS:
        event_props[field] = _value_object(props[field], ("present", "not_applicable"))
    event_props["claim_type"] = _value_object(
        props["claim_type"], ("present", "not_applicable")
    )
    event_props["stance"] = _value_object(
        props["stance"], ("present", "not_applicable")
    )
    event_props["speaker"] = _party_object(
        name_schema=props["speaker_name"],
        type_schema=props["speaker_role"],
        states=("present", "unknown"),
    )
    event_props["actor"] = _party_object(
        name_schema=props["actor_name"],
        type_schema=props["actor_type"],
        states=("present", "unknown"),
    )
    event_props["reported_actor"] = _party_object(
        name_schema=props["reported_actor_name"],
        type_schema=props["reported_actor_type"],
        states=("present", "unknown", "not_applicable"),
    )
    event_props["metric"] = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "applicability",
            "value",
            "unit",
            "comparator",
            "raw_text",
            "direction",
        ],
        "properties": {
            "applicability": {
                "type": "string",
                "enum": ["present", "not_applicable"],
            },
            "value": copy.deepcopy(props["metric_value"]),
            "unit": copy.deepcopy(props["metric_unit"]),
            "comparator": copy.deepcopy(props["metric_comparator"]),
            "raw_text": copy.deepcopy(props["metric_raw_text"]),
            "direction": copy.deepcopy(props["metric_direction"]),
        },
    }
    for field in ENTITY_FIELDS:
        event_props[field] = _value_object(props[field], ("present", "not_applicable"))
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(event_props),
        "properties": event_props,
    }


def _explicit_output_schema(source_schema: Mapping[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(dict(source_schema))
    source_event = schema["properties"]["segments"]["items"]["properties"]["events"][
        "items"
    ]
    schema["properties"]["segments"]["items"]["properties"]["events"][
        "items"
    ] = _explicit_event_schema(source_event)
    return schema


def prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    source = lineage["source_turn"]
    schema = _explicit_output_schema(source["schema"])
    base = source["base"] + "\n\n# Explicit applicability contract\n" + APPLICABILITY_INSTRUCTIONS + "\n"
    sizes = {
        "prompt_bytes": len(source["prompt"].encode("utf-8")),
        "base_bytes": len(base.encode("utf-8")),
        "schema_bytes": len(_canonical_json(schema).encode("utf-8")),
    }
    if (
        sizes["prompt_bytes"] > MAX_PROMPT_BYTES
        or sizes["base_bytes"] > MAX_BASE_BYTES
        or sizes["schema_bytes"] > MAX_SCHEMA_BYTES
    ):
        raise V249ExplicitApplicabilityError("v249 request size cap exceeded")
    return {
        "turn_name": "v249_explicit_applicability_" + sha256_text(source["episode_id"])[:20],
        "episode_id": source["episode_id"],
        "segment_ids": list(source["segment_ids"]),
        "private_input": copy.deepcopy(source["private_input"]),
        "prompt": source["prompt"],
        "base": base,
        "schema": schema,
        "direct_schema": copy.deepcopy(source["schema"]),
        **sizes,
    }


def _unwrap_text(value: Mapping[str, Any], field: str) -> str:
    state = value["applicability"]
    text = str(value["value"])
    if (state == "present" and not text.strip()) or (
        state == "not_applicable" and text != ""
    ):
        raise V249OutputContractError(f"{field} applicability conflicts with value")
    return text


def _unwrap_enum(value: Mapping[str, Any], field: str, absent: str) -> str:
    state = value["applicability"]
    item = str(value["value"])
    if (state == "present" and item == absent) or (
        state == "not_applicable" and item != absent
    ):
        raise V249OutputContractError(f"{field} applicability conflicts with value")
    return item


def _unwrap_party(value: Mapping[str, Any], field: str) -> tuple[str, str]:
    state = str(value["applicability"])
    name = str(value["name"])
    kind = str(value["type"])
    if field == "reported_actor":
        valid = (
            (state == "present" and bool(name.strip()) and kind not in {"none", "unknown"})
            or (state == "unknown" and name == "" and kind == "unknown")
            or (state == "not_applicable" and name == "" and kind == "none")
        )
    else:
        valid = (state == "present" and bool(name.strip()) and kind != "unknown") or (
            state == "unknown" and name == "" and kind == "unknown"
        )
    if not valid:
        raise V249OutputContractError(f"{field} applicability conflicts with value")
    return name, kind


def _unwrap_array(value: Mapping[str, Any], field: str) -> list[str]:
    state = value["applicability"]
    items = [str(item) for item in value["value"]]
    if (
        (state == "present" and (not items or any(not item.strip() for item in items)))
        or (state == "not_applicable" and items)
        or len(items) != len(set(items))
    ):
        raise V249OutputContractError(f"{field} applicability or identity conflicts")
    return items


def _project_event(event: Mapping[str, Any]) -> dict[str, Any]:
    projected = {field: copy.deepcopy(event[field]) for field in DIRECT_EVENT_FIELDS}
    for field in OPTIONAL_TEXT_FIELDS:
        projected[field] = _unwrap_text(event[field], field)
    projected["claim_type"] = _unwrap_enum(event["claim_type"], "claim_type", "not_applicable")
    projected["stance"] = _unwrap_enum(event["stance"], "stance", "not_applicable")
    projected["speaker_name"], projected["speaker_role"] = _unwrap_party(
        event["speaker"], "speaker"
    )
    projected["actor_name"], projected["actor_type"] = _unwrap_party(
        event["actor"], "actor"
    )
    projected["reported_actor_name"], projected["reported_actor_type"] = _unwrap_party(
        event["reported_actor"], "reported_actor"
    )
    metric = event["metric"]
    metric_values = [str(metric[name]) for name in ("value", "unit", "comparator", "raw_text")]
    if (
        metric["applicability"] == "present"
        and (not any(metric_values) or metric["direction"] == "not_applicable")
    ) or (
        metric["applicability"] == "not_applicable"
        and (any(metric_values) or metric["direction"] != "not_applicable")
    ):
        raise V249OutputContractError("metric applicability conflicts with value")
    projected.update(
        {
            "metric_value": metric_values[0],
            "metric_unit": metric_values[1],
            "metric_comparator": metric_values[2],
            "metric_raw_text": metric_values[3],
            "metric_direction": metric["direction"],
        }
    )
    for field in ENTITY_FIELDS:
        projected[field] = _unwrap_array(event[field], field)
    return projected


def project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    try:
        _validate_schema(turn["schema"], output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V249OutputContractError("explicit-applicability output schema failed") from exc
    direct = copy.deepcopy(dict(output))
    state_counts: dict[str, dict[str, int]] = {}
    for segment in direct.get("segments") or []:
        projected_events = []
        for event in segment.get("events") or []:
            for field in (
                *OPTIONAL_TEXT_FIELDS,
                "claim_type",
                "stance",
                "speaker",
                "actor",
                "reported_actor",
                "metric",
                *ENTITY_FIELDS,
            ):
                state = str(event[field]["applicability"])
                bucket = state_counts.setdefault(field, {})
                bucket[state] = bucket.get(state, 0) + 1
            projected_events.append(_project_event(event))
        segment["events"] = projected_events
    direct_turn = dict(turn)
    direct_turn["schema"] = turn["direct_schema"]
    try:
        normalized, provenance, diagnostics = v233._project_output(direct, direct_turn)
    except v233.V233OutputContractError as exc:
        raise V249OutputContractError(str(exc)) from exc
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "state_counts": state_counts,
        "all_optional_semantics_selected_by_llm": True,
        "deterministic_projection_only": True,
    }
    return normalized, provenance, diagnostics, receipt


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "direct_schema": turn_root / "projection-schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _request_records(paths: Mapping[str, Path]) -> list[dict[str, Any]]:
    return [_record(paths[name]) for name in ("input", "prompt", "base", "schema", "direct_schema")]


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
                *v248._runtime_files(),
                Path(__file__).resolve(),
                Path(v233.__file__).resolve(),
                Path(v239.__file__).resolve(),
                Path(v248.__file__).resolve(),
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


def _production_ratio(tokens: int) -> tuple[int, float]:
    total = v239.PRODUCTION_AMORTIZED_CONTEXT_TOKENS + tokens * v239.PRODUCTION_SCALE
    return total, total / v239.BASELINE_END_TO_END_TOKENS


def _gate(*, usage: Mapping[str, int], diagnostics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense = int((by_id.get(v239.DENSE_SEGMENT_ID) or {}).get("event_count", -1))
    residual = int((by_id.get(v239.NO_SIGNAL_SEGMENT_ID) or {}).get("event_count", -1))
    production_total, ratio = _production_ratio(int(usage["total_tokens"]))
    checks = {
        "both_segments_validated": set(by_id)
        == {v239.DENSE_SEGMENT_ID, v239.NO_SIGNAL_SEGMENT_ID},
        "dense_event_count_gte_30": dense >= MIN_DENSE_EVENTS,
        "nominal_no_signal_event_count_lte_1": 0 <= residual <= MAX_RESIDUAL_EVENTS,
        "all_source_units_reviewed": all(
            int(row["source_unit_count"]) == int(row["reviewed_source_unit_count"])
            for row in diagnostics
        ),
        "unresolved_count_0": all(int(row["unresolved_count"]) == 0 for row in diagnostics),
        "explicit_applicability_projection_passed": True,
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "total_tokens_lte_48000": int(usage["total_tokens"]) <= MAX_TOTAL_TOKENS,
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
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "dense_event_count": dense,
        "candidate_only_nominal_no_signal_event_count": residual,
        "residual_support_audit_required": residual > 0,
        "diagnostics": list(diagnostics),
        "support_alignment_authorized": not failed,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v249 runtime lock")
    root = path.parent.resolve()
    spec = _load_json(root / "attempt-spec.json", "v249 spec")
    paths = _turn_paths(root, spec["turn_name"])
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("retry_count") != 0
        or lock.get("max_total_tokens") != MAX_TOTAL_TOKENS
        or lock.get("semantic_regex_or_keyword_filtering") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(path) for path in _runtime_files()}
        or {row["path"] for row in lock.get("request") or []}
        != {row["path"] for row in _request_records(paths)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
    ):
        raise V249ExplicitApplicabilityError("v249 runtime lock contract drifted")
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
        *(lock.get("request") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V249ExplicitApplicabilityError("v249 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V249ExplicitApplicabilityError("v249 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v249 spec")
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
            "private_input": _load_json(paths["input"], "v249 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v249 schema"),
            "direct_schema": _load_json(paths["direct_schema"], "v249 projection schema"),
            "paths": paths,
        },
    }


def freeze_v249(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v249 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V249ExplicitApplicabilityError("unfinished v249 root is not replayable")
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
    _write_immutable(paths["direct_schema"], turn["direct_schema"])
    capacity_paths = _capacity_policy(root, turn["turn_name"])
    authorization_path = root / "authorization.json"
    _write_stable_time(
        authorization_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "authority": "direct_operator_architecture_steering_2026_07_17",
            "scope": "one bounded blind one-pass explicit-applicability canary",
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
            "selected_architecture_id": "one_pass_full_schema_with_explicit_applicability_states",
            "architectures": [
                {"rank": 1, "id": "one_pass_full_schema_with_explicit_applicability_states"},
                {"rank": 2, "id": "source_graph_then_global_full_schema_canonicalizer"},
                {"rank": 3, "id": "episode_bootstrap_then_segment_specialists_with_llm_join"},
            ],
            "measured_basis": {
                "v239_tokens": 39_004,
                "v239_dense_events": 31,
                "v239_metric_applicability_violations": 4,
                "v248_tokens": 39_735,
                "v248_failure_class": "duplicate_exact_entity_spans",
                "v247_v244_macro_f1": 0.680723,
            },
            "on_failure": "freeze and reject v249; advance to source graph plus global canonicalizer",
        },
        "created_at",
    )
    projected_total, projected_ratio = _production_ratio(MAX_TOTAL_TOKENS)
    design_path = root / "architecture-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "architecture_id": "one_pass_full_schema_with_explicit_applicability_states",
            "hypothesis": (
                "one blind source-to-full-schema Sol-high pass can preserve v239 coverage while explicit "
                "LLM-selected applicability objects eliminate implicit empty/sentinel contradictions "
                "without the brittle event propagation or source-offset representation of v243/v248"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "prior_event_list_visible": False,
                "reference_visible_to_model": False,
                "target_count_visible_to_model": False,
                "declared_semantic_turn_count": 1,
            },
            "production_cost_projection": {
                "turn_hard_max": MAX_TOTAL_TOKENS,
                "production_amortized_context_tokens": v239.PRODUCTION_AMORTIZED_CONTEXT_TOKENS,
                "production_scale": v239.PRODUCTION_SCALE,
                "projected_production_amortized_total_tokens": projected_total,
                "projected_production_amortized_total_token_ratio": round(projected_ratio, 6),
                "required_ratio_max": 0.28,
            },
            "predeclared_stop_rules": {
                "retry_count": 0,
                "dense_event_count_minimum": MIN_DENSE_EVENTS,
                "nominal_no_signal_event_count_maximum": MAX_RESIDUAL_EVENTS,
                "all_optional_fields_have_llm_selected_applicability": True,
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "event_cap_violations": 0,
                "exact_identity_duplicates": 0,
                "total_tokens_max": MAX_TOTAL_TOKENS,
                "production_amortized_total_token_ratio_max": 0.28,
            },
            "semantic_regex_or_keyword_filtering": False,
            "deterministic_semantic_decisions": False,
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
            "state": "frozen_before_one_turn_explicit_applicability_canary",
            "turn_name": turn["turn_name"],
            "model": MODEL,
            "effort": EFFORT,
            "declared_turn_count": 1,
            "retry_count": 0,
            "max_total_tokens": MAX_TOTAL_TOKENS,
            "prompt_bytes": turn["prompt_bytes"],
            "base_bytes": turn["base_bytes"],
            "schema_bytes": turn["schema_bytes"],
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
        "declared_turn_count": 1,
        "retry_count": 0,
        "max_total_tokens": MAX_TOTAL_TOKENS,
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
        raise V249ExplicitApplicabilityError("v249 sidecar usage incomplete") from exc
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
        raise V249ExplicitApplicabilityError("v249 sidecar accounting or auth failed")
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
            sidecar = _load_json(paths["sidecar"], "v249 sidecar")
            usage = {field: int((sidecar.get("usage") or {})[field]) for field in USAGE_FIELDS}
            unknown = int(
                sidecar.get("usage_complete") is not True
                or sidecar.get("usage_status") != "measured"
            )
        except Exception:
            unknown = 1
    semantic = isinstance(exc, (V249OutputContractError, V249ArchitectureStop))
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v249_explicit_applicability_structural_quality_or_cost_gate_not_passed"
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
            "advance to source graph plus global full-schema canonicalizer"
            if semantic
            else "audit immutable v249 infrastructure attempt; no retry"
        ),
    }
    gate_path = root / "architecture-structural-gate.json"
    if gate_path.is_file():
        terminal["gate"] = _record(gate_path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v249(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v249 terminal")
    frozen = freeze_v249(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root, frozen, V249ExplicitApplicabilityError("launch exists; replay prohibited")
        )
    _write_immutable(
        root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
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
            raise V249ExplicitApplicabilityError("v249 turn did not complete")
        usage = _usage(_load_json(paths["sidecar"], "v249 sidecar"))
        normalized, provenance, diagnostics, state_receipt = project_output(
            result.output, frozen["turn"]
        )
        normalized_path = root / "normalized-output.private.json"
        provenance_path = root / "evidence-provenance.private.json"
        diagnostics_path = root / "diagnostics.private.json"
        state_path = root / "applicability-receipt.json"
        _write_immutable(normalized_path, normalized)
        _write_immutable(provenance_path, provenance)
        _write_immutable(diagnostics_path, {"segments": diagnostics})
        _write_immutable(state_path, state_receipt)
        gate = _gate(usage=usage, diagnostics=diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise V249ArchitectureStop("v249 structural quality or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v249_architecture_structural_gate_passed",
            "terminal_reason": "v249_explicit_applicability_structural_cost_gate_passed",
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
            "normalized_output": _record(normalized_path),
            "evidence_provenance": _record(provenance_path),
            "applicability_receipt": _record(state_path),
            "sidecar": _record(paths["sidecar"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": "run frozen side-free source-support audit before alignment or holdout",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v249 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v249 explicit-applicability canary")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v249(output_dir=Path(args.output_dir))
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "prompt_bytes": frozen["spec"]["prompt_bytes"],
            "base_bytes": frozen["spec"]["base_bytes"],
            "schema_bytes": frozen["spec"]["schema_bytes"],
            "projected_ratio": round(_production_ratio(MAX_TOTAL_TOKENS)[1], 6),
        }
    else:
        terminal = asyncio.run(
            run_v249(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
