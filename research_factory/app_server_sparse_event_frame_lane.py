from __future__ import annotations

"""Run one blind source-complete extraction into sparse semantic event frames."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import app_server_configured_experiment as configured
from . import app_server_flat_request_model_lane as flat
from . import codex_app_server
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .app_server_runtime_verifier import (
    ContentHashCache,
    RuntimeVerificationError,
    closure_receipt,
    normalize_record,
    verify_runtime_lock,
)
from .labels import ValidationError, _validate_schema
from .util import now_iso


CONFIG_VERSION = "pif_sparse_event_frame_lane_config_v1"
LOCK_VERSION = "pif_sparse_event_frame_lane_runtime_lock_v1"
TERMINAL_VERSION = "pif_sparse_event_frame_lane_terminal_v1"
ARCHITECTURE_ID = "source_complete_cardinality_typed_sparse_event_frames"
TURN_NAME = "source_complete_sparse_event_frames"
MODEL = "gpt-5.6-luna"
EFFORT = "high"
MAX_TOTAL_TOKENS = 70_000
MINIMUM_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
BASELINE_TOTAL_TOKENS = 10_065_426
PRODUCTION_CONTEXT_TOKENS = 600_538
PRODUCTION_SCALE = 30
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
SOURCE_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v239-frontier-long-horizon"
).resolve()
SOURCE_TURN_ROOT = (
    SOURCE_ROOT
    / "turns/v239-frontier-long-horizon-d7c914bc1cee2c432b36"
).resolve()
PRIOR_LANE_ROOT = (
    PIPELINE_ROOT
    / "development-canary-flat-total-field-gpt55-high-2026-07-18"
).resolve()
PHASE_ROOT = (
    PIPELINE_ROOT / "phase-boundary-v275-v277-2026-07-17"
).resolve()
JUDGE_ROOT = (
    PIPELINE_ROOT / "judge-calibration-v5_4-v174-exact-evidence-continuation"
).resolve()
EVALUATOR_ROOT = (
    PIPELINE_ROOT / "reusable-candidate-semantic-evaluator-v3"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-canary-sparse-event-frame-luna-high-2026-07-18"
).resolve()
PINNED_CODEX = configured.PINNED_CODEX
USAGE_FIELDS = configured.USAGE_FIELDS
DIRECT_FIELDS = (
    "event_type",
    "claim_text",
    "certainty",
    "temporal_horizon",
    "source_context_kind",
    "confidence",
    "evidence_start_unit_id",
    "evidence_end_unit_id",
)
OPTIONAL_TEXT_FIELDS = (
    "event_subtype",
    "target_concept",
    "causal_mechanism",
    "counterclaim",
    "signal_reason",
)
ENTITY_FIELDS = ("model_names", "product_names", "organizations", "people")
LINEAGE_KEYS = (
    "source_input",
    "source_prompt",
    "source_base",
    "projection_schema",
    "prior_lane_terminal",
    "prior_lane_runtime_lock",
    "current_evaluator_checkpoint",
    "judge_calibration_terminal",
    "evaluator_runtime_lock",
    "architecture_decision",
)
EXPECTED_SOURCE_HASHES = {
    "source_input": "b0b693ff7d774a61d004d7dca6991a1324c27ed4c3c5ce7ca5d9d0a9c7b44d63",
    "source_prompt": "0ae782f2653b13b1fa1136b1a7ed9639f988583a28e3c32c593c09a608e5754c",
    "source_base": "7acabf3adfa2c5097dfd8c6017e1d5260750575a633c09967be9bdbf8fdcdf75",
    "projection_schema": "1f84977a2a884ba746c56f99a42ab2620d14accd9a97ccb72d17a07d096f85df",
}

SPARSE_FRAME_INSTRUCTIONS = """# Source-complete sparse event-frame contract

Perform a fresh blind extraction from the supplied source packet. Do not copy or infer a prior candidate, reference answer, expected event count, topic list, or target score.

Privately complete three source-grounded stages before returning JSON. First, inspect every source unit in chronological order and identify each independently truth-valued eligible event with its smallest sufficient contiguous evidence range. Second, reconstruct every event as a complete semantic frame, distinguishing actor, speaker, reported actor, attribution, target, claim, stance, certainty, temporal horizon, causal mechanism, event type, event boundary, and metric applicability. Third, globally reconcile the full episode event set against the source: retain events found by either pass, merge only exact semantic duplicates, split combined claims when their truth conditions differ, and re-read every selected evidence range before finalizing fields.

The output represents every optional semantic role as a zero-or-one array. Inclusion is your semantic decision. Use an empty array only when the role is absent or not applicable; use one item only when the selected evidence supports it. A party item is either a named party or an explicitly unknown applicable party. A metric array is empty when no metric applies; when present, metric raw_text and every nonempty metric string must be literal contiguous substrings of the selected evidence, and direction must describe that metric. Entity arrays contain only distinct evidence-supported names.

Review every supplied unit ID and return its receipt in source order. Event ownership is by evidence_start_unit_id. Return complete schema-valid JSON only. Never use keywords, regex rules, phrase lists, embeddings, semantic similarity, confidence voting, or deterministic semantic heuristics."""


class SparseEventFrameLaneError(RuntimeError):
    pass


class SparseEventFrameOutputError(SparseEventFrameLaneError):
    pass


class SparseEventFrameArchitectureStop(SparseEventFrameLaneError):
    pass


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SparseEventFrameLaneError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    configured._write_immutable(path, value)  # noqa: SLF001


def _write_private_text(path: Path, value: str) -> None:
    configured._write_private_text(path, value)  # noqa: SLF001


def _record(path: Path, *, cache: ContentHashCache | None = None) -> dict[str, Any]:
    return (cache or ContentHashCache()).record(path)


def _verify_record(record: Mapping[str, Any], *, cache: ContentHashCache) -> None:
    if not cache.verify_record(record):
        raise SparseEventFrameLaneError("frozen direct-lineage artifact drifted")


def _turn_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns" / TURN_NAME.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "projection_schema": turn_root / "projection-schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _source_paths() -> dict[str, Path]:
    return {
        "source_input": SOURCE_TURN_ROOT / "input.private.json",
        "source_prompt": SOURCE_TURN_ROOT / "prompt.private.md",
        "source_base": SOURCE_TURN_ROOT / "base-instructions.private.md",
        "projection_schema": SOURCE_TURN_ROOT / "schema.json",
        "prior_lane_terminal": PRIOR_LANE_ROOT / "terminal.json",
        "prior_lane_runtime_lock": PRIOR_LANE_ROOT / "runtime-lock.json",
        "current_evaluator_checkpoint": PHASE_ROOT
        / "current-evaluator-identity-adoption-checkpoint.json",
        "judge_calibration_terminal": JUDGE_ROOT / "terminal.json",
        "evaluator_runtime_lock": EVALUATOR_ROOT / "runtime-lock.json",
    }


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(flat.__file__).resolve(),
                *configured._runtime_files(),  # noqa: SLF001
            },
            key=str,
        )
    )


def _string_schema(source: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(source))
    value["minLength"] = max(1, int(value.get("minLength", 0)))
    return value


def _zero_or_one(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "type": "array",
        "minItems": 0,
        "maxItems": 1,
        "items": copy.deepcopy(dict(item)),
    }


def _enum_without(source: Mapping[str, Any], absent: str) -> dict[str, Any]:
    value = copy.deepcopy(dict(source))
    if "enum" in value:
        value["enum"] = [item for item in value["enum"] if item != absent]
    return value


def _party_schema(
    name_schema: Mapping[str, Any],
    type_schema: Mapping[str, Any],
    *,
    absent_types: set[str],
) -> dict[str, Any]:
    type_value = copy.deepcopy(dict(type_schema))
    if "enum" in type_value:
        type_value["enum"] = [
            item for item in type_value["enum"] if item not in absent_types
        ]
        if "unknown" not in type_value["enum"]:
            type_value["enum"].append("unknown")
    return _zero_or_one(
        {
            "type": "object",
            "additionalProperties": False,
            "required": ["identity_kind", "name", "type"],
            "properties": {
                "identity_kind": {"type": "string", "enum": ["named", "unknown"]},
                "name": copy.deepcopy(dict(name_schema)),
                "type": type_value,
            },
        }
    )


def build_sparse_schema(projection_schema: Mapping[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(dict(projection_schema))
    source_event = schema["properties"]["segments"]["items"]["properties"]["events"][
        "items"
    ]
    props = source_event["properties"]
    sparse: dict[str, Any] = {
        field: copy.deepcopy(props[field]) for field in DIRECT_FIELDS
    }
    for field in OPTIONAL_TEXT_FIELDS:
        sparse[field] = _zero_or_one(_string_schema(props[field]))
    sparse["claim_type"] = _zero_or_one(
        _enum_without(props["claim_type"], "not_applicable")
    )
    sparse["stance"] = _zero_or_one(
        _enum_without(props["stance"], "not_applicable")
    )
    sparse["speaker"] = _party_schema(
        props["speaker_name"], props["speaker_role"], absent_types={"unknown"}
    )
    sparse["actor"] = _party_schema(
        props["actor_name"], props["actor_type"], absent_types={"unknown"}
    )
    sparse["reported_actor"] = _party_schema(
        props["reported_actor_name"],
        props["reported_actor_type"],
        absent_types={"none", "unknown"},
    )
    sparse["metric"] = _zero_or_one(
        {
            "type": "object",
            "additionalProperties": False,
            "required": ["value", "unit", "comparator", "direction", "raw_text"],
            "properties": {
                "value": copy.deepcopy(props["metric_value"]),
                "unit": copy.deepcopy(props["metric_unit"]),
                "comparator": copy.deepcopy(props["metric_comparator"]),
                "direction": _enum_without(props["metric_direction"], "not_applicable"),
                "raw_text": _string_schema(props["metric_raw_text"]),
            },
        }
    )
    for field in ENTITY_FIELDS:
        value = copy.deepcopy(props[field])
        value["uniqueItems"] = True
        if isinstance(value.get("items"), dict):
            value["items"] = _string_schema(value["items"])
        sparse[field] = value
    schema["properties"]["segments"]["items"]["properties"]["events"]["items"] = {
        "type": "object",
        "additionalProperties": False,
        "required": list(sparse),
        "properties": sparse,
    }
    return schema


def _one(value: Any, field: str) -> Any | None:
    if not isinstance(value, list) or len(value) > 1:
        raise SparseEventFrameOutputError(f"{field} cardinality drifted")
    return value[0] if value else None


def _party(value: Any, field: str, absent_type: str) -> tuple[str, str]:
    item = _one(value, field)
    if item is None:
        return "", absent_type
    kind = str(item["identity_kind"])
    name = str(item["name"])
    party_type = str(item["type"])
    if (kind == "named" and (not name.strip() or party_type == "unknown")) or (
        kind == "unknown" and (name != "" or party_type != "unknown")
    ):
        raise SparseEventFrameOutputError(f"{field} identity contract drifted")
    return name, party_type


def project_sparse_event(event: Mapping[str, Any]) -> dict[str, Any]:
    projected = {field: copy.deepcopy(event[field]) for field in DIRECT_FIELDS}
    for field in OPTIONAL_TEXT_FIELDS:
        item = _one(event[field], field)
        projected[field] = "" if item is None else str(item)
    claim_type = _one(event["claim_type"], "claim_type")
    stance = _one(event["stance"], "stance")
    projected["claim_type"] = "not_applicable" if claim_type is None else str(claim_type)
    projected["stance"] = "not_applicable" if stance is None else str(stance)
    projected["speaker_name"], projected["speaker_role"] = _party(
        event["speaker"], "speaker", "unknown"
    )
    projected["actor_name"], projected["actor_type"] = _party(
        event["actor"], "actor", "unknown"
    )
    projected["reported_actor_name"], projected["reported_actor_type"] = _party(
        event["reported_actor"], "reported_actor", "none"
    )
    metric = _one(event["metric"], "metric")
    if metric is None:
        projected.update(
            {
                "metric_value": "",
                "metric_unit": "",
                "metric_comparator": "",
                "metric_direction": "not_applicable",
                "metric_raw_text": "",
            }
        )
    else:
        projected.update(
            {
                "metric_value": str(metric["value"]),
                "metric_unit": str(metric["unit"]),
                "metric_comparator": str(metric["comparator"]),
                "metric_direction": str(metric["direction"]),
                "metric_raw_text": str(metric["raw_text"]),
            }
        )
    for field in ENTITY_FIELDS:
        values = [str(item) for item in event[field]]
        if len(values) != len(set(values)):
            raise SparseEventFrameOutputError(f"{field} identity duplicate")
        projected[field] = values
    return projected


def validate_and_project_output(
    output: Mapping[str, Any], frozen: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    turn = frozen["turn"]
    try:
        _validate_schema(_load_json(turn["schema"], "sparse schema"), output, path="$")
    except (ValidationError, TypeError, ValueError) as exc:
        raise SparseEventFrameOutputError("sparse output schema failed") from exc
    direct = copy.deepcopy(dict(output))
    for segment in direct.get("segments") or []:
        segment["events"] = [project_sparse_event(event) for event in segment.get("events") or []]
    direct_frozen = {
        "turn": {"schema": turn["projection_schema"]},
        "source": frozen["source"],
    }
    try:
        normalized, provenance, diagnostics = flat.validate_and_project_output(
            direct, direct_frozen
        )
    except flat.FlatRequestOutputError as exc:
        raise SparseEventFrameOutputError(str(exc)) from exc
    diagnostics = copy.deepcopy(diagnostics)
    diagnostics["representation"] = "cardinality_typed_sparse_event_frames"
    diagnostics["all_semantic_decisions_owned_by_llm"] = True
    return normalized, provenance, diagnostics


def _architecture_decision(root: Path, records: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "pif_sparse_event_frame_architecture_decision_v1",
        "created_at": now_iso(),
        "state": "presemantic_architecture_decision_frozen",
        "production_mutated": False,
        "holdout_authorized": False,
        "semantic_model_call_count_for_decision": 0,
        "measured_predecessor_failure": {
            "architecture_id": flat.ARCHITECTURE_ID,
            "model": flat.MODEL,
            "usage_total_tokens": 43108,
            "failure_class": "metric_direction_applicability_drift",
            "disposition": "rejected_without_repair_or_retry",
        },
        "ranked_architectures": [
            {
                "rank": 1,
                "architecture_id": ARCHITECTURE_ID,
                "status": "selected_smallest_decision_changing_canary",
                "evidence": "A sparse cardinality-typed frame eliminates independent applicability/value contradictions across every optional role while Luna-high performs a fresh source-complete resynthesis. It is not a field patch or prior-candidate editor.",
            },
            {
                "rank": 2,
                "architecture_id": "llm_signal_router_then_frontier_dense_extraction",
                "status": "deferred_recall_bottleneck",
                "evidence": "An LLM-only segment router can reduce dense extraction cost, but a false-negative route prevents downstream recovery and has not shown a quality advantage on this development cohort.",
            },
            {
                "rank": 3,
                "architecture_id": "baseline_equivalent_per_segment_frontier_extraction",
                "status": "rejected_measured_cost_path",
                "evidence": "The baseline-quality family remains the fidelity control but exceeds the frozen production-amortized token target under paired whole-pipeline accounting.",
            },
        ],
        "selected_canary": {
            "architecture_id": ARCHITECTURE_ID,
            "model": MODEL,
            "effort": EFFORT,
            "declared_turn_count": 1,
            "retry_count": 0,
            "maximum_total_tokens": MAX_TOTAL_TOKENS,
            "hypothesis": "A source-complete Luna-high extraction using sparse zero-or-one semantic roles can preserve independent event boundaries and material fields while structurally preventing the cross-field applicability failures seen in both flat total-field frontier lanes.",
            "representative_development_sample": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "source_unit_count": 69,
                "reference_visible_to_model": False,
                "target_count_visible_to_model": False,
            },
            "production_cost_projection": {
                "maximum_production_amortized_total_token_ratio": round(
                    (PRODUCTION_CONTEXT_TOKENS + MAX_TOTAL_TOKENS * PRODUCTION_SCALE)
                    / BASELINE_TOTAL_TOKENS,
                    6,
                ),
                "baseline_total_tokens": BASELINE_TOTAL_TOKENS,
                "production_amortized_context_tokens": PRODUCTION_CONTEXT_TOKENS,
                "production_scale": PRODUCTION_SCALE,
            },
            "stop_rules": {
                "total_tokens_maximum": MAX_TOTAL_TOKENS,
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "exact_identity_duplicates": 0,
                "event_cap_violations": 0,
                "semantic_event_count_proxy_used": False,
                "production_amortized_total_token_ratio_maximum": 0.28,
                "on_structural_or_cost_failure": "freeze and reject the architecture without repair or retry",
                "on_pass": "run the unchanged frozen support-first and neutral two-permutation alignment evaluator",
            },
        },
        "direct_predecessor_records": copy.deepcopy(dict(records)),
    }


def prepare_experiment(root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    root = root.expanduser().resolve()
    paths = _source_paths()
    cache = ContentHashCache()
    records = {key: _record(path, cache=cache) for key, path in paths.items()}
    for key, expected_hash in EXPECTED_SOURCE_HASHES.items():
        if records[key]["sha256"] != expected_hash:
            raise SparseEventFrameLaneError(f"{key} source artifact drifted")
    prior = _load_json(paths["prior_lane_terminal"], "prior lane terminal")
    judge = _load_json(paths["judge_calibration_terminal"], "judge calibration terminal")
    if (
        prior.get("terminal_reason")
        != "flat_request_model_lane_structural_or_cost_gate_not_passed"
        or prior.get("usage_status") != "complete"
        or (prior.get("usage") or {}).get("total_tokens") != 43108
        or prior.get("semantic_retry_count") != 0
        or prior.get("production_mutated") is not False
        or judge.get("selection_authorized") is not True
    ):
        raise SparseEventFrameLaneError("direct predecessor state drifted")
    decision_path = root / "architecture-decision.json"
    configured._write_stable_time(  # noqa: SLF001
        decision_path,
        _architecture_decision(root, records),
        "created_at",
    )
    config = {
        "schema_version": CONFIG_VERSION,
        "experiment_id": "development_canary_sparse_event_frame_luna_high_2026_07_18",
        "architecture_id": ARCHITECTURE_ID,
        "output_root": str(root),
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_total_tokens": MAX_TOTAL_TOKENS,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "production_amortized_context_tokens": PRODUCTION_CONTEXT_TOKENS,
        "production_scale": PRODUCTION_SCALE,
        "baseline_total_tokens": BASELINE_TOTAL_TOKENS,
        **records,
        "architecture_decision": _record(decision_path, cache=cache),
    }
    config_path = root / "experiment-config.json"
    _write_immutable(config_path, config)
    return config_path


def _validate_config(config_path: Path) -> dict[str, Any]:
    value = _load_json(config_path, "experiment config")
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != CONFIG_VERSION
        or value.get("architecture_id") != ARCHITECTURE_ID
        or value.get("model") != MODEL
        or value.get("effort") != EFFORT
        or value.get("declared_turn_count") != 1
        or value.get("retry_count") != 0
        or value.get("maximum_total_tokens") != MAX_TOTAL_TOKENS
        or value.get("managed_chatgpt_auth_only") is not True
        or value.get("official_persistent_codex_app_server_only") is not True
        or value.get("semantic_regex_or_keyword_filtering") is not False
        or value.get("production_mutation_allowed") is not False
        or value.get("holdout_authorized") is not False
        or value.get("minimum_remaining_reserve_percent")
        != MINIMUM_REMAINING_RESERVE_PERCENT
        or value.get("quota_points_per_million_tokens")
        != QUOTA_POINTS_PER_MILLION_TOKENS
        or value.get("production_amortized_context_tokens")
        != PRODUCTION_CONTEXT_TOKENS
        or value.get("production_scale") != PRODUCTION_SCALE
        or value.get("baseline_total_tokens") != BASELINE_TOTAL_TOKENS
    ):
        raise SparseEventFrameLaneError("experiment config contract drifted")
    if Path(str(value.get("output_root") or "")).expanduser().resolve() != config_path.parent.resolve():
        raise SparseEventFrameLaneError("experiment output root drifted")
    for key in LINEAGE_KEYS:
        normalize_record(value.get(key) or {})
    projected = (
        PRODUCTION_CONTEXT_TOKENS + MAX_TOTAL_TOKENS * PRODUCTION_SCALE
    ) / BASELINE_TOTAL_TOKENS
    if projected > 0.28:
        raise SparseEventFrameLaneError("frozen token ceiling exceeds production target")
    return value


def _capacity_policy(root: Path, config: Mapping[str, Any]) -> dict[str, Path]:
    audit = root / "capacity-policy-audit.json"
    policy = root / "capacity-policy.json"
    projected_points = math.ceil(
        MAX_TOTAL_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    configured._write_stable_time(  # noqa: SLF001
        audit,
        {
            "schema_version": configured.CAPACITY_AUDIT_VERSION,
            "phase_id": str(config["experiment_id"]),
            "created_at": now_iso(),
            "production_mutation_performed": False,
            "measured_basis": {
                "declared_turn_count": 1,
                "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
                "phase_total_token_bound": MAX_TOTAL_TOKENS,
                "projected_phase_quota_points": projected_points,
                "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
            },
        },
        "created_at",
    )
    configured._write_stable_time(  # noqa: SLF001
        policy,
        {
            "schema_version": configured.CAPACITY_POLICY_VERSION,
            "phase_id": str(config["experiment_id"]),
            "created_at": now_iso(),
            "managed_chatgpt_auth_only": True,
            "official_persistent_codex_app_server_only": True,
            "retry_count_per_turn": 0,
            "production_mutation_allowed": False,
            "rate_limit_reached_type_must_be_null": True,
            "unknown_usage_hard_stop": True,
            "ordered_turn_names": [TURN_NAME],
            "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
            "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
            "phase_total_token_bound": MAX_TOTAL_TOKENS,
            "projected_phase_quota_points": projected_points,
            "semantic_output_root": str(root),
            "audit": _record(audit),
        },
        "created_at",
    )
    configured.reserve_module.load_reserve_capacity_policy(policy)
    return {"audit": audit, "policy": policy}


def freeze_experiment(config_path: Path) -> dict[str, Any]:
    config_path = config_path.expanduser().resolve()
    config = _validate_config(config_path)
    root = config_path.parent
    if (root / "runtime-lock.json").is_file():
        return verify_frozen(root)
    cache = ContentHashCache()
    for key in LINEAGE_KEYS:
        _verify_record(config[key], cache=cache)
    turn = _turn_paths(root)
    _write_private_text(
        turn["input"], Path(config["source_input"]["path"]).read_text(encoding="utf-8")
    )
    _write_private_text(
        turn["prompt"], Path(config["source_prompt"]["path"]).read_text(encoding="utf-8")
    )
    source_base = Path(config["source_base"]["path"]).read_text(encoding="utf-8")
    _write_private_text(turn["base"], source_base + "\n\n" + SPARSE_FRAME_INSTRUCTIONS + "\n")
    projection_schema = _load_json(
        Path(config["projection_schema"]["path"]), "projection schema"
    )
    _write_immutable(turn["projection_schema"], projection_schema)
    _write_immutable(turn["schema"], build_sparse_schema(projection_schema))
    capacity = _capacity_policy(root, config)
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "experiment_id": config["experiment_id"],
        "architecture_id": ARCHITECTURE_ID,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_total_tokens": MAX_TOTAL_TOKENS,
        "managed_chatgpt_auth_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "pinned_codex_cli": _record(PINNED_CODEX, cache=cache),
        "runtime_files": [_record(path, cache=cache) for path in _runtime_files()],
        "config": _record(config_path, cache=cache),
        "direct_lineage": [copy.deepcopy(config[key]) for key in LINEAGE_KEYS],
        "capacity_audit": _record(capacity["audit"], cache=cache),
        "capacity_policy": _record(capacity["policy"], cache=cache),
        "frozen_request": [
            _record(turn[name], cache=cache)
            for name in ("input", "prompt", "base", "schema", "projection_schema")
        ],
    }
    lock_path = root / "runtime-lock.json"
    configured._write_stable_time(lock_path, lock, "frozen_at")  # noqa: SLF001
    _write_immutable(
        root / "runtime-lock-closure.json",
        closure_receipt(lock_path, cache=ContentHashCache()),
    )
    return verify_frozen(root)


def verify_frozen(root: Path) -> dict[str, Any]:
    root = root.expanduser().resolve()
    config = _validate_config(root / "experiment-config.json")
    receipt = _load_json(root / "runtime-lock-closure.json", "runtime closure")
    try:
        result = verify_runtime_lock(
            root / "runtime-lock.json",
            cache=ContentHashCache(),
            expected_manifest_record=receipt.get("manifest"),
            expected_closure_digest=receipt.get("closure_digest"),
            required_fields={
                "schema_version": LOCK_VERSION,
                "experiment_id": config["experiment_id"],
                "architecture_id": ARCHITECTURE_ID,
                "model": MODEL,
                "effort": EFFORT,
                "declared_turn_count": 1,
                "retry_count": 0,
                "production_mutation_allowed": False,
            },
            required_record_paths=_runtime_files(),
        )
    except RuntimeVerificationError as exc:
        raise SparseEventFrameLaneError("runtime lock verification failed") from exc
    if result.manifest.get("pinned_codex_cli") != _record(PINNED_CODEX):
        raise SparseEventFrameLaneError("pinned Codex binary drifted")
    configured.reserve_module.load_reserve_capacity_policy(root / "capacity-policy.json")
    cache = ContentHashCache()
    for key in LINEAGE_KEYS:
        _verify_record(config[key], cache=cache)
    launch = root / "launch-receipt.json"
    if launch.exists():
        value = _load_json(launch, "launch receipt")
        for key in ("runtime_lock", "runtime_closure", "config"):
            _verify_record(value.get(key) or {}, cache=cache)
        if value.get("semantic_attempt_count") != 1 or value.get("retry_count") != 0:
            raise SparseEventFrameLaneError("launch receipt contract drifted")
    return {
        "root": root,
        "config": config,
        "runtime_lock": root / "runtime-lock.json",
        "runtime_closure": root / "runtime-lock-closure.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": _turn_paths(root),
        "source": _load_json(Path(config["source_input"]["path"]), "source input"),
    }


def _usage_from_sidecar(path: Path) -> dict[str, int]:
    sidecar = _load_json(path, "turn sidecar")
    values = sidecar.get("usage")
    if not isinstance(values, Mapping):
        raise SparseEventFrameLaneError("turn usage is absent")
    usage: dict[str, int] = {}
    for field in USAGE_FIELDS:
        value = values.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise SparseEventFrameLaneError("turn usage is incomplete")
        usage[field] = value
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
        or usage["cached_input_tokens"] > usage["input_tokens"]
        or usage["reasoning_output_tokens"] > usage["output_tokens"]
        or usage["total_tokens"] != usage["input_tokens"] + usage["output_tokens"]
    ):
        raise SparseEventFrameLaneError("measured sidecar contract failed")
    return usage


def _gate(usage: Mapping[str, int], diagnostics: Mapping[str, Any]) -> dict[str, Any]:
    rows = list(diagnostics["diagnostics"])
    total = PRODUCTION_CONTEXT_TOKENS + int(usage["total_tokens"]) * PRODUCTION_SCALE
    ratio = total / BASELINE_TOTAL_TOKENS
    checks = {
        "one_turn_measured": True,
        "total_tokens_lte_70000": int(usage["total_tokens"]) <= MAX_TOTAL_TOKENS,
        "all_source_units_reviewed": all(
            int(row["source_unit_count"]) == int(row["reviewed_source_unit_count"])
            for row in rows
        ),
        "unresolved_count_0": all(int(row["unresolved_count"]) == 0 for row in rows),
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "exact_identity_duplicates_0": True,
        "event_cap_violations_0": True,
        "semantic_event_count_proxy_not_used": True,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": "pif_sparse_event_frame_structural_gate_v1",
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "usage": dict(usage),
        "production_amortized_total_tokens": total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "segment_event_counts": {
            str(row["segment_id"]): int(row["event_count"]) for row in rows
        },
        "support_alignment_authorized": not failed,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    turn = frozen["turn"]
    measured: dict[str, int] | None = None
    unknown = 0
    if turn["sidecar"].exists():
        try:
            measured = _usage_from_sidecar(turn["sidecar"])
        except Exception:
            unknown = 1
    elif turn["capacity"].exists() or turn["output"].exists():
        unknown = 1
    semantic = isinstance(
        exc, (SparseEventFrameOutputError, SparseEventFrameArchitectureStop)
    )
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "sparse_event_frame_structural_or_cost_gate_not_passed"
            if semantic
            else "infrastructure_or_extraction_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": int(measured is not None) + unknown,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "usage": measured or {field: 0 for field in USAGE_FIELDS},
        "unknown_usage_attempt_count": unknown,
        "architecture_strategy_rejected": semantic,
        "support_alignment_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "exact_next_action": (
            "freeze this sparse-frame architecture as rejected; do not repair or retry it"
            if semantic
            else "audit the immutable attempt; do not retry this root"
        ),
    }
    configured._write_stable_time(root / "terminal.json", terminal, "terminal_at")  # noqa: SLF001
    return terminal


async def run_experiment(
    config_path: Path,
    *,
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = config_path.expanduser().resolve().parent
    if (root / "terminal.json").is_file():
        return _load_json(root / "terminal.json", "terminal")
    frozen = freeze_experiment(config_path)
    try:
        verify_frozen(root)
        launch = root / "launch-receipt.json"
        if not launch.exists():
            configured._write_stable_time(  # noqa: SLF001
                launch,
                {
                    "schema_version": "pif_sparse_event_frame_launch_v1",
                    "launched_at": now_iso(),
                    "semantic_attempt_count": 1,
                    "retry_count": 0,
                    "managed_chatgpt_auth_only": True,
                    "holdout_authorized": False,
                    "production_mutation_allowed": False,
                    "runtime_lock": _record(frozen["runtime_lock"]),
                    "runtime_closure": _record(frozen["runtime_closure"]),
                    "config": _record(config_path),
                },
                "launched_at",
            )
        verify_frozen(root)
        turn = frozen["turn"]
        if not turn["sidecar"].exists() and (
            turn["capacity"].exists() or turn["output"].exists()
        ):
            raise SparseEventFrameLaneError("attempt artifact exists without sidecar")
        async with client_factory(frozen["capacity_policy"]) as client:
            if not turn["sidecar"].exists():
                await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
                    base_instructions=turn["base"].read_text(encoding="utf-8"),
                    prompt=turn["prompt"].read_text(encoding="utf-8"),
                    output_schema=_load_json(turn["schema"], "sparse structured schema"),
                    cwd=PROJECT_ROOT,
                    sidecar_path=turn["sidecar"],
                    output_path=turn["output"],
                    batch_size=2,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=turn["capacity"],
                )
        usage = _usage_from_sidecar(turn["sidecar"])
        if usage["total_tokens"] > MAX_TOTAL_TOKENS:
            raise SparseEventFrameArchitectureStop("turn exceeded frozen token ceiling")
        output = _load_json(turn["output"], "sparse structured output")
        normalized, provenance, diagnostics = validate_and_project_output(output, frozen)
        _write_immutable(root / "normalized-output.private.json", normalized)
        _write_immutable(root / "evidence-provenance.private.json", provenance)
        _write_immutable(root / "diagnostics.private.json", diagnostics)
        gate = _gate(usage, diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise SparseEventFrameArchitectureStop("structural or production-cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "sparse_event_frame_structural_cost_passed",
            "terminal_reason": "sparse_event_frame_structural_cost_passed_support_alignment_required",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "production_amortized_total_token_ratio": gate[
                "production_amortized_total_token_ratio"
            ],
            "support_alignment_authorized": True,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "gate": _record(gate_path),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "sidecar": _record(turn["sidecar"]),
            "normalized_output": _record(root / "normalized-output.private.json"),
            "evidence_provenance": _record(root / "evidence-provenance.private.json"),
            "exact_next_action": "run the frozen source-support and neutral alignment evaluator",
        }
        configured._write_stable_time(root / "terminal.json", terminal, "terminal_at")  # noqa: SLF001
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "freeze", "verify", "run"))
    parser.add_argument("--root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--config", type=Path)
    args = parser.parse_args(argv)
    if args.command == "prepare":
        path = prepare_experiment(args.root)
        print(json.dumps({"config": str(path), "prepared": True}, sort_keys=True))
        return 0
    config = args.config or (args.root / "experiment-config.json")
    if args.command == "freeze":
        frozen = freeze_experiment(config)
        print(json.dumps({"root": str(frozen["root"]), "frozen": True}, sort_keys=True))
        return 0
    if args.command == "verify":
        frozen = verify_frozen(config.expanduser().resolve().parent)
        print(json.dumps({"root": str(frozen["root"]), "verified": True}, sort_keys=True))
        return 0
    terminal = asyncio.run(run_experiment(config))
    print(json.dumps(terminal, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
    return 0 if terminal.get("support_alignment_authorized") else 2


if __name__ == "__main__":
    raise SystemExit(main())
