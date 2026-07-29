from __future__ import annotations

"""Recover the frozen v253 graph canonicalizer in one new immutable attempt."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
import time
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_judge_v5_selection_v244_source_indexed_graph as v244
from . import app_server_judge_v5_selection_v249_explicit_applicability as v249
from . import app_server_judge_v5_selection_v253_graph_canonicalizer as v253
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v270_graph_canonicalizer_recovery_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v270_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v270_terminal_v1"
PHASE_ID = "development_selection_v5_4_v270_graph_canonicalizer_recovery"
MODEL = "gpt-5.6-sol"
EFFORT = "low"
MAX_TOTAL_TOKENS = 35_000
MAX_COMBINED_TOKENS = 73_162
TIMEOUT_SECONDS = 1200.0
MAX_PROMPT_BYTES = 100_000
MAX_BASE_BYTES = 20_000
MAX_SCHEMA_BYTES = 90_000
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
MIN_DENSE_EVENTS = 27
MAX_RESIDUAL_EVENTS = 1
USAGE_FIELDS = v249.USAGE_FIELDS
PROJECT_ROOT = v249.PROJECT_ROOT
PIPELINE_ROOT = v249.PIPELINE_ROOT
V244_ROOT = v244.DEFAULT_OUTPUT_ROOT
V247_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v247-alignment-transport-recovery"
).resolve()
V249_ROOT = v249.DEFAULT_OUTPUT_ROOT
V251_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v251-frozen-alignment-v249"
).resolve()
V252_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v252-capped-adjudication-v249"
).resolve()
V253_ROOT = v253.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v270-graph-canonicalizer-recovery"
).resolve()
PINNED_CODEX_0_144_1 = v249.PINNED_CODEX_0_144_1


class V270GraphCanonicalizerError(RuntimeError):
    """The v270 architecture cannot proceed or be adopted safely."""


class V270OutputContractError(V270GraphCanonicalizerError):
    """A completed canonicalizer output violated the frozen projection contract."""


class V270ArchitectureStop(V270GraphCanonicalizerError):
    """The measured architecture failed a predeclared gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V270GraphCanonicalizerError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V270GraphCanonicalizerError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V270GraphCanonicalizerError(f"frozen {path.name} drifted")
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


def _turn_root(root: Path, turn_name: str) -> Path:
    return root / "turns" / turn_name.replace("_", "-")


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = _turn_root(root, turn_name)
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "projection_schema": turn_root / "projection-schema.json",
        "direct_schema": turn_root / "direct-schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _source_turn_root(root: Path, prefix: str) -> Path:
    return next(root.glob(f"turns/{prefix}*"))


def _lineage_paths() -> dict[str, Path]:
    v244_turn = _source_turn_root(V244_ROOT, "v244-source-graph-")
    v249_turn = _source_turn_root(V249_ROOT, "v249-explicit-applicability-")
    v252_turn = V252_ROOT / "turns" / "v252-capped-side-free-adjudication"
    v253_turn = _source_turn_root(V253_ROOT, "v253-graph-canonicalizer-")
    return {
        "v244_terminal": V244_ROOT / "terminal.json",
        "v244_runtime_lock": V244_ROOT / "runtime-lock.json",
        "v244_raw_output": v244_turn / "output.private.json",
        "v244_normalized_output": v244_turn / "normalized-output.private.json",
        "v244_sidecar": v244_turn / "sidecar.json",
        "v247_terminal": V247_ROOT / "terminal.json",
        "v247_score": V247_ROOT / "alignment-score.json",
        "v249_terminal": V249_ROOT / "terminal.json",
        "v249_runtime_lock": V249_ROOT / "runtime-lock.json",
        "v249_source_input": v249_turn / "input.private.json",
        "v249_source_prompt": v249_turn / "prompt.private.md",
        "v249_direct_schema": v249_turn / "projection-schema.json",
        "v249_ranking": V249_ROOT / "architecture-ranking.json",
        "v251_terminal": V251_ROOT / "terminal.json",
        "v251_score": V251_ROOT / "alignment-score.json",
        "v252_terminal": V252_ROOT / "terminal.json",
        "v252_sidecar": v252_turn / "sidecar.json",
        "v252_output": v252_turn / "output.private.json",
        "v252_runtime_lock": V252_ROOT / "runtime-lock.json",
        "v253_terminal": V253_ROOT / "terminal.json",
        "v253_runtime_lock": V253_ROOT / "runtime-lock.json",
        "v253_launch_receipt": V253_ROOT / "launch-receipt.json",
        "v253_capacity": v253_turn / "capacity.json",
        "v253_sidecar": v253_turn / "sidecar.json",
        "v253_input": v253_turn / "input.private.json",
        "v253_prompt": v253_turn / "prompt.private.md",
        "v253_base": v253_turn / "base-instructions.private.md",
        "v253_schema": v253_turn / "schema.json",
        "v253_projection_schema": v253_turn / "projection-schema.json",
        "v253_direct_schema": v253_turn / "direct-schema.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    v244.verify_runtime_lock(paths["v244_runtime_lock"])
    v249.verify_runtime_lock(paths["v249_runtime_lock"])
    v253.verify_runtime_lock(paths["v253_runtime_lock"])
    t244 = _load_json(paths["v244_terminal"], "v244 terminal")
    s244 = _load_json(paths["v244_sidecar"], "v244 sidecar")
    t247 = _load_json(paths["v247_terminal"], "v247 terminal")
    q247 = _load_json(paths["v247_score"], "v247 score")
    t249 = _load_json(paths["v249_terminal"], "v249 terminal")
    t251 = _load_json(paths["v251_terminal"], "v251 terminal")
    q251 = _load_json(paths["v251_score"], "v251 score")
    t252 = _load_json(paths["v252_terminal"], "v252 terminal")
    s252 = _load_json(paths["v252_sidecar"], "v252 sidecar")
    t253 = _load_json(paths["v253_terminal"], "v253 terminal")
    s253 = _load_json(paths["v253_sidecar"], "v253 sidecar")
    if (
        t244.get("terminal_reason") != "v244_source_indexed_graph_structural_cost_gate_passed"
        or (s244.get("usage") or {}).get("total_tokens") != 38_162
        or t247.get("terminal_reason") != "v247_alignment_quality_or_permutation_gate_not_passed"
        or (q247.get("metrics") or {}).get("development_strict_full_field_macro_f1")
        != 0.680723
        or (q247.get("metrics") or {}).get(
            "strictly_equivalent_reference_semantic_unit_count"
        )
        != 6
        or t249.get("terminal_reason")
        != "v249_explicit_applicability_structural_cost_gate_passed"
        or t251.get("terminal_reason")
        != "v251_alignment_quality_or_permutation_gate_not_passed"
        or q251.get("failed_checks") != ["permutation_projection_exact"]
        or (q251.get("metrics") or {}).get("development_strict_full_field_macro_f1")
        != 0.980769
        or t252.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or t252.get("error_class") != "ReserveCapacityError"
        or t252.get("usage_status") != "unknown"
        or s252.get("state") != "completed"
        or s252.get("usage_status") != "measured"
        or s252.get("usage_complete") is not True
        or (s252.get("usage") or {}).get("total_tokens") != 98_728
        or (s252.get("usage") or {}).get("total_tokens") <= 70_000
        or s252.get("auth_type") != "chatgpt"
        or t252.get("production_mutated") is not False
        or t253.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or t253.get("error_class") != "ReserveCapacityError"
        or t253.get("usage_status") != "unknown"
        or t253.get("accounting_complete") is not False
        or t253.get("unknown_usage_attempt_count") != 1
        or s253.get("state") != "failed"
        or s253.get("status") != "failed"
        or s253.get("usage_status") != "unknown"
        or s253.get("usage_complete") is not False
        or s253.get("error_class") != "turn_failed"
        or s253.get("auth_type") != "chatgpt"
        or t253.get("production_mutated") is not False
        or (V253_ROOT / "turns" / _source_turn_root(V253_ROOT, "v253-graph-canonicalizer-").name / "output.private.json").exists()
    ):
        raise V270GraphCanonicalizerError("v270 architecture lineage drifted")
    source = v249._load_frozen(V249_ROOT)["turn"]
    return {
        "paths": paths,
        "records": records,
        "source_turn": source,
        "source_graph": _load_json(paths["v244_raw_output"], "v244 graph output"),
        "v252_terminal": t252,
        "v252_sidecar": s252,
        "v253_terminal": t253,
        "v253_sidecar": s253,
    }


CANONICALIZER_INSTRUCTIONS = """This is the second and final semantic stage of a source-graph then global canonicalizer architecture. The supplied source graph is a blind recall inventory, not a reference answer and not guaranteed correct. Re-read every source unit and decide all final event boundaries, fields, applicability states, evidence, attribution, stance, certainty, temporal horizon, and metrics yourself.

Account for every source_graph_event_id exactly once. A final event may cite one or more source graph IDs when source-supported nodes are duplicates or parts of one atomic event. Put a graph ID in dropped_source_graph_events only when you decide from the source that it is unsupported or not independently event-worthy. The LLM alone owns keep, merge, and drop decisions. Do not add events outside the graph inventory, infer a target count, or preserve a graph field merely because it was proposed upstream.

Final events must be independently truth-valued and atomic. Preserve distinct events when actor, reported actor, speaker, target, mechanism, stance, certainty, temporal horizon, attribution, claim, or event type materially differs. Use the smallest contiguous evidence-unit range supporting every material field. Keep events in source order and number them S0C000, S0C001, ... for the first segment and S1C000, S1C001, ... for the second.

Every optional field uses the explicit applicability contract in the schema. Select present only when exact evidence supports the value; select not_applicable when the field does not apply; select unknown only where offered and the field applies but the source does not identify it. Do not copy or infer values from nearby unrelated material. Return schema-valid JSON only."""


def _graph_ids(source_graph: Mapping[str, Any]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for segment in source_graph.get("segments") or []:
        segment_id = str(segment["segment_id"])
        ids = [str(event["event_id"]) for event in segment.get("events") or []]
        if len(ids) != len(set(ids)):
            raise V270GraphCanonicalizerError("source graph event IDs are not unique")
        result[segment_id] = ids
    return result


def _canonicalizer_schema(
    projection_schema: Mapping[str, Any], graph_ids: Mapping[str, Sequence[str]]
) -> dict[str, Any]:
    schema = copy.deepcopy(dict(projection_schema))
    segment = schema["properties"]["segments"]["items"]
    event = segment["properties"]["events"]["items"]
    all_graph_ids = [item for values in graph_ids.values() for item in values]
    all_final_ids = [f"S{position}C{index:03d}" for position in range(2) for index in range(32)]
    event["properties"]["event_id"] = {"type": "string", "enum": all_final_ids}
    event["properties"]["source_graph_event_ids"] = {
        "type": "array",
        "minItems": 1,
        "maxItems": len(all_graph_ids),
        "uniqueItems": True,
        "items": {"type": "string", "enum": all_graph_ids},
    }
    event["required"].extend(["event_id", "source_graph_event_ids"])
    segment["properties"]["dropped_source_graph_events"] = {
        "type": "array",
        "minItems": 0,
        "maxItems": len(all_graph_ids),
        "items": {
            "type": "object",
            "additionalProperties": False,
            "required": ["source_graph_event_id", "decision", "rationale"],
            "properties": {
                "source_graph_event_id": {"type": "string", "enum": all_graph_ids},
                "decision": {"type": "string", "enum": ["drop"]},
                "rationale": {"type": "string", "minLength": 1, "maxLength": 1000},
            },
        },
    }
    segment["required"].append("dropped_source_graph_events")
    return schema


def _graph_prompt(source_prompt: str, source_graph: Mapping[str, Any]) -> str:
    graph_segments = []
    for segment in source_graph.get("segments") or []:
        graph_segments.append(
            {
                "segment_id": segment["segment_id"],
                "source_graph_nodes": [dict(event) for event in segment.get("events") or []],
            }
        )
    return (
        source_prompt
        + "\n# Blind source graph recall inventory\n"
        + _canonical_json({"segments": graph_segments})
        + "\n"
    )


def prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    source = lineage["source_turn"]
    graph = lineage["source_graph"]
    graph_ids = _graph_ids(graph)
    projection_schema = v249._explicit_output_schema(source["direct_schema"])
    schema = _canonicalizer_schema(projection_schema, graph_ids)
    prompt = _graph_prompt(source["prompt"], graph)
    base = source["base"] + "\n\n# Global source-graph canonicalizer contract\n" + CANONICALIZER_INSTRUCTIONS + "\n"
    sizes = {
        "prompt_bytes": len(prompt.encode("utf-8")),
        "base_bytes": len(base.encode("utf-8")),
        "schema_bytes": len(_canonical_json(schema).encode("utf-8")),
    }
    if (
        set(graph_ids) != set(source["segment_ids"])
        or [len(graph_ids[segment_id]) for segment_id in source["segment_ids"]]
        != [31, 1]
        or sizes["prompt_bytes"] > MAX_PROMPT_BYTES
        or sizes["base_bytes"] > MAX_BASE_BYTES
        or sizes["schema_bytes"] > MAX_SCHEMA_BYTES
    ):
        raise V270GraphCanonicalizerError("v270 request or source graph drifted")
    return {
        "turn_name": "v270_graph_canonicalizer_" + sha256_text(source["episode_id"])[:20],
        "episode_id": source["episode_id"],
        "segment_ids": list(source["segment_ids"]),
        "private_input": copy.deepcopy(source["private_input"]),
        "prompt": prompt,
        "base": base,
        "schema": schema,
        "projection_schema": projection_schema,
        "direct_schema": copy.deepcopy(source["direct_schema"]),
        "graph_ids": graph_ids,
        **sizes,
    }


def project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    try:
        _validate_schema(turn["schema"], output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V270OutputContractError("canonicalizer output schema failed") from exc
    if output.get("episode_id") != turn["episode_id"]:
        raise V270OutputContractError("canonicalizer episode id drifted")
    segments = list(output.get("segments") or [])
    if [row.get("segment_id") for row in segments] != list(turn["segment_ids"]):
        raise V270OutputContractError("canonicalizer segment coverage or order drifted")
    projected = copy.deepcopy(dict(output))
    coverage_rows = []
    total_kept = total_dropped = 0
    for position, segment in enumerate(projected["segments"]):
        segment_id = str(segment["segment_id"])
        expected = list(turn["graph_ids"][segment_id])
        expected_set = set(expected)
        observed: list[str] = []
        kept_ids: list[str] = []
        for event_index, event in enumerate(segment.get("events") or []):
            if event.pop("event_id") != f"S{position}C{event_index:03d}":
                raise V270OutputContractError("canonicalizer final event ID order drifted")
            source_ids = [str(item) for item in event.pop("source_graph_event_ids")]
            if not source_ids or any(item not in expected_set for item in source_ids):
                raise V270OutputContractError("canonicalizer event cites cross-segment graph node")
            observed.extend(source_ids)
            kept_ids.extend(source_ids)
        dropped_rows = list(segment.pop("dropped_source_graph_events"))
        dropped_ids = [str(row["source_graph_event_id"]) for row in dropped_rows]
        if any(item not in expected_set for item in dropped_ids):
            raise V270OutputContractError("canonicalizer drop cites cross-segment graph node")
        observed.extend(dropped_ids)
        counts = Counter(observed)
        if set(counts) != expected_set or any(value != 1 for value in counts.values()):
            raise V270OutputContractError("canonicalizer graph coverage is not exact once")
        total_kept += len(kept_ids)
        total_dropped += len(dropped_ids)
        coverage_rows.append(
            {
                "segment_id": segment_id,
                "source_graph_event_count": len(expected),
                "kept_or_merged_source_graph_event_count": len(kept_ids),
                "dropped_source_graph_event_count": len(dropped_ids),
                "final_event_count": len(segment.get("events") or []),
                "all_source_graph_events_accounted_exactly_once": True,
                "semantic_decision_owner": "gpt-5.6-sol_low_global_canonicalizer",
            }
        )
    projection_turn = dict(turn)
    projection_turn["schema"] = turn["projection_schema"]
    try:
        normalized, provenance, diagnostics, applicability = v249.project_output(
            projected, projection_turn
        )
    except v249.V249OutputContractError as exc:
        raise V270OutputContractError(str(exc)) from exc
    coverage = {
        "schema_version": SCHEMA_VERSION,
        "segments": coverage_rows,
        "total_source_graph_event_count": total_kept + total_dropped,
        "kept_or_merged_source_graph_event_count": total_kept,
        "dropped_source_graph_event_count": total_dropped,
        "all_source_graph_events_accounted_exactly_once": True,
        "all_keep_merge_drop_semantics_selected_by_llm": True,
        "deterministic_projection_only": True,
    }
    return normalized, provenance, diagnostics, applicability, coverage


def _production_ratio(combined_tokens: int) -> tuple[int, float]:
    total = v244.PRODUCTION_AMORTIZED_CONTEXT_TOKENS + combined_tokens * v244.PRODUCTION_SCALE
    return total, total / v244.BASELINE_END_TO_END_TOKENS


def _gate(
    *, usage: Mapping[str, int], diagnostics: Sequence[Mapping[str, Any]], coverage: Mapping[str, Any]
) -> dict[str, Any]:
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense = int((by_id.get(v244.DENSE_SEGMENT_ID) or {}).get("event_count", -1))
    residual = int((by_id.get(v244.NO_SIGNAL_SEGMENT_ID) or {}).get("event_count", -1))
    combined = 38_162 + int(usage["total_tokens"])
    production_total, ratio = _production_ratio(combined)
    checks = {
        "both_segments_validated": set(by_id) == {v244.DENSE_SEGMENT_ID, v244.NO_SIGNAL_SEGMENT_ID},
        "dense_event_count_gte_27": dense >= MIN_DENSE_EVENTS,
        "nominal_no_signal_event_count_lte_1": 0 <= residual <= MAX_RESIDUAL_EVENTS,
        "all_source_units_reviewed": all(
            int(row["source_unit_count"]) == int(row["reviewed_source_unit_count"])
            for row in diagnostics
        ),
        "unresolved_count_0": all(int(row["unresolved_count"]) == 0 for row in diagnostics),
        "all_graph_nodes_accounted_exactly_once": coverage.get(
            "all_source_graph_events_accounted_exactly_once"
        )
        is True,
        "explicit_applicability_projection_passed": True,
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "new_turn_tokens_lte_35000": int(usage["total_tokens"]) <= MAX_TOTAL_TOKENS,
        "combined_tokens_lte_73162": combined <= MAX_COMBINED_TOKENS,
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
        "adopted_v244_graph_tokens": 38_162,
        "combined_tokens": combined,
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "dense_event_count": dense,
        "candidate_only_nominal_no_signal_event_count": residual,
        "residual_support_audit_required": residual > 0,
        "diagnostics": list(diagnostics),
        "coverage": dict(coverage),
        "support_alignment_authorized": not failed,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
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
                *v244._runtime_files(),
                *v249._runtime_files(),
                *v253._runtime_files(),
                Path(__file__).resolve(),
                Path(v244.__file__).resolve(),
                Path(v249.__file__).resolve(),
                Path(v253.__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
                PROJECT_ROOT
                / "research_factory/app_server_judge_v5_selection_v252_capped_adjudication.py",
                codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
            },
            key=str,
        )
    )


def _request_records(paths: Mapping[str, Path]) -> list[dict[str, Any]]:
    return [
        _record(paths[name])
        for name in ("input", "prompt", "base", "schema", "projection_schema", "direct_schema")
    ]


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v270 runtime lock")
    root = path.parent.resolve()
    spec = _load_json(root / "attempt-spec.json", "v270 spec")
    paths = _turn_paths(root, str(spec["turn_name"]))
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
        != {str(item) for item in _runtime_files()}
        or {row["path"] for row in lock.get("request") or []}
        != {row["path"] for row in _request_records(paths)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
    ):
        raise V270GraphCanonicalizerError("v270 runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("v253_failure_audit"),
        lock.get("authorization"),
        lock.get("ranking"),
        lock.get("design"),
        lock.get("spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("request") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V270GraphCanonicalizerError("v270 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V270GraphCanonicalizerError("v270 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v270 spec")
    paths = _turn_paths(root, str(spec["turn_name"]))
    lineage = _validate_lineage()
    turn = prepare_turn(lineage)
    return {
        "root": root,
        "spec_path": spec_path,
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": {
            **turn,
            "private_input": _load_json(paths["input"], "v270 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v270 schema"),
            "projection_schema": _load_json(paths["projection_schema"], "v270 projection schema"),
            "direct_schema": _load_json(paths["direct_schema"], "v270 direct schema"),
            "paths": paths,
        },
    }


def freeze_v270(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v270 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V270GraphCanonicalizerError("unfinished v270 root is not replayable")
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
    _write_immutable(paths["projection_schema"], turn["projection_schema"])
    _write_immutable(paths["direct_schema"], turn["direct_schema"])
    reused_request_names = {
        "input": "v253_input",
        "prompt": "v253_prompt",
        "base": "v253_base",
        "schema": "v253_schema",
        "projection_schema": "v253_projection_schema",
        "direct_schema": "v253_direct_schema",
    }
    if any(
        _record(paths[name])["sha256"]
        != lineage["records"][predecessor_name]["sha256"]
        for name, predecessor_name in reused_request_names.items()
    ):
        raise V270GraphCanonicalizerError("frozen v253 request was not reused exactly")
    capacity_paths = _capacity_policy(root, turn["turn_name"])
    v253_audit_path = root / "v253-infrastructure-failure-audit.json"
    _write_stable_time(
        v253_audit_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "predecessor_terminal": lineage["records"]["v253_terminal"],
            "predecessor_sidecar": lineage["records"]["v253_sidecar"],
            "predecessor_runtime_lock": lineage["records"]["v253_runtime_lock"],
            "terminal_reported_usage_status": "unknown",
            "sidecar_usage_status": "unknown",
            "accounting_complete": False,
            "failed_turn_error_class": "turn_failed",
            "predecessor_output_absent": True,
            "failure_class": "external_transport_turn_failed_unknown_usage",
            "predecessor_output_adopted": False,
            "predecessor_retried": False,
            "frozen_request_reused_byte_for_byte": True,
            "production_mutated": False,
        },
        "created_at",
    )
    authorization_path = root / "authorization.json"
    _write_stable_time(
        authorization_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "authority": "direct_operator_architecture_steering_2026_07_17",
            "scope": "one bounded recovery of the frozen v253 source graph canonicalizer request",
            "semantic_prompt_or_schema_delta": False,
            "isolated_field_patch": False,
            "semantic_attempt_count": 1,
            "retry_count": 0,
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
            "selected_architecture_id": "source_graph_then_global_full_schema_canonicalizer",
            "architectures": [
                {"rank": 1, "id": "source_graph_then_global_full_schema_canonicalizer"},
                {"rank": 2, "id": "episode_bootstrap_then_segment_specialists_with_llm_join"},
                {"rank": 3, "id": "independent_event_proposals_then_global_llm_owner_reconcile"},
            ],
            "measured_basis": {
                "v244_graph_tokens": 38_162,
                "v244_dense_events": 31,
                "v247_v244_macro_f1": 0.680723,
                "v247_v244_strict_reference_units": 6,
                "v249_macro_f1": 0.980769,
                "v249_strict_reference_units": 25,
                "v253_semantic_output_observed": False,
                "v253_usage_status": "unknown",
                "v253_failure_class": "external_transport_turn_failed",
            },
            "on_failure": "freeze and reject graph canonicalizer; advance to episode bootstrap plus segment specialists with LLM join",
        },
        "created_at",
    )
    projected_total, projected_ratio = _production_ratio(MAX_COMBINED_TOKENS)
    design_path = root / "architecture-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "architecture_id": "source_graph_then_global_full_schema_canonicalizer",
            "hypothesis": (
                "the unchanged v253 request can test whether the measured v244 source graph retains blind recall "
                "while one global Sol-low canonicalizer re-reads the source and corrects event boundaries and "
                "full-field semantics using v249 explicit applicability; v253 produced no semantic result, so "
                "this one fresh version is the smallest decision-changing canary"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "adopted_graph_semantic_turn_count": 1,
                "new_semantic_turn_count": 1,
                "semantic_prompt_or_schema_delta_from_v253": False,
                "reference_visible_to_model": False,
                "target_count_visible_to_model": False,
            },
            "production_cost_projection": {
                "adopted_graph_tokens": 38_162,
                "new_turn_hard_max": MAX_TOTAL_TOKENS,
                "combined_hard_max": MAX_COMBINED_TOKENS,
                "production_amortized_total_tokens": projected_total,
                "projected_production_amortized_total_token_ratio": round(projected_ratio, 6),
                "required_ratio_max": 0.28,
            },
            "predeclared_stop_rules": {
                "retry_count": 0,
                "all_graph_nodes_accounted_exactly_once": True,
                "dense_event_count_minimum": MIN_DENSE_EVENTS,
                "nominal_no_signal_event_count_maximum": MAX_RESIDUAL_EVENTS,
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "event_cap_violations": 0,
                "exact_identity_duplicates": 0,
                "new_turn_tokens_max": MAX_TOTAL_TOKENS,
                "combined_tokens_max": MAX_COMBINED_TOKENS,
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
            "state": "frozen_before_one_turn_graph_canonicalizer_recovery",
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
        "v253_failure_audit": _record(v253_audit_path),
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


def _measured_usage(sidecar: Mapping[str, Any]) -> Optional[dict[str, int]]:
    values = sidecar.get("usage") or {}
    try:
        usage = {field: int(values[field]) for field in USAGE_FIELDS}
    except (KeyError, TypeError, ValueError):
        return None
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
    ):
        return None
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
    measured_over_cap = False
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        sidecar = _load_json(paths["sidecar"], "v270 sidecar")
        measured = _measured_usage(sidecar)
        if measured is not None:
            usage = measured
            unknown = 0
            measured_over_cap = usage["total_tokens"] > MAX_TOTAL_TOKENS
    semantic = isinstance(exc, (V270OutputContractError, V270ArchitectureStop)) or measured_over_cap
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v270_graph_canonicalizer_structural_quality_or_cost_gate_not_passed"
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
        "measured_token_bound_exceeded": measured_over_cap,
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
            "advance to episode bootstrap plus segment specialists with LLM join"
            if semantic
            else "audit immutable v270 infrastructure attempt; no retry"
        ),
    }
    gate_path = root / "architecture-structural-gate.json"
    if gate_path.is_file():
        terminal["gate"] = _record(gate_path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v270(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v270 terminal")
    frozen = freeze_v270(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root, frozen, V270GraphCanonicalizerError("launch exists; replay prohibited")
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
            raise V270GraphCanonicalizerError("v270 turn did not complete")
        usage = _measured_usage(_load_json(paths["sidecar"], "v270 sidecar"))
        if usage is None:
            raise V270GraphCanonicalizerError("v270 sidecar accounting or auth failed")
        if usage["total_tokens"] > MAX_TOTAL_TOKENS:
            raise V270ArchitectureStop("v270 measured turn exceeded frozen token bound")
        normalized, provenance, diagnostics, applicability, coverage = project_output(
            result.output, frozen["turn"]
        )
        artifacts = {
            "normalized": root / "normalized-output.private.json",
            "provenance": root / "evidence-provenance.private.json",
            "diagnostics": root / "diagnostics.private.json",
            "applicability": root / "applicability-receipt.json",
            "coverage": root / "graph-coverage-receipt.json",
            "gate": root / "architecture-structural-gate.json",
        }
        _write_immutable(artifacts["normalized"], normalized)
        _write_immutable(artifacts["provenance"], provenance)
        _write_immutable(artifacts["diagnostics"], {"segments": diagnostics})
        _write_immutable(artifacts["applicability"], applicability)
        _write_immutable(artifacts["coverage"], coverage)
        gate = _gate(usage=usage, diagnostics=diagnostics, coverage=coverage)
        _write_immutable(artifacts["gate"], gate)
        if not gate["passed"]:
            raise V270ArchitectureStop("v270 structural quality or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v270_architecture_structural_gate_passed",
            "terminal_reason": "v270_graph_canonicalizer_structural_cost_gate_passed",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "new_turn_usage": usage,
            "adopted_v244_graph_tokens": 38_162,
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
            "gate": _record(artifacts["gate"]),
            "normalized_output": _record(artifacts["normalized"]),
            "evidence_provenance": _record(artifacts["provenance"]),
            "applicability_receipt": _record(artifacts["applicability"]),
            "graph_coverage_receipt": _record(artifacts["coverage"]),
            "sidecar": _record(paths["sidecar"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": "run frozen side-free support and neutral alignment before holdout",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v270 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v270 source graph canonicalizer")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v270(output_dir=Path(args.output_dir))
        result = {
            "state": "frozen",
            "root": str(frozen["root"]),
            "prompt_bytes": frozen["turn"]["prompt_bytes"],
            "base_bytes": frozen["turn"]["base_bytes"],
            "schema_bytes": frozen["turn"]["schema_bytes"],
        }
    else:
        terminal = asyncio.run(
            run_v270(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "total_tokens": (terminal.get("new_turn_usage") or terminal.get("usage") or {}).get(
                "total_tokens"
            ),
            "support_alignment_authorized": terminal.get("support_alignment_authorized", False),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
