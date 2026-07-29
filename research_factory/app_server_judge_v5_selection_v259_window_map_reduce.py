from __future__ import annotations

"""Run a one-turn overlapping-window map plus global LLM owner-reduce canary."""

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
from . import app_server_judge_v5_selection_v249_explicit_applicability as v249
from . import app_server_judge_v5_selection_v258_graph_canonicalizer_recovery as v258
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v259_window_map_reduce_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v259_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v259_terminal_v1"
PHASE_ID = "development_selection_v5_4_v259_window_map_reduce"
MODEL = "gpt-5.6-sol"
EFFORT = "high"
MAX_TOTAL_TOKENS = 55_000
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
MAX_PROMPT_BYTES = 100_000
MAX_BASE_BYTES = 24_000
MAX_SCHEMA_BYTES = 100_000
WINDOW_IDS = (0, 1, 2, 3)
MAX_PROPOSALS_PER_WINDOW = 20
MIN_DENSE_EVENTS = 27
MAX_RESIDUAL_EVENTS = 1
USAGE_FIELDS = v249.USAGE_FIELDS
PROJECT_ROOT = v249.PROJECT_ROOT
PIPELINE_ROOT = v249.PIPELINE_ROOT
V249_ROOT = v249.DEFAULT_OUTPUT_ROOT
V257_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v257-frozen-alignment-v255"
).resolve()
V258_ROOT = v258.DEFAULT_OUTPUT_ROOT
V238_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v238-specialist-ensemble"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v259-window-map-reduce"
).resolve()
PINNED_CODEX_0_144_1 = v249.PINNED_CODEX_0_144_1

EXPECTED_LINEAGE_HASHES = {
    "v249_terminal": "56497104fa40aee405a769f39fba5ba082418e3149909222708954e63bb06a5d",
    "v249_runtime_lock": "5f20ee4aef5ca6a997786a5590fd78b7bd92fd3724d9a274ba273c3fdd53c796",
    "v257_terminal": "70bb6ae63b31c6a5d6bb35ccbe246e10adf0744f77f7ac54d540d255d9926bed",
    "v257_score": "b5c1accbb4190e2e1f911c3b94a9e3aeaded93b6da2eb197357876b985aa28f5",
    "v258_terminal": "f4c4ae562dd8adb5ac4b99ffa1d289854fec4919740208999bfe54854cddb900",
    "v258_runtime_lock": "ae7cc0fc39c101d5a8c313503fccba7441400a340df7bf8b14422db0dbf2b99f",
    "v258_sidecar": "f0af7dcb77022c9a5af8a232f2303e9865168b9974aa9b7b5601c1d5cba5dcfb",
    "v238_terminal": "93f22c731e66c8438b4db3f85ff86f9af63f9e612c86121dc93b83910e84e949",
    "v238_sidecar": "8f0b5df4fe877f23abac9a0f8da363de9916856d487032fa3a30ad9033a2368b",
}

WINDOW_MAP_INSTRUCTIONS = """Use the four fixed overlapping source windows in each segment as independent local map views before creating final events. The source packet contains exact owner ranges and wider extract ranges for windows 0, 1, 2, and 3. For every window, inspect its entire extract range and list compact atomic source-supported propositions visible there in source order. Overlap may cause the same proposition to be proposed in multiple windows; preserve those local proposals rather than silently deduplicating them.

After all window maps are complete, perform one global semantic owner/reduce pass over their union while re-reading the full segment. Every local proposal must be assigned exactly once: cite it from exactly one final full-schema event, or place it in dropped_window_proposals with an LLM-authored source-based rationale. A final event may cite multiple local proposals when they are overlapping duplicates or jointly describe one independently truth-valued event. The LLM alone decides proposition meaning, final event boundaries, merge versus separation, fields, applicability, evidence, attribution, and drops.

Keep final events separate whenever actor, reported actor, speaker, target, causal mechanism, stance, certainty, temporal horizon, attribution, claim, or event type materially differs. Use the smallest contiguous exact evidence-unit range supporting every material final field. Do not infer a target count, reference answer, prior event list, topic keyword rule, or semantic shortcut. A genuine no-signal segment still returns all four window maps with empty proposal arrays, plus empty final events and drops. Return schema-valid JSON only."""


class V259WindowMapReduceError(RuntimeError):
    """The v259 architecture cannot proceed or be adopted safely."""


class V259OutputContractError(V259WindowMapReduceError):
    """A completed output violated the frozen map/reduce ownership contract."""


class V259ArchitectureStop(V259WindowMapReduceError):
    """The measured architecture failed a predeclared gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V259WindowMapReduceError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V259WindowMapReduceError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V259WindowMapReduceError(f"frozen {path.name} drifted")
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
    v258_turn = next(V258_ROOT.glob("turns/v258-graph-canonicalizer-*"))
    v238_turn = next(V238_ROOT.glob("turns/v238-specialist-ensemble-*"))
    return {
        "v249_terminal": V249_ROOT / "terminal.json",
        "v249_runtime_lock": V249_ROOT / "runtime-lock.json",
        "v257_terminal": V257_ROOT / "terminal.json",
        "v257_score": V257_ROOT / "alignment-score.json",
        "v258_terminal": V258_ROOT / "terminal.json",
        "v258_runtime_lock": V258_ROOT / "runtime-lock.json",
        "v258_sidecar": v258_turn / "sidecar.json",
        "v238_terminal": V238_ROOT / "terminal.json",
        "v238_sidecar": v238_turn / "sidecar.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V259WindowMapReduceError(f"frozen lineage {name} drifted")
    v249.verify_runtime_lock(paths["v249_runtime_lock"])
    v258.verify_runtime_lock(paths["v258_runtime_lock"])
    t249 = _load_json(paths["v249_terminal"], "v249 terminal")
    t257 = _load_json(paths["v257_terminal"], "v257 terminal")
    q257 = _load_json(paths["v257_score"], "v257 score")
    t258 = _load_json(paths["v258_terminal"], "v258 terminal")
    s258 = _load_json(paths["v258_sidecar"], "v258 sidecar")
    t238 = _load_json(paths["v238_terminal"], "v238 terminal")
    s238 = _load_json(paths["v238_sidecar"], "v238 sidecar")
    if (
        t249.get("terminal_reason")
        != "v249_explicit_applicability_structural_cost_gate_passed"
        or t257.get("terminal_reason")
        != "v257_alignment_quality_or_permutation_gate_not_passed"
        or (q257.get("metrics") or {}).get("development_strict_full_field_macro_f1")
        != 0.77027
        or t258.get("terminal_reason")
        != "v258_graph_canonicalizer_structural_quality_or_cost_gate_not_passed"
        or t258.get("measured_token_bound_exceeded") is not True
        or (s258.get("usage") or {}).get("total_tokens") != 49_354
        or t238.get("terminal_reason")
        != "v238_specialist_ensemble_structural_or_count_gate_not_passed"
        or t238.get("error_class") != "V238OutputContractError"
        or (s238.get("usage") or {}).get("total_tokens") != 33_721
        or t249.get("production_mutated") is not False
        or t257.get("production_mutated") is not False
        or t258.get("production_mutated") is not False
        or t238.get("production_mutated") is not False
    ):
        raise V259WindowMapReduceError("v259 predecessor evidence drifted")
    return {
        "paths": paths,
        "records": records,
        "source": v249._load_frozen(V249_ROOT)["turn"],
    }


def _proposal_ids() -> list[str]:
    return [
        f"W{window_id}P{index:02d}"
        for window_id in WINDOW_IDS
        for index in range(MAX_PROPOSALS_PER_WINDOW)
    ]


def _window_schema(v249_schema: Mapping[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(dict(v249_schema))
    segment = schema["properties"]["segments"]["items"]
    segment_props = segment["properties"]
    events = segment_props["events"]
    event_item = events["items"]
    event_props = event_item["properties"]
    proposal_ids = _proposal_ids()
    proposal = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "proposal_id",
            "evidence_start_unit_id",
            "evidence_end_unit_id",
            "proposition_text",
        ],
        "properties": {
            "proposal_id": {"type": "string", "enum": proposal_ids},
            "evidence_start_unit_id": copy.deepcopy(
                event_props["evidence_start_unit_id"]
            ),
            "evidence_end_unit_id": copy.deepcopy(event_props["evidence_end_unit_id"]),
            "proposition_text": {"type": "string", "minLength": 1, "maxLength": 500},
        },
    }
    window_maps = {
        "type": "array",
        "minItems": len(WINDOW_IDS),
        "maxItems": len(WINDOW_IDS),
        "items": {
            "type": "object",
            "additionalProperties": False,
            "required": ["window_id", "proposals"],
            "properties": {
                "window_id": {"type": "integer", "enum": list(WINDOW_IDS)},
                "proposals": {
                    "type": "array",
                    "minItems": 0,
                    "maxItems": MAX_PROPOSALS_PER_WINDOW,
                    "items": proposal,
                },
            },
        },
    }
    dropped = {
        "type": "array",
        "minItems": 0,
        "maxItems": len(proposal_ids),
        "items": {
            "type": "object",
            "additionalProperties": False,
            "required": ["window_proposal_id", "decision", "rationale"],
            "properties": {
                "window_proposal_id": {"type": "string", "enum": proposal_ids},
                "decision": {"type": "string", "enum": ["drop"]},
                "rationale": {"type": "string", "minLength": 1, "maxLength": 1000},
            },
        },
    }
    event_item["properties"] = {
        "source_window_proposal_ids": {
            "type": "array",
            "minItems": 1,
            "maxItems": len(proposal_ids),
            "items": {"type": "string", "enum": proposal_ids},
        },
        **event_props,
    }
    event_item["required"] = ["source_window_proposal_ids", *event_item["required"]]
    new_segment_props: dict[str, Any] = {}
    for name, value in segment_props.items():
        if name == "events":
            new_segment_props["window_maps"] = window_maps
            new_segment_props["dropped_window_proposals"] = dropped
        new_segment_props[name] = value
    segment["properties"] = new_segment_props
    required = list(segment["required"])
    event_index = required.index("events")
    required[event_index:event_index] = ["window_maps", "dropped_window_proposals"]
    segment["required"] = required
    return schema


def _source_window_ids(source: Mapping[str, Any]) -> dict[str, list[int]]:
    result = {}
    for segment in source["private_input"]["segments"]:
        ids = [int(row["window_id"]) for row in segment.get("boundaries") or []]
        if ids != list(WINDOW_IDS):
            raise V259WindowMapReduceError("source fixed-window coverage drifted")
        result[str(segment["segment_id"])] = ids
    return result


def prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    source = lineage["source"]
    window_ids = _source_window_ids(source)
    schema = _window_schema(source["schema"])
    base = (
        source["base"]
        + "\n\n# Overlapping-window map and global owner/reduce contract\n"
        + WINDOW_MAP_INSTRUCTIONS
        + "\n"
    )
    sizes = {
        "prompt_bytes": len(source["prompt"].encode("utf-8")),
        "base_bytes": len(base.encode("utf-8")),
        "schema_bytes": len(_canonical_json(schema).encode("utf-8")),
    }
    if (
        set(window_ids) != set(source["segment_ids"])
        or sizes["prompt_bytes"] > MAX_PROMPT_BYTES
        or sizes["base_bytes"] > MAX_BASE_BYTES
        or sizes["schema_bytes"] > MAX_SCHEMA_BYTES
    ):
        raise V259WindowMapReduceError("v259 request size or window coverage failed")
    return {
        "turn_name": "v259_window_map_reduce_" + sha256_text(source["episode_id"])[:20],
        "episode_id": source["episode_id"],
        "segment_ids": list(source["segment_ids"]),
        "private_input": copy.deepcopy(source["private_input"]),
        "prompt": source["prompt"],
        "base": base,
        "schema": schema,
        "v249_schema": copy.deepcopy(source["schema"]),
        "direct_schema": copy.deepcopy(source["direct_schema"]),
        "window_ids": window_ids,
        **sizes,
    }


def project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    try:
        _validate_schema(turn["schema"], output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V259OutputContractError("window-map output schema failed") from exc
    if output.get("episode_id") != turn["episode_id"]:
        raise V259OutputContractError("window-map episode id drifted")
    direct = copy.deepcopy(dict(output))
    if [row.get("segment_id") for row in direct.get("segments") or []] != list(
        turn["segment_ids"]
    ):
        raise V259OutputContractError("window-map segment coverage or order drifted")
    ownership_rows = []
    for segment in direct["segments"]:
        segment_id = str(segment["segment_id"])
        maps = list(segment.pop("window_maps"))
        if [int(row["window_id"]) for row in maps] != list(WINDOW_IDS):
            raise V259OutputContractError("window-map order drifted")
        proposal_ids: list[str] = []
        for window in maps:
            window_id = int(window["window_id"])
            proposals = list(window.get("proposals") or [])
            expected = [f"W{window_id}P{index:02d}" for index in range(len(proposals))]
            observed = [str(row["proposal_id"]) for row in proposals]
            if observed != expected:
                raise V259OutputContractError("window proposal ID order drifted")
            proposal_ids.extend(observed)
        observed_ownership: list[str] = []
        for event in segment.get("events") or []:
            observed_ownership.extend(
                str(item) for item in event.pop("source_window_proposal_ids")
            )
        dropped = list(segment.pop("dropped_window_proposals"))
        observed_ownership.extend(str(row["window_proposal_id"]) for row in dropped)
        counts = Counter(observed_ownership)
        if set(counts) != set(proposal_ids) or any(value != 1 for value in counts.values()):
            raise V259OutputContractError(
                "window proposals are not owned exactly once by final events or drops"
            )
        ownership_rows.append(
            {
                "segment_id": segment_id,
                "window_count": len(maps),
                "window_proposal_count": len(proposal_ids),
                "final_event_count": len(segment.get("events") or []),
                "dropped_window_proposal_count": len(dropped),
                "all_window_proposals_accounted_exactly_once": True,
            }
        )
    v249_turn = dict(turn)
    v249_turn["schema"] = turn["v249_schema"]
    try:
        normalized, provenance, diagnostics, applicability = v249.project_output(
            direct, v249_turn
        )
    except v249.V249OutputContractError as exc:
        raise V259OutputContractError(str(exc)) from exc
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "segments": ownership_rows,
        "all_map_reduce_semantics_selected_by_llm": True,
        "deterministic_ownership_projection_only": True,
        "applicability": applicability,
    }
    return normalized, provenance, diagnostics, receipt


def _production_ratio(tokens: int) -> tuple[int, float]:
    return v249._production_ratio(tokens)


def _gate(
    *,
    usage: Mapping[str, int],
    diagnostics: Sequence[Mapping[str, Any]],
    ownership: Mapping[str, Any],
) -> dict[str, Any]:
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense = int((by_id.get(v249.v239.DENSE_SEGMENT_ID) or {}).get("event_count", -1))
    residual = int(
        (by_id.get(v249.v239.NO_SIGNAL_SEGMENT_ID) or {}).get("event_count", -1)
    )
    total, ratio = _production_ratio(int(usage["total_tokens"]))
    ownership_rows = list(ownership.get("segments") or [])
    checks = {
        "both_segments_validated": set(by_id)
        == {v249.v239.DENSE_SEGMENT_ID, v249.v239.NO_SIGNAL_SEGMENT_ID},
        "dense_event_count_gte_27": dense >= MIN_DENSE_EVENTS,
        "nominal_no_signal_event_count_lte_1": 0 <= residual <= MAX_RESIDUAL_EVENTS,
        "all_source_units_reviewed": all(
            int(row["source_unit_count"]) == int(row["reviewed_source_unit_count"])
            for row in diagnostics
        ),
        "unresolved_count_0": all(
            int(row["unresolved_count"]) == 0 for row in diagnostics
        ),
        "all_windows_mapped_and_proposals_owned_exactly_once": len(ownership_rows) == 2
        and all(
            int(row["window_count"]) == len(WINDOW_IDS)
            and row.get("all_window_proposals_accounted_exactly_once") is True
            for row in ownership_rows
        ),
        "explicit_applicability_projection_passed": True,
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "total_tokens_lte_55000": int(usage["total_tokens"]) <= MAX_TOTAL_TOKENS,
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
        "production_amortized_total_tokens": total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "dense_event_count": dense,
        "candidate_only_nominal_no_signal_event_count": residual,
        "residual_support_audit_required": residual > 0,
        "diagnostics": list(diagnostics),
        "ownership": dict(ownership),
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
        "v249_schema": turn_root / "v249-projection-schema.json",
        "direct_schema": turn_root / "direct-projection-schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _request_records(paths: Mapping[str, Path]) -> list[dict[str, Any]]:
    return [
        _record(paths[name])
        for name in ("input", "prompt", "base", "schema", "v249_schema", "direct_schema")
    ]


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
                *v249._runtime_files(),
                *v258._runtime_files(),
                Path(__file__).resolve(),
                Path(v249.__file__).resolve(),
                Path(v258.__file__).resolve(),
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


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v259 runtime lock")
    root = path.parent.resolve()
    spec = _load_json(root / "attempt-spec.json", "v259 spec")
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
        != {str(file) for file in _runtime_files()}
        or {row["path"] for row in lock.get("request") or []}
        != {row["path"] for row in _request_records(paths)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
    ):
        raise V259WindowMapReduceError("v259 runtime lock contract drifted")
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
        raise V259WindowMapReduceError("v259 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V259WindowMapReduceError("v259 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v259 spec")
    paths = _turn_paths(root, spec["turn_name"])
    lineage = _validate_lineage()
    turn = prepare_turn(lineage)
    return {
        "root": root,
        "spec_path": spec_path,
        "spec": spec,
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": {
            **turn,
            "private_input": _load_json(paths["input"], "v259 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v259 schema"),
            "v249_schema": _load_json(paths["v249_schema"], "v249 projection schema"),
            "direct_schema": _load_json(paths["direct_schema"], "direct projection schema"),
            "paths": paths,
        },
    }


def freeze_v259(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v259 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V259WindowMapReduceError("unfinished v259 root is not replayable")
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
    _write_immutable(paths["v249_schema"], turn["v249_schema"])
    _write_immutable(paths["direct_schema"], turn["direct_schema"])
    capacity_paths = _capacity_policy(root, turn["turn_name"])
    authorization_path = root / "authorization.json"
    _write_stable_time(
        authorization_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "authority": "direct_operator_bounded_architecture_steering_2026_07_17",
            "scope": "one blind one-turn overlapping-window map plus global owner-reduce canary",
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
            "selected_architecture_id": "one_turn_overlapping_window_maps_global_llm_owner_reduce",
            "architectures": [
                {"rank": 1, "id": "one_turn_overlapping_window_maps_global_llm_owner_reduce"},
                {"rank": 2, "id": "episode_bootstrap_segment_specialists_global_llm_join"},
                {"rank": 3, "id": "dual_independent_global_drafts_single_turn_llm_selection"},
            ],
            "measured_basis": {
                "v249_total_tokens": 44_474,
                "v249_authoritative_macro_f1": 0.96,
                "v255_v257_macro_f1": 0.77027,
                "v258_new_turn_tokens": 49_354,
                "v258_combined_cost_failed": True,
                "v238_specialist_tokens": 33_721,
                "v238_failure_class": "specialist_candidate_ownership_contract",
            },
            "on_failure": "freeze and reject window map/reduce; advance to rank 2 without field repair",
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
            "architecture_id": "one_turn_overlapping_window_maps_global_llm_owner_reduce",
            "hypothesis": (
                "fixed overlapping local maps will expose boundary-local propositions while a same-turn global "
                "LLM owner/reduce pass prevents duplicated events, preserving one transcript prefill and v249 full-field quality"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "fixed_windows_per_segment": len(WINDOW_IDS),
                "declared_semantic_turn_count": 1,
                "reference_visible_to_model": False,
                "target_count_visible_to_model": False,
            },
            "production_cost_projection": {
                "turn_hard_max": MAX_TOTAL_TOKENS,
                "projected_production_amortized_total_tokens": projected_total,
                "projected_production_amortized_total_token_ratio": round(
                    projected_ratio, 6
                ),
                "required_ratio_max": 0.28,
            },
            "predeclared_stop_rules": {
                "retry_count": 0,
                "all_four_windows_mapped": True,
                "all_window_proposals_accounted_exactly_once": True,
                "dense_event_count_minimum": MIN_DENSE_EVENTS,
                "nominal_no_signal_event_count_maximum": MAX_RESIDUAL_EVENTS,
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "event_cap_violations": 0,
                "exact_identity_duplicates": 0,
                "total_tokens_max": MAX_TOTAL_TOKENS,
                "production_amortized_total_token_ratio_max": 0.28,
                "on_structural_pass": "run frozen side-free support then alignment",
                "on_failure": "reject architecture without isolated field repair",
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
            "state": "frozen_before_one_turn_window_map_reduce_canary",
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
        "runtime_files": [_record(file) for file in _runtime_files()],
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
        raise V259WindowMapReduceError("v259 sidecar usage incomplete") from exc
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None
    ):
        raise V259WindowMapReduceError("v259 sidecar accounting or auth failed")
    return usage


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
    measured_over_cap = False
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        try:
            sidecar = _load_json(paths["sidecar"], "v259 sidecar")
            usage = {
                field: int((sidecar.get("usage") or {})[field]) for field in USAGE_FIELDS
            }
            unknown = int(
                sidecar.get("usage_complete") is not True
                or sidecar.get("usage_status") != "measured"
            )
            measured_over_cap = unknown == 0 and usage["total_tokens"] > MAX_TOTAL_TOKENS
        except Exception:
            unknown = 1
    semantic = isinstance(exc, (V259OutputContractError, V259ArchitectureStop)) or measured_over_cap
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v259_window_map_reduce_structural_quality_or_cost_gate_not_passed"
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
            "reject v259 and advance to ranked architecture 2 without field repair"
            if semantic
            else "audit immutable v259 infrastructure attempt; no retry"
        ),
    }
    gate_path = root / "architecture-structural-gate.json"
    if gate_path.is_file():
        terminal["gate"] = _record(gate_path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v259(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v259 terminal")
    frozen = freeze_v259(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root, frozen, V259WindowMapReduceError("launch exists; replay prohibited")
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
            raise V259WindowMapReduceError("v259 turn did not complete")
        usage = _usage(_load_json(paths["sidecar"], "v259 sidecar"))
        if usage["total_tokens"] > MAX_TOTAL_TOKENS:
            raise V259ArchitectureStop("v259 measured turn exceeded frozen token bound")
        normalized, provenance, diagnostics, ownership = project_output(
            result.output, frozen["turn"]
        )
        artifacts = {
            "normalized": root / "normalized-output.private.json",
            "provenance": root / "evidence-provenance.private.json",
            "diagnostics": root / "diagnostics.private.json",
            "ownership": root / "window-ownership-receipt.json",
            "gate": root / "architecture-structural-gate.json",
        }
        _write_immutable(artifacts["normalized"], normalized)
        _write_immutable(artifacts["provenance"], provenance)
        _write_immutable(artifacts["diagnostics"], {"segments": diagnostics})
        _write_immutable(artifacts["ownership"], ownership)
        gate = _gate(usage=usage, diagnostics=diagnostics, ownership=ownership)
        _write_immutable(artifacts["gate"], gate)
        if not gate["passed"]:
            raise V259ArchitectureStop("v259 structural quality or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v259_architecture_structural_gate_passed",
            "terminal_reason": "v259_window_map_reduce_structural_cost_gate_passed",
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
            "gate": _record(artifacts["gate"]),
            "normalized_output": _record(artifacts["normalized"]),
            "evidence_provenance": _record(artifacts["provenance"]),
            "window_ownership_receipt": _record(artifacts["ownership"]),
            "sidecar": _record(paths["sidecar"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": "run frozen side-free source-support audit before alignment or holdout",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v259 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v259 window map/reduce canary")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v259(output_dir=Path(args.output_dir))
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
            run_v259(
                output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds
            )
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "total_tokens": (terminal.get("usage") or {}).get("total_tokens"),
            "production_amortized_total_token_ratio": terminal.get(
                "production_amortized_total_token_ratio"
            ),
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
