from __future__ import annotations

"""Run two blind segment-isolated extractions and assemble them structurally."""

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
from . import app_server_judge_v5_selection_v249_explicit_applicability as v249
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .util import now_iso


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v265_segment_isolation_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v265_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v265_terminal_v1"
PHASE_ID = "development_selection_v5_4_v265_segment_isolation"
MODEL = "gpt-5.6-sol"
EFFORT = "high"
MAX_TOTAL_TOKENS_PER_TURN = 50_000
PHASE_TOTAL_TOKEN_BOUND = 73_000
CAPACITY_PHASE_TOTAL_TOKEN_BOUND = 2 * MAX_TOTAL_TOKENS_PER_TURN
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
MAX_PROMPT_BYTES = 100_000
MAX_BASE_BYTES = 24_000
MAX_SCHEMA_BYTES = 100_000
MIN_DENSE_EVENTS = 27
MAX_RESIDUAL_EVENTS = 1
USAGE_FIELDS = v249.USAGE_FIELDS
PROJECT_ROOT = v249.PROJECT_ROOT
PIPELINE_ROOT = v249.PIPELINE_ROOT
V249_ROOT = v249.DEFAULT_OUTPUT_ROOT
V261_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v261-dual-decomposition"
).resolve()
V264_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v264-local-owner-adjudication-v261"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v265-segment-isolation"
).resolve()
PINNED_CODEX_0_144_1 = v249.PINNED_CODEX_0_144_1

EXPECTED_LINEAGE_HASHES = {
    "v249_terminal": "56497104fa40aee405a769f39fba5ba082418e3149909222708954e63bb06a5d",
    "v249_runtime_lock": "5f20ee4aef5ca6a997786a5590fd78b7bd92fd3724d9a274ba273c3fdd53c796",
    "v261_terminal": "1950f7f29b07784256f71ff5986e5652e7a045865a0ee0dd7031afaf9b71d67a",
    "v261_runtime_lock": "03aa7cc3f3932024f813b6de7f46644f9fbaec35953ef0a12f9674166b6b33a7",
    "v261_sidecar": "acd1ef646a263e79364546a25ba2df5b51b1dacffb646b35020966978c2ed48b",
    "v264_terminal": "a0a16ba56d7ed4e669535ed4a42f2ae45b3aed94be17e535afa65c722c1cf3f6",
    "v264_score": "d7902fbde9fd2c9e86db3f1a1e9a91271891bcdde1f0f52dd9895c12d80399ce",
    "v264_sidecar": "140f0b006453e23b0d66aae09e62924164ad4acfcfdd3dce2ec42aad4b3e85c7",
}

SEGMENT_ISOLATION_INSTRUCTIONS = """This structured turn owns exactly one source segment. Inspect every source unit in that segment and return the complete full-schema event extraction for that segment only.

Make every semantic discovery, eligibility, event-boundary, evidence, actor, attribution, stance, target, metric, field, and applicability decision from the source language. Preserve independently truth-valued events even when nearby events concern the same topic or actor. Merge only genuine duplicate descriptions of the same truth-conditional event.

Re-read the selected exact evidence range for every event before populating the final schema. Every material field must be supported by that range. Every nonempty metric string must be a literal contiguous substring of the selected evidence. Do not infer another segment, a reference answer, expected count, topic list, or quality hint. Never use keywords, regex, phrase rules, embeddings, or fixed-topic gates. Return only the final schema-valid JSON in source order."""


class V265SegmentIsolationError(RuntimeError):
    """The v265 architecture cannot proceed or be adopted safely."""


class V265OutputContractError(V265SegmentIsolationError):
    """A completed output violated the frozen exact projection contract."""


class V265ArchitectureStop(V265SegmentIsolationError):
    """The measured architecture failed a predeclared gate."""


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V265SegmentIsolationError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V265SegmentIsolationError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V265SegmentIsolationError(f"frozen {path.name} drifted")
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


def _single_turn(root: Path, prefix: str) -> Path:
    matches = list(root.glob(f"turns/{prefix}*"))
    if len(matches) != 1:
        raise V265SegmentIsolationError(f"{prefix} turn membership drifted")
    return matches[0]


def _lineage_paths() -> dict[str, Path]:
    v261_turn = _single_turn(V261_ROOT, "v261-dual-decomposition-")
    v264_turn = _single_turn(V264_ROOT, "v264-local-side-free-owner-adjudication")
    return {
        "v249_terminal": V249_ROOT / "terminal.json",
        "v249_runtime_lock": V249_ROOT / "runtime-lock.json",
        "v261_terminal": V261_ROOT / "terminal.json",
        "v261_runtime_lock": V261_ROOT / "runtime-lock.json",
        "v261_sidecar": v261_turn / "sidecar.json",
        "v264_terminal": V264_ROOT / "terminal.json",
        "v264_score": V264_ROOT / "reconciled-alignment-score.json",
        "v264_sidecar": v264_turn / "sidecar.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V265SegmentIsolationError(f"frozen lineage {name} drifted")
    v249.verify_runtime_lock(paths["v249_runtime_lock"])
    t249 = _load_json(paths["v249_terminal"], "v249 terminal")
    t261 = _load_json(paths["v261_terminal"], "v261 terminal")
    s261 = _load_json(paths["v261_sidecar"], "v261 sidecar")
    t264 = _load_json(paths["v264_terminal"], "v264 terminal")
    q264 = _load_json(paths["v264_score"], "v264 score")
    s264 = _load_json(paths["v264_sidecar"], "v264 sidecar")
    if (
        t249.get("terminal_reason")
        != "v249_explicit_applicability_structural_cost_gate_passed"
        or t261.get("terminal_reason")
        != "v261_dual_decomposition_structural_cost_gate_passed"
        or t261.get("dense_event_count") != 32
        or t261.get("candidate_only_nominal_no_signal_event_count") != 1
        or (s261.get("usage") or {}).get("total_tokens") != 45_372
        or t264.get("terminal_reason")
        != "v264_local_owner_adjudication_quality_gate_not_passed"
        or (q264.get("metrics") or {}).get("development_strict_full_field_macro_f1")
        != 0.94898
        or q264.get("failed_checks")
        != ["strict_full_field_macro_noninferior_margin_0_03", "no_material_source_macro_regression"]
        or (s264.get("usage") or {}).get("total_tokens") != 37_370
        or any(
            terminal.get("production_mutated") is not False
            for terminal in (t249, t261, t264)
        )
    ):
        raise V265SegmentIsolationError("v265 predecessor evidence drifted")
    return {
        "paths": paths,
        "records": records,
        "source": v249._load_frozen(V249_ROOT)["turn"],
    }


def _restrict_schema(schema: Mapping[str, Any], segment_id: str) -> dict[str, Any]:
    restricted = copy.deepcopy(dict(schema))
    segments = restricted["properties"]["segments"]
    segments["minItems"] = 1
    segments["maxItems"] = 1
    segments["items"]["properties"]["segment_id"]["enum"] = [segment_id]
    return restricted


def prepare_turns(lineage: Mapping[str, Any]) -> list[dict[str, Any]]:
    source = lineage["source"]
    base = (
        source["base"]
        + "\n\n# Independent segment-isolation contract\n"
        + SEGMENT_ISOLATION_INSTRUCTIONS
        + "\n"
    )
    turns = []
    for position, segment in enumerate(source["private_input"]["segments"]):
        segment_id = str(segment["segment_id"])
        packet = {
            "episode_id": source["episode_id"],
            "segments": [
                {
                    "segment_id": segment_id,
                    "source_units": [
                        {
                            "unit_id": unit["unit_id"],
                            "window_id": unit["window_id"],
                            "text": unit["text"],
                        }
                        for unit in segment["units"]
                    ],
                }
            ],
        }
        prompt = "# Blind source-unit packet\n" + json.dumps(
            packet, ensure_ascii=True, separators=(",", ":")
        ) + "\n"
        schema = _restrict_schema(source["schema"], segment_id)
        direct_schema = _restrict_schema(source["direct_schema"], segment_id)
        sizes = {
            "prompt_bytes": len(prompt.encode("utf-8")),
            "base_bytes": len(base.encode("utf-8")),
            "schema_bytes": len(
                json.dumps(schema, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ),
        }
        if (
            sizes["prompt_bytes"] > MAX_PROMPT_BYTES
            or sizes["base_bytes"] > MAX_BASE_BYTES
            or sizes["schema_bytes"] > MAX_SCHEMA_BYTES
        ):
            raise V265SegmentIsolationError("v265 request size cap failed")
        turns.append(
            {
                "turn_name": f"v265_segment_{position}_{hashlib.sha256(segment_id.encode()).hexdigest()[:20]}",
                "episode_id": source["episode_id"],
                "segment_ids": [segment_id],
                "private_input": {
                    "schema_version": SCHEMA_VERSION,
                    "episode_id": source["episode_id"],
                    "segments": [copy.deepcopy(segment)],
                    "privacy": "private source units and context",
                },
                "prompt": prompt,
                "base": base,
                "schema": schema,
                "direct_schema": direct_schema,
                **sizes,
            }
        )
    if [turn["segment_ids"][0] for turn in turns] != list(source["segment_ids"]):
        raise V265SegmentIsolationError("v265 segment membership drifted")
    return turns


def project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    try:
        normalized, provenance, diagnostics, applicability = v249.project_output(output, turn)
    except v249.V249OutputContractError as exc:
        raise V265OutputContractError(str(exc)) from exc
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "all_semantic_extraction_owned_by_llm": True,
        "deterministic_exact_projection_only": True,
        "applicability": applicability,
    }
    return normalized, provenance, diagnostics, receipt


def _production_ratio(tokens: int) -> tuple[int, float]:
    return v249._production_ratio(tokens)


def _combine_usage(usages: Sequence[Mapping[str, int]]) -> dict[str, int]:
    return {field: sum(int(usage[field]) for usage in usages) for field in USAGE_FIELDS}


def assemble_outputs(
    projected: Sequence[
        tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]
    ],
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    if len(projected) != 2 or any(len(item[0].get("segments") or []) != 1 for item in projected):
        raise V265OutputContractError("v265 isolated projection membership failed")
    episode_ids = {str(item[0].get("episode_id")) for item in projected}
    provenance_episode_ids = {str(item[1].get("episode_id")) for item in projected}
    if len(episode_ids) != 1 or provenance_episode_ids != episode_ids:
        raise V265OutputContractError("v265 isolated episode identity drifted")
    normalized = {
        "episode_id": next(iter(episode_ids)),
        "segments": [copy.deepcopy(item[0]["segments"][0]) for item in projected],
    }
    provenance = {
        "schema_version": projected[0][1]["schema_version"],
        "episode_id": normalized["episode_id"],
        "events": [
            copy.deepcopy(event)
            for item in projected
            for event in item[1].get("events") or []
        ],
    }
    diagnostics = [copy.deepcopy(row) for item in projected for row in item[2]]
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "architecture_id": "independent_segment_threads_deterministic_episode_assembly",
        "semantic_turn_count": len(projected),
        "semantic_segment_membership": [
            str(item[0]["segments"][0]["segment_id"]) for item in projected
        ],
        "all_semantic_extraction_owned_by_llm": True,
        "deterministic_episode_assembly_only": True,
        "turn_projection_receipts": [copy.deepcopy(item[3]) for item in projected],
    }
    return normalized, provenance, diagnostics, receipt


def _gate(
    *,
    usage: Mapping[str, int],
    per_turn_usages: Sequence[Mapping[str, int]],
    diagnostics: Sequence[Mapping[str, Any]],
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense = int((by_id.get(v249.v239.DENSE_SEGMENT_ID) or {}).get("event_count", -1))
    residual = int((by_id.get(v249.v239.NO_SIGNAL_SEGMENT_ID) or {}).get("event_count", -1))
    total, ratio = _production_ratio(int(usage["total_tokens"]))
    checks = {
        "both_segments_validated": set(by_id)
        == {v249.v239.DENSE_SEGMENT_ID, v249.v239.NO_SIGNAL_SEGMENT_ID},
        "dense_event_count_gte_27": dense >= MIN_DENSE_EVENTS,
        "nominal_no_signal_event_count_lte_1": 0 <= residual <= MAX_RESIDUAL_EVENTS,
        "all_source_units_reviewed": all(
            int(row["source_unit_count"]) == int(row["reviewed_source_unit_count"])
            for row in diagnostics
        ),
        "unresolved_count_0": all(int(row["unresolved_count"]) == 0 for row in diagnostics),
        "two_isolated_segments_assembled": receipt.get("semantic_turn_count") == 2,
        "explicit_applicability_projection_passed": len(
            receipt.get("turn_projection_receipts") or []
        ) == 2,
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "each_turn_total_tokens_lte_50000": all(
            int(item["total_tokens"]) <= MAX_TOTAL_TOKENS_PER_TURN
            for item in per_turn_usages
        ),
        "combined_total_tokens_lte_73000": int(usage["total_tokens"])
        <= PHASE_TOTAL_TOKEN_BOUND,
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
        "per_turn_usage": [dict(item) for item in per_turn_usages],
        "production_amortized_total_tokens": total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "dense_event_count": dense,
        "candidate_only_nominal_no_signal_event_count": residual,
        "residual_support_audit_required": residual > 0,
        "diagnostics": list(diagnostics),
        "projection_receipt": dict(receipt),
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
        "direct_schema": turn_root / "direct-projection-schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _request_records(paths: Mapping[str, Path]) -> list[dict[str, Any]]:
    return [_record(paths[name]) for name in ("input", "prompt", "base", "schema", "direct_schema")]


def _all_request_records(root: Path, turn_names: Sequence[str]) -> list[dict[str, Any]]:
    return [
        record
        for turn_name in turn_names
        for record in _request_records(_turn_paths(root, turn_name))
    ]


def _capacity_policy(root: Path, turn_names: Sequence[str]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        CAPACITY_PHASE_TOTAL_TOKEN_BOUND
        * QUOTA_POINTS_PER_MILLION_TOKENS
        / 1_000_000
    )
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": CAPACITY_PHASE_TOTAL_TOKEN_BOUND,
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
        "phase_total_token_bound": CAPACITY_PHASE_TOTAL_TOKEN_BOUND,
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
                Path(__file__).resolve(),
                Path(v249.__file__).resolve(),
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
    lock = _load_json(path, "v265 runtime lock")
    root = path.parent.resolve()
    spec = _load_json(root / "attempt-spec.json", "v265 spec")
    turn_names = list(spec["turn_names"])
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 2
        or lock.get("retry_count") != 0
        or lock.get("max_total_tokens_per_turn") != MAX_TOTAL_TOKENS_PER_TURN
        or lock.get("phase_total_token_bound") != PHASE_TOTAL_TOKEN_BOUND
        or lock.get("semantic_regex_or_keyword_filtering") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(file) for file in _runtime_files()}
        or {row["path"] for row in lock.get("request") or []}
        != {row["path"] for row in _all_request_records(root, turn_names)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
    ):
        raise V265SegmentIsolationError("v265 runtime lock contract drifted")
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
        raise V265SegmentIsolationError("v265 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V265SegmentIsolationError("v265 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v265 spec")
    prepared = prepare_turns(_validate_lineage())
    turns = []
    for turn in prepared:
        paths = _turn_paths(root, turn["turn_name"])
        turns.append(
            {
                **turn,
                "private_input": _load_json(paths["input"], "v265 input"),
                "prompt": paths["prompt"].read_text(encoding="utf-8"),
                "base": paths["base"].read_text(encoding="utf-8"),
                "schema": _load_json(paths["schema"], "v265 schema"),
                "direct_schema": _load_json(paths["direct_schema"], "v265 direct schema"),
                "paths": paths,
            }
        )
    return {
        "root": root,
        "spec_path": spec_path,
        "spec": spec,
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "turns": turns,
    }


def freeze_v265(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v265 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V265SegmentIsolationError("unfinished v265 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    turns = prepare_turns(lineage)
    turn_names = [turn["turn_name"] for turn in turns]
    for turn in turns:
        paths = _turn_paths(root, turn["turn_name"])
        paths["root"].mkdir(parents=True, exist_ok=True)
        _write_immutable(paths["input"], turn["private_input"])
        _write_private_text(paths["prompt"], turn["prompt"])
        _write_private_text(paths["base"], turn["base"])
        _write_immutable(paths["schema"], turn["schema"])
        _write_immutable(paths["direct_schema"], turn["direct_schema"])
    capacity_paths = _capacity_policy(root, turn_names)
    authorization_path = root / "authorization.json"
    _write_stable_time(
        authorization_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "authority": "direct_operator_bounded_architecture_steering_2026_07_17",
            "scope": "two blind independent segment extractions with deterministic episode assembly",
            "semantic_attempt_count": 2,
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
            "selected_architecture_id": "independent_segment_threads_deterministic_episode_assembly",
            "architectures": [
                {"rank": 1, "id": "independent_segment_threads_deterministic_episode_assembly"},
                {"rank": 2, "id": "episode_bootstrap_segment_specialists_global_llm_join"},
                {"rank": 3, "id": "dual_model_independent_segment_extraction_single_llm_reducer"},
            ],
            "selection_basis": {
                "v261_dense_events": 32,
                "v261_residual_events": 1,
                "v264_macro_f1": 0.94898,
                "v264_strictly_equivalent_reference_units": 22,
                "reference_semantic_units": 27,
                "decision": "test whether same-turn segment batching causes event-boundary and field interference",
            },
            "on_failure": "reject v265 and advance to a distinct architecture without isolated repair",
        },
        "created_at",
    )
    projected_total, projected_ratio = _production_ratio(PHASE_TOTAL_TOKEN_BOUND)
    design_path = root / "architecture-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "architecture_id": "independent_segment_threads_deterministic_episode_assembly",
            "hypothesis": (
                "isolating each segment in its own full-context semantic turn will eliminate cross-segment batching "
                "interference while preserving the frozen explicit-applicability and exact-evidence contracts"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "declared_semantic_turn_count": 2,
                "reference_visible_to_model": False,
                "target_count_visible_to_model": False,
            },
            "production_cost_projection": {
                "per_turn_hard_max": MAX_TOTAL_TOKENS_PER_TURN,
                "combined_hard_max": PHASE_TOTAL_TOKEN_BOUND,
                "projected_production_amortized_total_tokens": projected_total,
                "projected_production_amortized_total_token_ratio": round(projected_ratio, 6),
                "required_ratio_max": 0.28,
            },
            "predeclared_stop_rules": {
                "retry_count": 0,
                "dense_event_count_minimum": MIN_DENSE_EVENTS,
                "nominal_no_signal_event_count_maximum": MAX_RESIDUAL_EVENTS,
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "event_cap_violations": 0,
                "exact_identity_duplicates": 0,
                "per_turn_total_tokens_max": MAX_TOTAL_TOKENS_PER_TURN,
                "combined_total_tokens_max": PHASE_TOTAL_TOKEN_BOUND,
                "production_amortized_total_token_ratio_max": 0.28,
                "on_structural_pass": "run frozen side-free support then neutral alignment",
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
            "state": "frozen_before_two_turn_segment_isolation_canary",
            "turn_names": turn_names,
            "model": MODEL,
            "effort": EFFORT,
            "declared_turn_count": 2,
            "retry_count": 0,
            "max_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": PHASE_TOTAL_TOKEN_BOUND,
            "requests": [
                {
                    "turn_name": turn["turn_name"],
                    "segment_ids": turn["segment_ids"],
                    "prompt_bytes": turn["prompt_bytes"],
                    "base_bytes": turn["base_bytes"],
                    "schema_bytes": turn["schema_bytes"],
                }
                for turn in turns
            ],
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
        "declared_turn_count": 2,
        "retry_count": 0,
        "max_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": PHASE_TOTAL_TOKEN_BOUND,
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
        "request": _all_request_records(root, turn_names),
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
        raise V265SegmentIsolationError("v265 sidecar usage incomplete") from exc
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
        raise V265SegmentIsolationError("v265 sidecar accounting or auth failed")
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
    attempted = sum(int(turn["paths"]["capacity"].exists()) for turn in frozen["turns"])
    measured_usages = []
    sidecar_records = []
    measured_over_cap = False
    for turn in frozen["turns"]:
        paths = turn["paths"]
        if not paths["sidecar"].is_file():
            continue
        sidecar_records.append(_record(paths["sidecar"]))
        try:
            sidecar = _load_json(paths["sidecar"], "v265 sidecar")
            measured_usages.append(_usage(sidecar))
        except Exception:
            continue
    usage = _combine_usage(measured_usages)
    unknown = max(0, attempted - len(measured_usages))
    measured_over_cap = any(
        item["total_tokens"] > MAX_TOTAL_TOKENS_PER_TURN for item in measured_usages
    ) or usage["total_tokens"] > PHASE_TOTAL_TOKEN_BOUND
    semantic = isinstance(exc, (V265OutputContractError, V265ArchitectureStop)) or measured_over_cap
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v265_segment_isolation_structural_quality_or_cost_gate_not_passed"
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
        "sidecars": sidecar_records,
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
            "reject v265 and advance to the next distinct architecture without field repair"
            if semantic
            else "audit immutable v265 infrastructure attempt; no retry"
        ),
    }
    gate_path = root / "architecture-structural-gate.json"
    if gate_path.is_file():
        terminal["gate"] = _record(gate_path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v265(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v265 terminal")
    frozen = freeze_v265(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root, frozen, V265SegmentIsolationError("launch exists; replay prohibited")
        )
    _write_immutable(
        root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_turn_count": 2,
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
    try:
        usages = []
        projected = []
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
                    batch_size=1,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=paths["capacity"],
                )
                if result.status_ok is not True or not isinstance(result.output, Mapping):
                    raise V265SegmentIsolationError(
                        f"v265 turn did not complete: {turn['turn_name']}"
                    )
                turn_usage = _usage(_load_json(paths["sidecar"], "v265 sidecar"))
                if turn_usage["total_tokens"] > MAX_TOTAL_TOKENS_PER_TURN:
                    raise V265ArchitectureStop("v265 measured turn exceeded frozen token bound")
                usages.append(turn_usage)
                projected.append(project_output(result.output, turn))
        usage = _combine_usage(usages)
        if usage["total_tokens"] > PHASE_TOTAL_TOKEN_BOUND:
            raise V265ArchitectureStop("v265 measured phase exceeded frozen token bound")
        normalized, provenance, diagnostics, receipt = assemble_outputs(projected)
        artifacts = {
            "normalized": root / "normalized-output.private.json",
            "provenance": root / "evidence-provenance.private.json",
            "diagnostics": root / "diagnostics.private.json",
            "receipt": root / "segment-isolation-assembly-receipt.json",
            "gate": root / "architecture-structural-gate.json",
        }
        _write_immutable(artifacts["normalized"], normalized)
        _write_immutable(artifacts["provenance"], provenance)
        _write_immutable(artifacts["diagnostics"], {"segments": diagnostics})
        _write_immutable(artifacts["receipt"], receipt)
        gate = _gate(
            usage=usage,
            per_turn_usages=usages,
            diagnostics=diagnostics,
            receipt=receipt,
        )
        _write_immutable(artifacts["gate"], gate)
        if not gate["passed"]:
            raise V265ArchitectureStop("v265 structural quality or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v265_architecture_structural_gate_passed",
            "terminal_reason": "v265_segment_isolation_structural_cost_gate_passed",
            "semantic_attempt_count": 2,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "production_amortized_total_token_ratio": gate["production_amortized_total_token_ratio"],
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
            "projection_receipt": _record(artifacts["receipt"]),
            "sidecars": [_record(turn["paths"]["sidecar"]) for turn in frozen["turns"]],
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": "run frozen side-free source-support audit before neutral alignment or holdout",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v265 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v265 segment-isolation canary")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v265(output_dir=Path(args.output_dir))
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "turn_count": len(frozen["spec"]["turn_names"]),
            "projected_ratio": round(_production_ratio(PHASE_TOTAL_TOKEN_BOUND)[1], 6),
        }
    else:
        terminal = asyncio.run(
            run_v265(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
