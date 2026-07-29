from __future__ import annotations

"""Run the bounded v237 opaque exact-coverage gap extraction canary."""

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
from . import app_server_judge_v5_selection_v234_two_pass_blind_inventory as v234
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v237_opaque_gap_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v237_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v237_terminal_v1"
PHASE_ID = "development_selection_v5_4_v237_opaque_exact_coverage_gap"
MODEL = "gpt-5.6-sol"
EFFORT = "low"
MAX_TOTAL_TOKENS = 35_000
MAX_EVENTS_PER_SEGMENT = 32
MAX_PROMPT_BYTES = 90_000
MAX_BASE_BYTES = 20_000
MAX_SCHEMA_BYTES = 40_000
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
BASELINE_END_TO_END_TOKENS = 10_065_426
BASE_PRODUCTION_AMORTIZED_TOKENS = 1_650_538
PRODUCTION_SCALE = 30
DENSE_SEGMENT_ID = v234.DENSE_SEGMENT_ID
NO_SIGNAL_SEGMENT_ID = v234.NO_SIGNAL_SEGMENT_ID
MIN_DENSE_FINAL_EVENTS = v234.MIN_DENSE_PROPOSITIONS
USAGE_FIELDS = v234.USAGE_FIELDS
IDENTITY_FIELDS = v232.IDENTITY_FIELDS
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
SOURCE_TURN_ROOT = v234.SOURCE_TURN_ROOT
V220_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v220-fresh-integrated-base-design"
).resolve()
V236_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v236-core-then-metadata"
).resolve()
V236_TURN_ROOT = (
    V236_ROOT / "turns/v236-sol-core-d7c914bc1cee2c432b36"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v237-opaque-coverage-gap"
).resolve()
PINNED_CODEX_0_144_1 = v234.PINNED_CODEX_0_144_1
EXPECTED_LINEAGE_HASHES = {
    "source_input": "71433895667434bedfa9f1b5fd57d7c3400d5f8a94e64f4e2c6efc78af8a8332",
    "source_base": "6b66e1ca9d00cf6558b84020b9879a3b86644b449f7498daea9a2c9837b4b0fc",
    "source_schema": "bd6b128cba67cc77700cc063778406c120107fde2cdadfa435d93eafe824db00",
    "v220_design": "1358fd2da7af12d62e61fdebec847321783dae847bdf21368951ae9b701276bc",
    "v236_terminal": "5d6d0a77a0c55992a437735bbc311c1daa279febcd43b03a731ef55a8443b603",
    "v236_sidecar": "6b69d3ef3680dbbc2f63bf09214426580d993b90c8f01cbd09dc1765415ce528",
    "v236_output": "e87dcfba8101f7dc14238291d8f85c36cb46c885b9915dd26047f2c6be330508",
}


class V237OpaqueGapError(RuntimeError):
    """The v237 opaque-gap architecture cannot proceed safely."""


class V237OutputContractError(V237OpaqueGapError):
    """A completed output violated the frozen gap contract."""


class V237ArchitectureStop(V237OpaqueGapError):
    """The architecture cannot reach the frozen development gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V237OpaqueGapError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V237OpaqueGapError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V237OpaqueGapError(f"frozen {path.name} drifted")
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
    return {"path": str(resolved), "sha256": hashlib.sha256(data).hexdigest(), "size_bytes": len(data)}


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
        "v220_design": V220_ROOT / "integrated-base-design.json",
        "v236_terminal": V236_ROOT / "terminal.json",
        "v236_sidecar": V236_TURN_ROOT / "sidecar.json",
        "v236_output": V236_TURN_ROOT / "output.private.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V237OpaqueGapError(f"frozen lineage {name} drifted")
    design = _load_json(paths["v220_design"], "v220 design")
    terminal = _load_json(paths["v236_terminal"], "v236 terminal")
    sidecar = _load_json(paths["v236_sidecar"], "v236 sidecar")
    output = _load_json(paths["v236_output"], "v236 output")
    counts = [
        (len(row.get("events") or []), sum(item["eligible_event_count"] for item in row.get("unit_receipts") or []))
        for row in output.get("segments") or []
    ]
    if (
        design.get("cost_bound", {}).get("base_production_amortized_total_tokens")
        != BASE_PRODUCTION_AMORTIZED_TOKENS
        or terminal.get("terminal_reason")
        != "v236_core_metadata_structural_or_count_gate_not_passed"
        or terminal.get("next_distinct_architecture_authorized") is not True
        or terminal.get("production_mutated") is not False
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or counts != [(21, 22), (1, 1)]
    ):
        raise V237OpaqueGapError("v236 predecessor or base cost state drifted")
    return {"paths": paths, "records": records}


def _event_span(text: str, event: Mapping[str, Any]) -> tuple[int, int]:
    evidence = str(event.get("evidence") or "")
    if not evidence or text.count(evidence) != 1:
        raise V237OpaqueGapError("base event evidence is not uniquely projectable")
    start = text.index(evidence)
    return start, start + len(evidence)


def _prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    paths = lineage["paths"]
    source = _load_json(paths["source_input"], "source input")
    source_schema = _load_json(paths["source_schema"], "source schema")
    context = v232._shared_context(paths["source_base"])
    episode_id = str(source["episode_id"])
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
        base_events = [dict(event) for event in row.get("existing_events") or []]
        base_spans = []
        base_identities = set()
        for index, event in enumerate(base_events):
            start, end = _event_span(text, event)
            metric_values = [
                str(event.get(field) or "")
                for field in (
                    "metric_value",
                    "metric_unit",
                    "metric_comparator",
                    "metric_raw_text",
                )
            ]
            if any(value and value not in str(event["evidence"]) for value in metric_values):
                raise V237OpaqueGapError("base event metric is not literally grounded")
            identity = _canonical_json(
                {field: event.get(field) for field in IDENTITY_FIELDS}
            )
            if identity in base_identities:
                raise V237OpaqueGapError("base event identity duplicate")
            base_identities.add(identity)
            base_spans.append({"base_event_id": f"B{index:03d}", "start_char": start, "end_char": end})
        coverage_by_unit = {}
        prompt_units = []
        for unit in units:
            unit_id = str(unit["unit_id"])
            all_unit_ids.append(unit_id)
            coverage = [
                span["base_event_id"]
                for span in base_spans
                if int(unit["start_char"]) < int(span["end_char"])
                and int(span["start_char"]) < int(unit["end_char"])
            ]
            coverage_by_unit[unit_id] = coverage
            prompt_units.append({
                "unit_id": unit_id, "window_id": unit["window_id"],
                "base_coverage_ids": coverage, "text": unit["text"],
            })
        prompt_segments.append({"segment_id": segment_id, "source_units": prompt_units})
        private_segments.append({
            "segment_id": segment_id, "segment_text": text, "boundaries": boundaries,
            "units": units, "density_stratum": row["density_stratum"],
            "base_events": base_events, "base_spans": base_spans,
            "coverage_by_unit": coverage_by_unit,
        })
    if segment_ids != [DENSE_SEGMENT_ID, NO_SIGNAL_SEGMENT_ID]:
        raise V237OpaqueGapError("canary segment membership drifted")
    prompt = "# Opaque exact-coverage source packet\n" + json.dumps(
        {"episode_id": episode_id, "segments": prompt_segments},
        ensure_ascii=True, separators=(",", ":")
    ) + "\n"
    base = GAP_INSTRUCTIONS + "\n\n# Immutable episode context\n" + json.dumps(
        context, ensure_ascii=True, separators=(",", ":")
    ) + "\n"
    schema = v233._output_schema(
        episode_id=episode_id, segment_ids=segment_ids, source_schema=source_schema,
        unit_ids=all_unit_ids,
        max_unit_count=max(len(row["units"]) for row in private_segments),
    )
    prompt_bytes = len(prompt.encode("utf-8"))
    base_bytes = len(base.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if (
        prompt_bytes > MAX_PROMPT_BYTES
        or base_bytes > MAX_BASE_BYTES
        or schema_bytes > MAX_SCHEMA_BYTES
    ):
        raise V237OpaqueGapError("v237 request size cap exceeded")
    return {
        "turn_name": "v237_opaque_gap_" + sha256_text(episode_id)[:20],
        "episode_id": episode_id, "segment_ids": segment_ids,
        "source_schema": source_schema,
        "private_input": {"schema_version": SCHEMA_VERSION, "episode_id": episode_id, "segments": private_segments},
        "prompt": prompt, "base": base, "schema": schema,
        "prompt_bytes": prompt_bytes,
        "base_bytes": base_bytes,
        "schema_bytes": schema_bytes,
    }


GAP_INSTRUCTIONS = """You are a topic-general semantic gap extractor for a private podcast research corpus. A separate frozen base extractor already owns some exact source spans. You cannot see any base event semantics, reference answer, density label, target count, expected topic, or quality hint. base_coverage_ids are opaque exact-span ownership markers only; never infer event meaning from an ID.

Read every source unit. Extract every distinct research-useful atomic proposition whose smallest self-contained evidence begins in a source unit with base_coverage_ids=[]. A new proposition may include adjacent covered units for attribution or coreference, but its evidence_start_unit_id must be uncovered. Do not emit a proposition already wholly owned by covered source. Do not invent an event to fill a receipt. Make all eligibility, boundary, and semantic decisions from language meaning, never keywords, regex, fixed topics, or counts.

Split independently truth-conditional premises, mechanisms, capabilities, constraints, comparisons, outcomes, alternatives, frames, uncertainties, counterclaims, product signals, market signals, risks, adoption signals, term uses, and stance positions. Do not extract greetings, logistics, acknowledgements, unasserted questions, or bare mentions without a research-relevant relationship, identity, position, or claim.

Return one unit_receipt per source unit in exact input order. Counts cover only newly extracted gap events whose evidence begins at that unit. unresolved_count must be zero, receipt totals must equal event totals, and coverage_audit must confirm every unit was reviewed.

Select the smallest contiguous evidence-unit range supporting every material field. Populate the full schema. Every nonempty metric string must be a literal substring of selected evidence; metric_direction=not_applicable if and only if all metric strings are empty. Order events by source evidence. Use coded only when new gap events are returned; otherwise no_signal. Return schema-valid JSON only."""


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
    return {
        "root": turn_root, "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md", "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json", "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json", "output": turn_root / "output.private.json",
        "gap_normalized": turn_root / "gap-normalized.private.json",
        "normalized": turn_root / "normalized-output.private.json",
        "provenance": turn_root / "evidence-provenance.private.json",
        "diagnostics": turn_root / "diagnostics.private.json",
    }


def _capacity_policy(root: Path, turn_name: str) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(MAX_TOTAL_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20", "phase_id": PHASE_ID,
        "created_at": now_iso(), "production_mutation_performed": False,
        "measured_basis": {
            "declared_turn_count": 1, "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
            "phase_total_token_bound": MAX_TOTAL_TOKENS,
            "base_production_amortized_tokens": BASE_PRODUCTION_AMORTIZED_TOKENS,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": "pif_app_server_capacity_policy_v20", "phase_id": PHASE_ID,
        "created_at": now_iso(), "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True, "retry_count_per_turn": 0,
        "production_mutation_allowed": False, "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True, "ordered_turn_names": [turn_name],
        "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
        "phase_total_token_bound": MAX_TOTAL_TOKENS,
        "projected_phase_quota_points": projected, "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    reserve.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _runtime_files() -> tuple[Path, ...]:
    return tuple(sorted({
        Path(__file__).resolve(), Path(v234.__file__).resolve(), Path(v233.__file__).resolve(),
        Path(v232.__file__).resolve(), Path(capacity.__file__).resolve(), Path(reserve.__file__).resolve(),
        Path(codex_app_server.__file__).resolve(), Path(labels_module.__file__).resolve(),
        Path(util_module.__file__).resolve(), codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
    }, key=str))


def _request_records(root: Path, turn_name: str) -> list[dict[str, Any]]:
    paths = _turn_paths(root, turn_name)
    return [_record(paths[name]) for name in ("input", "prompt", "base", "schema")]


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v237 runtime lock")
    root = path.parent.resolve()
    turn_name = str(lock.get("turn_name") or "")
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1 or lock.get("retry_count") != 0
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(path) for path in _runtime_files()}
        or {row["path"] for row in lock.get("frozen_request") or []}
        != {row["path"] for row in _request_records(root, turn_name)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V237OpaqueGapError("v237 runtime lock contract drifted")
    records = [lock.get("pinned_codex_cli"), *(lock.get("runtime_files") or []),
               *(lock.get("direct_lineage") or []), lock.get("authorization"),
               lock.get("ranking"), lock.get("design"), lock.get("spec"),
               lock.get("capacity_audit"), lock.get("capacity_policy"),
               *(lock.get("frozen_request") or [])]
    if any(not _verify_record(row or {}) for row in records):
        raise V237OpaqueGapError("v237 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {row["path"] for row in lineage["records"].values()}:
        raise V237OpaqueGapError("v237 lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v237 spec")
    paths = _turn_paths(root, spec["turn_name"])
    return {
        "root": root, "spec": spec, "spec_path": spec_path,
        "design_path": root / "architecture-design.json", "ranking_path": root / "architecture-ranking.json",
        "runtime_lock": root / "runtime-lock.json", "capacity_policy": root / "capacity-policy.json",
        "turn": {
            "turn_name": spec["turn_name"], "episode_id": spec["episode_id"],
            "segment_ids": spec["segment_ids"], "paths": paths,
            "private_input": _load_json(paths["input"], "v237 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v237 schema"),
        },
    }


def freeze_v237(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v237 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V237OpaqueGapError("unfinished v237 root is not replayable")
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
    authorization = {"schema_version": SCHEMA_VERSION, "created_at": now_iso(),
                     "authority": "direct_operator_steering_2026_07_17",
                     "scope": "bounded opaque exact-coverage gap architecture",
                     "holdout_authorized": False, "production_mutation_allowed": False}
    authorization_path = root / "authorization.json"
    _write_stable_time(authorization_path, authorization, "created_at")
    ranking = {"schema_version": SCHEMA_VERSION, "created_at": now_iso(),
               "selected_architecture_id": "exact_coverage_mask_gap_without_semantic_base_anchor",
               "architectures": [
                   {"rank": 1, "id": "exact_coverage_mask_gap_without_semantic_base_anchor"},
                   {"rank": 2, "id": "specialist_ensemble_with_llm_consolidation"},
                   {"rank": 3, "id": "full_schema_frontier_single_pass"},
               ],
               "on_failure": "freeze and advance to another distinct architecture"}
    ranking_path = root / "architecture-ranking.json"
    _write_stable_time(ranking_path, ranking, "created_at")
    projected_total = BASE_PRODUCTION_AMORTIZED_TOKENS + MAX_TOTAL_TOKENS * PRODUCTION_SCALE
    base_counts = {row["segment_id"]: len(row["base_events"]) for row in turn["private_input"]["segments"]}
    design = {
        "schema_version": SCHEMA_VERSION, "created_at": now_iso(), "phase_id": PHASE_ID,
        "architecture_id": "exact_coverage_mask_gap_without_semantic_base_anchor",
        "hypothesis": (
            "opaque exact-span ownership will remove semantic base anchoring while focusing "
            "Sol on uncovered propositions, adding at least 13 dense events to the frozen 11-event base"
        ),
        "representative_canary": {"episode_count": 1, "segment_count": 2,
                                  "base_event_counts": base_counts,
                                  "base_semantics_visible_to_model": False},
        "production_cost_projection": {
            "base_production_amortized_tokens": BASE_PRODUCTION_AMORTIZED_TOKENS,
            "gap_turn_hard_max": MAX_TOTAL_TOKENS, "production_scale": PRODUCTION_SCALE,
            "projected_production_amortized_total_tokens": projected_total,
            "projected_production_amortized_total_token_ratio": round(projected_total / BASELINE_END_TO_END_TOKENS, 6),
            "required_ratio_max": 0.28,
        },
        "predeclared_stop_rules": {
            "retry_count": 0, "all_source_units_reviewed": True, "unresolved_count": 0,
            "gap_evidence_start_owned_by_base": 0, "dense_final_event_count_minimum": MIN_DENSE_FINAL_EVENTS,
            "exact_evidence_rate": 1.0, "metric_grounding_error_events": 0,
            "event_cap_violations": 0, "exact_identity_duplicates": 0,
            "candidate_only_residuals": "frozen side-free support audit; no automatic false positive",
            "production_amortized_total_token_ratio_max": 0.28,
        },
        "holdout_authorized": False, "production_mutation_allowed": False,
    }
    design_path = root / "architecture-design.json"
    _write_stable_time(design_path, design, "created_at")
    request = _request_records(root, turn["turn_name"])
    spec = {"schema_version": SCHEMA_VERSION, "created_at": now_iso(), "phase_id": PHASE_ID,
            "state": "frozen_before_one_turn_opaque_gap_canary", "declared_turn_count": 1,
            "turn_name": turn["turn_name"], "episode_id": turn["episode_id"],
            "segment_ids": turn["segment_ids"], "model": MODEL, "effort": EFFORT,
            "retry_count": 0, "maximum_total_tokens": MAX_TOTAL_TOKENS,
            "base_semantics_visible_to_model": False, "reference_visible_to_model": False,
            "density_visible_to_model": False, "holdout_authorized": False,
            "production_mutation_allowed": False, "authorization": _record(authorization_path),
            "ranking": _record(ranking_path), "design": _record(design_path),
            "direct_lineage": lineage["records"], "frozen_request": request,
            "capacity_audit": _record(capacity_paths["audit"]),
            "capacity_policy": _record(capacity_paths["policy"])}
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    lock = {"schema_version": RUNTIME_LOCK_VERSION, "created_at": now_iso(), "phase_id": PHASE_ID,
            "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
            "runtime_files": [_record(path) for path in _runtime_files()],
            "direct_lineage": list(lineage["records"].values()),
            "authorization": _record(authorization_path), "ranking": _record(ranking_path),
            "design": _record(design_path), "spec": _record(spec_path),
            "capacity_audit": _record(capacity_paths["audit"]),
            "capacity_policy": _record(capacity_paths["policy"]), "frozen_request": request,
            "turn_name": turn["turn_name"], "model": MODEL, "effort": EFFORT,
            "declared_turn_count": 1, "retry_count": 0, "holdout_authorized": False,
            "production_mutation_allowed": False}
    _write_stable_time(root / "runtime-lock.json", lock, "created_at")
    verify_runtime_lock(root / "runtime-lock.json")
    return _load_frozen(root)


def _merge_gap(
    *, gap: Mapping[str, Any], provenance: Mapping[str, Any], turn: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    source_by_id = {str(row["segment_id"]): row for row in turn["private_input"]["segments"]}
    provenance_by_segment: dict[str, list[Mapping[str, Any]]] = {}
    for row in provenance["events"]:
        provenance_by_segment.setdefault(str(row["segment_id"]), []).append(row)
    merged_rows = []
    diagnostics = []
    source_event_schema = _load_json(SOURCE_TURN_ROOT / "schema.json", "source schema")["properties"]["segments"]["items"]["properties"]["events"]["items"]
    for gap_row in gap["segments"]:
        segment_id = str(gap_row["segment_id"])
        source = source_by_id[segment_id]
        gap_events = list(gap_row["events"])
        gap_provenance = sorted(
            provenance_by_segment.get(segment_id, []),
            key=lambda row: int(row["event_index"]),
        )
        if len(gap_events) != len(gap_provenance):
            raise V237OutputContractError("gap provenance count drifted")
        for event_index, row in enumerate(gap_provenance):
            if int(row["event_index"]) != event_index:
                raise V237OutputContractError("gap provenance index drifted")
            start_id = str(row["evidence_start_unit_id"])
            if source["coverage_by_unit"].get(start_id):
                raise V237OutputContractError("gap event starts in base-owned source")
        combined: list[tuple[int, int, dict[str, Any]]] = []
        seen = set()
        for origin_order, events in enumerate((source["base_events"], gap_events)):
            for event_index, event in enumerate(events):
                value = dict(event)
                try:
                    _validate_schema(source_event_schema, value, path="$.event")
                except (ValidationError, ValueError, TypeError) as exc:
                    raise V237OutputContractError("merged event schema failed") from exc
                identity = _canonical_json({field: value.get(field) for field in IDENTITY_FIELDS})
                if identity in seen:
                    raise V237OutputContractError("exact base-gap event duplicate")
                seen.add(identity)
                if origin_order == 0:
                    start, _ = _event_span(str(source["segment_text"]), value)
                else:
                    event_provenance = gap_provenance[event_index]
                    start = int(event_provenance["start_char"])
                    end = int(event_provenance["end_char"])
                    if value.get("evidence") != str(source["segment_text"])[start:end]:
                        raise V237OutputContractError("gap evidence projection drifted")
                combined.append((start, origin_order, value))
        combined.sort(key=lambda item: (item[0], item[1]))
        if len(combined) > MAX_EVENTS_PER_SEGMENT:
            raise V237OutputContractError("merged event cap exceeded")
        events = [item[2] for item in combined]
        merged_rows.append({
            "segment_id": segment_id, "status": "coded" if events else "no_signal",
            "segment_source_context": gap_row["segment_source_context"],
            "no_signal_reason": "" if events else gap_row["no_signal_reason"],
            "events": events,
        })
        diagnostics.append({
            "segment_id": segment_id, "density_stratum": source["density_stratum"],
            "base_event_count": len(source["base_events"]), "gap_event_count": len(gap_events),
            "final_event_count": len(events), "gap_owned_start_violation_count": 0,
            "exact_evidence_violation_count": 0,
            "metric_grounding_error_event_count": 0,
            "event_cap_violation_count": 0,
            "exact_identity_duplicate_count": 0,
        })
    return {"episode_id": turn["episode_id"], "segments": merged_rows}, diagnostics


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    usage = sidecar.get("usage")
    if not isinstance(usage, Mapping):
        raise V237OpaqueGapError("turn usage absent")
    result = {}
    for field in USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise V237OpaqueGapError("turn usage incomplete")
        result[field] = value
    if (sidecar.get("state") != "completed" or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured" or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt" or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != MODEL or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None or result["total_tokens"] > MAX_TOTAL_TOKENS):
        raise V237OpaqueGapError("measured sidecar contract failed")
    return result


def _gate(*, usage: Mapping[str, int], diagnostics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense = int(by_id.get(DENSE_SEGMENT_ID, {}).get("final_event_count", -1))
    residual = int(by_id.get(NO_SIGNAL_SEGMENT_ID, {}).get("final_event_count", -1))
    production_total = BASE_PRODUCTION_AMORTIZED_TOKENS + int(usage["total_tokens"]) * PRODUCTION_SCALE
    ratio = production_total / BASELINE_END_TO_END_TOKENS
    checks = {
        "both_segments_validated": set(by_id) == {DENSE_SEGMENT_ID, NO_SIGNAL_SEGMENT_ID},
        "gap_owned_start_violations_0": all(row["gap_owned_start_violation_count"] == 0 for row in diagnostics),
        "dense_final_event_count_gte_24": dense >= MIN_DENSE_FINAL_EVENTS,
        "exact_evidence_rate_1": all(
            row["exact_evidence_violation_count"] == 0 for row in diagnostics
        ),
        "metric_grounding_error_events_0": all(
            row["metric_grounding_error_event_count"] == 0 for row in diagnostics
        ),
        "event_cap_violations_0": all(
            row["event_cap_violation_count"] == 0 for row in diagnostics
        ),
        "exact_identity_duplicates_0": all(
            row["exact_identity_duplicate_count"] == 0 for row in diagnostics
        ),
        "total_tokens_lte_35000": int(usage["total_tokens"]) <= MAX_TOTAL_TOKENS,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {"schema_version": SCHEMA_VERSION, "phase_id": PHASE_ID, "passed": not failed,
            "checks": checks, "failed_checks": failed, "usage": dict(usage),
            "diagnostics": list(diagnostics), "dense_final_event_count": dense,
            "candidate_only_nominal_no_signal_event_count": residual,
            "residual_support_audit_required": residual > 0,
            "production_amortized_total_tokens": production_total,
            "production_amortized_total_token_ratio": round(ratio, 6),
            "support_alignment_authorized": not failed, "development_winner_frozen": False,
            "holdout_authorized": False, "production_mutated": False}


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(command=[str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"])


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(policy_path=policy_path, inner_factory=_inner_factory)


def _failure_terminal(root: Path, frozen: Mapping[str, Any], exc: BaseException) -> dict[str, Any]:
    paths = frozen["turn"]["paths"]
    attempted = int(paths["capacity"].exists())
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = attempted
    sidecar_record = None
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        try:
            raw = _load_json(paths["sidecar"], "sidecar")
            values = raw.get("usage") or {}
            usage = {field: int(values[field]) for field in USAGE_FIELDS}
            unknown = 0
        except Exception:
            unknown = 1
    semantic = isinstance(exc, (V237OutputContractError, V237ArchitectureStop))
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {"schema_version": TERMINAL_VERSION, "terminal_at": now_iso(),
                "state": "inactive_incomplete_recovery_required",
                "terminal_reason": "v237_opaque_gap_structural_or_count_gate_not_passed" if semantic else "infrastructure_or_judge_attempt_failed",
                "error_class": type(exc).__name__, "error_message_sha256": hashlib.sha256(message).hexdigest(),
                "error_message_bytes": len(message), "semantic_attempt_count": attempted,
                "semantic_retry_count": 0, "usage_status": "unknown" if unknown else "complete",
                "accounting_complete": unknown == 0, "usage": usage,
                "unknown_usage_attempt_count": unknown, "sidecar": sidecar_record,
                "architecture_strategy_rejected": semantic,
                "next_distinct_architecture_authorized": semantic,
                "isolated_field_repair_authorized": False, "support_alignment_authorized": False,
                "development_winner_frozen": False, "holdout_authorized": False,
                "production_mutated": False, "overall_goal_complete": False,
                "goal_status_required": "active", "runtime_lock": _record(frozen["runtime_lock"]),
                "attempt_spec": _record(frozen["spec_path"]),
                "exact_next_action": "advance to another distinct architecture" if semantic else "audit immutable infrastructure attempt; no retry"}
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v237(*, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
                   client_factory: Callable[[Path], Any] = _client_factory) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v237 terminal")
    frozen = freeze_v237(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(root, frozen, V237OpaqueGapError("launch exists; replay prohibited"))
    _write_immutable(root / "launch-receipt.json", {"schema_version": SCHEMA_VERSION, "launched_at": now_iso(),
                     "phase_id": PHASE_ID, "turn_name": frozen["turn"]["turn_name"],
                     "declared_turn_count": 1, "retry_count": 0, "model": MODEL, "effort": EFFORT,
                     "managed_chatgpt_auth_only": True, "runtime_lock": _record(frozen["runtime_lock"]),
                     "holdout_authorized": False, "production_mutation_allowed": False})
    started = time.monotonic()
    paths = frozen["turn"]["paths"]
    try:
        async with client_factory(frozen["capacity_policy"]) as client:
            result = await client.run_ephemeral_structured_turn(
                model=MODEL, effort=EFFORT, base_instructions=frozen["turn"]["base"],
                prompt=frozen["turn"]["prompt"], output_schema=frozen["turn"]["schema"],
                cwd=PROJECT_ROOT, sidecar_path=paths["sidecar"], output_path=paths["output"],
                batch_size=2, thread_mode="new_thread", timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=paths["capacity"])
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise V237OpaqueGapError("gap turn did not complete")
        usage = _usage(_load_json(paths["sidecar"], "sidecar"))
        try:
            gap, provenance, base_diagnostics = v233._project_output(
                result.output, frozen["turn"]
            )
        except v233.V233OutputContractError as exc:
            raise V237OutputContractError(str(exc)) from exc
        merged, diagnostics = _merge_gap(gap=gap, provenance=provenance, turn=frozen["turn"])
        _write_immutable(paths["gap_normalized"], gap)
        _write_immutable(paths["normalized"], merged)
        _write_immutable(paths["provenance"], provenance)
        _write_immutable(paths["diagnostics"], {"gap": base_diagnostics, "merged": diagnostics})
        gate = _gate(usage=usage, diagnostics=diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise V237ArchitectureStop("v237 structural or count gate failed")
        terminal = {"schema_version": TERMINAL_VERSION, "terminal_at": now_iso(),
                    "state": "v237_architecture_structural_gate_passed",
                    "terminal_reason": "v237_opaque_gap_structural_cost_gate_passed",
                    "semantic_attempt_count": 1, "semantic_retry_count": 0,
                    "usage_status": "complete", "accounting_complete": True, "usage": usage,
                    "production_amortized_total_token_ratio": gate["production_amortized_total_token_ratio"],
                    "candidate_only_nominal_no_signal_event_count": gate["candidate_only_nominal_no_signal_event_count"],
                    "residual_support_audit_required": gate["residual_support_audit_required"],
                    "support_alignment_authorized": True, "development_winner_frozen": False,
                    "holdout_authorized": False, "production_mutated": False,
                    "overall_goal_complete": False, "goal_status_required": "active",
                    "wall_seconds": round(time.monotonic() - started, 6), "gate": _record(gate_path),
                    "runtime_lock": _record(frozen["runtime_lock"]), "attempt_spec": _record(frozen["spec_path"]),
                    "sidecar": _record(paths["sidecar"]),
                    "exact_next_action": "run frozen v174 support and neutral alignment; source-supported residuals may augment reference"}
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v237 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v237 opaque coverage gap canary")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v237(output_dir=Path(args.output_dir))
        design = _load_json(frozen["design_path"], "design")
        result = {"state": frozen["spec"]["state"], "root": str(frozen["root"]),
                  "projected_ratio": design["production_cost_projection"]["projected_production_amortized_total_token_ratio"]}
    else:
        terminal = asyncio.run(run_v237(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
        result = {"state": terminal["state"], "terminal_reason": terminal["terminal_reason"],
                  "usage_status": terminal.get("usage_status"),
                  "total_tokens": (terminal.get("usage") or {}).get("total_tokens"),
                  "support_alignment_authorized": terminal.get("support_alignment_authorized", False),
                  "holdout_authorized": terminal.get("holdout_authorized", False),
                  "production_mutated": terminal.get("production_mutated", False)}
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
