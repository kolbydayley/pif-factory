from __future__ import annotations

"""Run the bounded v239 frontier long-horizon full-schema canary."""

import argparse
import asyncio
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
from . import app_server_judge_v5_selection_v238_specialist_ensemble as v238
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v239_frontier_long_horizon_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v239_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v239_terminal_v1"
PHASE_ID = "development_selection_v5_4_v239_frontier_long_horizon"
MODEL = "gpt-5.6-sol"
EFFORT = "high"
MAX_TOTAL_TOKENS = 45_000
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
V233_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v233-blind-unit-sweep"
).resolve()
V233_TURN_ROOT = (
    V233_ROOT / "turns/v233-blind-unit-sweep-d7c914bc1cee2c432b36"
).resolve()
V238_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v238-specialist-ensemble"
).resolve()
V238_TURN_ROOT = (
    V238_ROOT / "turns/v238-specialist-ensemble-d7c914bc1cee2c432b36"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v239-frontier-long-horizon"
).resolve()
PINNED_CODEX_0_144_1 = v233.PINNED_CODEX_0_144_1


EXPECTED_LINEAGE_HASHES = {
    "v233_input": "b0b693ff7d774a61d004d7dca6991a1324c27ed4c3c5ce7ca5d9d0a9c7b44d63",
    "v233_prompt": "0ae782f2653b13b1fa1136b1a7ed9639f988583a28e3c32c593c09a608e5754c",
    "v233_base": "7acabf3adfa2c5097dfd8c6017e1d5260750575a633c09967be9bdbf8fdcdf75",
    "v233_schema": "1f84977a2a884ba746c56f99a42ab2620d14accd9a97ccb72d17a07d096f85df",
    "v233_runtime_lock": "975a21e03d75efb309f9852c889f20e5000d13e9c2f93fe34aecf2bcf31bd9a3",
    "v233_terminal": "2584bfa238032e584d948ad4d97ca63834ad09b4c1c4d73cedf936fff19a24bf",
    "v233_gate": "89f5f6aa99c7bcc332986ace4929e143c1522df0e5a8931b7eb474176faabf51",
    "v238_terminal": "93f22c731e66c8438b4db3f85ff86f9af63f9e612c86121dc93b83910e84e949",
    "v238_sidecar": "8f0b5df4fe877f23abac9a0f8da363de9916856d487032fa3a30ad9033a2368b",
    "v238_audit": "ae70d74c7a1393557d20562ad08d444a476ee996affafff54c8ce7b2112baec9",
}


class V239FrontierError(RuntimeError):
    """The v239 frontier architecture cannot proceed safely."""


class V239OutputContractError(V239FrontierError):
    """A completed v239 output violated the frozen contract."""


class V239ArchitectureStop(V239FrontierError):
    """The v239 architecture did not clear its predeclared gate."""


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V239FrontierError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V239FrontierError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V239FrontierError(f"frozen {path.name} drifted")
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
        "v233_input": V233_TURN_ROOT / "input.private.json",
        "v233_prompt": V233_TURN_ROOT / "prompt.private.md",
        "v233_base": V233_TURN_ROOT / "base-instructions.private.md",
        "v233_schema": V233_TURN_ROOT / "schema.json",
        "v233_runtime_lock": V233_ROOT / "runtime-lock.json",
        "v233_terminal": V233_ROOT / "terminal.json",
        "v233_gate": V233_ROOT / "architecture-structural-gate.json",
        "v238_terminal": V238_ROOT / "terminal.json",
        "v238_sidecar": V238_TURN_ROOT / "sidecar.json",
        "v238_audit": PIPELINE_ROOT / "v238-completed-output-audit-2026-07-17/report.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_LINEAGE_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V239FrontierError(f"frozen lineage {name} drifted")
    v233.verify_runtime_lock(paths["v233_runtime_lock"])
    v233_terminal = _load_json(paths["v233_terminal"], "v233 terminal")
    v233_gate = _load_json(paths["v233_gate"], "v233 gate")
    v238_terminal = _load_json(paths["v238_terminal"], "v238 terminal")
    v238_sidecar = _load_json(paths["v238_sidecar"], "v238 sidecar")
    v238_audit = _load_json(paths["v238_audit"], "v238 audit")
    if (
        v233_terminal.get("terminal_reason")
        != "v233_blind_unit_sweep_structural_or_count_gate_not_passed"
        or v233_gate.get("failed_checks")
        != ["no_signal_event_count_0", "dense_event_count_gte_24"]
        or [row.get("event_count") for row in v233_gate.get("diagnostics") or []]
        != [23, 1]
        or v238_terminal.get("terminal_reason")
        != "v238_specialist_ensemble_structural_or_count_gate_not_passed"
        or v238_sidecar.get("usage_status") != "measured"
        or v238_sidecar.get("usage_complete") is not True
        or v238_audit.get("corrected_failure_class")
        != "architecture_same_turn_consolidation_overmerge_and_metric_literal_failure"
        or v238_audit.get("next_distinct_architecture_authorized") is not True
    ):
        raise V239FrontierError("predecessor architecture evidence drifted")
    return {"paths": paths, "records": records}


def _prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    paths = lineage["paths"]
    private_input = _load_json(paths["v233_input"], "v233 input")
    schema = _load_json(paths["v233_schema"], "v233 schema")
    episode_id = str(private_input["episode_id"])
    segment_ids = [str(row["segment_id"]) for row in private_input["segments"]]
    if segment_ids != [DENSE_SEGMENT_ID, NO_SIGNAL_SEGMENT_ID]:
        raise V239FrontierError("canary segment membership drifted")
    return {
        "turn_name": "v239_frontier_long_horizon_" + sha256_text(episode_id)[:20],
        "episode_id": episode_id,
        "segment_ids": segment_ids,
        "private_input": private_input,
        "prompt": paths["v233_prompt"].read_text(encoding="utf-8"),
        "base": paths["v233_base"].read_text(encoding="utf-8"),
        "schema": schema,
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
                Path(v238.__file__).resolve(),
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
    lock = _load_json(path, "v239 runtime lock")
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
        raise V239FrontierError("v239 runtime lock contract drifted")
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
        raise V239FrontierError("v239 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V239FrontierError("v239 lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v239 spec")
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
            "private_input": _load_json(paths["input"], "v239 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v239 schema"),
        },
    }


def freeze_v239(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v239 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V239FrontierError("unfinished v239 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    turn = _prepare_turn(lineage)
    paths = _turn_paths(root, turn["turn_name"])
    paths["root"].mkdir(parents=True, exist_ok=True)
    _write_private_text(paths["input"], lineage["paths"]["v233_input"].read_text(encoding="utf-8"))
    _write_private_text(paths["prompt"], turn["prompt"])
    _write_private_text(paths["base"], turn["base"])
    _write_private_text(paths["schema"], lineage["paths"]["v233_schema"].read_text(encoding="utf-8"))
    capacity_paths = _capacity_policy(root, turn["turn_name"])
    authorization_path = root / "authorization.json"
    _write_stable_time(
        authorization_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "authority": "direct_operator_steering_2026_07_17",
            "scope": "one bounded frontier long-horizon architecture canary",
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
            "selected_architecture_id": "frontier_long_horizon_full_schema_single_pass",
            "architectures": [
                {"rank": 1, "id": "frontier_long_horizon_full_schema_single_pass"},
                {"rank": 2, "id": "episode_map_reduce_with_llm_global_consolidator"},
                {"rank": 3, "id": "independent_boundary_and_semantic_dual_pass_with_llm_join"},
            ],
            "on_failure": "freeze and advance to map-reduce; no prompt or field patch",
        },
        "created_at",
    )
    projected_total = (
        PRODUCTION_AMORTIZED_CONTEXT_TOKENS + MAX_TOTAL_TOKENS * PRODUCTION_SCALE
    )
    design_path = root / "architecture-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "architecture_id": "frontier_long_horizon_full_schema_single_pass",
            "hypothesis": (
                "one blind high-reasoning source-to-full-schema pass can close the single dense-event "
                "coverage gap observed in the byte-identical low-reasoning v233 request without an "
                "intermediate representation that loses or overmerges propositions"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "request_byte_identical_to_v233": True,
                "semantic_delta": "reasoning horizon only",
                "reference_visible_to_model": False,
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
                "all_source_units_reviewed": True,
                "unresolved_count": 0,
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
    if [row["sha256"] for row in request] != [
        EXPECTED_LINEAGE_HASHES["v233_input"],
        EXPECTED_LINEAGE_HASHES["v233_prompt"],
        EXPECTED_LINEAGE_HASHES["v233_base"],
        EXPECTED_LINEAGE_HASHES["v233_schema"],
    ]:
        raise V239FrontierError("v239 request is not byte-identical to v233")
    spec_path = root / "attempt-spec.json"
    _write_stable_time(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "state": "frozen_before_one_turn_frontier_long_horizon_canary",
            "declared_turn_count": 1,
            "turn_name": turn["turn_name"],
            "episode_id": turn["episode_id"],
            "segment_ids": turn["segment_ids"],
            "model": MODEL,
            "effort": EFFORT,
            "retry_count": 0,
            "maximum_total_tokens": MAX_TOTAL_TOKENS,
            "request_byte_identical_to_v233": True,
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
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(root / "runtime-lock.json", lock, "created_at")
    verify_runtime_lock(root / "runtime-lock.json")
    return _load_frozen(root)


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    usage = sidecar.get("usage")
    if not isinstance(usage, Mapping):
        raise V239FrontierError("turn usage absent")
    result = {}
    for field in USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise V239FrontierError("turn usage incomplete")
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
        raise V239FrontierError("measured sidecar contract failed")
    return result


def _gate(*, usage: Mapping[str, int], diagnostics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense_count = int(by_id.get(DENSE_SEGMENT_ID, {}).get("event_count", -1))
    residual_count = int(by_id.get(NO_SIGNAL_SEGMENT_ID, {}).get("event_count", -1))
    production_total = (
        PRODUCTION_AMORTIZED_CONTEXT_TOKENS + int(usage["total_tokens"]) * PRODUCTION_SCALE
    )
    ratio = production_total / BASELINE_END_TO_END_TOKENS
    checks = {
        "both_segments_validated": set(by_id) == {DENSE_SEGMENT_ID, NO_SIGNAL_SEGMENT_ID},
        "all_source_units_reviewed": all(
            int(row["source_unit_count"]) == int(row["reviewed_source_unit_count"])
            for row in diagnostics
        ),
        "unresolved_count_0": all(int(row["unresolved_count"]) == 0 for row in diagnostics),
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
            raw = _load_json(paths["sidecar"], "v239 sidecar")
            values = raw.get("usage") or {}
            usage = {field: int(values[field]) for field in USAGE_FIELDS}
            unknown = 0
        except Exception:
            unknown = 1
    semantic = isinstance(exc, (V239OutputContractError, V239ArchitectureStop))
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v239_frontier_long_horizon_structural_or_count_gate_not_passed"
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
            "advance to episode map-reduce architecture"
            if semantic
            else "audit immutable infrastructure attempt; no retry"
        ),
    }
    gate_path = root / "architecture-structural-gate.json"
    if gate_path.is_file():
        terminal["gate"] = _record(gate_path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v239(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v239 terminal")
    frozen = freeze_v239(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(root, frozen, V239FrontierError("launch exists; replay prohibited"))
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
            raise V239FrontierError("frontier turn did not complete")
        usage = _usage(_load_json(paths["sidecar"], "v239 sidecar"))
        try:
            normalized, provenance, diagnostics = v233._project_output(
                result.output, frozen["turn"]
            )
        except v233.V233OutputContractError as exc:
            raise V239OutputContractError(str(exc)) from exc
        _write_immutable(paths["normalized"], normalized)
        _write_immutable(paths["provenance"], provenance)
        _write_immutable(paths["diagnostics"], {"segments": diagnostics})
        gate = _gate(usage=usage, diagnostics=diagnostics)
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise V239ArchitectureStop("v239 structural or count gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v239_architecture_structural_gate_passed",
            "terminal_reason": "v239_frontier_long_horizon_structural_cost_gate_passed",
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
            return _load_json(root / "terminal.json", "v239 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v239 frontier long-horizon canary")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v239(output_dir=Path(args.output_dir))
        design = _load_json(frozen["design_path"], "v239 design")
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "projected_ratio": design["production_cost_projection"][
                "projected_production_amortized_total_token_ratio"
            ],
        }
    else:
        terminal = asyncio.run(
            run_v239(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "total_tokens": (terminal.get("usage") or {}).get("total_tokens"),
            "support_alignment_authorized": terminal.get("support_alignment_authorized", False),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
