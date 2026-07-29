from __future__ import annotations

"""Run and score the one-turn v207 adaptive-router canary."""

import argparse
import asyncio
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity_module
from . import app_server_capacity_reserve as reserve_module
from . import app_server_dev_selection as selection_module
from . import app_server_judge_v5_calibration_v26_diagnostic as v26
from . import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from . import app_server_judge_v5_selection_v188_composite_postprocess as v188
from . import app_server_judge_v5_selection_v192_residual_repair_design as v192
from . import app_server_judge_v5_selection_v206_convergence_blocker as v206
from . import app_server_judge_v5_selection_v207_adaptive_router_design as v207
from . import codex_app_server as codex_app_server_module
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import load_reserve_capacity_policy
from .app_server_judge_v5_calibration_v25_diagnostic import (
    PINNED_CODEX_0_144_1,
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .labels import ValidationError, _validate_schema
from .util import now_iso


V208_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V208_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V208_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v208_spec_v1"
V208_RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_4_selection_v208_runtime_lock_v1"
V208_LAUNCH_VERSION = "pif_app_server_judge_v5_4_selection_v208_launch_v1"
V208_GATE_VERSION = "pif_app_server_judge_v5_4_selection_v208_gate_v1"
V208_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v208_failure_v1"
V208_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v208_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v208_adaptive_router_canary"
TURN_NAME = "selection_adaptive_router_canary_00"
DEFAULT_OUTPUT_ROOT = (
    v207.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v208-adaptive-router-canary"
).resolve()


class JudgeV5SelectionV208Error(RuntimeError):
    """The adaptive-router canary cannot continue safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v207_authorization(
    design_root: Path = v207.DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    root = design_root.expanduser().resolve()
    paths = {
        "terminal": root / "terminal.json",
        "design": root / "adaptive-router-design.json",
        "oracle": root / "adaptive-router-oracle-audit.json",
    }
    values = {name: _load_json(path, f"v207 {name}") for name, path in paths.items()}
    terminal = values["terminal"]
    design = values["design"]
    oracle = values["oracle"]
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v207_zero_extraction_adaptive_router_canary_authorized"
        or terminal.get("semantic_attempt_authorized") is not True
        or terminal.get("authorized_turn_count") != 1
        or terminal.get("authorized_model") != v207.MODEL
        or terminal.get("authorized_effort") != v207.EFFORT
        or terminal.get("extraction_model_calls_authorized") != 0
        or terminal.get("full_development_router_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage", {}).get("total_tokens") != 0
        or terminal.get("design") != _record(paths["design"])
        or terminal.get("oracle_audit") != _record(paths["oracle"])
        or design.get("state") != "frozen_before_semantic_attempt"
        or design.get("declared_canary_turn_count") != 1
        or design.get("declared_full_turn_count") != 0
        or design.get("retry_count_per_turn") != 0
        or design.get("maximum_canary_total_tokens")
        != v207.MAX_CANARY_TOTAL_TOKENS
        or design.get("semantic_output_scope")
        != "package_selection_only_no_event_extraction_or_rewrite"
        or design.get("extraction_model_calls_authorized") != 0
        or design.get("extraction_replay_allowed") is not False
        or design.get("batch_5_replay_allowed") is not False
        or design.get("full_development_router_authorized") is not False
        or design.get("holdout_authorized") is not False
        or design.get("production_mutation_allowed") is not False
        or oracle.get("router_model_never_receives_reference_scores_or_density")
        is not True
        or oracle.get("production_semantic_routing_by_reference_forbidden") is not True
        or oracle.get("full_development", {}).get("remaining_router_headroom_tokens", 0)
        < v207.MAX_FULL_ROUTER_TOTAL_TOKENS
    ):
        raise JudgeV5SelectionV208Error("v207 authorization contract drifted")
    for path in paths.values():
        if not _verify_record(_record(path)):
            raise JudgeV5SelectionV208Error("v207 core artifact drifted")
    for record in design.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV208Error("v207 runtime binding drifted")
    frozen = design.get("frozen_inputs") or {}
    required = (
        "canary_input",
        "canary_prompt",
        "canary_schema",
        "full_input",
        "full_prompt",
        "full_schema",
        "instructions",
        "oracle_audit",
    )
    if any(not _verify_record(frozen.get(key) or {}) for key in required):
        raise JudgeV5SelectionV208Error("v207 frozen request artifact drifted")
    predecessor = v207._validate_v206_checkpoint()
    if terminal.get("cumulative_known_usage_lower_bound") != predecessor["terminal"].get(
        "cumulative_known_usage_lower_bound"
    ):
        raise JudgeV5SelectionV208Error("v207 cumulative accounting drifted")
    canary_input = _load_json(Path(frozen["canary_input"]["path"]), "v207 canary input")
    canary_schema = _load_json(
        Path(frozen["canary_schema"]["path"]), "v207 canary schema"
    )
    full_schema = _load_json(Path(frozen["full_schema"]["path"]), "v207 full schema")
    canary_prompt = Path(frozen["canary_prompt"]["path"]).read_text(encoding="utf-8")
    full_prompt = Path(frozen["full_prompt"]["path"]).read_text(encoding="utf-8")
    instructions = Path(frozen["instructions"]["path"]).read_text(encoding="utf-8")
    if (
        len(canary_input.get("cases") or []) != v207.CANARY_CASE_COUNT
        or [row.get("case_id") for row in canary_input.get("cases") or []]
        != design.get("canary_case_ids")
        or len(canary_prompt.encode("utf-8"))
        != Path(frozen["canary_prompt"]["path"]).stat().st_size
        or len(full_prompt.encode("utf-8"))
        != Path(frozen["full_prompt"]["path"]).stat().st_size
    ):
        raise JudgeV5SelectionV208Error("v207 request metadata drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "design": design,
        "oracle": oracle,
        "predecessor": predecessor,
        "input": canary_input,
        "prompt": canary_prompt,
        "schema": canary_schema,
        "full_prompt": full_prompt,
        "full_schema": full_schema,
        "instructions": instructions,
    }


def _selected_extraction_cost(
    output: Mapping[str, Any], predecessor: Mapping[str, Any]
) -> float:
    package_to_arm = {
        package_id: arm
        for arm, package_id in predecessor["design"]["package_mapping_private"].items()
    }
    costs = predecessor["oracle"]["arm_cost_tokens_per_segment"]
    total = 0.0
    for row in output.get("cases") or []:
        selected = [package_to_arm[item] for item in row["selected_package_ids"]]
        total += float(costs[v207.BASE_ARM])
        total += sum(float(costs[arm]) for arm in selected if arm != v207.BASE_ARM)
    return total


def validate_router_output(
    output: Any, predecessor: Mapping[str, Any]
) -> list[str]:
    if not isinstance(output, dict):
        return ["output_not_object"]
    try:
        _validate_schema(dict(predecessor["schema"]), output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        return [f"schema:{type(exc).__name__}"]
    expected = list(predecessor["design"]["canary_case_ids"])
    actual = [row.get("case_id") for row in output.get("cases") or []]
    if actual != expected:
        return ["case_order_or_coverage"]
    for row in output["cases"]:
        selected = row["selected_package_ids"]
        if selected != sorted(selected):
            return ["package_order"]
    if _selected_extraction_cost(output, predecessor) > float(
        predecessor["oracle"]["canary"][
            "extraction_budget_after_declared_router_bound"
        ]
    ):
        return ["declared_extraction_budget"]
    return []


def _candidate_row(
    *,
    augmented: Mapping[str, Any],
    score: Mapping[str, Any],
    segment_id: str,
    subset: Sequence[str],
    baseline: Mapping[str, Any],
) -> tuple[float, int]:
    if not subset:
        return (1.0 if int(baseline["reference_units"]) == 0 else 0.0, 0)
    ordered = tuple(arm for arm in v188.ARM_IDS if arm in subset)
    system_id = (
        f"arm:{ordered[0]}:normalized"
        if len(ordered) == 1
        else v188._composite_system_id(ordered)
    )
    row = next(
        candidate
        for candidate in score["systems"][system_id]["cases"]
        if candidate["segment_id"] == segment_id
    )
    event_count = int(
        augmented["membership"]["system_cases"][system_id][segment_id][
            "submitted_event_count"
        ]
    )
    return float(row["f1"]), event_count


def _score_router(
    *,
    output: Mapping[str, Any],
    usage: Mapping[str, int],
    predecessor: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    errors = validate_router_output(output, predecessor)
    if errors:
        raise JudgeV5SelectionV208Error("router output invalid: " + ";".join(errors))
    _augmented, score, _combinations = v192._all_composite_score()
    baseline = {
        str(row["segment_id"]): row
        for row in score["systems"][selection_module.BASELINE_REPAIRED_SYSTEM]["cases"]
    }
    base_rows = {
        str(row["segment_id"]): row
        for row in score["systems"][f"arm:{v207.BASE_ARM}:normalized"]["cases"]
    }
    case_to_segment = {
        str(row["case_id"]): str(row["segment_id"])
        for row in baseline.values()
    }
    package_to_arm = {
        package_id: arm
        for arm, package_id in predecessor["design"]["package_mapping_private"].items()
    }
    oracle_picks = {
        str(row["segment_id"]): row
        for row in predecessor["oracle"]["canary"]["best_affordable_picks"]
    }
    private_rows = []
    no_signal_passed = True
    dense_improvements = 0
    dense_regrets = []
    selected_f1_total = 0.0
    maximum_event_count = 0
    for route in output["cases"]:
        segment_id = case_to_segment[str(route["case_id"])]
        baseline_row = baseline[segment_id]
        subset = [package_to_arm[item] for item in route["selected_package_ids"]]
        candidate_f1, event_count = _candidate_row(
            augmented=_augmented,
            score=score,
            segment_id=segment_id,
            subset=subset,
            baseline=baseline_row,
        )
        oracle_f1 = float(oracle_picks[segment_id]["candidate_f1"])
        regret = oracle_f1 - candidate_f1
        is_no_signal = baseline_row["density_stratum"] == "no_signal"
        if is_no_signal:
            no_signal_passed = no_signal_passed and candidate_f1 == 1.0
        else:
            dense_improvements += candidate_f1 > float(base_rows[segment_id]["f1"])
            dense_regrets.append(regret)
        selected_f1_total += candidate_f1
        maximum_event_count = max(maximum_event_count, event_count)
        private_rows.append(
            {
                "case_id": route["case_id"],
                "segment_id": segment_id,
                "source_id": baseline_row["source_id"],
                "density_stratum": baseline_row["density_stratum"],
                "selected_package_ids": list(route["selected_package_ids"]),
                "base_coverage": route["base_coverage"],
                "route_reason": route["route_reason"],
                "baseline_f1": baseline_row["f1"],
                "base_f1": base_rows[segment_id]["f1"],
                "candidate_f1": round(candidate_f1, 6),
                "oracle_f1": round(oracle_f1, 6),
                "oracle_regret": round(regret, 6),
                "selected_event_count": event_count,
            }
        )
    candidate_mean = selected_f1_total / len(private_rows)
    oracle_mean = float(
        predecessor["oracle"]["canary"]["best_affordable_candidate_f1"]
    )
    mean_regret = oracle_mean - candidate_mean
    selected_extraction = _selected_extraction_cost(output, predecessor)
    request_bytes = predecessor["design"]["request_bytes"]
    byte_ratio = float(request_bytes["full_projection"]) / float(
        request_bytes["canary"]
    )
    projected_full_input = math.ceil(int(usage["input_tokens"]) * byte_ratio)
    projected_full_output = math.ceil(
        int(usage["output_tokens"])
        * len(baseline)
        / v207.CANARY_CASE_COUNT
    )
    projected_full_total = projected_full_input + projected_full_output
    scope_budget = float(
        predecessor["oracle"]["canary"]["extraction_and_router_budget_tokens"]
    )
    checks = {
        "schema_order_and_package_budget": True,
        "all_four_no_signal_cases_candidate_f1_1": no_signal_passed,
        "dense_cases_improved_over_base_min_3": dense_improvements >= 3,
        "mean_f1_regret_lte_0_05": mean_regret <= 0.05 + 1e-12,
        "maximum_dense_case_regret_lte_0_15": max(dense_regrets) <= 0.15 + 1e-12,
        "selected_event_cap_lte_32": maximum_event_count <= v207.MAX_EVENTS_PER_CASE,
        "actual_canary_total_tokens_lte_25000": int(usage["total_tokens"])
        <= v207.MAX_CANARY_TOTAL_TOKENS,
        "projected_full_router_total_tokens_lte_56000": projected_full_total
        <= v207.MAX_FULL_ROUTER_TOTAL_TOKENS,
        "actual_extraction_plus_router_within_scope_budget": selected_extraction
        + int(usage["total_tokens"])
        <= scope_budget,
    }
    gate = {
        "schema_version": V208_GATE_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(key for key, passed in checks.items() if not passed),
        "case_count": len(private_rows),
        "no_signal_case_count": sum(
            row["density_stratum"] == "no_signal" for row in private_rows
        ),
        "dense_case_count": sum(
            row["density_stratum"] != "no_signal" for row in private_rows
        ),
        "dense_improvement_count": dense_improvements,
        "candidate_mean_f1": round(candidate_mean, 6),
        "best_affordable_oracle_mean_f1": round(oracle_mean, 6),
        "mean_f1_regret_to_oracle": round(mean_regret, 6),
        "maximum_dense_case_regret_to_oracle": round(max(dense_regrets), 6),
        "maximum_selected_event_count": maximum_event_count,
        "selected_extraction_cost_tokens": round(selected_extraction, 6),
        "actual_router_usage": dict(usage),
        "scope_extraction_and_router_budget_tokens": round(scope_budget, 6),
        "request_byte_projection_ratio": round(byte_ratio, 6),
        "projected_full_router_input_tokens": projected_full_input,
        "projected_full_router_output_tokens": projected_full_output,
        "projected_full_router_total_tokens": projected_full_total,
        "full_development_router_authorized": all(checks.values()),
        "holdout_authorized": False,
        "production_mutated": False,
    }
    private_score = {
        "schema_version": "pif_app_server_adaptive_router_canary_score_private_v1",
        "cases": private_rows,
        "gate": gate,
    }
    return gate, private_score


def _build_capacity_policy(
    root: Path, predecessor: Mapping[str, Any]
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = v207.MAX_CANARY_TOTAL_TOKENS
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V208_CAPACITY_AUDIT_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v207_terminal": predecessor["records"]["terminal"],
        "v207_design": predecessor["records"]["design"],
        "v207_oracle": predecessor["records"]["oracle"],
        "measured_basis": {
            "predecessor_known_total_tokens_lower_bound": predecessor["terminal"][
                "cumulative_known_usage_lower_bound"
            ]["total_tokens"],
            "predecessor_unknown_usage_turn_count": predecessor["terminal"][
                "cumulative_unknown_usage_turn_count"
            ],
            "predecessor_unknown_usage_upper_bound": predecessor["terminal"][
                "cumulative_conservative_unknown_usage_upper_bound"
            ],
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": bound,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V208_CAPACITY_POLICY_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [TURN_NAME],
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": bound,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(v207.__file__).resolve(),
                Path(v206.__file__).resolve(),
                Path(v192.__file__).resolve(),
                Path(v188.__file__).resolve(),
                Path(v186.__file__).resolve(),
                Path(v26.__file__).resolve(),
                Path(reserve_module.__file__).resolve(),
                Path(capacity_module.__file__).resolve(),
                Path(codex_app_server_module.__file__).resolve(),
                Path(selection_module.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
            },
            key=str,
        )
    )


def _freeze_runtime_lock(
    *,
    root: Path,
    predecessor: Mapping[str, Any],
    spec_path: Path,
    capacity: Mapping[str, Path],
) -> Path:
    path = root / "runtime-lock.json"
    lock = {
        "schema_version": V208_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "pinned_protocol_schema": _record(codex_app_server_module.PROTOCOL_SCHEMA_PATH),
        "runtime_files": [_record(item) for item in _expected_runtime_paths()],
        "v207_attempt": [
            predecessor["records"][name] for name in ("terminal", "design", "oracle")
        ],
        "v207_frozen_inputs": list(
            predecessor["design"]["frozen_inputs"].values()
        ),
        "attempt_spec": _record(spec_path),
        "capacity_audit": _record(capacity["audit"]),
        "capacity_policy": _record(capacity["policy"]),
        "managed_chatgpt_auth_only": True,
        "production_mutation_allowed": False,
    }
    _write_stable_time(path, lock, "created_at")
    verify_runtime_lock(path, design_root=predecessor["root"])
    return path


def verify_runtime_lock(
    path: Path, *, design_root: Path = v207.DEFAULT_OUTPUT_ROOT
) -> dict[str, Any]:
    lock = _load_json(path, "v208 runtime lock")
    predecessor = _validate_v207_authorization(design_root)
    expected_paths = {str(item) for item in _expected_runtime_paths()}
    actual_paths = {
        str(Path(str(record.get("path") or "")).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping)
    }
    if (
        lock.get("schema_version") != V208_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("managed_chatgpt_auth_only") is not True
        or lock.get("production_mutation_allowed") is not False
        or actual_paths != expected_paths
        or lock.get("v207_attempt")
        != [predecessor["records"][name] for name in ("terminal", "design", "oracle")]
        or lock.get("v207_frozen_inputs")
        != list(predecessor["design"]["frozen_inputs"].values())
    ):
        raise JudgeV5SelectionV208Error("v208 runtime lock drifted")
    records = [
        lock.get("pinned_codex_cli"),
        lock.get("pinned_protocol_schema"),
        lock.get("attempt_spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("runtime_files") or []),
        *(lock.get("v207_attempt") or []),
        *(lock.get("v207_frozen_inputs") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV208Error("v208 runtime lock record drifted")
    policy = load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    if policy.get("audit") != lock.get("capacity_audit"):
        raise JudgeV5SelectionV208Error("v208 policy/audit cross-link drifted")
    return lock


def freeze_v208(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    design_root: Path = v207.DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = v207.TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v208 terminal")}
    predecessor = _validate_v207_authorization(design_root)
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=predecessor["input"],
        prompt=predecessor["prompt"],
        schema=predecessor["schema"],
    )
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V208_SPEC_VERSION,
        "state": "frozen_before_model_call",
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "model": v207.MODEL,
        "reasoning_effort": v207.EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_turn_base_observed_source_grounded_package_router",
        "turn_plan": [TURN_NAME],
        "retry_count_per_turn": 0,
        "maximum_total_tokens_per_turn": v207.MAX_CANARY_TOTAL_TOKENS,
        "existing_extraction_outputs_reused_only": True,
        "extraction_model_calls_authorized": 0,
        "extraction_replay_allowed": False,
        "batch_5_replay_allowed": False,
        "full_development_router_authorized_before_canary_pass": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "frozen_request": {
            "input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
            "instructions": predecessor["design"]["frozen_inputs"]["instructions"],
        },
        "privacy": "private_source_prompts_outputs_sanitized_counts_hashes_metrics_only",
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    runtime_lock = _freeze_runtime_lock(
        root=root,
        predecessor=predecessor,
        spec_path=spec_path,
        capacity=capacity,
    )
    return {
        "root": root,
        "predecessor": predecessor,
        "paths": paths,
        "capacity_policy": capacity["policy"],
        "capacity_audit": capacity["audit"],
        "spec": spec,
        "spec_path": spec_path,
        "runtime_lock": runtime_lock,
    }


def _freeze_launch_receipt(frozen: Mapping[str, Any]) -> Path:
    path = frozen["root"] / "launch-receipt.json"
    if path.exists():
        receipt = _load_json(path, "v208 launch receipt")
        if (
            receipt.get("schema_version") != V208_LAUNCH_VERSION
            or receipt.get("phase_id") != PHASE_ID
            or receipt.get("declared_turn_count") != 1
            or receipt.get("turn_plan") != [TURN_NAME]
            or receipt.get("retry_count_per_turn") != 0
            or receipt.get("managed_chatgpt_auth_only") is not True
            or receipt.get("runtime_lock") != _record(frozen["runtime_lock"])
            or receipt.get("attempt_spec") != _record(frozen["spec_path"])
            or receipt.get("capacity_policy") != _record(frozen["capacity_policy"])
            or receipt.get("capacity_checkpoint_exists_before_launch") is not False
            or receipt.get("sidecar_exists_before_launch") is not False
            or receipt.get("output_exists_before_launch") is not False
            or receipt.get("production_mutated") is not False
        ):
            raise JudgeV5SelectionV208Error("v208 launch receipt drifted")
        return path
    receipt = {
        "schema_version": V208_LAUNCH_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "state": "semantic_attempt_not_started",
        "declared_turn_count": 1,
        "turn_plan": [TURN_NAME],
        "retry_count_per_turn": 0,
        "managed_chatgpt_auth_only": True,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "capacity_policy": _record(frozen["capacity_policy"]),
        "capacity_checkpoint_exists_before_launch": frozen["paths"]["capacity"].exists(),
        "sidecar_exists_before_launch": frozen["paths"]["sidecar"].exists(),
        "output_exists_before_launch": frozen["paths"]["output"].exists(),
        "production_mutated": False,
    }
    if any(
        receipt[key]
        for key in (
            "capacity_checkpoint_exists_before_launch",
            "sidecar_exists_before_launch",
            "output_exists_before_launch",
        )
    ):
        raise JudgeV5SelectionV208Error("v208 semantic artifacts predate launch")
    _write_stable_time(path, receipt, "created_at")
    return path


def _write_failure(
    root: Path, predecessor: Mapping[str, Any], error_class: str
) -> dict[str, Any]:
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        sidecar_record = attempt.get("sidecar")
        if not isinstance(sidecar_record, Mapping):
            unknown += 1
            continue
        try:
            usage = _validate_usage(
                _load_json(Path(sidecar_record["path"]), "v208 failed sidecar")
            )
        except Exception:
            unknown += 1
            continue
        known = _sum_usage(known, usage)
    accounting_complete = unknown == 0
    cumulative = _sum_usage(
        predecessor["terminal"]["cumulative_known_usage_lower_bound"], known
    )
    failure = {
        "schema_version": V208_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": accounting_complete,
        "usage_status": "complete" if accounting_complete else "unknown",
        "usage": known if accounting_complete else None,
        "known_usage_lower_bound": known,
        "unknown_usage_turn_count": unknown,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V208_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "semantic_retry_allowed": False,
        "semantic_retry_count": 0,
        "full_development_router_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "accounting_complete": accounting_complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_known_usage_lower_bound": cumulative,
        "cumulative_unknown_usage_turn_count": predecessor["terminal"][
            "cumulative_unknown_usage_turn_count"
        ]
        + unknown,
        "cumulative_conservative_unknown_usage_upper_bound": predecessor["terminal"][
            "cumulative_conservative_unknown_usage_upper_bound"
        ]
        + unknown * v207.MAX_CANARY_TOTAL_TOKENS,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v208(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    design_root: Path = v207.DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = v207.TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v208 terminal")
    frozen = freeze_v208(
        output_dir=root,
        design_root=design_root,
        timeout_seconds=timeout_seconds,
    )
    verify_runtime_lock(frozen["runtime_lock"], design_root=design_root)
    launch_path = _freeze_launch_receipt(frozen)
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["predecessor"]["prompt"],
                schema=frozen["predecessor"]["schema"],
                base_instructions=frozen["predecessor"]["instructions"],
                model=v207.MODEL,
                effort=v207.EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=v207.CANARY_CASE_COUNT,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda value: validate_router_output(
                    value, frozen["predecessor"]
                ),
            )
        accounting = _aggregate_usage([sidecar])
        gate, private_score = _score_router(
            output=output,
            usage=accounting["usage"],
            predecessor=frozen["predecessor"],
        )
        gate_path = root / "adaptive-router-canary-gate.json"
        score_path = root / "adaptive-router-canary-score.private.json"
        _write_immutable(gate_path, gate)
        _write_immutable(score_path, private_score)
        cumulative = _sum_usage(
            frozen["predecessor"]["terminal"]["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        passed = bool(gate["passed"])
        terminal = {
            "schema_version": V208_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v208_adaptive_router_canary_passed_full_development_router_authorized"
                if passed
                else "v208_adaptive_router_canary_quality_or_cost_gate_not_passed"
            ),
            "terminal_classification": "inactive_incomplete_recovery_required",
            "overall_evaluation_complete": False,
            "launch_receipt": _record(launch_path),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "gate": _record(gate_path),
            "private_score": _record(score_path),
            "full_development_router_authorized": passed,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_allowed": False,
            "semantic_retry_count": 0,
            "cumulative_known_usage_lower_bound": cumulative,
            "cumulative_unknown_usage_turn_count": frozen["predecessor"]["terminal"][
                "cumulative_unknown_usage_turn_count"
            ],
            "cumulative_conservative_unknown_usage_upper_bound": frozen[
                "predecessor"
            ]["terminal"]["cumulative_conservative_unknown_usage_upper_bound"],
            "required_next_artifact_path": (
                str(
                    root.parent
                    / "development-selection-v5_4-v209-adaptive-router-full-development"
                    / "terminal.json"
                )
                if passed
                else None
            ),
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, frozen["predecessor"], exc.error_class)
    except Exception as exc:
        return _write_failure(root, frozen["predecessor"], type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v208 adaptive-router canary")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--design-root", default=str(v207.DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=v207.TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v208(
            output_dir=Path(args.output_dir),
            design_root=Path(args.design_root),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "full_development_router_authorized": terminal.get(
                    "full_development_router_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
                "holdout_authorized": terminal.get("holdout_authorized", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
