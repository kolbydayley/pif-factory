from __future__ import annotations

"""Prove the remaining external router/selector designs cannot meet both gates."""

import argparse
import copy
import json
import math
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_dev_selection as selection_module
from . import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from . import app_server_judge_v5_selection_v188_composite_postprocess as v188
from . import app_server_judge_v5_selection_v192_residual_repair_design as v192
from . import app_server_judge_v5_selection_v207_adaptive_router_design as v207
from . import app_server_judge_v5_selection_v218_compact_support_canary as v218
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS
from .util import now_iso


V219_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v219_spec_v1"
V219_AUDIT_VERSION = "pif_app_server_judge_v5_4_selection_v219_audit_v1"
V219_REPORT_VERSION = "pif_app_server_judge_v5_4_selection_v219_report_v1"
V219_RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_4_selection_v219_runtime_lock_v1"
)
V219_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v219_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v219_full_coverage_feasibility_blocker"
DEFAULT_OUTPUT_ROOT = (
    v218.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v219-full-coverage-feasibility-blocker"
).resolve()

FULL_DEVELOPMENT_CASE_COUNT = 22
FULL_DEVELOPMENT_SIGNAL_CASE_COUNT = 18
BASELINE_SEGMENT_SCOPE = 60
BASELINE_END_TO_END_TOKENS = 10_065_426
MEASURED_ROUTER_PROJECTED_FULL_TOKENS = 100_488


class JudgeV5SelectionV219Error(RuntimeError):
    """The v219 feasibility result cannot preserve its frozen evidence."""


def _v218_paths() -> dict[str, Path]:
    root = v218.DEFAULT_OUTPUT_ROOT
    turn = root / "turns" / v218.TURN_NAME.replace("_", "-")
    return {
        "spec": root / "attempt-spec.json",
        "capacity_audit": root / "capacity-policy-audit.json",
        "capacity_policy": root / "capacity-policy.json",
        "gate": root / "compact-support-canary-gate.json",
        "score": root / "compact-support-canary-score.private.json",
        "identity": root / "identity-projection.private.json",
        "launch": root / "launch-receipt.json",
        "model_audit": root / "model-selection-audit.json",
        "runtime_lock": root / "runtime-lock.json",
        "instructions": root / "selector-instructions.private.md",
        "selected_output": root / "support-positive-selector-output.private.json",
        "terminal": root / "terminal.json",
        "projection": root / "vector-projection-audit.private.json",
        "capacity": turn / "capacity.json",
        "input": turn / "input.private.json",
        "output": turn / "output.private.json",
        "prompt": turn / "prompt.private.md",
        "schema": turn / "schema.json",
        "sidecar": turn / "sidecar.json",
    }


def _validate_v218_checkpoint() -> dict[str, Any]:
    root = v218.DEFAULT_OUTPUT_ROOT
    paths = _v218_paths()
    expected = {path.resolve() for path in paths.values()}
    actual = {path.resolve() for path in root.rglob("*") if path.is_file()}
    if actual != expected:
        raise JudgeV5SelectionV219Error("v218 immutable file set drifted")
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV219Error("v218 immutable artifact drifted")
    v218.verify_runtime_lock(paths["runtime_lock"])

    terminal = _load_json(paths["terminal"], "v218 terminal")
    gate = _load_json(paths["gate"], "v218 gate")
    capacity = _load_json(paths["capacity"], "v218 capacity")
    sidecar = _load_json(paths["sidecar"], "v218 sidecar")
    output = _load_json(paths["output"], "v218 output")
    predecessor = v218._validate_v217_checkpoint()
    request = v218._build_request(predecessor)
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v218_compact_support_canary_passed_full_router_authorized"
        or terminal.get("full_development_router_authorized") is not True
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage", {}).get("total_tokens") != 33_423
        or terminal.get("cumulative_known_usage_lower_bound", {}).get(
            "total_tokens"
        )
        != 9_086_613
        or gate.get("passed") is not True
        or gate.get("failed_checks") != []
        or gate.get("candidate_mean_f1") != 0.80494
        or gate.get("maximum_dense_case_regret_to_oracle") != 0.124286
        or gate.get("actual_selector_usage", {}).get("total_tokens") != 33_423
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("rate_limit_reached_type") is not None
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != v218.MODEL
        or sidecar.get("effort") != v218.EFFORT
        or sidecar.get("usage_complete") is not True
        or sidecar.get("usage") != terminal.get("usage")
        or v218.validate_support_output(
            output,
            request["packet"],
            request["schema"],
        )
    ):
        raise JudgeV5SelectionV219Error("v218 success contract drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "gate": gate,
        "capacity": capacity,
        "sidecar": sidecar,
    }


def _perfect_support_selector_gate(
    score: Mapping[str, Any],
    system_id: str,
) -> dict[str, Any]:
    projected = copy.deepcopy(dict(score))
    for row in projected["systems"][system_id]["cases"]:
        row["submitted_units"] = row["supported_units"]
        row["precision"] = 1.0
        row["f1"] = v192._support_filter_f1(row)
    return selection_module.evaluate_semantic_gates(
        projected,
        candidate_system_id=system_id,
    )


def _feasibility_audit() -> tuple[dict[str, Any], dict[str, Any]]:
    _augmented, score, _combinations = v192._all_composite_score()
    composite_path = v188.DEFAULT_OUTPUT_ROOT / "composite-selection-score.json"
    adaptive_path = v207.DEFAULT_OUTPUT_ROOT / "adaptive-router-oracle-audit.json"
    composite = _load_json(composite_path, "v188 composite score")
    adaptive = _load_json(adaptive_path, "v207 adaptive oracle")
    combinations = composite["combinations"]
    if len(combinations) != 26:
        raise JudgeV5SelectionV219Error("fixed bundle coverage drifted")

    selector_tokens = math.ceil(
        int(_load_json(_v218_paths()["sidecar"], "v218 sidecar")["usage"][
            "total_tokens"
        ])
        / 4
        * FULL_DEVELOPMENT_SIGNAL_CASE_COUNT
    )
    selector_added_ratio = (
        selector_tokens
        * BASELINE_SEGMENT_SCOPE
        / FULL_DEVELOPMENT_CASE_COUNT
        / BASELINE_END_TO_END_TOKENS
    )
    rows = []
    for combination in combinations:
        system_id = str(combination["system_id"])
        if system_id not in score["systems"]:
            continue
        gate = _perfect_support_selector_gate(score, system_id)
        base_ratio = float(
            combination["cost"]["production_amortized_total_token_ratio"]
        )
        projected_ratio = base_ratio + selector_added_ratio
        cost_cap_eligible = (
            projected_ratio <= 0.28 + 1e-12
            and int(combination["cap_hit_segment_count"]) == 0
        )
        rows.append(
            {
                "system_id": system_id,
                "arms": list(combination["arms"]),
                "base_production_amortized_ratio": base_ratio,
                "projected_ratio_with_measured_selector": round(
                    projected_ratio,
                    9,
                ),
                "cap_hit_segment_count": int(
                    combination["cap_hit_segment_count"]
                ),
                "cost_and_cap_eligible_after_selector": cost_cap_eligible,
                "perfect_support_selector_semantic_passed": bool(gate["passed"]),
                "perfect_support_selector_candidate_f1": gate["bootstrap"][
                    "candidate_f1"
                ],
                "perfect_support_selector_ci_lower": gate["bootstrap"][
                    "ci_lower"
                ],
                "perfect_support_selector_macro_source_delta": gate[
                    "macro_source_delta"
                ],
                "perfect_support_selector_worst_source_delta": gate[
                    "worst_source_delta"
                ],
                "semantic_checks": dict(gate["checks"]),
            }
        )
    eligible = [row for row in rows if row["cost_and_cap_eligible_after_selector"]]
    passing = [
        row
        for row in eligible
        if row["perfect_support_selector_semantic_passed"]
    ]
    best = max(
        eligible,
        key=lambda row: (
            float(row["perfect_support_selector_candidate_f1"]),
            -float(row["projected_ratio_with_measured_selector"]),
        ),
    )
    full = adaptive["full_development"]
    headroom = float(full["remaining_router_headroom_tokens"])
    report = {
        "schema_version": V219_REPORT_VERSION,
        "state": "external_authorization_required",
        "v218_canary_quality_and_cost_passed": True,
        "fixed_bundle_count": len(rows),
        "fixed_cost_and_cap_eligible_count": len(eligible),
        "fixed_quality_pass_count_under_perfect_support_selector": len(passing),
        "best_cost_eligible_fixed_bundle": best,
        "selector_projected_full_development_tokens": selector_tokens,
        "selector_added_production_amortized_ratio": round(
            selector_added_ratio,
            9,
        ),
        "adaptive_minimum_passing_extraction_tokens": int(
            full["minimum_passing_extraction_cost_upper_rounded_tokens"]
        ),
        "adaptive_scope_budget_tokens": float(
            full["extraction_and_router_budget_tokens"]
        ),
        "adaptive_remaining_router_and_selector_headroom_tokens": headroom,
        "selector_tokens_over_adaptive_headroom": math.ceil(
            selector_tokens - headroom
        ),
        "measured_router_projected_full_tokens": (
            MEASURED_ROUTER_PROJECTED_FULL_TOKENS
        ),
        "fixed_path_feasible": len(passing) > 0,
        "adaptive_external_router_plus_selector_feasible": (
            selector_tokens + MEASURED_ROUTER_PROJECTED_FULL_TOKENS <= headroom
        ),
        "semantic_reference_use": (
            "evaluation_only_perfect_support_upper_bound_never_production_routing"
        ),
        "blocker_class": "fresh_integrated_extraction_validation_authorization",
        "blocker_reason": (
            "all_fixed_extraction_bundles_fail_quality_even_with_a_perfect_support_"
            "selector_or_exceed_cost_and_the_quality_capable_adaptive_frontier_has_"
            "insufficient_headroom_for_the_measured_selector"
        ),
        "required_strategy": (
            "emit_support_and_completeness_gap_routing_inside_one_fresh_base_"
            "extraction_turn_then_run_extra_arms_only_for_observable_gaps"
        ),
        "current_goal_prohibits_required_fresh_extraction": True,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    private = {
        "schema_version": V219_AUDIT_VERSION,
        "fixed_bundle_rows": rows,
        "records": {
            "v188_composite_score": _record(composite_path),
            "v207_adaptive_oracle": _record(adaptive_path),
        },
        "deterministic_semantic_decisions_made": False,
        "semantic_filtering_or_routing_used_in_production": False,
        "new_semantic_model_calls": 0,
        "new_usage": {field: 0 for field in USAGE_FIELDS},
    }
    if (
        report["fixed_bundle_count"] != 26
        or report["fixed_cost_and_cap_eligible_count"] != 9
        or report["fixed_quality_pass_count_under_perfect_support_selector"] != 0
        or report["selector_projected_full_development_tokens"] != 150_404
        or report["adaptive_minimum_passing_extraction_tokens"] != 755_000
        or report["adaptive_remaining_router_and_selector_headroom_tokens"]
        != 58_186.366667
        or report["selector_tokens_over_adaptive_headroom"] != 92_218
        or report["fixed_path_feasible"] is not False
        or report["adaptive_external_router_plus_selector_feasible"] is not False
    ):
        raise JudgeV5SelectionV219Error("v219 feasibility result drifted")
    return report, private


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            set(v218._expected_runtime_paths())
            | {
                Path(v186.__file__).resolve(),
                Path(v188.__file__).resolve(),
                Path(v192.__file__).resolve(),
                Path(v207.__file__).resolve(),
                Path(selection_module.__file__).resolve(),
                Path(__file__).resolve(),
            },
            key=str,
        )
    )


def _freeze_runtime_lock(
    *,
    root: Path,
    checkpoint: Mapping[str, Any],
    spec_path: Path,
    report_path: Path,
    audit_path: Path,
) -> Path:
    path = root / "runtime-lock.json"
    audit = _load_json(audit_path, "v219 audit")
    lock = {
        "schema_version": V219_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "runtime_files": [_record(item) for item in _expected_runtime_paths()],
        "v218_attempt": list(checkpoint["records"].values()),
        "semantic_sources": list(audit["records"].values()),
        "attempt_spec": _record(spec_path),
        "feasibility_report": _record(report_path),
        "feasibility_audit": _record(audit_path),
        "semantic_model_calls_authorized": 0,
        "production_mutation_allowed": False,
    }
    _write_immutable(path, lock)
    verify_runtime_lock(path)
    return path


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v219 runtime lock")
    checkpoint = _validate_v218_checkpoint()
    expected_paths = {str(item) for item in _expected_runtime_paths()}
    actual_paths = {
        str(Path(str(record.get("path") or "")).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping)
    }
    audit = _load_json(Path(lock["feasibility_audit"]["path"]), "v219 audit")
    if (
        lock.get("schema_version") != V219_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("semantic_model_calls_authorized") != 0
        or lock.get("production_mutation_allowed") is not False
        or actual_paths != expected_paths
        or lock.get("v218_attempt") != list(checkpoint["records"].values())
        or lock.get("semantic_sources") != list(audit["records"].values())
    ):
        raise JudgeV5SelectionV219Error("v219 runtime lock drifted")
    records = [
        lock.get("attempt_spec"),
        lock.get("feasibility_report"),
        lock.get("feasibility_audit"),
        *(lock.get("runtime_files") or []),
        *(lock.get("v218_attempt") or []),
        *(lock.get("semantic_sources") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV219Error("v219 runtime lock record drifted")
    return lock


def freeze_v219(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v219 terminal")
    if any(root.iterdir()):
        raise JudgeV5SelectionV219Error(
            "v219 root is nonempty without a terminal"
        )

    checkpoint = _validate_v218_checkpoint()
    report, audit = _feasibility_audit()
    spec = {
        "schema_version": V219_SPEC_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "scope": "zero_token_full_development_quality_cost_feasibility_proof",
        "v218_canary_replayed": False,
        "semantic_model_calls_authorized": 0,
        "extraction_model_calls_authorized": 0,
        "semantic_retry_authorized": False,
        "quality_gates_unchanged": True,
        "production_amortized_total_token_ratio_max": 0.28,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "v218_attempt": checkpoint["records"],
    }
    spec_path = root / "attempt-spec.json"
    report_path = root / "full-coverage-feasibility-report.json"
    audit_path = root / "full-coverage-feasibility.private.json"
    _write_immutable(spec_path, spec)
    _write_immutable(report_path, report)
    _write_immutable(audit_path, audit)
    runtime_lock = _freeze_runtime_lock(
        root=root,
        checkpoint=checkpoint,
        spec_path=spec_path,
        report_path=report_path,
        audit_path=audit_path,
    )
    predecessor = checkpoint["terminal"]
    terminal = {
        "schema_version": V219_TERMINAL_VERSION,
        "state": "waiting_for_external_authorization",
        "terminal_at": now_iso(),
        "terminal_reason": (
            "fresh_integrated_extraction_validation_authorization_required"
        ),
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "runtime_lock": _record(runtime_lock),
        "attempt_spec": _record(spec_path),
        "feasibility_report": _record(report_path),
        "feasibility_audit": _record(audit_path),
        "external_blocker": True,
        "blocker_class": (
            "fresh_integrated_extraction_validation_authorization"
        ),
        "blocker_is_quality_failure": False,
        "blocker_is_transport_failure": False,
        "v218_canary_quality_and_cost_passed": True,
        "new_semantic_model_calls": 0,
        "new_extraction_model_calls": 0,
        "semantic_retry_allowed": False,
        "semantic_retry_count": 0,
        "full_development_router_authorized": False,
        "full_development_selector_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": {field: 0 for field in USAGE_FIELDS},
        "cumulative_known_usage_lower_bound": predecessor[
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": predecessor[
            "cumulative_unknown_usage_turn_count"
        ],
        "cumulative_conservative_unknown_usage_upper_bound": predecessor[
            "cumulative_conservative_unknown_usage_upper_bound"
        ],
        "viable_systems": [
            "sol_low_compact_support_selector_canary_only_not_full_pipeline"
        ],
        "unresolved_selection_decision": (
            "authorize_one_fresh_integrated_base_extraction_completeness_strategy"
        ),
        "more_development_cases_can_change_selection": False,
        "shortest_path_to_holdout_verdict": (
            "explicit_authorization_then_one_four_source_integrated_extraction_"
            "canary_then_freeze_or_nonacceptance"
        ),
        "required_next_artifact_path": str(
            root.parent
            / "development-selection-v5_4-v220-integrated-base-extraction-authorization"
            / "authorization.json"
        ),
        "evaluation_acceptance_receipt_emitted": False,
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Freeze the v219 full-coverage feasibility blocker"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v219(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "v218_canary_quality_and_cost_passed": terminal[
                    "v218_canary_quality_and_cost_passed"
                ],
                "external_blocker": terminal["external_blocker"],
                "holdout_authorized": terminal["holdout_authorized"],
                "production_mutated": terminal["production_mutated"],
                "new_semantic_model_calls": terminal[
                    "new_semantic_model_calls"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
