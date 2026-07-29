from __future__ import annotations

"""Adopt the measured v209 output for zero-token nonacceptance scoring."""

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v207_adaptive_router_design as v207
from . import app_server_judge_v5_selection_v208_adaptive_router_canary as v208
from . import app_server_judge_v5_selection_v209_adaptive_router_schema_recovery as v209
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import _validate_usage
from .util import now_iso


V210_REPORT_VERSION = "pif_app_server_judge_v5_4_selection_v210_nonacceptance_report_v1"
V210_NEXT_VERSION = "pif_app_server_judge_v5_4_selection_v210_next_strategy_v1"
V210_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v210_spec_v1"
V210_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v210_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v210_adaptive_router_nonacceptance"
DEFAULT_OUTPUT_ROOT = (
    v209.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v210-adaptive-router-nonacceptance"
).resolve()

EXPECTED_USAGE = {
    "input_tokens": 30_264,
    "cached_input_tokens": 0,
    "output_tokens": 875,
    "reasoning_output_tokens": 502,
    "total_tokens": 31_139,
}
EXPECTED_FAILED_CHECKS = [
    "actual_canary_total_tokens_lte_25000",
    "dense_cases_improved_over_base_min_3",
    "maximum_dense_case_regret_lte_0_15",
    "mean_f1_regret_lte_0_05",
    "projected_full_router_total_tokens_lte_56000",
]


class JudgeV5SelectionV210Error(RuntimeError):
    """The measured v209 output cannot be adopted safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v209_measured_output_failure() -> dict[str, Any]:
    root = v209.DEFAULT_OUTPUT_ROOT
    turn_root = root / "turns" / v209.TURN_NAME.replace("_", "-")
    paths = {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "launch_receipt": root / "launch-receipt.json",
        "runtime_lock": root / "runtime-lock.json",
        "spec": root / "attempt-spec.json",
        "schema_audit": root / "schema-recovery-audit.json",
        "capacity_policy": root / "capacity-policy.json",
        "capacity_audit": root / "capacity-policy-audit.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "output": turn_root / "output.private.json",
    }
    json_names = set(paths) - {"prompt"}
    values = {
        name: _load_json(path, f"v209 {name}")
        for name, path in paths.items()
        if name in json_names
    }
    prompt = paths["prompt"].read_text(encoding="utf-8")
    terminal = values["terminal"]
    failure = values["failure"]
    sidecar = values["sidecar"]
    capacity = values["capacity"]
    usage = _validate_usage(sidecar)
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != EXPECTED_USAGE
        or terminal.get("full_development_router_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("cumulative_known_usage_lower_bound", {}).get("total_tokens")
        != 8_931_442
        or terminal.get("cumulative_unknown_usage_turn_count") != 3
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 245_000
        or terminal.get("failure") != _record(paths["failure"])
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("error_class") != "ReserveCapacityError"
        or failure.get("usage_status") != "complete"
        or failure.get("accounting_complete") is not True
        or failure.get("usage") != EXPECTED_USAGE
        or failure.get("unknown_usage_turn_count") != 0
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("error_class") is not None
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or usage != EXPECTED_USAGE
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != v207.MODEL
        or sidecar.get("effort") != v207.EFFORT
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("rate_limit_reached_type") is not None
        or values["schema_audit"].get("unsupported_paths_after") != []
        or values["schema_audit"].get("semantic_contract_changed") is not False
    ):
        raise JudgeV5SelectionV210Error("v209 measured-output failure contract drifted")
    for path in paths.values():
        if not _verify_record(_record(path)):
            raise JudgeV5SelectionV210Error("v209 attempt artifact drifted")
    v209.verify_runtime_lock(paths["runtime_lock"])
    v208_failure = v209._validate_v208_schema_failure()
    predecessor = v208_failure["predecessor"]
    if prompt != predecessor["prompt"] or values["input"] != predecessor["input"]:
        raise JudgeV5SelectionV210Error("v209 semantic request drifted")
    validation_errors = v209.validate_router_output_v209(
        values["output"], predecessor, values["schema"]
    )
    if validation_errors:
        raise JudgeV5SelectionV210Error("v209 completed output is not scoreable")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "failure": failure,
        "sidecar": sidecar,
        "usage": usage,
        "output": values["output"],
        "schema": values["schema"],
        "predecessor": predecessor,
    }


def freeze_v210(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v210 terminal")
    if any(root.iterdir()):
        raise JudgeV5SelectionV210Error("v210 root is nonempty without a terminal")
    predecessor = _validate_v209_measured_output_failure()
    gate, private_score = v208._score_router(
        output=predecessor["output"],
        usage=predecessor["usage"],
        predecessor=predecessor["predecessor"],
    )
    if (
        gate.get("passed") is not False
        or gate.get("failed_checks") != EXPECTED_FAILED_CHECKS
        or gate.get("candidate_mean_f1") != 0.734343
        or gate.get("best_affordable_oracle_mean_f1") != 0.832541
        or gate.get("mean_f1_regret_to_oracle") != 0.098198
        or gate.get("maximum_dense_case_regret_to_oracle") != 0.297661
        or gate.get("dense_improvement_count") != 2
        or gate.get("projected_full_router_total_tokens") != 100_488
        or gate.get("selected_extraction_cost_tokens") != 105_797.375
        or gate.get("full_development_router_authorized") is not False
    ):
        raise JudgeV5SelectionV210Error("v209 adopted score drifted")
    gate_path = root / "adaptive-router-canary-gate.json"
    private_score_path = root / "adaptive-router-canary-score.private.json"
    _write_immutable(gate_path, gate)
    _write_immutable(private_score_path, private_score)
    full_oracle = predecessor["predecessor"]["oracle"]["full_development"]
    projected_joint = int(
        full_oracle["minimum_passing_extraction_cost_upper_rounded_tokens"]
    ) + int(gate["projected_full_router_total_tokens"])
    full_budget = float(full_oracle["extraction_and_router_budget_tokens"])
    report = {
        "schema_version": V210_REPORT_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "inactive_incomplete_recovery_required",
        "blocker_class": "measured_zero_extraction_router_quality_and_token_shortfall",
        "viable_systems_meeting_joint_gates": [],
        "canary": {
            "case_count": gate["case_count"],
            "candidate_mean_f1": gate["candidate_mean_f1"],
            "best_affordable_oracle_mean_f1": gate[
                "best_affordable_oracle_mean_f1"
            ],
            "mean_f1_regret_to_oracle": gate["mean_f1_regret_to_oracle"],
            "maximum_dense_case_regret_to_oracle": gate[
                "maximum_dense_case_regret_to_oracle"
            ],
            "dense_improvement_count": gate["dense_improvement_count"],
            "failed_checks": gate["failed_checks"],
            "measured_router_total_tokens": EXPECTED_USAGE["total_tokens"],
            "projected_full_router_total_tokens": gate[
                "projected_full_router_total_tokens"
            ],
        },
        "full_quality_ceiling": {
            "minimum_passing_extraction_cost_upper_rounded_tokens": full_oracle[
                "minimum_passing_extraction_cost_upper_rounded_tokens"
            ],
            "maximum_router_headroom_tokens": full_oracle[
                "remaining_router_headroom_tokens"
            ],
            "projected_router_plus_minimum_passing_extraction_tokens": projected_joint,
            "development_extraction_and_router_budget_tokens": full_budget,
            "projected_budget_overrun_tokens": round(projected_joint - full_budget, 6),
        },
        "more_development_cases_can_change_current_strategy_verdict": False,
        "reason": (
            "the canary missed three semantic routing gates and the raw full-source router "
            "projection exceeds the remaining token headroom before a full development call"
        ),
        "holdout_authorized": False,
        "production_mutated": False,
        "cumulative_known_total_tokens": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ]["total_tokens"],
        "cumulative_unknown_usage_turn_count": predecessor["terminal"][
            "cumulative_unknown_usage_turn_count"
        ],
        "cumulative_unknown_usage_upper_bound_tokens": predecessor["terminal"][
            "cumulative_conservative_unknown_usage_upper_bound"
        ],
        "privacy": "sanitized_counts_hashes_and_metrics_no_source_or_event_text",
    }
    report_path = root / "development-nonacceptance-report.json"
    _write_stable_time(report_path, report, "created_at")
    next_strategy = {
        "schema_version": V210_NEXT_VERSION,
        "state": "prepared_not_authorized",
        "strategy": "emit_semantic_completeness_and_followup_routing_inside_the_base_extraction_turn",
        "why_it_is_shorter": (
            "the base model already reads the complete source, so a routing receipt adds only "
            "bounded structured output instead of rereading all source text"
        ),
        "why_it_cannot_run_under_current_goal": (
            "validation requires a fresh extraction attempt, while the active objective "
            "explicitly prohibits extraction reruns"
        ),
        "required_authorization": (
            "explicitly permit one new immutable development extraction strategy while "
            "preserving every prior extraction attempt"
        ),
        "gates_unchanged": True,
        "production_amortized_total_token_ratio_max": 0.28,
        "semantic_quality_noninferiority_required": True,
        "holdout_remains_closed": True,
        "production_mutation_allowed": False,
    }
    next_path = root / "next-strategy.json"
    _write_immutable(next_path, next_strategy)
    spec = {
        "schema_version": V210_SPEC_VERSION,
        "state": "zero_model_call_nonacceptance_completed",
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "v209_completed_output_adopted_without_replay": True,
        "v209_failure_terminal_mutated": False,
        "semantic_model_calls_started": 0,
        "extraction_model_calls_started": 0,
        "new_usage_tokens": 0,
        "full_development_router_started": False,
        "holdout_started": False,
        "production_mutation_allowed": False,
        "predecessor": predecessor["records"],
        "frozen_outputs": {
            "gate": _record(gate_path),
            "private_score": _record(private_score_path),
            "report": _record(report_path),
            "next_strategy": _record(next_path),
        },
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v209.__file__)),
            _record(Path(v208.__file__)),
            _record(Path(v207.__file__)),
        ],
        "privacy": "private_case_score_separate_sanitized_terminal_and_report",
    }
    spec_path = root / "nonacceptance-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    terminal = {
        "schema_version": V210_TERMINAL_VERSION,
        "state": "waiting_for_external_strategy_authorization",
        "terminal_at": now_iso(),
        "terminal_reason": "development_quality_and_token_targets_not_met_holdout_closed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "evaluation_accepted": False,
        "semantic_quality_passed": False,
        "production_amortized_token_target_passed": False,
        "development_winner_frozen": False,
        "full_development_router_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "report": _record(report_path),
        "next_strategy": _record(next_path),
        "spec": _record(spec_path),
        "usage_status": "complete",
        "usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "total_tokens": 0,
        },
        "cumulative_known_usage_lower_bound": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": predecessor["terminal"][
            "cumulative_unknown_usage_turn_count"
        ],
        "cumulative_conservative_unknown_usage_upper_bound": predecessor["terminal"][
            "cumulative_conservative_unknown_usage_upper_bound"
        ],
        "required_next_artifact_path": str(
            root.parent
            / "development-selection-v5_4-v211-integrated-routing-extraction-authorization"
            / "terminal.json"
        ),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v210 router nonacceptance")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v210(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "evaluation_accepted": terminal["evaluation_accepted"],
                "holdout_authorized": terminal["holdout_authorized"],
                "production_mutated": terminal["production_mutated"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
