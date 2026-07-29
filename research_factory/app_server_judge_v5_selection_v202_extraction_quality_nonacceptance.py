from __future__ import annotations

"""Freeze the v201 residual-repair quality rejection as a sanitized checkpoint."""

import argparse
import json
from pathlib import Path
from typing import Any, Optional, Sequence

from . import app_server_judge_v5_selection_v201_residual_repair_score as v201
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS
from .util import now_iso


V202_REPORT_VERSION = "pif_app_server_judge_v5_4_selection_v202_nonacceptance_v1"
V202_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v202_spec_v1"
V202_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v202_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    v201.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v202-extraction-quality-nonacceptance"
).resolve()


class JudgeV5SelectionV202Error(RuntimeError):
    """The v201 quality rejection cannot be frozen safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v201_nonacceptance() -> dict[str, Any]:
    root = v201.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "report": root / "residual-repair-score-report.json",
        "audit": root / "residual-repair-score-audit.json",
        "spec": root / "residual-repair-score-spec.json",
    }
    values = {name: _load_json(path, f"v201 {name}") for name, path in paths.items()}
    terminal = values["terminal"]
    report = values["report"]
    audit = values["audit"]
    spec = values["spec"]
    gate = report.get("semantic_gate") or {}
    bootstrap = gate.get("bootstrap") or {}
    oracle = audit.get("conflict_perfect_oracle_gate") or {}
    oracle_bootstrap = oracle.get("bootstrap") or {}
    expected_cumulative = {
        "input_tokens": 7355828,
        "cached_input_tokens": 876032,
        "output_tokens": 1352413,
        "reasoning_output_tokens": 439201,
        "total_tokens": 8708241,
    }
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason")
        != "development_residual_repair_quality_gate_not_passed_conflict_recovery_cannot_change_verdict"
        or terminal.get("terminal_classification")
        != "inactive_incomplete_recovery_required"
        or terminal.get("development_quality_passed") is not False
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("production_amortized_token_target_passed") is not True
        or terminal.get("production_amortized_total_token_ratio") != 0.236727
        or terminal.get("conflict_recovery_could_change_verdict") is not False
        or terminal.get("more_development_cases_can_change_selection") is not False
        or terminal.get("safe_local_judge_only_experiment_remaining") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_attempt_started") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != zero_usage
        or terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative
        or terminal.get("cumulative_unknown_usage_turn_count") != 2
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 220000
        or terminal.get("score_report") != _record(paths["report"])
        or terminal.get("score_audit") != _record(paths["audit"])
        or terminal.get("spec") != _record(paths["spec"])
        or report.get("promotion_passed") is not False
        or report.get("production_amortized_token_target_passed") is not True
        or report.get("production_amortized_total_token_ratio") != 0.236727
        or report.get("conflict_recovery_could_change_verdict") is not False
        or report.get("more_development_cases_can_change_selection") is not False
        or report.get("viable_systems") != []
        or report.get("development_case_count") != 22
        or gate.get("semantic_passed") is not False
        or bootstrap.get("baseline_f1") != 0.725098
        or bootstrap.get("candidate_f1") != 0.461543
        or bootstrap.get("candidate_minus_baseline") != -0.263555
        or bootstrap.get("ci_lower") != -0.352446
        or bootstrap.get("ci_upper") != -0.14031
        or gate.get("macro_source_delta") != -0.233135
        or gate.get("worst_source_delta") != -0.378111
        or gate.get("abstained_case_rate") != 0.090909
        or (gate.get("no_signal") or {}).get("false_positive_rate") != 0.0
        or gate.get("normalized_nonexact_evidence_events") != 0
        or audit.get("promotion_passed") is not False
        or audit.get("maximum_candidate_event_count") != 25
        or audit.get("conflict_recovery_could_change_verdict") is not False
        or oracle.get("semantic_passed") is not False
        or oracle_bootstrap.get("candidate_minus_baseline") != -0.172646
        or oracle_bootstrap.get("ci_upper") != -0.122663
        or spec.get("state") != "zero_token_development_score_completed"
        or spec.get("semantic_model_calls_declared") != 0
        or spec.get("semantic_model_calls_started") != 0
        or spec.get("production_mutation_allowed") is not False
        or spec.get("holdout_authorized") is not False
    ):
        raise JudgeV5SelectionV202Error("v201 measured nonacceptance drifted")
    records = [
        *spec.get("runtime_files", []),
        *spec.get("predecessor", {}).values(),
        *spec.get("frozen_inputs", {}).values(),
        *spec.get("private_artifacts", {}).values(),
        spec.get("report"),
        spec.get("audit"),
    ]
    if any(not isinstance(record, dict) or not _verify_record(record) for record in records):
        raise JudgeV5SelectionV202Error("v201 frozen binding drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "report": report,
        "audit": audit,
        "spec": spec,
    }


def freeze_v202(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v202 terminal")
    predecessor = _validate_v201_nonacceptance()
    gate = predecessor["report"]["semantic_gate"]
    bootstrap = gate["bootstrap"]
    oracle = predecessor["audit"]["conflict_perfect_oracle_gate"]
    failed_checks = sorted(
        check for check, passed in gate["checks"].items() if not passed
    )
    report = {
        "schema_version": V202_REPORT_VERSION,
        "created_at": now_iso(),
        "blocker_class": "measured_development_extraction_quality_shortfall",
        "infrastructure_failure": False,
        "judge_protocol_failure": False,
        "token_target_passed": True,
        "production_amortized_total_token_ratio": 0.236727,
        "semantic_quality_passed": False,
        "failed_semantic_checks": failed_checks,
        "observed_quality_leader": predecessor["report"]["observed_quality_leader"],
        "viable_systems": [],
        "quality": {
            "development_case_count": predecessor["report"]["development_case_count"],
            "source_cluster_count": bootstrap["source_clusters"],
            "baseline_f1": bootstrap["baseline_f1"],
            "candidate_f1": bootstrap["candidate_f1"],
            "candidate_minus_baseline": bootstrap["candidate_minus_baseline"],
            "paired_ci_lower": bootstrap["ci_lower"],
            "paired_ci_upper": bootstrap["ci_upper"],
            "macro_source_delta": gate["macro_source_delta"],
            "worst_source_delta": gate["worst_source_delta"],
            "abstained_case_rate": gate["abstained_case_rate"],
            "no_signal_false_positive_rate": gate["no_signal"]["false_positive_rate"],
            "normalized_nonexact_evidence_events": gate[
                "normalized_nonexact_evidence_events"
            ],
            "maximum_candidate_event_count": predecessor["audit"][
                "maximum_candidate_event_count"
            ],
        },
        "conflict_perfect_upper_bound": {
            "candidate_minus_baseline": oracle["bootstrap"][
                "candidate_minus_baseline"
            ],
            "paired_ci_lower": oracle["bootstrap"]["ci_lower"],
            "paired_ci_upper": oracle["bootstrap"]["ci_upper"],
            "semantic_quality_passed": oracle["semantic_passed"],
        },
        "why_judge_recovery_stopped": (
            "perfect scores on both unresolved conflict cases still fail the unchanged paired, "
            "macro-source, and worst-source noninferiority gates"
        ),
        "more_development_cases_can_change_selection": False,
        "safe_local_judge_only_experiment_remaining": False,
        "constraint_blocking_next_quality_experiment": (
            "frozen extractor outputs are the measured quality bottleneck; another judge-only "
            "turn cannot improve them and completed extraction calls cannot be replayed"
        ),
        "authorization_needed_to_continue": (
            "explicit authorization for a new immutable extraction-quality strategy on "
            "development data, distinct from every completed extraction attempt"
        ),
        "evaluation_acceptance_receipt_emitted": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "prospective_shadow_authorized": False,
        "production_mutated": False,
        "cumulative_measured_usage_lower_bound": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": 2,
        "cumulative_conservative_unknown_usage_upper_bound": 220000,
        "predecessor": predecessor["records"],
        "privacy": "sanitized_metrics_hashes_counts_and_blocker_class_no_source_or_event_text",
    }
    report_path = root / "development-nonacceptance-report.json"
    _write_stable_time(report_path, report, "created_at")
    spec = {
        "schema_version": V202_SPEC_VERSION,
        "state": "zero_token_nonacceptance_receipt_completed",
        "created_at": now_iso(),
        "semantic_model_calls_declared": 0,
        "semantic_model_calls_started": 0,
        "extraction_model_calls_started": 0,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "runtime_files": [_record(Path(__file__)), _record(Path(v201.__file__))],
        "frozen_inputs": predecessor["records"],
        "report": _record(report_path),
        "privacy": "sanitized_metrics_hashes_counts_and_blocker_class_no_source_or_event_text",
    }
    spec_path = root / "development-nonacceptance-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    terminal = {
        "schema_version": V202_TERMINAL_VERSION,
        "state": "waiting_for_extraction_quality_strategy_authorization",
        "terminal_at": now_iso(),
        "terminal_reason": "development_quality_target_not_met_token_target_met",
        "terminal_classification": "external_policy_authorization_required",
        "overall_evaluation_complete": False,
        "evaluation_accepted": False,
        "development_quality_passed": False,
        "development_winner_frozen": False,
        "production_amortized_token_target_passed": True,
        "production_amortized_total_token_ratio": 0.236727,
        "conflict_recovery_could_change_verdict": False,
        "more_development_cases_can_change_selection": False,
        "safe_local_judge_only_experiment_remaining": False,
        "additional_alignment_authorized": False,
        "extraction_rerun_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "prospective_shadow_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": {field: 0 for field in USAGE_FIELDS},
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": 2,
        "cumulative_conservative_unknown_usage_upper_bound": 220000,
        "nonacceptance_report": _record(report_path),
        "spec": _record(spec_path),
        "required_user_decision": (
            "authorize a new immutable development extraction-quality strategy or keep the "
            "current candidate rejected"
        ),
        "required_next_artifact_path": str(
            root.parent
            / "development-selection-v5_4-v203-authorized-extraction-quality-strategy"
            / "terminal.json"
        ),
    }
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v202 development nonacceptance")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v202(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "development_quality_passed": terminal[
                    "development_quality_passed"
                ],
                "production_amortized_token_target_passed": terminal[
                    "production_amortized_token_target_passed"
                ],
                "holdout_authorized": terminal["holdout_authorized"],
                "production_mutated": terminal["production_mutated"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
