from __future__ import annotations

"""Record the development-quality blocker after bounded composite convergence."""

import argparse
import json
from pathlib import Path
from typing import Any, Optional, Sequence

from . import app_server_judge_v5_selection_v190_composite_convergence as v190
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS
from .util import now_iso


V191_REPORT_VERSION = "pif_app_server_judge_v5_4_selection_v191_nonacceptance_v1"
V191_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v191_spec_v1"
V191_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v191_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    v190.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v191-extraction-quality-nonacceptance"
).resolve()


class JudgeV5SelectionV191Error(RuntimeError):
    """The v191 nonacceptance receipt cannot be frozen safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v190_stop() -> dict[str, Any]:
    root = v190.DEFAULT_OUTPUT_ROOT
    terminal_path = root / "terminal.json"
    score_path = root / "composite-convergence-score.json"
    bound_path = root / "composite-optimistic-bound.json"
    spec_path = root / "composite-convergence-spec.json"
    terminal = _load_json(terminal_path, "v190 terminal")
    score = _load_json(score_path, "v190 score")
    bound = _load_json(bound_path, "v190 bound")
    spec = _load_json(spec_path, "v190 spec")
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason")
        != "development_composite_empirical_convergence_stop_quality_gate_not_passed"
        or terminal.get("terminal_classification") != "inactive_incomplete_recovery_required"
        or terminal.get("overall_evaluation_complete") is not False
        or terminal.get("judge_protocol_passed") is not True
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("observed_viable_systems") != []
        or terminal.get("empirical_convergence_stop") is not True
        or terminal.get("additional_alignment_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("attempt_usage", {}).get("total_tokens") != 0
        or terminal.get("cumulative_usage_status") != "unknown"
        or terminal.get("cumulative_known_usage_lower_bound", {}).get("total_tokens")
        != 8294282
        or terminal.get("cumulative_unknown_usage_turn_count") != 1
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 120000
        or terminal.get("score") != _record(score_path)
        or terminal.get("optimistic_remaining_bound") != _record(bound_path)
        or terminal.get("spec") != _record(spec_path)
        or score.get("observed_viable_systems") != []
        or score.get("empirical_convergence_stop") is not True
        or score.get("source_critical_sample", {}).get("met_required_average") is not False
        or spec.get("state") != "zero_token_postprocess_completed"
        or spec.get("semantic_model_calls_started") != 0
        or spec.get("extraction_model_calls_started") != 0
    ):
        raise JudgeV5SelectionV191Error("v190 convergence-stop contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV191Error("v190 runtime binding drifted")
    v189_success = v190._validate_v189_success()
    if terminal.get("cumulative_known_usage_lower_bound") != v189_success["terminal"].get(
        "cumulative_known_usage_lower_bound"
    ):
        raise JudgeV5SelectionV191Error("v190 cumulative accounting drifted")
    return {
        "root": root,
        "terminal": terminal,
        "score": score,
        "bound": bound,
        "spec": spec,
        "records": {
            "terminal": _record(terminal_path),
            "score": _record(score_path),
            "bound": _record(bound_path),
            "spec": _record(spec_path),
        },
    }


def freeze_v191(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v191 terminal")
    predecessor = _validate_v190_stop()
    leader_id = predecessor["score"]["observed_quality_leader"]
    leader = predecessor["score"]["systems"][leader_id]
    gate = leader["gate"]
    failed_checks = sorted(
        check for check, passed in gate["checks"].items() if not passed
    )
    report = {
        "schema_version": V191_REPORT_VERSION,
        "created_at": now_iso(),
        "blocker_class": "measured_development_semantic_quality_shortfall",
        "infrastructure_failure": False,
        "judge_protocol_failure": False,
        "token_target_passed": leader["cost"]["passed_lte_0_28"],
        "production_amortized_total_token_ratio": leader["cost"][
            "production_amortized_total_token_ratio"
        ],
        "semantic_quality_passed": gate["semantic_passed"],
        "failed_semantic_checks": failed_checks,
        "observed_quality_leader": leader_id,
        "quality": {
            "scored_case_count": predecessor["score"]["case_count"],
            "remaining_unjudged_case_count": predecessor["score"]["remaining_case_count"],
            "baseline_f1": gate["bootstrap"]["baseline_f1"],
            "candidate_f1": gate["bootstrap"]["candidate_f1"],
            "candidate_minus_baseline": gate["bootstrap"]["candidate_minus_baseline"],
            "paired_ci_lower": gate["bootstrap"]["ci_lower"],
            "paired_ci_upper": gate["bootstrap"]["ci_upper"],
            "macro_source_delta": gate["macro_source_delta"],
            "worst_source_delta": gate["worst_source_delta"],
            "no_signal_false_positive_rate": gate["no_signal"]["false_positive_rate"],
            "normalized_nonexact_evidence_events": gate[
                "normalized_nonexact_evidence_events"
            ],
        },
        "structural_cap": {
            "maximum_allowed_events_per_segment": 32,
            "observed_maximum_exact_union_event_count": leader[
                "max_exact_union_event_count"
            ],
            "passed": leader["max_exact_union_event_count"] <= 32,
        },
        "source_critical_convergence_sample": predecessor["score"][
            "source_critical_sample"
        ],
        "perfect_remaining_case_bound_still_open": predecessor["score"][
            "perfect_remaining_case_bound_still_open"
        ],
        "perfect_bound_interpretation": (
            "mathematical possibility under candidate_f1_1_baseline_f1_0 for every remaining "
            "case is not measured acceptance evidence"
        ),
        "why_more_judge_sampling_stopped": (
            "the preselected most-demanding-source case realized a negative delta and missed "
            "the predeclared average delta required to recover the worst-source gate"
        ),
        "safe_local_experiment_remaining_under_current_constraints": False,
        "constraint_blocking_next_quality_experiment": (
            "extraction reruns and production mutation are prohibited; judge-only calls cannot "
            "change the frozen extractor outputs"
        ),
        "authorization_needed_to_continue": (
            "explicit authorization for a new immutable extraction-quality strategy or a changed "
            "acceptance contract; untouched holdout remains prohibited"
        ),
        "evaluation_acceptance_receipt_emitted": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "prospective_shadow_authorized": False,
        "production_mutated": False,
        "predecessor": predecessor["records"],
        "privacy": "sanitized_metrics_hashes_counts_and_blocker_class_no_source_or_event_text",
    }
    report_path = root / "development-nonacceptance-report.json"
    _write_stable_time(report_path, report, "created_at")
    spec = {
        "schema_version": V191_SPEC_VERSION,
        "state": "zero_token_nonacceptance_receipt_completed",
        "created_at": now_iso(),
        "semantic_model_calls_declared": 0,
        "semantic_model_calls_started": 0,
        "extraction_model_calls_started": 0,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v190.__file__)),
        ],
        "frozen_inputs": predecessor["records"],
        "report": _record(report_path),
        "privacy": "sanitized_metrics_hashes_counts_and_blocker_class_no_source_or_event_text",
    }
    spec_path = root / "development-nonacceptance-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    terminal = {
        "schema_version": V191_TERMINAL_VERSION,
        "state": "waiting_for_extraction_quality_strategy_authorization",
        "terminal_at": now_iso(),
        "terminal_reason": "development_quality_target_not_met_token_target_met",
        "terminal_classification": "external_policy_authorization_required",
        "overall_evaluation_complete": False,
        "evaluation_accepted": False,
        "judge_protocol_passed": True,
        "development_winner_frozen": False,
        "semantic_quality_passed": False,
        "production_amortized_token_target_passed": True,
        "production_amortized_total_token_ratio": report[
            "production_amortized_total_token_ratio"
        ],
        "safe_local_experiment_remaining_under_current_constraints": False,
        "additional_alignment_authorized": False,
        "extraction_rerun_authorized": False,
        "holdout_authorized": False,
        "prospective_shadow_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "attempt_usage_status": "complete",
        "attempt_usage": {field: 0 for field in USAGE_FIELDS},
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": 1,
        "cumulative_conservative_unknown_usage_upper_bound": 120000,
        "nonacceptance_report": _record(report_path),
        "spec": _record(spec_path),
        "required_user_decision": (
            "authorize a new immutable extraction-quality experiment or keep the current candidate rejected"
        ),
        "required_next_artifact_path": str(
            root.parent
            / "development-selection-v5_4-v192-authorized-extraction-quality-strategy"
            / "terminal.json"
        ),
    }
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v191 development nonacceptance")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v191(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "semantic_quality_passed": terminal["semantic_quality_passed"],
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
