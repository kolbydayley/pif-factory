from __future__ import annotations

"""Apply the frozen composite gates after the one-case v189 diagnostic."""

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from . import app_server_judge_v5_selection_v188_composite_postprocess as v188
from . import app_server_judge_v5_selection_v189_composite_minimal_alignment as v189
from .app_server_dev_selection import BASELINE_REPAIRED_SYSTEM, QUALITY_GATES, evaluate_semantic_gates
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _aggregate_usage,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import _validate_usage
from .util import now_iso


V190_SCORE_VERSION = "pif_app_server_judge_v5_4_selection_v190_composite_score_v1"
V190_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v190_spec_v1"
V190_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v190_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    v189.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v190-composite-convergence"
).resolve()


class JudgeV5SelectionV190Error(RuntimeError):
    """The v190 convergence checkpoint cannot be frozen safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v189_success() -> dict[str, Any]:
    root = v189.DEFAULT_OUTPUT_ROOT
    terminal_path = root / "terminal.json"
    spec_path = root / "composite-minimal-alignment-spec.json"
    output_path = root / "composite-minimal-alignment.private.json"
    turn_root = root / "turns" / v189.TURN_NAME.replace("_", "-")
    sidecar_path = turn_root / "sidecar.json"
    capacity_path = turn_root / "capacity.json"
    audit_path = turn_root / "structural-projection-audit.json"
    projected_path = turn_root / "alignment-projected.private.json"
    terminal = _load_json(terminal_path, "v189 terminal")
    spec = _load_json(spec_path, "v189 spec")
    normalized = _load_json(output_path, "v189 normalized alignment")
    sidecar = _load_json(sidecar_path, "v189 sidecar")
    capacity = _load_json(capacity_path, "v189 capacity")
    audit = _load_json(audit_path, "v189 projection audit")
    usage = _validate_usage(sidecar)
    predecessor = v189._validate_v188_authorization()
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v189_minimal_composite_alignment_completed_postprocess_authorized"
        or terminal.get("selected_case_id") != predecessor["next_case"]["case_id"]
        or terminal.get("selected_source_id") != predecessor["next_case"]["critical_source_id"]
        or terminal.get("alignment_output") != _record(output_path)
        or terminal.get("postprocess_authorized") is not True
        or terminal.get("additional_alignment_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != usage
        or spec.get("state") != "frozen_before_model_calls"
        or spec.get("turn_plan") != [v189.TURN_NAME]
        or spec.get("retry_count_per_turn") != 0
        or spec.get("new_judge_prompt_or_rubric_created") is not False
        or spec.get("extraction_model_calls_allowed") is not False
        or len(normalized.get("cases") or []) != 1
        or normalized.get("cases", [{}])[0].get("case_id") != terminal.get("selected_case_id")
        or sidecar.get("state") != "completed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("cli_version") != "0.144.1"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("rate_limit_reached_type") is not None
        or audit.get("dropped_nonexact_span_count") != 0
        or audit.get("semantic_alignment_pairs_changed") is not False
        or audit.get("semantic_checklist_decisions_changed") is not False
        or audit.get("semantic_equivalence_groups_changed") is not False
        or audit.get("witness_assignments_changed") is not False
        or audit.get("original_validation_errors") != []
        or not projected_path.is_file()
    ):
        raise JudgeV5SelectionV190Error("v189 success contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV190Error("v189 runtime binding drifted")
    accounting = _aggregate_usage([sidecar])
    if accounting.get("usage") != usage:
        raise JudgeV5SelectionV190Error("v189 accounting drifted")
    expected_total = (
        predecessor["terminal"]["cumulative_known_usage_lower_bound"]["total_tokens"]
        + usage["total_tokens"]
    )
    if (
        terminal.get("cumulative_known_usage_lower_bound", {}).get("total_tokens")
        != expected_total
        or terminal.get("cumulative_unknown_usage_turn_count") != 1
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 120000
    ):
        raise JudgeV5SelectionV190Error("v189 cumulative accounting drifted")
    return {
        "root": root,
        "terminal": terminal,
        "terminal_record": _record(terminal_path),
        "spec": spec,
        "spec_record": _record(spec_path),
        "normalized": normalized,
        "normalized_record": _record(output_path),
        "turn_records": {
            "capacity": _record(capacity_path),
            "sidecar": _record(sidecar_path),
            "projection_audit": _record(audit_path),
            "projected_output": _record(projected_path),
        },
        "predecessor": predecessor,
    }


def freeze_v190(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v190 terminal")
    success = _validate_v189_success()
    sources = v186._selection_sources()
    combinations, cost_contract = v188._candidate_combinations(sources=sources)
    eligible = [row for row in combinations if row["cost_and_cap_eligible"]]
    composite_sources = v188._with_composite_systems(
        sources=sources, combinations=combinations
    )
    aligned_cases = [
        *v189.v186_completed_cases(success["predecessor"]),
        *success["normalized"]["cases"],
    ]
    if len(aligned_cases) != 18 or len(
        {str(row["case_id"]) for row in aligned_cases}
    ) != 18:
        raise JudgeV5SelectionV190Error("v190 aligned-case coverage drifted")
    consensus, selected_case_ids = v186._build_consensus(
        aligned_cases=aligned_cases, sources=composite_sources
    )
    if len(selected_case_ids) != 22:
        raise JudgeV5SelectionV190Error("v190 scored-case coverage drifted")
    consensus_path = root / "composite-consensus.private.json"
    _write_immutable(consensus_path, consensus)
    score = v186._score_subset(
        consensus=consensus,
        selected_case_ids=selected_case_ids,
        sources=composite_sources,
    )
    score_path = root / "composite-score.private.json"
    _write_immutable(score_path, score)
    gates = {
        row["system_id"]: evaluate_semantic_gates(
            score, candidate_system_id=row["system_id"]
        )
        for row in eligible
    }
    observed_viable = sorted(
        system_id for system_id, gate in gates.items() if gate["passed"]
    )
    bound = v188._candidate_favoring_bound(
        score=score,
        selected_case_ids=selected_case_ids,
        sources=composite_sources,
        eligible=eligible,
    )
    bound_path = root / "composite-optimistic-bound.json"
    _write_immutable(bound_path, bound)
    partition = success["predecessor"]["v187"]["predecessor"]["stop"]["predecessor"][
        "partition"
    ]
    partition_rows = [
        partition["adopted_case"],
        *[row for phase in partition["phases"] for row in phase],
    ]
    next_case = v188._next_case(
        bound=bound,
        score=score,
        selected_case_ids=selected_case_ids,
        sources=sources,
        partition_rows=partition_rows,
    )
    leader = max(
        gates,
        key=lambda system_id: (
            gates[system_id]["bootstrap"]["candidate_f1"],
            system_id,
        ),
    )
    baseline_case = next(
        row
        for row in score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
        if row["case_id"] == success["terminal"]["selected_case_id"]
    )
    candidate_case = next(
        row
        for row in score["systems"][leader]["cases"]
        if row["case_id"] == success["terminal"]["selected_case_id"]
    )
    realized_delta = round(candidate_case["f1"] - baseline_case["f1"], 6)
    required_delta = float(
        success["predecessor"]["next_case"][
            "required_average_remaining_delta_for_worst_source_gate"
        ]
    )
    source_critical_sample_met_required_average = realized_delta >= required_delta
    report = {
        "schema_version": V190_SCORE_VERSION,
        "created_at": now_iso(),
        "quality_gates": QUALITY_GATES,
        "case_count": len(selected_case_ids),
        "remaining_case_count": len(sources["mapping"]["cases"]) - len(selected_case_ids),
        "cost_and_cap_eligible_combination_count": len(eligible),
        "systems": {
            row["system_id"]: {
                "arms": row["arms"],
                "cost": row["cost"],
                "max_exact_union_event_count": row["max_exact_union_event_count"],
                "gate": v188._sanitize_gate(gates[row["system_id"]]),
            }
            for row in eligible
        },
        "observed_quality_leader": leader,
        "observed_viable_systems": observed_viable,
        "any_observed_composite_passed": bool(observed_viable),
        "source_critical_sample": {
            "case_id": success["terminal"]["selected_case_id"],
            "source_id": success["terminal"]["selected_source_id"],
            "baseline_f1": baseline_case["f1"],
            "candidate_f1": candidate_case["f1"],
            "realized_candidate_minus_baseline": realized_delta,
            "predeclared_required_average_remaining_delta": required_delta,
            "met_required_average": source_critical_sample_met_required_average,
        },
        "optimistic_remaining_bound": _record(bound_path),
        "perfect_remaining_case_bound_still_open": bound[
            "any_system_can_become_semantically_viable"
        ],
        "empirical_convergence_stop": bool(
            not observed_viable and not source_critical_sample_met_required_average
        ),
        "next_case_if_not_stopped": next_case,
        "production_mutated": False,
    }
    report_path = root / "composite-convergence-score.json"
    _write_stable_time(report_path, report, "created_at")
    spec = {
        "schema_version": V190_SPEC_VERSION,
        "state": "zero_token_postprocess_completed",
        "created_at": now_iso(),
        "semantic_model_calls_declared": 0,
        "semantic_model_calls_started": 0,
        "extraction_model_calls_started": 0,
        "new_judge_prompt_or_rubric_created": False,
        "convergence_rule": (
            "stop additional alignment when the v188-preselected source-critical case fails "
            "to meet the predeclared average delta required for the worst-source gate"
        ),
        "deterministic_operations": [
            "verify_immutable_v189_turn_and_usage",
            "union_candidate_events_by_exact_canonical_identity",
            "apply_frozen_cost_cap_and_semantic_gates",
            "compare_realized_source_critical_delta_to_v188_predeclared_requirement",
            "compute_deliberately_candidate_favoring_remaining_case_bound",
        ],
        "semantic_regex_or_keyword_rules_used": False,
        "semantic_similarity_or_embeddings_used": False,
        "semantic_routing_or_pruning_used": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v189.__file__)),
            _record(Path(v188.__file__)),
            _record(Path(v186.__file__)),
        ],
        "frozen_inputs": {
            "v189_terminal": success["terminal_record"],
            "v189_spec": success["spec_record"],
            "v189_alignment": success["normalized_record"],
            "v189_turn": success["turn_records"],
            "v188_terminal": success["predecessor"]["records"]["terminal"],
            "private_mapping": sources["mapping_record"],
            "membership_index": sources["membership_record"],
            "cost_contract": {
                key: value
                for key, value in cost_contract.items()
                if key.endswith("_path") or key.endswith("_sha256")
            },
            "consensus": _record(consensus_path),
            "private_score": _record(score_path),
            "sanitized_score": _record(report_path),
            "optimistic_bound": _record(bound_path),
        },
        "privacy": "private_witness_scores_separate_sanitized_hash_count_metric_reports",
    }
    spec_path = root / "composite-convergence-spec.json"
    _write_stable_time(spec_path, spec, "created_at")

    if observed_viable:
        reason = "composite_core_candidate_passed_end_to_end_enrichment_accounting_required"
        empirical_stop = False
        additional_alignment = False
        required_next = (
            root.parent
            / "development-selection-v5_4-v191-composite-end-to-end-accounting"
            / "terminal.json"
        )
        shortest = "audit enrichment and full candidate accounting before freezing the winner"
    elif not source_critical_sample_met_required_average:
        reason = "development_composite_empirical_convergence_stop_quality_gate_not_passed"
        empirical_stop = True
        additional_alignment = False
        required_next = (
            root.parent
            / "development-selection-v5_4-v191-extraction-quality-nonacceptance"
            / "terminal.json"
        )
        shortest = (
            "record development nonacceptance; the bounded source-critical sample missed its "
            "predeclared required delta and extraction reruns remain prohibited"
        )
    elif bound["any_system_can_become_semantically_viable"]:
        reason = "composite_viability_unresolved_one_additional_case_authorized"
        empirical_stop = False
        additional_alignment = True
        required_next = (
            root.parent
            / "development-selection-v5_4-v191-composite-minimal-alignment"
            / "terminal.json"
        )
        shortest = "judge only the next preselected case, then apply the same convergence rule"
    else:
        reason = "composite_noninferiority_not_recoverable_under_cost_and_cap_gates"
        empirical_stop = True
        additional_alignment = False
        required_next = (
            root.parent
            / "development-selection-v5_4-v191-extraction-quality-nonacceptance"
            / "terminal.json"
        )
        shortest = "record development nonacceptance under the unchanged gates"
    terminal = {
        "schema_version": V190_TERMINAL_VERSION,
        "state": "inactive" if not additional_alignment else "completed",
        "terminal_at": now_iso(),
        "terminal_reason": reason,
        "terminal_classification": (
            "inactive_incomplete_recovery_required"
            if not additional_alignment
            else "active_development_recovery_required"
        ),
        "overall_evaluation_complete": False,
        "judge_protocol_passed": True,
        "development_winner_frozen": False,
        "observed_viable_systems": observed_viable,
        "observed_quality_leader": leader,
        "perfect_remaining_case_bound_still_open": bound[
            "any_system_can_become_semantically_viable"
        ],
        "empirical_convergence_stop": empirical_stop,
        "additional_alignment_authorized": additional_alignment,
        "authorized_next_case": next_case if additional_alignment else None,
        "holdout_authorized": False,
        "prospective_shadow_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "attempt_usage_status": "complete",
        "attempt_usage": {key: 0 for key in success["terminal"]["usage"]},
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": success["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": 1,
        "cumulative_conservative_unknown_usage_upper_bound": 120000,
        "score": _record(report_path),
        "optimistic_remaining_bound": _record(bound_path),
        "spec": _record(spec_path),
        "shortest_path_to_holdout_verdict": shortest,
        "required_next_artifact_path": str(required_next),
    }
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v190 composite convergence checkpoint")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v190(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "observed_viable_systems": terminal["observed_viable_systems"],
                "empirical_convergence_stop": terminal["empirical_convergence_stop"],
                "additional_alignment_authorized": terminal[
                    "additional_alignment_authorized"
                ],
                "holdout_authorized": terminal["holdout_authorized"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
