from __future__ import annotations

"""Score cap-safe exact-identity arm composites without new model calls."""

import argparse
import copy
import itertools
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from . import app_server_judge_v5_selection_v187_composite_stratified_diagnostic as v187
from .app_server_dev_selection import (
    BASELINE_REPAIRED_SYSTEM,
    QUALITY_GATES,
    evaluate_semantic_gates,
    load_exact_cost_contract,
    production_amortized_cost,
    source_cluster_paired_bootstrap,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _aggregate_usage,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .util import now_iso


V188_SCORE_VERSION = "pif_app_server_judge_v5_4_selection_v188_composite_score_v1"
V188_BOUND_VERSION = "pif_app_server_judge_v5_4_selection_v188_composite_bound_v1"
V188_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v188_spec_v1"
V188_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v188_terminal_v1"

ARM_IDS = (
    "batch_3_new_thread",
    "batch_3_same_thread",
    "batch_5_new_thread",
    "batch_8_new_thread",
    "batch_8_same_thread",
)
MAX_EVENTS_PER_SEGMENT = 32
DEFAULT_OUTPUT_ROOT = (
    v187.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v188-composite-postprocess"
).resolve()


class JudgeV5SelectionV188Error(RuntimeError):
    """The zero-token composite checkpoint cannot be frozen safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return {field: sum(int(row[field]) for row in rows) for field in USAGE_FIELDS}


def _validate_v187_success() -> dict[str, Any]:
    root = v187.DEFAULT_OUTPUT_ROOT
    terminal_path = root / "terminal.json"
    spec_path = root / "composite-stratified-diagnostic-spec.json"
    output_path = root / "composite-stratified-alignment.private.json"
    terminal = _load_json(terminal_path, "v187 terminal")
    spec = _load_json(spec_path, "v187 spec")
    normalized = _load_json(output_path, "v187 normalized alignment")
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v187_composite_stratified_alignment_completed_postprocess_authorized"
        or terminal.get("postprocess_authorized") is not True
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("selected_case_ids") != list(v187.SELECTED_CASE_IDS)
        or terminal.get("selected_source_ids") != list(v187.SELECTED_SOURCE_IDS)
        or terminal.get("alignment_output") != _record(output_path)
        or spec.get("state") != "frozen_before_model_calls"
        or spec.get("turn_plan") != list(v187.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("existing_extraction_outputs_only") is not True
        or spec.get("extraction_model_calls_allowed") is not False
        or spec.get("new_judge_prompt_or_rubric_created") is not False
        or normalized.get("case_count") != 2
        or [row.get("case_id") for row in normalized.get("cases") or []]
        != sorted(v187.SELECTED_CASE_IDS)
    ):
        raise JudgeV5SelectionV188Error("v187 success contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV188Error("v187 runtime binding drifted")
    sidecars = []
    turn_records = []
    for turn_name in v187.TURN_NAMES:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        sidecar_path = turn_root / "sidecar.json"
        capacity_path = turn_root / "capacity.json"
        audit_path = turn_root / "structural-projection-audit.json"
        projected_path = turn_root / "alignment-projected.private.json"
        sidecar = _load_json(sidecar_path, f"v187 {turn_name} sidecar")
        capacity = _load_json(capacity_path, f"v187 {turn_name} capacity")
        audit = _load_json(audit_path, f"v187 {turn_name} projection audit")
        usage = _validate_usage(sidecar)
        if (
            sidecar.get("state") != "completed"
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
            raise JudgeV5SelectionV188Error("v187 turn contract drifted")
        sidecars.append(sidecar)
        turn_records.append(
            {
                "turn_name": turn_name,
                "capacity": _record(capacity_path),
                "sidecar": _record(sidecar_path),
                "projection_audit": _record(audit_path),
                "projected_output": _record(projected_path),
                "usage": usage,
            }
        )
    accounting = _aggregate_usage(sidecars)
    if accounting.get("usage_status") != "complete" or accounting.get("usage") != terminal.get(
        "usage"
    ):
        raise JudgeV5SelectionV188Error("v187 accounting drifted")
    predecessor = v187._validate_v186_checkpoint()
    expected_cumulative = _sum_usage(
        [predecessor["cumulative_known_lower_bound"], accounting["usage"]]
    )
    if (
        terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative
        or terminal.get("cumulative_unknown_usage_turn_count") != 1
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 120000
    ):
        raise JudgeV5SelectionV188Error("v187 cumulative accounting drifted")
    return {
        "root": root,
        "terminal": terminal,
        "terminal_record": _record(terminal_path),
        "spec": spec,
        "spec_record": _record(spec_path),
        "normalized": normalized,
        "normalized_record": _record(output_path),
        "turn_records": turn_records,
        "predecessor": predecessor,
    }


def _component_system_id(arm_id: str) -> str:
    if arm_id not in ARM_IDS:
        raise JudgeV5SelectionV188Error("unknown clean arm")
    return f"arm:{arm_id}:normalized"


def _composite_system_id(arms: Sequence[str]) -> str:
    return "composite:" + "+".join(arms) + ":exact_union"


def _candidate_combinations(
    *, sources: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], Mapping[str, Any]]:
    reports = v186._arm_reports(sources)
    contract = load_exact_cost_contract(
        Path(sources["contract"]["selection_inputs"]["context_usage_recovery"]["path"]),
        verify_sidecars=True,
    )
    membership = sources["membership"]
    rows = []
    for size in range(2, len(ARM_IDS) + 1):
        for arms in itertools.combinations(ARM_IDS, size):
            usage = _sum_usage([reports[arm]["usage"] for arm in arms])
            cost = production_amortized_cost(
                arm_usage=usage,
                arm_segments=int(reports[arms[0]]["validated_segments"]),
                contract=contract,
            )
            counts = {}
            for segment_id in membership["segment_order"]:
                event_hashes = {
                    event_hash
                    for arm in arms
                    for event_hash in membership["system_cases"][_component_system_id(arm)][
                        segment_id
                    ]["event_hashes"]
                }
                counts[segment_id] = len(event_hashes)
            max_count = max(counts.values())
            cap_hit_segments = sorted(
                segment_id for segment_id, count in counts.items() if count > MAX_EVENTS_PER_SEGMENT
            )
            rows.append(
                {
                    "system_id": _composite_system_id(arms),
                    "arms": list(arms),
                    "usage": usage,
                    "cost": cost,
                    "max_exact_union_event_count": max_count,
                    "cap_hit_segment_count": len(cap_hit_segments),
                    "cap_hit_segments": cap_hit_segments,
                    "cost_and_cap_eligible": bool(
                        cost["passed_lte_0_28"] and not cap_hit_segments
                    ),
                }
            )
    return rows, contract


def _with_composite_systems(
    *, sources: Mapping[str, Any], combinations: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    selected = [row for row in combinations if row["cost_and_cap_eligible"]]
    mapping = copy.deepcopy(sources["mapping"])
    membership = copy.deepcopy(sources["membership"])
    case_by_segment = {str(row["case_key"]): row for row in mapping["cases"]}
    for combo in selected:
        system_id = str(combo["system_id"])
        component_ids = {_component_system_id(arm) for arm in combo["arms"]}
        membership["systems"][system_id] = {
            "kind": "deterministic_exact_identity_union",
            "component_system_ids": sorted(component_ids),
        }
        membership["system_cases"][system_id] = {}
        for segment_id in membership["segment_order"]:
            component_cases = [
                membership["system_cases"][component_id][segment_id]
                for component_id in sorted(component_ids)
            ]
            event_hashes = sorted(
                {
                    event_hash
                    for system_case in component_cases
                    for event_hash in system_case["event_hashes"]
                }
            )
            case = case_by_segment[str(segment_id)]
            exact_hashes = set()
            for witness in case["witnesses"]:
                provenance = witness["provenance"]
                matching = [
                    row
                    for row in provenance.get("memberships") or []
                    if row.get("system_id") in component_ids
                ]
                if not matching:
                    continue
                exact = any(row.get("submitted_evidence_exact") is True for row in matching)
                if exact:
                    exact_hashes.add(provenance["canonical_event_sha256"])
                provenance["memberships"].append(
                    {
                        "system_id": system_id,
                        "submitted_evidence_exact": exact,
                        "deterministic_union_component_count": len(component_ids),
                    }
                )
            if not set(event_hashes).issubset(
                {
                    witness["provenance"]["canonical_event_sha256"]
                    for witness in case["witnesses"]
                }
            ):
                raise JudgeV5SelectionV188Error("composite event hash lacks witness provenance")
            membership["system_cases"][system_id][segment_id] = {
                "event_hashes": event_hashes,
                "exact_evidence_event_count": len(set(event_hashes) & exact_hashes),
                "status": "coded" if event_hashes else "no_submitted_events",
                "submitted_event_count": len(event_hashes),
            }
    return {**sources, "mapping": mapping, "membership": membership}


def _sanitize_gate(gate: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "semantic_passed": gate["passed"],
        "checks": gate["checks"],
        "bootstrap": gate["bootstrap"],
        "macro_source_delta": gate["macro_source_delta"],
        "worst_source_delta": gate["worst_source_delta"],
        "by_source": gate["by_source"],
        "no_signal": gate["no_signal"],
        "abstained_case_rate": gate["abstained_case_rate"],
        "normalized_nonexact_evidence_events": gate[
            "normalized_nonexact_evidence_events"
        ],
    }


def _candidate_favoring_bound(
    *,
    score: Mapping[str, Any],
    selected_case_ids: set[str],
    sources: Mapping[str, Any],
    eligible: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    remaining = [
        row
        for row in sources["mapping"]["cases"]
        if str(row["case_id"]) not in selected_case_ids
    ]
    baseline_rows = {
        row["segment_id"]: row
        for row in score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
    }
    bounds = {}
    for combo in eligible:
        system_id = str(combo["system_id"])
        candidate_rows = {
            row["segment_id"]: row for row in score["systems"][system_id]["cases"]
        }
        rows = [
            {
                "segment_id": segment_id,
                "source_id": baseline_rows[segment_id]["source_id"],
                "density_stratum": baseline_rows[segment_id]["density_stratum"],
                "baseline_f1": baseline_rows[segment_id]["f1"],
                "candidate_f1": candidate_rows[segment_id]["f1"],
            }
            for segment_id in baseline_rows
        ]
        rows.extend(
            {
                "segment_id": row["case_key"],
                "source_id": row["case_provenance"]["source_id"],
                "density_stratum": row["case_provenance"]["density_stratum"],
                "baseline_f1": 0.0,
                "candidate_f1": 1.0,
            }
            for row in remaining
        )
        bootstrap = source_cluster_paired_bootstrap(rows)
        by_source: dict[str, list[float]] = defaultdict(list)
        for row in rows:
            by_source[str(row["source_id"])].append(
                float(row["candidate_f1"]) - float(row["baseline_f1"])
            )
        source_deltas = {
            source_id: sum(values) / len(values) for source_id, values in by_source.items()
        }
        macro = sum(source_deltas.values()) / len(source_deltas)
        worst = min(source_deltas.values())
        current_no_signal = [
            row for row in candidate_rows.values() if row["density_stratum"] == "no_signal"
        ]
        current_false_positives = sum(
            bool(row["supported_units"] > 0 or row["case_abstained_worst_case"])
            for row in current_no_signal
        )
        remaining_no_signal = sum(
            row["case_provenance"]["density_stratum"] == "no_signal" for row in remaining
        )
        no_signal_total = len(current_no_signal) + remaining_no_signal
        no_signal_rate = (
            current_false_positives / no_signal_total if no_signal_total else 1.0
        )
        abstention_rate = len(score["abstained_cases"]) / (
            len(selected_case_ids) + len(remaining)
        )
        current_nonexact = sum(
            int(row["submitted_nonexact_evidence_events"])
            for row in candidate_rows.values()
        )
        checks = {
            "paired_bootstrap_noninferiority": bootstrap["ci_lower"]
            >= -QUALITY_GATES["max_paired_f1_drop"],
            "macro_source_noninferiority": macro
            >= -QUALITY_GATES["max_macro_f1_drop"],
            "worst_source_noninferiority": worst
            >= -QUALITY_GATES["max_source_f1_drop"],
            "no_signal_worst_case": no_signal_rate
            <= QUALITY_GATES["max_no_signal_false_positive_rate"],
            "abstention_rate": abstention_rate
            <= QUALITY_GATES["max_abstained_case_rate"],
            "normalized_exact_evidence": current_nonexact == 0,
        }
        bounds[system_id] = {
            "assumption": "every_remaining_case_candidate_f1_1_baseline_f1_0_no_new_abstention_or_false_positive",
            "bootstrap": bootstrap,
            "macro_source_delta": round(macro, 6),
            "worst_source_delta": round(worst, 6),
            "source_deltas": {
                key: round(value, 6) for key, value in sorted(source_deltas.items())
            },
            "no_signal_false_positive_rate": round(no_signal_rate, 6),
            "abstained_case_rate": round(abstention_rate, 6),
            "normalized_nonexact_evidence_events": current_nonexact,
            "checks": checks,
            "could_pass_all_frozen_semantic_checks": all(checks.values()),
        }
    viable = sorted(
        system_id
        for system_id, row in bounds.items()
        if row["could_pass_all_frozen_semantic_checks"]
    )
    return {
        "schema_version": V188_BOUND_VERSION,
        "completed_case_count": len(selected_case_ids),
        "remaining_case_count": len(remaining),
        "bound_is_deliberately_candidate_favoring": True,
        "remaining_case_assumption": (
            "candidate_f1_1_baseline_f1_0_no_new_abstention_no_new_false_positive_exact_evidence"
        ),
        "systems": bounds,
        "systems_that_can_still_pass": viable,
        "any_system_can_become_semantically_viable": bool(viable),
        "more_development_alignment_can_change_selection_viability": bool(viable),
    }


def _next_case(
    *,
    bound: Mapping[str, Any],
    score: Mapping[str, Any],
    selected_case_ids: set[str],
    sources: Mapping[str, Any],
    partition_rows: Sequence[Mapping[str, Any]],
) -> Optional[dict[str, Any]]:
    viable = bound["systems_that_can_still_pass"]
    if not viable:
        return None
    leader = max(
        viable,
        key=lambda system_id: (
            score["systems"][system_id]["macro_f1"],
            system_id,
        ),
    )
    baseline = {
        row["source_id"]: []
        for row in score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
    }
    candidate = {key: [] for key in baseline}
    baseline_rows = score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
    candidate_by_segment = {
        row["segment_id"]: row for row in score["systems"][leader]["cases"]
    }
    for row in baseline_rows:
        baseline[row["source_id"]].append(float(row["f1"]))
        candidate[row["source_id"]].append(
            float(candidate_by_segment[row["segment_id"]]["f1"])
        )
    remaining = [
        row
        for row in sources["mapping"]["cases"]
        if str(row["case_id"]) not in selected_case_ids
    ]
    remaining_by_source: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in remaining:
        remaining_by_source[str(row["case_provenance"]["source_id"])].append(row)
    requirements = {}
    for source_id, rows in remaining_by_source.items():
        current_deltas = [
            candidate_value - baseline_value
            for candidate_value, baseline_value in zip(
                candidate.get(source_id, []), baseline.get(source_id, []), strict=True
            )
        ]
        final_count = len(current_deltas) + len(rows)
        required = (
            -QUALITY_GATES["max_source_f1_drop"] * final_count - sum(current_deltas)
        ) / len(rows)
        requirements[source_id] = required
    critical_source = max(requirements, key=lambda key: (requirements[key], key))
    prompt_by_case = {str(row["case_id"]): row for row in partition_rows}
    eligible_rows = remaining_by_source[critical_source]
    selected = min(
        eligible_rows,
        key=lambda row: (
            int(prompt_by_case[str(row["case_id"])]["prompt_bytes"]),
            str(row["case_id"]),
        ),
    )
    prompt_row = prompt_by_case[str(selected["case_id"])]
    return {
        "system_id": leader,
        "critical_source_id": critical_source,
        "required_average_remaining_delta_for_worst_source_gate": round(
            requirements[critical_source], 6
        ),
        "case_id": selected["case_id"],
        "prompt_bytes": prompt_row["prompt_bytes"],
        "schema_bytes": prompt_row["schema_bytes"],
        "witness_count": prompt_row["witness_count"],
        "selection_rule": "smallest_prompt_remaining_case_in_most_demanding_source_for_best_bound_viable_composite",
    }


def freeze_v188(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v188 terminal")
    v187_success = _validate_v187_success()
    sources = v186._selection_sources()
    combinations, cost_contract = _candidate_combinations(sources=sources)
    eligible = [row for row in combinations if row["cost_and_cap_eligible"]]
    if not eligible:
        raise JudgeV5SelectionV188Error("no cost-and-cap-safe composites exist")
    composite_sources = _with_composite_systems(
        sources=sources, combinations=combinations
    )
    previous = v186._completed_alignment_sets(
        v187_success["predecessor"]["stop"]
    )["all_completed"]
    aligned_cases = [*previous, *v187_success["normalized"]["cases"]]
    if len(aligned_cases) != 17 or len(
        {str(row["case_id"]) for row in aligned_cases}
    ) != 17:
        raise JudgeV5SelectionV188Error("v188 aligned-case coverage drifted")
    consensus, selected_case_ids = v186._build_consensus(
        aligned_cases=aligned_cases, sources=composite_sources
    )
    if len(selected_case_ids) != 21:
        raise JudgeV5SelectionV188Error("v188 scored-case coverage drifted")
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
    bound = _candidate_favoring_bound(
        score=score,
        selected_case_ids=selected_case_ids,
        sources=composite_sources,
        eligible=eligible,
    )
    bound_path = root / "composite-optimistic-bound.json"
    _write_immutable(bound_path, bound)
    partition = v187_success["predecessor"]["stop"]["predecessor"]["partition"]
    partition_rows = [
        partition["adopted_case"],
        *[row for phase in partition["phases"] for row in phase],
    ]
    next_case = _next_case(
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
    report = {
        "schema_version": V188_SCORE_VERSION,
        "created_at": now_iso(),
        "quality_gates": QUALITY_GATES,
        "case_count": len(selected_case_ids),
        "remaining_case_count": len(sources["mapping"]["cases"]) - len(selected_case_ids),
        "combination_count": len(combinations),
        "cost_and_cap_eligible_combination_count": len(eligible),
        "combinations": [
            {
                **row,
                "gate": _sanitize_gate(gates[row["system_id"]])
                if row["cost_and_cap_eligible"]
                else None,
            }
            for row in combinations
        ],
        "observed_quality_leader": leader,
        "observed_viable_systems": observed_viable,
        "any_observed_composite_passed": bool(observed_viable),
        "optimistic_remaining_bound": _record(bound_path),
        "more_development_cases_can_change_viability": bound[
            "more_development_alignment_can_change_selection_viability"
        ],
        "next_case": next_case,
        "composite_semantics": "deterministic_union_by_exact_canonical_event_identity_only",
        "semantic_routing_or_pruning_used": False,
        "production_mutated": False,
    }
    report_path = root / "composite-selection-score.json"
    _write_stable_time(report_path, report, "created_at")
    spec = {
        "schema_version": V188_SPEC_VERSION,
        "state": "zero_token_postprocess_completed",
        "created_at": now_iso(),
        "semantic_model_calls_declared": 0,
        "semantic_model_calls_started": 0,
        "extraction_model_calls_started": 0,
        "new_judge_prompt_or_rubric_created": False,
        "deterministic_operations": [
            "verify_immutable_v187_turns_and_usage",
            "union_candidate_events_by_exact_canonical_identity",
            "enforce_global_32_event_cap",
            "apply_frozen_production_amortized_cost_formula",
            "apply_frozen_shared_reference_semantic_gates",
            "compute_candidate_favoring_remaining_case_bound",
        ],
        "semantic_regex_or_keyword_rules_used": False,
        "semantic_similarity_or_embeddings_used": False,
        "semantic_routing_or_pruning_used": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v187.__file__)),
            _record(Path(v186.__file__)),
        ],
        "frozen_inputs": {
            "v187_terminal": v187_success["terminal_record"],
            "v187_spec": v187_success["spec_record"],
            "v187_alignment": v187_success["normalized_record"],
            "v187_turns": v187_success["turn_records"],
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
    spec_path = root / "composite-postprocess-spec.json"
    _write_stable_time(spec_path, spec, "created_at")

    if observed_viable:
        terminal_reason = "composite_core_candidate_passed_end_to_end_enrichment_accounting_required"
        classification = "inactive_incomplete_recovery_required"
        additional_alignment_authorized = False
        required_next = (
            root.parent
            / "development-selection-v5_4-v189-composite-end-to-end-accounting"
            / "terminal.json"
        )
        shortest = (
            "audit enrichment and complete end-to-end candidate accounting before freezing a winner"
        )
    elif bound["any_system_can_become_semantically_viable"]:
        terminal_reason = "composite_viability_unresolved_minimal_additional_alignment_required"
        classification = "active_development_recovery_required"
        additional_alignment_authorized = True
        required_next = (
            root.parent
            / "development-selection-v5_4-v189-composite-minimal-alignment"
            / "terminal.json"
        )
        shortest = "judge only the one preselected source-critical case, then rerun this stop rule"
    else:
        terminal_reason = "composite_noninferiority_not_recoverable_under_cost_and_cap_gates"
        classification = "inactive_incomplete_recovery_required"
        additional_alignment_authorized = False
        required_next = (
            root.parent
            / "development-selection-v5_4-v189-extraction-quality-recovery-nonacceptance"
            / "terminal.json"
        )
        shortest = (
            "record nonacceptance because neither individual nor cap-safe composite extraction can pass"
        )
    terminal = {
        "schema_version": V188_TERMINAL_VERSION,
        "state": "inactive" if not additional_alignment_authorized else "completed",
        "terminal_at": now_iso(),
        "terminal_reason": terminal_reason,
        "terminal_classification": classification,
        "overall_evaluation_complete": False,
        "judge_protocol_passed": True,
        "development_winner_frozen": False,
        "observed_viable_systems": observed_viable,
        "observed_quality_leader": leader,
        "more_development_cases_can_change_viability": bound[
            "more_development_alignment_can_change_selection_viability"
        ],
        "additional_alignment_authorized": additional_alignment_authorized,
        "authorized_next_case": next_case if additional_alignment_authorized else None,
        "holdout_authorized": False,
        "prospective_shadow_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "attempt_usage_status": "complete",
        "attempt_usage": {field: 0 for field in USAGE_FIELDS},
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": v187_success["terminal"][
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
    parser = argparse.ArgumentParser(description="Freeze v188 composite postprocess")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v188(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "observed_viable_systems": terminal["observed_viable_systems"],
                "more_development_cases_can_change_viability": terminal[
                    "more_development_cases_can_change_viability"
                ],
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
