from __future__ import annotations

"""Freeze the operator-stopped v185 evidence and close futile alignment work."""

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v175_support as v175
from . import app_server_judge_v5_selection_v179_primary_alignment_phase1 as v179
from . import app_server_judge_v5_selection_v181_structural_partition_recovery as v181
from . import app_server_judge_v5_selection_v184_schema_subset_recovery as v184
from . import app_server_judge_v5_selection_v185_primary_phase2 as v185
from .app_server_dev_selection import (
    BASELINE_REPAIRED_SYSTEM,
    QUALITY_GATES,
    evaluate_semantic_gates,
    load_exact_cost_contract,
    production_amortized_cost,
    score_dev_shared_reference,
    source_cluster_paired_bootstrap,
)
from .app_server_judge_v5 import normalize_neutral_alignment_output
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _aggregate_usage,
    _load_json,
    _validate_completed_turn,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_llm_judge import JUDGE_CONSENSUS_VERSION
from .util import now_iso


V186_AUDIT_VERSION = "pif_app_server_judge_v5_4_selection_v186_operator_stop_audit_v1"
V186_SCORE_VERSION = "pif_app_server_judge_v5_4_selection_v186_checkpoint_score_v1"
V186_BOUND_VERSION = "pif_app_server_judge_v5_4_selection_v186_remaining_bound_v1"
V186_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v186_spec_v1"
V186_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v186_terminal_v1"

COMPLETED_V185_TURN_COUNT = 5
NEVER_STARTED_V185_TURN_COUNT = 4
DEFAULT_OUTPUT_ROOT = (
    v185.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v186-convergence-checkpoint"
).resolve()


class JudgeV5SelectionV186Error(RuntimeError):
    """The deterministic convergence checkpoint cannot be frozen."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v185_operator_stop() -> dict[str, Any]:
    root = v185.DEFAULT_OUTPUT_ROOT
    spec_path = root / "primary-alignment-phase2-spec.json"
    policy_path = root / "capacity-policy.json"
    terminal_path = root / "terminal.json"
    failure_path = root / "failure.json"
    if terminal_path.exists() or failure_path.exists():
        raise JudgeV5SelectionV186Error("v185 operator stop was mutated into a terminal attempt")
    spec = _load_json(spec_path, "v185 spec")
    if (
        spec.get("state") != "frozen_before_model_calls"
        or spec.get("turn_plan") != list(v185.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("v184_turns_replayed") is not False
        or spec.get("predecessor_cumulative_usage_status") != "unknown"
        or spec.get("predecessor_unknown_usage_turn_count") != 1
        or spec.get("predecessor_unknown_usage_upper_bound") != 120000
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5SelectionV186Error("v185 frozen contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV186Error("v185 runtime record drifted")

    predecessor = v185._validate_v184_success()
    rows = predecessor["partition"]["phases"][v185.PHASE_INDEX]
    frozen_turns = spec.get("frozen_inputs", {}).get("turns") or []
    if len(rows) != 9 or len(frozen_turns) != 9:
        raise JudgeV5SelectionV186Error("v185 phase-two coverage drifted")

    completed, sidecars, normalized_cases = [], [], []
    never_started = []
    for index, (turn_name, row) in enumerate(zip(v185.TURN_NAMES, rows, strict=True)):
        turn_root = root / "turns" / turn_name.replace("_", "-")
        paths = {
            "input": turn_root / "input.private.json",
            "prompt": turn_root / "prompt.private.md",
            "schema": turn_root / "schema.json",
            "capacity": turn_root / "capacity.json",
            "sidecar": turn_root / "sidecar.json",
            "output": turn_root / "output.private.json",
            "projected": turn_root / "alignment-projected.private.json",
            "projection_audit": turn_root / "structural-projection-audit.json",
        }
        frozen = frozen_turns[index]
        if (
            frozen.get("turn_name") != turn_name
            or frozen.get("case_id") != row["case_id"]
            or frozen.get("input") != _record(paths["input"])
            or frozen.get("prompt") != _record(paths["prompt"])
            or frozen.get("schema") != _record(paths["schema"])
            or _load_json(paths["input"], "v185 input") != row["value"]
            or paths["prompt"].read_text() != row["prompt"]
            or _load_json(paths["schema"], "v185 schema") != row["schema"]
        ):
            raise JudgeV5SelectionV186Error("v185 frozen request drifted")
        if index >= COMPLETED_V185_TURN_COUNT:
            if any(paths[name].exists() for name in ("capacity", "sidecar", "output", "projected", "projection_audit")):
                raise JudgeV5SelectionV186Error("v185 later turn unexpectedly started")
            never_started.append({"turn_name": turn_name, "case_id": row["case_id"]})
            continue
        output, sidecar = _validate_completed_turn(
            paths={name: paths[name] for name in ("input", "prompt", "schema", "capacity", "sidecar", "output")},
            prompt=row["prompt"],
            schema=row["schema"],
            base_instructions=v185.v130.alignment_instructions_v130(),
            model=v185.MODEL,
            effort=v185.EFFORT,
            policy_path=policy_path,
            output_validator=lambda candidate, item=row["value"]: (
                v181.validate_structurally_completable_output(candidate, item)
            ),
        )
        projected, audit = v181.project_structural_unpaired_and_exact_spans(output, row["value"])
        if (
            projected != _load_json(paths["projected"], "v185 projected")
            or audit != _load_json(paths["projection_audit"], "v185 projection audit")
            or audit.get("dropped_nonexact_span_count") != 0
            or audit.get("semantic_alignment_pairs_changed") is not False
            or audit.get("semantic_checklist_decisions_changed") is not False
            or audit.get("semantic_equivalence_groups_changed") is not False
        ):
            raise JudgeV5SelectionV186Error("v185 structural projection drifted")
        normalized = normalize_neutral_alignment_output(projected, row["value"])["cases"]
        normalized_cases.extend(normalized)
        completed.append(
            {
                "turn_name": turn_name,
                "case_id": row["case_id"],
                "capacity": _record(paths["capacity"]),
                "sidecar": _record(paths["sidecar"]),
                "output": _record(paths["output"]),
                "projected": _record(paths["projected"]),
                "projection_audit": _record(paths["projection_audit"]),
                "total_tokens": sidecar["usage"]["total_tokens"],
                "wall_elapsed_seconds": sidecar["wall_elapsed_seconds"],
            }
        )
        sidecars.append(sidecar)
    accounting = _aggregate_usage(sidecars)
    if (
        len(completed) != COMPLETED_V185_TURN_COUNT
        or len(never_started) != NEVER_STARTED_V185_TURN_COUNT
        or accounting.get("accounting_complete") is not True
        or accounting.get("usage_status") != "complete"
        or accounting.get("usage", {}).get("total_tokens") != 405354
    ):
        raise JudgeV5SelectionV186Error("v185 operator-stop accounting drifted")
    cumulative_lower = v185._sum_usage(
        predecessor["cumulative_known_lower_bound"], accounting["usage"]
    )
    if cumulative_lower["total_tokens"] != 8087855:
        raise JudgeV5SelectionV186Error("v185 cumulative lower bound drifted")
    return {
        "root": root,
        "spec": spec,
        "spec_record": _record(spec_path),
        "policy_record": _record(policy_path),
        "predecessor": predecessor,
        "completed": completed,
        "never_started": never_started,
        "normalized_cases": sorted(normalized_cases, key=lambda row: str(row["case_id"])),
        "usage": accounting["usage"],
        "cumulative_known_lower_bound": cumulative_lower,
        "cumulative_unknown_usage_turn_count": 1,
        "cumulative_unknown_usage_upper_bound": 120000,
    }


def _selection_sources() -> dict[str, Any]:
    sources = v175._validate_selection_sources(v175.DEFAULT_REUSE_CONTRACT)
    mapping_record = sources["contract"]["selection_inputs"]["preassembled_private_mapping"]
    membership_record = sources["contract"]["selection_inputs"]["preassembled_membership_index"]
    return {
        **sources,
        "mapping": _load_json(Path(mapping_record["path"]), "private mapping"),
        "mapping_record": mapping_record,
        "membership": _load_json(Path(membership_record["path"]), "membership index"),
        "membership_record": membership_record,
    }


def _completed_alignment_sets(v185_stop: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    adopted = v179._validate_v178_success()["v177"]["normalized_base"]["cases"]
    phase1 = v185_stop["predecessor"]["normalized"]["cases"]
    incremental = v185_stop["normalized_cases"]
    if len(adopted) != 1 or len(phase1) != 9 or len(incremental) != 5:
        raise JudgeV5SelectionV186Error("completed primary alignment coverage drifted")
    ids = [str(row["case_id"]) for row in [*adopted, *phase1, *incremental]]
    if len(ids) != len(set(ids)):
        raise JudgeV5SelectionV186Error("completed primary alignment cases overlap")
    return {
        "phase1_checkpoint": [*adopted, *phase1],
        "all_completed": [*adopted, *phase1, *incremental],
    }


def _build_consensus(
    *, aligned_cases: Sequence[Mapping[str, Any]], sources: Mapping[str, Any]
) -> tuple[dict[str, Any], set[str]]:
    receipts_by_case: dict[str, list[dict[str, Any]]] = defaultdict(list)
    receipts = v179._validate_v178_success()["receipts"]
    for row in receipts["units"]:
        receipts_by_case[str(row["case_id"])].append(row)
    all_case_ids = {str(row["case_id"]) for row in sources["mapping"]["cases"]}
    no_supported = {
        case_id
        for case_id in all_case_ids
        if not any(
            row.get("proposition_verdict") == "supported"
            for row in receipts_by_case.get(case_id, [])
        )
    }
    aligned = {str(row["case_id"]): row for row in aligned_cases}
    selected_case_ids = set(aligned) | no_supported
    consensus_cases = []
    for case_id in sorted(selected_case_ids):
        rows = receipts_by_case.get(case_id, [])
        supported = {
            str(row["witness_id"])
            for row in rows
            if row["proposition_verdict"] == "supported"
        }
        if case_id in aligned:
            groups = [list(group) for group in aligned[case_id]["equivalence_groups"]]
            if {str(item) for group in groups for item in group} != supported:
                raise JudgeV5SelectionV186Error("aligned supported-witness coverage drifted")
            pairs = aligned[case_id]["alignment_pairs"]
        else:
            if supported:
                raise JudgeV5SelectionV186Error("no-signal case unexpectedly needs alignment")
            groups, pairs = [], []
        groups.extend(
            [[str(row["witness_id"])] for row in rows if row["proposition_verdict"] == "unsupported"]
        )
        consensus_cases.append(
            {
                "case_id": case_id,
                "status": "completed",
                "support_results": [
                    {
                        "witness_id": str(row["witness_id"]),
                        "verdict": row["proposition_verdict"],
                    }
                    for row in rows
                ],
                "equivalence_groups": groups,
                "partition_abstained_witness_ids": [],
                "alignment_results": [
                    {
                        "left_witness_id": row["witness_ids"][0],
                        "right_witness_id": row["witness_ids"][1],
                        "relation": row["relation"],
                    }
                    for row in pairs
                ],
                "alignment_abstained_witness_ids": [],
            }
        )
    return {
        "schema_version": JUDGE_CONSENSUS_VERSION,
        "cases": consensus_cases,
        "abstentions": [],
    }, selected_case_ids


def _score_subset(
    *, consensus: Mapping[str, Any], selected_case_ids: set[str], sources: Mapping[str, Any]
) -> dict[str, Any]:
    mapping_cases = [
        row for row in sources["mapping"]["cases"] if str(row["case_id"]) in selected_case_ids
    ]
    segments = {str(row["case_key"]) for row in mapping_cases}
    mapping = {**sources["mapping"], "cases": mapping_cases}
    membership = {
        **sources["membership"],
        "segment_order": [
            value for value in sources["membership"]["segment_order"] if str(value) in segments
        ],
        "case_order": [
            value
            for value in sources["membership"]["case_order"]
            if str(value) in selected_case_ids
        ],
        "system_cases": {
            system_id: {
                segment_id: value
                for segment_id, value in rows.items()
                if str(segment_id) in segments
            }
            for system_id, rows in sources["membership"]["system_cases"].items()
        },
    }
    manifest_rows = [
        {
            "segment_id": row["case_key"],
            "episode_id": row["case_provenance"]["episode_id"],
            "source_id": row["case_provenance"]["source_id"],
            "density_stratum": row["case_provenance"]["density_stratum"],
        }
        for row in mapping_cases
    ]
    return score_dev_shared_reference(
        private_mapping=mapping,
        membership_index=membership,
        consensus=consensus,
        manifest_rows=manifest_rows,
    )


def _arm_reports(sources: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    reports = {}
    for item in sources["contract"]["clean_arms"]:
        report = _load_json(Path(item["report"]["path"]), "clean arm report")
        variant_id = f"batch_{report['batch_size_ceiling']}_{report['thread_mode']}"
        reports[variant_id] = report
    if set(reports) != {
        "batch_3_new_thread",
        "batch_3_same_thread",
        "batch_5_new_thread",
        "batch_8_new_thread",
        "batch_8_same_thread",
    }:
        raise JudgeV5SelectionV186Error("clean arm coverage drifted")
    return reports


def _sanitized_semantic_score(score: Mapping[str, Any]) -> dict[str, Any]:
    systems = {}
    for variant_id in (
        "batch_3_new_thread",
        "batch_3_same_thread",
        "batch_5_new_thread",
        "batch_8_new_thread",
        "batch_8_same_thread",
    ):
        gate = evaluate_semantic_gates(
            score, candidate_system_id=f"arm:{variant_id}:normalized"
        )
        systems[variant_id] = {
            "semantic_passed": gate["passed"],
            "checks": gate["checks"],
            "bootstrap": gate["bootstrap"],
            "macro_source_delta": gate["macro_source_delta"],
            "worst_source_delta": gate["worst_source_delta"],
            "no_signal": gate["no_signal"],
            "abstained_case_rate": gate["abstained_case_rate"],
        }
    return {
        "reference_unit_count": score["reference_unit_count"],
        "reference_policy": score["reference_policy"],
        "case_count": len(score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]),
        "systems": systems,
    }


def _optimistic_remaining_bound(
    *, completed_score: Mapping[str, Any], completed_case_ids: set[str], sources: Mapping[str, Any]
) -> dict[str, Any]:
    remaining = [
        row
        for row in sources["mapping"]["cases"]
        if str(row["case_id"]) not in completed_case_ids
    ]
    bounds = {}
    baseline_rows = {
        row["segment_id"]: row
        for row in completed_score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
    }
    for variant_id in (
        "batch_3_new_thread",
        "batch_3_same_thread",
        "batch_5_new_thread",
        "batch_8_new_thread",
        "batch_8_same_thread",
    ):
        candidate_rows = {
            row["segment_id"]: row
            for row in completed_score["systems"][f"arm:{variant_id}:normalized"]["cases"]
        }
        rows = [
            {
                "segment_id": segment_id,
                "source_id": baseline_rows[segment_id]["source_id"],
                "baseline_f1": baseline_rows[segment_id]["f1"],
                "candidate_f1": candidate_rows[segment_id]["f1"],
            }
            for segment_id in baseline_rows
        ]
        rows.extend(
            {
                "segment_id": row["case_key"],
                "source_id": row["case_provenance"]["source_id"],
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
        checks = {
            "paired_bootstrap_noninferiority": bootstrap["ci_lower"]
            >= -QUALITY_GATES["max_paired_f1_drop"],
            "macro_source_noninferiority": macro >= -QUALITY_GATES["max_macro_f1_drop"],
            "worst_source_noninferiority": worst >= -QUALITY_GATES["max_source_f1_drop"],
        }
        bounds[variant_id] = {
            "assumption": "every_remaining_case_candidate_f1_1_and_baseline_f1_0",
            "bootstrap": bootstrap,
            "macro_source_delta": round(macro, 6),
            "worst_source_delta": round(worst, 6),
            "checks": checks,
            "could_pass_all_frozen_semantic_noninferiority_checks": all(checks.values()),
        }
    if any(row["could_pass_all_frozen_semantic_noninferiority_checks"] for row in bounds.values()):
        raise JudgeV5SelectionV186Error("remaining cases can still change semantic viability")
    return {
        "schema_version": V186_BOUND_VERSION,
        "completed_case_count": len(completed_case_ids),
        "remaining_case_count": len(remaining),
        "bound_is_deliberately_candidate_favoring": True,
        "remaining_case_assumption": "candidate_f1_1_baseline_f1_0_for_every_case",
        "systems": bounds,
        "any_system_can_become_semantically_viable": False,
        "more_development_alignment_can_change_selection_viability": False,
    }


def freeze_v186(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v186 terminal")
    stop = _validate_v185_operator_stop()
    sources = _selection_sources()
    alignments = _completed_alignment_sets(stop)

    audit = {
        "schema_version": V186_AUDIT_VERSION,
        "created_at": now_iso(),
        "classification": "operator_convergence_stop_after_completed_turn_before_next_semantic_turn",
        "v185_terminal_exists": False,
        "v185_retry_allowed": False,
        "completed_turn_count": len(stop["completed"]),
        "never_started_turn_count": len(stop["never_started"]),
        "completed_turns": stop["completed"],
        "never_started_turns": stop["never_started"],
        "turn05_capacity_checkpoint_exists": False,
        "turn05_sidecar_exists": False,
        "turn05_output_exists": False,
        "usage_status": "complete",
        "usage": stop["usage"],
        "unknown_usage_turn_count": 0,
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": stop["cumulative_known_lower_bound"],
        "cumulative_unknown_usage_turn_count": 1,
        "cumulative_conservative_unknown_usage_upper_bound": 120000,
        "production_mutated": False,
    }
    audit_path = root / "v185-operator-stop-audit.json"
    _write_stable_time(audit_path, audit, "created_at")

    score_records = {}
    sanitized_scores = {}
    private_scores = {}
    selected_ids = {}
    for name, cases in alignments.items():
        consensus, case_ids = _build_consensus(aligned_cases=cases, sources=sources)
        consensus_path = root / f"{name.replace('_', '-')}-consensus.private.json"
        _write_immutable(consensus_path, consensus)
        score = _score_subset(
            consensus=consensus, selected_case_ids=case_ids, sources=sources
        )
        private_score_path = root / f"{name.replace('_', '-')}-score.private.json"
        _write_immutable(private_score_path, score)
        private_scores[name] = score
        sanitized_scores[name] = _sanitized_semantic_score(score)
        selected_ids[name] = case_ids
        score_records[name] = {
            "consensus": _record(consensus_path),
            "private_score": _record(private_score_path),
        }

    arm_reports = _arm_reports(sources)
    cost_contract = load_exact_cost_contract(
        Path(sources["contract"]["selection_inputs"]["context_usage_recovery"]["path"]),
        verify_sidecars=True,
    )
    costs = {
        variant_id: production_amortized_cost(
            arm_usage=report["usage"],
            arm_segments=int(report["validated_segments"]),
            contract=cost_contract,
        )
        for variant_id, report in arm_reports.items()
    }
    completed_systems = sanitized_scores["all_completed"]["systems"]
    phase1_systems = sanitized_scores["phase1_checkpoint"]["systems"]
    leader = max(
        completed_systems,
        key=lambda name: (
            completed_systems[name]["bootstrap"]["candidate_f1"],
            name,
        ),
    )
    phase1_leader = max(
        phase1_systems,
        key=lambda name: (phase1_systems[name]["bootstrap"]["candidate_f1"], name),
    )
    viable = [
        name
        for name in completed_systems
        if completed_systems[name]["semantic_passed"] and costs[name]["passed_lte_0_28"]
    ]
    bound = _optimistic_remaining_bound(
        completed_score=private_scores["all_completed"],
        completed_case_ids=selected_ids["all_completed"],
        sources=sources,
    )
    bound_path = root / "optimistic-remaining-case-bound.json"
    _write_immutable(bound_path, bound)

    v178 = v179._validate_v178_success()
    canary = v178["values"]["score"]
    score_report = {
        "schema_version": V186_SCORE_VERSION,
        "created_at": now_iso(),
        "quality_gates": QUALITY_GATES,
        "phase1_checkpoint": sanitized_scores["phase1_checkpoint"],
        "all_completed": sanitized_scores["all_completed"],
        "phase1_quality_leader": phase1_leader,
        "completed_quality_leader": leader,
        "leader_stable_after_five_additional_cases": phase1_leader == leader,
        "costs": costs,
        "all_arms_pass_production_amortized_token_gate": all(
            row["passed_lte_0_28"] for row in costs.values()
        ),
        "viable_systems": viable,
        "unresolved_selection_decision": (
            "no_current_arm_is_semantically_viable; batch choice is not the blocker"
        ),
        "balanced_canary": {
            "reused_without_replay": True,
            "passed": canary["passed"],
            "base_canary_projection_exact": canary["base_canary_projection_exact"],
            "score": v178["records"]["score"],
        },
        "optimistic_remaining_bound": _record(bound_path),
        "more_development_cases_can_change_viability": False,
        "production_mutated": False,
    }
    score_path = root / "selection-convergence-score.json"
    _write_stable_time(score_path, score_report, "created_at")

    spec = {
        "schema_version": V186_SPEC_VERSION,
        "state": "zero_token_postprocess_completed",
        "created_at": now_iso(),
        "semantic_model_calls_declared": 0,
        "semantic_model_calls_started": 0,
        "v185_turns_replayed": False,
        "new_judge_prompt_or_rubric_created": False,
        "support_receipts_reused": True,
        "balanced_canary_reused": True,
        "deterministic_operations": [
            "validate_immutable_artifact_hashes",
            "adapt_frozen_support_and_alignment_receipts_to_existing_shared_reference_scorer",
            "run_frozen_source_clustered_bootstrap",
            "run_frozen_production_amortized_cost_formula",
            "compute_candidate_favoring_remaining_case_upper_bound",
        ],
        "semantic_regex_or_keyword_rules_used": False,
        "semantic_similarity_or_embeddings_used": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v185.__file__)),
            _record(Path(v184.__file__)),
            _record(Path(v179.__file__)),
            _record(Path(v175.__file__)),
        ],
        "frozen_inputs": {
            "v185_spec": stop["spec_record"],
            "v185_policy": stop["policy_record"],
            "reuse_contract": sources["contract_record"],
            "private_mapping": sources["mapping_record"],
            "membership_index": sources["membership_record"],
            "support_receipts": v178["receipts_record"],
            "v178_score": v178["records"]["score"],
            "score_records": score_records,
        },
        "privacy": "private_witness_ids_and_scores_separate_sanitized_terminal_counts_hashes_metrics_only",
    }
    spec_path = root / "convergence-checkpoint-spec.json"
    _write_stable_time(spec_path, spec, "created_at")

    terminal = {
        "schema_version": V186_TERMINAL_VERSION,
        "state": "inactive",
        "terminal_at": now_iso(),
        "terminal_reason": "development_semantic_noninferiority_not_recoverable_by_remaining_alignment_cases",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "judge_protocol_passed": True,
        "development_winner_frozen": False,
        "viable_systems": viable,
        "quality_leader": leader,
        "quality_leader_semantic_passed": completed_systems[leader]["semantic_passed"],
        "all_arms_cost_passed": True,
        "production_amortized_token_ratio_range": {
            "minimum": min(row["production_amortized_total_token_ratio"] for row in costs.values()),
            "maximum": max(row["production_amortized_total_token_ratio"] for row in costs.values()),
        },
        "unresolved_selection_decision": score_report["unresolved_selection_decision"],
        "more_development_cases_can_change_viability": False,
        "remaining_alignment_calls_authorized": False,
        "balanced_canary_passed": True,
        "holdout_authorized": False,
        "prospective_shadow_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "attempt_usage_status": "complete",
        "attempt_usage": stop["usage"],
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": stop["cumulative_known_lower_bound"],
        "cumulative_unknown_usage_turn_count": 1,
        "cumulative_conservative_unknown_usage_upper_bound": 120000,
        "operator_stop_audit": _record(audit_path),
        "score": _record(score_path),
        "optimistic_remaining_bound": _record(bound_path),
        "spec": _record(spec_path),
        "shortest_path_to_holdout_verdict": (
            "a separately authorized immutable extraction-quality recovery must improve recall, "
            "produce a new development matrix, and pass the unchanged semantic gates; further "
            "judge-only alignment cannot authorize holdout"
        ),
        "required_next_artifact_path": str(
            root.parent
            / "development-selection-v5_4-v187-extraction-quality-recovery-authorization"
            / "terminal.json"
        ),
    }
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v186 convergence checkpoint")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v186(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "viable_systems": terminal["viable_systems"],
                "quality_leader": terminal["quality_leader"],
                "more_development_cases_can_change_viability": terminal[
                    "more_development_cases_can_change_viability"
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
