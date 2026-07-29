from __future__ import annotations

"""Score the v200 conservative residual-repair candidate and its recovery ceiling."""

import argparse
import copy
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from . import app_server_judge_v5_selection_v188_composite_postprocess as v188
from . import app_server_judge_v5_selection_v191_extraction_quality_nonacceptance as v191
from . import app_server_judge_v5_selection_v192_residual_repair_design as v192
from . import app_server_judge_v5_selection_v200_reference_conflict_abstention as v200
from .app_server_dev_selection import _event_hash, evaluate_semantic_gates
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS
from .util import now_iso


V201_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v201_spec_v1"
V201_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v201_terminal_v1"
V201_REPORT_VERSION = "pif_app_server_residual_repair_score_report_v1"
V201_AUDIT_VERSION = "pif_app_server_residual_repair_score_audit_v1"
RESIDUAL_SYSTEM_ID = "residual_repair_batch8_pair_v200_abstention"
DEFAULT_OUTPUT_ROOT = (
    v200.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v201-residual-repair-score"
).resolve()


class JudgeV5SelectionV201Error(RuntimeError):
    """The v200 residual candidate cannot be scored against frozen evidence."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v200_success() -> dict[str, Any]:
    root = v200.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "spec": root / "reference-conflict-abstention-spec.json",
        "reconciliation": root / "residual-repair-alignment-reconciled.private.json",
        "audit": root / "reference-conflict-abstention-audit.json",
    }
    values = {name: _load_json(path, f"v200 {name}") for name, path in paths.items()}
    terminal, spec, reconciliation, audit = (
        values["terminal"],
        values["spec"],
        values["reconciliation"],
        values["audit"],
    )
    expected_cumulative = {
        "input_tokens": 7355828,
        "cached_input_tokens": 876032,
        "output_tokens": 1352413,
        "reasoning_output_tokens": 439201,
        "total_tokens": 8708241,
    }
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v200_predeclared_abstention_fallback_scoring_authorized"
        or terminal.get("terminal_classification")
        != "active_development_recovery_required"
        or terminal.get("frozen_reference_partition_preserved") is not True
        or terminal.get("unresolved_repair_placement_count") != 3
        or terminal.get("abstained_original_case_count") != 2
        or terminal.get("scoring_authorized") is not True
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_attempt_started") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != zero_usage
        or terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative
        or terminal.get("cumulative_unknown_usage_turn_count") != 2
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 220000
        or terminal.get("reconciliation") != _record(paths["reconciliation"])
        or terminal.get("reconciliation_audit") != _record(paths["audit"])
        or terminal.get("spec") != _record(paths["spec"])
        or spec.get("semantic_model_calls_started") != 0
        or spec.get("v199_semantic_attempt_replayed") is not False
        or spec.get("scoring_authorized") is not True
        or spec.get("holdout_authorized") is not False
        or audit.get("unresolved_repair_placement_count") != 3
        or audit.get("abstained_original_case_count") != 2
        or audit.get("post_reconciliation_reference_conflict_count") != 0
        or audit.get("scoring_authorized") is not True
        or len(reconciliation.get("abstained_original_case_ids") or []) != 2
        or reconciliation.get("frozen_reference_partition_preserved") is not True
    ):
        raise JudgeV5SelectionV201Error("v200 scoring authorization drifted")
    for path in paths.values():
        if not path.is_file():
            raise JudgeV5SelectionV201Error("v200 artifact disappeared")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV201Error("v200 runtime binding drifted")
    for record in spec["predecessor"].values():
        if not _verify_record(record):
            raise JudgeV5SelectionV201Error("v200 predecessor binding drifted")
    frozen_attempt = spec["frozen_inputs"].get("v199_attempt")
    if (
        not isinstance(frozen_attempt, Mapping)
        or frozen_attempt.get("state") != "interrupted"
        or frozen_attempt.get("status") != "timeout"
        or frozen_attempt.get("usage_status") != "unknown"
        or frozen_attempt.get("error_class") != "turn_timeout"
        or frozen_attempt.get("output") is not None
        or not isinstance(frozen_attempt.get("capacity"), Mapping)
        or not isinstance(frozen_attempt.get("sidecar"), Mapping)
        or not _verify_record(frozen_attempt["capacity"])
        or not _verify_record(frozen_attempt["sidecar"])
    ):
        raise JudgeV5SelectionV201Error("v200 frozen attempt binding drifted")
    predecessor = v200._validate_v199_timeout()
    recomputed_reconciliation, recomputed_audit = v200.build_abstention_reconciliation(
        predecessor
    )
    if recomputed_reconciliation != reconciliation or recomputed_audit != audit:
        raise JudgeV5SelectionV201Error("v200 reconciliation drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "spec": spec,
        "reconciliation": reconciliation,
        "audit": audit,
        "predecessor": predecessor,
    }


def _base_consensus() -> tuple[dict[str, Any], dict[str, Any]]:
    v190 = v191._validate_v190_stop()
    record = v190["spec"]["frozen_inputs"]["consensus"]
    if not _verify_record(record):
        raise JudgeV5SelectionV201Error("frozen v190 consensus drifted")
    return _load_json(Path(record["path"]), "v190 consensus"), record


def build_residual_score_inputs(
    predecessor: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    v198_source = predecessor["predecessor"]["source"]
    v197_source = v198_source["predecessor"]
    v196_source = v197_source["predecessor"]
    base_sources, _base_score, _combinations = v192._all_composite_score()
    consensus, consensus_record = _base_consensus()
    mapping = copy.deepcopy(base_sources["mapping"])
    membership = copy.deepcopy(base_sources["membership"])
    original_cases = {str(row["case_id"]): row for row in mapping["cases"]}
    base_system_id = v192.BASE_SYSTEM_ID

    for case in mapping["cases"]:
        for witness in case["witnesses"]:
            memberships = witness["provenance"]["memberships"]
            base = [
                row for row in memberships if row.get("system_id") == base_system_id
            ]
            if base:
                memberships.append(
                    {
                        "system_id": RESIDUAL_SYSTEM_ID,
                        "submitted_evidence_exact": any(
                            row.get("submitted_evidence_exact") is True for row in base
                        ),
                        "deterministic_base_system_reuse": True,
                    }
                )

    repair_mapping = v196_source["values"]["mapping"]
    repair_case_by_original = {
        str(row["case_provenance"]["original_case_id"]): row
        for row in repair_mapping["cases"]
    }
    if len(repair_case_by_original) != 5:
        raise JudgeV5SelectionV201Error("repair case coverage drifted")
    repair_hashes: dict[str, set[str]] = defaultdict(set)
    exact_identity_dedup_count = 0
    for original_case_id, repair_case in repair_case_by_original.items():
        original = original_cases[original_case_id]
        by_hash: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for witness in original["witnesses"]:
            by_hash[str(witness["provenance"]["canonical_event_sha256"])].append(
                witness
            )
        next_index = max(
            [int(row.get("canonical_index", -1)) for row in original["witnesses"]]
            + [-1]
        ) + 1
        for repair_witness in repair_case["witnesses"]:
            event = repair_witness["provenance"]["original_event"]
            event_hash = _event_hash(event)
            repair_hashes[str(original["case_key"])].add(event_hash)
            repair_memberships = []
            if by_hash.get(event_hash):
                target = sorted(
                    by_hash[event_hash], key=lambda row: str(row["witness_id"])
                )[0]
                memberships = target["provenance"]["memberships"]
                if not any(
                    row.get("system_id") == RESIDUAL_SYSTEM_ID
                    for row in memberships
                ):
                    memberships.append(
                        {
                            "system_id": RESIDUAL_SYSTEM_ID,
                            "submitted_evidence_exact": True,
                            "deterministic_exact_identity_repair_dedup": True,
                        }
                    )
                exact_identity_dedup_count += 1
            else:
                repair_memberships = [
                    {
                        "system_id": RESIDUAL_SYSTEM_ID,
                        "submitted_evidence_exact": True,
                        "repair_system_id": "residual_repair_batch8_pair_v195",
                    }
                ]
            new_witness = {
                "witness_id": str(repair_witness["witness_id"]),
                "canonical_side": "b",
                "canonical_index": next_index,
                "provenance": {
                    "canonical_event_sha256": event_hash,
                    "memberships": repair_memberships,
                    "original_event": event,
                    "structural_sentinel": False,
                },
            }
            next_index += 1
            original["witnesses"].append(new_witness)
            by_hash[event_hash].append(new_witness)

    membership["systems"][RESIDUAL_SYSTEM_ID] = {
        "kind": "base_exact_union_plus_source_grounded_residual_repair",
        "base_system_id": base_system_id,
        "repair_system_id": "residual_repair_batch8_pair_v195",
    }
    membership["system_cases"][RESIDUAL_SYSTEM_ID] = {}
    case_by_segment = {str(row["case_key"]): row for row in mapping["cases"]}
    for segment_id in membership["segment_order"]:
        base_case = membership["system_cases"][base_system_id][segment_id]
        event_hashes = set(base_case["event_hashes"]) | repair_hashes.get(
            str(segment_id), set()
        )
        exact_hashes = set()
        for witness in case_by_segment[str(segment_id)]["witnesses"]:
            provenance = witness["provenance"]
            event_hash = str(provenance["canonical_event_sha256"])
            if event_hash in event_hashes and any(
                row.get("system_id") == RESIDUAL_SYSTEM_ID
                and row.get("submitted_evidence_exact") is True
                for row in provenance["memberships"]
            ):
                exact_hashes.add(event_hash)
        membership["system_cases"][RESIDUAL_SYSTEM_ID][segment_id] = {
            "event_hashes": sorted(event_hashes),
            "exact_evidence_event_count": len(exact_hashes),
            "status": "coded" if event_hashes else "no_submitted_events",
            "submitted_event_count": len(event_hashes),
        }

    augmented_consensus = copy.deepcopy(consensus)
    consensus_by_case = {
        str(row["case_id"]): row for row in augmented_consensus["cases"]
    }
    reconciled_by_case = {
        str(row["case_id"]): row
        for row in predecessor["reconciliation"]["cases"]
    }
    representative_mapping = {
        str(row["case_id"]): row for row in v198_source["mapping"]["cases"]
    }
    receipts = {
        str(row["witness_id"]): row for row in v197_source["receipts"]["units"]
    }
    repair_witness_count = 0
    for original_case_id, repair_case in repair_case_by_original.items():
        repair_ids = [str(row["witness_id"]) for row in repair_case["witnesses"]]
        repair_witness_count += len(repair_ids)
        if not repair_ids:
            continue
        consensus_case = consensus_by_case[original_case_id]
        reconciled_case = reconciled_by_case[original_case_id]
        reference_mapping = representative_mapping[original_case_id]
        representative_to_full_group = {
            str(row["representative_witness_id"]): set(
                map(str, row["full_group_witness_ids"])
            )
            for row in reference_mapping["reference_groups"]
        }
        groups = [list(map(str, group)) for group in consensus_case["equivalence_groups"]]
        group_index = {
            witness_id: index for index, group in enumerate(groups) for witness_id in group
        }
        for repair_id in repair_ids:
            reconciled_group = next(
                set(map(str, group))
                for group in reconciled_case["equivalence_groups"]
                if repair_id in group
            )
            representatives = sorted(
                (reconciled_group - {repair_id}) & set(representative_to_full_group)
            )
            if len(representatives) == 1:
                groups[group_index[representatives[0]]].append(repair_id)
            elif not representatives:
                groups.append([repair_id])
            else:
                raise JudgeV5SelectionV201Error("reconciled repair crosses reference groups")
            consensus_case["support_results"].append(
                {
                    "witness_id": repair_id,
                    "verdict": receipts[repair_id]["proposition_verdict"],
                }
            )
        existing_pairs = {
            tuple(sorted((row["left_witness_id"], row["right_witness_id"])))
            for row in consensus_case["alignment_results"]
        }
        for pair in reconciled_case["alignment_pairs"]:
            witness_ids = sorted(map(str, pair["witness_ids"]))
            identity = tuple(witness_ids)
            if not (set(witness_ids) & set(repair_ids)) or identity in existing_pairs:
                continue
            consensus_case["alignment_results"].append(
                {
                    "left_witness_id": witness_ids[0],
                    "right_witness_id": witness_ids[1],
                    "relation": str(pair["relation"]),
                }
            )
            existing_pairs.add(identity)
        consensus_case["equivalence_groups"] = [
            sorted(set(group)) for group in groups
        ]
        if original_case_id in predecessor["reconciliation"][
            "abstained_original_case_ids"
        ]:
            consensus_case["status"] = "partial_abstain"
            consensus_case["alignment_abstained_witness_ids"] = sorted(
                set(consensus_case.get("alignment_abstained_witness_ids") or [])
                | set(repair_ids)
            )
    if repair_witness_count != 30:
        raise JudgeV5SelectionV201Error("repair witness count drifted")
    return (
        {**base_sources, "mapping": mapping, "membership": membership},
        augmented_consensus,
        {
            "base_consensus": consensus_record,
            "base_system_id": base_system_id,
            "residual_system_id": RESIDUAL_SYSTEM_ID,
            "repair_case_count": len(repair_case_by_original),
            "repair_witness_count": repair_witness_count,
            "exact_identity_dedup_count": exact_identity_dedup_count,
        },
    )


def score_v201(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    sources, consensus, build_audit = build_residual_score_inputs(predecessor)
    selected_case_ids = {str(row["case_id"]) for row in consensus["cases"]}
    score = v186._score_subset(
        consensus=consensus,
        selected_case_ids=selected_case_ids,
        sources=sources,
    )
    gate = evaluate_semantic_gates(score, candidate_system_id=RESIDUAL_SYSTEM_ID)
    base_rows = {
        str(row["case_id"]): row
        for row in score["systems"][v192.BASE_SYSTEM_ID]["cases"]
    }
    candidate_rows = {
        str(row["case_id"]): row
        for row in score["systems"][RESIDUAL_SYSTEM_ID]["cases"]
    }
    baseline_rows = {
        str(row["case_id"]): row
        for row in score["systems"][score["baseline_system_id"]]["cases"]
    }
    v198_source = predecessor["predecessor"]["source"]
    repair_mapping = v198_source["predecessor"]["predecessor"]["values"]["mapping"]
    selected_ids = {
        str(row["case_provenance"]["original_case_id"])
        for row in repair_mapping["cases"]
    }
    mapped_cases = {
        str(row["case_id"]): row for row in sources["mapping"]["cases"]
    }
    selected_rows = []
    for case_id in sorted(selected_ids):
        candidate = candidate_rows[case_id]
        base = base_rows[case_id]
        baseline = baseline_rows[case_id]
        segment_id = str(mapped_cases[case_id]["case_provenance"]["segment_id"])
        selected_rows.append(
            {
                "case_id": case_id,
                "segment_id": segment_id,
                "source_id": candidate["source_id"],
                "density_stratum": candidate["density_stratum"],
                "base_candidate_f1": base["f1"],
                "residual_candidate_f1": candidate["f1"],
                "baseline_f1": baseline["f1"],
                "delta_vs_base": round(candidate["f1"] - base["f1"], 6),
                "delta_vs_baseline": round(candidate["f1"] - baseline["f1"], 6),
                "case_abstained_worst_case": candidate["case_abstained_worst_case"],
            }
        )
    system_cases = sources["membership"]["system_cases"][RESIDUAL_SYSTEM_ID]
    no_signal_ids = {
        row["segment_id"]
        for row in selected_rows
        if row["density_stratum"] == "no_signal"
    }
    dense_rows = [row for row in selected_rows if row["density_stratum"] != "no_signal"]
    v195_gate = v198_source["predecessor"]["predecessor"]["predecessor"]["gate"]
    promotion_checks = {
        "all_four_dense_cases_improve_over_base_pair": len(dense_rows) == 4
        and all(row["delta_vs_base"] > 0 for row in dense_rows),
        "no_selected_case_worse_than_baseline_by_more_than_0_05": all(
            row["delta_vs_baseline"] >= -0.05 for row in selected_rows
        ),
        "selected_no_signal_false_positive_events_zero": all(
            int(system_cases[segment_id]["submitted_event_count"]) == 0
            for segment_id in no_signal_ids
        ),
        "all_residual_evidence_exact": all(
            int(row["submitted_event_count"]) == int(row["exact_evidence_event_count"])
            for row in system_cases.values()
        ),
        "event_cap_lte_32": max(
            int(row["submitted_event_count"]) for row in system_cases.values()
        )
        <= v192.MAX_EVENTS_PER_CASE,
        "production_amortized_total_token_ratio_lte_0_28": float(
            v195_gate["production_amortized_total_token_ratio"]
        )
        <= 0.28,
        "full_frozen_semantic_gates": bool(gate["passed"]),
    }

    oracle_score = copy.deepcopy(score)
    oracle_score["abstained_cases"] = []
    oracle_score["abstained_case_rate"] = 0.0
    oracle_rows = oracle_score["systems"][RESIDUAL_SYSTEM_ID]["cases"]
    abstained_ids = set(predecessor["reconciliation"]["abstained_original_case_ids"])
    for row in oracle_rows:
        if str(row["case_id"]) in abstained_ids:
            row["precision"] = row["recall"] = row["f1"] = 1.0
            row["case_abstained_worst_case"] = False
    oracle_score["systems"][RESIDUAL_SYSTEM_ID]["macro_f1"] = round(
        sum(float(row["f1"]) for row in oracle_rows) / len(oracle_rows), 6
    )
    oracle_gate = evaluate_semantic_gates(
        oracle_score, candidate_system_id=RESIDUAL_SYSTEM_ID
    )
    conflict_recovery_could_change_verdict = bool(oracle_gate["passed"])
    audit = {
        "schema_version": V201_AUDIT_VERSION,
        "build": build_audit,
        "selected_case_count": len(selected_rows),
        "dense_selected_case_count": len(dense_rows),
        "no_signal_selected_case_count": len(no_signal_ids),
        "maximum_candidate_event_count": max(
            int(row["submitted_event_count"]) for row in system_cases.values()
        ),
        "normalized_nonexact_evidence_events": gate[
            "normalized_nonexact_evidence_events"
        ],
        "production_amortized_total_token_ratio": v195_gate[
            "production_amortized_total_token_ratio"
        ],
        "promotion_checks": promotion_checks,
        "promotion_passed": all(promotion_checks.values()),
        "conflict_perfect_oracle_gate": v188._sanitize_gate(oracle_gate),
        "conflict_recovery_could_change_verdict": conflict_recovery_could_change_verdict,
        "more_development_alignment_cases_can_change_selection": False,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "production_mutated": False,
    }
    return {
        "sources": sources,
        "consensus": consensus,
        "score": score,
        "gate": gate,
        "selected_rows": selected_rows,
        "audit": audit,
    }


def freeze_v201(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v201 terminal")}
    predecessor = _validate_v200_success()
    result = score_v201(predecessor)
    private_paths = {
        "mapping": root / "residual-repair-mapping.private.json",
        "membership": root / "residual-repair-membership.private.json",
        "consensus": root / "residual-repair-consensus.private.json",
        "score": root / "residual-repair-score.private.json",
    }
    _write_immutable(private_paths["mapping"], result["sources"]["mapping"])
    _write_immutable(private_paths["membership"], result["sources"]["membership"])
    _write_immutable(private_paths["consensus"], result["consensus"])
    _write_immutable(private_paths["score"], result["score"])
    audit_path = root / "residual-repair-score-audit.json"
    _write_immutable(audit_path, result["audit"])
    gate = result["gate"]
    report = {
        "schema_version": V201_REPORT_VERSION,
        "created_at": now_iso(),
        "development_case_count": result["score"]["systems"][RESIDUAL_SYSTEM_ID][
            "case_count"
        ],
        "viable_systems": [RESIDUAL_SYSTEM_ID] if result["audit"]["promotion_passed"] else [],
        "observed_quality_leader": RESIDUAL_SYSTEM_ID,
        "unresolved_selection_decision": (
            "freeze residual repair winner"
            if result["audit"]["promotion_passed"]
            else "none; residual repair is rejected under the unchanged gates"
        ),
        "more_development_cases_can_change_selection": False,
        "conflict_recovery_could_change_verdict": result["audit"][
            "conflict_recovery_could_change_verdict"
        ],
        "semantic_gate": v188._sanitize_gate(gate),
        "selected_case_results": result["selected_rows"],
        "promotion_checks": result["audit"]["promotion_checks"],
        "promotion_passed": result["audit"]["promotion_passed"],
        "production_amortized_total_token_ratio": result["audit"][
            "production_amortized_total_token_ratio"
        ],
        "production_amortized_token_target_passed": result["audit"][
            "promotion_checks"
        ]["production_amortized_total_token_ratio_lte_0_28"],
        "cumulative_measured_usage_lower_bound": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": 2,
        "cumulative_conservative_unknown_usage_upper_bound": 220000,
        "shortest_path_to_holdout_verdict": (
            "freeze the winner and run the smallest balanced canary"
            if result["audit"]["promotion_passed"]
            else "no judge-only path remains; extraction-quality strategy authorization is required"
        ),
        "production_mutated": False,
        "privacy": "sanitized_metrics_counts_hashes_and_opaque_case_ids_no_source_or_event_text",
    }
    report_path = root / "residual-repair-score-report.json"
    _write_stable_time(report_path, report, "created_at")
    spec = {
        "schema_version": V201_SPEC_VERSION,
        "state": "zero_token_development_score_completed",
        "created_at": now_iso(),
        "semantic_model_calls_declared": 0,
        "semantic_model_calls_started": 0,
        "quality_gates_unchanged": True,
        "production_amortized_token_target_lte": 0.28,
        "conflict_perfect_upper_bound_computed": True,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v200.__file__)),
            _record(Path(v192.__file__)),
            _record(Path(v186.__file__)),
            _record(Path(v188.__file__)),
        ],
        "predecessor": predecessor["records"],
        "frozen_inputs": {
            "base_consensus": result["audit"]["build"]["base_consensus"],
            "reconciliation": predecessor["records"]["reconciliation"],
        },
        "private_artifacts": {
            name: _record(path) for name, path in private_paths.items()
        },
        "audit": _record(audit_path),
        "report": _record(report_path),
        "privacy": "private_witness_data_separate_from_sanitized_report",
    }
    spec_path = root / "residual-repair-score-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    passed = bool(result["audit"]["promotion_passed"])
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    terminal = {
        "schema_version": V201_TERMINAL_VERSION,
        "state": "completed" if passed else "inactive",
        "terminal_at": now_iso(),
        "terminal_reason": (
            "v201_residual_repair_development_winner_freeze_authorized"
            if passed
            else "development_residual_repair_quality_gate_not_passed_conflict_recovery_cannot_change_verdict"
        ),
        "terminal_classification": (
            "active_development_recovery_required"
            if passed
            else "inactive_incomplete_recovery_required"
        ),
        "overall_evaluation_complete": False,
        "evaluation_accepted": False,
        "development_quality_passed": passed,
        "development_winner_frozen": False,
        "production_amortized_token_target_passed": report[
            "production_amortized_token_target_passed"
        ],
        "production_amortized_total_token_ratio": report[
            "production_amortized_total_token_ratio"
        ],
        "conflict_recovery_could_change_verdict": report[
            "conflict_recovery_could_change_verdict"
        ],
        "more_development_cases_can_change_selection": False,
        "safe_local_judge_only_experiment_remaining": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "prospective_shadow_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": zero_usage,
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": 2,
        "cumulative_conservative_unknown_usage_upper_bound": 220000,
        "score_report": _record(report_path),
        "score_audit": _record(audit_path),
        "spec": _record(spec_path),
        "evaluation_acceptance_receipt_emitted": False,
        "required_next_artifact_path": str(
            root.parent
            / "development-selection-v5_4-v202-extraction-quality-nonacceptance"
            / "terminal.json"
        ),
    }
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return {
        "root": root,
        "predecessor": predecessor,
        "result": result,
        "report": report,
        "spec": spec,
        "terminal": terminal,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Score v201 residual repair")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v201(output_dir=Path(args.output_dir))["terminal"]
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "development_quality_passed": terminal["development_quality_passed"],
                "production_amortized_token_target_passed": terminal[
                    "production_amortized_token_target_passed"
                ],
                "conflict_recovery_could_change_verdict": terminal[
                    "conflict_recovery_could_change_verdict"
                ],
                "holdout_authorized": terminal["holdout_authorized"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
