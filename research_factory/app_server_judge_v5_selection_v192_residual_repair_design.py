from __future__ import annotations

"""Freeze a bounded residual-repair diagnostic over existing batch-8 outputs."""

import argparse
import copy
import itertools
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from . import app_server_judge_v5_selection_v188_composite_postprocess as v188
from . import app_server_judge_v5_selection_v191_extraction_quality_nonacceptance as v191
from .app_server_dev_selection import (
    BASELINE_REPAIRED_SYSTEM,
    QUALITY_GATES,
    source_cluster_paired_bootstrap,
)
from .app_server_evaluation import episode_batch_core_schema
from .app_server_judge_v5 import compact_empty_event_fields
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .efficient_backtest import (
    DEFAULT_WINDOWED_GUIDELINES_PATH,
    _load_windowed_guideline_instructions,
    _windowed_event_core_instruction,
    build_windowed_segment_packet,
)
from .util import now_iso, sha256_text


V192_AUDIT_VERSION = "pif_app_server_judge_v5_4_selection_v192_oracle_audit_v1"
V192_DESIGN_VERSION = "pif_app_server_judge_v5_4_selection_v192_design_v1"
V192_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v192_terminal_v1"

BASE_ARMS = ("batch_8_new_thread", "batch_8_same_thread")
BASE_SYSTEM_ID = "composite:batch_8_new_thread+batch_8_same_thread:exact_union"
DIAGNOSTIC_EPISODE_ID = "v193_residual_repair_diagnostic"
DIAGNOSTIC_CASE_COUNT = 5
MAX_EVENTS_PER_CASE = 32
MAXIMUM_TOTAL_TOKENS_PER_TURN = 100_000
MODEL = "gpt-5.6-sol"
EFFORT = "low"
DEFAULT_OUTPUT_ROOT = (
    v191.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v192-residual-repair-design"
).resolve()


class JudgeV5SelectionV192Error(RuntimeError):
    """The residual-repair diagnostic cannot be frozen safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v191_nonacceptance() -> dict[str, Any]:
    root = v191.DEFAULT_OUTPUT_ROOT
    terminal_path = root / "terminal.json"
    report_path = root / "development-nonacceptance-report.json"
    spec_path = root / "development-nonacceptance-spec.json"
    terminal = _load_json(terminal_path, "v191 terminal")
    report = _load_json(report_path, "v191 report")
    spec = _load_json(spec_path, "v191 spec")
    if (
        terminal.get("state") != "waiting_for_extraction_quality_strategy_authorization"
        or terminal.get("terminal_reason")
        != "development_quality_target_not_met_token_target_met"
        or terminal.get("overall_evaluation_complete") is not False
        or terminal.get("evaluation_accepted") is not False
        or terminal.get("semantic_quality_passed") is not False
        or terminal.get("production_amortized_token_target_passed") is not True
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("additional_alignment_authorized") is not False
        or terminal.get("extraction_rerun_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("nonacceptance_report") != _record(report_path)
        or terminal.get("spec") != _record(spec_path)
        or terminal.get("cumulative_known_usage_lower_bound", {}).get("total_tokens")
        != 8294282
        or terminal.get("cumulative_unknown_usage_turn_count") != 1
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 120000
        or report.get("blocker_class")
        != "measured_development_semantic_quality_shortfall"
        or report.get("evaluation_acceptance_receipt_emitted") is not False
        or spec.get("state") != "zero_token_nonacceptance_receipt_completed"
        or spec.get("semantic_model_calls_started") != 0
        or spec.get("extraction_model_calls_started") != 0
    ):
        raise JudgeV5SelectionV192Error("v191 nonacceptance contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV192Error("v191 runtime binding drifted")
    predecessor = v191._validate_v190_stop()
    if terminal.get("cumulative_known_usage_lower_bound") != predecessor["terminal"].get(
        "cumulative_known_usage_lower_bound"
    ):
        raise JudgeV5SelectionV192Error("v191 cumulative accounting drifted")
    return {
        "root": root,
        "terminal": terminal,
        "report": report,
        "spec": spec,
        "records": {
            "terminal": _record(terminal_path),
            "report": _record(report_path),
            "spec": _record(spec_path),
        },
        "predecessor": predecessor,
    }


def _support_filter_f1(row: Mapping[str, Any]) -> float:
    recall = float(row["recall"])
    if recall:
        return 2 * recall / (1 + recall)
    return 1.0 if int(row["reference_units"]) == 0 else 0.0


def _all_composite_score() -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    sources = v186._selection_sources()
    combinations, contract = v188._candidate_combinations(sources=sources)
    all_combinations = [{**row, "cost_and_cap_eligible": True} for row in combinations]
    augmented = v188._with_composite_systems(
        sources=sources, combinations=all_combinations
    )
    v190 = v191._validate_v190_stop()
    consensus_record = v190["spec"]["frozen_inputs"]["consensus"]
    if not _verify_record(consensus_record):
        raise JudgeV5SelectionV192Error("v190 consensus binding drifted")
    consensus = _load_json(Path(consensus_record["path"]), "v190 consensus")
    selected_ids = {str(row["case_id"]) for row in consensus["cases"]}
    score = v186._score_subset(
        consensus=consensus,
        selected_case_ids=selected_ids,
        sources=augmented,
    )
    return augmented, score, combinations


def _dynamic_oracle_audit(
    *, augmented: Mapping[str, Any], score: Mapping[str, Any]
) -> dict[str, Any]:
    sources = v186._selection_sources()
    reports = v186._arm_reports(sources)
    arms = tuple(reports)
    arm_cost = {
        arm: int(reports[arm]["usage"]["total_tokens"]) / 32 for arm in arms
    }
    baseline = {
        row["segment_id"]: row
        for row in score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
    }
    segments = list(baseline)
    options: dict[str, list[tuple[float, float, tuple[str, ...], int]]] = {}
    for segment_id in segments:
        baseline_row = baseline[segment_id]
        rows = [
            (
                0.0,
                1.0 if int(baseline_row["reference_units"]) == 0 else 0.0,
                (),
                0,
            )
        ]
        for size in range(1, len(arms) + 1):
            for subset in itertools.combinations(arms, size):
                system_id = (
                    f"arm:{subset[0]}:normalized"
                    if size == 1
                    else v188._composite_system_id(subset)
                )
                candidate = next(
                    row
                    for row in score["systems"][system_id]["cases"]
                    if row["segment_id"] == segment_id
                )
                event_count = int(
                    augmented["membership"]["system_cases"][system_id][segment_id][
                        "submitted_event_count"
                    ]
                )
                if event_count > MAX_EVENTS_PER_CASE:
                    continue
                rows.append(
                    (
                        sum(arm_cost[arm] for arm in subset),
                        _support_filter_f1(candidate),
                        subset,
                        event_count,
                    )
                )
        options[segment_id] = rows

    cost_contract = v186.load_exact_cost_contract(
        Path(sources["contract"]["selection_inputs"]["context_usage_recovery"]["path"]),
        verify_sidecars=True,
    )
    baseline_end = int(cost_contract["baseline_usage"]["total_tokens"]) + int(
        cost_contract["production_amortized_context_usage"]["total_tokens"]
    )
    maximum_candidate_end = baseline_end * 7 // 25
    context_total = int(
        cost_contract["production_amortized_context_usage"]["total_tokens"]
    )
    development_budget = (maximum_candidate_end - context_total) * len(segments) / 60
    unit = 100
    cap = int(development_budget // unit)
    states: dict[int, tuple[float, list[tuple[str, float, float, tuple[str, ...], int]]]] = {
        0: (0.0, [])
    }
    for segment_id in segments:
        expanded: dict[
            int, tuple[float, list[tuple[str, float, float, tuple[str, ...], int]]]
        ] = {}
        for used, (value, picks) in states.items():
            for cost, candidate_f1, subset, event_count in options[segment_id]:
                new_used = used + math.ceil(cost / unit)
                if new_used > cap:
                    continue
                new_value = value + candidate_f1
                if new_used not in expanded or new_value > expanded[new_used][0]:
                    expanded[new_used] = (
                        new_value,
                        [*picks, (segment_id, cost, candidate_f1, subset, event_count)],
                    )
        best = -1.0
        states = {}
        for used in sorted(expanded):
            if expanded[used][0] > best + 1e-12:
                states[used] = expanded[used]
                best = expanded[used][0]

    passing = []
    for used, (_value, picks) in states.items():
        rows = []
        by_source: dict[str, list[float]] = defaultdict(list)
        for segment_id, _cost, candidate_f1, _subset, _event_count in picks:
            baseline_row = baseline[segment_id]
            row = {
                "segment_id": segment_id,
                "source_id": baseline_row["source_id"],
                "baseline_f1": baseline_row["f1"],
                "candidate_f1": candidate_f1,
            }
            rows.append(row)
            by_source[str(row["source_id"])].append(
                float(candidate_f1) - float(baseline_row["f1"])
            )
        macro = sum(sum(values) / len(values) for values in by_source.values()) / len(
            by_source
        )
        worst = min(sum(values) / len(values) for values in by_source.values())
        if (
            macro < -QUALITY_GATES["max_macro_f1_drop"]
            or worst < -QUALITY_GATES["max_source_f1_drop"]
        ):
            continue
        bootstrap = source_cluster_paired_bootstrap(rows)
        if bootstrap["ci_lower"] < -QUALITY_GATES["max_paired_f1_drop"]:
            continue
        passing.append((used, bootstrap, macro, worst, picks))
    if not passing:
        raise JudgeV5SelectionV192Error("dynamic residual oracle cannot pass frozen gates")
    used, bootstrap, macro, worst, picks = min(passing, key=lambda row: row[0])
    subset_counts: dict[str, int] = defaultdict(int)
    for _segment_id, _cost, _candidate_f1, subset, _event_count in picks:
        subset_counts["+".join(subset) if subset else "no_candidate_events"] += 1
    return {
        "schema_version": V192_AUDIT_VERSION,
        "purpose": "development_ceiling_only_not_a_selectable_system",
        "scored_case_count": len(segments),
        "reference_oracle_used_for_ceiling_only": True,
        "production_semantic_routing_by_reference_forbidden": True,
        "perfect_support_filter_assumed": True,
        "minimum_passing_extraction_cost_upper_rounded_tokens": used * unit,
        "development_extraction_budget_tokens": round(development_budget, 6),
        "remaining_headroom_tokens": round(development_budget - used * unit, 6),
        "bootstrap": bootstrap,
        "macro_source_delta": round(macro, 6),
        "worst_source_delta": round(worst, 6),
        "subset_counts": dict(sorted(subset_counts.items())),
        "maximum_selected_event_count": max(row[4] for row in picks),
        "conclusion": (
            "simple routing and support filtering are insufficient, but source-grounded residual "
            "repair has a nonzero quality-and-token ceiling worth one bounded diagnostic"
        ),
    }


def _selected_cases(
    *, sources: Mapping[str, Any], score: Mapping[str, Any]
) -> list[dict[str, Any]]:
    baseline = {
        row["segment_id"]: row
        for row in score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
    }
    candidate = {
        row["segment_id"]: row for row in score["systems"][BASE_SYSTEM_ID]["cases"]
    }
    selected_ids = []
    source_ids = sorted({str(row["source_id"]) for row in baseline.values()})
    for source_id in source_ids:
        rows = [
            row
            for row in baseline.values()
            if row["source_id"] == source_id and row["density_stratum"] != "no_signal"
        ]
        selected = min(
            rows,
            key=lambda row: (
                float(candidate[row["segment_id"]]["f1"]) - float(row["f1"]),
                str(row["case_id"]),
            ),
        )
        selected_ids.append(str(selected["case_id"]))
    pool_by_id = {str(row["case_id"]): row for row in sources["pool"]["cases"]}
    no_signal = [
        row for row in baseline.values() if row["density_stratum"] == "no_signal"
    ]
    no_signal_case = max(
        no_signal,
        key=lambda row: (
            len(pool_by_id[str(row["case_id"])]["source_excerpt"]),
            str(row["case_id"]),
        ),
    )
    selected_ids.append(str(no_signal_case["case_id"]))
    if len(selected_ids) != DIAGNOSTIC_CASE_COUNT or len(set(selected_ids)) != len(
        selected_ids
    ):
        raise JudgeV5SelectionV192Error("diagnostic case selection drifted")

    mapping_by_id = {str(row["case_id"]): row for row in sources["mapping"]["cases"]}
    component_ids = {f"arm:{arm}:normalized" for arm in BASE_ARMS}
    selected_cases = []
    for case_id in selected_ids:
        mapping_case = mapping_by_id[case_id]
        events = []
        seen = set()
        for witness in mapping_case["witnesses"]:
            provenance = witness["provenance"]
            if not any(
                membership.get("system_id") in component_ids
                for membership in provenance.get("memberships") or []
            ):
                continue
            event_hash = str(provenance["canonical_event_sha256"])
            if event_hash in seen:
                continue
            seen.add(event_hash)
            events.append(compact_empty_event_fields(provenance["original_event"]))
        source_excerpt = str(pool_by_id[case_id]["source_excerpt"])
        windows, boundaries = build_windowed_segment_packet(
            source_excerpt, window_count=1, context_chars=0
        )
        baseline_row = baseline[str(mapping_case["case_key"])]
        candidate_row = candidate[str(mapping_case["case_key"])]
        selected_cases.append(
            {
                "case_id": case_id,
                "case_key": mapping_case["case_key"],
                "source_id": mapping_case["case_provenance"]["source_id"],
                "density_stratum": mapping_case["case_provenance"]["density_stratum"],
                "source_excerpt": source_excerpt,
                "windows": windows,
                "boundaries": boundaries,
                "candidate_events": events,
                "candidate_event_count": len(events),
                "baseline_f1": baseline_row["f1"],
                "base_candidate_f1": candidate_row["f1"],
            }
        )
    return selected_cases


def residual_repair_instructions() -> tuple[str, str]:
    guidelines, guideline_sha = _load_windowed_guideline_instructions(
        DEFAULT_WINDOWED_GUIDELINES_PATH
    )
    core = _windowed_event_core_instruction(
        guideline_instructions=guidelines,
        max_total_events=MAX_EVENTS_PER_CASE,
    )
    contract = f"""

# Residual repair contract
You receive {DIAGNOSTIC_CASE_COUNT} independent source cases. Each case includes the complete
source excerpt as window 0 plus a fallible draft assembled from two prior low-cost extraction
passes. Inspect every source excerpt yourself. The draft is evidence, not truth.

For each case, return the final source-grounded event set. Remove unsupported, duplicated,
misattributed, or materially malformed draft events. Recover material eligible events omitted by
the draft. Every semantic decision must come from your reading of the full source excerpt. Never
use event counts, field presence, keywords, or draft provenance as a semantic shortcut.

Every returned evidence value must be one exact contiguous substring of that case's source
excerpt. Use window_id 0. Preserve all material truth-conditional fields, including actor,
speaker, reported actor, stance, certainty, target, causal mechanism, metric, negation, event
boundary, event type, and temporal horizon. Return no events for true no-signal or excluded cases.
Return exactly one segment object for every requested case_id, in the supplied order, with no more
than {MAX_EVENTS_PER_CASE} events per case. This is one bounded repair attempt; do not discuss the
draft or your reasoning in the output.
""".strip()
    return core + "\n\n" + contract, str(guideline_sha or "")


def residual_repair_prompt(cases: Sequence[Mapping[str, Any]]) -> str:
    packets = [
        {
            "segment_id": row["case_id"],
            "windows": row["windows"],
            "draft_candidate_events": row["candidate_events"],
        }
        for row in cases
    ]
    return (
        "# Independent source cases and fallible draft events\n"
        + json.dumps(packets, ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )


def _repair_budget(sources: Mapping[str, Any]) -> dict[str, Any]:
    reports = v186._arm_reports(sources)
    contract = v186.load_exact_cost_contract(
        Path(sources["contract"]["selection_inputs"]["context_usage_recovery"]["path"]),
        verify_sidecars=True,
    )
    base_usage = {
        field: sum(int(reports[arm]["usage"][field]) for arm in BASE_ARMS)
        for field in reports[BASE_ARMS[0]]["usage"]
    }
    baseline_segments = int(contract["baseline_segments"])
    base_scaled = math.ceil(base_usage["total_tokens"] * baseline_segments / 32)
    baseline_end = int(contract["baseline_usage"]["total_tokens"]) + int(
        contract["production_amortized_context_usage"]["total_tokens"]
    )
    maximum_candidate_end = baseline_end * 7 // 25
    context_total = int(
        contract["production_amortized_context_usage"]["total_tokens"]
    )
    repair_headroom_for_60 = maximum_candidate_end - context_total - base_scaled
    per_segment = repair_headroom_for_60 / baseline_segments
    diagnostic_headroom = math.floor(per_segment * DIAGNOSTIC_CASE_COUNT)
    if diagnostic_headroom < MAXIMUM_TOTAL_TOKENS_PER_TURN:
        raise JudgeV5SelectionV192Error("repair diagnostic token bound exceeds production headroom")
    return {
        "base_arms": list(BASE_ARMS),
        "base_usage": base_usage,
        "base_production_amortized_total_token_ratio": 0.149116,
        "baseline_end_to_end_tokens": baseline_end,
        "maximum_candidate_end_to_end_tokens_lte_0_28": maximum_candidate_end,
        "shared_context_tokens": context_total,
        "base_extraction_tokens_scaled_to_60_segments": base_scaled,
        "repair_headroom_tokens_for_60_segments": repair_headroom_for_60,
        "repair_headroom_tokens_per_segment": round(per_segment, 6),
        "diagnostic_case_count": DIAGNOSTIC_CASE_COUNT,
        "diagnostic_repair_headroom_tokens": diagnostic_headroom,
        "declared_maximum_total_tokens": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "declared_bound_fits_production_headroom": True,
    }


def freeze_v192(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v192 terminal")
    predecessor = _validate_v191_nonacceptance()
    augmented, score, combinations = _all_composite_score()
    oracle = _dynamic_oracle_audit(augmented=augmented, score=score)
    oracle_path = root / "residual-repair-oracle-audit.json"
    _write_stable_time(oracle_path, oracle, "created_at")
    sources = v186._selection_sources()
    cases = _selected_cases(sources=sources, score=score)
    instructions, guideline_sha = residual_repair_instructions()
    prompt = residual_repair_prompt(cases)
    case_ids = [str(row["case_id"]) for row in cases]
    schema = episode_batch_core_schema(
        episode_id=DIAGNOSTIC_EPISODE_ID,
        segment_ids=case_ids,
        max_events_per_segment=MAX_EVENTS_PER_CASE,
    )
    private_input = {
        "schema_version": "pif_app_server_residual_repair_input_v1",
        "episode_id": DIAGNOSTIC_EPISODE_ID,
        "cases": cases,
    }
    input_path = root / "residual-repair-input.private.json"
    prompt_path = root / "residual-repair-prompt.private.md"
    schema_path = root / "residual-repair-schema.json"
    instructions_path = root / "residual-repair-instructions.private.md"
    _write_immutable(input_path, private_input)
    if prompt_path.exists() and prompt_path.read_text(encoding="utf-8") != prompt:
        raise JudgeV5SelectionV192Error("frozen repair prompt drifted")
    if not prompt_path.exists():
        prompt_path.write_text(prompt, encoding="utf-8")
    if instructions_path.exists() and instructions_path.read_text(encoding="utf-8") != instructions:
        raise JudgeV5SelectionV192Error("frozen repair instructions drifted")
    if not instructions_path.exists():
        instructions_path.write_text(instructions, encoding="utf-8")
    _write_immutable(schema_path, schema)
    budget = _repair_budget(sources)
    design = {
        "schema_version": V192_DESIGN_VERSION,
        "state": "frozen_before_semantic_attempt",
        "created_at": now_iso(),
        "strategy": "source_grounded_residual_repair_over_existing_two_batch8_arm_draft",
        "not_an_extraction_replay": True,
        "existing_extraction_outputs_reused_only": True,
        "batch_5_replayed": False,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count_per_turn": 0,
        "case_selection": (
            "worst_dense_base_pair_delta_per_source_plus_largest_source_excerpt_no_signal_case"
        ),
        "selected_cases": [
            {
                "case_id": row["case_id"],
                "source_id": row["source_id"],
                "density_stratum": row["density_stratum"],
                "source_excerpt_chars": len(row["source_excerpt"]),
                "draft_event_count": row["candidate_event_count"],
                "baseline_f1": row["baseline_f1"],
                "base_candidate_f1": row["base_candidate_f1"],
            }
            for row in cases
        ],
        "prompt_bytes": len(prompt.encode("utf-8")),
        "instructions_bytes": len(instructions.encode("utf-8")),
        "schema_bytes": len(json.dumps(schema, ensure_ascii=True).encode("utf-8")),
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "repair_budget": budget,
        "phase_one_structural_gates": {
            "schema_and_status_success": True,
            "exact_evidence_rate": 1.0,
            "metric_grounding_error_events": 0,
            "no_signal_false_positive_events": 0,
            "event_cap_per_case": MAX_EVENTS_PER_CASE,
            "projected_production_amortized_token_ratio_lte": 0.28,
        },
        "phase_two_authorization": (
            "only after every phase-one structural and token gate passes, run the frozen pointwise "
            "support and neutral alignment judge on newly returned events"
        ),
        "diagnostic_promotion_rule": (
            "after frozen support/alignment scoring, all four dense source cases must improve over "
            "the base pair, no source case may be worse than baseline by more than 0.05, no-signal "
            "false positives must be zero, exact evidence must remain 100%, and projected total "
            "tokens must remain at or below 0.28"
        ),
        "semantic_regex_or_keyword_rules_used": False,
        "semantic_similarity_or_embeddings_used": False,
        "deterministic_operations_allowed": [
            "schema_validation",
            "exact_evidence_offsets",
            "metric_literal_grounding",
            "global_event_cap",
            "provenance",
            "accounting",
        ],
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "oracle_audit": _record(oracle_path),
        "predecessor": predecessor["records"],
        "frozen_inputs": {
            "private_input": _record(input_path),
            "prompt": _record(prompt_path),
            "instructions": _record(instructions_path),
            "schema": _record(schema_path),
            "shared_pool": sources["pool_record"],
            "private_mapping": sources["mapping_record"],
            "membership_index": sources["membership_record"],
            "guideline_sha256": guideline_sha,
            "instructions_sha256": sha256_text(instructions),
        },
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v191.__file__)),
            _record(Path(v188.__file__)),
            _record(Path(v186.__file__)),
        ],
        "privacy": "private_source_and_events_sanitized_reports_counts_hashes_metrics_only",
    }
    design_path = root / "residual-repair-design.json"
    _write_stable_time(design_path, design, "created_at")
    terminal = {
        "schema_version": V192_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v192_residual_repair_diagnostic_frozen_semantic_attempt_authorized",
        "overall_evaluation_complete": False,
        "semantic_attempt_authorized": True,
        "authorized_turn_count": 1,
        "authorized_model": MODEL,
        "authorized_effort": EFFORT,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "attempt_usage_status": "complete",
        "attempt_usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "total_tokens": 0,
        },
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": 1,
        "cumulative_conservative_unknown_usage_upper_bound": 120000,
        "design": _record(design_path),
        "oracle_audit": _record(oracle_path),
        "required_next_artifact_path": str(
            root.parent
            / "development-selection-v5_4-v193-residual-repair-diagnostic"
            / "terminal.json"
        ),
    }
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v192 residual-repair design")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v192(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "semantic_attempt_authorized": terminal["semantic_attempt_authorized"],
                "holdout_authorized": terminal["holdout_authorized"],
                "production_mutated": terminal["production_mutated"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
