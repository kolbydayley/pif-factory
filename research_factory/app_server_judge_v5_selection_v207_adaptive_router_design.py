from __future__ import annotations

"""Freeze a bounded source-grounded router over the five completed extraction arms."""

import argparse
import itertools
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from . import app_server_judge_v5_selection_v188_composite_postprocess as v188
from . import app_server_judge_v5_selection_v192_residual_repair_design as v192
from . import app_server_judge_v5_selection_v206_convergence_blocker as v206
from .app_server_dev_selection import (
    BASELINE_REPAIRED_SYSTEM,
    QUALITY_GATES,
    source_cluster_paired_bootstrap,
)
from .app_server_judge_v5 import compact_empty_event_fields
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .util import now_iso, stable_id


V207_ORACLE_VERSION = "pif_app_server_judge_v5_4_selection_v207_oracle_v1"
V207_DESIGN_VERSION = "pif_app_server_judge_v5_4_selection_v207_design_v1"
V207_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v207_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v207_adaptive_router_design"
DEFAULT_OUTPUT_ROOT = (
    v206.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v207-adaptive-router-design"
).resolve()

MODEL = "gpt-5.6-sol"
EFFORT = "low"
BASE_ARM = "batch_5_new_thread"
CANARY_CASE_COUNT = 8
MAX_CANARY_TOTAL_TOKENS = 25_000
MAX_FULL_ROUTER_TOTAL_TOKENS = 56_000
MAX_EVENTS_PER_CASE = 32
TIMEOUT_SECONDS = 1200.0
MARKER_OPEN = "<<+"
MARKER_CLOSE = "<<-"


class JudgeV5SelectionV207Error(RuntimeError):
    """The adaptive-router diagnostic cannot be frozen safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v206_checkpoint() -> dict[str, Any]:
    root = v206.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "report": root / "convergence-report.json",
        "next_experiment": root / "next-experiment.json",
        "spec": root / "convergence-spec.json",
    }
    values = {name: _load_json(path, f"v206 {name}") for name, path in paths.items()}
    terminal = values["terminal"]
    report = values["report"]
    if (
        terminal.get("state") != "waiting_for_next_semantic_strategy_authorization"
        or terminal.get("terminal_reason")
        != "development_convergence_timebox_reached_no_system_meets_joint_gates"
        or terminal.get("terminal_classification")
        != "inactive_incomplete_recovery_required"
        or terminal.get("overall_evaluation_complete") is not False
        or terminal.get("semantic_quality_passed") is not False
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("next_semantic_attempt_started") is not False
        or terminal.get("cumulative_known_usage_lower_bound", {}).get("total_tokens")
        != 8_900_303
        or terminal.get("cumulative_unknown_usage_turn_count") != 2
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 220_000
        or terminal.get("convergence_report") != _record(paths["report"])
        or terminal.get("next_experiment") != _record(paths["next_experiment"])
        or terminal.get("spec") != _record(paths["spec"])
        or report.get("viable_systems_meeting_joint_gates") != []
    ):
        raise JudgeV5SelectionV207Error("v206 convergence checkpoint drifted")
    for path in paths.values():
        if not _verify_record(_record(path)):
            raise JudgeV5SelectionV207Error("v206 artifact binding drifted")
    predecessor = v206._validate_v205_quality_failure()
    historical = v206._validate_historical_frontier()
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "predecessor": predecessor,
        "historical": historical,
        **values,
    }


def _package_mapping(arms: Sequence[str]) -> dict[str, str]:
    ordered = sorted(
        arms,
        key=lambda arm: stable_id(PHASE_ID, arm, prefix="package_rank_"),
    )
    return {arm: f"pkg_{index:02d}" for index, arm in enumerate(ordered, start=1)}


def _arm_costs(sources: Mapping[str, Any]) -> dict[str, float]:
    reports = v186._arm_reports(sources)
    return {
        arm: int(report["usage"]["total_tokens"]) / 32
        for arm, report in reports.items()
    }


def _development_budget(
    sources: Mapping[str, Any], *, case_count: int
) -> dict[str, float | int]:
    contract_path = Path(
        sources["contract"]["selection_inputs"]["context_usage_recovery"]["path"]
    )
    contract = v186.load_exact_cost_contract(contract_path, verify_sidecars=True)
    baseline_end = int(contract["baseline_usage"]["total_tokens"]) + int(
        contract["production_amortized_context_usage"]["total_tokens"]
    )
    context = int(contract["production_amortized_context_usage"]["total_tokens"])
    maximum_candidate_end = baseline_end * 7 // 25
    extraction_and_router_for_60 = maximum_candidate_end - context
    return {
        "baseline_end_to_end_tokens": baseline_end,
        "production_amortized_context_tokens": context,
        "maximum_candidate_end_to_end_tokens": maximum_candidate_end,
        "extraction_and_router_budget_for_60_segments": extraction_and_router_for_60,
        "scope_case_count": case_count,
        "scope_extraction_and_router_budget_tokens": extraction_and_router_for_60
        * case_count
        / 60,
    }


def _selected_canary_rows(score: Mapping[str, Any]) -> list[dict[str, Any]]:
    baseline_rows = score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in baseline_rows:
        by_source[str(row["source_id"])].append(row)
    selected: list[dict[str, Any]] = []
    for source_id in sorted(by_source):
        rows = by_source[source_id]
        no_signal = [row for row in rows if row["density_stratum"] == "no_signal"]
        dense = [row for row in rows if row["density_stratum"] != "no_signal"]
        if not no_signal or not dense:
            raise JudgeV5SelectionV207Error("router canary strata are incomplete")

        def rank(row: Mapping[str, Any]) -> str:
            return stable_id(PHASE_ID, str(row["case_id"]), prefix="rank_")

        selected.extend((min(no_signal, key=rank), min(dense, key=rank)))
    if (
        len(selected) != CANARY_CASE_COUNT
        or len({str(row["source_id"]) for row in selected}) != 4
        or sum(row["density_stratum"] == "no_signal" for row in selected) != 4
        or sum(row["density_stratum"] != "no_signal" for row in selected) != 4
    ):
        raise JudgeV5SelectionV207Error("router canary selection drifted")
    return selected


def _events_for_arm(
    mapping_case: Mapping[str, Any], *, arm: str
) -> list[dict[str, Any]]:
    system_id = f"arm:{arm}:normalized"
    events = []
    for witness in mapping_case.get("witnesses") or []:
        provenance = witness.get("provenance") or {}
        if any(
            membership.get("system_id") == system_id
            for membership in provenance.get("memberships") or []
        ):
            events.append(compact_empty_event_fields(provenance["original_event"]))
    return events


def _all_exact_occurrences(source: str, evidence: str) -> list[tuple[int, int]]:
    if not evidence:
        raise JudgeV5SelectionV207Error("base event evidence is empty")
    occurrences = []
    offset = 0
    while True:
        start = source.find(evidence, offset)
        if start < 0:
            break
        occurrences.append((start, start + len(evidence)))
        offset = start + max(1, len(evidence))
    if not occurrences:
        raise JudgeV5SelectionV207Error("base event evidence is not exact source text")
    return occurrences


def _annotate_source(
    source: str, events: Sequence[Mapping[str, Any]]
) -> tuple[str, list[dict[str, str]]]:
    if MARKER_OPEN in source or MARKER_CLOSE in source:
        raise JudgeV5SelectionV207Error("source collides with router evidence markers")
    starts: dict[int, list[str]] = defaultdict(list)
    ends: dict[int, list[str]] = defaultdict(list)
    summaries = []
    for index, event in enumerate(events, start=1):
        event_id = f"e{index:02d}"
        evidence = str(event.get("evidence") or "")
        for start, end in _all_exact_occurrences(source, evidence):
            starts[start].append(event_id)
            ends[end].append(event_id)
        summaries.append({"id": event_id, "claim": str(event.get("claim_text") or "")})
    parts = []
    cursor = 0
    for position in sorted(set(starts) | set(ends)):
        parts.append(source[cursor:position])
        if ends[position]:
            parts.append(f"{MARKER_CLOSE}{','.join(sorted(ends[position]))}>>")
        if starts[position]:
            parts.append(f"{MARKER_OPEN}{','.join(sorted(starts[position]))}>>")
        cursor = position
    parts.append(source[cursor:])
    annotated = "".join(parts)
    return annotated, summaries


def strip_evidence_markers(value: str) -> str:
    """Remove only validated router markers to prove source-text preservation."""

    output = []
    cursor = 0
    while cursor < len(value):
        if value.startswith(MARKER_OPEN, cursor) or value.startswith(MARKER_CLOSE, cursor):
            end = value.find(">>", cursor)
            if end < 0:
                raise JudgeV5SelectionV207Error("unterminated router evidence marker")
            marker = value[cursor + 3 : end]
            if not marker or any(
                not item.startswith("e") or not item[1:].isdigit()
                for item in marker.split(",")
            ):
                raise JudgeV5SelectionV207Error("malformed router evidence marker")
            cursor = end + 2
            continue
        output.append(value[cursor])
        cursor += 1
    return "".join(output)


def _router_cases(
    *, rows: Sequence[Mapping[str, Any]], sources: Mapping[str, Any]
) -> list[dict[str, Any]]:
    mapping_by_key = {
        str(row["case_key"]): row for row in sources["mapping"]["cases"]
    }
    pool_by_id = {str(row["case_id"]): row for row in sources["pool"]["cases"]}
    cases = []
    for row in rows:
        case_id = str(row["case_id"])
        segment_id = str(row["segment_id"])
        mapping_case = mapping_by_key[segment_id]
        source = str(pool_by_id[case_id]["source_excerpt"])
        events = _events_for_arm(mapping_case, arm=BASE_ARM)
        annotated, summaries = _annotate_source(source, events)
        if strip_evidence_markers(annotated) != source:
            raise JudgeV5SelectionV207Error("router annotation changed source text")
        cases.append(
            {
                "case_id": case_id,
                "annotated_source": annotated,
                "base_event_claims": summaries,
            }
        )
    return cases


def _router_schema(case_ids: Sequence[str], package_ids: Sequence[str]) -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["cases"],
        "properties": {
            "cases": {
                "type": "array",
                "minItems": len(case_ids),
                "maxItems": len(case_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "case_id",
                        "base_coverage",
                        "route_reason",
                        "selected_package_ids",
                    ],
                    "properties": {
                        "case_id": {"type": "string", "enum": list(case_ids)},
                        "base_coverage": {
                            "type": "string",
                            "enum": [
                                "complete",
                                "material_gaps",
                                "unsupported_or_no_signal",
                                "abstain",
                            ],
                        },
                        "route_reason": {
                            "type": "string",
                            "enum": [
                                "base_sufficient",
                                "additional_independent_coverage_needed",
                                "discard_unsupported_base",
                                "no_eligible_events",
                                "abstain",
                            ],
                        },
                        "selected_package_ids": {
                            "type": "array",
                            "items": {"type": "string", "enum": list(package_ids)},
                            "uniqueItems": True,
                            "maxItems": len(package_ids),
                        },
                    },
                },
            }
        },
    }


def router_instructions() -> str:
    return """
You are a source-grounded completeness router, not an extractor and not a quality judge with
access to reference labels. Read every case's complete annotated source and every listed base
event claim. The source text is unchanged except for exact-evidence markers: <<+e01>> begins an
exact evidence occurrence for base event e01 and <<-e01>> ends it. An event ID may mark multiple
literal occurrences. Markers show evidence coverage, not that every material proposition inside a
span was captured.

Choose which opaque extraction packages should contribute events for each case. The base package
has already run and is always charged, but you may omit it from selected_package_ids when its
entire output should be discarded. Any other selected package is an additional independent pass
that will run after this decision. The final candidate will be the exact-identity union of the
selected package outputs; there is no later semantic filtering. Select no package for a true
no-signal case. Request extra packages only when the source contains material eligible events not
adequately represented by the base claims, and avoid paying for redundant passes. Use package
costs and the single global extraction budget supplied in the prompt. Return package IDs in
lexicographic order. Do not infer or discuss hidden systems, reference answers, density labels, or
scores. Output only the required structured object.
""".strip()


def _router_prompt(
    *,
    cases: Sequence[Mapping[str, Any]],
    catalog: Sequence[Mapping[str, Any]],
    base_package_id: str,
    extraction_budget_tokens: float,
) -> str:
    packet = {
        "marker_contract": (
            "<<+event_id>> opens and <<-event_id>> closes exact evidence; all non-marker "
            "characters are the complete unchanged source"
        ),
        "base_package_id": base_package_id,
        "base_package_always_charged": True,
        "global_extraction_budget_tokens": round(extraction_budget_tokens, 6),
        "package_catalog": list(catalog),
        "cases": list(cases),
    }
    return "# Source-grounded routing packet\n" + json.dumps(
        packet, ensure_ascii=True, separators=(",", ":")
    )


def _option_rows(
    *,
    augmented: Mapping[str, Any],
    score: Mapping[str, Any],
    baseline_rows: Sequence[Mapping[str, Any]],
    arms: Sequence[str],
) -> dict[str, list[tuple[tuple[str, ...], float, int]]]:
    options = {}
    for baseline in baseline_rows:
        segment_id = str(baseline["segment_id"])
        rows = [
            (
                (),
                1.0 if int(baseline["reference_units"]) == 0 else 0.0,
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
                if event_count <= MAX_EVENTS_PER_CASE:
                    rows.append((subset, float(candidate["f1"]), event_count))
        options[segment_id] = rows
    return options


def _frontier(
    *,
    rows: Sequence[Mapping[str, Any]],
    options: Mapping[str, Sequence[tuple[tuple[str, ...], float, int]]],
    arm_costs: Mapping[str, float],
    extraction_budget: float,
) -> dict[int, tuple[float, list[tuple[str, float, tuple[str, ...], int]]]]:
    unit = 100
    states: dict[
        int, tuple[float, list[tuple[str, float, tuple[str, ...], int]]]
    ] = {0: (0.0, [])}
    for baseline in rows:
        segment_id = str(baseline["segment_id"])
        expanded: dict[
            int, tuple[float, list[tuple[str, float, tuple[str, ...], int]]]
        ] = {}
        for used, (value, picks) in states.items():
            for subset, candidate_f1, event_count in options[segment_id]:
                cost = arm_costs[BASE_ARM] + sum(
                    arm_costs[arm] for arm in subset if arm != BASE_ARM
                )
                new_used = used + math.ceil(cost / unit)
                if new_used > int(extraction_budget // unit):
                    continue
                new_value = value + candidate_f1
                if new_used not in expanded or new_value > expanded[new_used][0]:
                    expanded[new_used] = (
                        new_value,
                        [*picks, (segment_id, candidate_f1, subset, event_count)],
                    )
        best = -1.0
        states = {}
        for used in sorted(expanded):
            if expanded[used][0] > best + 1e-12:
                states[used] = expanded[used]
                best = expanded[used][0]
    if not states:
        raise JudgeV5SelectionV207Error("router frontier is empty")
    return states


def _route_metrics(
    *,
    baseline_by_segment: Mapping[str, Mapping[str, Any]],
    picks: Sequence[tuple[str, float, tuple[str, ...], int]],
) -> tuple[dict[str, Any], float, float]:
    rows = []
    by_source: dict[str, list[float]] = defaultdict(list)
    for segment_id, candidate_f1, _subset, _event_count in picks:
        baseline = baseline_by_segment[segment_id]
        rows.append(
            {
                "segment_id": segment_id,
                "source_id": baseline["source_id"],
                "baseline_f1": baseline["f1"],
                "candidate_f1": candidate_f1,
            }
        )
        by_source[str(baseline["source_id"])].append(
            candidate_f1 - float(baseline["f1"])
        )
    macro = sum(sum(values) / len(values) for values in by_source.values()) / len(
        by_source
    )
    worst = min(sum(values) / len(values) for values in by_source.values())
    return source_cluster_paired_bootstrap(rows), macro, worst


def _oracle_audit(
    *,
    augmented: Mapping[str, Any],
    score: Mapping[str, Any],
    sources: Mapping[str, Any],
    canary_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    reports = v186._arm_reports(sources)
    arms = tuple(reports)
    costs = _arm_costs(sources)
    full_rows = score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
    options = _option_rows(
        augmented=augmented,
        score=score,
        baseline_rows=full_rows,
        arms=arms,
    )
    full_budget = _development_budget(sources, case_count=len(full_rows))
    full_frontier = _frontier(
        rows=full_rows,
        options=options,
        arm_costs=costs,
        extraction_budget=float(full_budget["scope_extraction_and_router_budget_tokens"]),
    )
    baseline_by_segment = {str(row["segment_id"]): row for row in full_rows}
    passing = []
    for used, (_value, picks) in full_frontier.items():
        bootstrap, macro, worst = _route_metrics(
            baseline_by_segment=baseline_by_segment, picks=picks
        )
        if (
            macro >= -QUALITY_GATES["max_macro_f1_drop"]
            and worst >= -QUALITY_GATES["max_source_f1_drop"]
            and bootstrap["ci_lower"] >= -QUALITY_GATES["max_paired_f1_drop"]
        ):
            passing.append((used, bootstrap, macro, worst, picks))
    if not passing:
        raise JudgeV5SelectionV207Error("base-observed router has no full quality ceiling")
    used, bootstrap, macro, worst, full_picks = min(passing, key=lambda row: row[0])
    full_cost = used * 100
    full_headroom = (
        float(full_budget["scope_extraction_and_router_budget_tokens"]) - full_cost
    )
    if full_headroom < MAX_FULL_ROUTER_TOTAL_TOKENS:
        raise JudgeV5SelectionV207Error("full router token bound exceeds quality ceiling")

    canary_budget = _development_budget(sources, case_count=len(canary_rows))
    canary_extraction_budget = (
        float(canary_budget["scope_extraction_and_router_budget_tokens"])
        - MAX_CANARY_TOTAL_TOKENS
    )
    canary_frontier = _frontier(
        rows=canary_rows,
        options=options,
        arm_costs=costs,
        extraction_budget=canary_extraction_budget,
    )
    canary_used, (canary_value, canary_picks) = max(
        canary_frontier.items(), key=lambda item: item[1][0]
    )
    return {
        "schema_version": V207_ORACLE_VERSION,
        "purpose": "development_ceiling_and_diagnostic_stop_rule_only",
        "reference_oracle_used_for_scoring_only": True,
        "router_model_never_receives_reference_scores_or_density": True,
        "production_semantic_routing_by_reference_forbidden": True,
        "base_arm_always_observed_and_charged": BASE_ARM,
        "full_development": {
            "case_count": len(full_rows),
            "extraction_and_router_budget_tokens": full_budget[
                "scope_extraction_and_router_budget_tokens"
            ],
            "minimum_passing_extraction_cost_upper_rounded_tokens": full_cost,
            "remaining_router_headroom_tokens": round(full_headroom, 6),
            "declared_full_router_bound_tokens": MAX_FULL_ROUTER_TOTAL_TOKENS,
            "bootstrap": bootstrap,
            "macro_source_delta": round(macro, 6),
            "worst_source_delta": round(worst, 6),
            "selected_event_count": sum(row[3] for row in full_picks),
            "passing_frontier_state_count": len(passing),
        },
        "canary": {
            "case_count": len(canary_rows),
            "extraction_and_router_budget_tokens": canary_budget[
                "scope_extraction_and_router_budget_tokens"
            ],
            "declared_router_bound_tokens": MAX_CANARY_TOTAL_TOKENS,
            "extraction_budget_after_declared_router_bound": round(
                canary_extraction_budget, 6
            ),
            "best_affordable_candidate_f1": round(
                canary_value / len(canary_rows), 6
            ),
            "best_affordable_extraction_cost_upper_rounded_tokens": canary_used * 100,
            "best_affordable_picks": [
                {
                    "segment_id": segment_id,
                    "candidate_f1": round(candidate_f1, 6),
                    "arms": list(subset),
                    "event_count": event_count,
                }
                for segment_id, candidate_f1, subset, event_count in canary_picks
            ],
        },
        "arm_cost_tokens_per_segment": {
            arm: round(value, 6) for arm, value in sorted(costs.items())
        },
        "conclusion": (
            "one base-observed source-grounded routing canary is the smallest remaining "
            "zero-extraction diagnostic"
        ),
    }


def freeze_v207(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v207 terminal")
    if any(root.iterdir()):
        raise JudgeV5SelectionV207Error("v207 root is nonempty without a terminal")

    predecessor = _validate_v206_checkpoint()
    augmented, score, _combinations = v192._all_composite_score()
    sources = v186._selection_sources()
    canary_rows = _selected_canary_rows(score)
    oracle = _oracle_audit(
        augmented=augmented,
        score=score,
        sources=sources,
        canary_rows=canary_rows,
    )
    oracle_path = root / "adaptive-router-oracle-audit.json"
    _write_immutable(oracle_path, oracle)

    arms = tuple(v186._arm_reports(sources))
    packages = _package_mapping(arms)
    costs = _arm_costs(sources)
    catalog = [
        {
            "package_id": packages[arm],
            "batch_size": int(arm.split("_")[1]),
            "context_mode": (
                "same_thread" if arm.endswith("same_thread") else "new_thread"
            ),
            "cost_tokens_per_segment": round(costs[arm], 6),
        }
        for arm in sorted(arms, key=lambda item: packages[item])
    ]
    full_rows = score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
    canary_cases = _router_cases(rows=canary_rows, sources=sources)
    full_cases = _router_cases(rows=full_rows, sources=sources)
    canary_budget = float(
        oracle["canary"]["extraction_budget_after_declared_router_bound"]
    )
    full_budget = float(
        oracle["full_development"]["extraction_and_router_budget_tokens"]
    ) - MAX_FULL_ROUTER_TOTAL_TOKENS
    instructions = router_instructions()
    canary_prompt = _router_prompt(
        cases=canary_cases,
        catalog=catalog,
        base_package_id=packages[BASE_ARM],
        extraction_budget_tokens=canary_budget,
    )
    full_prompt = _router_prompt(
        cases=full_cases,
        catalog=catalog,
        base_package_id=packages[BASE_ARM],
        extraction_budget_tokens=full_budget,
    )
    package_ids = [row["package_id"] for row in catalog]
    canary_ids = [str(row["case_id"]) for row in canary_rows]
    full_ids = [str(row["case_id"]) for row in full_rows]
    canary_schema = _router_schema(canary_ids, package_ids)
    full_schema = _router_schema(full_ids, package_ids)
    canary_input = {
        "schema_version": "pif_app_server_adaptive_router_input_v1",
        "scope": "canary",
        "cases": canary_cases,
        "package_catalog": catalog,
        "base_package_id": packages[BASE_ARM],
        "opaque_package_mapping": packages,
    }
    full_input = {
        "schema_version": "pif_app_server_adaptive_router_input_v1",
        "scope": "full_development_projection_not_authorized",
        "cases": full_cases,
        "package_catalog": catalog,
        "base_package_id": packages[BASE_ARM],
        "opaque_package_mapping": packages,
    }
    paths = {
        "canary_input": root / "canary-input.private.json",
        "canary_prompt": root / "canary-prompt.private.md",
        "canary_schema": root / "canary-schema.json",
        "full_input": root / "full-router-projection-input.private.json",
        "full_prompt": root / "full-router-projection-prompt.private.md",
        "full_schema": root / "full-router-projection-schema.json",
        "instructions": root / "router-instructions.private.md",
    }
    _write_immutable(paths["canary_input"], canary_input)
    _write_immutable(paths["canary_schema"], canary_schema)
    _write_immutable(paths["full_input"], full_input)
    _write_immutable(paths["full_schema"], full_schema)
    for key, value in (
        ("canary_prompt", canary_prompt),
        ("full_prompt", full_prompt),
        ("instructions", instructions),
    ):
        if paths[key].exists() and paths[key].read_text(encoding="utf-8") != value:
            raise JudgeV5SelectionV207Error(f"frozen {key} drifted")
        if not paths[key].exists():
            paths[key].write_text(value, encoding="utf-8")

    request_bytes = {
        "canary": len(canary_prompt.encode("utf-8"))
        + len(instructions.encode("utf-8"))
        + len(json.dumps(canary_schema, ensure_ascii=True).encode("utf-8")),
        "full_projection": len(full_prompt.encode("utf-8"))
        + len(instructions.encode("utf-8"))
        + len(json.dumps(full_schema, ensure_ascii=True).encode("utf-8")),
    }
    design = {
        "schema_version": V207_DESIGN_VERSION,
        "state": "frozen_before_semantic_attempt",
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "strategy": "base_observed_source_grounded_adaptive_arm_router",
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "declared_canary_turn_count": 1,
        "declared_full_turn_count": 0,
        "retry_count_per_turn": 0,
        "maximum_canary_total_tokens": MAX_CANARY_TOTAL_TOKENS,
        "maximum_full_router_total_tokens_if_later_authorized": MAX_FULL_ROUTER_TOTAL_TOKENS,
        "base_arm_always_observed_and_charged": BASE_ARM,
        "base_package_id": packages[BASE_ARM],
        "package_mapping_private": packages,
        "canary_case_ids": canary_ids,
        "canary_selection_policy": (
            "one stable-hash-ranked dense and one stable-hash-ranked no-signal case per source"
        ),
        "selection_uses_reference_quality_or_candidate_scores": False,
        "all_source_characters_preserved": True,
        "all_base_events_represented_by_exact_evidence_markers_and_claims": True,
        "semantic_output_scope": "package_selection_only_no_event_extraction_or_rewrite",
        "existing_extraction_outputs_reused_only": True,
        "extraction_model_calls_authorized": 0,
        "extraction_replay_allowed": False,
        "batch_5_replay_allowed": False,
        "full_development_router_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "canary_promotion_gates": {
            "schema_order_and_package_budget": True,
            "all_four_no_signal_cases_candidate_f1": 1.0,
            "dense_cases_improved_over_base_min": 3,
            "mean_f1_regret_to_best_affordable_oracle_max": 0.05,
            "maximum_dense_case_regret_to_oracle_max": 0.15,
            "selected_event_cap_lte": MAX_EVENTS_PER_CASE,
            "actual_canary_total_tokens_lte": MAX_CANARY_TOTAL_TOKENS,
            "projected_full_router_total_tokens_lte": MAX_FULL_ROUTER_TOTAL_TOKENS,
            "actual_canary_extraction_plus_router_within_scope_budget": True,
        },
        "request_bytes": request_bytes,
        "frozen_inputs": {
            name: _record(path) for name, path in paths.items()
        }
        | {"oracle_audit": _record(oracle_path)},
        "predecessor": predecessor["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v206.__file__)),
            _record(Path(v192.__file__)),
            _record(Path(v188.__file__)),
            _record(Path(v186.__file__)),
        ],
        "privacy": "private_source_and_claims_sanitized_reports_counts_hashes_metrics_only",
    }
    design_path = root / "adaptive-router-design.json"
    _write_stable_time(design_path, design, "created_at")
    terminal = {
        "schema_version": V207_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v207_zero_extraction_adaptive_router_canary_authorized",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "semantic_attempt_authorized": True,
        "authorized_turn_count": 1,
        "authorized_model": MODEL,
        "authorized_effort": EFFORT,
        "extraction_model_calls_authorized": 0,
        "full_development_router_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "design": _record(design_path),
        "oracle_audit": _record(oracle_path),
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
            / "development-selection-v5_4-v208-adaptive-router-canary"
            / "terminal.json"
        ),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v207 adaptive-router design")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v207(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "semantic_attempt_authorized": terminal["semantic_attempt_authorized"],
                "holdout_authorized": terminal["holdout_authorized"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
