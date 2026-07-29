from __future__ import annotations

"""Freeze a zero-extraction event selector over the measured v209 route."""

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from . import app_server_judge_v5_selection_v188_composite_postprocess as v188
from . import app_server_judge_v5_selection_v192_residual_repair_design as v192
from . import app_server_judge_v5_selection_v207_adaptive_router_design as v207
from . import app_server_judge_v5_selection_v210_adaptive_router_nonacceptance as v210
from .app_server_judge_v5 import compact_empty_event_fields
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_llm_judge import validate_app_server_output_schema_subset
from .util import now_iso, stable_id


V211_ORACLE_VERSION = "pif_app_server_judge_v5_4_selection_v211_oracle_v1"
V211_DESIGN_VERSION = "pif_app_server_judge_v5_4_selection_v211_design_v1"
V211_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v211_spec_v1"
V211_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v211_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v211_event_selector_design"
DEFAULT_OUTPUT_ROOT = (
    v210.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v211-event-selector-design"
).resolve()

MODEL = "gpt-5.6-sol"
EFFORT = "low"
BASE_ARM = "batch_5_new_thread"
COMPLETE_ARMS = ("batch_3_new_thread", BASE_ARM)
GAP_ARMS = ("batch_3_new_thread", BASE_ARM, "batch_8_new_thread")
CANARY_SELECTOR_TOTAL_TOKEN_GATE = 35_000
CANARY_SELECTOR_RUNTIME_TOKEN_BOUND = 45_000
MAX_EVENTS_PER_CASE = 32
TIMEOUT_SECONDS = 1200.0
MARKER_OPEN = "<<+"
MARKER_CLOSE = "<<-"
PROJECTED_FULL_ROUTER_TOTAL_TOKENS = 100_488


class JudgeV5SelectionV211Error(RuntimeError):
    """The event-selector diagnostic cannot be frozen safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v210_checkpoint() -> dict[str, Any]:
    root = v210.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "report": root / "development-nonacceptance-report.json",
        "next_strategy": root / "next-strategy.json",
        "spec": root / "nonacceptance-spec.json",
        "gate": root / "adaptive-router-canary-gate.json",
        "private_score": root / "adaptive-router-canary-score.private.json",
    }
    values = {name: _load_json(path, f"v210 {name}") for name, path in paths.items()}
    terminal = values["terminal"]
    report = values["report"]
    next_strategy = values["next_strategy"]
    if (
        terminal.get("state") != "waiting_for_external_strategy_authorization"
        or terminal.get("terminal_reason")
        != "development_quality_and_token_targets_not_met_holdout_closed"
        or terminal.get("terminal_classification")
        != "inactive_incomplete_recovery_required"
        or terminal.get("overall_evaluation_complete") is not False
        or terminal.get("semantic_quality_passed") is not False
        or terminal.get("production_amortized_token_target_passed") is not False
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("cumulative_known_usage_lower_bound", {}).get("total_tokens")
        != 8_931_442
        or terminal.get("cumulative_unknown_usage_turn_count") != 3
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound")
        != 245_000
        or terminal.get("report") != _record(paths["report"])
        or terminal.get("next_strategy") != _record(paths["next_strategy"])
        or terminal.get("spec") != _record(paths["spec"])
        or report.get("blocker_class")
        != "measured_zero_extraction_router_quality_and_token_shortfall"
        or report.get("more_development_cases_can_change_current_strategy_verdict")
        is not False
        or next_strategy.get("state") != "prepared_not_authorized"
        or next_strategy.get("holdout_remains_closed") is not True
    ):
        raise JudgeV5SelectionV211Error("v210 checkpoint drifted")
    for path in paths.values():
        if not _verify_record(_record(path)):
            raise JudgeV5SelectionV211Error("v210 artifact binding drifted")
    predecessor = v210._validate_v209_measured_output_failure()
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "report": report,
        "next_strategy": next_strategy,
        "gate": values["gate"],
        "private_score": values["private_score"],
        "v209": predecessor,
    }


def _bundle_for_coverage(coverage: str) -> tuple[str, ...]:
    if coverage == "unsupported_or_no_signal":
        return ()
    if coverage == "complete":
        return COMPLETE_ARMS
    if coverage == "material_gaps":
        return GAP_ARMS
    raise JudgeV5SelectionV211Error("v209 coverage class is not routable")


def _signal_rows(predecessor: Mapping[str, Any]) -> list[dict[str, Any]]:
    score_rows = {
        str(row["case_id"]): row
        for row in predecessor["private_score"]["cases"]
    }
    rows = []
    for route in predecessor["v209"]["output"]["cases"]:
        case_id = str(route["case_id"])
        scored = score_rows[case_id]
        rows.append(
            {
                "case_id": case_id,
                "segment_id": str(scored["segment_id"]),
                "source_id": str(scored["source_id"]),
                "density_stratum": str(scored["density_stratum"]),
                "coverage": str(route["base_coverage"]),
                "arms": list(_bundle_for_coverage(str(route["base_coverage"]))),
                "base_f1": float(scored["base_f1"]),
                "affordable_oracle_f1": float(scored["oracle_f1"]),
            }
        )
    expected = predecessor["v209"]["predecessor"]["design"]["canary_case_ids"]
    if (
        [row["case_id"] for row in rows] != expected
        or len(rows) != 8
        or sum(not row["arms"] for row in rows) != 4
        or sum(row["coverage"] == "material_gaps" for row in rows) != 2
        or sum(row["coverage"] == "complete" for row in rows) != 2
    ):
        raise JudgeV5SelectionV211Error("v211 route-policy coverage drifted")
    return rows


def _candidate_witnesses(
    *, mapping_case: Mapping[str, Any], arms: Sequence[str]
) -> list[dict[str, Any]]:
    component_ids = {f"arm:{arm}:normalized" for arm in arms}
    selected = []
    for witness in mapping_case.get("witnesses") or []:
        memberships = witness.get("provenance", {}).get("memberships") or []
        if any(row.get("system_id") in component_ids for row in memberships):
            selected.append(witness)
    return sorted(
        selected,
        key=lambda row: stable_id(
            PHASE_ID,
            str(mapping_case["case_key"]),
            str(row["provenance"]["canonical_event_sha256"]),
            prefix="rank_",
        ),
    )


def _all_exact_occurrences(source: str, evidence: str) -> list[tuple[int, int]]:
    if not evidence:
        raise JudgeV5SelectionV211Error("candidate evidence is empty")
    positions = []
    cursor = 0
    while True:
        start = source.find(evidence, cursor)
        if start < 0:
            break
        positions.append((start, start + len(evidence)))
        cursor = start + max(1, len(evidence))
    if not positions:
        raise JudgeV5SelectionV211Error("candidate evidence is not exact source text")
    return positions


def _annotate_source(
    source: str, events: Sequence[Mapping[str, Any]]
) -> str:
    if MARKER_OPEN in source or MARKER_CLOSE in source:
        raise JudgeV5SelectionV211Error("source collides with selector markers")
    starts: dict[int, list[str]] = defaultdict(list)
    ends: dict[int, list[str]] = defaultdict(list)
    for event in events:
        event_id = str(event["event_id"])
        for start, end in _all_exact_occurrences(source, str(event["evidence"])):
            starts[start].append(event_id)
            ends[end].append(event_id)
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
    return "".join(parts)


def strip_selector_markers(value: str) -> str:
    """Remove only validated selector evidence markers."""

    output = []
    cursor = 0
    while cursor < len(value):
        if value.startswith(MARKER_OPEN, cursor) or value.startswith(
            MARKER_CLOSE, cursor
        ):
            end = value.find(">>", cursor)
            if end < 0:
                raise JudgeV5SelectionV211Error("unterminated selector marker")
            payload = value[cursor + 3 : end]
            ids = payload.split(",") if payload else []
            if not ids or any(
                len(item) != 4 or item[0] != "e" or not item[1:].isdigit()
                for item in ids
            ):
                raise JudgeV5SelectionV211Error("malformed selector marker")
            cursor = end + 2
            continue
        output.append(value[cursor])
        cursor += 1
    return "".join(output)


def _selector_packet(
    *, rows: Sequence[Mapping[str, Any]], sources: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    mapping_by_segment = {
        str(row["case_key"]): row for row in sources["mapping"]["cases"]
    }
    pool_by_id = {str(row["case_id"]): row for row in sources["pool"]["cases"]}
    prepared = []
    field_names = set()
    private_cases = []
    for row in rows:
        if not row["arms"]:
            continue
        mapping_case = mapping_by_segment[str(row["segment_id"])]
        source = str(pool_by_id[str(row["case_id"])]["source_excerpt"])
        witnesses = _candidate_witnesses(
            mapping_case=mapping_case, arms=list(row["arms"])
        )
        events = []
        provenance = []
        for index, witness in enumerate(witnesses, start=1):
            event_id = f"e{index:03d}"
            event = compact_empty_event_fields(
                witness["provenance"]["original_event"]
            )
            evidence = str(event.pop("evidence"))
            event.pop("evidence_start", None)
            event.pop("evidence_end", None)
            field_names.update(event)
            events.append(
                {"event_id": event_id, "evidence": evidence, "fields": event}
            )
            provenance.append(
                {
                    "event_id": event_id,
                    "witness_id": str(witness["witness_id"]),
                    "canonical_event_sha256": str(
                        witness["provenance"]["canonical_event_sha256"]
                    ),
                }
            )
        if not events or len(events) > MAX_EVENTS_PER_CASE:
            raise JudgeV5SelectionV211Error("selector candidate event cap drifted")
        prepared.append((row, source, events))
        private_cases.append(
            {
                **dict(row),
                "event_count": len(events),
                "events": provenance,
            }
        )
    aliases = {
        name: f"f{index:02d}" for index, name in enumerate(sorted(field_names), start=1)
    }
    model_cases = []
    for row, source, events in prepared:
        projected = [
            {
                "event_id": event["event_id"],
                "values": {
                    aliases[name]: value for name, value in event["fields"].items()
                },
            }
            for event in events
        ]
        annotated = _annotate_source(source, events)
        if strip_selector_markers(annotated) != source:
            raise JudgeV5SelectionV211Error("selector markers changed source text")
        model_cases.append(
            {
                "case_id": str(row["case_id"]),
                "annotated_source": annotated,
                "candidate_events": projected,
            }
        )
    packet = {
        "evidence_marker_contract": (
            "<<+event_id>> opens and <<-event_id>> closes exact evidence; removing "
            "markers recovers the complete source exactly"
        ),
        "event_field_legend": {alias: name for name, alias in sorted(aliases.items())},
        "cases": model_cases,
    }
    provenance = {
        "schema_version": "pif_app_server_event_selector_provenance_private_v1",
        "cases": private_cases,
        "evidence_projection": (
            "nonempty evidence text and offsets are represented losslessly by markers in "
            "the complete source; every other nonempty event field is serialized"
        ),
    }
    return packet, provenance


def selector_instructions() -> str:
    return """
You are a side-free source-grounded event selector. You receive complete source text and opaque
candidate events from hidden extraction passes. You do not know system identity, reference labels,
scores, or density. The source is unchanged except for exact-evidence markers. A marker proves only
the literal evidence boundary; it does not prove that every material event field is supported.

For every candidate event, evaluate every nonempty material field against the source. Supported
paraphrase and coreference pass. Exact wording alone does not make an unsupported inference pass.
Use verdict keep only when every material proposition is source-supported and the event is not
truth-conditionally equivalent to an earlier kept event in that case. Use duplicate only when it
is supported but expresses the same event as an earlier kept event; point canonical_event_id to
that earlier kept ID. Keep multi-lens events separate when actor, attribution, causal mechanism,
certainty, boundary, type, metric, negation, reported actor, speaker, stance, target, or temporal
horizon is materially different. Use unsupported for a material conflict or ungrounded assertion.
Use abstain only when the source cannot resolve support or equivalence. Do not rewrite, add, merge,
or infer an event. Preserve case order and event order, classify every event exactly once, keep at
most 32 events per case, and output only the required structured object. Do not emit rationale or
confidence.
""".strip()


def _selector_prompt(packet: Mapping[str, Any]) -> str:
    return "# Source-grounded event selection packet\n" + json.dumps(
        packet, ensure_ascii=True, separators=(",", ":")
    )


def _selector_schema(packet: Mapping[str, Any]) -> dict[str, Any]:
    case_ids = [str(row["case_id"]) for row in packet["cases"]]
    event_ids = sorted(
        {
            str(event["event_id"])
            for row in packet["cases"]
            for event in row["candidate_events"]
        }
    )
    return {
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
                    "required": ["case_id", "decisions"],
                    "properties": {
                        "case_id": {"type": "string", "enum": case_ids},
                        "decisions": {
                            "type": "array",
                            "maxItems": MAX_EVENTS_PER_CASE,
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": [
                                    "event_id",
                                    "verdict",
                                    "canonical_event_id",
                                ],
                                "properties": {
                                    "event_id": {
                                        "type": "string",
                                        "enum": event_ids,
                                    },
                                    "verdict": {
                                        "type": "string",
                                        "enum": [
                                            "keep",
                                            "duplicate",
                                            "unsupported",
                                            "abstain",
                                        ],
                                    },
                                    "canonical_event_id": {"type": "string"},
                                },
                            },
                        },
                    },
                },
            }
        },
    }


def _arm_costs(sources: Mapping[str, Any]) -> dict[str, float]:
    return {
        arm: int(report["usage"]["total_tokens"]) / 32
        for arm, report in v186._arm_reports(sources).items()
    }


def _oracle_audit(
    *, rows: Sequence[Mapping[str, Any]], sources: Mapping[str, Any]
) -> dict[str, Any]:
    augmented, score, _combinations = v192._all_composite_score()
    costs = _arm_costs(sources)
    extraction_cost = len(rows) * costs[BASE_ARM]
    private_rows = []
    for row in rows:
        arms = tuple(row["arms"])
        extraction_cost += sum(costs[arm] for arm in arms if arm != BASE_ARM)
        if not arms:
            f1 = 1.0
            event_count = 0
            supported_units = 0
        else:
            system_id = (
                f"arm:{arms[0]}:normalized"
                if len(arms) == 1
                else v188._composite_system_id(arms)
            )
            candidate = next(
                item
                for item in score["systems"][system_id]["cases"]
                if item["segment_id"] == row["segment_id"]
            )
            f1 = v192._support_filter_f1(candidate)
            supported_units = int(candidate["supported_units"])
            event_count = int(
                augmented["membership"]["system_cases"][system_id][
                    row["segment_id"]
                ]["submitted_event_count"]
            )
        private_rows.append(
            {
                **dict(row),
                "perfect_selector_f1": f1,
                "regret_to_affordable_oracle": row["affordable_oracle_f1"] - f1,
                "candidate_event_count": event_count,
                "supported_unit_count": supported_units,
            }
        )
    dense = [row for row in private_rows if row["density_stratum"] != "no_signal"]
    selector_mean = sum(row["perfect_selector_f1"] for row in private_rows) / len(
        private_rows
    )
    oracle_mean = sum(row["affordable_oracle_f1"] for row in private_rows) / len(
        private_rows
    )
    joint = (
        extraction_cost
        + v210.EXPECTED_USAGE["total_tokens"]
        + CANARY_SELECTOR_TOTAL_TOKEN_GATE
    )
    scope_budget = float(
        v210._validate_v209_measured_output_failure()["predecessor"]["oracle"][
            "canary"
        ]["extraction_and_router_budget_tokens"]
    )
    checks = {
        "all_no_signal_cases_empty_and_f1_1": all(
            row["perfect_selector_f1"] == 1.0 and row["candidate_event_count"] == 0
            for row in private_rows
            if row["density_stratum"] == "no_signal"
        ),
        "dense_cases_improved_over_base_min_3": sum(
            row["perfect_selector_f1"] > row["base_f1"] for row in dense
        )
        >= 3,
        "mean_f1_regret_lte_0_05": oracle_mean - selector_mean <= 0.05 + 1e-12,
        "maximum_dense_case_regret_lte_0_15": max(
            row["regret_to_affordable_oracle"] for row in dense
        )
        <= 0.15 + 1e-12,
        "candidate_event_cap_lte_32": max(
            row["candidate_event_count"] for row in private_rows
        )
        <= MAX_EVENTS_PER_CASE,
        "canary_extraction_router_and_selector_within_scope_budget": joint
        <= scope_budget,
    }
    if not all(checks.values()):
        raise JudgeV5SelectionV211Error("event-selector canary ceiling does not pass")
    return {
        "schema_version": V211_ORACLE_VERSION,
        "purpose": "development_ceiling_only_not_model_input",
        "reference_oracle_used_for_scoring_only": True,
        "model_never_receives_reference_scores_density_or_arm_identity": True,
        "semantic_selection_decisions_must_come_from_llm": True,
        "case_count": len(private_rows),
        "signal_case_count": len(dense),
        "candidate_event_count": sum(
            row["candidate_event_count"] for row in private_rows
        ),
        "maximum_candidate_event_count": max(
            row["candidate_event_count"] for row in private_rows
        ),
        "perfect_selector_mean_f1": round(selector_mean, 6),
        "best_affordable_oracle_mean_f1": round(oracle_mean, 6),
        "mean_f1_regret_to_oracle": round(oracle_mean - selector_mean, 6),
        "maximum_dense_case_regret_to_oracle": round(
            max(row["regret_to_affordable_oracle"] for row in dense), 6
        ),
        "dense_improvement_count": sum(
            row["perfect_selector_f1"] > row["base_f1"] for row in dense
        ),
        "maximum_selected_supported_units": max(
            row["supported_unit_count"] for row in private_rows
        ),
        "selected_extraction_cost_tokens": round(extraction_cost, 6),
        "measured_v209_router_tokens": v210.EXPECTED_USAGE["total_tokens"],
        "declared_selector_token_gate": CANARY_SELECTOR_TOTAL_TOKEN_GATE,
        "joint_canary_token_bound": round(joint, 6),
        "scope_budget_tokens": round(scope_budget, 6),
        "checks": checks,
        "private_case_rows": private_rows,
    }


def freeze_v211(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v211 terminal")
    if any(root.iterdir()):
        raise JudgeV5SelectionV211Error("v211 root is nonempty without a terminal")

    predecessor = _validate_v210_checkpoint()
    rows = _signal_rows(predecessor)
    sources = v192._all_composite_score()[0]
    packet, provenance = _selector_packet(rows=rows, sources=sources)
    instructions = selector_instructions()
    prompt = _selector_prompt(packet)
    schema = _selector_schema(packet)
    unsupported = validate_app_server_output_schema_subset(schema)
    if unsupported:
        raise JudgeV5SelectionV211Error(
            "selector schema is unsupported: " + ";".join(unsupported)
        )
    oracle = _oracle_audit(rows=rows, sources=sources)

    paths = {
        "input": root / "selector-input.private.json",
        "provenance": root / "selector-provenance.private.json",
        "prompt": root / "selector-prompt.private.md",
        "schema": root / "selector-schema.json",
        "instructions": root / "selector-instructions.private.md",
        "oracle": root / "event-selector-oracle-audit.json",
    }
    _write_immutable(paths["input"], packet)
    _write_immutable(paths["provenance"], provenance)
    paths["prompt"].write_text(prompt, encoding="utf-8")
    _write_immutable(paths["schema"], schema)
    paths["instructions"].write_text(instructions, encoding="utf-8")
    _write_immutable(paths["oracle"], oracle)
    frozen_inputs = {name: _record(path) for name, path in paths.items()}

    design = {
        "schema_version": V211_DESIGN_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "strategy": "v209_coverage_routing_then_source_grounded_event_id_selection",
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "declared_turn_count": 1,
        "selector_runtime_total_token_bound": CANARY_SELECTOR_RUNTIME_TOKEN_BOUND,
        "selector_promotion_total_token_gate": CANARY_SELECTOR_TOTAL_TOKEN_GATE,
        "coverage_policy": {
            "unsupported_or_no_signal": [],
            "complete": list(COMPLETE_ARMS),
            "material_gaps": list(GAP_ARMS),
            "base_arm_always_charged": BASE_ARM,
        },
        "semantic_output_scope": (
            "opaque_existing_event_id_verdicts_only_no_event_generation_or_rewrite"
        ),
        "all_nonempty_event_fields_serialized": True,
        "exact_evidence_projected_losslessly_into_complete_source": True,
        "event_field_names_dictionary_encoded_nonsemantically": True,
        "router_reference_scores_density_and_arm_identity_absent_from_model_prompt": True,
        "extraction_model_calls_authorized": 0,
        "extraction_replay_allowed": False,
        "batch_5_replay_allowed": False,
        "retry_count_per_turn": 0,
        "canary_case_count": 8,
        "selector_case_count": len(packet["cases"]),
        "candidate_event_count": oracle["candidate_event_count"],
        "request_bytes": len(prompt.encode("utf-8"))
        + len(json.dumps(schema, ensure_ascii=True).encode("utf-8")),
        "promotion_gates": {
            "schema_order_coverage_and_canonical_links": True,
            "all_four_no_signal_cases_candidate_f1_1": True,
            "dense_cases_improved_over_base_min": 3,
            "mean_f1_regret_to_oracle_max": 0.05,
            "maximum_dense_case_regret_to_oracle_max": 0.15,
            "selected_event_cap_max": MAX_EVENTS_PER_CASE,
            "actual_selector_total_tokens_max": CANARY_SELECTOR_TOTAL_TOKEN_GATE,
            "actual_extraction_router_and_selector_within_scope_budget": True,
        },
        "full_router_authorized_only_after_canary_pass": False,
        "full_selector_authorized_only_after_measured_full_route_budget": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "frozen_inputs": frozen_inputs,
        "predecessor": predecessor["records"],
    }
    design_path = root / "event-selector-design.json"
    _write_stable_time(design_path, design, "created_at")
    spec = {
        "schema_version": V211_SPEC_VERSION,
        "state": "zero_model_call_selector_design_completed",
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "v209_completed_router_output_reused_without_replay": True,
        "v210_artifacts_mutated": False,
        "semantic_model_calls_started": 0,
        "extraction_model_calls_started": 0,
        "new_usage_tokens": 0,
        "frozen_design": _record(design_path),
        "oracle": _record(paths["oracle"]),
        "production_mutation_allowed": False,
    }
    spec_path = root / "event-selector-design-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    terminal = {
        "schema_version": V211_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v211_zero_extraction_event_selector_canary_authorized",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "semantic_attempt_authorized": True,
        "authorized_turn_count": 1,
        "authorized_model": MODEL,
        "authorized_effort": EFFORT,
        "extraction_model_calls_authorized": 0,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "design": _record(design_path),
        "oracle": _record(paths["oracle"]),
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
            / "development-selection-v5_4-v212-event-selector-canary"
            / "terminal.json"
        ),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v211 event-selector design")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v211(output_dir=Path(args.output_dir))
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
