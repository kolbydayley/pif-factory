from __future__ import annotations

"""Freeze the post-v205 convergence decision and its smallest next experiment."""

import argparse
import json
import math
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v205_fresh_exhaustive_recovery as v205
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .util import now_iso, stable_id


V206_REPORT_VERSION = "pif_app_server_judge_v5_4_selection_v206_convergence_report_v1"
V206_EXPERIMENT_VERSION = "pif_app_server_judge_v5_4_selection_v206_next_experiment_v1"
V206_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v206_spec_v1"
V206_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v206_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v206_convergence_blocker"
DEFAULT_OUTPUT_ROOT = (
    v205.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v206-convergence-blocker"
).resolve()
V201_ROOT = (
    v205.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v201-residual-repair-score"
).resolve()
V192_ROOT = (
    v205.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v192-residual-repair-design"
).resolve()
CANARY_MODEL = "gpt-5.6-sol"
CANARY_EFFORT = "high"
CANARY_TURN_TOKEN_BOUND = 44_000
CANARY_TURN_COUNT = 4
CANARY_SEGMENTS_PER_EPISODE = 2
FOLLOWUP_TURN_TOKEN_BOUND = 55_000
FOLLOWUP_TURN_COUNT = 4


class JudgeV5SelectionV206Error(RuntimeError):
    """The convergence blocker cannot bind the measured v205 result."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v205_quality_failure() -> dict[str, Any]:
    root = v205.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "gate": root / "structural-gate.json",
        "report": root / "arm" / "report.json",
        "launch_receipt": root / "launch-receipt.json",
        "runtime_lock": root / "runtime-lock.json",
        "spec": root / "attempt-spec.json",
        "capacity_policy": root / "capacity-policy.json",
        "capacity_audit": root / "capacity-policy-audit.json",
        "private_mapping": root / "arm" / "private-mapping.json",
    }
    values = {name: _load_json(path, f"v205 {name}") for name, path in paths.items()}
    terminal = values["terminal"]
    gate = values["gate"]
    report = values["report"]
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason")
        != "v205_extraction_structural_quality_gate_not_passed"
        or terminal.get("terminal_classification")
        != "inactive_incomplete_recovery_required"
        or terminal.get("fresh_judge_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 192062
        or terminal.get("production_amortized_total_token_ratio") != 0.131219
        or terminal.get("production_amortized_token_target_passed") is not True
        or terminal.get("structural_gate") != _record(paths["gate"])
        or terminal.get("arm_report") != _record(paths["report"])
        or gate.get("passed") is not False
        or gate.get("dense_median_candidate_to_reference_event_count_ratio") != 0.445
        or gate.get("failed_checks")
        != [
            "dense_median_event_count_ratio_gte_0_75",
            "metric_grounding_error_events_0",
            "no_signal_candidate_positive_segments_0",
        ]
        or gate.get("fresh_support_alignment_judge_authorized") is not False
        or report.get("requested_segments") != 16
        or report.get("validated_segments") != 16
        or report.get("attempted_calls") != 4
        or report.get("usage_measured_attempts") != 4
        or report.get("usage_unknown_attempts") != 0
        or report.get("accounting_complete") is not True
        or report.get("golden_events") != 255
        or report.get("candidate_events") != 110
        or report.get("normalized_exact_evidence_rate") != 1.0
        or report.get("metric_grounding_error_events") != 1
        or report.get("no_signal_candidate_positive_segments_unadjudicated") != 2
    ):
        raise JudgeV5SelectionV206Error("v205 quality-failure contract drifted")
    for field in USAGE_FIELDS:
        if terminal.get("usage", {}).get(field) != report.get("usage", {}).get(field):
            raise JudgeV5SelectionV206Error("v205 usage records disagree")
    sidecar_paths = sorted((root / "arm" / "sidecars").glob("*.json"))
    capacity_paths = sorted(root.rglob("capacity.json"))
    normalized_paths = sorted((root / "arm" / "normalized_outputs").glob("*.json"))
    if len(sidecar_paths) != 4 or len(capacity_paths) != 4 or len(normalized_paths) != 4:
        raise JudgeV5SelectionV206Error("v205 turn artifact coverage drifted")
    sidecars = []
    for path in sidecar_paths:
        sidecar = _load_json(path, "v205 sidecar")
        _validate_usage(sidecar)
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("usage_status") != "measured"
            or sidecar.get("usage_complete") is not True
            or sidecar.get("auth_type") != "chatgpt"
            or sidecar.get("plan_type") != "pro"
        ):
            raise JudgeV5SelectionV206Error("v205 sidecar contract drifted")
        sidecars.append(sidecar)
    if sum(_validate_usage(item)["total_tokens"] for item in sidecars) != 192062:
        raise JudgeV5SelectionV206Error("v205 sidecar usage sum drifted")
    for path in capacity_paths:
        checkpoint = _load_json(path, "v205 capacity checkpoint")
        if (
            checkpoint.get("cleared_for_semantic_turn") is not True
            or checkpoint.get("managed_chatgpt_auth_verified") is not True
            or checkpoint.get("rate_limit_reached_type") is not None
        ):
            raise JudgeV5SelectionV206Error("v205 capacity checkpoint drifted")
    predecessor = v205._validate_v204_presemantic_failure()
    v205.verify_runtime_lock(paths["runtime_lock"], predecessor=predecessor)
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "sidecar_records": [_record(path) for path in sidecar_paths],
        "capacity_records": [_record(path) for path in capacity_paths],
        "normalized_records": [_record(path) for path in normalized_paths],
        "predecessor": predecessor,
        **values,
    }


def _validate_historical_frontier() -> dict[str, Any]:
    paths = {
        "v201_terminal": V201_ROOT / "terminal.json",
        "v201_report": V201_ROOT / "residual-repair-score-report.json",
        "v192_terminal": V192_ROOT / "terminal.json",
        "v192_oracle": V192_ROOT / "residual-repair-oracle-audit.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t201 = values["v201_terminal"]
    r201 = values["v201_report"]
    t192 = values["v192_terminal"]
    oracle = values["v192_oracle"]
    bootstrap = r201.get("semantic_gate", {}).get("bootstrap", {})
    if (
        t201.get("state") != "inactive"
        or t201.get("terminal_reason")
        != "development_residual_repair_quality_gate_not_passed_conflict_recovery_cannot_change_verdict"
        or t201.get("production_amortized_total_token_ratio") != 0.236727
        or t201.get("production_amortized_token_target_passed") is not True
        or t201.get("holdout_authorized") is not False
        or t201.get("production_mutated") is not False
        or r201.get("promotion_passed") is not False
        or r201.get("more_development_cases_can_change_selection") is not False
        or r201.get("viable_systems") != []
        or bootstrap.get("baseline_f1") != 0.725098
        or bootstrap.get("candidate_f1") != 0.461543
        or bootstrap.get("candidate_minus_baseline") != -0.263555
        or bootstrap.get("ci_upper") != -0.14031
        or t192.get("state") != "completed"
        or t192.get("terminal_reason")
        != "v192_residual_repair_diagnostic_frozen_semantic_attempt_authorized"
        or oracle.get("purpose") != "development_ceiling_only_not_a_selectable_system"
        or oracle.get("reference_oracle_used_for_ceiling_only") is not True
        or oracle.get("production_semantic_routing_by_reference_forbidden") is not True
        or oracle.get("minimum_passing_extraction_cost_upper_rounded_tokens") != 702800
        or oracle.get("bootstrap", {}).get("candidate_minus_baseline") != -0.008388
        or oracle.get("bootstrap", {}).get("ci_lower") != -0.026097
        or oracle.get("macro_source_delta") != -0.012982
        or oracle.get("worst_source_delta") != -0.030888
    ):
        raise JudgeV5SelectionV206Error("historical quality-cost frontier drifted")
    return {"values": values, "records": {name: _record(path) for name, path in paths.items()}}


def _candidate_counts(predecessor: Mapping[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in predecessor["normalized_records"]:
        value = _load_json(Path(record["path"]), "v205 normalized output")
        for row in value.get("segments") or []:
            segment_id = str(row.get("segment_id") or "")
            events = row.get("events")
            if not segment_id or segment_id in counts or not isinstance(events, list):
                raise JudgeV5SelectionV206Error("v205 normalized segment coverage drifted")
            counts[segment_id] = len(events)
    return counts


def _select_canary(predecessor: Mapping[str, Any]) -> list[dict[str, Any]]:
    manifest = predecessor["predecessor"]["v203"]["manifest"]
    counts = _candidate_counts(predecessor)
    selected = []
    for episode in manifest.get("episodes") or []:
        no_signal = [
            segment
            for segment in episode.get("segments") or []
            if segment.get("density_stratum") == "no_signal"
        ]
        dense = [
            segment
            for segment in episode.get("segments") or []
            if segment.get("density_stratum") == "dense"
        ]
        if len(no_signal) != 1 or len(dense) != 3:
            raise JudgeV5SelectionV206Error("v203 density layout drifted")
        chosen_dense = min(
            dense,
            key=lambda item: stable_id(
                PHASE_ID,
                str(episode["episode_id"]),
                str(item["segment_id"]),
                prefix="rank_",
            ),
        )
        for segment in (chosen_dense, no_signal[0]):
            selected.append(
                {
                    "episode_id": str(episode["episode_id"]),
                    "source_id": str(episode["source_id"]),
                    "segment_id": str(segment["segment_id"]),
                    "density_stratum": str(segment["density_stratum"]),
                    "reference_event_count": int(segment["golden_event_count"]),
                    "v205_candidate_event_count": counts[str(segment["segment_id"])],
                }
            )
    if (
        len(selected) != 8
        or len({row["episode_id"] for row in selected}) != 4
        or sum(row["density_stratum"] == "dense" for row in selected) != 4
        or sum(row["density_stratum"] == "no_signal" for row in selected) != 4
    ):
        raise JudgeV5SelectionV206Error("v206 canary selection drifted")
    return selected


def _cost_envelope() -> dict[str, Any]:
    baseline = v205.v204.v203.BASELINE_END_TO_END_TOKENS
    context = v205.v204.v203.PRODUCTION_AMORTIZED_CONTEXT_TOKENS
    segment_scope = v205.v204.v203.BASELINE_SEGMENT_SCOPE
    development_segments = v205.v204.v203.EPISODE_COUNT * v205.v204.v203.SEGMENTS_PER_EPISODE
    maximum_development_extraction_tokens = math.floor(
        ((0.28 * baseline) - context) * development_segments / segment_scope
    )
    v205_tokens = 192062
    canary_bound = CANARY_TURN_TOKEN_BOUND * CANARY_TURN_COUNT
    followup_bound = FOLLOWUP_TURN_TOKEN_BOUND * FOLLOWUP_TURN_COUNT
    combined = v205_tokens + canary_bound + followup_bound
    scaled = math.ceil(combined * segment_scope / development_segments)
    numerator = scaled + context
    ratio = round(numerator / baseline, 6)
    return {
        "maximum_development_extraction_tokens_at_0_28": maximum_development_extraction_tokens,
        "v205_measured_tokens": v205_tokens,
        "remaining_development_extraction_token_envelope": maximum_development_extraction_tokens
        - v205_tokens,
        "canary_token_bound": canary_bound,
        "conditional_followup_token_bound": followup_bound,
        "combined_development_extraction_token_bound": combined,
        "production_amortized_total_token_ratio_bound": ratio,
        "passed_lte_0_28": ratio <= 0.28,
    }


def freeze_v206(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v206 terminal")
    if any(root.iterdir()):
        raise JudgeV5SelectionV206Error("v206 root is nonempty without a terminal")
    predecessor = _validate_v205_quality_failure()
    historical = _validate_historical_frontier()
    canary = _select_canary(predecessor)
    cost = _cost_envelope()
    if cost["passed_lte_0_28"] is not True:
        raise JudgeV5SelectionV206Error("v206 next experiment exceeds token envelope")
    next_experiment = {
        "schema_version": V206_EXPERIMENT_VERSION,
        "state": "prepared_not_launched",
        "strategy": "production_symmetric_topic_neutral_omission_audit",
        "hypothesis": (
            "a second LLM pass that sees source windows plus the first-pass event inventory can "
            "return only materially distinct omitted events and close the measured recall gap"
        ),
        "model": CANARY_MODEL,
        "reasoning_effort": CANARY_EFFORT,
        "canary_case_count": len(canary),
        "canary_turn_count": CANARY_TURN_COUNT,
        "cases_per_episode": CANARY_SEGMENTS_PER_EPISODE,
        "selection": canary,
        "selection_policy": "one_hash_ranked_dense_and_the_single_no_signal_segment_per_fresh_episode",
        "semantic_regex_keyword_or_embedding_selection_used": False,
        "existing_events_are_context_not_truth": True,
        "llm_returns_only_additional_exact_evidence_events": True,
        "deterministic_merge_allowed_only_by_exact_identity_and_owner": True,
        "retry_count_per_turn": 0,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "canary_stop_rules": {
            "all_8_segments_validated_and_usage_measured": True,
            "normalized_exact_evidence_rate": 1.0,
            "metric_grounding_error_events": 0,
            "dense_median_union_to_reference_event_count_ratio_min": 0.75,
            "no_signal_one_sided_events_require_side_free_llm_support_adjudication": True,
            "production_amortized_total_token_ratio_bound_lte": 0.28,
        },
        "promotion_rule": (
            "only a passing canary may authorize the four-turn complementary remainder; "
            "fresh support/alignment scoring remains required before any winner or holdout"
        ),
        "cost_envelope": cost,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "privacy": "opaque_ids_counts_and_hashes_no_transcript_or_event_text",
    }
    experiment_path = root / "next-experiment.json"
    _write_immutable(experiment_path, next_experiment)
    report = {
        "schema_version": V206_REPORT_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "viable_systems_meeting_joint_gates": [],
        "systems": {
            "fresh_baseline": {
                "quality_reference": True,
                "production_amortized_token_ratio": 1.0,
                "joint_gate_passed": False,
            },
            "v201_frozen_candidate": {
                "quality_delta": -0.263555,
                "production_amortized_token_ratio": 0.236727,
                "joint_gate_passed": False,
            },
            "v205_sol_high_fresh": {
                "semantic_quality_scored": False,
                "dense_median_event_count_ratio": 0.445,
                "candidate_events": 110,
                "reference_events": 255,
                "production_amortized_token_ratio": 0.131219,
                "structural_gate_passed": False,
                "joint_gate_passed": False,
            },
            "v192_dynamic_oracle": {
                "development_reference_ceiling_only": True,
                "selectable_production_system": False,
                "quality_and_token_feasibility_demonstrated": True,
            },
        },
        "unresolved_selection_decision": (
            "whether one production-symmetric topic-neutral omission-audit pass can close "
            "the source-wide recall gap within the remaining token envelope"
        ),
        "can_more_cases_change_v205_verdict": False,
        "can_a_materially_new_extraction_strategy_change_joint_feasibility": True,
        "v205_failure_is_quality_not_transport": True,
        "one_sided_no_signal_events_are_unadjudicated_not_counted_as_false_positives": True,
        "cumulative_known_usage_lower_bound": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": predecessor["terminal"][
            "cumulative_unknown_usage_turn_count"
        ],
        "cumulative_conservative_unknown_usage_upper_bound": predecessor["terminal"][
            "cumulative_conservative_unknown_usage_upper_bound"
        ],
        "shortest_path_to_holdout_verdict": (
            "run the frozen 8-case omission canary; if and only if it passes, run the "
            "complementary 8 development cases, freeze support/alignment quality, then open one untouched holdout"
        ),
        "operator_convergence_timebox_reached": True,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    report_path = root / "convergence-report.json"
    _write_stable_time(report_path, report, "created_at")
    spec = {
        "schema_version": V206_SPEC_VERSION,
        "state": "zero_token_convergence_blocker_frozen",
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "semantic_model_calls_started": 0,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v205.__file__)),
        ],
        "v205_attempt": {
            **predecessor["records"],
            "sidecars": predecessor["sidecar_records"],
            "capacities": predecessor["capacity_records"],
            "normalized_outputs": predecessor["normalized_records"],
        },
        "historical_frontier": historical["records"],
        "next_experiment": _record(experiment_path),
        "report": _record(report_path),
    }
    spec_path = root / "convergence-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    terminal = {
        "schema_version": V206_TERMINAL_VERSION,
        "state": "waiting_for_next_semantic_strategy_authorization",
        "terminal_at": now_iso(),
        "terminal_reason": "development_convergence_timebox_reached_no_system_meets_joint_gates",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "development_winner_frozen": False,
        "semantic_quality_passed": False,
        "production_amortized_token_target_passed_by_v205": True,
        "viable_system_count": 0,
        "can_more_cases_change_v205_verdict": False,
        "materially_new_extraction_strategy_prepared": True,
        "next_semantic_attempt_started": False,
        "holdout_authorized": False,
        "prospective_shadow_authorized": False,
        "production_mutated": False,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": {field: 0 for field in USAGE_FIELDS},
        "cumulative_known_usage_lower_bound": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": predecessor["terminal"][
            "cumulative_unknown_usage_turn_count"
        ],
        "cumulative_conservative_unknown_usage_upper_bound": predecessor["terminal"][
            "cumulative_conservative_unknown_usage_upper_bound"
        ],
        "convergence_report": _record(report_path),
        "next_experiment": _record(experiment_path),
        "spec": _record(spec_path),
        "required_next_artifact_path": str(
            root.parent
            / "development-selection-v5_4-v207-omission-audit-canary"
            / "terminal.json"
        ),
    }
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v206 convergence blocker")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v206(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "viable_system_count": terminal["viable_system_count"],
                "materially_new_extraction_strategy_prepared": terminal[
                    "materially_new_extraction_strategy_prepared"
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
