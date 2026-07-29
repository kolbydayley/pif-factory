from __future__ import annotations

"""Freeze the v225 quality failure as a zero-token nonacceptance receipt."""

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v219_feasibility_blocker as v219
from . import app_server_judge_v5_selection_v224_frozen_alignment_audit as v224
from . import app_server_judge_v5_selection_v225_alignment_transport_recovery as v225
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS
from .util import now_iso


V226_REPORT_VERSION = "pif_app_server_judge_v5_4_selection_v226_report_v1"
V226_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v226_spec_v1"
V226_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v226_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v226_alignment_nonacceptance"
BASELINE_END_TO_END_TOKENS = 10_065_426
TOKEN_RATIO_TARGET = 0.28
OBSERVED_TOKEN_RATIO = 0.164877
DEFAULT_OUTPUT_ROOT = (
    v225.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v226-alignment-nonacceptance"
).resolve()


class JudgeV5SelectionV226Error(RuntimeError):
    """The v225 nonacceptance cannot be frozen safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _v225_paths() -> dict[str, Path]:
    root = v225.DEFAULT_OUTPUT_ROOT
    base = root / "turns" / v225.TURN_NAMES[0].replace("_", "-")
    canary = root / "turns" / v225.TURN_NAMES[1].replace("_", "-")
    return {
        "attempt_spec": root / "attempt-spec.json",
        "capacity_audit": root / "capacity-policy-audit.json",
        "capacity_policy": root / "capacity-policy.json",
        "forensic_audit": root / "v224-presemantic-failure-audit.json",
        "launch": root / "launch-receipt.json",
        "runtime_lock": root / "runtime-lock.json",
        "score": root / "alignment-score.json",
        "terminal": root / "terminal.json",
        "base_capacity": base / "capacity.json",
        "base_sidecar": base / "sidecar.json",
        "base_output": base / "output.private.json",
        "base_normalized": base / "alignment-normalized.private.json",
        "canary_capacity": canary / "capacity.json",
        "canary_sidecar": canary / "sidecar.json",
        "canary_output": canary / "output.private.json",
        "canary_normalized": canary / "alignment-normalized.private.json",
    }


def _v219_paths() -> dict[str, Path]:
    root = v219.DEFAULT_OUTPUT_ROOT
    return {
        "attempt_spec": root / "attempt-spec.json",
        "audit": root / "full-coverage-feasibility.private.json",
        "report": root / "full-coverage-feasibility-report.json",
        "runtime_lock": root / "runtime-lock.json",
        "terminal": root / "terminal.json",
    }


def _validate_records(paths: Mapping[str, Path], label: str) -> None:
    actual = {
        path.resolve()
        for path in next(iter(paths.values())).parent.rglob("*")
        if path.is_file()
    }
    expected = {path.resolve() for path in paths.values()}
    if actual != expected:
        raise JudgeV5SelectionV226Error(f"{label} immutable file set drifted")
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV226Error(f"{label} immutable artifact drifted")


def _validate_v225_nonacceptance() -> dict[str, Any]:
    paths = _v225_paths()
    _validate_records(paths, "v225")
    v225.verify_runtime_lock(paths["runtime_lock"])
    terminal = _load_json(paths["terminal"], "v225 terminal")
    score = _load_json(paths["score"], "v225 score")
    base_usage = v225._validate_measured_turn(
        v225.DEFAULT_OUTPUT_ROOT, v225.TURN_NAMES[0]
    )
    canary_usage = v225._validate_measured_turn(
        v225.DEFAULT_OUTPUT_ROOT, v225.TURN_NAMES[1]
    )
    combined_usage = {
        field: int(base_usage[field]) + int(canary_usage[field])
        for field in USAGE_FIELDS
    }
    expected_failed = {
        "development_macro_noninferior_margin_0_03",
        "no_material_source_macro_regression",
        "permutation_projection_exact",
    }
    metrics = score.get("metrics") or {}
    if (
        terminal.get("state") != "development_strategy_not_accepted"
        or terminal.get("terminal_reason")
        != "v225_alignment_quality_or_permutation_gate_not_passed"
        or terminal.get("terminal_classification")
        != "inactive_incomplete_recovery_required"
        or terminal.get("development_quality_passed") is not False
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("overall_evaluation_complete") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != combined_usage
        or terminal.get("attempted_turn_count") != 2
        or terminal.get("measured_turn_count") != 2
        or terminal.get("unknown_usage_turn_count") != 0
        or terminal.get("semantic_retry_count") != 0
        or score.get("passed") is not False
        or set(score.get("failed_checks") or []) != expected_failed
        or metrics.get("development_supported_event_semantic_macro_f1")
        != 0.742424
        or metrics.get("baseline_reference_macro_f1") != 1.0
        or metrics.get("delta_vs_reference") != -0.257576
        or metrics.get("reference_semantic_unit_count") != 50
        or metrics.get("represented_reference_semantic_unit_count") != 16
        or metrics.get("permutation_exact_case_count") != 1
        or metrics.get("permutation_case_count") != 3
        or metrics.get("production_amortized_total_token_ratio")
        != OBSERVED_TOKEN_RATIO
    ):
        raise JudgeV5SelectionV226Error("v225 measured nonacceptance drifted")
    return {
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "score": score,
        "base": _load_json(paths["base_normalized"], "v225 base alignment"),
        "canary": _load_json(
            paths["canary_normalized"], "v225 canary alignment"
        ),
    }


def _validate_v219_strategy_space() -> dict[str, Any]:
    paths = _v219_paths()
    _validate_records(paths, "v219")
    report = _load_json(paths["report"], "v219 report")
    if (
        report.get("fixed_bundle_count") != 26
        or report.get("fixed_quality_pass_count_under_perfect_support_selector")
        != 0
        or report.get("fixed_path_feasible") is not False
        or report.get("adaptive_external_router_plus_selector_feasible")
        is not False
        or report.get("selector_tokens_over_adaptive_headroom") != 92218
        or report.get("holdout_authorized") is not False
        or report.get("production_mutated") is not False
    ):
        raise JudgeV5SelectionV226Error("v219 strategy-space evidence drifted")
    return {
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "report": report,
    }


def _represented_reference_witnesses(
    normalized: Mapping[str, Any], origin_by_id: Mapping[str, str]
) -> set[tuple[str, str]]:
    represented: set[tuple[str, str]] = set()
    for case in normalized.get("cases") or []:
        case_id = str(case["case_id"])
        for group in case.get("equivalence_groups") or []:
            origins = {origin_by_id[str(witness_id)] for witness_id in group}
            if origins != {"candidate", "reference"}:
                continue
            for witness_id in group:
                if origin_by_id[str(witness_id)] == "reference":
                    represented.add((case_id, str(witness_id)))
        for pair in case.get("alignment_pairs") or []:
            if pair.get("relation") != "partial":
                continue
            witness_ids = [str(value) for value in pair["witness_ids"]]
            if {origin_by_id[value] for value in witness_ids} != {
                "candidate",
                "reference",
            }:
                continue
            reference_id = next(
                value for value in witness_ids if origin_by_id[value] == "reference"
            )
            represented.add((case_id, reference_id))
    return represented


def _permutation_audit(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    mapping_path = v224.DEFAULT_OUTPUT_ROOT / "origin-map.private.json"
    mapping = _load_json(mapping_path, "v224 origin mapping")
    origin_by_id = {
        str(row["witness_id"]): str(row["origin"])
        for row in mapping.get("rows") or []
    }
    if len(origin_by_id) != v224.EXPECTED_WITNESS_COUNT:
        raise JudgeV5SelectionV226Error("v224 origin coverage drifted")
    base = _represented_reference_witnesses(predecessor["base"], origin_by_id)
    canary = _represented_reference_witnesses(
        predecessor["canary"], origin_by_id
    )
    union = base | canary
    intersection = base & canary
    if not (
        len(base) == 16
        and len(canary) == 14
        and len(union) == 16
        and len(intersection) == 14
        and canary < base
    ):
        raise JudgeV5SelectionV226Error(
            "v225 permutation representation evidence drifted"
        )
    return {
        "base_represented_reference_witness_count": len(base),
        "canary_represented_reference_witness_count": len(canary),
        "union_represented_reference_witness_count": len(union),
        "intersection_represented_reference_witness_count": len(intersection),
        "canary_is_strict_subset_of_base": True,
        "additional_reference_witnesses_hidden_by_permutation": 0,
        "mapping": _record(mapping_path),
    }


def freeze_v226(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v226 terminal")
    if any(root.iterdir()):
        raise JudgeV5SelectionV226Error(
            "v226 root is nonempty without a terminal"
        )
    predecessor = _validate_v225_nonacceptance()
    strategy_space = _validate_v219_strategy_space()
    permutation = _permutation_audit(predecessor)
    token_ceiling = round(BASELINE_END_TO_END_TOKENS * TOKEN_RATIO_TARGET)
    observed_tokens = round(BASELINE_END_TO_END_TOKENS * OBSERVED_TOKEN_RATIO)
    token_headroom = token_ceiling - observed_tokens
    report = {
        "schema_version": V226_REPORT_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "inactive_incomplete_recovery_required",
        "blocker_class": "measured_joint_quality_cost_strategy_exhaustion",
        "infrastructure_failure": False,
        "judge_protocol_failure": False,
        "development_quality_passed": False,
        "token_target_passed": True,
        "production_amortized_total_token_ratio": OBSERVED_TOKEN_RATIO,
        "production_amortized_token_ratio_target": TOKEN_RATIO_TARGET,
        "remaining_production_amortized_token_headroom_ratio": round(
            TOKEN_RATIO_TARGET - OBSERVED_TOKEN_RATIO, 6
        ),
        "remaining_production_amortized_token_headroom_tokens": token_headroom,
        "quality": predecessor["score"]["metrics"],
        "failed_quality_checks": predecessor["score"]["failed_checks"],
        "permutation_audit": permutation,
        "fixed_strategy_space": {
            "fixed_bundle_count": strategy_space["report"]["fixed_bundle_count"],
            "quality_passing_fixed_bundle_count": strategy_space["report"][
                "fixed_quality_pass_count_under_perfect_support_selector"
            ],
            "fixed_path_feasible": strategy_space["report"][
                "fixed_path_feasible"
            ],
            "adaptive_external_router_plus_selector_feasible": strategy_space[
                "report"
            ]["adaptive_external_router_plus_selector_feasible"],
        },
        "viable_systems_meeting_joint_gates": [],
        "unresolved_selection_decision": (
            "no evaluated system remains selectable; a new extraction-quality "
            "strategy must independently recover genuine omissions within the "
            "remaining token headroom"
        ),
        "more_current_strategy_development_cases_can_change_verdict": False,
        "additional_judge_only_turn_can_change_verdict": False,
        "shortest_path_to_holdout_verdict": (
            "freeze a new non-replay development extraction strategy with an "
            "independent LLM completeness pass bounded to 1158762 projected "
            "tokens, falsify it on the two observed dense cases, and open the "
            "untouched holdout only if the frozen quality gates pass"
        ),
        "cumulative_known_total_tokens": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ]["total_tokens"],
        "cumulative_unknown_usage_turn_count": predecessor["terminal"][
            "cumulative_unknown_usage_turn_count"
        ],
        "cumulative_unknown_usage_upper_bound_tokens": predecessor["terminal"][
            "cumulative_conservative_unknown_usage_upper_bound"
        ],
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "prospective_shadow_authorized": False,
        "production_mutated": False,
        "evaluation_acceptance_receipt_emitted": False,
        "predecessor": predecessor["records"],
        "strategy_space_evidence": strategy_space["records"],
        "privacy": (
            "sanitized_metrics_hashes_counts_and_blocker_class_no_source_or_"
            "event_text"
        ),
    }
    report_path = root / "development-nonacceptance-report.json"
    _write_stable_time(report_path, report, "created_at")
    spec = {
        "schema_version": V226_SPEC_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "zero_token_nonacceptance_receipt_completed",
        "semantic_model_calls_declared": 0,
        "semantic_model_calls_started": 0,
        "extraction_model_calls_started": 0,
        "judge_model_calls_started": 0,
        "retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v219.__file__)),
            _record(Path(v224.__file__)),
            _record(Path(v225.__file__)),
        ],
        "predecessor": predecessor["records"],
        "strategy_space_evidence": strategy_space["records"],
        "permutation_mapping": permutation["mapping"],
        "report": _record(report_path),
        "privacy": (
            "sanitized_metrics_hashes_counts_and_blocker_class_no_source_or_"
            "event_text"
        ),
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    terminal = {
        "schema_version": V226_TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "development_strategy_not_accepted",
        "terminal_reason": "v226_joint_quality_cost_strategy_not_accepted",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "evaluation_accepted": False,
        "development_quality_passed": False,
        "development_winner": None,
        "development_winner_frozen": False,
        "production_amortized_token_target_passed": True,
        "production_amortized_total_token_ratio": OBSERVED_TOKEN_RATIO,
        "viable_systems_meeting_joint_gates": [],
        "more_current_strategy_development_cases_can_change_verdict": False,
        "additional_judge_only_turn_authorized": False,
        "extraction_replay_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "prospective_shadow_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": {field: 0 for field in USAGE_FIELDS},
        "cumulative_known_usage_lower_bound": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": predecessor["terminal"][
            "cumulative_unknown_usage_turn_count"
        ],
        "cumulative_conservative_unknown_usage_upper_bound": predecessor[
            "terminal"
        ]["cumulative_conservative_unknown_usage_upper_bound"],
        "nonacceptance_report": _record(report_path),
        "spec": _record(spec_path),
        "required_next_artifact_path": str(
            root.parent
            / "development-selection-v5_4-v227-independent-completeness-strategy"
            / "terminal.json"
        ),
    }
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v226 nonacceptance")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v226(output_dir=Path(args.output_dir))
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
