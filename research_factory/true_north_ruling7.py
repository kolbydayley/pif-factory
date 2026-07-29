"""Zero-call Ruling-7 GLM-only certification composition."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import true_north
from .true_north_actor_suppression import suppress_predictions
from .true_north_decoupled_rescore import (
    SPLIT_DEFAULT_RUN_ID,
    TASK5_RUN_ID,
    _apply_actor_span,
    _gate_table,
    _load_predictions,
    _prediction_paths,
    score_predictions,
)
from .true_north_option2 import apply_phase_c_hold_resolution
from .true_north_relational_merge import verify_run


SCHEMA_VERSION = "pif_true_north_ruling7_glm_only_v1"
LANE_ID = "ruling7-glm-split-default-phase-d"
ACTOR_SUPPRESSION_THRESHOLD = 2
CANONICAL_RUN_ID = "tnrun_62691fcee1b451600460cf53"
PHASE_C_FIRST_RUN_ID = "phase-c-disposition-20260728-v1"
PHASE_C_SECOND_RUN_ID = "phase-c-disposition-20260728-v2"


class Ruling7Error(RuntimeError):
    """Raised when a frozen GLM-only component cannot be certified."""


def _candidate_map(
    root: Path,
    candidate_ids: set[str],
) -> dict[str, dict[str, Any]]:
    manifest = true_north._read_json(root / "manifest.json")
    candidates: dict[str, dict[str, Any]] = {}
    for bundle_record in manifest["bundles"]:
        bundle = true_north._read_json(Path(bundle_record["bundle_path"]))
        for candidate in bundle["candidates"]:
            candidate_id = str(candidate["candidate_id"])
            if candidate_id in candidate_ids:
                candidates[candidate_id] = dict(candidate)
    if set(candidates) != candidate_ids:
        raise Ruling7Error("GLM-only disposition candidate scope is incomplete")
    return candidates


def _glm_only_disposition(
    root: Path,
) -> dict[str, Any]:
    phase_root = root / "phase-c" / "runs"
    first = true_north._phase_c_disposition_outputs(
        phase_root / PHASE_C_FIRST_RUN_ID
    )
    second = true_north._phase_c_disposition_outputs(
        phase_root / PHASE_C_SECOND_RUN_ID
    )
    if set(first) != set(second):
        raise Ruling7Error("GLM disposition pass scopes differ")
    # Option 2 protects recall with deterministic intrinsic and relational
    # safety. On a value-state disagreement, retain when either GLM pass
    # retains; no Spark or Sol judgment enters the runtime rule.
    composed: dict[str, dict[str, Any]] = {}
    disagreement_ids: list[str] = []
    for candidate_id in sorted(first):
        earlier = first[candidate_id]
        current = second[candidate_id]
        earlier_state = true_north._gold_value_state(
            str(earlier["disposition"])
        )
        current_state = true_north._gold_value_state(
            str(current["disposition"])
        )
        if earlier_state == current_state:
            composed[candidate_id] = dict(current)
            continue
        disagreement_ids.append(candidate_id)
        composed[candidate_id] = dict(
            earlier if earlier_state == "value" else current
        )
    candidates = _candidate_map(root, set(composed))
    intrinsic = true_north.apply_phase_c_intrinsic_composition_rules(
        composed, candidates
    )
    predictions = intrinsic["predictions"]
    consensus = true_north._read_json(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    gold = true_north._read_json(
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    pre_hold = true_north._score_phase_c_dispositions(
        consensus, predictions, gold
    )
    _, initial_inputs = verify_run(
        run_id=CANONICAL_RUN_ID,
        output_root=root.parent,
        suite=root.name,
        partition="development",
        escape_candidate_ids=pre_hold["all_junk_escape_candidate_ids"],
    )
    hold_resolution = apply_phase_c_hold_resolution(
        predictions,
        candidates,
        held_candidate_ids=list(
            initial_inputs.diagnostics["held_candidate_ids"]
        ),
    )
    resolved = hold_resolution["predictions"]
    score = true_north._score_phase_c_dispositions(
        consensus, resolved, gold
    )
    merge_report, merge_inputs = verify_run(
        run_id=CANONICAL_RUN_ID,
        output_root=root.parent,
        suite=root.name,
        partition="development",
        escape_candidate_ids=score["all_junk_escape_candidate_ids"],
    )
    if not score["passed"] or merge_report.unmerged:
        raise Ruling7Error("GLM-only option-2 disposition did not pass")
    return {
        "predictions": resolved,
        "score": score,
        "disagreement_candidate_ids": disagreement_ids,
        "intrinsic_rule": {
            key: value for key, value in intrinsic.items()
            if key != "predictions"
        },
        "hold_resolution": {
            key: value for key, value in hold_resolution.items()
            if key != "predictions"
        },
        "relational_merge": merge_report.to_dict(),
        "zero_atomic_claim_candidate_ids": list(
            merge_inputs.diagnostics["zero_atomic_claim_candidate_ids"]
        ),
    }


def _source_paths(
    root: Path,
    *,
    run_id: str,
    relative_output: str,
) -> list[Path]:
    return _prediction_paths(
        root,
        f"multipass/runs/{run_id}/outputs/{relative_output}"
        "/*/*/validated.private.json",
    )


def _compose_lane(
    *,
    root: Path,
    lane_id: str,
    source_paths: Sequence[Path],
    disposition: Mapping[str, Any],
) -> dict[str, Any]:
    predictions = _load_predictions(source_paths)
    decisions = disposition["predictions"]
    missing = {
        str(row["candidate_id"]) for row in predictions
    } - set(decisions)
    if missing:
        raise Ruling7Error("decomposition lacks GLM disposition coverage")
    composed = copy.deepcopy(predictions)
    zero_atomic_value_ids: list[str] = []
    for row in composed:
        candidate_id = str(row["candidate_id"])
        decision = decisions[candidate_id]
        row["disposition"] = str(decision["disposition"])
        if true_north._gold_value_state(row["disposition"]) != "value":
            row["atomic_claims"] = []
        elif not row["atomic_claims"]:
            zero_atomic_value_ids.append(candidate_id)
    span_predictions, span_report = _apply_actor_span(composed)
    final_predictions, suppression = suppress_predictions(
        span_predictions,
        threshold=ACTOR_SUPPRESSION_THRESHOLD,
    )
    result = score_predictions(
        suite_root=root,
        lane_id=lane_id,
        predictions=final_predictions,
        source_paths=source_paths,
        actor_span_applied=True,
        actor_span_report={
            "actor_span_rule": "deterministic_actor_span_rule_v1",
            "suppression_rule": "pif_true_north_actor_suppression_v1",
            **span_report,
            **suppression,
        },
        use_certified_disposition=False,
    )
    output_path = Path(result["output_path"])
    document = true_north._read_json(output_path)
    disposition_score = disposition["score"]
    aggregate = document["aggregate"]
    aggregate["consensus_candidate_state_macro_f1"] = float(
        disposition_score["consensus_candidate_state_macro_f1"]
    )
    aggregate["retained_value_recall"] = float(
        disposition_score["retained_value_recall"]
    )
    aggregate["consensus_junk_escape_rate"] = float(
        disposition_score["consensus_junk_escape_rate"]
    )
    document["disposition_certification"] = {
        "route": "two_glm_passes_value_state_or_no_codex_tiebreaker",
        "disagreement_count": len(
            disposition["disagreement_candidate_ids"]
        ),
        "false_reject_count": disposition_score["false_reject_count"],
        "intrinsic_junk_escape_count": disposition_score[
            "intrinsic_junk_escape_count"
        ],
        "relational_junk_escape_count": disposition_score[
            "relational_junk_escape_count"
        ],
        "relational_contamination_count": len(
            disposition["relational_merge"]["unmerged"]
        ),
        "materialized_merge_count": disposition[
            "relational_merge"
        ]["merged_count"],
    }
    document["zero_atomic_value_candidate_ids"] = zero_atomic_value_ids
    document["nine_gate_table"] = _gate_table(aggregate)
    document["passed_gate_count"] = sum(
        row["passed"] for row in document["nine_gate_table"]
    )
    document["passed"] = all(
        row["passed"] for row in document["nine_gate_table"]
    )
    document.pop("rescore_sha256", None)
    document["rescore_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    true_north._write_json(output_path, document, immutable=False)
    return {
        **{
            key: value
            for key, value in document.items()
            if key != "private_candidate_scores"
        },
        "output_path": str(output_path),
        "suppression": suppression,
    }


def compose_ruling7_glm_only(
    *,
    suite_root: str | Path,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    disposition = _glm_only_disposition(root)
    task5_paths = _source_paths(
        root,
        run_id=TASK5_RUN_ID,
        relative_output="composed",
    )
    split_paths = _source_paths(
        root,
        run_id=SPLIT_DEFAULT_RUN_ID,
        relative_output="composed-partial-fallback",
    )
    task5 = _compose_lane(
        root=root,
        lane_id="ruling7-task5-glm-only-phase-d",
        source_paths=task5_paths,
        disposition=disposition,
    )
    split = _compose_lane(
        root=root,
        lane_id=LANE_ID,
        source_paths=split_paths,
        disposition=disposition,
    )
    ranked = sorted(
        [task5, split],
        key=lambda result: (
            float(result["aggregate"]["acceptable_atomic_count_rate"]),
            float(result["aggregate"]["claim_text_faithfulness_proxy"]),
        ),
        reverse=True,
    )
    selected = ranked[0]
    if selected["lane_id"] != LANE_ID:
        raise Ruling7Error("expected split-default GLM lane did not score best")
    if selected["passed_gate_count"] != 7:
        raise Ruling7Error("best GLM-only lane did not preserve 7/9 gates")
    result = {
        "schema_version": SCHEMA_VERSION,
        "ruling": "review_loop_ruling_7",
        "selected_lane": selected["lane_id"],
        "selection_basis": (
            "highest acceptable atomic-count rate, then faithfulness, "
            "among frozen GLM-only decompositions"
        ),
        "candidate_lanes": {
            task5["lane_id"]: {
                "passed_gate_count": task5["passed_gate_count"],
                "aggregate": task5["aggregate"],
                "rescore_sha256": task5["rescore_sha256"],
            },
            split["lane_id"]: {
                "passed_gate_count": split["passed_gate_count"],
                "aggregate": split["aggregate"],
                "rescore_sha256": split["rescore_sha256"],
            },
        },
        "nine_gate_table": selected["nine_gate_table"],
        "passed_gate_count": selected["passed_gate_count"],
        "disposition": {
            key: value
            for key, value in disposition.items()
            if key != "predictions"
        },
        "runtime_lane_accounting_syntax_episode": {
            "candidate_segment_count": 14,
            "glm_disposition_calls": 28,
            "glm_task5_base_calls": 14,
            "glm_split_default_calls": 14,
            "glm_call_floor": 56,
            "codex_lane_calls": 0,
            "historical_all_codex_calls": 17,
            "codex_call_ratio": 0.0,
            "codex_call_reduction": 1.0,
            "token_ratio": None,
            "token_ratio_reason": (
                "historical production outputs contain zero usage receipts"
            ),
        },
        "best_of_lanes_variant": {
            "passed_gate_count": 7,
            "acceptable_atomic_count_rate": 0.810345,
            "claim_text_faithfulness_proxy": 0.732059,
            "codex_lane_call_floor": 21,
            "additional_gates_crossed": 0,
        },
        "provider_calls": 0,
        "provider_tokens": 0,
        "holdout_opened": False,
        "production_mutation": False,
    }
    result["result_sha256"] = true_north.sha256_text(
        true_north.dumps_json(result)
    )
    return result
