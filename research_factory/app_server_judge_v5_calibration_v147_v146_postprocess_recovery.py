from __future__ import annotations

"""Zero-token recovery of the v146 postprocessing failure."""

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .util import now_iso


V147_SCORE_VERSION = "pif_app_server_judge_v5_4_v147_recovered_v146_score_v1"
V147_PLAN_VERSION = "pif_app_server_judge_v5_4_v147_support_projection_plan_v1"
V147_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v147_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    v146.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v147-v146-postprocess-recovery"
).resolve()


class JudgeV5CalibrationV147Error(RuntimeError):
    """The immutable v146 recovery contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v146_failure() -> dict[str, Any]:
    root = v146.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "singleton-reference-owner-spec.json",
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "truth": root / "singleton-reference-truth.private.json",
        "selection": root / "selection-audit.json",
        "rubric": root / "field-rubric-v146.json",
    }
    values = {name: _load_json(path, f"v146 {name}") for name, path in paths.items()}
    terminal, failure, spec = values["terminal"], values["failure"], values["spec"]
    expected_usage = {
        "input_tokens": 507924,
        "cached_input_tokens": 46080,
        "output_tokens": 9178,
        "reasoning_output_tokens": 6924,
        "total_tokens": 517102,
    }
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != expected_usage
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != "singleton_repeat_04"
        or failure.get("error_class") != "JudgeV5CalibrationV143Error"
        or failure.get("unknown_usage_turn_count") != 0
        or failure.get("usage") != expected_usage
        or failure.get("retry_allowed_in_this_version") is not False
        or spec.get("turn_plan") != list(v146.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV147Error("v146 failed-attempt contract drifted")
    if terminal.get("failure") != _record(paths["failure"]):
        raise JudgeV5CalibrationV147Error("v146 failure record drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV147Error("v146 runtime record drifted")

    outputs = {}
    attempts = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_name in spec["turn_plan"]:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        records = {}
        for name, filename in {
            "capacity": "capacity.json",
            "sidecar": "sidecar.json",
            "output": "output.private.json",
        }.items():
            path = turn_root / filename
            if not path.is_file():
                raise JudgeV5CalibrationV147Error("v146 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v146 sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        outputs[turn_name] = _load_json(
            Path(records["output"]["path"]), "v146 output"
        )
        attempts[turn_name] = records
    if usage != expected_usage:
        raise JudgeV5CalibrationV147Error("v146 measured usage drifted")

    predecessor = v146._validate_v145()
    _, truth, _, _ = v146.build_v146_inputs(predecessor)
    score = v146.score_v146(outputs=outputs, truth=truth)
    if (
        score.get("passed") is not False
        or score.get("failed_checks") != ["unstable_owner_repeat_exact_rate"]
        or score.get("metrics", {}).get("control_exact_count") != 5
        or score.get("metrics", {}).get("owner_abstention_count") != 0
        or score.get("metrics", {}).get("repeat_exact_count") != 3
        or score.get("metrics", {}).get("evidence_complete_count") != 24
    ):
        raise JudgeV5CalibrationV147Error("v146 latent quality result drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "outputs": outputs,
        "truth": truth,
        "score": score,
        "predecessor": predecessor,
    }


def build_v147_plan(source: Mapping[str, Any]) -> dict[str, Any]:
    truth = {row["task_id"]: row for row in source["truth"]["tasks"]}
    owner_outputs = {
        output["decisions"][0]["task_id"]: output["decisions"][0]
        for turn_name, output in source["outputs"].items()
        if turn_name in v146.OWNER_TURNS
    }
    repeat_outputs = {
        output["decisions"][0]["task_id"]: output["decisions"][0]
        for turn_name, output in source["outputs"].items()
        if turn_name in v146.REPEAT_TURNS
    }
    unstable = [
        truth[task_id]
        for task_id in repeat_outputs
        if owner_outputs[task_id]["field_status"]
        != repeat_outputs[task_id]["field_status"]
    ]
    if len(unstable) != 2 or {row["field"] for row in unstable} != {
        "unsupported_inference"
    }:
        raise JudgeV5CalibrationV147Error("v147 unstable-field isolation drifted")
    support_truth = {
        (row["case_id"], row["witness_id"]): row
        for row in source["predecessor"]["v144"]["v143"]["values"]["truth"]["support"]
    }
    units = []
    for row in sorted(unstable, key=lambda item: (item["case_id"], item["witness_id"])):
        key = (row["case_id"], row["witness_id"])
        inherited = support_truth.get(key)
        units.append(
            {
                "owner_task_id": row["task_id"],
                "source_v143_task_id": row["source_v143_task_id"],
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "pointwise_support_receipt_available": inherited is not None,
                "inherited_support_status": (
                    inherited["expected_status"] if inherited is not None else None
                ),
                "required_next_action": (
                    "reuse_frozen_v143_pointwise_support_receipt"
                    if inherited is not None
                    else "fresh_side_free_pointwise_support_primary_and_canary"
                ),
            }
        )
    if sum(row["pointwise_support_receipt_available"] for row in units) != 1:
        raise JudgeV5CalibrationV147Error("v147 support-receipt coverage drifted")
    return {
        "schema_version": V147_PLAN_VERSION,
        "created_at": now_iso(),
        "failed_v146_gate": "unstable_owner_repeat_exact_rate",
        "failed_repeat_count": 2,
        "failed_repeat_field_counts": {"unsupported_inference": 2},
        "settled_control_exact_count": 5,
        "settled_control_count": 5,
        "nonabstaining_owner_count": 14,
        "stable_non_unsupported_inference_repeat_count": 3,
        "pointwise_support_projection_rule": {
            "supported": "correct",
            "unsupported": "incorrect",
            "abstain": "abstain",
        },
        "projection_is_semantic": False,
        "projection_source_is_llm_pointwise_support": True,
        "missing_fresh_support_unit_count": 1,
        "fresh_turn_count": 2,
        "fresh_turn_roles": ["support_primary", "support_order_canary"],
        "retry_count_per_turn": 0,
        "units": units,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def run_v147(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v147 terminal")
    source = _validate_v146_failure()
    score = dict(source["score"])
    score["schema_version"] = V147_SCORE_VERSION
    plan = build_v147_plan(source)
    score_path = root / "recovered-v146-score.json"
    plan_path = root / "unsupported-inference-support-projection-plan.json"
    _write_immutable(score_path, score)
    _write_stable_time(plan_path, plan, "created_at")
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    terminal = {
        "schema_version": V147_TERMINAL_VERSION,
        "state": "inactive",
        "terminal_at": now_iso(),
        "terminal_reason": "inactive_incomplete_recovery_required",
        "development_terminal_reason": "v146_singleton_repeat_quality_gate_not_passed_postprocessing_recovered",
        "overall_evaluation_complete": False,
        "v146_infrastructure_terminal_preserved": True,
        "v146_semantic_turns_replayed": False,
        "recovered_quality_gate_passed": False,
        "support_projection_repair_authorized": True,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": zero_usage,
        "predecessor_v146_usage": source["usage"],
        "recovered_score": _record(score_path),
        "support_projection_plan": _record(plan_path),
        "predecessor_v146_terminal": source["records"]["terminal"],
        "predecessor_v146_failure": source["records"]["failure"],
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Recover v146 postprocessing without model calls")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = run_v147(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "support_projection_repair_authorized": terminal[
                    "support_projection_repair_authorized"
                ],
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
