from __future__ import annotations

"""Zero-model recovery of the fully measured v135 field-owner outputs."""

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v135_dominant_field_owner_diagnostic as v135
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import validate_output
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .util import now_iso


V136_SPEC_VERSION = "pif_app_server_judge_v5_4_v136_recovery_spec_v1"
V136_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v136_recovered_output_v1"
V136_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v136_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    v135.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v136-dominant-field-owner-recovery"
).resolve()


class JudgeV5CalibrationV136Error(RuntimeError):
    """The measured v135 outputs cannot be recovered without drift."""


def _validate_v135() -> dict[str, Any]:
    root = v135.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "dominant-field-owner-spec.json",
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "truth": root / "dominant-field-owner-truth.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v135 {name}") for name, path in paths.items()}
    spec, terminal, failure = values["spec"], values["terminal"], values["failure"]
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("expanded_field_owner_diagnostic_authorized") is not False
        or terminal.get("reference_patch_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 133874
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != "dominant_speaker_order_canary"
        or failure.get("error_class") != "KeyError"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("accounting_complete") is not True
        or failure.get("usage_status") != "complete"
        or failure.get("unknown_usage_turn_count") != 0
        or spec.get("model") != "gpt-5.6-sol"
        or spec.get("turn_plan") != list(v135.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV136Error("v135 failure contract drifted")
    if terminal.get("failure") != _record(paths["failure"]):
        raise JudgeV5CalibrationV136Error("v135 failure record drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV136Error("v135 runtime record drifted")

    turns = []
    usage = {field: 0 for field in USAGE_FIELDS}
    frozen_turns = {row["turn_name"]: row for row in spec["frozen_inputs"]["turns"]}
    for turn_name in spec["turn_plan"]:
        frozen = frozen_turns[turn_name]
        for key in ("input", "prompt", "schema"):
            if not _verify_record(frozen[key]):
                raise JudgeV5CalibrationV136Error(f"v135 frozen {key} drifted")
        turn_root = root / "turns" / turn_name.replace("_", "-")
        turn_paths = {
            name: turn_root / filename
            for name, filename in {
                "capacity": "capacity.json",
                "sidecar": "sidecar.json",
                "output": "output.private.json",
            }.items()
        }
        if any(not path.is_file() for path in turn_paths.values()):
            raise JudgeV5CalibrationV136Error("v135 turn coverage is incomplete")
        input_value = _load_json(Path(frozen["input"]["path"]), "v135 turn input")
        output = _load_json(turn_paths["output"], "v135 turn output")
        errors = validate_output(output, input_value)
        if errors:
            raise JudgeV5CalibrationV136Error("v135 structured output validation drifted")
        measured = _validate_usage(_load_json(turn_paths["sidecar"], "v135 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        turns.append(
            {
                "turn_name": turn_name,
                "role": frozen["role"],
                "field": frozen["field"],
                "input": frozen["input"],
                "capacity": _record(turn_paths["capacity"]),
                "sidecar": _record(turn_paths["sidecar"]),
                "output": _record(turn_paths["output"]),
                "output_value": output,
            }
        )
    if usage != terminal["usage"] or usage != failure["known_usage_lower_bound"]:
        raise JudgeV5CalibrationV136Error("v135 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "turns": turns,
        "usage": usage,
    }


def _merge_by_task_id(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    decisions = [row for output in outputs for row in output["decisions"]]
    task_ids = [str(row["task_id"]) for row in decisions]
    if len(task_ids) != len(set(task_ids)):
        raise JudgeV5CalibrationV136Error("recovered task IDs overlap")
    return {"schema_version": V136_OUTPUT_VERSION, "decisions": decisions}


def recover_v136(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v136 terminal")
    predecessor = _validate_v135()
    primary = _merge_by_task_id(
        [row["output_value"] for row in predecessor["turns"] if row["role"] == "primary"]
    )
    canary = _merge_by_task_id(
        [
            row["output_value"]
            for row in predecessor["turns"]
            if row["role"] == "order_canary"
        ]
    )
    truth = predecessor["values"]["truth"]
    score = v135.score_v135(primary=primary, canary=canary, truth=truth)
    paths = {
        "primary": root / "recovered-dominant-field-owner-output.private.json",
        "canary": root / "recovered-dominant-field-owner-canary.private.json",
        "score": root / "recovered-dominant-field-owner-score.json",
    }
    _write_immutable(paths["primary"], primary)
    _write_immutable(paths["canary"], canary)
    _write_immutable(paths["score"], score)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V136_SPEC_VERSION,
        "state": "deterministic_recovery_only",
        "created_at": now_iso(),
        "strategy": "hash_validated_task_id_merge_of_all_six_measured_v135_outputs",
        "semantic_turn_count": 0,
        "semantic_retry_count": 0,
        "new_semantic_usage": {field: 0 for field in USAGE_FIELDS},
        "predecessor_usage": predecessor["usage"],
        "predecessor": {
            **predecessor["records"],
            "turns": [
                {key: row[key] for key in ("turn_name", "role", "field", "input", "capacity", "sidecar", "output")}
                for row in predecessor["turns"]
            ],
        },
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v135_dominant_field_owner_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
        ],
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    spec_path = root / "dominant-field-owner-recovery-spec.json"
    _write_immutable(spec_path, spec)
    passed = bool(score["passed"])
    terminal = {
        "schema_version": V136_TERMINAL_VERSION,
        "state": "completed" if passed else "inactive",
        "terminal_at": now_iso(),
        "terminal_reason": (
            "v136_recovered_field_owner_passed_expansion_authorized"
            if passed
            else "inactive_incomplete_recovery_required"
        ),
        "development_terminal_reason": (
            "v136_zero_model_recovery_confirmed_v135_protocol_pass"
            if passed
            else "v136_recovered_field_owner_quality_gate_not_passed"
        ),
        "overall_evaluation_complete": False,
        "expanded_field_owner_diagnostic_authorized": passed,
        "reference_patch_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "usage_status": "no_new_semantic_usage",
        "accounting_complete": True,
        "usage": {field: 0 for field in USAGE_FIELDS},
        "reused_predecessor_usage": predecessor["usage"],
        "spec": _record(spec_path),
        "score": _record(paths["score"]),
        "primary_output": _record(paths["primary"]),
        "canary_output": _record(paths["canary"]),
        "failed_quality_gates": score["failed_checks"],
        "metrics": score["metrics"],
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Recover v135 field-owner outputs")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = recover_v136(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "expanded_field_owner_diagnostic_authorized": terminal.get(
                    "expanded_field_owner_diagnostic_authorized", False
                ),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
