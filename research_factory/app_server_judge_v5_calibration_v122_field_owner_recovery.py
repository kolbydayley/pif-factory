from __future__ import annotations

"""Zero-token recovery of v121's task-id aggregation failure."""

import json
from pathlib import Path
from typing import Any, Mapping

from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v118_capped_alignment_repair import _iter_records
from .app_server_judge_v5_calibration_v121_retained_field_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V121_ROOT,
    V121_REFERENCE_VERSION,
    score_v121,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .util import now_iso


V122_AUDIT_VERSION = "pif_app_server_judge_v5_4_v122_field_owner_recovery_audit_v1"
V122_TAXONOMY_VERSION = "pif_app_server_judge_v5_4_v122_sanitized_taxonomy_v1"
V122_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v122_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V121_ROOT.parent / "judge-calibration-v5_4-v122-field-owner-recovery"
).resolve()


class JudgeV5CalibrationV122Error(RuntimeError):
    """The v122 deterministic recovery cannot preserve v121 evidence."""


def _validate_v121() -> dict[str, Any]:
    root = DEFAULT_V121_ROOT
    paths = {
        "spec": root / "retained-field-owner-spec.json",
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "truth": root / "retained-field-truth.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v121 {name}") for name, path in paths.items()}
    terminal, failure, spec = values["terminal"], values["failure"], values["spec"]
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("retained_field_reference_patch_authorized") is not False
        or terminal.get("fresh_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 178899
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != "retained_field_owner_canary"
        or failure.get("error_class") != "KeyError"
        or failure.get("usage_status") != "complete"
        or failure.get("unknown_usage_turn_count") != 0
        or len(failure.get("attempts") or []) != 6
        or spec.get("model") != "gpt-5.6-terra"
        or spec.get("task_count") != 115
        or spec.get("retry_count_per_turn") != 0
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV122Error("v121 failed predecessor drifted")
    if not all(_verify_record(record) for record in _iter_records(spec)):
        raise JudgeV5CalibrationV122Error("v121 frozen record drifted")
    usage = {field: 0 for field in USAGE_FIELDS}
    attempts = {}
    for attempt in failure["attempts"]:
        turn_name = str(attempt["turn_name"])
        for key in ("capacity", "sidecar", "output"):
            if not isinstance(attempt.get(key), Mapping) or not _verify_record(attempt[key]):
                raise JudgeV5CalibrationV122Error("v121 attempt record drifted")
        measured = _validate_usage(_load_json(Path(attempt["sidecar"]["path"]), "v121 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = attempt
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV122Error("v121 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
    }


def recover_v121(v121: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    decisions = []
    source_records = []
    for index in range(5):
        turn_name = f"retained_field_owner_shard_{index:02d}"
        record = v121["attempts"][turn_name]["output"]
        output = _load_json(Path(record["path"]), f"v121 {turn_name}")
        decisions.extend(output["decisions"])
        source_records.append(record)
    primary = {"decisions": decisions}
    task_ids = [str(row["task_id"]) for row in decisions]
    if len(task_ids) != 115 or len(set(task_ids)) != 115:
        raise JudgeV5CalibrationV122Error("v121 recovered primary coverage drifted")
    canary_record = v121["attempts"]["retained_field_owner_canary"]["output"]
    canary = _load_json(Path(canary_record["path"]), "v121 canary")
    truth = v121["values"]["truth"]
    score = score_v121(primary, canary, truth)
    if (
        score.get("passed") is not False
        or score.get("capped_repair_authorized") is not True
        or score.get("metrics")
        != {
            "task_count": 115,
            "contested_task_count": 95,
            "matched_control_count": 20,
            "matched_control_exact_count": 19,
            "primary_abstention_count": 0,
            "permutation_canary_count": 12,
            "permutation_canary_exact_count": 9,
            "unsupported_inference_conflict_count": 1,
            "observable_repair_trigger_count": 5,
        }
    ):
        raise JudgeV5CalibrationV122Error("v121 recovered score drifted")
    audit = {
        "schema_version": V122_AUDIT_VERSION,
        "created_at": now_iso(),
        "source_failure_class": "KeyError",
        "root_cause": "generic_case_id_merge_helper_applied_to_task_id_decisions",
        "recovery_operation": "ordered_task_id_identity_merge_only",
        "source_primary_output_records": source_records,
        "source_canary_output_record": canary_record,
        "primary_decision_count": 115,
        "unique_task_id_count": 115,
        "new_semantic_turn_count": 0,
        "new_usage": {field: 0 for field in USAGE_FIELDS},
        "production_mutated": False,
    }
    return {"primary": primary, "canary": canary, "score": score}, audit


def freeze_v122(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v122 terminal")
    v121 = _validate_v121()
    recovered, audit = recover_v121(v121)
    primary_path = root / "retained-field-output.private.json"
    canary_path = root / "permutation-canary-output.private.json"
    score_path = root / "retained-field-owner-score.json"
    audit_path = root / "v121-recovery-audit.json"
    _write_immutable(primary_path, recovered["primary"])
    _write_immutable(canary_path, recovered["canary"])
    _write_immutable(score_path, recovered["score"])
    _write_immutable(audit_path, audit)
    taxonomy = {
        "schema_version": V122_TAXONOMY_VERSION,
        "created_at": now_iso(),
        "failed_gate_count": len(recovered["score"]["failed_checks"]),
        "failed_gates": recovered["score"]["failed_checks"],
        "observable_repair_trigger_count": 5,
        "trigger_reason_counts": dict(
            sorted(
                __import__("collections").Counter(
                    reason
                    for row in recovered["score"]["observable_repair_triggers"]
                    for reason in row["reasons"]
                ).items()
            )
        ),
        "capped_repair_authorized": True,
        "reference_patch_authorized": False,
        "privacy": "aggregate_counts_and_gate_names_only",
    }
    taxonomy_path = root / "sanitized-recovery-taxonomy.json"
    _write_immutable(taxonomy_path, taxonomy)
    terminal = {
        "schema_version": V122_TERMINAL_VERSION,
        "state": "inactive",
        "terminal_at": now_iso(),
        "terminal_reason": "inactive_incomplete_recovery_required",
        "development_terminal_reason": "v122_recovered_v121_quality_result_capped_repair_required",
        "overall_evaluation_complete": False,
        "retained_field_reference_patch_authorized": False,
        "capped_repair_authorized": True,
        "fresh_diagnostic_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": {field: 0 for field in USAGE_FIELDS},
        "predecessor_v121_usage": v121["usage"],
        "score": _record(score_path),
        "recovered_primary": _record(primary_path),
        "recovered_canary": _record(canary_path),
        "recovery_audit": _record(audit_path),
        "sanitized_taxonomy": _record(taxonomy_path),
        "failed_quality_gates": recovered["score"]["failed_checks"],
        "metrics": recovered["score"]["metrics"],
        "next_experiment": "one_side_free_capped_repair_for_five_observable_triggers",
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main() -> int:
    terminal = freeze_v122()
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "capped_repair_authorized": terminal["capped_repair_authorized"],
                "new_usage": terminal["usage"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
