from __future__ import annotations

"""Zero-token recovery and quality classification of the completed v123 output."""

import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import validate_output
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v121_retained_field_owner import score_v121
from .app_server_judge_v5_calibration_v123_capped_field_repair import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V123_ROOT,
    _validate_v119,
    _validate_v122,
    reconcile_v122,
    score_v123,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .util import now_iso


V124_AUDIT_VERSION = "pif_app_server_judge_v5_4_v124_scoreable_recovery_audit_v1"
V124_TAXONOMY_VERSION = "pif_app_server_judge_v5_4_v124_quality_taxonomy_v1"
V124_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v124_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V123_ROOT.parent / "judge-calibration-v5_4-v124-scoreable-recovery"
).resolve()


class JudgeV5CalibrationV124Error(RuntimeError):
    """The v124 recovery cannot preserve v123 evidence exactly."""


def _validate_v123() -> dict[str, Any]:
    root = DEFAULT_V123_ROOT
    paths = {
        "spec": root / "capped-repair-spec.json",
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "input": root / "capped-repair-input.private.json",
        "truth": root / "capped-repair-truth.private.json",
        "selection": root / "selection-audit.json",
        "capacity": root / "turns" / "capped-retained-field-repair" / "capacity.json",
        "sidecar": root / "turns" / "capped-retained-field-repair" / "sidecar.json",
        "output": root / "turns" / "capped-retained-field-repair" / "output.private.json",
    }
    values = {name: _load_json(path, f"v123 {name}") for name, path in paths.items()}
    terminal, failure, spec = values["terminal"], values["failure"], values["spec"]
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 26894
        or terminal.get("retained_field_reference_patch_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("error_class") != "JudgeV5CalibrationV26DiagnosticError"
        or failure.get("failed_turn_name") != "capped_retained_field_repair"
        or failure.get("usage_status") != "complete"
        or failure.get("unknown_usage_turn_count") != 0
        or len(failure.get("attempts") or []) != 1
        or spec.get("model") != "gpt-5.4"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV124Error("v123 failed predecessor contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5CalibrationV124Error("v123 runtime record drifted")
    for record in spec.get("frozen_inputs", {}).values():
        if not _verify_record(record):
            raise JudgeV5CalibrationV124Error("v123 frozen input record drifted")
    attempt = failure["attempts"][0]
    for key in ("capacity", "sidecar", "output"):
        if not isinstance(attempt.get(key), Mapping) or not _verify_record(attempt[key]):
            raise JudgeV5CalibrationV124Error(f"v123 {key} attempt record drifted")
        if dict(attempt[key]) != _record(paths[key]):
            raise JudgeV5CalibrationV124Error(f"v123 {key} path record drifted")
    usage = _validate_usage(values["sidecar"])
    if usage != terminal["usage"] or usage != failure["usage"]:
        raise JudgeV5CalibrationV124Error("v123 measured usage drifted")
    capacity = values["capacity"]
    sidecar = values["sidecar"]
    if (
        capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("rate_limit_reached_type") is not None
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("recovery_reran_model") is not False
    ):
        raise JudgeV5CalibrationV124Error("v123 managed-auth attempt telemetry drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "usage": usage,
    }


def recover_v123(v123: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    source = v123["values"]["output"]
    value = v123["values"]["input"]
    before_errors = validate_output(source, value)
    if before_errors != ["decision_6_evidence"]:
        raise JudgeV5CalibrationV124Error("v123 validator failure shape drifted")
    recovered = deepcopy(source)
    removed = 0
    changed_rows = []
    for row in recovered["decisions"]:
        original = list(row["source_evidence_spans"])
        deduplicated = list(dict.fromkeys(original))
        if deduplicated != original:
            removed += len(original) - len(deduplicated)
            changed_rows.append(str(row["task_id"]))
            row["source_evidence_spans"] = deduplicated
    if removed != 1 or len(changed_rows) != 1 or validate_output(recovered, value):
        raise JudgeV5CalibrationV124Error("v123 exact-identity evidence recovery drifted")
    for old, new in zip(source["decisions"], recovered["decisions"], strict=True):
        for key in ("task_id", "field_status", "rationale"):
            if old[key] != new[key]:
                raise JudgeV5CalibrationV124Error("v124 altered semantic output")
        if old["task_id"] not in changed_rows and old != new:
            raise JudgeV5CalibrationV124Error("v124 altered an unrelated row")
    audit = {
        "schema_version": V124_AUDIT_VERSION,
        "created_at": now_iso(),
        "source_failure_class": "JudgeV5CalibrationV26DiagnosticError",
        "source_validation_errors": before_errors,
        "recovery_operation": "exact_identity_deduplication_of_one_repeated_valid_evidence_span",
        "changed_row_count": 1,
        "removed_exact_duplicate_span_count": 1,
        "field_status_change_count": 0,
        "rationale_change_count": 0,
        "task_identity_change_count": 0,
        "new_semantic_turn_count": 0,
        "new_usage": {field: 0 for field in USAGE_FIELDS},
        "production_mutated": False,
    }
    return recovered, audit


def score_recovered_v123(
    *, v123: Mapping[str, Any], recovered: Mapping[str, Any], v122: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    truth = v123["values"]["truth"]
    reconciled = reconcile_v122(
        primary=v122["values"]["primary"], truth=truth, repair_output=recovered
    )
    full_score = score_v121(
        reconciled,
        v122["values"]["canary"],
        v122["v121"]["values"]["truth"],
    )
    score = score_v123(recovered, truth, full_score)
    if (
        score.get("passed") is not False
        or score.get("metrics", {}).get("matched_control_exact_count") != 3
        or score.get("metrics", {}).get("repair_gate_exact_count") != 0
        or score.get("metrics", {}).get("repair_abstention_count") != 0
        or score.get("metrics", {}).get("evidence_complete_count") != 9
        or full_score.get("metrics", {}).get("observable_repair_trigger_count") != 5
    ):
        raise JudgeV5CalibrationV124Error("v123 recovered quality classification drifted")
    return score, full_score, reconciled


def freeze_v124(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v124 terminal")
    v123 = _validate_v123()
    v122 = _validate_v122()
    _validate_v119()
    recovered, audit = recover_v123(v123)
    score, full_score, reconciled = score_recovered_v123(
        v123=v123, recovered=recovered, v122=v122
    )
    recovered_path = root / "scoreable-v123-output.private.json"
    score_path = root / "recovered-v123-score.json"
    full_score_path = root / "recomputed-full-field-owner-score.json"
    reconciled_path = root / "reconciled-retained-field-output.private.json"
    audit_path = root / "exact-identity-recovery-audit.json"
    _write_immutable(recovered_path, recovered)
    _write_immutable(score_path, score)
    _write_immutable(full_score_path, full_score)
    _write_immutable(reconciled_path, reconciled)
    _write_immutable(audit_path, audit)

    truth_map = {row["task_id"]: row for row in v123["values"]["truth"]["tasks"]}
    observed = {row["task_id"]: row for row in recovered["decisions"]}
    disagreements = [
        row
        for task_id, row in truth_map.items()
        if observed[task_id]["field_status"]
        != (row.get("required_gate_status") or row.get("control_expected_status"))
    ]
    if len(disagreements) != 6:
        raise JudgeV5CalibrationV124Error("v124 final-owner dispute coverage drifted")
    taxonomy = {
        "schema_version": V124_TAXONOMY_VERSION,
        "created_at": now_iso(),
        "quality_result_scoreable": True,
        "quality_gate_passed": False,
        "observable_final_owner_dispute_count": 6,
        "dispute_role_counts": dict(sorted(Counter(row["role"] for row in disagreements).items())),
        "dispute_field_counts": dict(sorted(Counter(row["field"] for row in disagreements).items())),
        "failed_checks": score["failed_checks"],
        "final_owner_authorized": True,
        "reference_patch_authorized": False,
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    taxonomy_path = root / "sanitized-quality-taxonomy.json"
    _write_immutable(taxonomy_path, taxonomy)
    terminal = {
        "schema_version": V124_TERMINAL_VERSION,
        "state": "inactive",
        "terminal_at": now_iso(),
        "terminal_reason": "inactive_incomplete_recovery_required",
        "development_terminal_reason": "v124_scoreable_v123_quality_disagreement_final_owner_required",
        "overall_evaluation_complete": False,
        "quality_result_scoreable": True,
        "quality_gate_passed": False,
        "final_owner_authorized": True,
        "retained_field_reference_patch_authorized": False,
        "proposition_reference_frozen": False,
        "alignment_reference_frozen": False,
        "fresh_diagnostic_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": {field: 0 for field in USAGE_FIELDS},
        "predecessor_v123_usage": v123["usage"],
        "recovered_output": _record(recovered_path),
        "score": _record(score_path),
        "full_field_owner_score": _record(full_score_path),
        "reconciled_output": _record(reconciled_path),
        "recovery_audit": _record(audit_path),
        "sanitized_taxonomy": _record(taxonomy_path),
        "failed_quality_gates": score["failed_checks"],
        "metrics": score["metrics"],
        "next_experiment": "one_side_free_final_owner_for_six_observable_disputes_with_matched_controls",
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main() -> int:
    terminal = freeze_v124()
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "quality_result_scoreable": terminal["quality_result_scoreable"],
                "quality_gate_passed": terminal["quality_gate_passed"],
                "final_owner_authorized": terminal["final_owner_authorized"],
                "new_usage": terminal["usage"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
