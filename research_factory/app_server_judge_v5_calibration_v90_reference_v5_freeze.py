from __future__ import annotations

"""Freeze reference v5 after the passed v89 control-truth audit."""

import argparse
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5 import CHECKLIST_FIELDS
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v85_reference_v4_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V85_ROOT,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record_matches,
    _verify_record,
)
from .app_server_judge_v5_calibration_v88_residual_reference_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V88_ROOT,
    score_v88,
)
from .app_server_judge_v5_calibration_v89_control_truth_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V89_ROOT,
)
from .app_server_judge_v5_diagnostic import _record
from .util import now_iso


V90_TRUTH_VERSION = "pif_app_server_judge_v5_4_reference_v5"
V90_RECONCILIATION_VERSION = "pif_app_server_judge_v5_4_v90_v88_reconciliation_v1"
V90_AUDIT_VERSION = "pif_app_server_judge_v5_4_v90_reference_patch_audit_v1"
V90_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v90_reference_receipt_v1"
V90_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v90_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V89_ROOT.parent / "judge-calibration-v5_4-v90-reference-v5-freeze"
).resolve()


class JudgeV5CalibrationV90Error(RuntimeError):
    """The v90 reference freeze cannot preserve the v89 authorization."""


def _validate_predecessors(
    *, v89_root: Path, v88_root: Path, v85_root: Path
) -> dict[str, Any]:
    paths = {
        "v89_terminal": v89_root / "terminal.json",
        "v89_spec": v89_root / "control-truth-spec.json",
        "v89_score": v89_root / "control-truth-score.json",
        "v89_output": v89_root / "control-truth-output.private.json",
        "v89_canary": v89_root / "permutation-canary-output.private.json",
        "v89_proposal": v89_root / "control-truth-patch-proposal.json",
        "v88_terminal": v88_root / "terminal.json",
        "v88_spec": v88_root / "residual-reference-spec.json",
        "v88_truth": v88_root / "residual-reference-truth.private.json",
        "v88_output": v88_root / "residual-reference-output.private.json",
        "v88_canary": v88_root / "permutation-canary-output.private.json",
        "v85_terminal": v85_root / "terminal.json",
        "v85_receipt": v85_root / "reference-receipt.json",
        "v85_truth": v85_root / "calibration-truth-v4.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t89 = values["v89_terminal"]
    s89 = values["v89_score"]
    sp89 = values["v89_spec"]
    proposal = values["v89_proposal"]
    t88 = values["v88_terminal"]
    sp88 = values["v88_spec"]
    t85 = values["v85_terminal"]
    receipt85 = values["v85_receipt"]
    if (
        t89.get("state") != "completed"
        or t89.get("v88_reconciliation_authorized") is not True
        or t89.get("reference_freeze_authorized") is not False
        or t89.get("usage_status") != "complete"
        or t89.get("accounting_complete") is not True
        or t89.get("semantic_retry_count") != 0
        or t89.get("production_mutated") is not False
        or len(t89.get("attempts") or []) != 3
        or not all(
            _verify_record(record)
            for attempt in t89.get("attempts") or []
            for record in (attempt.get("capacity"), attempt.get("sidecar"), attempt.get("output"))
        )
        or not _record_matches(t89.get("score"), paths["v89_score"])
        or not _record_matches(t89.get("output"), paths["v89_output"])
        or not _record_matches(t89.get("canary_output"), paths["v89_canary"])
        or not _record_matches(t89.get("patch_proposal"), paths["v89_proposal"])
        or s89.get("passed") is not True
        or s89.get("v88_reconciliation_authorized") is not True
        or s89.get("metrics", {}).get("settled_control_exact_count") != 4
        or s89.get("metrics", {}).get("permutation_canary_exact_count") != 2
        or s89.get("metrics", {}).get("reference_change_count") != 2
        or len(proposal.get("changes") or []) != 2
        or proposal.get("v88_reconciliation_authorized") is not True
        or not all(_verify_record(row) for row in sp89.get("runtime_files") or [])
        or t88.get("state") != "inactive"
        or t88.get("reference_patch_authorized") is not False
        or t88.get("usage_status") != "complete"
        or t88.get("production_mutated") is not False
        or not _record_matches(t88.get("output"), paths["v88_output"])
        or not _record_matches(t88.get("canary_output"), paths["v88_canary"])
        or not _record_matches(sp88.get("frozen_inputs", {}).get("truth"), paths["v88_truth"])
        or not all(_verify_record(row) for row in sp88.get("runtime_files") or [])
        or t85.get("reference_frozen") is not True
        or t85.get("production_mutated") is not False
        or not _record_matches(t85.get("truth"), paths["v85_truth"])
        or not _record_matches(t85.get("reference_receipt"), paths["v85_receipt"])
        or receipt85.get("case_count") != 66
        or receipt85.get("witness_count") != 182
    ):
        raise JudgeV5CalibrationV90Error("v85/v88/v89 reference-freeze contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def reconcile_v88_truth(
    truth: Mapping[str, Any], patch: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    adjusted = deepcopy(truth)
    rows = adjusted.get("tasks") or []
    operations = []
    for change in patch.get("changes") or []:
        matches = [
            row
            for row in rows
            if row.get("role") == "matched_control"
            and row.get("case_id") == change.get("case_id")
            and row.get("witness_id") == change.get("witness_id")
            and row.get("field") == change.get("field")
        ]
        if len(matches) != 1:
            raise JudgeV5CalibrationV90Error("v89 patch does not identify one v88 control")
        row = matches[0]
        prior = row["control_expected_status"]
        if prior != change.get("prior_status") or change.get("owner_status") not in {
            "correct",
            "incorrect",
        }:
            raise JudgeV5CalibrationV90Error("v89 patch prior/final status drifted")
        row["control_expected_status"] = change["owner_status"]
        row["prior_status"] = change["owner_status"]
        operations.append(
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "field": row["field"],
                "prior_status": prior,
                "final_status": change["owner_status"],
                "reference_changed": prior != change["owner_status"],
            }
        )
    if len(operations) != 2 or not all(row["reference_changed"] for row in operations):
        raise JudgeV5CalibrationV90Error("v90 control reconciliation count drifted")
    adjusted["schema_version"] = "pif_app_server_judge_v5_4_v90_reconciled_v88_truth_v1"
    return adjusted, operations


def derive_v88_dispute_patch(
    truth: Mapping[str, Any], output: Mapping[str, Any]
) -> list[dict[str, Any]]:
    observed = {row["task_id"]: row for row in output.get("decisions") or []}
    proposals = []
    for row in truth.get("tasks") or []:
        if row.get("role") != "reference_dispute":
            continue
        final = observed[row["task_id"]]["field_status"]
        if final not in {"correct", "incorrect"}:
            raise JudgeV5CalibrationV90Error("v88 dispute owner abstained")
        proposals.append(
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "field": row["field"],
                "prior_status": row["prior_status"],
                "owner_status": final,
                "reference_change": row["prior_status"] != final,
            }
        )
    if len(proposals) != 3 or sum(row["reference_change"] for row in proposals) != 2:
        raise JudgeV5CalibrationV90Error("v88 dispute patch count drifted")
    return proposals


def build_v90_reference(
    base: Mapping[str, Any], changes: Sequence[Mapping[str, Any]]
) -> tuple[dict[str, Any], dict[str, Any]]:
    if len(changes) != 5 or sum(row.get("reference_change") is True for row in changes) != 4:
        raise JudgeV5CalibrationV90Error("v90 combined patch count drifted")
    identities = [(row["case_id"], row["witness_id"], row["field"]) for row in changes]
    if len(set(identities)) != 5:
        raise JudgeV5CalibrationV90Error("v90 combined patch identities overlap")
    reference = deepcopy(base)
    reference["schema_version"] = V90_TRUTH_VERSION
    reference["reference_version"] = "fixture_reference_v5_v90_control_truth_reconciled"
    operations = []
    for row in changes:
        case = reference["cases"].get(row["case_id"])
        if case is None or row["witness_id"] not in case["field_issues"]:
            raise JudgeV5CalibrationV90Error("v90 patch identity is absent")
        fields = list(case["field_issues"][row["witness_id"]])
        prior = "incorrect" if row["field"] in fields else "correct"
        if prior != row["prior_status"]:
            raise JudgeV5CalibrationV90Error("v90 patch prior status drifted")
        final = row["owner_status"]
        if final == "incorrect" and row["field"] not in fields:
            fields.append(row["field"])
        if final == "correct" and row["field"] in fields:
            fields.remove(row["field"])
        field_set = set(fields)
        ordered = [field for field in CHECKLIST_FIELDS if field in field_set]
        if len(ordered) != len(field_set):
            raise JudgeV5CalibrationV90Error("v90 field issue is outside the checklist")
        case["field_issues"][row["witness_id"]] = ordered
        operations.append(
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "field": row["field"],
                "prior_status": prior,
                "final_status": final,
                "reference_changed": prior != final,
            }
        )
    if len(reference.get("cases") or {}) != 66:
        raise JudgeV5CalibrationV90Error("v90 case coverage drifted")
    witness_count = sum(len(case["field_issues"]) for case in reference["cases"].values())
    if witness_count != 182:
        raise JudgeV5CalibrationV90Error("v90 witness coverage drifted")
    audit = {
        "schema_version": V90_AUDIT_VERSION,
        "created_at": now_iso(),
        "operation_count": len(operations),
        "reference_change_count": sum(row["reference_changed"] for row in operations),
        "field_change_counts": dict(
            sorted(Counter(row["field"] for row in operations if row["reference_changed"]).items())
        ),
        "operations": operations,
        "semantic_decisions_from_llms_only": True,
        "deterministic_code_scope": "identity_mapping_field_set_projection_validation_and_rescoring_only",
        "majority_voting_used": False,
        "privacy": "opaque_ids_fields_statuses_and_counts_only",
    }
    return reference, audit


def freeze_v90(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v89_root: Path = DEFAULT_V89_ROOT,
    v88_root: Path = DEFAULT_V88_ROOT,
    v85_root: Path = DEFAULT_V85_ROOT,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v90 terminal")
    predecessor = _validate_predecessors(
        v89_root=v89_root.resolve(), v88_root=v88_root.resolve(), v85_root=v85_root.resolve()
    )
    values = predecessor["values"]
    adjusted_truth, control_operations = reconcile_v88_truth(
        values["v88_truth"], values["v89_proposal"]
    )
    reconcored = score_v88(values["v88_output"], values["v88_canary"], adjusted_truth)
    if (
        reconcored.get("passed") is not True
        or reconcored.get("reference_patch_authorized") is not True
        or reconcored.get("metrics", {}).get("matched_control_exact_count") != 6
        or reconcored.get("metrics", {}).get("permutation_canary_exact_count") != 3
        or reconcored.get("metrics", {}).get("abstention_count") != 0
    ):
        raise JudgeV5CalibrationV90Error("v88 does not pass after authorized truth reconciliation")
    dispute_patch = derive_v88_dispute_patch(adjusted_truth, values["v88_output"])
    combined_changes = list(values["v89_proposal"]["changes"]) + dispute_patch
    reference, audit = build_v90_reference(values["v85_truth"], combined_changes)
    truth_path = root / "calibration-truth-v5.private.json"
    adjusted_path = root / "v88-reconciled-truth.private.json"
    score_path = root / "v88-reconciliation-score.json"
    audit_path = root / "reference-patch-audit.json"
    _write_immutable(truth_path, reference)
    _write_immutable(adjusted_path, adjusted_truth)
    _write_immutable(score_path, {**reconcored, "schema_version": V90_RECONCILIATION_VERSION})
    _write_immutable(audit_path, audit)
    receipt = {
        "schema_version": V90_RECEIPT_VERSION,
        "created_at": now_iso(),
        "state": "frozen",
        "reference_version": reference["reference_version"],
        "case_count": 66,
        "witness_count": 182,
        "operation_count": audit["operation_count"],
        "reference_change_count": audit["reference_change_count"],
        "control_reconciliation_operation_count": len(control_operations),
        "truth": _record(truth_path),
        "adjusted_v88_truth": _record(adjusted_path),
        "v88_reconciliation_score": _record(score_path),
        "patch_audit": _record(audit_path),
        "predecessor": predecessor["records"],
        "reference_frozen": True,
        "fresh_diagnostic_authorized": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "new_semantic_turn_count": 0,
    }
    receipt_path = root / "reference-receipt.json"
    _write_immutable(receipt_path, receipt)
    terminal = {
        "schema_version": V90_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v90_reference_v5_frozen_fresh_diagnostic_authorized",
        "overall_evaluation_complete": False,
        "reference_frozen": True,
        "fresh_diagnostic_authorized": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "total_tokens": 0,
        },
        "truth": _record(truth_path),
        "adjusted_v88_truth": _record(adjusted_path),
        "v88_reconciliation_score": _record(score_path),
        "patch_audit": _record(audit_path),
        "reference_receipt": _record(receipt_path),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v90 reference v5")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v90(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal["reference_frozen"],
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
