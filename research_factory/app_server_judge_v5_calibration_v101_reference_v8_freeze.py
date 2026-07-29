from __future__ import annotations

"""Freeze reference v8 after the passed v100 stance/inference audit."""

import argparse
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5 import CHECKLIST_FIELDS
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import _record_matches, _verify_record
from .app_server_judge_v5_calibration_v98_reference_v7_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V98_ROOT,
)
from .app_server_judge_v5_calibration_v100_stance_inference_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V100_ROOT,
)
from .app_server_judge_v5_diagnostic import _record
from .util import now_iso


V101_TRUTH_VERSION = "pif_app_server_judge_v5_4_reference_v8"
V101_AUDIT_VERSION = "pif_app_server_judge_v5_4_v101_reference_patch_audit_v1"
V101_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v101_reference_receipt_v1"
V101_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v101_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V100_ROOT.parent / "judge-calibration-v5_4-v101-reference-v8-freeze"
).resolve()


class JudgeV5CalibrationV101Error(RuntimeError):
    """The v101 reference freeze cannot preserve v100's authorization."""


def _validate_predecessors(*, v100_root: Path, v98_root: Path) -> dict[str, Any]:
    paths = {
        "v100_terminal": v100_root / "terminal.json",
        "v100_spec": v100_root / "stance-inference-spec.json",
        "v100_score": v100_root / "stance-inference-score.json",
        "v100_output": v100_root / "stance-inference-output.private.json",
        "v100_canary": v100_root / "permutation-canary-output.private.json",
        "v100_proposal": v100_root / "reference-patch-proposal.json",
        "v100_rubric": v100_root / "field-rubric.json",
        "v98_terminal": v98_root / "terminal.json",
        "v98_receipt": v98_root / "reference-receipt.json",
        "v98_truth": v98_root / "calibration-truth-v7.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t100, s100, sp100, p100 = (
        values["v100_terminal"],
        values["v100_score"],
        values["v100_spec"],
        values["v100_proposal"],
    )
    t98 = values["v98_terminal"]
    if (
        t100.get("state") != "completed"
        or t100.get("terminal_reason")
        != "v100_stance_inference_audit_passed_reference_patch_authorized"
        or t100.get("reference_patch_authorized") is not True
        or t100.get("usage_status") != "complete"
        or t100.get("accounting_complete") is not True
        or t100.get("production_mutated") is not False
        or len(t100.get("attempts") or []) != 3
        or not all(
            _verify_record(record)
            for attempt in t100.get("attempts") or []
            for record in (attempt.get("capacity"), attempt.get("sidecar"), attempt.get("output"))
        )
        or not _record_matches(t100.get("score"), paths["v100_score"])
        or not _record_matches(t100.get("output"), paths["v100_output"])
        or not _record_matches(t100.get("canary_output"), paths["v100_canary"])
        or not _record_matches(t100.get("patch_proposal"), paths["v100_proposal"])
        or s100.get("passed") is not True
        or s100.get("metrics", {}).get("reference_change_count") != 1
        or s100.get("metrics", {}).get("reference_change_field_counts")
        != {"unsupported_inference": 1}
        or len(p100.get("changes") or []) != 2
        or not _record_matches(sp100.get("frozen_inputs", {}).get("rubric"), paths["v100_rubric"])
        or not all(_verify_record(row) for row in sp100.get("runtime_files") or [])
        or t98.get("state") != "completed"
        or t98.get("reference_frozen") is not True
        or t98.get("production_mutated") is not False
        or not _record_matches(t98.get("truth"), paths["v98_truth"])
        or not _record_matches(t98.get("reference_receipt"), paths["v98_receipt"])
    ):
        raise JudgeV5CalibrationV101Error("v98/v100 reference-freeze contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def build_v101_reference(
    base: Mapping[str, Any], patch: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    changes = patch.get("changes") or []
    if (
        len(changes) != 2
        or sum(row.get("reference_change") is True for row in changes) != 1
        or Counter(row["field"] for row in changes if row.get("reference_change") is True)
        != Counter({"unsupported_inference": 1})
    ):
        raise JudgeV5CalibrationV101Error("v101 patch coverage drifted")
    reference = deepcopy(base)
    reference["schema_version"] = V101_TRUTH_VERSION
    reference["reference_version"] = "fixture_reference_v8_v101_stance_inference_reconciled"
    operations = []
    for row in changes:
        case = reference["cases"].get(row["case_id"])
        if case is None or row["witness_id"] not in case["field_issues"]:
            raise JudgeV5CalibrationV101Error("v101 patch identity is absent")
        fields = list(case["field_issues"][row["witness_id"]])
        prior = "incorrect" if row["field"] in fields else "correct"
        if prior != row["prior_status"] or row["owner_status"] not in {"correct", "incorrect"}:
            raise JudgeV5CalibrationV101Error("v101 patch status drifted")
        if row["owner_status"] == "correct" and row["field"] in fields:
            fields.remove(row["field"])
        if row["owner_status"] == "incorrect" and row["field"] not in fields:
            fields.append(row["field"])
        field_set = set(fields)
        ordered = [field for field in CHECKLIST_FIELDS if field in field_set]
        if len(ordered) != len(field_set):
            raise JudgeV5CalibrationV101Error("v101 field issue is outside checklist")
        case["field_issues"][row["witness_id"]] = ordered
        operations.append(
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "field": row["field"],
                "prior_status": prior,
                "final_status": row["owner_status"],
                "reference_changed": prior != row["owner_status"],
            }
        )
    normalization = []
    for case_id, case in reference["cases"].items():
        base_case = base["cases"][case_id]
        for witness_id, fields in case["field_issues"].items():
            final = "incorrect" if fields else "correct"
            prior = base_case["structured_fields"][witness_id]
            case["structured_fields"][witness_id] = final
            if prior != final:
                normalization.append(
                    {"case_id": case_id, "witness_id": witness_id, "prior_status": prior, "final_status": final}
                )
    if len(reference.get("cases") or {}) != 66 or sum(
        len(case["field_issues"]) for case in reference["cases"].values()
    ) != 182:
        raise JudgeV5CalibrationV101Error("v101 coverage drifted")
    if any(
        reference["cases"][case_id]["proposition"] != base["cases"][case_id]["proposition"]
        for case_id in reference["cases"]
    ):
        raise JudgeV5CalibrationV101Error("v101 proposition truth changed")
    audit = {
        "schema_version": V101_AUDIT_VERSION,
        "created_at": now_iso(),
        "operation_count": 2,
        "reference_change_count": 1,
        "field_change_counts": {"unsupported_inference": 1},
        "normalization_operation_count": len(normalization),
        "operations": operations,
        "normalization_operations": normalization,
        "semantic_decisions_from_llms_only": True,
        "deterministic_code_scope": (
            "identity_mapping_field_set_projection_structured_status_normalization_and_validation_only"
        ),
        "proposition_truth_changed": False,
        "majority_voting_used": False,
        "privacy": "opaque_ids_fields_statuses_and_counts_only",
    }
    return reference, audit


def freeze_v101(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v100_root: Path = DEFAULT_V100_ROOT,
    v98_root: Path = DEFAULT_V98_ROOT,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v101 terminal")
    predecessor = _validate_predecessors(v100_root=v100_root.resolve(), v98_root=v98_root.resolve())
    reference, audit = build_v101_reference(
        predecessor["values"]["v98_truth"], predecessor["values"]["v100_proposal"]
    )
    truth_path, audit_path = root / "calibration-truth-v8.private.json", root / "reference-patch-audit.json"
    _write_immutable(truth_path, reference)
    _write_immutable(audit_path, audit)
    receipt = {
        "schema_version": V101_RECEIPT_VERSION,
        "created_at": now_iso(),
        "state": "frozen",
        "reference_version": reference["reference_version"],
        "case_count": 66,
        "witness_count": 182,
        "operation_count": 2,
        "reference_change_count": 1,
        "field_change_counts": {"unsupported_inference": 1},
        "normalization_operation_count": audit["normalization_operation_count"],
        "truth": _record(truth_path),
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
        "schema_version": V101_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v101_reference_v8_frozen_fresh_diagnostic_authorized",
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
        "usage": {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0, "reasoning_output_tokens": 0, "total_tokens": 0},
        "truth": _record(truth_path),
        "patch_audit": _record(audit_path),
        "reference_receipt": _record(receipt_path),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v101 reference v8")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v101(output_dir=Path(args.output_dir))
    print(json.dumps({"state": terminal["state"], "terminal_reason": terminal["terminal_reason"], "reference_frozen": terminal["reference_frozen"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
