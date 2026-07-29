from __future__ import annotations

"""Freeze reference v7 after the passed v97 residual field audit."""

import argparse
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5 import CHECKLIST_FIELDS
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record_matches,
    _verify_record,
)
from .app_server_judge_v5_calibration_v95_reference_v6_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V95_ROOT,
)
from .app_server_judge_v5_calibration_v97_residual_field_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V97_ROOT,
)
from .app_server_judge_v5_diagnostic import _record
from .util import now_iso


V98_TRUTH_VERSION = "pif_app_server_judge_v5_4_reference_v7"
V98_AUDIT_VERSION = "pif_app_server_judge_v5_4_v98_reference_patch_audit_v1"
V98_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v98_reference_receipt_v1"
V98_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v98_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V97_ROOT.parent / "judge-calibration-v5_4-v98-reference-v7-freeze"
).resolve()


class JudgeV5CalibrationV98Error(RuntimeError):
    """The v98 reference freeze cannot preserve v97's authorization."""


def _validate_predecessors(*, v97_root: Path, v95_root: Path) -> dict[str, Any]:
    paths = {
        "v97_terminal": v97_root / "terminal.json",
        "v97_spec": v97_root / "residual-field-spec.json",
        "v97_score": v97_root / "residual-field-score.json",
        "v97_output": v97_root / "residual-field-output.private.json",
        "v97_canary": v97_root / "permutation-canary-output.private.json",
        "v97_proposal": v97_root / "reference-patch-proposal.json",
        "v97_rubric": v97_root / "field-rubric.json",
        "v95_terminal": v95_root / "terminal.json",
        "v95_receipt": v95_root / "reference-receipt.json",
        "v95_truth": v95_root / "calibration-truth-v6.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t97 = values["v97_terminal"]
    s97 = values["v97_score"]
    sp97 = values["v97_spec"]
    p97 = values["v97_proposal"]
    t95 = values["v95_terminal"]
    if (
        t97.get("state") != "completed"
        or t97.get("terminal_reason")
        != "v97_residual_field_audit_passed_reference_patch_authorized"
        or t97.get("reference_patch_authorized") is not True
        or t97.get("reference_freeze_authorized") is not False
        or t97.get("usage_status") != "complete"
        or t97.get("accounting_complete") is not True
        or t97.get("semantic_retry_count") != 0
        or t97.get("production_mutated") is not False
        or len(t97.get("attempts") or []) != 5
        or not all(
            _verify_record(record)
            for attempt in t97.get("attempts") or []
            for record in (attempt.get("capacity"), attempt.get("sidecar"), attempt.get("output"))
        )
        or not _record_matches(t97.get("score"), paths["v97_score"])
        or not _record_matches(t97.get("output"), paths["v97_output"])
        or not _record_matches(t97.get("canary_output"), paths["v97_canary"])
        or not _record_matches(t97.get("patch_proposal"), paths["v97_proposal"])
        or s97.get("passed") is not True
        or s97.get("metrics", {}).get("settled_control_exact_count") != 8
        or s97.get("metrics", {}).get("permutation_canary_exact_count") != 4
        or s97.get("metrics", {}).get("reference_change_count") != 2
        or s97.get("metrics", {}).get("reference_change_field_counts")
        != {"metric": 1, "speaker": 1}
        or len(p97.get("changes") or []) != 4
        or not _record_matches(sp97.get("frozen_inputs", {}).get("rubric"), paths["v97_rubric"])
        or not all(_verify_record(row) for row in sp97.get("runtime_files") or [])
        or t95.get("state") != "completed"
        or t95.get("reference_frozen") is not True
        or t95.get("production_mutated") is not False
        or not _record_matches(t95.get("truth"), paths["v95_truth"])
        or not _record_matches(t95.get("reference_receipt"), paths["v95_receipt"])
    ):
        raise JudgeV5CalibrationV98Error("v95/v97 reference-freeze contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def build_v98_reference(
    base: Mapping[str, Any], patch: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    changes = patch.get("changes") or []
    if (
        len(changes) != 4
        or sum(row.get("reference_change") is True for row in changes) != 2
        or Counter(row["field"] for row in changes if row.get("reference_change") is True)
        != Counter({"metric": 1, "speaker": 1})
    ):
        raise JudgeV5CalibrationV98Error("v98 patch coverage drifted")
    identities = [(row["case_id"], row["witness_id"], row["field"]) for row in changes]
    if len(set(identities)) != 4:
        raise JudgeV5CalibrationV98Error("v98 patch identities overlap")
    reference = deepcopy(base)
    reference["schema_version"] = V98_TRUTH_VERSION
    reference["reference_version"] = "fixture_reference_v7_v98_residual_field_reconciled"
    operations = []
    for row in changes:
        case = reference["cases"].get(row["case_id"])
        if case is None or row["witness_id"] not in case["field_issues"]:
            raise JudgeV5CalibrationV98Error("v98 patch identity is absent")
        fields = list(case["field_issues"][row["witness_id"]])
        prior = "incorrect" if row["field"] in fields else "correct"
        if prior != row["prior_status"] or row["owner_status"] not in {"correct", "incorrect"}:
            raise JudgeV5CalibrationV98Error("v98 patch status drifted")
        if row["owner_status"] == "incorrect" and row["field"] not in fields:
            fields.append(row["field"])
        if row["owner_status"] == "correct" and row["field"] in fields:
            fields.remove(row["field"])
        field_set = set(fields)
        ordered = [field for field in CHECKLIST_FIELDS if field in field_set]
        if len(ordered) != len(field_set):
            raise JudgeV5CalibrationV98Error("v98 field issue is outside the checklist")
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
    normalization_operations = []
    for case_id, case in reference["cases"].items():
        base_case = base["cases"][case_id]
        if set(case["field_issues"]) != set(case["structured_fields"]):
            raise JudgeV5CalibrationV98Error("v98 witness identity coverage drifted")
        for witness_id, fields in case["field_issues"].items():
            final = "incorrect" if fields else "correct"
            prior = base_case["structured_fields"][witness_id]
            case["structured_fields"][witness_id] = final
            if prior != final:
                normalization_operations.append(
                    {"case_id": case_id, "witness_id": witness_id, "prior_status": prior, "final_status": final}
                )
    if len(reference.get("cases") or {}) != 66 or sum(
        len(case["field_issues"]) for case in reference["cases"].values()
    ) != 182:
        raise JudgeV5CalibrationV98Error("v98 reference coverage drifted")
    if any(
        reference["cases"][case_id]["proposition"] != base["cases"][case_id]["proposition"]
        for case_id in reference["cases"]
    ):
        raise JudgeV5CalibrationV98Error("v98 proposition truth changed")
    audit = {
        "schema_version": V98_AUDIT_VERSION,
        "created_at": now_iso(),
        "operation_count": len(operations),
        "reference_change_count": sum(row["reference_changed"] for row in operations),
        "field_change_counts": dict(
            sorted(Counter(row["field"] for row in operations if row["reference_changed"]).items())
        ),
        "normalization_operation_count": len(normalization_operations),
        "operations": operations,
        "normalization_operations": normalization_operations,
        "semantic_decisions_from_llms_only": True,
        "deterministic_code_scope": (
            "identity_mapping_field_set_projection_structured_status_normalization_and_validation_only"
        ),
        "proposition_truth_changed": False,
        "majority_voting_used": False,
        "privacy": "opaque_ids_fields_statuses_and_counts_only",
    }
    return reference, audit


def freeze_v98(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v97_root: Path = DEFAULT_V97_ROOT,
    v95_root: Path = DEFAULT_V95_ROOT,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v98 terminal")
    predecessor = _validate_predecessors(v97_root=v97_root.resolve(), v95_root=v95_root.resolve())
    reference, audit = build_v98_reference(
        predecessor["values"]["v95_truth"], predecessor["values"]["v97_proposal"]
    )
    truth_path = root / "calibration-truth-v7.private.json"
    audit_path = root / "reference-patch-audit.json"
    _write_immutable(truth_path, reference)
    _write_immutable(audit_path, audit)
    receipt = {
        "schema_version": V98_RECEIPT_VERSION,
        "created_at": now_iso(),
        "state": "frozen",
        "reference_version": reference["reference_version"],
        "case_count": 66,
        "witness_count": 182,
        "operation_count": audit["operation_count"],
        "reference_change_count": audit["reference_change_count"],
        "field_change_counts": audit["field_change_counts"],
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
        "schema_version": V98_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v98_reference_v7_frozen_fresh_diagnostic_authorized",
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
        "patch_audit": _record(audit_path),
        "reference_receipt": _record(receipt_path),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v98 reference v7")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v98(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal["reference_frozen"],
                "fresh_diagnostic_authorized": terminal["fresh_diagnostic_authorized"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
