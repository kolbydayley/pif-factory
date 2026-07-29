from __future__ import annotations

"""Freeze reference v6 after the passed v94 systematic field audit."""

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
from .app_server_judge_v5_calibration_v90_reference_v5_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V90_ROOT,
)
from .app_server_judge_v5_calibration_v94_systematic_field_audit import (
    CHALLENGE_COUNTS,
    CONTROL_COUNT,
    DEFAULT_OUTPUT_ROOT as DEFAULT_V94_ROOT,
)
from .app_server_judge_v5_diagnostic import _record
from .util import now_iso


V95_TRUTH_VERSION = "pif_app_server_judge_v5_4_reference_v6"
V95_AUDIT_VERSION = "pif_app_server_judge_v5_4_v95_reference_patch_audit_v1"
V95_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v95_reference_receipt_v1"
V95_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v95_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V94_ROOT.parent / "judge-calibration-v5_4-v95-reference-v6-freeze"
).resolve()


class JudgeV5CalibrationV95Error(RuntimeError):
    """The v95 reference freeze cannot preserve v94's authorization."""


def _validate_predecessors(*, v94_root: Path, v90_root: Path) -> dict[str, Any]:
    paths = {
        "v94_terminal": v94_root / "terminal.json",
        "v94_spec": v94_root / "systematic-field-spec.json",
        "v94_score": v94_root / "systematic-field-score.json",
        "v94_output": v94_root / "systematic-field-output.private.json",
        "v94_canary": v94_root / "permutation-canary-output.private.json",
        "v94_proposal": v94_root / "reference-patch-proposal.json",
        "v94_rubric": v94_root / "field-rubric.json",
        "v90_terminal": v90_root / "terminal.json",
        "v90_receipt": v90_root / "reference-receipt.json",
        "v90_truth": v90_root / "calibration-truth-v5.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t94 = values["v94_terminal"]
    s94 = values["v94_score"]
    sp94 = values["v94_spec"]
    p94 = values["v94_proposal"]
    t90 = values["v90_terminal"]
    r90 = values["v90_receipt"]
    attempts = t94.get("attempts") or []
    if (
        t94.get("state") != "completed"
        or t94.get("terminal_reason")
        != "v94_systematic_field_audit_passed_reference_patch_authorized"
        or t94.get("reference_patch_authorized") is not True
        or t94.get("reference_freeze_authorized") is not False
        or t94.get("fresh_diagnostic_authorized") is not False
        or t94.get("usage_status") != "complete"
        or t94.get("accounting_complete") is not True
        or t94.get("semantic_retry_count") != 0
        or t94.get("production_mutated") is not False
        or len(attempts) != 10
        or not all(
            _verify_record(record)
            for attempt in attempts
            for record in (attempt.get("capacity"), attempt.get("sidecar"), attempt.get("output"))
        )
        or not _record_matches(t94.get("score"), paths["v94_score"])
        or not _record_matches(t94.get("output"), paths["v94_output"])
        or not _record_matches(t94.get("canary_output"), paths["v94_canary"])
        or not _record_matches(t94.get("patch_proposal"), paths["v94_proposal"])
        or s94.get("passed") is not True
        or s94.get("reference_patch_authorized") is not True
        or s94.get("metrics", {}).get("reference_challenge_field_counts")
        != dict(sorted(CHALLENGE_COUNTS.items()))
        or s94.get("metrics", {}).get("settled_control_exact_count") != CONTROL_COUNT
        or s94.get("metrics", {}).get("permutation_canary_exact_count") != 6
        or s94.get("metrics", {}).get("owner_abstention_count") != 0
        or s94.get("metrics", {}).get("canary_abstention_count") != 0
        or s94.get("metrics", {}).get("reference_change_count") != 14
        or s94.get("metrics", {}).get("reference_change_field_counts")
        != {"certainty": 12, "target": 2}
        or len(p94.get("changes") or []) != 25
        or p94.get("reference_freeze_authorized") is not False
        or not _record_matches(sp94.get("frozen_inputs", {}).get("rubric"), paths["v94_rubric"])
        or not all(_verify_record(row) for row in sp94.get("runtime_files") or [])
        or t90.get("state") != "completed"
        or t90.get("reference_frozen") is not True
        or t90.get("production_mutated") is not False
        or not _record_matches(t90.get("truth"), paths["v90_truth"])
        or not _record_matches(t90.get("reference_receipt"), paths["v90_receipt"])
        or r90.get("case_count") != 66
        or r90.get("witness_count") != 182
    ):
        raise JudgeV5CalibrationV95Error("v90/v94 reference-freeze contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def build_v95_reference(
    base: Mapping[str, Any], patch: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    changes = patch.get("changes") or []
    if (
        len(changes) != 25
        or sum(row.get("reference_change") is True for row in changes) != 14
        or Counter(
            row["field"] for row in changes if row.get("reference_change") is True
        )
        != Counter({"certainty": 12, "target": 2})
    ):
        raise JudgeV5CalibrationV95Error("v95 patch coverage drifted")
    identities = [(row["case_id"], row["witness_id"], row["field"]) for row in changes]
    if len(set(identities)) != len(identities):
        raise JudgeV5CalibrationV95Error("v95 patch identities overlap")
    reference = deepcopy(base)
    reference["schema_version"] = V95_TRUTH_VERSION
    reference["reference_version"] = "fixture_reference_v6_v95_systematic_field_reconciled"
    operations = []
    for row in changes:
        if row.get("prior_status") != "incorrect" or row.get("owner_status") not in {
            "correct",
            "incorrect",
        }:
            raise JudgeV5CalibrationV95Error("v95 patch status drifted")
        case = reference["cases"].get(row["case_id"])
        if case is None or row["witness_id"] not in case["field_issues"]:
            raise JudgeV5CalibrationV95Error("v95 patch identity is absent")
        fields = list(case["field_issues"][row["witness_id"]])
        prior = "incorrect" if row["field"] in fields else "correct"
        if prior != row["prior_status"]:
            raise JudgeV5CalibrationV95Error("v95 patch prior status drifted")
        if row["owner_status"] == "correct":
            fields.remove(row["field"])
        field_set = set(fields)
        ordered = [field for field in CHECKLIST_FIELDS if field in field_set]
        if len(ordered) != len(field_set):
            raise JudgeV5CalibrationV95Error("v95 field issue is outside the checklist")
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
        if set(case["field_issues"]) != set(case["structured_fields"]):
            raise JudgeV5CalibrationV95Error("v95 structured-field identity coverage drifted")
        base_case = base["cases"][case_id]
        for witness_id, fields in case["field_issues"].items():
            final_status = "incorrect" if fields else "correct"
            prior_status = base_case["structured_fields"][witness_id]
            case["structured_fields"][witness_id] = final_status
            if prior_status != final_status:
                normalization_operations.append(
                    {
                        "case_id": case_id,
                        "witness_id": witness_id,
                        "prior_status": prior_status,
                        "final_status": final_status,
                    }
                )
    if len(reference.get("cases") or {}) != 66:
        raise JudgeV5CalibrationV95Error("v95 case coverage drifted")
    witness_count = sum(len(case["field_issues"]) for case in reference["cases"].values())
    if witness_count != 182:
        raise JudgeV5CalibrationV95Error("v95 witness coverage drifted")
    if any(
        case["structured_fields"][witness_id]
        != ("incorrect" if fields else "correct")
        for case in reference["cases"].values()
        for witness_id, fields in case["field_issues"].items()
    ):
        raise JudgeV5CalibrationV95Error("v95 structured-field projection is inconsistent")
    if any(
        reference["cases"][case_id]["proposition"] != base["cases"][case_id]["proposition"]
        for case_id in reference["cases"]
    ):
        raise JudgeV5CalibrationV95Error("v95 proposition truth changed")
    audit = {
        "schema_version": V95_AUDIT_VERSION,
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


def freeze_v95(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v94_root: Path = DEFAULT_V94_ROOT,
    v90_root: Path = DEFAULT_V90_ROOT,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v95 terminal")
    predecessor = _validate_predecessors(v94_root=v94_root.resolve(), v90_root=v90_root.resolve())
    values = predecessor["values"]
    reference, audit = build_v95_reference(values["v90_truth"], values["v94_proposal"])
    truth_path = root / "calibration-truth-v6.private.json"
    audit_path = root / "reference-patch-audit.json"
    _write_immutable(truth_path, reference)
    _write_immutable(audit_path, audit)
    receipt = {
        "schema_version": V95_RECEIPT_VERSION,
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
        "schema_version": V95_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v95_reference_v6_frozen_fresh_diagnostic_authorized",
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
    parser = argparse.ArgumentParser(description="Freeze v95 reference v6")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v95(output_dir=Path(args.output_dir))
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
