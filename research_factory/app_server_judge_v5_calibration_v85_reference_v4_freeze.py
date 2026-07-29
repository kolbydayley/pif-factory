from __future__ import annotations

"""Freeze reference v4 from the passed v84 metric/target audit."""

import argparse
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5 import CHECKLIST_FIELDS
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v82_reference_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V82_ROOT,
)
from .app_server_judge_v5_calibration_v84_metric_target_reference_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V84_ROOT,
)
from .app_server_judge_v5_diagnostic import _record, _sha256_file
from .util import now_iso


V85_TRUTH_VERSION = "pif_app_server_judge_v5_4_reference_v4"
V85_AUDIT_VERSION = "pif_app_server_judge_v5_4_v85_reference_patch_audit_v1"
V85_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v85_reference_receipt_v1"
V85_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v85_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V84_ROOT.parent / "judge-calibration-v5_4-v85-reference-v4-freeze"
).resolve()


class JudgeV5CalibrationV85Error(RuntimeError):
    """The v85 reference freeze cannot preserve the v84 authorization."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _verify_record(record: Any) -> bool:
    return (
        isinstance(record, Mapping)
        and isinstance(record.get("path"), str)
        and _record_matches(record, Path(record["path"]))
    )


def _validate_predecessors(v84_root: Path, v82_root: Path) -> dict[str, Any]:
    paths = {
        "v84_terminal": v84_root / "terminal.json",
        "v84_spec": v84_root / "metric-target-spec.json",
        "v84_score": v84_root / "metric-target-score.json",
        "v84_output": v84_root / "metric-target-output.private.json",
        "v84_canary": v84_root / "permutation-canary-output.private.json",
        "v84_proposal": v84_root / "reference-patch-proposal.json",
        "v84_projection": v84_root / "projection-audit.json",
        "v82_terminal": v82_root / "terminal.json",
        "v82_receipt": v82_root / "reference-receipt.json",
        "v82_truth": v82_root / "calibration-truth-v3.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v84_terminal"]
    spec = values["v84_spec"]
    score = values["v84_score"]
    proposal = values["v84_proposal"]
    v82_terminal = values["v82_terminal"]
    if (
        terminal.get("state") != "completed"
        or terminal.get("reference_audit_passed") is not True
        or terminal.get("reference_freeze_authorized") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or len(terminal.get("attempts") or []) != 4
        or not all(
            _verify_record(record)
            for attempt in terminal.get("attempts") or []
            for record in (attempt.get("capacity"), attempt.get("sidecar"), attempt.get("output"))
        )
        or not _record_matches(terminal.get("score"), paths["v84_score"])
        or not _record_matches(terminal.get("output"), paths["v84_output"])
        or not _record_matches(terminal.get("canary_output"), paths["v84_canary"])
        or not _record_matches(terminal.get("reference_patch_proposal"), paths["v84_proposal"])
        or not _record_matches(terminal.get("projection_audit"), paths["v84_projection"])
        or score.get("passed") is not True
        or score.get("reference_freeze_authorized") is not True
        or score.get("metrics", {}).get("reference_change_count") != 2
        or proposal.get("state") != "authorized"
        or len(proposal.get("rows") or []) != 3
        or sum(row.get("reference_change") is True for row in proposal.get("rows") or []) != 2
        or proposal.get("majority_voting_used") is not False
        or spec.get("model") != "gpt-5.5"
        or spec.get("majority_voting_used") is not False
        or not all(_verify_record(row) for row in spec.get("runtime_files") or [])
        or v82_terminal.get("reference_frozen") is not True
        or not _record_matches(v82_terminal.get("truth"), paths["v82_truth"])
        or not _record_matches(v82_terminal.get("reference_receipt"), paths["v82_receipt"])
    ):
        raise JudgeV5CalibrationV85Error("v84/v82 predecessor contract drifted")
    return {name: _record(path) for name, path in paths.items()}


def build_v85_reference(
    base: Mapping[str, Any], proposal: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    if proposal.get("state") != "authorized" or len(proposal.get("rows") or []) != 3:
        raise JudgeV5CalibrationV85Error("v84 proposal is not authorized")
    reference = deepcopy(base)
    reference["schema_version"] = V85_TRUTH_VERSION
    reference["reference_version"] = "fixture_reference_v4_v85_metric_target_reconciled"
    operations = []
    for row in proposal["rows"]:
        case = reference["cases"].get(row["case_id"])
        if case is None or row["witness_id"] not in case["field_issues"]:
            raise JudgeV5CalibrationV85Error("v84 proposal identity is absent")
        fields = list(case["field_issues"][row["witness_id"]])
        prior = "incorrect" if row["field"] in fields else "correct"
        if prior != row["prior_status"]:
            raise JudgeV5CalibrationV85Error("v84 proposal prior status drifted")
        final = row["owner_status"]
        if final not in {"correct", "incorrect"}:
            raise JudgeV5CalibrationV85Error("v84 proposal final status is invalid")
        if final == "incorrect" and row["field"] not in fields:
            fields.append(row["field"])
        if final == "correct" and row["field"] in fields:
            fields.remove(row["field"])
        field_set = set(fields)
        ordered = [field for field in CHECKLIST_FIELDS if field in field_set]
        if len(ordered) != len(field_set):
            raise JudgeV5CalibrationV85Error("v85 field issue is outside the checklist")
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
    if len(operations) != 3 or sum(row["reference_changed"] for row in operations) != 2:
        raise JudgeV5CalibrationV85Error("v85 operation count drifted")
    if len(reference.get("cases") or {}) != 66:
        raise JudgeV5CalibrationV85Error("v85 case coverage drifted")
    witness_count = sum(len(case["field_issues"]) for case in reference["cases"].values())
    if witness_count != 182:
        raise JudgeV5CalibrationV85Error("v85 witness coverage drifted")
    audit = {
        "schema_version": V85_AUDIT_VERSION,
        "created_at": now_iso(),
        "operation_count": 3,
        "reference_change_count": 2,
        "field_change_counts": dict(
            sorted(Counter(row["field"] for row in operations if row["reference_changed"]).items())
        ),
        "operations": operations,
        "semantic_decisions_from_llms_only": True,
        "deterministic_code_scope": "identity_mapping_and_field_set_projection_only",
        "majority_voting_used": False,
        "privacy": "opaque_ids_fields_statuses_and_counts_only",
    }
    return reference, audit


def freeze_v85(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v84_root: Path = DEFAULT_V84_ROOT,
    v82_root: Path = DEFAULT_V82_ROOT,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v85 terminal")
    predecessor = _validate_predecessors(v84_root.resolve(), v82_root.resolve())
    reference, audit = build_v85_reference(
        _load_json(v82_root / "calibration-truth-v3.private.json", "v82 truth"),
        _load_json(v84_root / "reference-patch-proposal.json", "v84 proposal"),
    )
    truth_path = root / "calibration-truth-v4.private.json"
    audit_path = root / "reference-patch-audit.json"
    _write_immutable(truth_path, reference)
    _write_immutable(audit_path, audit)
    receipt = {
        "schema_version": V85_RECEIPT_VERSION,
        "created_at": now_iso(),
        "state": "frozen",
        "reference_version": reference["reference_version"],
        "case_count": 66,
        "witness_count": 182,
        "operation_count": 3,
        "reference_change_count": 2,
        "truth": _record(truth_path),
        "patch_audit": _record(audit_path),
        "predecessor": predecessor,
        "reference_frozen": True,
        "fresh_enhanced_diagnostic_authorized": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "new_semantic_turn_count": 0,
    }
    receipt_path = root / "reference-receipt.json"
    _write_immutable(receipt_path, receipt)
    terminal = {
        "schema_version": V85_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v85_reference_v4_frozen_fresh_enhanced_diagnostic_authorized",
        "overall_evaluation_complete": False,
        "reference_frozen": True,
        "fresh_enhanced_diagnostic_authorized": True,
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
    parser = argparse.ArgumentParser(description="Freeze v85 reference v4")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v85(output_dir=Path(args.output_dir))
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
