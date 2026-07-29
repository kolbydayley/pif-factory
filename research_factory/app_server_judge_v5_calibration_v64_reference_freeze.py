from __future__ import annotations

"""Zero-token freeze of the v63 Sol reference-owner patch proposal."""

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v53_reference import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V53_ROOT,
)
from .app_server_judge_v5_calibration_v63_reference_reaudit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V63_ROOT,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _record, _sha256_file, _validate_usage
from .util import now_iso


V64_REFERENCE_VERSION = "pif_app_server_judge_v5_4_v64_reference_v2"
V64_AUDIT_VERSION = "pif_app_server_judge_v5_4_v64_reference_patch_audit_v1"
V64_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v64_reference_receipt_v1"
V64_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v64_reference_freeze_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V63_ROOT.parent / "judge-calibration-v5_4-v64-reference-freeze"
).resolve()


class JudgeV5CalibrationV64ReferenceError(RuntimeError):
    """The v63 proposal cannot be frozen into the full reference."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _validate_predecessors(v53_root: Path, v63_root: Path) -> dict[str, Any]:
    paths = {
        "v53_terminal": v53_root / "terminal.json",
        "v53_truth": v53_root / "scoped-calibration-truth.private.json",
        "v53_receipt": v53_root / "reference-receipt.json",
        "v63_terminal": v63_root / "terminal.json",
        "v63_spec": v63_root / "reference-reaudit-spec.json",
        "v63_proposal": v63_root / "reference-patch-proposal.json",
        "v63_output": v63_root / "reference-output.private.json",
        "v63_audit": v63_root / "contract-projection-audit.json",
        "v63_turn_input": v63_root / "turns/side-free-reference-owner-reaudit/input.private.json",
        "v63_turn_output": v63_root / "turns/side-free-reference-owner-reaudit/output.private.json",
        "v63_turn_capacity": v63_root / "turns/side-free-reference-owner-reaudit/capacity.json",
        "v63_turn_sidecar": v63_root / "turns/side-free-reference-owner-reaudit/sidecar.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    v53 = values["v53_terminal"]
    v63 = values["v63_terminal"]
    proposal = values["v63_proposal"]
    sidecar = values["v63_turn_sidecar"]
    capacity = values["v63_turn_capacity"]
    if (
        v53.get("state") != "completed"
        or v53.get("reference_frozen") is not True
        or v53.get("production_mutated") is not False
        or not _record_matches(v53.get("scoped_truth"), paths["v53_truth"])
        or not _record_matches(v53.get("reference_receipt"), paths["v53_receipt"])
        or v63.get("state") != "completed"
        or v63.get("terminal_reason") != "v63_reference_patch_proposal_completed_freeze_required"
        or v63.get("reference_patch_proposed") is not True
        or v63.get("reference_freeze_authorized") is not True
        or v63.get("reference_frozen") is not False
        or v63.get("usage_status") != "complete"
        or v63.get("accounting_complete") is not True
        or v63.get("semantic_retry_count") != 0
        or v63.get("production_mutated") is not False
        or not _record_matches(v63.get("proposal"), paths["v63_proposal"])
        or not _record_matches(v63.get("output"), paths["v63_output"])
        or not _record_matches(v63.get("contract_projection_audit"), paths["v63_audit"])
        or proposal.get("reference_patch_proposed") is not True
        or proposal.get("reference_freeze_authorized") is not True
        or proposal.get("change_count") != 5
        or proposal.get("audited_witness_count") != 6
        or sidecar.get("model") != "gpt-5.6-sol"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("state") != "completed"
        or sidecar.get("usage_status") != "measured"
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("rate_limit_reached_type") is not None
    ):
        raise JudgeV5CalibrationV64ReferenceError("v53/v63 predecessors are inadmissible")
    if _validate_usage(sidecar) != v63.get("usage"):
        raise JudgeV5CalibrationV64ReferenceError("v63 usage drifted")
    attempts = v63.get("attempts") or []
    if (
        len(attempts) != 1
        or not _record_matches(attempts[0].get("sidecar"), paths["v63_turn_sidecar"])
        or not _record_matches(attempts[0].get("capacity"), paths["v63_turn_capacity"])
        or not _record_matches(attempts[0].get("output"), paths["v63_turn_output"])
    ):
        raise JudgeV5CalibrationV64ReferenceError("v63 attempt binding drifted")
    proposed = {
        (row["case_id"], row["witness_id"]): {
            item["field"]
            for item in row["checklist"]
            if item["independent_root_status"] == "root"
        }
        for row in values["v63_output"]["units"]
    }
    for change in proposal["changes"]:
        key = (change["case_id"], change["witness_id"])
        if set(change["proposed_root_fields"]) != proposed[key]:
            raise JudgeV5CalibrationV64ReferenceError("v63 proposal/output drifted")
    return {name: _record(path) for name, path in paths.items()}


def build_v64_reference(
    base_truth: Mapping[str, Any], proposal: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    patched = deepcopy(base_truth)
    operations = []
    for change in proposal["changes"]:
        case_id = change["case_id"]
        witness_id = change["witness_id"]
        case = patched["cases"].get(case_id)
        if case is None or witness_id not in case["field_issues"]:
            raise JudgeV5CalibrationV64ReferenceError("v64 patch identity is absent")
        old_fields = sorted(case["field_issues"][witness_id])
        if old_fields != change["old_root_fields"]:
            raise JudgeV5CalibrationV64ReferenceError("v64 old truth drifted")
        new_fields = sorted(change["proposed_root_fields"])
        case["field_issues"][witness_id] = new_fields
        case["structured_fields"][witness_id] = "incorrect" if new_fields else "correct"
        operations.append(
            {
                "case_id": case_id,
                "witness_id": witness_id,
                "old_root_fields": old_fields,
                "new_root_fields": new_fields,
                "add_fields": sorted(set(new_fields) - set(old_fields)),
                "remove_fields": sorted(set(old_fields) - set(new_fields)),
            }
        )
    patched["reference_version"] = V64_REFERENCE_VERSION
    patched["reference_frozen_at"] = now_iso()
    patched["structured_reference_source"] = "v63_side_free_sol_high_reference_owner_reaudit"
    patched["structured_reference_owner"] = "fresh_side_free_gpt_5_6_sol_adjudication"
    audit = {
        "schema_version": V64_AUDIT_VERSION,
        "created_at": now_iso(),
        "base_reference_version": base_truth.get("reference_version"),
        "patched_reference_version": V64_REFERENCE_VERSION,
        "operation_count": len(operations),
        "operations": operations,
        "semantic_model_calls": 0,
        "semantic_source": "v63_side_free_sol_high_reference_owner_output",
        "proposition_truth_changed": False,
        "alignment_truth_changed": False,
        "privacy": "opaque_ids_and_field_enums_only",
    }
    if len(operations) != 5:
        raise JudgeV5CalibrationV64ReferenceError("v64 patch count drifted")
    return patched, audit


def freeze_v64_reference(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v53_root: Path = DEFAULT_V53_ROOT,
    v63_root: Path = DEFAULT_V63_ROOT,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v64 terminal")
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors(v53_root.resolve(), v63_root.resolve())
    base_truth = _load_json(v53_root / "scoped-calibration-truth.private.json", "v53 truth")
    proposal = _load_json(v63_root / "reference-patch-proposal.json", "v63 proposal")
    truth, audit = build_v64_reference(base_truth, proposal)
    truth_path = root / "calibration-truth.private.json"
    audit_path = root / "reference-patch-audit.json"
    _write_immutable(truth_path, truth)
    _write_immutable(audit_path, audit)
    receipt = {
        "schema_version": V64_RECEIPT_VERSION,
        "created_at": now_iso(),
        "reference_version": V64_REFERENCE_VERSION,
        "reference_frozen": True,
        "case_count": len(truth["cases"]),
        "witness_count": sum(len(case["field_issues"]) for case in truth["cases"].values()),
        "truth": _record(truth_path),
        "patch_audit": _record(audit_path),
        "predecessor": predecessor,
        "fresh_diagnostic_required": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    receipt_path = root / "reference-receipt.json"
    _write_immutable(receipt_path, receipt)
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    terminal = {
        "schema_version": V64_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v64_reference_v2_frozen_fresh_diagnostic_required",
        "overall_evaluation_complete": False,
        "reference_frozen": True,
        "reference_version": V64_REFERENCE_VERSION,
        "fresh_diagnostic_required": True,
        "fresh_diagnostic_authorized": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "new_semantic_turn_count": 0,
        "new_usage": zero_usage,
        "usage_status": "complete",
        "accounting_complete": True,
        "truth": _record(truth_path),
        "patch_audit": _record(audit_path),
        "reference_receipt": _record(receipt_path),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v64 reference v2")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v53-root", default=str(DEFAULT_V53_ROOT))
    parser.add_argument("--v63-root", default=str(DEFAULT_V63_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v64_reference(
        output_dir=Path(args.output_dir),
        v53_root=Path(args.v53_root),
        v63_root=Path(args.v63_root),
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal["reference_frozen"],
                "new_semantic_turn_count": terminal["new_semantic_turn_count"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
