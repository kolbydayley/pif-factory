from __future__ import annotations

"""Project the v52 reference back onto its frozen pointwise-only contract."""

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v52_reference import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V52_ROOT,
)
from .app_server_judge_v5_diagnostic import _record, _sha256_file
from .util import now_iso


V53_AUDIT_VERSION = "pif_app_server_judge_v5_4_v53_reference_scope_audit_v1"
V53_TRUTH_VERSION = "pif_app_server_judge_v5_4_v53_scoped_reference_v1"
V53_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v53_reference_receipt_v1"
V53_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v53_reference_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V52_ROOT.parent / "judge-calibration-v5_4-v53-reference-scope-projection"
).resolve()


class JudgeV5CalibrationV53ReferenceError(RuntimeError):
    """The v52 output cannot be projected without a semantic decision."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def project_v52_pointwise_scope(
    *,
    truth: Mapping[str, Any],
    semantic_output: Mapping[str, Any],
    semantic_input: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    projected = deepcopy(truth)
    input_by_id = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in semantic_input.get("units") or []
    }
    operations = []
    for row in semantic_output.get("units") or []:
        case_id = str(row["case_id"])
        witness_id = str(row["witness_id"])
        fields = list(projected["cases"][case_id]["field_issues"][witness_id])
        out_of_scope = sorted(set(fields) & {"event_boundary", "evidence"})
        if not out_of_scope:
            continue
        if out_of_scope != ["event_boundary", "evidence"]:
            raise JudgeV5CalibrationV53ReferenceError("boundary/evidence scope pair drifted")
        unit = input_by_id.get((case_id, witness_id))
        event = (unit or {}).get("structured_event") or {}
        source = str((unit or {}).get("source_excerpt") or "")
        evidence = event.get("evidence")
        proposition = projected["cases"][case_id]["proposition"][witness_id]
        if (
            not isinstance(evidence, str)
            or not evidence
            or evidence not in source
            or proposition != "supported"
        ):
            raise JudgeV5CalibrationV53ReferenceError(
                "scope projection would require semantic evidence adjudication"
            )
        after = [field for field in fields if field not in {"event_boundary", "evidence"}]
        if not after:
            raise JudgeV5CalibrationV53ReferenceError("scope projection erased all issues")
        projected["cases"][case_id]["field_issues"][witness_id] = after
        operations.append(
            {
                "case_id": case_id,
                "witness_id": witness_id,
                "removed_fields": out_of_scope,
                "remaining_fields": after,
                "basis": (
                    "event_boundary_is_alignment_only_and_exact_evidence_supports_"
                    "the_frozen_supported_proposition"
                ),
            }
        )
    if len(operations) != 4:
        raise JudgeV5CalibrationV53ReferenceError("expected four scope violations")
    projected["reference_version"] = V53_TRUTH_VERSION
    projected["reference_frozen_at"] = now_iso()
    projected["structured_reference_source"] = "v52_with_pointwise_scope_projection"
    return projected, operations


def freeze_v53_reference(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, v52_root: Path = DEFAULT_V52_ROOT
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v53 terminal")
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "v52_terminal": v52_root / "terminal.json",
        "v52_receipt": v52_root / "reference-receipt.json",
        "v52_truth": v52_root / "adjudicated-calibration-truth.private.json",
        "v52_output": v52_root / "turns/structured-reference-adjudication/output.private.json",
        "v52_input": v52_root / "turns/structured-reference-adjudication/input.private.json",
        "v52_sidecar": v52_root / "turns/structured-reference-adjudication/sidecar.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v52_terminal"]
    if (
        terminal.get("state") != "completed"
        or terminal.get("reference_frozen") is not True
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("production_mutated") is not False
        or not _record_matches(terminal.get("reference_receipt"), paths["v52_receipt"])
        or not _record_matches(terminal.get("adjudicated_truth"), paths["v52_truth"])
    ):
        raise JudgeV5CalibrationV53ReferenceError("v52 predecessor is not admissible")
    projected, operations = project_v52_pointwise_scope(
        truth=values["v52_truth"],
        semantic_output=values["v52_output"],
        semantic_input=values["v52_input"],
    )
    truth_path = root / "scoped-calibration-truth.private.json"
    audit_path = root / "reference-scope-audit.json"
    _write_immutable(truth_path, projected)
    audit = {
        "schema_version": V53_AUDIT_VERSION,
        "created_at": now_iso(),
        "v52_terminal": _record(paths["v52_terminal"]),
        "v52_receipt": _record(paths["v52_receipt"]),
        "v52_truth": _record(paths["v52_truth"]),
        "v52_semantic_output": _record(paths["v52_output"]),
        "v52_semantic_sidecar": _record(paths["v52_sidecar"]),
        "operation_count": len(operations),
        "operations": operations,
        "semantic_model_calls_performed": 0,
        "semantic_decisions_added": 0,
        "production_mutated": False,
        "privacy": "opaque_ids_and_enum_fields_only_no_source_or_event_text",
    }
    _write_immutable(audit_path, audit)
    receipt = {
        "schema_version": V53_RECEIPT_VERSION,
        "created_at": now_iso(),
        "reference_version": V53_TRUTH_VERSION,
        "reference_frozen": True,
        "scope_audit": _record(audit_path),
        "scoped_truth": _record(truth_path),
        "v52_reference_superseded_for_pointwise_scope": True,
        "fresh_diagnostic_required": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    receipt_path = root / "reference-receipt.json"
    _write_immutable(receipt_path, receipt)
    result = {
        "schema_version": V53_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v53_scoped_reference_frozen_fresh_diagnostic_required",
        "overall_evaluation_complete": False,
        "reference_frozen": True,
        "fresh_diagnostic_required": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "accounting_complete": True,
        "usage_status": "not_applicable_no_semantic_turn_started",
        "usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "total_tokens": 0,
        },
        "reference_receipt": _record(receipt_path),
        "scoped_truth": _record(truth_path),
        "scope_audit": _record(audit_path),
    }
    _write_immutable(terminal_path, result)
    return result


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Project v52 onto pointwise scope")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v52-root", default=str(DEFAULT_V52_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v53_reference(
        output_dir=Path(args.output_dir), v52_root=Path(args.v52_root)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal["reference_frozen"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
