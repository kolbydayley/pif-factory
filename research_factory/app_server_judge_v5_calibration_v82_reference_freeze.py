from __future__ import annotations

"""Deterministically freeze the v81-authorized full calibration reference."""

import argparse
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5 import CHECKLIST_FIELDS
from .app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V75_ROOT,
)
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V78_ROOT,
    V23_ROOT,
)
from .app_server_judge_v5_calibration_v81_capped_disagreement import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V81_ROOT,
)
from .app_server_judge_v5_diagnostic import _record, _sha256_file
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .util import now_iso


V82_TRUTH_VERSION = "pif_app_server_judge_v5_4_reference_v3"
V82_AUDIT_VERSION = "pif_app_server_judge_v5_4_v82_reference_patch_audit_v1"
V82_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v82_reference_receipt_v1"
V82_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v82_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V81_ROOT.parent / "judge-calibration-v5_4-v82-reference-v3-freeze"
).resolve()


class JudgeV5CalibrationV82Error(RuntimeError):
    """The v82 reference freeze cannot preserve its authorized evidence."""


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


def _validate_predecessors(
    *, v81_root: Path, v75_root: Path, v78_root: Path, v23_root: Path
) -> dict[str, Any]:
    paths = {
        "v81_terminal": v81_root / "terminal.json",
        "v81_spec": v81_root / "capped-adjudication-spec.json",
        "v81_score": v81_root / "capped-adjudication-score.json",
        "v81_output": v81_root / "capped-adjudication-output.private.json",
        "v81_reconciliation": v81_root / "reference-reconciliation.private.json",
        "v81_projection": v81_root / "projection-audit.json",
        "v75_spec": v75_root / "exact-span-remaining-shard-spec.json",
        "v75_truth": v75_root / "selected-truth.private.json",
        "v78_spec": v78_root / "fresh-reconcile-spec.json",
        "v78_truth": v78_root / "fresh-truth.private.json",
        "v23_spec": v23_root / "calibration-spec.json",
        "v23_truth": v23_root / "calibration-truth.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v81_terminal"]
    spec = values["v81_spec"]
    score = values["v81_score"]
    reconciliation = values["v81_reconciliation"]
    v75_spec = values["v75_spec"]
    v78_spec = values["v78_spec"]
    v23_spec = values["v23_spec"]
    if (
        terminal.get("state") != "completed"
        or terminal.get("capped_adjudication_passed") is not True
        or terminal.get("reference_freeze_authorized") is not True
        or terminal.get("full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("semantic_retry_count") != 0
        or len(terminal.get("attempts") or []) != 1
        or not all(
            _verify_record(record)
            for attempt in terminal.get("attempts") or []
            for record in (attempt.get("capacity"), attempt.get("sidecar"), attempt.get("output"))
        )
        or not _record_matches(terminal.get("score"), paths["v81_score"])
        or not _record_matches(terminal.get("output"), paths["v81_output"])
        or not _record_matches(terminal.get("reconciliation"), paths["v81_reconciliation"])
        or not _record_matches(terminal.get("projection_audit"), paths["v81_projection"])
        or score.get("passed") is not True
        or score.get("reference_freeze_authorized") is not True
        or score.get("failed_checks") != []
        or reconciliation.get("state") != "authorized"
        or reconciliation.get("row_count") != 10
        or reconciliation.get("reference_change_count") != 8
        or reconciliation.get("reference_freeze_authorized") is not True
        or reconciliation.get("majority_voting_used") is not False
        or spec.get("model") != "gpt-5.4"
        or spec.get("majority_voting_used") is not False
        or not all(_verify_record(row) for row in spec.get("runtime_files") or [])
        or not _record_matches(v75_spec.get("frozen_inputs", {}).get("selected_truth"), paths["v75_truth"])
        or not _record_matches(v78_spec.get("frozen_inputs", {}).get("truth"), paths["v78_truth"])
        or not _record_matches(v23_spec.get("frozen_inputs", {}).get("truth"), paths["v23_truth"])
        or v23_spec.get("case_count") != 66
        or v23_spec.get("witness_count") != 182
    ):
        raise JudgeV5CalibrationV82Error("v81/v75/v78/v23 predecessor contract drifted")
    return {name: _record(path) for name, path in paths.items()}


def _task_locations(
    v75_truth: Mapping[str, Any], v78_truth: Mapping[str, Any]
) -> dict[tuple[str, str], dict[str, Any]]:
    locations = {}
    for source_version, truth in (("v75_v77", v75_truth), ("v78", v78_truth)):
        for row in truth.get("tasks") or []:
            key = (source_version, row["task_id"])
            if key in locations:
                raise JudgeV5CalibrationV82Error("duplicate reference task identity")
            locations[key] = {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "field": row["field"],
                "expected_status": row["expected_status"],
            }
    return locations


def build_v82_reference(
    *,
    base_truth: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    v75_truth: Mapping[str, Any],
    v78_truth: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if reconciliation.get("state") != "authorized" or reconciliation.get("row_count") != 10:
        raise JudgeV5CalibrationV82Error("reconciliation is not authorized")
    locations = _task_locations(v75_truth, v78_truth)
    reference = deepcopy(base_truth)
    reference["schema_version"] = V82_TRUTH_VERSION
    reference["reference_version"] = "fixture_reference_v3_v82_llm_only_reconciled"
    operations = []
    for row in reconciliation["rows"]:
        key = (row["source_version"], row["original_task_id"])
        location = locations.get(key)
        if location is None or location["field"] != row["field"]:
            raise JudgeV5CalibrationV82Error("reconciliation task location drifted")
        case = reference["cases"].get(location["case_id"])
        if case is None or location["witness_id"] not in case["field_issues"]:
            raise JudgeV5CalibrationV82Error("reconciliation witness is absent")
        fields = list(case["field_issues"][location["witness_id"]])
        base_status = "incorrect" if row["field"] in fields else "correct"
        if row["prior_status"] != location["expected_status"]:
            raise JudgeV5CalibrationV82Error("reconciliation selected-truth status drifted")
        final_status = row["final_status"]
        if final_status not in {"correct", "incorrect"}:
            raise JudgeV5CalibrationV82Error("reconciliation final status is invalid")
        if final_status == "incorrect" and row["field"] not in fields:
            fields.append(row["field"])
        if final_status == "correct" and row["field"] in fields:
            fields.remove(row["field"])
        ordered = [field for field in CHECKLIST_FIELDS if field in set(fields)]
        if len(ordered) != len(set(fields)):
            raise JudgeV5CalibrationV82Error("reference field issue is outside the frozen checklist")
        case["field_issues"][location["witness_id"]] = ordered
        operations.append(
            {
                "case_id": location["case_id"],
                "witness_id": location["witness_id"],
                "field": row["field"],
                "selected_prior_status": row["prior_status"],
                "full_reference_base_status": base_status,
                "final_status": final_status,
                "adjudication_changed": row["prior_status"] != final_status,
                "reference_changed": base_status != final_status,
                "basis": row["basis"],
            }
        )
    if (
        len(operations) != 10
        or sum(row["adjudication_changed"] for row in operations) != 8
        or sum(row["reference_changed"] for row in operations) != 3
    ):
        raise JudgeV5CalibrationV82Error("v82 reference operation count drifted")
    if len(reference.get("cases") or {}) != 66:
        raise JudgeV5CalibrationV82Error("v82 reference case coverage drifted")
    witness_count = sum(len(case["field_issues"]) for case in reference["cases"].values())
    if witness_count != 182:
        raise JudgeV5CalibrationV82Error("v82 reference witness coverage drifted")
    audit = {
        "schema_version": V82_AUDIT_VERSION,
        "created_at": now_iso(),
        "operation_count": 10,
        "adjudication_change_count": 8,
        "full_reference_change_count": 3,
        "field_change_counts": dict(
            sorted(Counter(row["field"] for row in operations if row["reference_changed"]).items())
        ),
        "already_present_in_full_reference_count": 5,
        "operations": operations,
        "semantic_decisions_from_llms_only": True,
        "deterministic_code_scope": "identity_mapping_and_field_set_projection_only",
        "majority_voting_used": False,
        "privacy": "opaque_ids_fields_statuses_and_counts_only",
    }
    return reference, audit


def freeze_v82(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v81_root: Path = DEFAULT_V81_ROOT,
    v75_root: Path = DEFAULT_V75_ROOT,
    v78_root: Path = DEFAULT_V78_ROOT,
    v23_root: Path = V23_ROOT,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v82 terminal")
    predecessor = _validate_predecessors(
        v81_root=v81_root.resolve(),
        v75_root=v75_root.resolve(),
        v78_root=v78_root.resolve(),
        v23_root=v23_root.resolve(),
    )
    reference, audit = build_v82_reference(
        base_truth=_load_json(v23_root / "calibration-truth.private.json", "v23 truth"),
        reconciliation=_load_json(
            v81_root / "reference-reconciliation.private.json", "v81 reconciliation"
        ),
        v75_truth=_load_json(v75_root / "selected-truth.private.json", "v75 truth"),
        v78_truth=_load_json(v78_root / "fresh-truth.private.json", "v78 truth"),
    )
    reference_path = root / "calibration-truth-v3.private.json"
    audit_path = root / "reference-patch-audit.json"
    _write_immutable(reference_path, reference)
    _write_immutable(audit_path, audit)
    receipt = {
        "schema_version": V82_RECEIPT_VERSION,
        "created_at": now_iso(),
        "state": "frozen",
        "reference_version": reference["reference_version"],
        "case_count": 66,
        "witness_count": 182,
        "operation_count": 10,
        "adjudication_change_count": 8,
        "full_reference_change_count": 3,
        "already_present_in_full_reference_count": 5,
        "truth": _record(reference_path),
        "patch_audit": _record(audit_path),
        "predecessor": predecessor,
        "reference_frozen": True,
        "fresh_primary_repair_diagnostic_authorized": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "new_semantic_turn_count": 0,
    }
    receipt_path = root / "reference-receipt.json"
    _write_immutable(receipt_path, receipt)
    terminal = {
        "schema_version": V82_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v82_reference_v3_frozen_fresh_primary_repair_diagnostic_authorized",
        "overall_evaluation_complete": False,
        "reference_frozen": True,
        "fresh_primary_repair_diagnostic_authorized": True,
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
        "truth": _record(reference_path),
        "patch_audit": _record(audit_path),
        "reference_receipt": _record(receipt_path),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v82 full reference v3")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v82(output_dir=Path(args.output_dir))
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
