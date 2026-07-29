from __future__ import annotations

"""Project the audited 18-case v118 reference into the frozen 66-case fixture."""

import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

from .app_server_judge_v5_calibration import (
    CALIBRATION_TRUTH_VERSION,
    make_v5_calibration_pool,
    validate_v5_calibration_truth,
)
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v101_reference_v8_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V101_ROOT,
)
from .app_server_judge_v5_calibration_v118_capped_alignment_repair import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V118_ROOT,
    _iter_records,
)
from .util import now_iso


V119_REFERENCE_VERSION = "pif_app_server_judge_v5_4_calibration_truth_v9_full_frozen"
V119_AUDIT_VERSION = "pif_app_server_judge_v5_4_v119_full_reference_projection_audit_v1"
V119_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v119_full_reference_receipt_v1"
V119_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v119_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V118_ROOT.parent / "judge-calibration-v5_4-v119-full-reference-v9-freeze"
).resolve()


class JudgeV5CalibrationV119Error(RuntimeError):
    """The v119 full-reference projection cannot preserve its evidence contract."""


def _validate_sources() -> dict[str, Any]:
    paths = {
        "v118_terminal": DEFAULT_V118_ROOT / "terminal.json",
        "v118_spec": DEFAULT_V118_ROOT / "capped-alignment-spec.json",
        "v118_score": DEFAULT_V118_ROOT / "capped-alignment-score.json",
        "v118_reference": DEFAULT_V118_ROOT / "fixture-reference-v9-frozen.private.json",
        "v101_terminal": DEFAULT_V101_ROOT / "terminal.json",
        "v101_receipt": DEFAULT_V101_ROOT / "reference-receipt.json",
        "v101_truth": DEFAULT_V101_ROOT / "calibration-truth-v8.private.json",
        "v101_audit": DEFAULT_V101_ROOT / "reference-patch-audit.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t118, s118, sp118, r118 = (
        values["v118_terminal"],
        values["v118_score"],
        values["v118_spec"],
        values["v118_reference"],
    )
    t101, rec101 = values["v101_terminal"], values["v101_receipt"]
    if (
        t118.get("state") != "completed"
        or t118.get("terminal_reason")
        != "v118_capped_alignment_repair_passed_reference_frozen"
        or t118.get("reference_frozen") is not True
        or t118.get("fresh_full_calibration_authorized") is not True
        or t118.get("selection_authorized") is not False
        or t118.get("holdout_authorized") is not False
        or t118.get("production_mutated") is not False
        or t118.get("usage_status") != "complete"
        or t118.get("accounting_complete") is not True
        or t118.get("usage", {}).get("total_tokens") != 42316
        or s118.get("passed") is not True
        or s118.get("failed_checks") != []
        or sp118.get("retry_count_per_turn") != 0
        or sp118.get("holdout_authorized") is not False
        or sp118.get("production_mutation_allowed") is not False
        or r118.get("reference_frozen") is not True
        or r118.get("alignment_reference_frozen") is not True
        or r118.get("pointwise_reference_patch_authorized") is not True
        or r118.get("fresh_full_calibration_authorized") is not True
        or len(r118.get("cases") or {}) != 18
        or t101.get("state") != "completed"
        or t101.get("terminal_reason")
        != "v101_reference_v8_frozen_fresh_diagnostic_authorized"
        or t101.get("reference_frozen") is not True
        or t101.get("production_mutated") is not False
        or rec101.get("state") != "frozen"
        or rec101.get("case_count") != 66
        or rec101.get("witness_count") != 182
        or rec101.get("reference_frozen") is not True
        or rec101.get("selection_authorized") is not False
        or rec101.get("holdout_authorized") is not False
        or rec101.get("production_mutated") is not False
        or len(values["v101_truth"].get("cases") or {}) != 66
    ):
        raise JudgeV5CalibrationV119Error("v101/v118 reference source drifted")
    if not all(_verify_record(record) for record in _iter_records(sp118)):
        raise JudgeV5CalibrationV119Error("v118 frozen record drifted")
    for record in (
        t118.get("reference"),
        t118.get("score"),
        t101.get("truth"),
        t101.get("patch_audit"),
        t101.get("reference_receipt"),
        rec101.get("truth"),
        rec101.get("patch_audit"),
    ):
        if not isinstance(record, Mapping) or not _verify_record(record):
            raise JudgeV5CalibrationV119Error("reference receipt record drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def build_full_reference(
    full_v8: Mapping[str, Any], audited_v9_subset: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    full = deepcopy(full_v8)
    subset_cases = audited_v9_subset["cases"]
    if len(full.get("cases") or {}) != 66 or len(subset_cases) != 18:
        raise JudgeV5CalibrationV119Error("v119 source coverage drifted")
    if not set(subset_cases).issubset(full["cases"]):
        raise JudgeV5CalibrationV119Error("v119 subset contains unknown cases")
    retained_hashes = {
        case_id: json.dumps(case, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        for case_id, case in full["cases"].items()
        if case_id not in subset_cases
    }
    field_changes: Counter[str] = Counter()
    alignment_component_changes = 0
    changed_cases = 0
    for case_id, new_case in subset_cases.items():
        old_case = full["cases"][case_id]
        changed = False
        for witness_id in new_case["proposition"]:
            if old_case["proposition"][witness_id] != new_case["proposition"][witness_id]:
                field_changes["proposition"] += 1
                changed = True
            if (
                old_case["structured_fields"][witness_id]
                != new_case["structured_fields"][witness_id]
            ):
                field_changes["structured_fields"] += 1
                changed = True
            for field in set(old_case["field_issues"][witness_id]) ^ set(
                new_case["field_issues"][witness_id]
            ):
                field_changes[field] += 1
                changed = True
        for key in ("pairs", "equivalence_groups", "unpaired_witness_ids"):
            if old_case[key] != new_case[key]:
                alignment_component_changes += 1
                changed = True
        full["cases"][case_id] = deepcopy(new_case)
        changed_cases += int(changed)
    if any(
        json.dumps(full["cases"][case_id], ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        != serialized
        for case_id, serialized in retained_hashes.items()
    ):
        raise JudgeV5CalibrationV119Error("v119 changed an unaudited case")
    full["schema_version"] = CALIBRATION_TRUTH_VERSION
    full["fixture_reference_schema_version"] = V119_REFERENCE_VERSION
    full["reference_version"] = "fixture_reference_v9_full_pointwise_alignment_owner_frozen"
    full["reference_frozen"] = True
    full["audited_subset_case_count"] = 18
    full["retained_v8_case_count"] = 48
    full["fresh_full_calibration_authorized"] = True
    pool, mapping, _ = make_v5_calibration_pool()
    validate_v5_calibration_truth(pool=pool, mapping=mapping, expected=full)
    witness_count = sum(len(case["proposition"]) for case in full["cases"].values())
    if witness_count != 182 or len(full.get("canary_case_ids") or []) != 12:
        raise JudgeV5CalibrationV119Error("v119 full fixture invariant drifted")
    audit = {
        "schema_version": V119_AUDIT_VERSION,
        "created_at": now_iso(),
        "case_count": 66,
        "witness_count": 182,
        "canary_case_count": 12,
        "audited_subset_case_count": 18,
        "retained_v8_case_count": 48,
        "changed_case_count": changed_cases,
        "pointwise_change_counts": dict(sorted(field_changes.items())),
        "alignment_component_change_count": alignment_component_changes,
        "semantic_decisions_by_deterministic_code": False,
        "operation": "exact_case_id_projection_of_llm_owned_reference_decisions",
        "production_mutated": False,
    }
    return full, audit


def freeze_v119(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v119 terminal")
    sources = _validate_sources()
    reference, audit = build_full_reference(
        sources["values"]["v101_truth"], sources["values"]["v118_reference"]
    )
    truth_path = root / "calibration-truth-v9-full.private.json"
    audit_path = root / "full-reference-projection-audit.json"
    _write_immutable(truth_path, reference)
    _write_immutable(audit_path, audit)
    receipt = {
        "schema_version": V119_RECEIPT_VERSION,
        "created_at": now_iso(),
        "state": "frozen",
        "reference_version": reference["reference_version"],
        "case_count": 66,
        "witness_count": 182,
        "canary_case_count": 12,
        "audited_subset_case_count": 18,
        "retained_v8_case_count": 48,
        "truth": _record(truth_path),
        "projection_audit": _record(audit_path),
        "predecessor": sources["records"],
        "reference_frozen": True,
        "fresh_full_calibration_authorized": True,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "new_semantic_turn_count": 0,
        "usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "total_tokens": 0,
        },
    }
    receipt_path = root / "reference-receipt.json"
    _write_immutable(receipt_path, receipt)
    terminal = {
        "schema_version": V119_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v119_full_reference_v9_frozen_full_calibration_authorized",
        "overall_evaluation_complete": False,
        "reference_frozen": True,
        "fresh_full_calibration_authorized": True,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": receipt["usage"],
        "truth": _record(truth_path),
        "projection_audit": _record(audit_path),
        "reference_receipt": _record(receipt_path),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main() -> int:
    terminal = freeze_v119()
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal["reference_frozen"],
                "fresh_full_calibration_authorized": terminal[
                    "fresh_full_calibration_authorized"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
