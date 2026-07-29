from __future__ import annotations

"""Freeze a rubric-consistent successor to the v45 diagnostic reference."""

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5_calibration_v26_diagnostic import (
    DEFAULT_PIPELINE_ROOT,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_diagnostic import _record, _sha256_file
from .util import now_iso


V47_AUDIT_VERSION = "pif_app_server_judge_v5_4_v47_reference_consistency_audit_v1"
V47_TRUTH_VERSION = "pif_app_server_judge_v5_4_v47_rubric_consistent_truth_v1"
V47_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v47_reference_freeze_receipt_v1"
V47_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v47_reference_freeze_terminal_v1"

DEFAULT_V45_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v45-reference-freeze-receipt"
).resolve()
DEFAULT_V46_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v46-full-development-diagnostic"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v47-reference-consistency-freeze"
).resolve()


class JudgeV5CalibrationV47ReferenceError(RuntimeError):
    """The v47 reference cannot be frozen without changing its declared contract."""


def _project_relation(fields: set[str]) -> str:
    if not fields:
        return "equivalent"
    if fields == {"event_boundary", "evidence"}:
        return "partial"
    return "non_equivalent"


def audit_reference_consistency(truth: Mapping[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    cases = truth.get("cases")
    if not isinstance(cases, Mapping):
        raise JudgeV5CalibrationV47ReferenceError("reference cases are malformed")
    for case_id, case in sorted(cases.items()):
        proposition = case.get("proposition")
        if not isinstance(proposition, Mapping):
            raise JudgeV5CalibrationV47ReferenceError("reference proposition map is malformed")
        group_by_witness: dict[str, int] = {}
        for index, group in enumerate(case.get("equivalence_groups") or []):
            for witness_id in group:
                if witness_id in group_by_witness:
                    raise JudgeV5CalibrationV47ReferenceError(
                        "reference equivalence groups overlap"
                    )
                group_by_witness[str(witness_id)] = index
        for pair in case.get("pairs") or []:
            witness_ids = sorted(str(value) for value in pair.get("witness_ids") or [])
            if len(witness_ids) != 2:
                raise JudgeV5CalibrationV47ReferenceError("reference pair is malformed")
            left, right = witness_ids
            verdicts = [str(proposition[left]), str(proposition[right])]
            fields = set(str(value) for value in pair.get("mismatch_fields") or [])
            expected_unsupported = (
                "abstain"
                if "abstain" in verdicts
                else "different"
                if set(verdicts) == {"supported", "unsupported"}
                else "same"
            )
            observed_unsupported = (
                "different" if "unsupported_inference" in fields else "same"
            )
            if expected_unsupported != observed_unsupported:
                issues.append(
                    {
                        "case_id": str(case_id),
                        "witness_ids": witness_ids,
                        "issue_type": "support_alignment_contradiction",
                        "proposition_verdicts": verdicts,
                        "required_unsupported_inference_decision": expected_unsupported,
                        "frozen_unsupported_inference_decision": observed_unsupported,
                        "frozen_relation": str(pair.get("relation")),
                        "frozen_mismatch_fields": sorted(fields),
                    }
                )
            projected_relation = _project_relation(fields)
            if str(pair.get("relation")) != projected_relation:
                issues.append(
                    {
                        "case_id": str(case_id),
                        "witness_ids": witness_ids,
                        "issue_type": "relation_precedence_contradiction",
                        "frozen_relation": str(pair.get("relation")),
                        "projected_relation": projected_relation,
                        "frozen_mismatch_fields": sorted(fields),
                    }
                )
            if ("event_boundary" in fields) != ("evidence" in fields):
                issues.append(
                    {
                        "case_id": str(case_id),
                        "witness_ids": witness_ids,
                        "issue_type": "boundary_evidence_cooccurrence_contradiction",
                        "frozen_mismatch_fields": sorted(fields),
                    }
                )
            same_group = group_by_witness.get(left) == group_by_witness.get(right)
            if (str(pair.get("relation")) == "equivalent") != same_group:
                issues.append(
                    {
                        "case_id": str(case_id),
                        "witness_ids": witness_ids,
                        "issue_type": "partition_relation_contradiction",
                        "frozen_relation": str(pair.get("relation")),
                        "same_equivalence_group": same_group,
                    }
                )
    return issues


def apply_reference_consistency_patch(
    truth: Mapping[str, Any], issues: Sequence[Mapping[str, Any]]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    patched = deepcopy(truth)
    operations: list[dict[str, Any]] = []
    for issue in issues:
        if issue.get("issue_type") != "support_alignment_contradiction":
            raise JudgeV5CalibrationV47ReferenceError(
                "v47 only projects frozen pointwise support into alignment truth"
            )
        if issue.get("required_unsupported_inference_decision") != "different":
            raise JudgeV5CalibrationV47ReferenceError(
                "v47 cannot deterministically project an abstaining support pair"
            )
        case_id = str(issue["case_id"])
        witness_ids = sorted(str(value) for value in issue["witness_ids"])
        case = patched["cases"][case_id]
        pair = next(
            (
                item
                for item in case["pairs"]
                if sorted(str(value) for value in item["witness_ids"]) == witness_ids
            ),
            None,
        )
        if pair is None:
            raise JudgeV5CalibrationV47ReferenceError("contradictory pair disappeared")
        before_fields = sorted(str(value) for value in pair.get("mismatch_fields") or [])
        after_fields = sorted(set(before_fields) | {"unsupported_inference"})
        before_relation = str(pair["relation"])
        after_relation = _project_relation(set(after_fields))
        pair["mismatch_fields"] = after_fields
        pair["relation"] = after_relation
        if before_relation == "equivalent" and after_relation != "equivalent":
            matching = [
                group
                for group in case["equivalence_groups"]
                if set(str(value) for value in group) == set(witness_ids)
            ]
            if len(matching) != 1:
                raise JudgeV5CalibrationV47ReferenceError(
                    "equivalent support contradiction is not an isolated pair"
                )
            case["equivalence_groups"].remove(matching[0])
            case["equivalence_groups"].extend([[value] for value in witness_ids])
            case["equivalence_groups"] = sorted(
                (sorted(group) for group in case["equivalence_groups"]),
                key=lambda values: tuple(values),
            )
        operations.append(
            {
                "case_id": case_id,
                "witness_ids": witness_ids,
                "operation": "project_frozen_support_verdict_difference",
                "before_relation": before_relation,
                "after_relation": after_relation,
                "before_mismatch_fields": before_fields,
                "after_mismatch_fields": after_fields,
            }
        )
    patched["schema_version"] = V47_TRUTH_VERSION
    patched["reference_freeze_version"] = "v47"
    patched["reference_frozen_at"] = now_iso()
    patched["reference_freeze_basis"] = (
        "deterministic_projection_of_frozen_pointwise_support_under_frozen_rubric"
    )
    patched["reference_freeze_authorized"] = True
    return patched, operations


def freeze_v47_reference_consistency(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v45_root: Path = DEFAULT_V45_ROOT,
    v46_root: Path = DEFAULT_V46_ROOT,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v47 terminal")
    root.mkdir(parents=True, exist_ok=True)
    v45_terminal_path = v45_root / "terminal.json"
    v45_receipt_path = v45_root / "reference-freeze-receipt.json"
    v45_truth_path = v45_root / "frozen-diagnostic-truth.private.json"
    v46_terminal_path = v46_root / "terminal.json"
    v46_score_path = v46_root / "diagnostic-score.json"
    v45_terminal = _load_json(v45_terminal_path, "v45 terminal")
    v45_receipt = _load_json(v45_receipt_path, "v45 reference receipt")
    v46_terminal = _load_json(v46_terminal_path, "v46 terminal")
    if (
        v45_terminal.get("state") != "completed"
        or v45_terminal.get("reference_freeze_authorized") is not True
        or v45_terminal.get("production_mutated") is not False
        or v45_receipt.get("reference_freeze_authorized") is not True
    ):
        raise JudgeV5CalibrationV47ReferenceError("v45 reference is not admissible")
    frozen_record = v45_receipt.get("frozen_truth")
    if (
        not isinstance(frozen_record, Mapping)
        or frozen_record.get("sha256") != _sha256_file(v45_truth_path)
        or frozen_record.get("size_bytes") != v45_truth_path.stat().st_size
    ):
        raise JudgeV5CalibrationV47ReferenceError("v45 reference receipt drifted")
    if (
        v46_terminal.get("state") != "inactive"
        or v46_terminal.get("development_terminal_reason")
        != "v46_full_development_diagnostic_quality_gate_not_passed"
        or v46_terminal.get("accounting_complete") is not True
        or v46_terminal.get("usage_status") != "complete"
        or v46_terminal.get("production_mutated") is not False
        or v46_terminal.get("full_calibration_authorized") is not False
    ):
        raise JudgeV5CalibrationV47ReferenceError("v46 quality failure is not admissible")
    score_record = v46_terminal.get("score")
    if (
        not isinstance(score_record, Mapping)
        or score_record.get("sha256") != _sha256_file(v46_score_path)
        or score_record.get("size_bytes") != v46_score_path.stat().st_size
    ):
        raise JudgeV5CalibrationV47ReferenceError("v46 score record drifted")
    source_truth = _load_json(v45_truth_path, "v45 frozen truth")
    issues = audit_reference_consistency(source_truth)
    if len(issues) != 1 or issues[0].get("issue_type") != "support_alignment_contradiction":
        raise JudgeV5CalibrationV47ReferenceError(
            "v47 expected exactly one frozen support/alignment contradiction"
        )
    patched_truth, operations = apply_reference_consistency_patch(source_truth, issues)
    remaining = audit_reference_consistency(patched_truth)
    if remaining:
        raise JudgeV5CalibrationV47ReferenceError("v47 patched reference remains inconsistent")
    audit = {
        "schema_version": V47_AUDIT_VERSION,
        "created_at": now_iso(),
        "source_truth": _record(v45_truth_path),
        "source_terminal": _record(v45_terminal_path),
        "triggering_quality_terminal": _record(v46_terminal_path),
        "triggering_quality_score": _record(v46_score_path),
        "contradiction_count": len(issues),
        "contradictions": issues,
        "patch_operations": operations,
        "remaining_contradiction_count": 0,
        "semantic_model_calls_performed": 0,
        "production_mutated": False,
        "privacy": "opaque_ids_and_enum_decisions_only_no_source_or_witness_text",
    }
    audit_path = root / "reference-consistency-audit.json"
    truth_path = root / "frozen-diagnostic-truth.private.json"
    _write_immutable(audit_path, audit)
    _write_immutable(truth_path, patched_truth)
    receipt = {
        "schema_version": V47_RECEIPT_VERSION,
        "created_at": now_iso(),
        "reference_freeze_authorized": True,
        "reference_version": "v47",
        "projection_contract": (
            "unsupported_inference_decision_is_different_exactly_when_frozen_"
            "proposition_verdicts_are_supported_vs_unsupported"
        ),
        "source_reference": _record(v45_receipt_path),
        "consistency_audit": _record(audit_path),
        "frozen_truth": _record(truth_path),
        "patch_operation_count": len(operations),
        "targeted_diagnostic_authorized": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    receipt_path = root / "reference-freeze-receipt.json"
    _write_immutable(receipt_path, receipt)
    terminal = {
        "schema_version": V47_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v47_reference_consistency_freeze_completed",
        "development_terminal_reason": (
            "v47_reference_consistency_freeze_completed_targeted_diagnostic_authorized"
        ),
        "reference_freeze_authorized": True,
        "targeted_diagnostic_authorized": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "accounting_complete": True,
        "usage_status": "not_applicable_no_semantic_turn_started",
        "reference_receipt": _record(receipt_path),
        "frozen_truth": _record(truth_path),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze the v47 consistent reference")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v45-root", default=str(DEFAULT_V45_ROOT))
    parser.add_argument("--v46-root", default=str(DEFAULT_V46_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v47_reference_consistency(
        output_dir=Path(args.output_dir),
        v45_root=Path(args.v45_root),
        v46_root=Path(args.v46_root),
    )
    print(json.dumps({
        "state": terminal["state"],
        "terminal_reason": terminal["terminal_reason"],
        "targeted_diagnostic_authorized": terminal["targeted_diagnostic_authorized"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
