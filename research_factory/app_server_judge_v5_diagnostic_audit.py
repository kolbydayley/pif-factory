from __future__ import annotations

"""Executable truth audit between pipeline-v5 diagnostic versions."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_llm_judge import write_immutable_json


DIAGNOSTIC_V1_TRUTH_AUDIT_VERSION = "pif_judge_v5_diagnostic_v1_truth_audit_v1"
DIAGNOSTIC_V1_TRUTH_AUDIT_RECEIPT_VERSION = (
    "pif_judge_v5_diagnostic_v1_truth_audit_receipt_v1"
)
DEFAULT_AUDIT_PATH = (
    Path(__file__).resolve().parent
    / "evaluation/judge_v5_diagnostic_v1_truth_audit.json"
)
CHECKLIST_FIELDS = (
    "actor",
    "attribution",
    "causal_mechanism",
    "certainty",
    "event_boundary",
    "event_type",
    "evidence",
    "metric",
    "negation",
    "reported_actor",
    "speaker",
    "stance",
    "target",
    "temporal_horizon",
    "unsupported_inference",
)


class DiagnosticTruthAuditError(ValueError):
    """The diagnostic truth/version receipt does not match frozen evidence."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, *, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DiagnosticTruthAuditError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise DiagnosticTruthAuditError(f"{purpose} is not an object")
    return value


def _file_record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise DiagnosticTruthAuditError("required diagnostic audit artifact is missing")
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _resolve_record(repo_root: Path, record: Mapping[str, Any]) -> Path:
    relative = record.get("path")
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise DiagnosticTruthAuditError("diagnostic audit record path is invalid")
    path = (repo_root / relative).resolve()
    try:
        path.relative_to(repo_root)
    except ValueError as exc:
        raise DiagnosticTruthAuditError("diagnostic audit record escapes repository") from exc
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise DiagnosticTruthAuditError("diagnostic audit source record drifted")
    return path


def _recompute_v1_errors(repo_root: Path) -> dict[str, Any]:
    diagnostic_root = (
        repo_root
        / "work/app-server-development-v2/unattended-pipeline-v5/judge-diagnostic-v1"
    )
    truth = _load_json(
        diagnostic_root / "diagnostic-truth.private.json",
        purpose="diagnostic-v1 private truth",
    )
    pointwise = _load_json(
        diagnostic_root / "turns/pointwise-support/output.private.json",
        purpose="diagnostic-v1 pointwise output",
    )
    disagreements = _load_json(
        diagnostic_root / "observable-disagreements.private.json",
        purpose="diagnostic-v1 disagreements",
    )
    rows = {
        str(item["witness_id"]): item for item in pointwise.get("units") or []
    }
    support_false_negatives = []
    support_false_positives = []
    structured_errors = []
    for case_id, case in (truth.get("cases") or {}).items():
        for witness_id, expected in case["proposition"].items():
            observed = rows[witness_id]["proposition_verdict"]
            if expected == "supported" and observed != "supported":
                support_false_negatives.append(
                    {"case_key": case["case_key"], "witness_id": witness_id}
                )
            if expected == "unsupported" and observed != "unsupported":
                support_false_positives.append(
                    {"case_key": case["case_key"], "witness_id": witness_id}
                )
        for witness_id, expected in case["structured_fields"].items():
            observed = rows[witness_id]["structured_field_verdict"]
            if observed != expected:
                structured_errors.append(
                    {
                        "case_key": case["case_key"],
                        "witness_id": witness_id,
                        "expected": expected,
                        "observed": observed,
                    }
                )
    return {
        "support_false_negative_count": len(support_false_negatives),
        "support_false_negative_case_keys": sorted(
            {item["case_key"] for item in support_false_negatives}
        ),
        "support_false_positive_count": len(support_false_positives),
        "structured_field_error_count": len(structured_errors),
        "structured_field_error_case_keys": sorted(
            {item["case_key"] for item in structured_errors}
        ),
        "canary_changed_case_count": disagreements.get("disagreement_case_count"),
    }


def _validate_fresh_v2_fixture(
    *, v1_fixture: Mapping[str, Any], v2_fixture: Mapping[str, Any]
) -> dict[str, Any]:
    v1_keys = {str(case["case_key"]) for case in v1_fixture.get("cases") or []}
    v2_cases = v2_fixture.get("cases")
    if (
        v2_fixture.get("schema_version") != "pif_judge_v5_diagnostic_fixture_v2"
        or v2_fixture.get("case_count") != 18
        or v2_fixture.get("predecessor_case_content_reused") is not False
        or not isinstance(v2_cases, list)
        or len(v2_cases) != 18
    ):
        raise DiagnosticTruthAuditError("diagnostic-v2 fixture envelope is invalid")
    v2_keys = {str(case["case_key"]) for case in v2_cases}
    if len(v2_keys) != 18 or v1_keys & v2_keys:
        raise DiagnosticTruthAuditError("diagnostic-v2 case keys are not fresh")
    focus = {str(case["focus_field"]) for case in v2_cases}
    if not set(CHECKLIST_FIELDS) <= focus:
        raise DiagnosticTruthAuditError("diagnostic-v2 does not cover all checklist fields")
    canary = v2_fixture.get("canary_case_keys")
    if (
        not isinstance(canary, list)
        or len(canary) != 6
        or len(set(canary)) != 6
        or not set(canary) <= v2_keys
    ):
        raise DiagnosticTruthAuditError("diagnostic-v2 canary is invalid")
    unsupported_cases = 0
    field_only_cases = 0
    partial_cases = 0
    for case in v2_cases:
        source = case.get("source_excerpt")
        expected = case.get("expected")
        if (
            not isinstance(source, str)
            or not source
            or not isinstance(expected, dict)
            or case.get("event_a", {}).get("evidence") not in source
            or case.get("event_b", {}).get("evidence") not in source
        ):
            raise DiagnosticTruthAuditError("diagnostic-v2 exact evidence truth is invalid")
        prop_b = expected.get("proposition_b")
        structured_b = expected.get("structured_b")
        issues = set(expected.get("field_issues_b") or [])
        mismatches = set(expected.get("mismatch_fields") or [])
        if prop_b == "unsupported":
            unsupported_cases += 1
            if (
                structured_b != "incorrect"
                or "unsupported_inference" not in issues
                or "unsupported_inference" not in mismatches
                or len(mismatches - {"unsupported_inference"}) < 1
            ):
                raise DiagnosticTruthAuditError(
                    "diagnostic-v2 unsupported assertion precedence is invalid"
                )
        elif prop_b == "supported" and structured_b == "incorrect":
            field_only_cases += 1
            if "unsupported_inference" in issues or "unsupported_inference" in mismatches:
                raise DiagnosticTruthAuditError(
                    "diagnostic-v2 field-only mutation is overlabelled"
                )
        if expected.get("relation") == "partial":
            partial_cases += 1
            if (
                expected.get("structured_a") != "correct"
                or structured_b != "correct"
                or mismatches != {"event_boundary", "evidence"}
            ):
                raise DiagnosticTruthAuditError(
                    "diagnostic-v2 merge/split truth is not isolated"
                )
    exact = next(
        case for case in v2_cases if case["case_key"] == "diag2_exact_evidence_not_support"
    )
    if set(exact["expected"]["field_issues_b"]) != {
        "certainty",
        "target",
        "unsupported_inference",
    }:
        raise DiagnosticTruthAuditError("diagnostic-v2 exact-evidence truth drifted")
    return {
        "case_count": len(v2_cases),
        "fresh_case_key_count": len(v2_keys),
        "v1_case_key_overlap_count": len(v1_keys & v2_keys),
        "unsupported_claim_case_count": unsupported_cases,
        "field_only_mutation_case_count": field_only_cases,
        "merge_split_only_case_count": partial_cases,
        "canary_case_count": len(canary),
        "all_checklist_fields_covered": True,
    }


def build_diagnostic_v1_truth_audit_receipt(
    *, repo_root: Path, output_path: Path, audit_path: Path = DEFAULT_AUDIT_PATH
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    audit_file = audit_path.expanduser().resolve()
    audit = _load_json(audit_file, purpose="diagnostic-v1 truth audit")
    if audit.get("schema_version") != DIAGNOSTIC_V1_TRUTH_AUDIT_VERSION:
        raise DiagnosticTruthAuditError("unsupported diagnostic-v1 truth audit")
    resolved = {
        name: _resolve_record(root, record)
        for name, record in (audit.get("source_records") or {}).items()
    }
    if set(resolved) != {
        "diagnostic_v1_fixture",
        "diagnostic_v1_terminal",
        "diagnostic_v1_score",
        "diagnostic_v1_terminal_receipt",
        "diagnostic_v2_fresh_fixture",
    }:
        raise DiagnosticTruthAuditError("diagnostic truth audit source coverage drifted")
    terminal = _load_json(resolved["diagnostic_v1_terminal"], purpose="v1 terminal")
    score = _load_json(resolved["diagnostic_v1_score"], purpose="v1 score")
    measured = audit.get("diagnostic_v1_measured_result") or {}
    if (
        terminal.get("terminal_reason")
        != "judge_diagnostic_quality_gate_not_passed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage", {}).get("total_tokens") != 138211
        or score.get("passed") is not False
        or score.get("metrics", {}).get("support_sensitivity") != 0.724138
        or score.get("metrics", {}).get("support_specificity") != 1.0
        or score.get("metrics", {}).get("structured_field_accuracy") != 0.944444
        or score.get("metrics", {}).get("field_diagnostic_f1") != 0.921053
        or score.get("metrics", {}).get("order_bias") != 0.666667
        or measured.get("full_calibration_authorized") is not False
    ):
        raise DiagnosticTruthAuditError("diagnostic-v1 measured result drifted")
    recomputed = _recompute_v1_errors(root)
    if recomputed != {
        "support_false_negative_count": 8,
        "support_false_negative_case_keys": [
            "diag_attribution",
            "diag_event_boundary",
            "diag_evidence",
            "diag_reported_actor",
            "diag_speaker",
        ],
        "support_false_positive_count": 0,
        "structured_field_error_count": 2,
        "structured_field_error_case_keys": ["diag_event_boundary", "diag_evidence"],
        "canary_changed_case_count": 4,
    }:
        raise DiagnosticTruthAuditError("diagnostic-v1 error forensics drifted")
    v1_fixture = _load_json(resolved["diagnostic_v1_fixture"], purpose="v1 fixture")
    v2_fixture = _load_json(
        resolved["diagnostic_v2_fresh_fixture"], purpose="v2 fixture"
    )
    v2_validation = _validate_fresh_v2_fixture(
        v1_fixture=v1_fixture, v2_fixture=v2_fixture
    )
    admissibility = audit.get("admissibility") or {}
    if (
        admissibility.get(
            "diagnostic_v1_remains_immutable_intent_to_treat_evidence"
        )
        is not True
        or admissibility.get(
            "diagnostic_v1_is_admissible_as_final_judge_quality_evidence"
        )
        is not False
        or admissibility.get("diagnostic_v1_semantic_turns_may_be_replayed")
        is not False
        or admissibility.get("full_calibration_remains_unauthorized") is not True
    ):
        raise DiagnosticTruthAuditError("diagnostic-v1 admissibility contract drifted")
    receipt = {
        "schema_version": DIAGNOSTIC_V1_TRUTH_AUDIT_RECEIPT_VERSION,
        "audit": _file_record(audit_file),
        "verified_source_records": {
            name: _file_record(path) for name, path in resolved.items()
        },
        "recomputed_v1_errors": recomputed,
        "diagnostic_v2_fixture_validation": v2_validation,
        "diagnostic_v1_replay_allowed": False,
        "diagnostic_v1_outputs_admissible_as_passing_evidence": False,
        "diagnostic_v2_requires_new_immutable_root": True,
        "full_calibration_authorized": False,
        "semantic_model_calls_performed": 0,
        "privacy": "hashes_case_keys_counts_metrics_and_classifications_only",
    }
    write_immutable_json(output_path.expanduser().resolve(), receipt)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit diagnostic-v1 truth for v2")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", required=True)
    parser.add_argument("--audit", default=str(DEFAULT_AUDIT_PATH))
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    receipt = build_diagnostic_v1_truth_audit_receipt(
        repo_root=Path(args.repo_root),
        output_path=Path(args.output),
        audit_path=Path(args.audit),
    )
    print(
        json.dumps(
            {
                "ok": True,
                "schema_version": receipt["schema_version"],
                "v1_support_false_negatives": receipt["recomputed_v1_errors"][
                    "support_false_negative_count"
                ],
                "v2_case_count": receipt["diagnostic_v2_fixture_validation"][
                    "case_count"
                ],
                "semantic_model_calls": receipt["semantic_model_calls_performed"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
