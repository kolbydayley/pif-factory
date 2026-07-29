from __future__ import annotations

"""Executable audit of the diagnostic-v3 order-only quality failure."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_llm_judge import write_immutable_json


DIAGNOSTIC_V3_QUALITY_AUDIT_VERSION = "pif_judge_v5_diagnostic_v3_quality_audit_v1"
DIAGNOSTIC_V3_QUALITY_AUDIT_RECEIPT_VERSION = (
    "pif_judge_v5_diagnostic_v3_quality_audit_receipt_v1"
)
DEFAULT_AUDIT_PATH = (
    Path(__file__).resolve().parent
    / "evaluation/judge_v5_diagnostic_v3_quality_audit.json"
)


class DiagnosticV3QualityAuditError(ValueError):
    """The diagnostic-v3 quality evidence or v4 recovery rule drifted."""


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
        raise DiagnosticV3QualityAuditError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise DiagnosticV3QualityAuditError(f"{purpose} is not an object")
    return value


def _resolve_record(repo_root: Path, record: Mapping[str, Any]) -> Path:
    relative = record.get("path")
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise DiagnosticV3QualityAuditError("v3 audit record path is invalid")
    path = (repo_root / relative).resolve()
    try:
        path.relative_to(repo_root)
    except ValueError as exc:
        raise DiagnosticV3QualityAuditError("v3 audit record escapes repository") from exc
    if not path.is_file() or record.get("sha256") != _sha256_file(path):
        raise DiagnosticV3QualityAuditError("v3 audit source record drifted")
    return path


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _unsupported_decision(case: Mapping[str, Any]) -> str:
    pair = (case.get("alignment_pairs") or [None])[0]
    if not isinstance(pair, dict):
        raise DiagnosticV3QualityAuditError("v3 alignment pair is missing")
    row = next(
        (
            item
            for item in pair.get("checklist") or []
            if item.get("field") == "unsupported_inference"
        ),
        None,
    )
    if not isinstance(row, dict) or row.get("decision") not in {
        "same",
        "different",
        "abstain",
    }:
        raise DiagnosticV3QualityAuditError("v3 unsupported row is malformed")
    return str(row["decision"])


def build_diagnostic_v3_quality_audit_receipt(
    *, repo_root: Path, output_path: Path, audit_path: Path = DEFAULT_AUDIT_PATH
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    audit_file = audit_path.expanduser().resolve()
    audit = _load_json(audit_file, purpose="diagnostic-v3 quality audit")
    if audit.get("schema_version") != DIAGNOSTIC_V3_QUALITY_AUDIT_VERSION:
        raise DiagnosticV3QualityAuditError("unsupported diagnostic-v3 quality audit")
    resolved = {
        name: _resolve_record(root, record)
        for name, record in (audit.get("source_records") or {}).items()
    }
    if len(resolved) != 8:
        raise DiagnosticV3QualityAuditError("diagnostic-v3 audit source coverage drifted")
    diagnostic_root = (
        root
        / "work/app-server-development-v2/unattended-pipeline-v5/judge-diagnostic-v3"
    )
    terminal = _load_json(resolved["diagnostic_v3_terminal"], purpose="v3 terminal")
    score = _load_json(resolved["diagnostic_v3_score"], purpose="v3 score")
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "judge_diagnostic_quality_gate_not_passed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage", {}).get("total_tokens") != 150769
        or terminal.get("turn_count") != 4
        or terminal.get("semantic_retry_count") != 0
        or score.get("passed") is not False
    ):
        raise DiagnosticV3QualityAuditError("diagnostic-v3 terminal drifted")
    checks = score.get("checks") or {}
    if (
        {key for key, passed in checks.items() if passed is not True} != {"order_bias"}
        or score.get("metrics", {}).get("order_bias") != 0.333333
        or score.get("metrics", {}).get("support_sensitivity") != 1.0
        or score.get("metrics", {}).get("support_specificity") != 1.0
        or score.get("metrics", {}).get("structured_field_accuracy") != 1.0
        or score.get("metrics", {}).get("relation_accuracy") != 1.0
    ):
        raise DiagnosticV3QualityAuditError("diagnostic-v3 quality metrics drifted")
    truth = _load_json(
        diagnostic_root / "diagnostic-truth.private.json", purpose="v3 truth"
    )["cases"]
    supports = _load_json(
        diagnostic_root / "support-receipts.private.json", purpose="v3 support receipts"
    )
    support_by_id = {
        str(row["witness_id"]): row for row in supports.get("units") or []
    }
    disagreements = _load_json(
        resolved["diagnostic_v3_disagreements"], purpose="v3 disagreements"
    )
    case_ids = {item["case_id"] for item in disagreements.get("disagreements") or []}
    case_keys = sorted(truth[case_id]["case_key"] for case_id in case_ids)
    if case_keys != ["diag2_actor", "diag2_pronunciation_coach_field"]:
        raise DiagnosticV3QualityAuditError("diagnostic-v3 changed cases drifted")
    base = _load_json(
        diagnostic_root / "turns/neutral-alignment-base/output.private.json",
        purpose="v3 base output",
    )
    canary = _load_json(
        diagnostic_root / "turns/neutral-alignment-canary/output.private.json",
        purpose="v3 canary output",
    )
    adjudication = _load_json(
        diagnostic_root / "turns/disagreement-adjudication/output.private.json",
        purpose="v3 adjudication output",
    )
    base_by_id = {row["case_id"]: row for row in base["cases"]}
    canary_by_id = {row["case_id"]: row for row in canary["cases"]}
    adjudication_by_id = {row["case_id"]: row for row in adjudication["cases"]}
    for case_id in case_ids:
        witness_ids = truth[case_id]["pair_witness_ids"]
        if any(
            support_by_id[witness_id]["proposition_verdict"] != "supported"
            for witness_id in witness_ids
        ):
            raise DiagnosticV3QualityAuditError("v3 support receipt truth drifted")
        if (
            _unsupported_decision(base_by_id[case_id]) != "same"
            or _unsupported_decision(canary_by_id[case_id]) != "different"
            or _unsupported_decision(adjudication_by_id[case_id]) != "different"
        ):
            raise DiagnosticV3QualityAuditError("v3 order failure shape drifted")
    recovery = audit.get("diagnostic_v4_recovery_contract") or {}
    if (
        recovery.get("gate_thresholds_changed") is not False
        or recovery.get("fixture_content_changed") is not False
        or recovery.get("fixture_truth_changed") is not False
        or recovery.get("unsupported_inference_consistency_rule_added") is not True
        or recovery.get("validator_must_reject_inconsistent_row") is not True
        or recovery.get("all_v4_turns_must_run_fresh") is not True
        or recovery.get("diagnostic_v3_output_reuse_allowed") is not False
        or recovery.get("diagnostic_v3_replay_allowed") is not False
        or recovery.get("full_calibration_remains_unauthorized") is not True
    ):
        raise DiagnosticV3QualityAuditError("diagnostic-v4 recovery contract drifted")
    receipt = {
        "schema_version": DIAGNOSTIC_V3_QUALITY_AUDIT_RECEIPT_VERSION,
        "audit": _record(audit_file),
        "verified_source_records": {
            name: _record(path) for name, path in resolved.items()
        },
        "only_failed_gate": "order_bias",
        "changed_case_keys": case_keys,
        "both_proposition_verdicts_supported": True,
        "base_unsupported_decision": "same",
        "canary_unsupported_decision": "different",
        "adjudication_unsupported_decision": "different",
        "diagnostic_v3_replay_allowed": False,
        "diagnostic_v3_output_reuse_allowed": False,
        "diagnostic_v4_requires_new_immutable_root": True,
        "full_calibration_authorized": False,
        "semantic_model_calls_performed": 0,
    }
    write_immutable_json(output_path.expanduser().resolve(), receipt)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit diagnostic-v3 quality")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", required=True)
    parser.add_argument("--audit", default=str(DEFAULT_AUDIT_PATH))
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    receipt = build_diagnostic_v3_quality_audit_receipt(
        repo_root=Path(args.repo_root),
        output_path=Path(args.output),
        audit_path=Path(args.audit),
    )
    print(json.dumps({
        "ok": True,
        "schema_version": receipt["schema_version"],
        "only_failed_gate": receipt["only_failed_gate"],
        "changed_case_count": len(receipt["changed_case_keys"]),
        "semantic_model_calls": receipt["semantic_model_calls_performed"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
