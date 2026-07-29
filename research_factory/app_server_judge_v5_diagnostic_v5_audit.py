from __future__ import annotations

"""Executable audit of the diagnostic-v5 order-only judge failure."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_llm_judge import write_immutable_json


AUDIT_VERSION = "pif_judge_v5_diagnostic_v5_quality_audit_v1"
AUDIT_RECEIPT_VERSION = "pif_judge_v5_diagnostic_v5_quality_audit_receipt_v1"
DEFAULT_AUDIT_PATH = (
    Path(__file__).resolve().parent
    / "evaluation/judge_v5_diagnostic_v5_quality_audit.json"
)


class DiagnosticV5QualityAuditError(ValueError):
    """The v5 quality evidence or v6 recovery rule drifted."""


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
        raise DiagnosticV5QualityAuditError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise DiagnosticV5QualityAuditError(f"{purpose} is not an object")
    return value


def _resolve_record(root: Path, record: Mapping[str, Any]) -> Path:
    relative = record.get("path")
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise DiagnosticV5QualityAuditError("v5 audit record path is invalid")
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise DiagnosticV5QualityAuditError("v5 audit record escapes repository") from exc
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise DiagnosticV5QualityAuditError("v5 audit source record drifted")
    return path


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _different_fields(output: Mapping[str, Any], case_id: str) -> list[str]:
    case = next(item for item in output["cases"] if item["case_id"] == case_id)
    return [
        row["field"]
        for row in case["alignment_pairs"][0]["checklist"]
        if row["decision"] == "different"
    ]


def build_diagnostic_v5_quality_audit_receipt(
    *, repo_root: Path, output_path: Path, audit_path: Path = DEFAULT_AUDIT_PATH
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    audit_file = audit_path.expanduser().resolve()
    audit = _load_json(audit_file, purpose="diagnostic-v5 quality audit")
    if audit.get("schema_version") != AUDIT_VERSION:
        raise DiagnosticV5QualityAuditError("unsupported v5 quality audit")
    resolved = {
        name: _resolve_record(root, record)
        for name, record in (audit.get("source_records") or {}).items()
    }
    if len(resolved) != 11:
        raise DiagnosticV5QualityAuditError("v5 audit source coverage drifted")
    terminal = _load_json(resolved["diagnostic_v5_terminal"], purpose="v5 terminal")
    score = _load_json(resolved["diagnostic_v5_score"], purpose="v5 score")
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason") != "judge_diagnostic_quality_gate_not_passed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage", {}).get("total_tokens") != 149074
        or terminal.get("wall_elapsed_seconds_sum") != 715.93
        or terminal.get("turn_count") != 4
        or terminal.get("semantic_retry_count") != 0
        or score.get("passed") is not False
        or {key for key, passed in score.get("checks", {}).items() if passed is not True}
        != {"order_bias"}
        or score.get("metrics", {}).get("order_bias") != 0.166667
    ):
        raise DiagnosticV5QualityAuditError("diagnostic-v5 terminal drifted")
    finding = audit.get("finding") or {}
    case_id = str(finding.get("case_id"))
    expected = {
        "diagnostic_v5_base": finding.get("base_different_fields"),
        "diagnostic_v5_canary": finding.get("canary_different_fields"),
        "diagnostic_v5_adjudication": finding.get("adjudication_different_fields"),
    }
    for name, fields in expected.items():
        if _different_fields(_load_json(resolved[name], purpose=name), case_id) != fields:
            raise DiagnosticV5QualityAuditError("v5 disagreement shape drifted")
    if (
        finding.get("case_key") != "diag2_unsupported_inference"
        or finding.get("base_boundary_without_evidence") is not True
        or finding.get("fixture_truth_is_corrected") is not True
    ):
        raise DiagnosticV5QualityAuditError("v5 audited finding drifted")
    recovery = audit.get("diagnostic_v6_recovery_contract") or {}
    required_false = (
        "diagnostic_v5_replay_allowed",
        "diagnostic_v5_output_reuse_allowed",
        "fixture_content_changed",
        "fixture_truth_changed",
        "gate_thresholds_changed",
        "full_calibration_allowed_before_v6_pass",
    )
    required_true = (
        "all_diagnostic_v6_turns_run_fresh",
        "boundary_evidence_consistency_rule_added",
        "unsupported_assertion_not_boundary_rule_added",
        "event_type_direct_category_rule_added",
    )
    if any(recovery.get(key) is not False for key in required_false) or any(
        recovery.get(key) is not True for key in required_true
    ):
        raise DiagnosticV5QualityAuditError("diagnostic-v6 recovery contract drifted")
    receipt = {
        "schema_version": AUDIT_RECEIPT_VERSION,
        "audit": _record(audit_file),
        "verified_source_records": {
            name: _record(path) for name, path in resolved.items()
        },
        "only_failed_gate": "order_bias",
        "changed_case_key": finding["case_key"],
        "base_boundary_without_evidence": True,
        "canary_and_adjudication_event_type": True,
        "diagnostic_v5_replay_allowed": False,
        "diagnostic_v5_output_reuse_allowed": False,
        "diagnostic_v6_requires_new_immutable_root": True,
        "full_calibration_authorized": False,
        "semantic_model_calls_performed": 0,
    }
    write_immutable_json(output_path.expanduser().resolve(), receipt)
    return receipt


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Audit diagnostic-v5 quality")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", required=True)
    parser.add_argument("--audit", default=str(DEFAULT_AUDIT_PATH))
    args = parser.parse_args(argv)
    receipt = build_diagnostic_v5_quality_audit_receipt(
        repo_root=Path(args.repo_root),
        output_path=Path(args.output),
        audit_path=Path(args.audit),
    )
    print(json.dumps({"ok": True, "schema_version": receipt["schema_version"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
