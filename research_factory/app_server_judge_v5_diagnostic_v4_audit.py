from __future__ import annotations

"""Executable audit of the diagnostic-v4 fixture-truth contradiction."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5 import load_v5_diagnostic_fixture
from .app_server_llm_judge import write_immutable_json


AUDIT_VERSION = "pif_judge_v5_diagnostic_v4_fixture_audit_v1"
AUDIT_RECEIPT_VERSION = "pif_judge_v5_diagnostic_v4_fixture_audit_receipt_v1"
DEFAULT_AUDIT_PATH = (
    Path(__file__).resolve().parent
    / "evaluation/judge_v5_diagnostic_v4_fixture_audit.json"
)


class DiagnosticV4FixtureAuditError(ValueError):
    """The v4 evidence or audited fixture correction drifted."""


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
        raise DiagnosticV4FixtureAuditError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise DiagnosticV4FixtureAuditError(f"{purpose} is not an object")
    return value


def _resolve_record(root: Path, record: Mapping[str, Any]) -> Path:
    relative = record.get("path")
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise DiagnosticV4FixtureAuditError("v4 audit record path is invalid")
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise DiagnosticV4FixtureAuditError("v4 audit record escapes repository") from exc
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise DiagnosticV4FixtureAuditError("v4 audit source record drifted")
    return path


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _different_fields(output: Mapping[str, Any], case_id: str) -> set[str]:
    case = next(
        (item for item in output.get("cases", []) if item.get("case_id") == case_id),
        None,
    )
    if not isinstance(case, Mapping) or len(case.get("alignment_pairs") or []) != 1:
        raise DiagnosticV4FixtureAuditError("v4 audited alignment case is missing")
    return {
        str(row["field"])
        for row in case["alignment_pairs"][0].get("checklist", [])
        if row.get("decision") == "different"
    }


def build_diagnostic_v4_fixture_audit_receipt(
    *, repo_root: Path, output_path: Path, audit_path: Path = DEFAULT_AUDIT_PATH
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    audit_file = audit_path.expanduser().resolve()
    audit = _load_json(audit_file, purpose="diagnostic-v4 fixture audit")
    if audit.get("schema_version") != AUDIT_VERSION:
        raise DiagnosticV4FixtureAuditError("unsupported v4 fixture audit")
    resolved = {
        name: _resolve_record(root, record)
        for name, record in (audit.get("source_records") or {}).items()
    }
    if len(resolved) != 12:
        raise DiagnosticV4FixtureAuditError("v4 audit source coverage drifted")

    terminal = _load_json(resolved["diagnostic_v4_terminal"], purpose="v4 terminal")
    score = _load_json(resolved["diagnostic_v4_score"], purpose="v4 score")
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason") != "judge_diagnostic_quality_gate_not_passed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage", {}).get("total_tokens") != 147819
        or terminal.get("turn_count") != 4
        or terminal.get("semantic_retry_count") != 0
        or score.get("passed") is not False
        or {key for key, passed in score.get("checks", {}).items() if passed is not True}
        != {"order_bias"}
        or score.get("metrics", {}).get("order_bias") != 0.166667
    ):
        raise DiagnosticV4FixtureAuditError("diagnostic-v4 terminal evidence drifted")

    finding = audit.get("finding") or {}
    case_key = finding.get("case_key")
    case_id = finding.get("case_id")
    base_fixture = _load_json(resolved["diagnostic_fixture_v2"], purpose="v2 fixture")
    old_case = next(
        (item for item in base_fixture.get("cases", []) if item.get("case_key") == case_key),
        None,
    )
    if (
        not isinstance(old_case, dict)
        or old_case["event_a"].get("event_type") != "measurement_claim"
        or old_case["event_b"].get("event_type") != "capability_claim"
        or "event_type" in old_case["expected"]["field_issues_b"]
        or "event_type" in old_case["expected"]["mismatch_fields"]
    ):
        raise DiagnosticV4FixtureAuditError("v2 fixture contradiction drifted")
    expected_differences = {
        "diagnostic_v4_base": {"event_type", "target", "unsupported_inference"},
        "diagnostic_v4_canary": {"target", "unsupported_inference"},
        "diagnostic_v4_adjudication": {
            "event_type",
            "target",
            "unsupported_inference",
        },
    }
    for name, expected_fields in expected_differences.items():
        output = _load_json(resolved[name], purpose=name)
        if _different_fields(output, str(case_id)) != expected_fields:
            raise DiagnosticV4FixtureAuditError("v4 disagreement shape drifted")

    corrected = load_v5_diagnostic_fixture(resolved["diagnostic_fixture_patch_v3"])
    new_case = next(item for item in corrected["cases"] if item["case_key"] == case_key)
    old_copy = json.loads(json.dumps(old_case, sort_keys=True))
    new_copy = json.loads(json.dumps(new_case, sort_keys=True))
    for key in ("field_issues_b", "mismatch_fields"):
        old_copy["expected"][key] = new_copy["expected"][key]
    if old_copy != new_copy:
        raise DiagnosticV4FixtureAuditError("fixture correction changed nontruth content")
    expected_truth = ["event_type", "target", "unsupported_inference"]
    if (
        new_case["expected"]["field_issues_b"] != expected_truth
        or new_case["expected"]["mismatch_fields"] != expected_truth
    ):
        raise DiagnosticV4FixtureAuditError("corrected event_type truth drifted")

    recovery = audit.get("recovery_contract") or {}
    required_false = (
        "diagnostic_v4_replay_allowed",
        "diagnostic_v4_output_reuse_allowed",
        "fixture_source_or_event_inputs_changed",
        "prompt_protocol_changed",
        "gate_thresholds_changed",
        "full_calibration_allowed_before_next_diagnostic_pass",
    )
    required_true = (
        "all_next_diagnostic_turns_run_fresh",
        "fixture_truth_changed",
    )
    if (
        recovery.get("semantic_truth_correction_count") != 1
        or any(recovery.get(key) is not False for key in required_false)
        or any(recovery.get(key) is not True for key in required_true)
    ):
        raise DiagnosticV4FixtureAuditError("v4 recovery contract drifted")
    receipt = {
        "schema_version": AUDIT_RECEIPT_VERSION,
        "audit": _record(audit_file),
        "verified_source_records": {
            name: _record(path) for name, path in resolved.items()
        },
        "only_failed_gate": "order_bias",
        "failed_case_key": case_key,
        "failed_case_id": case_id,
        "semantic_truth_correction": "event_type",
        "truth_projection_paths": [
            "expected.field_issues_b",
            "expected.mismatch_fields",
        ],
        "source_and_event_inputs_changed": False,
        "prompt_protocol_changed": False,
        "gate_thresholds_changed": False,
        "diagnostic_v4_replay_allowed": False,
        "diagnostic_v4_output_reuse_allowed": False,
        "next_diagnostic_requires_new_immutable_root": True,
        "full_calibration_authorized": False,
        "semantic_model_calls_performed": 0,
    }
    write_immutable_json(output_path.expanduser().resolve(), receipt)
    return receipt


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Audit diagnostic-v4 fixture truth")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", required=True)
    parser.add_argument("--audit", default=str(DEFAULT_AUDIT_PATH))
    args = parser.parse_args(argv)
    receipt = build_diagnostic_v4_fixture_audit_receipt(
        repo_root=Path(args.repo_root),
        output_path=Path(args.output),
        audit_path=Path(args.audit),
    )
    print(json.dumps({"ok": True, "schema_version": receipt["schema_version"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
