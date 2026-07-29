from __future__ import annotations

"""Executable audit of the diagnostic-v2 evidence-envelope failure."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_llm_judge import write_immutable_json


DIAGNOSTIC_V2_ATTEMPT_AUDIT_VERSION = "pif_judge_v5_diagnostic_v2_attempt_audit_v1"
DIAGNOSTIC_V2_ATTEMPT_AUDIT_RECEIPT_VERSION = (
    "pif_judge_v5_diagnostic_v2_attempt_audit_receipt_v1"
)
DEFAULT_AUDIT_PATH = (
    Path(__file__).resolve().parent
    / "evaluation/judge_v5_diagnostic_v2_attempt_audit.json"
)


class DiagnosticV2AttemptAuditError(ValueError):
    """The v2 failure or v3 recovery boundary drifted."""


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
        raise DiagnosticV2AttemptAuditError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise DiagnosticV2AttemptAuditError(f"{purpose} is not an object")
    return value


def _resolve_record(repo_root: Path, record: Mapping[str, Any]) -> Path:
    relative = record.get("path")
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise DiagnosticV2AttemptAuditError("v2 audit record path is invalid")
    path = (repo_root / relative).resolve()
    try:
        path.relative_to(repo_root)
    except ValueError as exc:
        raise DiagnosticV2AttemptAuditError("v2 audit record escapes repository") from exc
    if not path.is_file() or record.get("sha256") != _sha256_file(path):
        raise DiagnosticV2AttemptAuditError("v2 audit source record drifted")
    return path


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _recompute_invalid_canary_spans(
    *, canary_input: Mapping[str, Any], canary_output: Mapping[str, Any]
) -> dict[str, int]:
    inputs = {str(case["case_id"]): case for case in canary_input.get("cases") or []}
    invalid_rows = 0
    invalid_spans = 0
    affected_cases = set()
    for case in canary_output.get("cases") or []:
        case_id = str(case["case_id"])
        source = inputs[case_id]["source_excerpt"]
        for pair in case.get("alignment_pairs") or []:
            for row in pair.get("checklist") or []:
                bad = [
                    span
                    for span in row.get("evidence_spans") or []
                    if not isinstance(span, str) or span not in source
                ]
                if bad:
                    invalid_rows += 1
                    invalid_spans += len(bad)
                    affected_cases.add(case_id)
    return {
        "invalid_checklist_row_count": invalid_rows,
        "invalid_source_span_count": invalid_spans,
        "affected_canary_case_count": len(affected_cases),
    }


def build_diagnostic_v2_attempt_audit_receipt(
    *, repo_root: Path, output_path: Path, audit_path: Path = DEFAULT_AUDIT_PATH
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    audit_file = audit_path.expanduser().resolve()
    audit = _load_json(audit_file, purpose="diagnostic-v2 attempt audit")
    if audit.get("schema_version") != DIAGNOSTIC_V2_ATTEMPT_AUDIT_VERSION:
        raise DiagnosticV2AttemptAuditError("unsupported diagnostic-v2 attempt audit")
    resolved = {
        name: _resolve_record(root, record)
        for name, record in (audit.get("source_records") or {}).items()
    }
    if len(resolved) != 8:
        raise DiagnosticV2AttemptAuditError("diagnostic-v2 audit source coverage drifted")
    diagnostic_root = (
        root
        / "work/app-server-development-v2/unattended-pipeline-v5/judge-diagnostic-v2"
    )
    terminal = _load_json(resolved["diagnostic_v2_terminal"], purpose="v2 terminal")
    failure = _load_json(resolved["diagnostic_v2_failure"], purpose="v2 failure")
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("full_calibration_authorized") is not False
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != "neutral_alignment_canary"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("known_usage_turn_count") != 3
        or failure.get("unknown_usage_turn_count") != 0
        or failure.get("known_usage_lower_bound", {}).get("total_tokens") != 107187
    ):
        raise DiagnosticV2AttemptAuditError("diagnostic-v2 terminal classification drifted")
    usages = {field: 0 for field in (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )}
    sidecar_count = 0
    for path in sorted(diagnostic_root.glob("turns/*/sidecar.json")):
        sidecar = _load_json(path, purpose="v2 sidecar")
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("usage_complete") is not True
            or sidecar.get("usage_status") != "measured"
        ):
            raise DiagnosticV2AttemptAuditError("diagnostic-v2 sidecar is incomplete")
        sidecar_count += 1
        for field in usages:
            usages[field] += sidecar["usage"][field]
    if sidecar_count != 3 or usages["total_tokens"] != 107187:
        raise DiagnosticV2AttemptAuditError("diagnostic-v2 measured usage drifted")
    canary_input = _load_json(
        diagnostic_root / "turns/neutral-alignment-canary/input.private.json",
        purpose="v2 canary input",
    )
    canary_output = _load_json(
        resolved["diagnostic_v2_canary_output"], purpose="v2 canary output"
    )
    recomputed = _recompute_invalid_canary_spans(
        canary_input=canary_input, canary_output=canary_output
    )
    if recomputed != {
        "invalid_checklist_row_count": 29,
        "invalid_source_span_count": 34,
        "affected_canary_case_count": 6,
    }:
        raise DiagnosticV2AttemptAuditError("diagnostic-v2 failure forensics drifted")
    recovery = audit.get("diagnostic_v3_recovery_contract") or {}
    if (
        recovery.get("fixture_truth_changed") is not False
        or recovery.get("fixture_content_changed") is not False
        or recovery.get("support_protocol_changed") is not False
        or recovery.get("alignment_evidence_envelope_changed") is not True
        or recovery.get("all_v3_turns_must_run_fresh") is not True
        or recovery.get("diagnostic_v2_output_reuse_allowed") is not False
        or recovery.get("diagnostic_v2_replay_allowed") is not False
        or recovery.get("full_calibration_remains_unauthorized") is not True
    ):
        raise DiagnosticV2AttemptAuditError("diagnostic-v3 recovery contract drifted")
    receipt = {
        "schema_version": DIAGNOSTIC_V2_ATTEMPT_AUDIT_RECEIPT_VERSION,
        "audit": _record(audit_file),
        "verified_source_records": {
            name: _record(path) for name, path in resolved.items()
        },
        "recomputed_failure_forensics": recomputed,
        "measured_sidecar_usage": usages,
        "measured_sidecar_count": sidecar_count,
        "quality_scored": False,
        "diagnostic_v2_replay_allowed": False,
        "diagnostic_v2_output_reuse_allowed": False,
        "diagnostic_v3_requires_new_immutable_root": True,
        "full_calibration_authorized": False,
        "semantic_model_calls_performed": 0,
    }
    write_immutable_json(output_path.expanduser().resolve(), receipt)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit diagnostic-v2 attempt")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", required=True)
    parser.add_argument("--audit", default=str(DEFAULT_AUDIT_PATH))
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    receipt = build_diagnostic_v2_attempt_audit_receipt(
        repo_root=Path(args.repo_root),
        output_path=Path(args.output),
        audit_path=Path(args.audit),
    )
    print(json.dumps({
        "ok": True,
        "schema_version": receipt["schema_version"],
        "invalid_source_spans": receipt["recomputed_failure_forensics"]["invalid_source_span_count"],
        "measured_total_tokens": receipt["measured_sidecar_usage"]["total_tokens"],
        "semantic_model_calls": receipt["semantic_model_calls_performed"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
