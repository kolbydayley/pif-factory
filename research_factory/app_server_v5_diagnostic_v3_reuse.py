from __future__ import annotations

"""Hash-bound reuse contract for pipeline-v5 diagnostic-v3."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5_diagnostic_v2_audit import (
    DIAGNOSTIC_V2_ATTEMPT_AUDIT_RECEIPT_VERSION,
)
from .app_server_llm_judge import write_immutable_json
from .app_server_v5_diagnostic_reuse import verify_diagnostic_v2_reuse_contract


DIAGNOSTIC_V3_REUSE_CONTRACT_VERSION = (
    "pif_app_server_pipeline_v5_diagnostic_v3_reuse_contract_v1"
)


class DiagnosticV3ReuseError(ValueError):
    """The diagnostic-v3 predecessor boundary is missing, changed, or unsafe."""


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
        raise DiagnosticV3ReuseError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise DiagnosticV3ReuseError(f"{purpose} is not an object")
    return value


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise DiagnosticV3ReuseError("required diagnostic-v3 predecessor is missing")
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _verify_record(record: Mapping[str, Any], *, root: Optional[Path] = None) -> Path:
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if root is not None:
        try:
            path.relative_to(root.resolve())
        except ValueError as exc:
            raise DiagnosticV3ReuseError("diagnostic-v3 record escapes root") from exc
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise DiagnosticV3ReuseError("diagnostic-v3 predecessor record drifted")
    return path


def build_diagnostic_v3_reuse_contract(
    *, repo_root: Path, output_path: Path
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    pipeline_root = root / "work/app-server-development-v2/unattended-pipeline-v5"
    v2_root = pipeline_root / "judge-diagnostic-v2"
    v3_root = pipeline_root / "judge-diagnostic-v3"
    prior_contract_path = pipeline_root / "reuse-contract-v5.json"
    verify_diagnostic_v2_reuse_contract(prior_contract_path)
    terminal = _load_json(v2_root / "terminal.json", purpose="diagnostic-v2 terminal")
    failure = _load_json(v2_root / "failure.json", purpose="diagnostic-v2 failure")
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("full_calibration_authorized") is not False
        or failure.get("failed_turn_name") != "neutral_alignment_canary"
        or failure.get("known_usage_turn_count") != 3
        or failure.get("unknown_usage_turn_count") != 0
        or failure.get("known_usage_lower_bound", {}).get("total_tokens") != 107187
        or failure.get("retry_allowed_in_this_version") is not False
    ):
        raise DiagnosticV3ReuseError("diagnostic-v2 is not the frozen validation failure")
    attempts = []
    measured_usage = {field: 0 for field in (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )}
    for turn_root in sorted((v2_root / "turns").iterdir()):
        if not (turn_root / "sidecar.json").is_file():
            continue
        sidecar = _load_json(turn_root / "sidecar.json", purpose="v2 sidecar")
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("usage_complete") is not True
            or sidecar.get("usage_status") != "measured"
        ):
            raise DiagnosticV3ReuseError("diagnostic-v2 sidecar is incomplete")
        for field in measured_usage:
            measured_usage[field] += sidecar["usage"][field]
        attempts.append(
            {
                "turn_name": turn_root.name.replace("-", "_"),
                "sidecar": _record(turn_root / "sidecar.json"),
                "capacity_checkpoint": _record(turn_root / "capacity.json"),
                "output": _record(turn_root / "output.private.json"),
                "input": _record(turn_root / "input.private.json"),
                "prompt": _record(turn_root / "prompt.private.md"),
                "output_schema": _record(turn_root / "schema.json"),
            }
        )
    if len(attempts) != 3 or measured_usage["total_tokens"] != 107187:
        raise DiagnosticV3ReuseError("diagnostic-v2 measured attempt coverage drifted")
    audit_receipt_path = pipeline_root / "diagnostic-v2-attempt-audit-receipt-v1.json"
    audit = _load_json(audit_receipt_path, purpose="diagnostic-v2 audit receipt")
    if (
        audit.get("schema_version") != DIAGNOSTIC_V2_ATTEMPT_AUDIT_RECEIPT_VERSION
        or audit.get("quality_scored") is not False
        or audit.get("diagnostic_v2_replay_allowed") is not False
        or audit.get("diagnostic_v2_output_reuse_allowed") is not False
        or audit.get("full_calibration_authorized") is not False
        or audit.get("semantic_model_calls_performed") != 0
    ):
        raise DiagnosticV3ReuseError("diagnostic-v2 audit receipt is unsafe")
    payload = {
        "schema_version": DIAGNOSTIC_V3_REUSE_CONTRACT_VERSION,
        "pipeline_root": str(pipeline_root.resolve()),
        "source_diagnostic_root": str(v2_root.resolve()),
        "target_diagnostic_root": str(v3_root.resolve()),
        "prior_diagnostic_v2_reuse_contract": _record(prior_contract_path),
        "diagnostic_v2_incident": {
            "classification": "infrastructure_or_judge_attempt_failed",
            "transport_failed": False,
            "quality_scored": False,
            "completed_turn_count": 3,
            "failed_turn_count": 0,
            "unknown_usage_turn_count": 0,
            "sidecar_usage_status": "complete",
            "sidecar_usage": measured_usage,
            "failed_stage": "neutral_alignment_canary_exact_source_evidence_validation",
            "retry_allowed": False,
            "output_reuse_allowed": False,
        },
        "diagnostic_v2_artifacts": {
            "runtime_lock_v7": _record(
                root / "work/app-server-development-v2/unattended-runtime-lock-v7.json"
            ),
            "launch_receipt_v7": _record(
                root / "work/app-server-development-v2/unattended-control-v7/launch-receipt-v7.json"
            ),
            "terminal_receipt_v7": _record(
                root / "work/app-server-development-v2/unattended-control-v7/terminal-receipt-v7.json"
            ),
            "diagnostic_spec": _record(v2_root / "diagnostic-spec.json"),
            "terminal": _record(v2_root / "terminal.json"),
            "failure": _record(v2_root / "failure.json"),
        },
        "diagnostic_v2_attempts": attempts,
        "attempt_audit_receipt": _record(audit_receipt_path),
        "diagnostic_fixture_v2": _record(
            root / "research_factory/evaluation/judge_v5_diagnostic_v2.json"
        ),
        "policy": {
            "diagnostic_v2_replay_allowed": False,
            "diagnostic_v2_output_reuse_allowed": False,
            "diagnostic_v3_root_must_be_new": True,
            "all_diagnostic_v3_turns_must_run_fresh": True,
            "fixture_content_changed": False,
            "fixture_truth_changed": False,
            "support_protocol_changed": False,
            "alignment_evidence_envelope_changed": True,
            "one_attempt_per_v3_semantic_turn": True,
            "full_calibration_allowed_before_v3_pass": False,
            "production_mutation_allowed": False,
        },
    }
    output = output_path.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    write_immutable_json(output, payload)
    return verify_diagnostic_v3_reuse_contract(output)


def verify_diagnostic_v3_reuse_contract(path: Path) -> dict[str, Any]:
    payload = _load_json(path.expanduser().resolve(), purpose="diagnostic-v3 reuse contract")
    if payload.get("schema_version") != DIAGNOSTIC_V3_REUSE_CONTRACT_VERSION:
        raise DiagnosticV3ReuseError("unsupported diagnostic-v3 reuse contract")
    pipeline_root = Path(str(payload.get("pipeline_root") or "")).resolve()
    source = Path(str(payload.get("source_diagnostic_root") or "")).resolve()
    target = Path(str(payload.get("target_diagnostic_root") or "")).resolve()
    if (
        source.parent != pipeline_root
        or target.parent != pipeline_root
        or source.name != "judge-diagnostic-v2"
        or target.name != "judge-diagnostic-v3"
        or source == target
    ):
        raise DiagnosticV3ReuseError("diagnostic-v3 roots drifted")
    prior = _verify_record(payload.get("prior_diagnostic_v2_reuse_contract") or {})
    verify_diagnostic_v2_reuse_contract(prior)
    incident = payload.get("diagnostic_v2_incident") or {}
    if (
        incident.get("classification") != "infrastructure_or_judge_attempt_failed"
        or incident.get("quality_scored") is not False
        or incident.get("completed_turn_count") != 3
        or incident.get("unknown_usage_turn_count") != 0
        or incident.get("sidecar_usage", {}).get("total_tokens") != 107187
        or incident.get("retry_allowed") is not False
        or incident.get("output_reuse_allowed") is not False
    ):
        raise DiagnosticV3ReuseError("diagnostic-v2 incident drifted")
    for record in (payload.get("diagnostic_v2_artifacts") or {}).values():
        _verify_record(record)
    attempts = payload.get("diagnostic_v2_attempts")
    if not isinstance(attempts, list) or len(attempts) != 3:
        raise DiagnosticV3ReuseError("diagnostic-v2 attempt coverage drifted")
    for attempt in attempts:
        for key in (
            "sidecar",
            "capacity_checkpoint",
            "output",
            "input",
            "prompt",
            "output_schema",
        ):
            _verify_record(attempt.get(key) or {}, root=source)
    _verify_record(payload.get("attempt_audit_receipt") or {})
    _verify_record(payload.get("diagnostic_fixture_v2") or {})
    policy = payload.get("policy") or {}
    required_false = (
        "diagnostic_v2_replay_allowed",
        "diagnostic_v2_output_reuse_allowed",
        "fixture_content_changed",
        "fixture_truth_changed",
        "support_protocol_changed",
        "full_calibration_allowed_before_v3_pass",
        "production_mutation_allowed",
    )
    if any(policy.get(key) is not False for key in required_false) or any(
        policy.get(key) is not True
        for key in (
            "diagnostic_v3_root_must_be_new",
            "all_diagnostic_v3_turns_must_run_fresh",
            "alignment_evidence_envelope_changed",
            "one_attempt_per_v3_semantic_turn",
        )
    ):
        raise DiagnosticV3ReuseError("diagnostic-v3 policy drifted")
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Freeze/verify diagnostic-v3 reuse")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", required=True)
    parser.add_argument("--verify-only", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    output = Path(args.output)
    payload = (
        verify_diagnostic_v3_reuse_contract(output)
        if args.verify_only
        else build_diagnostic_v3_reuse_contract(
            repo_root=Path(args.repo_root), output_path=output
        )
    )
    print(json.dumps({
        "ok": True,
        "schema_version": payload["schema_version"],
        "diagnostic_v2_replay_allowed": payload["policy"]["diagnostic_v2_replay_allowed"],
        "output": str(output.expanduser().resolve()),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
