from __future__ import annotations

"""Hash-bound predecessor contract for pipeline-v5 diagnostic-v6."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5_diagnostic_v5_audit import AUDIT_RECEIPT_VERSION
from .app_server_llm_judge import write_immutable_json
from .app_server_v5_diagnostic_v5_reuse import verify_diagnostic_v5_reuse_contract


DIAGNOSTIC_V6_REUSE_CONTRACT_VERSION = (
    "pif_app_server_pipeline_v5_diagnostic_v6_reuse_contract_v1"
)


class DiagnosticV6ReuseError(ValueError):
    """The diagnostic-v6 predecessor boundary is missing, changed, or unsafe."""


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
        raise DiagnosticV6ReuseError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise DiagnosticV6ReuseError(f"{purpose} is not an object")
    return value


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise DiagnosticV6ReuseError("required diagnostic-v6 predecessor is missing")
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
            raise DiagnosticV6ReuseError("diagnostic-v6 record escapes root") from exc
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise DiagnosticV6ReuseError("diagnostic-v6 predecessor record drifted")
    return path


def build_diagnostic_v6_reuse_contract(
    *, repo_root: Path, output_path: Path
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    pipeline_root = root / "work/app-server-development-v2/unattended-pipeline-v5"
    v5_root = pipeline_root / "judge-diagnostic-v5"
    v6_root = pipeline_root / "judge-diagnostic-v6"
    prior_contract_path = pipeline_root / "reuse-contract-v8.json"
    verify_diagnostic_v5_reuse_contract(prior_contract_path)
    terminal = _load_json(v5_root / "terminal.json", purpose="diagnostic-v5 terminal")
    score = _load_json(v5_root / "diagnostic-score.json", purpose="diagnostic-v5 score")
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
        raise DiagnosticV6ReuseError("diagnostic-v5 is not the frozen order-only failure")
    attempts = []
    for turn_root in sorted((v5_root / "turns").iterdir()):
        if not (turn_root / "sidecar.json").is_file():
            continue
        sidecar = _load_json(turn_root / "sidecar.json", purpose="v5 sidecar")
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("usage_complete") is not True
            or sidecar.get("usage_status") != "measured"
        ):
            raise DiagnosticV6ReuseError("diagnostic-v5 sidecar is incomplete")
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
    if len(attempts) != 4:
        raise DiagnosticV6ReuseError("diagnostic-v5 attempt coverage drifted")
    audit_path = pipeline_root / "diagnostic-v5-quality-audit-receipt-v1.json"
    audit = _load_json(audit_path, purpose="diagnostic-v5 quality audit receipt")
    if (
        audit.get("schema_version") != AUDIT_RECEIPT_VERSION
        or audit.get("only_failed_gate") != "order_bias"
        or audit.get("changed_case_key") != "diag2_unsupported_inference"
        or audit.get("base_boundary_without_evidence") is not True
        or audit.get("canary_and_adjudication_event_type") is not True
        or audit.get("diagnostic_v5_replay_allowed") is not False
        or audit.get("diagnostic_v5_output_reuse_allowed") is not False
        or audit.get("full_calibration_authorized") is not False
    ):
        raise DiagnosticV6ReuseError("diagnostic-v5 audit receipt is unsafe")
    payload = {
        "schema_version": DIAGNOSTIC_V6_REUSE_CONTRACT_VERSION,
        "pipeline_root": str(pipeline_root.resolve()),
        "source_diagnostic_root": str(v5_root.resolve()),
        "target_diagnostic_root": str(v6_root.resolve()),
        "prior_diagnostic_v5_reuse_contract": _record(prior_contract_path),
        "diagnostic_v5_incident": {
            "classification": "judge_diagnostic_quality_gate_not_passed",
            "only_failed_gate": "order_bias",
            "quality_scored": True,
            "completed_turn_count": 4,
            "usage_status": "complete",
            "usage": terminal["usage"],
            "wall_elapsed_seconds_sum": terminal["wall_elapsed_seconds_sum"],
            "order_bias": score["metrics"]["order_bias"],
            "retry_allowed": False,
            "output_reuse_allowed": False,
        },
        "diagnostic_v5_artifacts": {
            "runtime_lock_v10": _record(
                root / "work/app-server-development-v2/unattended-runtime-lock-v10.json"
            ),
            "launch_receipt_v10": _record(
                root / "work/app-server-development-v2/unattended-control-v10/launch-receipt-v10.json"
            ),
            "terminal_receipt_v10": _record(
                root / "work/app-server-development-v2/unattended-control-v10/terminal-receipt-v10.json"
            ),
            "diagnostic_spec": _record(v5_root / "diagnostic-spec.json"),
            "terminal": _record(v5_root / "terminal.json"),
            "score": _record(v5_root / "diagnostic-score.json"),
            "disagreements": _record(v5_root / "observable-disagreements.private.json"),
        },
        "diagnostic_v5_attempts": attempts,
        "quality_audit_receipt": _record(audit_path),
        "diagnostic_fixture_patch_v3": _record(
            root / "research_factory/evaluation/judge_v5_diagnostic_v3.json"
        ),
        "policy": {
            "diagnostic_v5_replay_allowed": False,
            "diagnostic_v5_output_reuse_allowed": False,
            "diagnostic_v6_root_must_be_new": True,
            "all_diagnostic_v6_turns_must_run_fresh": True,
            "fixture_content_changed": False,
            "fixture_truth_changed": False,
            "gate_thresholds_changed": False,
            "boundary_evidence_consistency_rule_added": True,
            "unsupported_assertion_not_boundary_rule_added": True,
            "event_type_direct_category_rule_added": True,
            "one_attempt_per_v6_semantic_turn": True,
            "full_calibration_allowed_before_v6_pass": False,
            "production_mutation_allowed": False,
        },
    }
    output = output_path.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    write_immutable_json(output, payload)
    return verify_diagnostic_v6_reuse_contract(output)


def verify_diagnostic_v6_reuse_contract(path: Path) -> dict[str, Any]:
    payload = _load_json(path.expanduser().resolve(), purpose="diagnostic-v6 reuse contract")
    if payload.get("schema_version") != DIAGNOSTIC_V6_REUSE_CONTRACT_VERSION:
        raise DiagnosticV6ReuseError("unsupported diagnostic-v6 reuse contract")
    pipeline_root = Path(str(payload.get("pipeline_root") or "")).resolve()
    source = Path(str(payload.get("source_diagnostic_root") or "")).resolve()
    target = Path(str(payload.get("target_diagnostic_root") or "")).resolve()
    if (
        source.parent != pipeline_root
        or target.parent != pipeline_root
        or source.name != "judge-diagnostic-v5"
        or target.name != "judge-diagnostic-v6"
        or source == target
    ):
        raise DiagnosticV6ReuseError("diagnostic-v6 roots drifted")
    prior = _verify_record(payload.get("prior_diagnostic_v5_reuse_contract") or {})
    verify_diagnostic_v5_reuse_contract(prior)
    incident = payload.get("diagnostic_v5_incident") or {}
    if (
        incident.get("classification") != "judge_diagnostic_quality_gate_not_passed"
        or incident.get("only_failed_gate") != "order_bias"
        or incident.get("completed_turn_count") != 4
        or incident.get("usage", {}).get("total_tokens") != 149074
        or incident.get("order_bias") != 0.166667
        or incident.get("retry_allowed") is not False
        or incident.get("output_reuse_allowed") is not False
    ):
        raise DiagnosticV6ReuseError("diagnostic-v5 incident drifted")
    for record in (payload.get("diagnostic_v5_artifacts") or {}).values():
        _verify_record(record)
    attempts = payload.get("diagnostic_v5_attempts")
    if not isinstance(attempts, list) or len(attempts) != 4:
        raise DiagnosticV6ReuseError("diagnostic-v5 attempt coverage drifted")
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
    _verify_record(payload.get("quality_audit_receipt") or {})
    _verify_record(payload.get("diagnostic_fixture_patch_v3") or {})
    policy = payload.get("policy") or {}
    required_false = (
        "diagnostic_v5_replay_allowed",
        "diagnostic_v5_output_reuse_allowed",
        "fixture_content_changed",
        "fixture_truth_changed",
        "gate_thresholds_changed",
        "full_calibration_allowed_before_v6_pass",
        "production_mutation_allowed",
    )
    required_true = (
        "diagnostic_v6_root_must_be_new",
        "all_diagnostic_v6_turns_must_run_fresh",
        "boundary_evidence_consistency_rule_added",
        "unsupported_assertion_not_boundary_rule_added",
        "event_type_direct_category_rule_added",
        "one_attempt_per_v6_semantic_turn",
    )
    if any(policy.get(key) is not False for key in required_false) or any(
        policy.get(key) is not True for key in required_true
    ):
        raise DiagnosticV6ReuseError("diagnostic-v6 policy drifted")
    return payload


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze/verify diagnostic-v6 reuse")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", required=True)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args(argv)
    output = Path(args.output)
    payload = (
        verify_diagnostic_v6_reuse_contract(output)
        if args.verify_only
        else build_diagnostic_v6_reuse_contract(
            repo_root=Path(args.repo_root), output_path=output
        )
    )
    print(json.dumps({"ok": True, "schema_version": payload["schema_version"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
