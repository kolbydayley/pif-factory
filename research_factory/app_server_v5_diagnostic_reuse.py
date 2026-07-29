from __future__ import annotations

"""Hash-bound reuse contract for pipeline-v5 diagnostic-v2."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5_diagnostic_audit import (
    DIAGNOSTIC_V1_TRUTH_AUDIT_RECEIPT_VERSION,
)
from .app_server_llm_judge import write_immutable_json
from .app_server_v5_reuse import verify_v5_reuse_contract


DIAGNOSTIC_V2_REUSE_CONTRACT_VERSION = (
    "pif_app_server_pipeline_v5_diagnostic_v2_reuse_contract_v1"
)
EXPECTED_V1_TURNS = 4


class DiagnosticV2ReuseError(ValueError):
    """The diagnostic-v2 predecessor contract is missing, changed, or unsafe."""


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
        raise DiagnosticV2ReuseError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise DiagnosticV2ReuseError(f"{purpose} is not an object")
    return value


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise DiagnosticV2ReuseError("required diagnostic-v2 predecessor is missing")
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
            raise DiagnosticV2ReuseError("diagnostic-v2 record escapes expected root") from exc
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise DiagnosticV2ReuseError("diagnostic-v2 predecessor record drifted")
    return path


def build_diagnostic_v2_reuse_contract(
    *, repo_root: Path, output_path: Path
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    pipeline_root = root / "work/app-server-development-v2/unattended-pipeline-v5"
    diagnostic_v1_root = pipeline_root / "judge-diagnostic-v1"
    diagnostic_v2_root = pipeline_root / "judge-diagnostic-v2"
    predecessor_contract = pipeline_root / "reuse-contract-v4.json"
    verify_v5_reuse_contract(predecessor_contract)
    v1_terminal_path = diagnostic_v1_root / "terminal.json"
    v1_score_path = diagnostic_v1_root / "diagnostic-score.json"
    terminal = _load_json(v1_terminal_path, purpose="diagnostic-v1 terminal")
    score = _load_json(v1_score_path, purpose="diagnostic-v1 score")
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "judge_diagnostic_quality_gate_not_passed"
        or terminal.get("diagnostic_passed") is not False
        or terminal.get("full_calibration_authorized") is not False
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("turn_count") != EXPECTED_V1_TURNS
        or terminal.get("usage", {}).get("total_tokens") != 138211
        or terminal.get("semantic_retry_count") != 0
        or score.get("passed") is not False
    ):
        raise DiagnosticV2ReuseError("diagnostic-v1 terminal is not the frozen quality failure")
    attempts = []
    for turn_root in sorted((diagnostic_v1_root / "turns").iterdir()):
        if not (turn_root / "sidecar.json").is_file():
            continue
        sidecar = _load_json(turn_root / "sidecar.json", purpose="v1 turn sidecar")
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("usage_complete") is not True
            or sidecar.get("usage_status") != "measured"
        ):
            raise DiagnosticV2ReuseError("diagnostic-v1 turn is not complete")
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
    if len(attempts) != EXPECTED_V1_TURNS:
        raise DiagnosticV2ReuseError("diagnostic-v1 attempt coverage drifted")
    audit_receipt_path = pipeline_root / "diagnostic-v1-truth-audit-receipt-v1.json"
    audit_receipt = _load_json(audit_receipt_path, purpose="diagnostic truth audit receipt")
    if (
        audit_receipt.get("schema_version")
        != DIAGNOSTIC_V1_TRUTH_AUDIT_RECEIPT_VERSION
        or audit_receipt.get("diagnostic_v1_replay_allowed") is not False
        or audit_receipt.get("diagnostic_v1_outputs_admissible_as_passing_evidence")
        is not False
        or audit_receipt.get("full_calibration_authorized") is not False
        or audit_receipt.get("semantic_model_calls_performed") != 0
    ):
        raise DiagnosticV2ReuseError("diagnostic truth audit receipt is unsafe")
    payload = {
        "schema_version": DIAGNOSTIC_V2_REUSE_CONTRACT_VERSION,
        "pipeline_root": str(pipeline_root.resolve()),
        "source_diagnostic_root": str(diagnostic_v1_root.resolve()),
        "target_diagnostic_root": str(diagnostic_v2_root.resolve()),
        "prior_pipeline_v5_reuse_contract": _record(predecessor_contract),
        "diagnostic_v1_incident": {
            "classification": "judge_diagnostic_quality_gate_not_passed",
            "transport_failed": False,
            "completed_turn_count": EXPECTED_V1_TURNS,
            "failed_turn_count": 0,
            "unknown_usage_turn_count": 0,
            "retry_count": 0,
            "usage_status": "complete",
            "usage": terminal["usage"],
            "support_sensitivity": score["metrics"]["support_sensitivity"],
            "structured_field_accuracy": score["metrics"][
                "structured_field_accuracy"
            ],
            "field_diagnostic_f1": score["metrics"]["field_diagnostic_f1"],
            "order_bias": score["metrics"]["order_bias"],
            "retry_allowed": False,
            "outputs_admissible_as_passing_evidence": False,
        },
        "diagnostic_v1_artifacts": {
            "runtime_lock_v6": _record(
                root / "work/app-server-development-v2/unattended-runtime-lock-v6.json"
            ),
            "launch_receipt_v6": _record(
                root
                / "work/app-server-development-v2/unattended-control-v6/launch-receipt-v6.json"
            ),
            "terminal_receipt_v6": _record(
                root
                / "work/app-server-development-v2/unattended-control-v6/terminal-receipt-v6.json"
            ),
            "diagnostic_spec": _record(diagnostic_v1_root / "diagnostic-spec.json"),
            "terminal": _record(v1_terminal_path),
            "score": _record(v1_score_path),
            "observable_disagreements": _record(
                diagnostic_v1_root / "observable-disagreements.private.json"
            ),
            "reconciled_alignment": _record(
                diagnostic_v1_root / "reconciled-alignment.private.json"
            ),
        },
        "diagnostic_v1_attempts": attempts,
        "truth_audit_receipt": _record(audit_receipt_path),
        "diagnostic_v2_fixture": _record(
            root / "research_factory/evaluation/judge_v5_diagnostic_v2.json"
        ),
        "policy": {
            "diagnostic_v1_replay_allowed": False,
            "diagnostic_v1_output_reuse_as_pass_allowed": False,
            "diagnostic_v2_root_must_be_new": True,
            "one_attempt_per_v2_semantic_turn": True,
            "full_calibration_allowed_before_v2_pass": False,
            "support_ab_ba_allowed": False,
            "production_mutation_allowed": False,
            "pipeline_v1_replay_allowed": False,
            "pipeline_v2_replay_allowed": False,
            "pipeline_v3_replay_allowed": False,
            "pipeline_v4_replay_allowed": False,
        },
        "privacy": "paths_hashes_sizes_counts_metrics_and_failure_classes_no_private_outputs",
    }
    output = output_path.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    write_immutable_json(output, payload)
    return verify_diagnostic_v2_reuse_contract(output)


def verify_diagnostic_v2_reuse_contract(path: Path) -> dict[str, Any]:
    payload = _load_json(path.expanduser().resolve(), purpose="diagnostic-v2 reuse contract")
    if payload.get("schema_version") != DIAGNOSTIC_V2_REUSE_CONTRACT_VERSION:
        raise DiagnosticV2ReuseError("unsupported diagnostic-v2 reuse contract")
    pipeline_root = Path(str(payload.get("pipeline_root") or "")).resolve()
    source = Path(str(payload.get("source_diagnostic_root") or "")).resolve()
    target = Path(str(payload.get("target_diagnostic_root") or "")).resolve()
    if (
        source == target
        or source.parent != pipeline_root
        or target.parent != pipeline_root
        or source.name != "judge-diagnostic-v1"
        or target.name != "judge-diagnostic-v2"
    ):
        raise DiagnosticV2ReuseError("diagnostic-v2 roots overlap or drifted")
    prior = _verify_record(payload.get("prior_pipeline_v5_reuse_contract") or {})
    verify_v5_reuse_contract(prior)
    incident = payload.get("diagnostic_v1_incident") or {}
    if (
        incident.get("classification") != "judge_diagnostic_quality_gate_not_passed"
        or incident.get("transport_failed") is not False
        or incident.get("completed_turn_count") != EXPECTED_V1_TURNS
        or incident.get("failed_turn_count") != 0
        or incident.get("unknown_usage_turn_count") != 0
        or incident.get("retry_count") != 0
        or incident.get("usage_status") != "complete"
        or incident.get("usage", {}).get("total_tokens") != 138211
        or incident.get("retry_allowed") is not False
        or incident.get("outputs_admissible_as_passing_evidence") is not False
    ):
        raise DiagnosticV2ReuseError("diagnostic-v1 incident contract drifted")
    for record in (payload.get("diagnostic_v1_artifacts") or {}).values():
        _verify_record(record)
    attempts = payload.get("diagnostic_v1_attempts")
    if not isinstance(attempts, list) or len(attempts) != EXPECTED_V1_TURNS:
        raise DiagnosticV2ReuseError("diagnostic-v1 attempt coverage drifted")
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
    _verify_record(payload.get("truth_audit_receipt") or {})
    _verify_record(payload.get("diagnostic_v2_fixture") or {})
    expected_policy = {
        "diagnostic_v1_replay_allowed": False,
        "diagnostic_v1_output_reuse_as_pass_allowed": False,
        "diagnostic_v2_root_must_be_new": True,
        "one_attempt_per_v2_semantic_turn": True,
        "full_calibration_allowed_before_v2_pass": False,
        "support_ab_ba_allowed": False,
        "production_mutation_allowed": False,
        "pipeline_v1_replay_allowed": False,
        "pipeline_v2_replay_allowed": False,
        "pipeline_v3_replay_allowed": False,
        "pipeline_v4_replay_allowed": False,
    }
    if payload.get("policy") != expected_policy:
        raise DiagnosticV2ReuseError("diagnostic-v2 policy drifted")
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Freeze/verify diagnostic-v2 reuse")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", required=True)
    parser.add_argument("--verify-only", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    output = Path(args.output)
    payload = (
        verify_diagnostic_v2_reuse_contract(output)
        if args.verify_only
        else build_diagnostic_v2_reuse_contract(
            repo_root=Path(args.repo_root), output_path=output
        )
    )
    print(
        json.dumps(
            {
                "ok": True,
                "schema_version": payload["schema_version"],
                "diagnostic_v1_replay_allowed": payload["policy"][
                    "diagnostic_v1_replay_allowed"
                ],
                "output": str(output.expanduser().resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
