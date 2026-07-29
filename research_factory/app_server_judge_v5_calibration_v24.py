from __future__ import annotations

"""Deterministically score v23 after the scoreable canary normalization failure."""

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5 import (
    JudgeV5ProtocolError,
    build_neutral_alignment_input,
    find_observable_alignment_disagreements,
    validate_neutral_alignment_output,
)
from .app_server_judge_v5_calibration import score_v5_calibration
from .app_server_judge_v5_diagnostic import (
    _record,
    _sha256_file,
    _write_immutable_json,
)
from .app_server_judge_v5_fixture_audit_recovery import (
    find_scoreable_alignment_disagreements,
    reconcile_scoreable_alignment,
)
from .util import now_iso


V24_ADOPTION_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v24_adoption_receipt_v1"
V24_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v24_terminal_v1"
DEFAULT_SOURCE_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "judge-calibration-v5_4-reference-v2-capacity-v23/fresh-attempt"
).resolve()
DEFAULT_OUTPUT_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "judge-calibration-v5_4-reference-v2-scoreable-recovery-v24"
).resolve()


class JudgeV5CalibrationV24Error(RuntimeError):
    """The v23 completed-output recovery cannot be trusted."""


def _load_json(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JudgeV5CalibrationV24Error(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise JudgeV5CalibrationV24Error(f"{purpose} is not an object")
    return value


def _verify_record(record: Mapping[str, Any]) -> Path:
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise JudgeV5CalibrationV24Error("v23 source record drifted")
    return path


def _failed_quality_gates(score: Mapping[str, Any]) -> list[str]:
    checks = score.get("checks")
    if not isinstance(checks, Mapping):
        raise JudgeV5CalibrationV24Error("calibration score is missing checks")
    return sorted(str(key) for key, passed in checks.items() if passed is not True)


def _verify_v23_terminal(source_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    terminal = _load_json(source_root / "terminal.json", "v23 inner terminal")
    failure_record = terminal.get("failure")
    if (
        terminal.get("schema_version")
        != "pif_app_server_judge_v5_4_calibration_terminal_v3"
        or terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("calibration_passed") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or not isinstance(failure_record, Mapping)
    ):
        raise JudgeV5CalibrationV24Error("v23 terminal is not an adoptable local failure")
    failure_path = _verify_record(failure_record)
    failure = _load_json(failure_path, "v23 failure")
    attempts = failure.get("attempts")
    if (
        failure.get("schema_version")
        != "pif_app_server_judge_v5_4_calibration_failure_v3"
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("error_class") != "JudgeV5ProtocolError"
        or failure.get("failed_turn_name") != "neutral_alignment_canary"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("score_authorized") is not False
        or failure.get("selection_authorized") is not False
        or failure.get("accounting_complete") is not True
        or failure.get("usage_status") != "complete"
        or failure.get("unknown_usage_turn_count") != 0
        or not isinstance(attempts, list)
        or len(attempts) != 23
    ):
        raise JudgeV5CalibrationV24Error("v23 failure is not an adoptable canary failure")
    for attempt in attempts:
        if (
            attempt.get("state") != "completed"
            or attempt.get("status") != "completed"
            or attempt.get("usage_status") != "measured"
            or attempt.get("error_class") is not None
        ):
            raise JudgeV5CalibrationV24Error("v23 attempt coverage is incomplete")
        for key in ("capacity", "output", "sidecar"):
            record = attempt.get(key)
            if not isinstance(record, Mapping):
                raise JudgeV5CalibrationV24Error("v23 attempt record is incomplete")
            _verify_record(record)
    if (source_root / "turns/disagreement-adjudication").exists():
        raise JudgeV5CalibrationV24Error("v23 unexpectedly started adjudication")
    return terminal, failure


def build_v24_scoreable_recovery(
    *, source_root: Path = DEFAULT_SOURCE_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT
) -> dict[str, Any]:
    source = source_root.expanduser().resolve()
    root = output_root.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v24 terminal")
    root.mkdir(parents=True, exist_ok=True)

    terminal, failure = _verify_v23_terminal(source)
    load = lambda name: _load_json(source / name, f"v23 {name}")
    pool = load("shared-witness-pool.private.json")
    truth = load("calibration-truth.private.json")
    support_receipts = load("support-receipts.private.json")
    pointwise_output = load("pointwise-output-full.private.json")
    base_output = load("base-alignment-full.private.json")
    canary_output = _load_json(
        source / "turns/neutral-alignment-canary/output.private.json",
        "v23 canary output",
    )
    base_input = build_neutral_alignment_input(pool, support_receipts)
    canary_input = build_neutral_alignment_input(
        pool,
        support_receipts,
        case_ids=truth["canary_case_ids"],
        permutation="balanced_canary",
    )

    strict_canary_errors = validate_neutral_alignment_output(canary_output, canary_input)
    try:
        find_observable_alignment_disagreements(
            base_output=base_output,
            base_input=base_input,
            canary_output=canary_output,
            canary_input=canary_input,
            support_receipts=support_receipts,
        )
    except JudgeV5ProtocolError as exc:
        strict_failure_message = str(exc)
    else:
        raise JudgeV5CalibrationV24Error("v23 strict canary failure no longer reproduces")
    expected_message = (
        "invalid alignment output: case_26_pair_0_unsupported_without_specific_root; "
        "case_38_pair_0_unsupported_without_specific_root"
    )
    if strict_failure_message != expected_message:
        raise JudgeV5CalibrationV24Error("v23 strict canary failure drifted")

    disagreements = find_scoreable_alignment_disagreements(
        base_output=base_output,
        base_input=base_input,
        canary_output=canary_output,
        canary_input=canary_input,
        support_receipts=support_receipts,
    )
    if disagreements.get("adjudication_required") is not False:
        raise JudgeV5CalibrationV24Error("v24 unexpectedly requires semantic adjudication")
    disagreements_path = root / "observable-disagreements.private.json"
    _write_immutable_json(disagreements_path, disagreements)
    reconciled = reconcile_scoreable_alignment(
        base_output=base_output,
        base_input=base_input,
        disagreements=disagreements,
        adjudication_output=None,
        adjudication_input=None,
    )
    reconciled_path = root / "reconciled-alignment.private.json"
    _write_immutable_json(reconciled_path, reconciled)
    score = score_v5_calibration(
        pointwise_output=pointwise_output,
        reconciled_alignment=reconciled,
        expected=truth,
        observable_disagreements=disagreements,
    )
    score_path = root / "calibration-score.json"
    _write_immutable_json(score_path, score)
    failed_gates = _failed_quality_gates(score)

    adoption = {
        "schema_version": V24_ADOPTION_RECEIPT_VERSION,
        "status": "v23_complete_outputs_adopted_for_deterministic_scoreable_recovery",
        "created_at": now_iso(),
        "source_terminal": _record(source / "terminal.json"),
        "source_failure": dict(terminal["failure"]),
        "source_attempt_count": 23,
        "source_usage": failure["usage"],
        "source_usage_status": "complete",
        "source_unknown_usage_turn_count": 0,
        "source_semantic_turn_replay_allowed": False,
        "all_completed_outputs_adopted": True,
        "new_semantic_turn_count": 0,
        "new_semantic_usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "total_tokens": 0,
        },
        "deterministic_normalization_only": True,
        "scoreable_root_omissions_preserved_as_model_errors": True,
        "strict_failure_message": strict_failure_message,
        "strict_canary_error_count": len(strict_canary_errors),
        "observable_disagreement_case_count": disagreements[
            "disagreement_case_count"
        ],
        "adjudication_required": False,
        "score_authorized": True,
        "calibration_passed": score["passed"],
        "selection_authorized": False,
        "failed_quality_gates": failed_gates,
        "production_mutation_performed": False,
    }
    adoption_path = root / "v23-adoption-receipt.json"
    _write_immutable_json(adoption_path, adoption)
    v23_usage = failure["usage"]
    terminal_value = {
        "schema_version": V24_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": (
            "judge_full_calibration_passed_selection_authorized"
            if score["passed"]
            else "judge_full_calibration_quality_gate_not_passed"
        ),
        "source_v23_terminal": _record(source / "terminal.json"),
        "source_v23_failure": dict(terminal["failure"]),
        "source_v23_usage": v23_usage,
        "new_recovery_usage": adoption["new_semantic_usage"],
        "cumulative_usage": dict(v23_usage),
        "usage_status": "complete",
        "accounting_complete": True,
        "calibration_passed": score["passed"],
        "selection_authorized": bool(score["passed"]),
        "holdout_authorized": False,
        "semantic_retry_count": 0,
        "semantic_retry_allowed": False,
        "production_mutated": False,
        "v23_adoption_receipt": _record(adoption_path),
        "observable_disagreements": _record(disagreements_path),
        "reconciled_alignment": _record(reconciled_path),
        "score": _record(score_path),
        "failed_quality_gates": failed_gates,
        "metrics": score["metrics"],
        "checks": score["checks"],
        "gates": score["gates"],
    }
    _write_immutable_json(terminal_path, terminal_value)
    return terminal_value


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Deterministically score v23 as v24")
    parser.add_argument("--source-root", default=str(DEFAULT_SOURCE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = build_v24_scoreable_recovery(
        source_root=Path(args.source_root),
        output_root=Path(args.output_root),
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "calibration_passed": terminal["calibration_passed"],
                "selection_authorized": terminal["selection_authorized"],
                "total_tokens": terminal["cumulative_usage"]["total_tokens"],
                "failed_quality_gates": terminal["failed_quality_gates"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal.get("state") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
