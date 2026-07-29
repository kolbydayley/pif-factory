from __future__ import annotations

"""Executable audit of the calibration-v1 validator-classification failure."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5 import validate_neutral_alignment_output
from .app_server_llm_judge import write_immutable_json


AUDIT_RECEIPT_VERSION = "pif_app_server_judge_v5_4_calibration_v1_audit_receipt_v1"


class CalibrationV1AuditError(ValueError):
    """The calibration-v1 evidence or v2 recovery boundary drifted."""


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
        raise CalibrationV1AuditError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise CalibrationV1AuditError(f"{purpose} is not an object")
    return value


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _verify_record(record: Mapping[str, Any]) -> Path:
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise CalibrationV1AuditError("calibration-v1 record drifted")
    return path


def build_calibration_v1_audit_receipt(
    *, repo_root: Path, output_path: Path
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    control = root / "work/app-server-development-v2/unattended-control-v12"
    calibration = (
        root
        / "work/app-server-development-v2/unattended-pipeline-v5/"
        "judge-calibration-v5_4-v1"
    )
    terminal_receipt_path = control / "terminal-receipt-v12.json"
    terminal_receipt = _load_json(terminal_receipt_path, purpose="v1 terminal receipt")
    if (
        _sha256_file(terminal_receipt_path)
        != "ed31339e3c49918a78a00ef676361fa7ab3afd3e86dcbe23e27598e7a82aed2b"
        or terminal_receipt.get("status") != "failed"
        or terminal_receipt.get("terminal_classification")
        != "infrastructure_or_judge_attempt_failed"
        or terminal_receipt.get("quality_scored") is not False
        or terminal_receipt.get("usage", {}).get("total_tokens") != 496703
        or terminal_receipt.get("completed_turn_count") != 12
        or terminal_receipt.get("canary_started") is not False
        or terminal_receipt.get("calibration_v1_replay_allowed") is not False
        or terminal_receipt.get("calibration_v1_output_reuse_allowed") is not False
    ):
        raise CalibrationV1AuditError("calibration-v1 terminal receipt drifted")
    for record in terminal_receipt["records"].values():
        _verify_record(record)
    failed_root = calibration / "turns/neutral-alignment-base-shard-05"
    output = _load_json(failed_root / "output.private.json", purpose="v1 failed output")
    input_value = _load_json(failed_root / "input.private.json", purpose="v1 failed input")
    errors = validate_neutral_alignment_output(output, input_value)
    if errors != ["case_1_pair_0_unsupported_without_specific_root"]:
        raise CalibrationV1AuditError("calibration-v1 validator failure drifted")
    pair = output["cases"][1]["alignment_pairs"][0]
    decisions = {
        row["field"]: row["decision"] for row in pair["checklist"]
    }
    if (
        decisions.get("event_boundary") != "different"
        or decisions.get("evidence") != "different"
        or decisions.get("unsupported_inference") != "different"
        or pair.get("relation") != "non_equivalent"
    ):
        raise CalibrationV1AuditError("calibration-v1 failed pair shape drifted")
    receipt = {
        "schema_version": AUDIT_RECEIPT_VERSION,
        "terminal_receipt": _record(terminal_receipt_path),
        "failed_input": _record(failed_root / "input.private.json"),
        "failed_output": _record(failed_root / "output.private.json"),
        "failed_sidecar": _record(failed_root / "sidecar.json"),
        "replayed_validator_errors": errors,
        "corrected_classification": "complete_semantic_output_must_be_scored",
        "hard_validator_rule_removed": "unsupported_without_specific_root",
        "structural_schema_validation_retained": True,
        "support_receipt_consistency_validation_retained": True,
        "relation_projection_validation_retained": True,
        "semantic_root_omission_remains_scoreable": True,
        "frozen_judge_prompt_changed": False,
        "frozen_rubric_changed": False,
        "calibration_gates_changed": False,
        "calibration_v1_replay_allowed": False,
        "calibration_v1_output_reuse_allowed": False,
        "calibration_v2_all_turns_must_run_fresh": True,
        "selection_authorized": False,
        "semantic_model_calls_performed": 0,
    }
    write_immutable_json(output_path.expanduser().resolve(), receipt)
    return receipt


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Audit calibration-v1 failure")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    receipt = build_calibration_v1_audit_receipt(
        repo_root=Path(args.repo_root), output_path=Path(args.output)
    )
    print(json.dumps({"ok": True, "schema_version": receipt["schema_version"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
