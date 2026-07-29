from __future__ import annotations

"""Zero-token projection and non-acceptance for the completed v58 turns."""

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v55_structured import validate_v55_output
from .app_server_judge_v5_calibration_v56_structured import sanitize_nonexact_spans
from .app_server_judge_v5_calibration_v57_repair import score_v57
from .app_server_judge_v5_calibration_v58_recovery import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V58_ROOT,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _record, _sha256_file, _validate_usage
from .util import now_iso


V59_AUDIT_VERSION = "pif_app_server_judge_v5_4_v59_frozen_support_projection_audit_v1"
V59_SCORE_VERSION = "pif_app_server_judge_v5_4_v59_projected_root_score_v1"
V59_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v59_projection_nonacceptance_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V58_ROOT.parent / "judge-calibration-v5_4-v59-frozen-support-projection"
).resolve()


class JudgeV5CalibrationV59ProjectionError(RuntimeError):
    """The completed v58 evidence cannot be projected without semantic replay."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _validate_v58(v58_root: Path) -> dict[str, Any]:
    paths = {
        "v58_terminal": v58_root / "terminal.json",
        "v58_failure": v58_root / "failure.json",
        "v58_spec": v58_root / "root-projection-recovery-spec.json",
        "v58_taxonomy": v58_root / "error-taxonomy.json",
        "v58_truth": v58_root / "diagnostic-truth.private.json",
    }
    for index in range(2):
        turn = v58_root / "turns" / f"root-projection-shard-{index:02d}"
        for kind, filename in (
            ("input", "input.private.json"),
            ("raw_output", "output.private.json"),
            ("capacity", "capacity.json"),
            ("sidecar", "sidecar.json"),
        ):
            paths[f"v58_shard{index:02d}_{kind}"] = turn / filename
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v58_terminal"]
    failure = values["v58_failure"]
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or not _record_matches(terminal.get("failure"), paths["v58_failure"])
        or failure.get("failed_turn_name") != "root_projection_shard_01"
        or failure.get("error_class") != "JudgeV5CalibrationV26DiagnosticError"
        or failure.get("usage_status") != "complete"
        or failure.get("accounting_complete") is not True
        or failure.get("retry_allowed_in_this_version") is not False
    ):
        raise JudgeV5CalibrationV59ProjectionError("v58 failure is inadmissible")
    attempts = failure.get("attempts") or []
    if len(attempts) != 2:
        raise JudgeV5CalibrationV59ProjectionError("v58 attempt coverage drifted")
    summed = {field: 0 for field in USAGE_FIELDS}
    for index, attempt in enumerate(attempts):
        expected_name = f"root_projection_shard_{index:02d}"
        sidecar = values[f"v58_shard{index:02d}_sidecar"]
        capacity = values[f"v58_shard{index:02d}_capacity"]
        usage = _validate_usage(sidecar)
        for field in USAGE_FIELDS:
            summed[field] += usage[field]
        if (
            attempt.get("turn_name") != expected_name
            or attempt.get("state") != "completed"
            or attempt.get("status") != "completed"
            or attempt.get("usage_status") != "measured"
            or sidecar.get("auth_type") != "chatgpt"
            or capacity.get("cleared_for_semantic_turn") is not True
            or capacity.get("managed_chatgpt_auth_verified") is not True
            or capacity.get("rate_limit_reached_type") is not None
            or not _record_matches(attempt.get("sidecar"), paths[f"v58_shard{index:02d}_sidecar"])
            or not _record_matches(attempt.get("capacity"), paths[f"v58_shard{index:02d}_capacity"])
            or not _record_matches(attempt.get("output"), paths[f"v58_shard{index:02d}_raw_output"])
        ):
            raise JudgeV5CalibrationV59ProjectionError("v58 measured turn drifted")
    if summed != terminal.get("usage") or summed != failure.get("usage"):
        raise JudgeV5CalibrationV59ProjectionError("v58 usage aggregation drifted")
    return {name: _record(path) for name, path in paths.items()}


def project_v59(
    v58_root: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    taxonomy = _load_json(v58_root / "error-taxonomy.json", "v58 taxonomy")
    truth = _load_json(v58_root / "diagnostic-truth.private.json", "v58 truth")
    units = []
    operations = []
    for index in range(2):
        turn = v58_root / "turns" / f"root-projection-shard-{index:02d}"
        value = _load_json(turn / "input.private.json", f"v58 shard{index:02d} input")
        raw = _load_json(turn / "output.private.json", f"v58 shard{index:02d} output")
        output, span_operations = sanitize_nonexact_spans(raw, value)
        if span_operations:
            raise JudgeV5CalibrationV59ProjectionError("v58 root projection has nonexact spans")
        expected = {
            (row["case_id"], row["witness_id"]): {
                "supported": "same",
                "unsupported": "different",
                "abstain": "abstain",
            }[row["frozen_proposition_verdict"]]
            for row in value["units"]
        }
        for row in output["units"]:
            key = (row["case_id"], row["witness_id"])
            checklist = {item["field"]: item for item in row["checklist"]}
            prior = checklist["unsupported_inference"]["decision"]
            projected = expected[key]
            if prior != projected:
                checklist["unsupported_inference"]["decision"] = projected
                operations.append(
                    {
                        "case_id": key[0],
                        "witness_id": key[1],
                        "field": "unsupported_inference",
                        "prior_decision": prior,
                        "projected_decision": projected,
                        "projection_source": "frozen_llm_proposition_receipt",
                    }
                )
            decisions = {item["decision"] for item in row["checklist"]}
            row["structured_field_verdict"] = (
                "incorrect"
                if "different" in decisions
                else "abstain"
                if "abstain" in decisions
                else "correct"
            )
        if validate_v55_output(output, value):
            raise JudgeV5CalibrationV59ProjectionError("projected v58 output remained invalid")
        units.extend(output["units"])
    if len(operations) != 2:
        raise JudgeV5CalibrationV59ProjectionError("v59 projection count drifted")
    merged = {"units": units}
    score = score_v57(merged, truth, taxonomy)
    score["schema_version"] = V59_SCORE_VERSION
    return merged, operations, score, taxonomy


def run_v59_projection(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, v58_root: Path = DEFAULT_V58_ROOT
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v59 terminal")
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v58(v58_root.resolve())
    merged, operations, score, _taxonomy = project_v59(v58_root.resolve())
    output_path = root / "projected-root-output.private.json"
    _write_immutable(output_path, merged)
    audit = {
        "schema_version": V59_AUDIT_VERSION,
        "created_at": now_iso(),
        "operation_count": len(operations),
        "operations": operations,
        "new_semantic_model_calls": 0,
        "new_semantic_tokens": 0,
        "semantic_source": "frozen_llm_proposition_receipts_only",
        "privacy": "opaque_ids_enums_counts_only",
    }
    audit_path = root / "projection-audit.json"
    _write_immutable(audit_path, audit)
    score["projection_audit"] = _record(audit_path)
    score_path = root / "projected-root-score.json"
    _write_immutable(score_path, score)
    predecessor_terminal = _load_json(v58_root / "terminal.json", "v58 terminal")
    terminal = {
        "schema_version": V59_TERMINAL_VERSION,
        "state": "inactive",
        "terminal_at": now_iso(),
        "terminal_reason": "inactive_incomplete_recovery_required",
        "development_terminal_reason": "v59_projected_root_quality_gate_not_passed",
        "overall_evaluation_complete": False,
        "projection_passed": False,
        "alternate_structured_specialist_diagnostic_authorized": True,
        "integrated_diagnostic_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "new_semantic_turn_count": 0,
        "new_usage": {field: 0 for field in USAGE_FIELDS},
        "predecessor_usage": predecessor_terminal["usage"],
        "intent_to_treat_usage": predecessor_terminal["usage"],
        "usage_status": "complete",
        "accounting_complete": True,
        "predecessor": predecessor,
        "output": _record(output_path),
        "projection_audit": _record(audit_path),
        "score": _record(score_path),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Project and score completed v58 turns")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v58-root", default=str(DEFAULT_V58_ROOT))
    args = parser.parse_args(argv)
    terminal = run_v59_projection(output_dir=Path(args.output_dir), v58_root=Path(args.v58_root))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "projection_passed": terminal["projection_passed"],
                "new_semantic_turn_count": terminal["new_semantic_turn_count"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
