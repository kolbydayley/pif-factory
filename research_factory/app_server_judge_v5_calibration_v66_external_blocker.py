from __future__ import annotations

"""Prepare the private external reference-owner packet after v65 non-acceptance."""

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5 import CHECKLIST_FIELDS
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v64_reference_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V64_ROOT,
)
from .app_server_judge_v5_calibration_v65_fresh_structured import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V65_ROOT,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _record, _sha256_file, _validate_usage
from .util import now_iso


V66_PACKET_VERSION = "pif_app_server_judge_v5_4_v66_reference_owner_packet_v1"
V66_SUMMARY_VERSION = "pif_app_server_judge_v5_4_v66_external_blocker_summary_v1"
V66_DECISION_SCHEMA_VERSION = "pif_app_server_judge_v5_4_v66_reference_owner_decision_schema_v1"
V66_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v66_external_blocker_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V65_ROOT.parent / "judge-calibration-v5_4-v66-external-reference-blocker"
).resolve()


class JudgeV5CalibrationV66BlockerError(RuntimeError):
    """The external reference-owner blocker cannot be prepared safely."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _validate_predecessors(v64_root: Path, v65_root: Path) -> dict[str, Any]:
    paths = {
        "v64_terminal": v64_root / "terminal.json",
        "v64_truth": v64_root / "calibration-truth.private.json",
        "v64_receipt": v64_root / "reference-receipt.json",
        "v64_audit": v64_root / "reference-patch-audit.json",
        "v65_terminal": v65_root / "terminal.json",
        "v65_spec": v65_root / "fresh-structured-spec.json",
        "v65_score": v65_root / "fresh-structured-score.json",
        "v65_input": v65_root / "fresh-structured-input.private.json",
        "v65_truth": v65_root / "diagnostic-truth.private.json",
        "v65_output": v65_root / "fresh-structured-output.private.json",
        "v65_audit": v65_root / "contract-projection-audit.json",
    }
    for index in range(3):
        turn = v65_root / "turns" / f"fresh-root-status-shard-{index:02d}"
        paths[f"v65_shard{index:02d}_capacity"] = turn / "capacity.json"
        paths[f"v65_shard{index:02d}_sidecar"] = turn / "sidecar.json"
        paths[f"v65_shard{index:02d}_raw_output"] = turn / "output.private.json"
        paths[f"v65_shard{index:02d}_projected_output"] = turn / "projected-output.private.json"
    values = {name: _load_json(path, name) for name, path in paths.items()}
    v64 = values["v64_terminal"]
    v65 = values["v65_terminal"]
    score = values["v65_score"]
    if (
        v64.get("state") != "completed"
        or v64.get("reference_frozen") is not True
        or v64.get("fresh_diagnostic_authorized") is not True
        or v64.get("production_mutated") is not False
        or not _record_matches(v64.get("truth"), paths["v64_truth"])
        or not _record_matches(v64.get("reference_receipt"), paths["v64_receipt"])
        or not _record_matches(v64.get("patch_audit"), paths["v64_audit"])
        or v65.get("state") != "inactive"
        or v65.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or v65.get("development_terminal_reason") != "v65_fresh_structured_quality_gate_not_passed"
        or v65.get("fresh_structured_diagnostic_passed") is not False
        or v65.get("full_calibration_authorized") is not False
        or v65.get("usage_status") != "complete"
        or v65.get("accounting_complete") is not True
        or v65.get("semantic_retry_count") != 0
        or v65.get("production_mutated") is not False
        or not _record_matches(v65.get("score"), paths["v65_score"])
        or not _record_matches(v65.get("output"), paths["v65_output"])
        or not _record_matches(v65.get("contract_projection_audit"), paths["v65_audit"])
        or score.get("passed") is not False
        or score.get("metrics", {}).get("witness_count") != 36
        or score.get("metrics", {}).get("exact_case_rate") != 0.722222
        or score.get("metrics", {}).get("root_field_f1") != 0.846847
    ):
        raise JudgeV5CalibrationV66BlockerError("v64/v65 predecessors are inadmissible")
    attempts = v65.get("attempts") or []
    if len(attempts) != 3:
        raise JudgeV5CalibrationV66BlockerError("v65 attempt coverage drifted")
    total = {field: 0 for field in USAGE_FIELDS}
    for index, attempt in enumerate(attempts):
        sidecar = values[f"v65_shard{index:02d}_sidecar"]
        capacity = values[f"v65_shard{index:02d}_capacity"]
        usage = _validate_usage(sidecar)
        for field in USAGE_FIELDS:
            total[field] += usage[field]
        if (
            attempt.get("turn_name") != f"fresh_root_status_shard_{index:02d}"
            or attempt.get("state") != "completed"
            or attempt.get("usage_status") != "measured"
            or sidecar.get("model") != "gpt-5.5"
            or sidecar.get("auth_type") != "chatgpt"
            or capacity.get("cleared_for_semantic_turn") is not True
            or capacity.get("managed_chatgpt_auth_verified") is not True
            or capacity.get("rate_limit_reached_type") is not None
            or not _record_matches(attempt.get("sidecar"), paths[f"v65_shard{index:02d}_sidecar"])
            or not _record_matches(attempt.get("capacity"), paths[f"v65_shard{index:02d}_capacity"])
            or not _record_matches(attempt.get("output"), paths[f"v65_shard{index:02d}_raw_output"])
        ):
            raise JudgeV5CalibrationV66BlockerError("v65 measured turn drifted")
    if total != v65.get("usage"):
        raise JudgeV5CalibrationV66BlockerError("v65 usage aggregation drifted")
    return {name: _record(path) for name, path in paths.items()}


def build_v66_packet(
    value: Mapping[str, Any], output: Mapping[str, Any], truth: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    sources = {
        (row["case_id"], row["witness_id"]): row for row in value.get("units") or []
    }
    rows = {
        (row["case_id"], row["witness_id"]): row for row in output.get("units") or []
    }
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    units = []
    summary = []
    for key in sorted(rows):
        observed = {
            item["field"]
            for item in rows[key]["checklist"]
            if item["independent_root_status"] == "root"
        }
        if observed == expected[key]:
            continue
        source = sources[key]
        units.append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "source_excerpt": source["source_excerpt"],
                "structured_event": deepcopy(source["structured_event"]),
                "frozen_proposition_verdict": source["frozen_proposition_verdict"],
                "current_reference_root_fields": sorted(expected[key]),
                "fresh_judge_root_fields": sorted(observed),
                "fresh_judge_checklist": deepcopy(rows[key]["checklist"]),
            }
        )
        summary.append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "current_reference_root_fields": sorted(expected[key]),
                "fresh_judge_root_fields": sorted(observed),
                "reference_only_fields": sorted(expected[key] - observed),
                "judge_only_fields": sorted(observed - expected[key]),
            }
        )
    if len(units) != 10:
        raise JudgeV5CalibrationV66BlockerError("v66 residual count drifted")
    packet = {
        "schema_version": V66_PACKET_VERSION,
        "created_at": now_iso(),
        "reference_version": truth["reference_version"],
        "unit_count": len(units),
        "units": units,
        "required_decision_contract": {
            "reviewer_type": "human_reference_owner",
            "coverage": "exactly_all_packet_units",
            "fields": [
                "case_id",
                "witness_id",
                "final_root_fields",
                "decision_basis",
                "reviewer_type",
                "reviewed_at",
            ],
            "field_enum": list(CHECKLIST_FIELDS),
        },
        "privacy": "private_source_and_event_content_do_not_publish",
    }
    sanitized = {
        "schema_version": V66_SUMMARY_VERSION,
        "created_at": now_iso(),
        "blocker_class": "external_reference_owner_adjudication_required",
        "residual_unit_count": len(summary),
        "residuals": summary,
        "why_external": (
            "v63 changed five reference rows using the Sol reference owner; a fresh gpt-5.5 pass "
            "still disagreed on ten rows. Further automatic truth mutation from judge output would "
            "be circular and cannot establish calibrated ground truth."
        ),
        "safe_local_semantic_experiment_remaining": False,
        "privacy": "opaque_ids_and_field_enums_only",
    }
    return packet, sanitized


def decision_schema() -> dict[str, Any]:
    return {
        "schema_version": V66_DECISION_SCHEMA_VERSION,
        "type": "object",
        "additionalProperties": False,
        "required": ["reference_version", "reviewer_type", "reviewed_at", "decisions"],
        "properties": {
            "reference_version": {"type": "string"},
            "reviewer_type": {"type": "string", "const": "human_reference_owner"},
            "reviewed_at": {"type": "string"},
            "decisions": {
                "type": "array",
                "minItems": 10,
                "maxItems": 10,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "case_id",
                        "witness_id",
                        "final_root_fields",
                        "decision_basis",
                    ],
                    "properties": {
                        "case_id": {"type": "string"},
                        "witness_id": {"type": "string"},
                        "final_root_fields": {
                            "type": "array",
                            "items": {"type": "string", "enum": list(CHECKLIST_FIELDS)},
                        },
                        "decision_basis": {"type": "string", "minLength": 1},
                    },
                },
            },
        },
    }


def freeze_v66_blocker(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v64_root: Path = DEFAULT_V64_ROOT,
    v65_root: Path = DEFAULT_V65_ROOT,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v66 terminal")
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors(v64_root.resolve(), v65_root.resolve())
    value = _load_json(v65_root / "fresh-structured-input.private.json", "v65 input")
    output = _load_json(v65_root / "fresh-structured-output.private.json", "v65 output")
    truth = _load_json(v65_root / "diagnostic-truth.private.json", "v65 truth")
    packet, summary = build_v66_packet(value, output, truth)
    packet_path = root / "reference-owner-adjudication.private.json"
    summary_path = root / "blocker-summary.json"
    schema_path = root / "reference-owner-decision-schema.json"
    _write_immutable(packet_path, packet)
    _write_immutable(summary_path, summary)
    _write_immutable(schema_path, decision_schema())
    required_decision_path = root / "reference-owner-decision.private.json"
    if required_decision_path.exists():
        raise JudgeV5CalibrationV66BlockerError("unvalidated reference decision already exists")
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    terminal = {
        "schema_version": V66_TERMINAL_VERSION,
        "state": "blocked",
        "terminal_at": now_iso(),
        "terminal_reason": "external_reference_owner_adjudication_required",
        "overall_evaluation_complete": False,
        "blocker_is_external": True,
        "safe_local_semantic_experiment_remaining": False,
        "required_input": {
            "path": str(required_decision_path),
            "must_not_be_unlabeled_model_output": True,
            "required_reviewer_type": "human_reference_owner",
        },
        "private_packet": _record(packet_path),
        "sanitized_summary": _record(summary_path),
        "decision_schema": _record(schema_path),
        "predecessor": predecessor,
        "judge_passed": False,
        "development_winner_frozen": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "new_semantic_turn_count": 0,
        "new_usage": zero_usage,
        "usage_status": "complete",
        "accounting_complete": True,
        "heartbeat_disable_authorized": True,
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v66 external reference blocker")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v64-root", default=str(DEFAULT_V64_ROOT))
    parser.add_argument("--v65-root", default=str(DEFAULT_V65_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v66_blocker(
        output_dir=Path(args.output_dir),
        v64_root=Path(args.v64_root),
        v65_root=Path(args.v65_root),
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "blocker_is_external": terminal["blocker_is_external"],
                "new_semantic_turn_count": terminal["new_semantic_turn_count"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
