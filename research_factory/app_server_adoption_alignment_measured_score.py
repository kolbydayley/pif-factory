from __future__ import annotations

"""Score both measured alignment turns after bounded structural projections."""

import argparse
import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_adoption_alignment_second_turn_recovery as v4
from .util import now_iso


SCHEMA_VERSION = "pif_adoption_alignment_measured_score_v1"
LOCK_VERSION = "pif_adoption_alignment_measured_score_runtime_lock_v1"
TERMINAL_VERSION = "pif_adoption_alignment_measured_score_terminal_v1"
PIPELINE_ROOT = v4.PIPELINE_ROOT
V4_ROOT = v4.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-adoption-output-semantic-evaluation-v1-alignment-measured-score-v5"
).resolve()
EXPECTED_SECOND_ERRORS = [
    "case_0_pair_10_checklist_0_invalid",
    "case_0_pair_10_checklist_10_invalid",
    "case_0_pair_11_checklist_0_invalid",
    "case_0_pair_11_checklist_10_invalid",
]


class MeasuredAlignmentScoreError(RuntimeError):
    """The two measured alignment turns cannot be scored without semantic repair."""


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MeasuredAlignmentScoreError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise MeasuredAlignmentScoreError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _record(path: Path) -> dict[str, Any]:
    return v4._record(path)


def _verify_record(record: Mapping[str, Any]) -> bool:
    return v4._verify_record(record)


def validate_v4_predecessor() -> dict[str, Any]:
    root = V4_ROOT
    alignment = root / "alignment"
    lock_path = alignment / "runtime-lock.json"
    terminal_path = root / "terminal.json"
    paths = v4._turn_paths(alignment, v4.SECOND_TURN_NAME)
    lock = v4.verify_runtime_lock(lock_path)
    terminal = _load_json(terminal_path, "v4 terminal")
    sidecar = _load_json(paths["sidecar"], "v4 second sidecar")
    output = _load_json(paths["output"], "v4 second output")
    frozen = v4._load_frozen(root)
    errors = v4.v3.v2.semantic.judge.validate_neutral_alignment_output(
        output, frozen["second"]["value"]
    )
    if (
        terminal.get("state") != "inactive_incomplete_recovery_required"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("error_class") != "SecondTurnRecoveryError"
        or terminal.get("second_turn_attempted_count") != 1
        or terminal.get("unknown_usage_turn_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or terminal.get("runtime_lock") != _record(lock_path)
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != v4.MODEL
        or sidecar.get("effort") != v4.EFFORT
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("error_class") is not None
        or (sidecar.get("usage") or {}).get("total_tokens") != 78_781
        or errors != EXPECTED_SECOND_ERRORS
        or not paths["capacity"].is_file()
        or not paths["output"].is_file()
        or paths["normalized"].exists()
    ):
        raise MeasuredAlignmentScoreError("v4 measured predecessor contract drifted")
    return {
        "lock": lock,
        "lock_record": _record(lock_path),
        "terminal": terminal,
        "terminal_record": _record(terminal_path),
        "sidecar": sidecar,
        "sidecar_record": _record(paths["sidecar"]),
        "capacity_record": _record(paths["capacity"]),
        "output": output,
        "output_record": _record(paths["output"]),
        "frozen": frozen,
        "v3": v4.validate_v3_predecessor(),
    }


def _without_source_spans(value: Mapping[str, Any]) -> dict[str, Any]:
    payload = copy.deepcopy(value)
    for case in payload.get("cases") or []:
        for pair in case.get("alignment_pairs") or []:
            for item in pair.get("checklist") or []:
                item.pop("source_evidence_spans", None)
    return payload


def project_exact_span_components(
    output: Mapping[str, Any], alignment_input: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    raw_errors = v4.v3.v2.semantic.judge.validate_neutral_alignment_output(
        output, alignment_input
    )
    if raw_errors != EXPECTED_SECOND_ERRORS:
        raise MeasuredAlignmentScoreError("second output has an unapproved defect set")
    source_by_case = {
        str(case["case_id"]): str(case["source_excerpt"])
        for case in alignment_input["cases"]
    }
    projected = copy.deepcopy(output)
    rows = []
    for case_index, case in enumerate(projected["cases"]):
        source = source_by_case[str(case["case_id"])]
        for pair_index, pair in enumerate(case["alignment_pairs"]):
            for checklist_index, item in enumerate(pair["checklist"]):
                replacement = []
                for span in item["source_evidence_spans"]:
                    if span in source:
                        replacement.append(span)
                        continue
                    parts = span.split("\n")
                    if (
                        len(parts) < 2
                        or any(not part or part not in source for part in parts)
                        or "\n".join(parts) != span
                    ):
                        raise MeasuredAlignmentScoreError(
                            "nonexact evidence is not a structural newline join"
                        )
                    replacement.extend(parts)
                    rows.append(
                        {
                            "case_index": case_index,
                            "pair_index": pair_index,
                            "checklist_index": checklist_index,
                            "field": item["field"],
                            "original_span_sha256": hashlib.sha256(
                                span.encode("utf-8")
                            ).hexdigest(),
                            "component_sha256": [
                                hashlib.sha256(part.encode("utf-8")).hexdigest()
                                for part in parts
                            ],
                            "component_count": len(parts),
                        }
                    )
                item["source_evidence_spans"] = replacement
    errors = v4.v3.v2.semantic.judge.validate_neutral_alignment_output(
        projected, alignment_input
    )
    if errors:
        raise MeasuredAlignmentScoreError("exact-span projection did not validate")
    if _without_source_spans(projected) != _without_source_spans(output):
        raise MeasuredAlignmentScoreError("exact-span projection changed semantics")
    audit = {
        "schema_version": SCHEMA_VERSION,
        "raw_error_classes": raw_errors,
        "projection_rule": "split_newline_join_only_when_every_component_is_exact_source_substring",
        "projected_field": "source_evidence_spans",
        "projected_entry_count": len(rows),
        "semantic_payload_changed": False,
        "rows": rows,
        "privacy": "indices counts and hashes only no source or event text",
    }
    return projected, audit


def freeze_and_score(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.is_file():
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_json(terminal_path, "measured score terminal")
    if root.exists() and any(root.iterdir()):
        raise MeasuredAlignmentScoreError("measured score root is not empty")
    predecessor = validate_v4_predecessor()
    root.mkdir(parents=True, exist_ok=True)
    second_projected, second_audit = project_exact_span_components(
        predecessor["output"], predecessor["frozen"]["second"]["value"]
    )
    second_projected_path = root / "second-permutation-projected.private.json"
    second_audit_path = root / "second-permutation-projection-audit.json"
    _write_immutable(second_projected_path, second_projected)
    _write_immutable(second_audit_path, second_audit)
    second_normalized = v4.v3.v2.semantic.judge.normalize_neutral_alignment_output(
        second_projected, predecessor["frozen"]["second"]["value"]
    )
    second_normalized_path = root / "second-permutation-normalized.private.json"
    _write_immutable(second_normalized_path, second_normalized)
    score = v4.v3.v2.semantic.score_alignment(
        base=predecessor["frozen"]["first_normalized"],
        canary=second_normalized,
        mapping=predecessor["frozen"]["mapping"],
    )
    score_path = root / "alignment-score.json"
    _write_immutable(score_path, score)
    receipt_path = root / "structural-projection-authorization.json"
    _write_immutable(
        receipt_path,
        {
            "schema_version": SCHEMA_VERSION,
            "authority": "frozen_deterministic_nonsemantic_projection_policy",
            "allowed_projection_fields": [
                "unpaired_witness_ids",
                "source_evidence_spans",
            ],
            "semantic_decisions_changed": False,
            "request_bytes_changed": False,
            "model_calls_performed": 0,
            "v3_first_output": predecessor["v3"]["first_output_record"],
            "v4_second_output": predecessor["output_record"],
            "frozen_quality_threshold": v4.v3.v2.semantic.QUALITY_THRESHOLD,
            "production_mutation_allowed": False,
        },
    )
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "phase_id": "adoption_existing_output_alignment_measured_score_v5",
        "runtime_adapter": _record(Path(__file__).resolve()),
        "v4_runtime_adapter": _record(Path(v4.__file__).resolve()),
        "v3_runtime_lock": predecessor["v3"]["lock_record"],
        "v3_terminal": predecessor["v3"]["terminal_record"],
        "v3_first_sidecar": predecessor["v3"]["first_sidecar_record"],
        "v3_first_output": predecessor["v3"]["first_output_record"],
        "v4_runtime_lock": predecessor["lock_record"],
        "v4_terminal": predecessor["terminal_record"],
        "v4_second_sidecar": predecessor["sidecar_record"],
        "v4_second_output": predecessor["output_record"],
        "v4_first_projection_audit": _record(
            V4_ROOT / "alignment" / "first-permutation-projection-audit.json"
        ),
        "v4_first_normalized": _record(
            V4_ROOT / "alignment" / "first-permutation-normalized.private.json"
        ),
        "second_projection": _record(second_projected_path),
        "second_projection_audit": _record(second_audit_path),
        "second_normalized": _record(second_normalized_path),
        "score": _record(score_path),
        "projection_authorization": _record(receipt_path),
        "model_calls_performed": 0,
        "request_bytes_changed": False,
        "semantic_decisions_changed": False,
        "frozen_quality_threshold": v4.v3.v2.semantic.QUALITY_THRESHOLD,
        "production_mutation_allowed": False,
    }
    lock_path = root / "runtime-lock.json"
    _write_stable_time(lock_path, lock, "frozen_at")
    verify_runtime_lock(lock_path)
    first_usage = predecessor["v3"]["first_sidecar"]["usage"]
    second_usage = predecessor["sidecar"]["usage"]
    alignment_usage = {
        field: int(first_usage[field]) + int(second_usage[field])
        for field in v4.USAGE_FIELDS
    }
    support_terminal = _load_json(
        v4.v3.v2.SOURCE_ROOT / "support" / "terminal.json", "support terminal"
    )
    support_usage = support_terminal["usage"]
    total_judge_usage = {
        field: int(support_usage[field]) + alignment_usage[field]
        for field in v4.USAGE_FIELDS
    }
    passed = bool(score["passed"])
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "completed" if passed else "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "adoption_semantic_quality_passed_development_winner_freeze_required"
            if passed
            else "development_semantic_quality_gate_not_passed"
        ),
        "zero_token_postprocess": True,
        "model_calls_performed": 0,
        "measured_alignment_turn_count": 2,
        "semantic_retry_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "first_turn_usage": first_usage,
        "second_turn_usage": second_usage,
        "alignment_usage": alignment_usage,
        "support_usage": support_usage,
        "total_judge_usage": total_judge_usage,
        "development_quality_passed": passed,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "holdout_executed": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "production_amortized_total_token_ratio": v4.v3.v2.semantic.PRODUCTION_TOKEN_RATIO,
        "score": _record(score_path),
        "runtime_lock": _record(lock_path),
        "sidecars": [
            predecessor["v3"]["first_sidecar_record"],
            predecessor["sidecar_record"],
        ],
        "failed_checks": score["failed_checks"],
        "exact_next_action": (
            "freeze the development winner under separate holdout authorization"
            if passed
            else "reject the adoption-output alignment candidate; keep holdout and production closed"
        ),
    }
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return terminal


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "measured score runtime lock")
    predecessor = validate_v4_predecessor()
    if (
        lock.get("schema_version") != LOCK_VERSION
        or lock.get("phase_id") != "adoption_existing_output_alignment_measured_score_v5"
        or lock.get("runtime_adapter") != _record(Path(__file__).resolve())
        or lock.get("v4_runtime_adapter") != _record(Path(v4.__file__).resolve())
        or lock.get("v3_runtime_lock") != predecessor["v3"]["lock_record"]
        or lock.get("v3_terminal") != predecessor["v3"]["terminal_record"]
        or lock.get("v3_first_sidecar") != predecessor["v3"]["first_sidecar_record"]
        or lock.get("v3_first_output") != predecessor["v3"]["first_output_record"]
        or lock.get("v4_runtime_lock") != predecessor["lock_record"]
        or lock.get("v4_terminal") != predecessor["terminal_record"]
        or lock.get("v4_second_sidecar") != predecessor["sidecar_record"]
        or lock.get("v4_second_output") != predecessor["output_record"]
        or lock.get("model_calls_performed") != 0
        or lock.get("request_bytes_changed") is not False
        or lock.get("semantic_decisions_changed") is not False
        or lock.get("frozen_quality_threshold") != v4.v3.v2.semantic.QUALITY_THRESHOLD
        or lock.get("production_mutation_allowed") is not False
    ):
        raise MeasuredAlignmentScoreError("measured score runtime lock drifted")
    for field in (
        "v4_first_projection_audit",
        "v4_first_normalized",
        "second_projection",
        "second_projection_audit",
        "second_normalized",
        "score",
        "projection_authorization",
    ):
        if not _verify_record(lock[field]):
            raise MeasuredAlignmentScoreError(f"measured score {field} drifted")
    return lock


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Score the two measured alignment turns")
    parser.add_argument("action", choices=("finalize", "verify"))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    root = Path(args.output_dir)
    if args.action == "finalize":
        result = freeze_and_score(output_dir=root)
    else:
        verify_runtime_lock(root / "runtime-lock.json")
        result = {"state": "verified"}
    sanitized = {
        key: result.get(key)
        for key in (
            "state",
            "terminal_reason",
            "usage_status",
            "development_quality_passed",
            "development_winner_frozen",
            "holdout_authorized",
            "holdout_executed",
            "production_mutated",
        )
        if key in result
    }
    print(json.dumps(sanitized, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
