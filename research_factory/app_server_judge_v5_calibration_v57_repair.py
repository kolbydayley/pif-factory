from __future__ import annotations

"""Bounded root-cause projection diagnostic for the v56 checklist errors."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import CHECKLIST_FIELDS
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
from .app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_TOKENS_PER_TURN,
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v55_structured import (
    score_v55,
    v55_output_schema,
    validate_v55_output,
)
from .app_server_judge_v5_calibration_v56_structured import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V56_ROOT,
    sanitize_nonexact_spans,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _canonical_json,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso


V57_INPUT_VERSION = "pif_app_server_judge_v5_4_v57_root_projection_input_v1"
V57_TRUTH_VERSION = "pif_app_server_judge_v5_4_v57_root_projection_truth_v1"
V57_TAXONOMY_VERSION = "pif_app_server_judge_v5_4_v57_error_taxonomy_v1"
V57_SPEC_VERSION = "pif_app_server_judge_v5_4_v57_root_projection_spec_v1"
V57_SCORE_VERSION = "pif_app_server_judge_v5_4_v57_root_projection_score_v1"
V57_AUDIT_VERSION = "pif_app_server_judge_v5_4_v57_exact_span_sanitization_audit_v1"
V57_FAILURE_VERSION = "pif_app_server_judge_v5_4_v57_root_projection_failure_v1"
V57_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v57_root_projection_terminal_v1"
V57_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V57_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V57_PHASE_ID = "judge_v5_4_v57_root_cause_projection_diagnostic"
TURN_NAMES = ("root_projection_shard_00", "root_projection_shard_01")
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V56_ROOT.parent / "judge-calibration-v5_4-v57-root-projection-diagnostic"
).resolve()


class JudgeV5CalibrationV57RepairError(RuntimeError):
    """The bounded v57 root-projection diagnostic cannot run safely."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _matches_immutable_json(path: Path, value: Any) -> bool:
    rendered = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    return path.is_file() and path.read_text(encoding="utf-8") == rendered


def _validate_predecessor(v56_root: Path) -> dict[str, Any]:
    paths = {
        "v56_terminal": v56_root / "terminal.json",
        "v56_spec": v56_root / "structured-checklist-continuation-spec.json",
        "v56_score": v56_root / "structured-checklist-score.json",
        "v56_input": v56_root / "structured-checklist-input-full.private.json",
        "v56_truth": v56_root / "diagnostic-truth.private.json",
        "v56_output": v56_root / "structured-checklist-output-full.private.json",
        "v56_sanitization_audit": v56_root / "exact-span-sanitization-audit.json",
        "v56_adopted_output": v56_root / "adopted-shard-00-sanitized.private.json",
    }
    for index in range(1, 4):
        turn = v56_root / "turns" / f"structured-checklist-shard-{index:02d}"
        paths[f"v56_shard{index:02d}_input"] = turn / "input.private.json"
        paths[f"v56_shard{index:02d}_capacity"] = turn / "capacity.json"
        paths[f"v56_shard{index:02d}_sidecar"] = turn / "sidecar.json"
        paths[f"v56_shard{index:02d}_raw_output"] = turn / "output.private.json"
        paths[f"v56_shard{index:02d}_sanitized_output"] = (
            turn / "sanitized-output.private.json"
        )
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v56_terminal"]
    score = values["v56_score"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v56_structured_checklist_quality_gate_not_passed"
        or terminal.get("structured_checklist_passed") is not False
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or terminal.get("new_turn_count") != 3
        or not _record_matches(terminal.get("score"), paths["v56_score"])
        or not _record_matches(terminal.get("output"), paths["v56_output"])
        or not _record_matches(
            terminal.get("sanitization_audit"), paths["v56_sanitization_audit"]
        )
        or score.get("passed") is not False
        or score.get("failed_checks")
        != [
            "attribution_specificity",
            "checklist_row_accuracy",
            "field_issue_f1",
            "structured_field_accuracy",
        ]
    ):
        raise JudgeV5CalibrationV57RepairError("v56 terminal is not admissible")
    attempts = terminal.get("attempts") or []
    if (
        len(attempts) != 3
        or [attempt.get("turn_name") for attempt in attempts]
        != [f"structured_checklist_shard_{index:02d}" for index in range(1, 4)]
    ):
        raise JudgeV5CalibrationV57RepairError("v56 attempt coverage drifted")
    reconstructed_units = []
    reconstructed_operations = []
    for index in range(1, 4):
        sidecar = values[f"v56_shard{index:02d}_sidecar"]
        capacity = values[f"v56_shard{index:02d}_capacity"]
        attempt = attempts[index - 1]
        reconstructed, operations = sanitize_nonexact_spans(
            values[f"v56_shard{index:02d}_raw_output"],
            values[f"v56_shard{index:02d}_input"],
        )
        _validate_usage(sidecar)
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("usage_status") != "measured"
            or sidecar.get("auth_type") != "chatgpt"
            or capacity.get("cleared_for_semantic_turn") is not True
            or capacity.get("managed_chatgpt_auth_verified") is not True
            or capacity.get("rate_limit_reached_type") is not None
            or not _record_matches(
                attempt.get("capacity"), paths[f"v56_shard{index:02d}_capacity"]
            )
            or not _record_matches(
                attempt.get("sidecar"), paths[f"v56_shard{index:02d}_sidecar"]
            )
            or not _record_matches(
                attempt.get("output"), paths[f"v56_shard{index:02d}_raw_output"]
            )
            or reconstructed != values[f"v56_shard{index:02d}_sanitized_output"]
            or not _matches_immutable_json(
                paths[f"v56_shard{index:02d}_sanitized_output"],
                values[f"v56_shard{index:02d}_sanitized_output"],
            )
        ):
            raise JudgeV5CalibrationV57RepairError("v56 turn evidence is inadmissible")
        reconstructed_units.extend(reconstructed["units"])
        reconstructed_operations.extend(operations)
    sanitization_audit = values["v56_sanitization_audit"]
    audit_operations = sanitization_audit.get("operations") or []
    if (
        not _matches_immutable_json(
            paths["v56_adopted_output"], values["v56_adopted_output"]
        )
        or
        {
            "units": [
                *values["v56_adopted_output"]["units"],
                *reconstructed_units,
            ]
        }
        != values["v56_output"]
        or len(audit_operations) != sanitization_audit.get("dropped_span_count")
        or (
            bool(reconstructed_operations)
            and audit_operations[-len(reconstructed_operations) :] != reconstructed_operations
        )
        or sanitization_audit.get("semantic_decisions_changed") is not False
        or sanitization_audit.get("rationales_changed") is not False
    ):
        raise JudgeV5CalibrationV57RepairError("v56 sanitization lineage drifted")
    return {name: _record(path) for name, path in paths.items()}


def _row_maps(
    output: Mapping[str, Any], truth: Mapping[str, Any]
) -> tuple[dict[tuple[str, str], Mapping[str, Any]], dict[tuple[str, str], set[str]], dict[tuple[str, str], str]]:
    rows = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in output.get("units") or []
    }
    fields: dict[tuple[str, str], set[str]] = {}
    verdicts: dict[tuple[str, str], str] = {}
    for case_id, case in (truth.get("cases") or {}).items():
        for witness_id, issues in case["field_issues"].items():
            key = (str(case_id), str(witness_id))
            fields[key] = set(issues)
            verdicts[key] = str(case["structured_fields"][witness_id])
    if set(rows) != set(fields):
        raise JudgeV5CalibrationV57RepairError("v56 output/truth coverage drifted")
    return rows, fields, verdicts


def build_v57_taxonomy(
    output: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    rows, expected_fields, expected_verdicts = _row_maps(output, truth)
    failures = []
    exact_empty_controls = []
    exact_nonempty_controls = []
    tp = fp = fn = verdict_correct = row_correct = 0
    for key in sorted(rows):
        row = rows[key]
        observed = {
            item["field"] for item in row["checklist"] if item["decision"] == "different"
        }
        expected = expected_fields[key]
        false_positive = sorted(observed - expected)
        false_negative = sorted(expected - observed)
        tp += len(observed & expected)
        fp += len(false_positive)
        fn += len(false_negative)
        verdict_correct += int(row["structured_field_verdict"] == expected_verdicts[key])
        row_correct += len(CHECKLIST_FIELDS) - len(false_positive) - len(false_negative)
        summary = {
            "case_id": key[0],
            "witness_id": key[1],
            "expected_fields": sorted(expected),
            "observed_fields": sorted(observed),
            "false_positive_fields": false_positive,
            "false_negative_fields": false_negative,
            "expected_verdict": expected_verdicts[key],
            "observed_verdict": row["structured_field_verdict"],
        }
        if false_positive or false_negative:
            if false_positive and not false_negative:
                causal_class = (
                    "isolated_false_positive"
                    if not expected
                    else "derivative_field_overreport"
                )
            elif false_negative and not false_positive:
                causal_class = "root_field_omission"
            else:
                causal_class = "mixed_overreport_and_omission"
            failures.append({**summary, "likely_causal_class": causal_class})
        elif not expected:
            exact_empty_controls.append(summary)
        else:
            exact_nonempty_controls.append(summary)
    if len(failures) != 16 or not exact_empty_controls or not exact_nonempty_controls:
        raise JudgeV5CalibrationV57RepairError("v57 diagnostic cohort invariant drifted")
    controls = [exact_empty_controls[0], exact_nonempty_controls[0]]
    selected = [
        {"case_id": row["case_id"], "witness_id": row["witness_id"], "role": "failure"}
        for row in failures
    ] + [
        {"case_id": row["case_id"], "witness_id": row["witness_id"], "role": "control"}
        for row in controls
    ]
    field_den = 2 * tp + fp + fn
    total_cells = len(rows) * len(CHECKLIST_FIELDS)
    current_correct_cells = row_correct
    minimum_field_fixes = None
    for fixes in range(fp + fn + 1):
        if any(
            (2 * (tp + fn_fix))
            / (2 * (tp + fn_fix) + (fp - fp_fix) + (fn - fn_fix))
            >= 0.95
            for fp_fix in range(min(fp, fixes) + 1)
            for fn_fix in [fixes - fp_fix]
            if fn_fix <= fn
        ):
            minimum_field_fixes = fixes
            break
    return {
        "schema_version": V57_TAXONOMY_VERSION,
        "created_at": now_iso(),
        "source_witness_count": len(rows),
        "failed_witness_count": len(failures),
        "failures": failures,
        "controls": controls,
        "selected_units": selected,
        "selected_unit_count": len(selected),
        "current_counts": {
            "field_true_positive": tp,
            "field_false_positive": fp,
            "field_false_negative": fn,
            "verdict_correct": verdict_correct,
            "checklist_cells_correct": current_correct_cells,
            "checklist_cells_total": total_cells,
            "field_f1": round((2 * tp) / field_den, 6),
        },
        "minimum_corrections_to_frozen_gates": {
            "field_decisions": minimum_field_fixes,
            "structured_verdicts": max(0, math.ceil(0.95 * len(rows)) - verdict_correct),
            "checklist_cells": max(0, math.ceil(0.99 * total_cells) - current_correct_cells),
            "attribution_false_positives": 8,
        },
        "privacy": "opaque_ids_enums_counts_only_no_source_or_event_text",
    }


def build_v57_input(
    full_input: Mapping[str, Any], output: Mapping[str, Any], taxonomy: Mapping[str, Any]
) -> dict[str, Any]:
    inputs = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in full_input.get("units") or []
    }
    outputs = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in output.get("units") or []
    }
    units = []
    for selected in taxonomy["selected_units"]:
        key = (selected["case_id"], selected["witness_id"])
        source = inputs[key]
        prior = outputs[key]
        units.append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "source_excerpt": source["source_excerpt"],
                "structured_event": deepcopy(source["structured_event"]),
                "frozen_proposition_verdict": source["frozen_proposition_verdict"],
                "prior_different_fields": [
                    item["field"]
                    for item in prior["checklist"]
                    if item["decision"] == "different"
                ],
            }
        )
    if len(units) != 18:
        raise JudgeV5CalibrationV57RepairError("v57 input must contain 18 units")
    return {
        "schema_version": V57_INPUT_VERSION,
        "units": units,
        "checklist_field_order": list(CHECKLIST_FIELDS),
        "prior_checklist_is_fallible": True,
        "side_labels_present": False,
        "system_identity_present": False,
        "support_receipts_frozen": True,
        "alignment_outputs_present": False,
    }


def build_v57_truth(truth: Mapping[str, Any], taxonomy: Mapping[str, Any]) -> dict[str, Any]:
    selected = {
        (row["case_id"], row["witness_id"]) for row in taxonomy["selected_units"]
    }
    cases: dict[str, Any] = {}
    for case_id, case in truth["cases"].items():
        witnesses = sorted(wid for cid, wid in selected if cid == case_id)
        if witnesses:
            cases[case_id] = {
                "structured_fields": {
                    wid: case["structured_fields"][wid] for wid in witnesses
                },
                "field_issues": {wid: case["field_issues"][wid] for wid in witnesses},
            }
    if sum(len(case["field_issues"]) for case in cases.values()) != 18:
        raise JudgeV5CalibrationV57RepairError("v57 truth coverage drifted")
    return {
        "schema_version": V57_TRUTH_VERSION,
        "witness_count": 18,
        "cases": cases,
        "source_truth_sha256": _sha256_file(DEFAULT_V56_ROOT / "diagnostic-truth.private.json"),
    }


def v57_base_instructions() -> str:
    return (
        "You are a side-free root-cause structured-event verifier. Proposition support is "
        "already frozen and must not be relitigated. Re-evaluate every anonymous event against "
        "its entire source and return all 15 rows. The prior different-field list is fallible "
        "development evidence, never truth. Report only minimal independent truth-conditional "
        "root conflicts. Before marking a field different, substitute source-supported values "
        "for every other root conflict already found; if this field's conflict disappears, mark "
        "it same. An actor or speaker identity conflict does not by itself make attribution, "
        "evidence, event_type, target, or any other field different. attribution is different "
        "only when the reporting/source relation itself is independently wrong. evidence is "
        "different only when the populated evidence is independently inexact or fails to support "
        "the frozen proposition after root substitutions. event_type is different only when the "
        "action or event category itself remains wrong after participant correction. target is "
        "different only when the semantic object or recipient remains wrong after actor/speaker "
        "correction. speaker is who voices or authors the claim; actor is who performs the action "
        "or holds the stance. event_boundary must be same. unsupported_inference must exactly "
        "follow the frozen proposition verdict. Use same when no independent conflict remains, "
        "different for a direct material conflict, and abstain only when the source cannot decide. "
        "Cite exact source substrings or [] for witness-only conflicts. Do not infer origin, vote, "
        "use confidence, regex, keywords, token overlap, or embeddings."
    )


def build_v57_prompt(value: Mapping[str, Any]) -> str:
    return (
        "Return every case_id/witness_id exactly once and every checklist field exactly once in "
        "the supplied order. structured_field_verdict=correct iff all rows are same; incorrect "
        "iff at least one row is different; abstain iff no row is different and at least one row "
        "abstains. Apply the root-substitution test independently before each different decision. "
        "Every nonempty source_evidence_span must be an exact substring; use [] rather than "
        "paraphrasing.\n\n# Root-cause projection units\n"
        + _canonical_json({"units": value["units"]})
        + "\n"
    )


def _shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    units = value["units"]
    chunks = [units[:9], units[9:]]
    if any(len(chunk) != 9 for chunk in chunks):
        raise JudgeV5CalibrationV57RepairError("v57 shard shape drifted")
    return [
        {**{k: deepcopy(v) for k, v in value.items() if k != "units"}, "units": deepcopy(chunk)}
        for chunk in chunks
    ]


def _validate_sanitizable_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping):
        return ["invalid_v57_output_root"]
    sanitized, _operations = sanitize_nonexact_spans(output, value)
    return validate_v55_output(sanitized, value)


def score_v57(output: Mapping[str, Any], truth: Mapping[str, Any], taxonomy: Mapping[str, Any]) -> dict[str, Any]:
    base = score_v55(output, truth)
    selected_roles = {
        (row["case_id"], row["witness_id"]): row["role"]
        for row in taxonomy["selected_units"]
    }
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    rows = {
        (row["case_id"], row["witness_id"]): row for row in output["units"]
    }
    exact = controls_exact = 0
    for key, fields in expected.items():
        observed = {
            item["field"] for item in rows[key]["checklist"] if item["decision"] == "different"
        }
        is_exact = observed == fields
        exact += int(is_exact)
        if selected_roles[key] == "control":
            controls_exact += int(is_exact)
    metrics = dict(base["metrics"])
    metrics.update(
        {
            "exact_case_rate": round(exact / 18, 6),
            "control_exact_rate": round(controls_exact / 2, 6),
        }
    )
    checks = dict(base["checks"])
    checks.update(
        {
            "exact_case_rate": metrics["exact_case_rate"] >= 0.95,
            "control_exact_rate": metrics["control_exact_rate"] == 1.0,
        }
    )
    return {
        "schema_version": V57_SCORE_VERSION,
        "passed": all(checks.values()),
        "metrics": metrics,
        "checks": checks,
        "failed_checks": sorted(key for key, passed in checks.items() if not passed),
        "gates_frozen_before_semantic_calls": True,
        "next_authorization_if_passed": "fresh_integrated_12_case_development_diagnostic_only",
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V57_CAPACITY_AUDIT_VERSION,
        "phase_id": V57_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": len(TURN_NAMES) * MAX_TOKENS_PER_TURN,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v57 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV57RepairError("immutable v57 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V57_CAPACITY_POLICY_VERSION,
        "phase_id": V57_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(TURN_NAMES),
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": len(TURN_NAMES) * MAX_TOKENS_PER_TURN,
        "projected_phase_quota_points": math.ceil(
            len(TURN_NAMES)
            * MAX_TOKENS_PER_TURN
            * QUOTA_POINTS_PER_MILLION_TOKENS
            / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    if policy_path.exists():
        prior = _load_json(policy_path, "v57 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV57RepairError("immutable v57 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v57_repair(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v56_root: Path = DEFAULT_V56_ROOT,
    model: str = "gpt-5.6-luna",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessor(v56_root.resolve())
    full_input = _load_json(v56_root / "structured-checklist-input-full.private.json", "v56 input")
    full_truth = _load_json(v56_root / "diagnostic-truth.private.json", "v56 truth")
    prior_output = _load_json(v56_root / "structured-checklist-output-full.private.json", "v56 output")
    taxonomy = build_v57_taxonomy(prior_output, full_truth)
    value = build_v57_input(full_input, prior_output, taxonomy)
    truth = build_v57_truth(full_truth, taxonomy)
    taxonomy_path = root / "error-taxonomy.json"
    input_path = root / "root-projection-input-full.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    _write_immutable(taxonomy_path, taxonomy)
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    frozen_shards = []
    for turn_name, shard_input in zip(TURN_NAMES, _shards(value)):
        prompt = build_v57_prompt(shard_input)
        schema = v55_output_schema(shard_input)
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=shard_input,
            prompt=prompt,
            schema=schema,
        )
        frozen_shards.append(
            {"turn_name": turn_name, "input": shard_input, "prompt": prompt, "schema": schema, "paths": paths}
        )
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V57_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "bounded_root_cause_projection_over_16_failures_plus_2_controls",
        "case_unit_count": 18,
        "failure_unit_count": 16,
        "control_unit_count": 2,
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "promotion_rule": "pass_authorizes_one_fresh_integrated_12_case_development_diagnostic_only",
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v56_structured.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v55_structured.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "taxonomy": _record(taxonomy_path),
            "full": _record(input_path),
            "truth": _record(truth_path),
            "shards": [
                {
                    "turn_name": shard["turn_name"],
                    "input": _record(shard["paths"]["input"]),
                    "prompt": _record(shard["paths"]["prompt"]),
                    "schema": _record(shard["paths"]["schema"]),
                }
                for shard in frozen_shards
            ],
        },
        "privacy": "private_inputs_prompts_outputs_no_source_text_in_reports",
    }
    spec_path = root / "root-projection-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v57 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV57RepairError("immutable v57 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "taxonomy": taxonomy,
        "truth": truth,
        "shards": frozen_shards,
        "capacity_policy": capacity["policy"],
    }


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = _attempt_records(root)
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    partial = False
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            if attempt.get("capacity") is not None or attempt.get("output") is not None:
                partial = True
            continue
        try:
            usage = _validate_usage(_load_json(Path(record["path"]), "v57 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = not partial and unknown == 0
    failure = {
        "schema_version": V57_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": known if complete else None,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V57_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "integrated_diagnostic_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v57_repair(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v56_root: Path = DEFAULT_V56_ROOT,
    model: str = "gpt-5.6-luna",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v57 terminal")
    frozen = freeze_v57_repair(
        output_dir=root,
        v56_root=v56_root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
    )
    factory = client_factory or _client_factory
    outputs = []
    operations = []
    sidecars = []
    adopted = {}
    current_turn = None
    try:
        async with factory(frozen["capacity_policy"]) as client:
            for shard in frozen["shards"]:
                current_turn = shard["turn_name"]
                output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=shard["paths"],
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                    base_instructions=v57_base_instructions(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["units"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda value, item=shard: _validate_sanitizable_output(value, item["input"]),
                )
                sanitized, shard_operations = sanitize_nonexact_spans(output, shard["input"])
                if validate_v55_output(sanitized, shard["input"]):
                    raise JudgeV5CalibrationV57RepairError("sanitized v57 output remained invalid")
                sanitized_path = shard["paths"]["output"].with_name("sanitized-output.private.json")
                _write_immutable(sanitized_path, sanitized)
                outputs.extend(sanitized["units"])
                operations.extend(shard_operations)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
        merged = {"units": outputs}
        output_path = root / "root-projection-output-full.private.json"
        _write_immutable(output_path, merged)
        audit = {
            "schema_version": V57_AUDIT_VERSION,
            "created_at": now_iso(),
            "dropped_span_count": len(operations),
            "operations": operations,
            "semantic_decisions_changed": False,
            "rationales_changed": False,
            "privacy": "opaque_ids_field_enums_and_span_hashes_only",
        }
        audit_path = root / "exact-span-sanitization-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v57(merged, frozen["truth"], frozen["taxonomy"])
        score["exact_span_sanitization"] = _record(audit_path)
        score_path = root / "root-projection-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V57_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v57_root_projection_passed_integrated_diagnostic_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v57_root_projection_passed_integrated_diagnostic_authorized"
                if passed
                else "v57_root_projection_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "root_projection_passed": passed,
            "integrated_diagnostic_authorized": passed,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(output_path),
            "sanitization_audit": _record(audit_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": adopted,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run bounded v57 root projection")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v56-root", default=str(DEFAULT_V56_ROOT))
    parser.add_argument("--model", default="gpt-5.6-luna")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v57_repair(
            output_dir=Path(args.output_dir),
            v56_root=Path(args.v56_root),
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "root_projection_passed": terminal.get("root_projection_passed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
