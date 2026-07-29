from __future__ import annotations

"""Explicit 15-row structured-field checklist diagnostic for judge v5.4."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import (
    CHECKLIST_DECISIONS,
    CHECKLIST_FIELDS,
    STRUCTURED_FIELD_VERDICTS,
    validate_app_server_output_schema_subset,
)
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
from .app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_PROMPT_BYTES,
    MAX_SCHEMA_BYTES,
    MAX_TOKENS_PER_TURN,
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _subset_truth,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v53_reference import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V53_ROOT,
)
from .app_server_judge_v5_calibration_v54_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V54_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _canonical_json,
    _record,
    _sha256_file,
    _validate_usage,
)
from .app_server_judge_v5_fixture import load_fixture_truth_audit
from .util import now_iso


V55_INPUT_VERSION = "pif_app_server_judge_v5_4_v55_structured_checklist_input_v1"
V55_SPEC_VERSION = "pif_app_server_judge_v5_4_v55_structured_checklist_spec_v1"
V55_SCORE_VERSION = "pif_app_server_judge_v5_4_v55_structured_checklist_score_v1"
V55_FAILURE_VERSION = "pif_app_server_judge_v5_4_v55_structured_checklist_failure_v1"
V55_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v55_structured_checklist_terminal_v1"
V55_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V55_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V55_PHASE_ID = "judge_v5_4_v55_structured_checklist_diagnostic"
UNITS_PER_SHARD = 9

DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V54_ROOT.parent / "judge-calibration-v5_4-v55-structured-checklist-diagnostic"
).resolve()


class JudgeV5CalibrationV55StructuredError(RuntimeError):
    """The v55 structured checklist cannot satisfy its frozen contract."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def build_v55_input(
    pointwise_input: Mapping[str, Any], support_receipts: Mapping[str, Any]
) -> dict[str, Any]:
    support = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in support_receipts.get("units") or []
    }
    units = []
    for row in pointwise_input.get("units") or []:
        key = (str(row["case_id"]), str(row["witness_id"]))
        receipt = support.get(key)
        if receipt is None:
            raise JudgeV5CalibrationV55StructuredError("support receipt coverage drifted")
        units.append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "source_excerpt": row["source_excerpt"],
                "structured_event": deepcopy(row["structured_event"]),
                "frozen_proposition_verdict": receipt["proposition_verdict"],
            }
        )
    if len(units) != 36 or len({(row["case_id"], row["witness_id"]) for row in units}) != 36:
        raise JudgeV5CalibrationV55StructuredError("v55 requires all 36 diagnostic witnesses")
    return {
        "schema_version": V55_INPUT_VERSION,
        "units": units,
        "checklist_field_order": list(CHECKLIST_FIELDS),
        "side_labels_present": False,
        "system_identity_present": False,
        "support_receipts_frozen": True,
        "alignment_outputs_present": False,
    }


def _shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    units = value.get("units") or []
    chunks = [units[index : index + UNITS_PER_SHARD] for index in range(0, len(units), UNITS_PER_SHARD)]
    if len(chunks) != 4 or any(len(chunk) != UNITS_PER_SHARD for chunk in chunks):
        raise JudgeV5CalibrationV55StructuredError("v55 shard layout drifted")
    return [
        {
            **{key: deepcopy(child) for key, child in value.items() if key != "units"},
            "units": deepcopy(chunk),
        }
        for chunk in chunks
    ]


def v55_base_instructions() -> str:
    return (
        "You are a side-free structured-event field verifier. Judge one anonymous structured "
        "event against its entire source excerpt. Proposition support is already frozen and "
        "must not be relitigated. Complete all 15 independent rows with same, different, or "
        "abstain. same means the event's value and truth condition for that row are licensed "
        "by the source or no independent conflict exists; different means a populated material "
        "value is unlicensed or contradicted. Report minimal root differences only. speaker is "
        "who voices or authors the claim. actor is who performs the action or holds the stance. "
        "attribution is only the reporting/source relation, never a duplicate of actor or "
        "speaker identity. event_boundary is alignment-only and must be same in this pointwise "
        "pass. unsupported_inference must exactly follow the frozen proposition verdict: "
        "different for unsupported, same for supported, abstain for abstain. An unsupported "
        "proposition does not automatically make certainty or temporal_horizon different; "
        "those require independent source conflict. evidence is different only when the "
        "populated evidence itself is inexact or does not support the frozen proposition. "
        "Cite exact source substrings or [] for witness-only conflicts. Do not infer origin, "
        "vote, use confidence, regex, keywords, token overlap, or embeddings."
    )


def build_v55_prompt(value: Mapping[str, Any]) -> str:
    audit = load_fixture_truth_audit()
    packet = {
        "rubric": audit["mismatch_checklist"],
        "mismatch_precedence": audit["mismatch_precedence"],
        "units": value["units"],
    }
    return (
        "Return every case_id/witness_id exactly once and every checklist field exactly once "
        "in the supplied order. structured_field_verdict=correct iff all rows are same; "
        "incorrect iff at least one row is different; abstain iff no row is different and at "
        "least one row abstains. event_boundary must be same. unsupported_inference must match "
        "the frozen proposition verdict exactly. Every nonempty source_evidence_span must be an "
        "exact substring; use [] rather than paraphrasing.\n\n"
        "# Side-free structured checklist units\n"
        + _canonical_json(packet)
        + "\n"
    )


def v55_output_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    units = value.get("units") or []
    case_ids = sorted({str(row["case_id"]) for row in units})
    witness_ids = sorted(str(row["witness_id"]) for row in units)
    checklist_row = {
        "type": "object",
        "additionalProperties": False,
        "required": ["field", "decision", "source_evidence_spans", "rationale"],
        "properties": {
            "field": {"type": "string", "enum": list(CHECKLIST_FIELDS)},
            "decision": {"type": "string", "enum": list(CHECKLIST_DECISIONS)},
            "source_evidence_spans": {
                "type": "array",
                "maxItems": 2,
                "items": {"type": "string", "minLength": 1, "maxLength": 1000},
            },
            "rationale": {"type": "string", "minLength": 1, "maxLength": 240},
        },
    }
    unit = {
        "type": "object",
        "additionalProperties": False,
        "required": ["case_id", "witness_id", "structured_field_verdict", "checklist"],
        "properties": {
            "case_id": {"type": "string", "enum": case_ids},
            "witness_id": {"type": "string", "enum": witness_ids},
            "structured_field_verdict": {
                "type": "string",
                "enum": list(STRUCTURED_FIELD_VERDICTS),
            },
            "checklist": {
                "type": "array",
                "minItems": len(CHECKLIST_FIELDS),
                "maxItems": len(CHECKLIST_FIELDS),
                "items": checklist_row,
            },
        },
    }
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["units"],
        "properties": {
            "units": {
                "type": "array",
                "minItems": len(units),
                "maxItems": len(units),
                "items": unit,
            }
        },
    }
    if validate_app_server_output_schema_subset(schema):
        raise JudgeV5CalibrationV55StructuredError("v55 schema exceeds supported subset")
    return schema


def validate_v55_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping) or set(output) != {"units"} or not isinstance(output.get("units"), list):
        return ["invalid_v55_output_root"]
    expected = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in value.get("units") or []
    }
    seen = set()
    errors = []
    for index, row in enumerate(output["units"]):
        prefix = f"unit_{index}"
        if not isinstance(row, Mapping) or set(row) != {
            "case_id",
            "witness_id",
            "structured_field_verdict",
            "checklist",
        }:
            errors.append(prefix + "_shape")
            continue
        key = (str(row.get("case_id")), str(row.get("witness_id")))
        if key not in expected or key in seen:
            errors.append(prefix + "_identity")
            continue
        seen.add(key)
        checklist = row.get("checklist")
        if (
            not isinstance(checklist, list)
            or [item.get("field") for item in checklist if isinstance(item, Mapping)]
            != list(CHECKLIST_FIELDS)
        ):
            errors.append(prefix + "_checklist_order")
            continue
        decisions = {}
        source = expected[key]["source_excerpt"]
        for item_index, item in enumerate(checklist):
            if not isinstance(item, Mapping) or set(item) != {
                "field",
                "decision",
                "source_evidence_spans",
                "rationale",
            }:
                errors.append(f"{prefix}_row_{item_index}_shape")
                continue
            decision = item.get("decision")
            spans = item.get("source_evidence_spans")
            if decision not in CHECKLIST_DECISIONS:
                errors.append(f"{prefix}_row_{item_index}_decision")
            if (
                not isinstance(spans, list)
                or len(spans) > 2
                or len(spans) != len(set(spans))
                or any(not isinstance(span, str) or not span or span not in source for span in spans)
            ):
                errors.append(f"{prefix}_row_{item_index}_evidence")
            decisions[str(item.get("field"))] = decision
        if decisions.get("event_boundary") != "same":
            errors.append(prefix + "_event_boundary_scope")
        expected_unsupported = {
            "supported": "same",
            "unsupported": "different",
            "abstain": "abstain",
        }[expected[key]["frozen_proposition_verdict"]]
        if decisions.get("unsupported_inference") != expected_unsupported:
            errors.append(prefix + "_unsupported_projection")
        verdict = row.get("structured_field_verdict")
        projected = (
            "incorrect"
            if "different" in decisions.values()
            else "abstain"
            if "abstain" in decisions.values()
            else "correct"
        )
        if verdict != projected:
            errors.append(prefix + "_verdict_projection")
    if seen != set(expected):
        errors.append("v55_output_coverage")
    return errors


def score_v55(output: Mapping[str, Any], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {
        (str(case_id), str(witness_id)): (case["structured_fields"][witness_id], set(fields))
        for case_id, case in (truth.get("cases") or {}).items()
        for witness_id, fields in case["field_issues"].items()
    }
    rows = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in output.get("units") or []
    }
    if set(rows) != set(expected):
        raise JudgeV5CalibrationV55StructuredError("v55 score coverage drifted")
    structured_correct = field_tp = field_fp = field_fn = row_correct = 0
    unsupported_correct = boundary_correct = attribution_tn = attribution_total = abstentions = 0
    for key, (expected_verdict, expected_fields) in expected.items():
        row = rows[key]
        structured_correct += int(row["structured_field_verdict"] == expected_verdict)
        observed = {
            item["field"] for item in row["checklist"] if item["decision"] == "different"
        }
        field_tp += len(expected_fields & observed)
        field_fp += len(observed - expected_fields)
        field_fn += len(expected_fields - observed)
        decisions = {item["field"]: item["decision"] for item in row["checklist"]}
        row_correct += sum(
            decisions[field] == ("different" if field in expected_fields else "same")
            for field in CHECKLIST_FIELDS
        )
        unsupported_correct += int(
            decisions["unsupported_inference"]
            == ("different" if "unsupported_inference" in expected_fields else "same")
        )
        boundary_correct += int(decisions["event_boundary"] == "same")
        if "attribution" not in expected_fields:
            attribution_total += 1
            attribution_tn += int(decisions["attribution"] == "same")
        abstentions += sum(item["decision"] == "abstain" for item in row["checklist"])
    total = len(expected)
    field_den = 2 * field_tp + field_fp + field_fn
    metrics = {
        "witness_count": total,
        "structured_field_accuracy": round(structured_correct / total, 6),
        "field_issue_f1": round((2 * field_tp) / field_den, 6) if field_den else 0.0,
        "checklist_row_accuracy": round(row_correct / (total * len(CHECKLIST_FIELDS)), 6),
        "unsupported_inference_accuracy": round(unsupported_correct / total, 6),
        "event_boundary_scope_accuracy": round(boundary_correct / total, 6),
        "attribution_specificity": round(attribution_tn / attribution_total, 6),
        "abstention_count": abstentions,
    }
    checks = {
        "structured_field_accuracy": metrics["structured_field_accuracy"] >= 0.95,
        "field_issue_f1": metrics["field_issue_f1"] >= 0.95,
        "checklist_row_accuracy": metrics["checklist_row_accuracy"] >= 0.99,
        "unsupported_inference_accuracy": metrics["unsupported_inference_accuracy"] == 1.0,
        "event_boundary_scope_accuracy": metrics["event_boundary_scope_accuracy"] == 1.0,
        "attribution_specificity": metrics["attribution_specificity"] >= 0.95,
        "abstention_count": metrics["abstention_count"] == 0,
    }
    return {
        "schema_version": V55_SCORE_VERSION,
        "passed": all(checks.values()),
        "metrics": metrics,
        "checks": checks,
        "failed_checks": sorted(key for key, passed in checks.items() if not passed),
        "gates_frozen_before_semantic_calls": True,
    }


def _validate_predecessors(v53_root: Path, v54_root: Path) -> dict[str, Any]:
    paths = {
        "v53_terminal": v53_root / "terminal.json",
        "v53_truth": v53_root / "scoped-calibration-truth.private.json",
        "v54_terminal": v54_root / "terminal.json",
        "v54_score": v54_root / "diagnostic-score.json",
        "v54_pointwise_input": v54_root / "pointwise-input-full.private.json",
        "v54_support_receipts": v54_root / "support-receipts.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    if (
        values["v53_terminal"].get("reference_frozen") is not True
        or values["v53_terminal"].get("production_mutated") is not False
        or not _record_matches(values["v53_terminal"].get("scoped_truth"), paths["v53_truth"])
    ):
        raise JudgeV5CalibrationV55StructuredError("v53 predecessor drifted")
    v54 = values["v54_terminal"]
    if (
        v54.get("state") != "inactive"
        or v54.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or v54.get("accounting_complete") is not True
        or v54.get("usage_status") != "complete"
        or v54.get("production_mutated") is not False
        or v54.get("diagnostic_passed") is not False
        or not _record_matches(v54.get("score"), paths["v54_score"])
        or not _record_matches(v54.get("support_receipts"), paths["v54_support_receipts"])
    ):
        raise JudgeV5CalibrationV55StructuredError("v54 quality predecessor drifted")
    return {name: _record(path) for name, path in paths.items()}


def _build_capacity_policy(root: Path, predecessors: Mapping[str, Any], v54_root: Path) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    turn_names = [f"structured_checklist_shard_{index:02d}" for index in range(4)]
    v54 = _load_json(v54_root / "terminal.json", "v54 terminal")
    audit = {
        "schema_version": V55_CAPACITY_AUDIT_VERSION,
        "phase_id": V55_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessors": predecessors,
        "measured_basis": {
            "v54_total_tokens": (v54.get("usage") or {}).get("total_tokens"),
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": len(turn_names) * MAX_TOKENS_PER_TURN,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v55 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV55StructuredError("immutable v55 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V55_CAPACITY_POLICY_VERSION,
        "phase_id": V55_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": turn_names,
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": len(turn_names) * MAX_TOKENS_PER_TURN,
        "projected_phase_quota_points": math.ceil(
            len(turn_names) * MAX_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    if policy_path.exists():
        prior = _load_json(policy_path, "v55 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV55StructuredError("immutable v55 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v55_structured(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v53_root: Path = DEFAULT_V53_ROOT,
    v54_root: Path = DEFAULT_V54_ROOT,
    model: str = "gpt-5.6-luna",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessors = _validate_predecessors(v53_root.resolve(), v54_root.resolve())
    full_truth = _load_json(
        v53_root / "scoped-calibration-truth.private.json", "v53 truth"
    )
    value = build_v55_input(
        _load_json(v54_root / "pointwise-input-full.private.json", "v54 pointwise input"),
        _load_json(v54_root / "support-receipts.private.json", "v54 support receipts"),
    )
    truth = _subset_truth(
        full_truth, sorted({str(row["case_id"]) for row in value["units"]})
    )
    input_path = root / "structured-checklist-input-full.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    frozen_shards = []
    for index, shard_input in enumerate(_shards(value)):
        prompt = build_v55_prompt(shard_input)
        schema = v55_output_schema(shard_input)
        turn_name = f"structured_checklist_shard_{index:02d}"
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=shard_input, prompt=prompt, schema=schema
        )
        frozen_shards.append(
            {
                "index": index,
                "turn_name": turn_name,
                "input": shard_input,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    capacity = _build_capacity_policy(root, predecessors, v54_root)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V55_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "explicit_15_row_pointwise_structured_checklist",
        "case_count": 12,
        "witness_count": 36,
        "turn_plan": [shard["turn_name"] for shard in frozen_shards],
        "retry_count_per_turn": 0,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessors": predecessors,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "full": _record(input_path),
            "truth": _record(truth_path),
            "shards": [
                {
                    "index": shard["index"],
                    "input": _record(shard["paths"]["input"]),
                    "prompt": _record(shard["paths"]["prompt"]),
                    "schema": _record(shard["paths"]["schema"]),
                }
                for shard in frozen_shards
            ],
        },
        "gates": {
            "structured_field_accuracy_min": 0.95,
            "field_issue_f1_min": 0.95,
            "checklist_row_accuracy_min": 0.99,
            "unsupported_inference_accuracy": 1.0,
            "event_boundary_scope_accuracy": 1.0,
            "attribution_specificity_min": 0.95,
            "abstention_count": 0,
        },
        "request_byte_caps": {"prompt": MAX_PROMPT_BYTES, "output_schema": MAX_SCHEMA_BYTES},
        "privacy": "private_inputs_prompts_outputs_no_source_text_in_terminal",
    }
    spec_path = root / "structured-checklist-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v55 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV55StructuredError("immutable v55 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "truth": truth,
        "input": value,
        "shards": frozen_shards,
        "capacity_policy": capacity["policy"],
    }


def _write_failure(root: Path, spec_path: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
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
            usage = _validate_usage(_load_json(Path(record["path"]), "v55 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = not partial and unknown == 0
    failure = {
        "schema_version": V55_FAILURE_VERSION,
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
        "schema_version": V55_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "integration_diagnostic_authorized": False,
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


async def run_v55_structured(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v53_root: Path = DEFAULT_V53_ROOT,
    v54_root: Path = DEFAULT_V54_ROOT,
    model: str = "gpt-5.6-luna",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v55 terminal")
    frozen = freeze_v55_structured(
        output_dir=root,
        v53_root=v53_root,
        v54_root=v54_root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
    )
    factory = client_factory or _client_factory
    sidecars = []
    outputs = []
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
                    base_instructions=v55_base_instructions(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["units"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda value, item=shard: validate_v55_output(
                        value, item["input"]
                    ),
                )
                outputs.extend(output["units"])
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
        merged = {"units": outputs}
        output_path = root / "structured-checklist-output-full.private.json"
        _write_immutable(output_path, merged)
        score = score_v55(merged, frozen["truth"])
        score_path = root / "structured-checklist-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V55_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v55_structured_checklist_passed_integration_diagnostic_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v55_structured_checklist_passed_integration_diagnostic_authorized"
                if passed
                else "v55_structured_checklist_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "structured_checklist_passed": passed,
            "integration_diagnostic_authorized": passed,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(output_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": adopted,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, frozen["spec_path"], exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, frozen["spec_path"], current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v55 structured checklist diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v53-root", default=str(DEFAULT_V53_ROOT))
    parser.add_argument("--v54-root", default=str(DEFAULT_V54_ROOT))
    parser.add_argument("--model", default="gpt-5.6-luna")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v55_structured(
            output_dir=Path(args.output_dir),
            v53_root=Path(args.v53_root),
            v54_root=Path(args.v54_root),
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
                "structured_checklist_passed": terminal.get("structured_checklist_passed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
