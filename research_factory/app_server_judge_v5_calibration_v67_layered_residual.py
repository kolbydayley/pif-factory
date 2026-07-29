from __future__ import annotations

"""Layered LLM-only diagnostic over the v65 structured-field residuals."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import CHECKLIST_FIELDS, validate_app_server_output_schema_subset
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
from .app_server_judge_v5_calibration_v64_reference_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V64_ROOT,
)
from .app_server_judge_v5_calibration_v65_fresh_structured import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V65_ROOT,
)
from .app_server_judge_v5_calibration_v66_external_blocker import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V66_ROOT,
    _validate_predecessors as _validate_v66_upstream,
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
from .util import now_iso, sha256_text


V67_INPUT_VERSION = "pif_app_server_judge_v5_4_v67_layered_residual_input_v1"
V67_TRUTH_VERSION = "pif_app_server_judge_v5_4_v67_layered_residual_truth_v1"
V67_ROLE_VERSION = "pif_app_server_judge_v5_4_v67_layered_residual_roles_v1"
V67_POINTWISE_RECEIPT_VERSION = (
    "pif_app_server_judge_v5_4_v67_pointwise_field_receipts_v1"
)
V67_SPEC_VERSION = "pif_app_server_judge_v5_4_v67_layered_residual_spec_v1"
V67_SCORE_VERSION = "pif_app_server_judge_v5_4_v67_layered_residual_score_v1"
V67_AUDIT_VERSION = "pif_app_server_judge_v5_4_v67_projection_audit_v1"
V67_FAILURE_VERSION = "pif_app_server_judge_v5_4_v67_failure_v1"
V67_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v67_terminal_v1"
V67_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V67_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V67_PHASE_ID = "judge_v5_4_v67_layered_residual_diagnostic"
POINTWISE_TURN = "pointwise_field_facts"
ROOT_TURN = "neutral_root_verification"
TURN_NAMES = (POINTWISE_TURN, ROOT_TURN)
POINTWISE_MODEL = "gpt-5.6-luna"
POINTWISE_EFFORT = "low"
ROOT_MODEL = "gpt-5.6-sol"
ROOT_EFFORT = "high"
FIELD_STATES = ("correct", "incorrect", "omitted_required", "not_applicable", "abstain")
ROOT_STATUSES = ("root", "not_root", "abstain")
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V66_ROOT.parent / "judge-calibration-v5_4-v67-layered-residual-diagnostic"
).resolve()


class JudgeV5CalibrationV67Error(RuntimeError):
    """The layered v67 diagnostic cannot preserve its frozen contract."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _verify_record(record: Any) -> bool:
    return (
        isinstance(record, Mapping)
        and isinstance(record.get("path"), str)
        and _record_matches(record, Path(record["path"]))
    )


def _validate_predecessor(v66_root: Path) -> dict[str, Any]:
    paths = {
        "v66_terminal": v66_root / "terminal.json",
        "v66_packet": v66_root / "reference-owner-adjudication.private.json",
        "v66_summary": v66_root / "blocker-summary.json",
        "v66_decision_schema": v66_root / "reference-owner-decision-schema.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v66_terminal"]
    packet = values["v66_packet"]
    decision_path = v66_root / "reference-owner-decision.private.json"
    upstream = _validate_v66_upstream(DEFAULT_V64_ROOT, DEFAULT_V65_ROOT)
    if (
        terminal.get("state") != "blocked"
        or terminal.get("terminal_reason")
        != "external_reference_owner_adjudication_required"
        or terminal.get("overall_evaluation_complete") is not False
        or terminal.get("new_semantic_turn_count") != 0
        or terminal.get("production_mutated") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("predecessor") != upstream
        or packet.get("unit_count") != 10
        or len(packet.get("units") or []) != 10
        or not _record_matches(terminal.get("private_packet"), paths["v66_packet"])
        or not _record_matches(terminal.get("sanitized_summary"), paths["v66_summary"])
        or not _record_matches(terminal.get("decision_schema"), paths["v66_decision_schema"])
        or decision_path.exists()
    ):
        raise JudgeV5CalibrationV67Error("v66 predecessor is inadmissible")
    return {name: _record(path) for name, path in paths.items()}


def _compact_event(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: deepcopy(value)
        for key, value in event.items()
        if value not in (None, "", [], {})
    }


def _root_fields(row: Mapping[str, Any]) -> set[str]:
    return {
        item["field"]
        for item in row.get("checklist") or []
        if item.get("independent_root_status") == "root"
    }


def build_v67_cohort(
    source: Mapping[str, Any],
    prior_output: Mapping[str, Any],
    full_truth: Mapping[str, Any],
    v66_packet: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    sources = {
        (row["case_id"], row["witness_id"]): row for row in source.get("units") or []
    }
    observed = {
        (row["case_id"], row["witness_id"]): _root_fields(row)
        for row in prior_output.get("units") or []
    }
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in full_truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
        if (case_id, witness_id) in sources
    }
    residual_keys = sorted(
        (row["case_id"], row["witness_id"]) for row in v66_packet.get("units") or []
    )
    calculated_residuals = sorted(key for key in expected if observed[key] != expected[key])
    if residual_keys != calculated_residuals or len(residual_keys) != 10:
        raise JudgeV5CalibrationV67Error("v67 residual identity drifted")
    exact_keys = sorted(key for key in expected if observed[key] == expected[key])
    empty_controls = [key for key in exact_keys if not expected[key]]
    nonempty_controls = [key for key in exact_keys if expected[key]]
    if not empty_controls or not nonempty_controls:
        raise JudgeV5CalibrationV67Error("v67 controls are unavailable")
    selected = residual_keys + [empty_controls[0], nonempty_controls[0]]
    if len(selected) != 12 or len(set(selected)) != 12:
        raise JudgeV5CalibrationV67Error("v67 cohort coverage drifted")
    units = []
    roles = []
    truth_cases: dict[str, Any] = {}
    for key in selected:
        source_row = sources[key]
        event = _compact_event(source_row["structured_event"])
        evidence = event.get("evidence")
        evidence_exact = bool(
            isinstance(evidence, str)
            and evidence
            and evidence in source_row["source_excerpt"]
            and event.get("submitted_evidence_exact") is True
        )
        units.append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "source_excerpt": source_row["source_excerpt"],
                "structured_event": event,
                "frozen_proposition_verdict": source_row["frozen_proposition_verdict"],
                "exact_evidence_receipt": evidence_exact,
            }
        )
        if key in residual_keys:
            symmetric = expected[key] ^ observed[key]
            role = "evidence_residual" if symmetric == {"evidence"} else "semantic_residual"
        else:
            role = "empty_control" if not expected[key] else "nonempty_control"
        roles.append({"case_id": key[0], "witness_id": key[1], "role": role})
        case = truth_cases.setdefault(key[0], {"field_issues": {}, "structured_fields": {}})
        case["field_issues"][key[1]] = sorted(expected[key])
        case["structured_fields"][key[1]] = (
            "incorrect" if expected[key] else "correct"
        )
    role_counts = {role: sum(row["role"] == role for row in roles) for role in {
        "evidence_residual", "semantic_residual", "empty_control", "nonempty_control"
    }}
    if role_counts != {
        "evidence_residual": 6,
        "semantic_residual": 4,
        "empty_control": 1,
        "nonempty_control": 1,
    }:
        raise JudgeV5CalibrationV67Error("v67 role distribution drifted")
    value = {
        "schema_version": V67_INPUT_VERSION,
        "units": units,
        "checklist_field_order": list(CHECKLIST_FIELDS),
        "prior_labels_present": False,
        "candidate_outputs_present": False,
        "system_identity_present": False,
        "empty_event_fields_omitted": True,
    }
    truth = {
        "schema_version": V67_TRUTH_VERSION,
        "reference_version": full_truth["reference_version"],
        "case_count": len(truth_cases),
        "witness_count": 12,
        "cases": truth_cases,
    }
    role_value = {
        "schema_version": V67_ROLE_VERSION,
        "selection_rule": (
            "all_v65_residuals_plus_lexicographically_first_exact_empty_and_nonempty_controls"
        ),
        "selection_uses_source_text": False,
        "units": roles,
        "role_counts": role_counts,
    }
    return value, truth, role_value


def pointwise_base_instructions() -> str:
    return (
        "You are a side-free pointwise structured-field analyst. No reference labels, system names, "
        "or prior judge decisions are shown. Evaluate each field independently against the selected "
        "source proposition after mentally correcting every other field. correct means the populated "
        "field preserves the source value; incorrect means its own populated value materially conflicts; "
        "omitted_required means the field is empty or not-applicable but the selected proposition requires "
        "a material value; not_applicable means no value is required and no conflict exists. Do not turn an "
        "actor error into speaker, attribution, target, event_type, metric, or evidence errors. Evidence is "
        "about exact source grounding, while unsupported_inference follows the frozen proposition support "
        "receipt. Use exact source substrings or an empty list. Do not infer origin, vote, use confidence, "
        "regex, keywords, overlap, or embeddings."
    )


def build_pointwise_prompt(value: Mapping[str, Any]) -> str:
    fixture = load_fixture_truth_audit()
    packet = {
        "rubric": fixture["mismatch_checklist"],
        "mismatch_precedence": fixture["mismatch_precedence"],
        "field_state_definitions": {
            "correct": "field value is source-correct for the selected proposition",
            "incorrect": "populated field independently conflicts with source",
            "omitted_required": "source requires a material field value that is absent",
            "not_applicable": "source requires no value and the field creates no conflict",
            "abstain": "source is insufficient to decide this field",
        },
        "units": value["units"],
    }
    return (
        "Return every opaque unit once and all 15 fields in supplied order. Decide only the field's "
        "own source correctness; do not produce mismatch labels or compare systems. Every nonempty "
        "source span must be an exact substring.\n\n# Pointwise field-fact units\n"
        + _canonical_json(packet)
        + "\n"
    )


def pointwise_output_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    units = value["units"]
    row = {
        "type": "object",
        "additionalProperties": False,
        "required": ["field", "field_state", "source_evidence_spans", "rationale"],
        "properties": {
            "field": {"type": "string", "enum": list(CHECKLIST_FIELDS)},
            "field_state": {"type": "string", "enum": list(FIELD_STATES)},
            "source_evidence_spans": {
                "type": "array",
                "maxItems": 2,
                "items": {"type": "string", "minLength": 1, "maxLength": 1000},
            },
            "rationale": {"type": "string", "minLength": 1, "maxLength": 260},
        },
    }
    unit = {
        "type": "object",
        "additionalProperties": False,
        "required": ["case_id", "witness_id", "field_facts"],
        "properties": {
            "case_id": {"type": "string", "enum": sorted({row["case_id"] for row in units})},
            "witness_id": {"type": "string", "enum": sorted(row["witness_id"] for row in units)},
            "field_facts": {
                "type": "array",
                "minItems": len(CHECKLIST_FIELDS),
                "maxItems": len(CHECKLIST_FIELDS),
                "items": row,
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
    errors = validate_app_server_output_schema_subset(schema)
    if errors:
        raise JudgeV5CalibrationV67Error("v67 pointwise schema exceeds supported subset")
    return schema


def validate_pointwise_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping) or set(output) != {"units"}:
        return ["invalid_pointwise_root"]
    expected = {(row["case_id"], row["witness_id"]): row for row in value["units"]}
    seen = set()
    errors = []
    for index, unit in enumerate(output.get("units") or []):
        prefix = f"unit_{index}"
        if not isinstance(unit, Mapping) or set(unit) != {"case_id", "witness_id", "field_facts"}:
            errors.append(prefix + "_shape")
            continue
        key = (unit.get("case_id"), unit.get("witness_id"))
        if key not in expected or key in seen:
            errors.append(prefix + "_identity")
            continue
        seen.add(key)
        facts = unit.get("field_facts")
        if not isinstance(facts, list) or [row.get("field") for row in facts if isinstance(row, Mapping)] != list(CHECKLIST_FIELDS):
            errors.append(prefix + "_field_order")
            continue
        source = expected[key]["source_excerpt"]
        for row_index, row in enumerate(facts):
            rp = f"{prefix}_row_{row_index}"
            if not isinstance(row, Mapping) or set(row) != {
                "field", "field_state", "source_evidence_spans", "rationale"
            }:
                errors.append(rp + "_shape")
                continue
            spans = row.get("source_evidence_spans")
            if row.get("field_state") not in FIELD_STATES:
                errors.append(rp + "_state")
            if (
                not isinstance(spans, list)
                or len(spans) > 2
                or len(spans) != len(set(spans))
                or any(not isinstance(span, str) or not span or span not in source for span in spans)
            ):
                errors.append(rp + "_evidence")
            if not isinstance(row.get("rationale"), str) or not row["rationale"]:
                errors.append(rp + "_rationale")
    if seen != set(expected):
        errors.append("pointwise_coverage")
    return errors


def freeze_pointwise_receipts(output: Mapping[str, Any], value: Mapping[str, Any]) -> dict[str, Any]:
    errors = validate_pointwise_output(output, value)
    if errors:
        raise JudgeV5CalibrationV67Error("invalid pointwise receipts: " + "; ".join(errors))
    return {
        "schema_version": V67_POINTWISE_RECEIPT_VERSION,
        "created_at": now_iso(),
        "unit_count": len(output["units"]),
        "units": deepcopy(output["units"]),
        "semantic_source": POINTWISE_MODEL,
        "labels_or_truth_present": False,
    }


def build_root_input(
    value: Mapping[str, Any], receipts: Mapping[str, Any]
) -> dict[str, Any]:
    receipt_rows = {
        (row["case_id"], row["witness_id"]): row for row in receipts["units"]
    }
    units = []
    for row in value["units"]:
        key = (row["case_id"], row["witness_id"])
        if key not in receipt_rows:
            raise JudgeV5CalibrationV67Error("v67 root receipt coverage drifted")
        units.append({**deepcopy(row), "pointwise_field_receipt": deepcopy(receipt_rows[key]["field_facts"])})
    return {
        "schema_version": V67_INPUT_VERSION,
        "units": units,
        "checklist_field_order": list(CHECKLIST_FIELDS),
        "prior_labels_present": False,
        "candidate_outputs_present": False,
        "system_identity_present": False,
        "pointwise_receipts_frozen": True,
    }


def root_base_instructions() -> str:
    return (
        "You are the neutral final structured-field verifier in a layered LLM-only judge. The Luna "
        "pointwise receipts are source-analysis evidence, not votes or truth. Recheck each field against "
        "the source and event. Mark root only if that field's own populated value independently conflicts "
        "or a material required value is omitted after mentally correcting all other fields. Otherwise "
        "mark not_root. Do not propagate actor or speaker errors into attribution, target, event_type, "
        "metric, evidence, or reported_actor. Evidence root status is controlled by the exact-evidence "
        "receipt, and unsupported_inference is controlled by the frozen proposition support receipt; "
        "your semantic output will be audited against those receipts. Use exact source substrings or []. "
        "Do not infer origin, vote, use confidence, regex, keywords, overlap, or embeddings."
    )


def build_root_prompt(value: Mapping[str, Any]) -> str:
    fixture = load_fixture_truth_audit()
    packet = {
        "rubric": fixture["mismatch_checklist"],
        "mismatch_precedence": fixture["mismatch_precedence"],
        "root_rule": (
            "root iff the field remains independently wrong or materially omitted after all other fields are corrected"
        ),
        "units": value["units"],
    }
    return (
        "Return every opaque unit once and all 15 root statuses in supplied order. This is a staged "
        "verification, not voting or system comparison. Every nonempty source span must be exact.\n\n"
        "# Neutral root-verification units\n"
        + _canonical_json(packet)
        + "\n"
    )


def root_output_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    units = value["units"]
    row = {
        "type": "object",
        "additionalProperties": False,
        "required": ["field", "root_status", "source_evidence_spans", "rationale"],
        "properties": {
            "field": {"type": "string", "enum": list(CHECKLIST_FIELDS)},
            "root_status": {"type": "string", "enum": list(ROOT_STATUSES)},
            "source_evidence_spans": {
                "type": "array",
                "maxItems": 2,
                "items": {"type": "string", "minLength": 1, "maxLength": 1000},
            },
            "rationale": {"type": "string", "minLength": 1, "maxLength": 260},
        },
    }
    unit = {
        "type": "object",
        "additionalProperties": False,
        "required": ["case_id", "witness_id", "structured_field_verdict", "checklist"],
        "properties": {
            "case_id": {"type": "string", "enum": sorted({row["case_id"] for row in units})},
            "witness_id": {"type": "string", "enum": sorted(row["witness_id"] for row in units)},
            "structured_field_verdict": {
                "type": "string",
                "enum": ["correct", "incorrect", "abstain"],
            },
            "checklist": {
                "type": "array",
                "minItems": len(CHECKLIST_FIELDS),
                "maxItems": len(CHECKLIST_FIELDS),
                "items": row,
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
    errors = validate_app_server_output_schema_subset(schema)
    if errors:
        raise JudgeV5CalibrationV67Error("v67 root schema exceeds supported subset")
    return schema


def project_root_contract(
    output: Mapping[str, Any], value: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    projected = deepcopy(output)
    sources = {(row["case_id"], row["witness_id"]): row for row in value["units"]}
    operations = []
    for unit in projected.get("units") or []:
        key = (unit.get("case_id"), unit.get("witness_id"))
        source = sources.get(key)
        if source is None:
            continue
        for row in unit.get("checklist") or []:
            spans = row.get("source_evidence_spans")
            if isinstance(spans, list):
                retained = []
                for span in spans:
                    if isinstance(span, str) and span and span in source["source_excerpt"]:
                        retained.append(span)
                    else:
                        rendered = span if isinstance(span, str) else json.dumps(span, sort_keys=True)
                        operations.append(
                            {
                                "operation_type": "drop_nonexact_evidence_span",
                                "case_id": key[0],
                                "witness_id": key[1],
                                "field": row.get("field"),
                                "span_sha256": sha256_text(rendered),
                                "span_size_bytes": len(rendered.encode("utf-8")),
                            }
                        )
                row["source_evidence_spans"] = retained
        checklist = {row.get("field"): row for row in unit.get("checklist") or []}
        evidence = checklist.get("evidence")
        if isinstance(evidence, Mapping):
            expected = "not_root" if source["exact_evidence_receipt"] else "root"
            if evidence.get("root_status") != expected:
                operations.append(
                    {
                        "operation_type": "project_exact_evidence_receipt",
                        "case_id": key[0],
                        "witness_id": key[1],
                        "field": "evidence",
                        "prior": evidence.get("root_status"),
                        "projected": expected,
                    }
                )
                evidence["root_status"] = expected
        unsupported = checklist.get("unsupported_inference")
        if isinstance(unsupported, Mapping):
            expected = {
                "supported": "not_root",
                "unsupported": "root",
                "abstain": "abstain",
            }[source["frozen_proposition_verdict"]]
            if unsupported.get("root_status") != expected:
                operations.append(
                    {
                        "operation_type": "project_frozen_support_receipt",
                        "case_id": key[0],
                        "witness_id": key[1],
                        "field": "unsupported_inference",
                        "prior": unsupported.get("root_status"),
                        "projected": expected,
                    }
                )
                unsupported["root_status"] = expected
        statuses = {row.get("root_status") for row in unit.get("checklist") or []}
        unit["structured_field_verdict"] = (
            "incorrect" if "root" in statuses else "abstain" if "abstain" in statuses else "correct"
        )
    return projected, operations


def validate_root_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping) or set(output) != {"units"}:
        return ["invalid_root_output"]
    expected = {(row["case_id"], row["witness_id"]): row for row in value["units"]}
    seen = set()
    errors = []
    for index, unit in enumerate(output.get("units") or []):
        prefix = f"unit_{index}"
        if not isinstance(unit, Mapping) or set(unit) != {
            "case_id", "witness_id", "structured_field_verdict", "checklist"
        }:
            errors.append(prefix + "_shape")
            continue
        key = (unit.get("case_id"), unit.get("witness_id"))
        if key not in expected or key in seen:
            errors.append(prefix + "_identity")
            continue
        seen.add(key)
        checklist = unit.get("checklist")
        if not isinstance(checklist, list) or [row.get("field") for row in checklist if isinstance(row, Mapping)] != list(CHECKLIST_FIELDS):
            errors.append(prefix + "_field_order")
            continue
        source = expected[key]["source_excerpt"]
        for row_index, row in enumerate(checklist):
            rp = f"{prefix}_row_{row_index}"
            if not isinstance(row, Mapping) or set(row) != {
                "field", "root_status", "source_evidence_spans", "rationale"
            }:
                errors.append(rp + "_shape")
                continue
            spans = row.get("source_evidence_spans")
            if row.get("root_status") not in ROOT_STATUSES:
                errors.append(rp + "_status")
            if (
                not isinstance(spans, list)
                or len(spans) > 2
                or len(spans) != len(set(spans))
                or any(not isinstance(span, str) or not span or span not in source for span in spans)
            ):
                errors.append(rp + "_evidence")
        rows = {row.get("field"): row for row in checklist if isinstance(row, Mapping)}
        expected_evidence = "not_root" if expected[key]["exact_evidence_receipt"] else "root"
        expected_unsupported = {
            "supported": "not_root",
            "unsupported": "root",
            "abstain": "abstain",
        }[expected[key]["frozen_proposition_verdict"]]
        if (rows.get("evidence") or {}).get("root_status") != expected_evidence:
            errors.append(prefix + "_evidence_projection")
        if (rows.get("unsupported_inference") or {}).get("root_status") != expected_unsupported:
            errors.append(prefix + "_support_projection")
        statuses = {row.get("root_status") for row in checklist if isinstance(row, Mapping)}
        verdict = "incorrect" if "root" in statuses else "abstain" if "abstain" in statuses else "correct"
        if unit.get("structured_field_verdict") != verdict:
            errors.append(prefix + "_verdict_projection")
    if seen != set(expected):
        errors.append("root_output_coverage")
    return errors


def score_v67(
    output: Mapping[str, Any], truth: Mapping[str, Any], roles: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    role_map = {(row["case_id"], row["witness_id"]): row["role"] for row in roles["units"]}
    rows = {(row["case_id"], row["witness_id"]): row for row in output["units"]}
    if set(rows) != set(expected) or set(role_map) != set(expected):
        raise JudgeV5CalibrationV67Error("v67 score coverage drifted")
    tp = fp = fn = exact = verdict_correct = cells_correct = abstentions = 0
    role_exact = {role: 0 for role in roles["role_counts"]}
    for key, fields in expected.items():
        observed = {
            row["field"] for row in rows[key]["checklist"] if row["root_status"] == "root"
        }
        tp += len(fields & observed)
        fp += len(observed - fields)
        fn += len(fields - observed)
        is_exact = observed == fields
        exact += int(is_exact)
        role_exact[role_map[key]] += int(is_exact)
        verdict_correct += int(rows[key]["structured_field_verdict"] == ("incorrect" if fields else "correct"))
        cells_correct += sum(
            (row["root_status"] == "root") == (row["field"] in fields)
            for row in rows[key]["checklist"]
        )
        abstentions += sum(row["root_status"] == "abstain" for row in rows[key]["checklist"])
    denominator = 2 * tp + fp + fn
    metrics = {
        "witness_count": 12,
        "structured_field_accuracy": round(verdict_correct / 12, 6),
        "root_field_f1": round((2 * tp) / denominator, 6) if denominator else 0.0,
        "root_checklist_cell_accuracy": round(cells_correct / (12 * len(CHECKLIST_FIELDS)), 6),
        "exact_case_rate": round(exact / 12, 6),
        "evidence_residual_exact_rate": round(role_exact["evidence_residual"] / 6, 6),
        "semantic_residual_exact_rate": round(role_exact["semantic_residual"] / 4, 6),
        "control_exact_rate": round(
            (role_exact["empty_control"] + role_exact["nonempty_control"]) / 2, 6
        ),
        "abstention_count": abstentions,
    }
    checks = {
        "structured_field_accuracy": metrics["structured_field_accuracy"] == 1.0,
        "root_field_f1": metrics["root_field_f1"] == 1.0,
        "root_checklist_cell_accuracy": metrics["root_checklist_cell_accuracy"] == 1.0,
        "exact_case_rate": metrics["exact_case_rate"] == 1.0,
        "evidence_residual_exact_rate": metrics["evidence_residual_exact_rate"] == 1.0,
        "semantic_residual_exact_rate": metrics["semantic_residual_exact_rate"] == 1.0,
        "control_exact_rate": metrics["control_exact_rate"] == 1.0,
        "abstention_count": metrics["abstention_count"] == 0,
    }
    return {
        "schema_version": V67_SCORE_VERSION,
        "passed": all(checks.values()),
        "metrics": metrics,
        "checks": checks,
        "failed_checks": sorted(key for key, passed in checks.items() if not passed),
        "gates_frozen_before_semantic_calls": True,
        "next_authorization_if_passed": "one_fresh_all_36_layered_protocol_diagnostic_only",
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V67_CAPACITY_AUDIT_VERSION,
        "phase_id": V67_PHASE_ID,
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
        prior = _load_json(audit_path, "v67 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV67Error("immutable v67 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V67_CAPACITY_POLICY_VERSION,
        "phase_id": V67_PHASE_ID,
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
            len(TURN_NAMES) * MAX_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    if policy_path.exists():
        prior = _load_json(policy_path, "v67 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV67Error("immutable v67 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v67(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v66_root: Path = DEFAULT_V66_ROOT,
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessor(v66_root.resolve())
    source = _load_json(DEFAULT_V65_ROOT / "fresh-structured-input.private.json", "v65 input")
    prior_output = _load_json(DEFAULT_V65_ROOT / "fresh-structured-output.private.json", "v65 output")
    full_truth = _load_json(DEFAULT_V64_ROOT / "calibration-truth.private.json", "v64 truth")
    packet = _load_json(v66_root / "reference-owner-adjudication.private.json", "v66 packet")
    value, truth, roles = build_v67_cohort(source, prior_output, full_truth, packet)
    input_path = root / "layered-input.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    role_path = root / "cohort-roles.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(role_path, roles)
    pointwise_prompt = build_pointwise_prompt(value)
    pointwise_schema = pointwise_output_schema(value)
    pointwise_paths = _freeze_turn_request(
        root=root,
        turn_name=POINTWISE_TURN,
        input_value=value,
        prompt=pointwise_prompt,
        schema=pointwise_schema,
    )
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V67_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "luna_pointwise_field_facts_then_sol_neutral_root_verification",
        "case_count": truth["case_count"],
        "witness_count": 12,
        "turn_plan": [
            {"turn_name": POINTWISE_TURN, "model": POINTWISE_MODEL, "effort": POINTWISE_EFFORT},
            {"turn_name": ROOT_TURN, "model": ROOT_MODEL, "effort": ROOT_EFFORT},
        ],
        "prior_labels_in_model_input": False,
        "candidate_outputs_in_model_input": False,
        "retry_count_per_turn": 0,
        "evidence_projection": "deterministic_exact_substring_and_submitted_exact_flag_only",
        "unsupported_projection": "frozen_llm_proposition_support_receipt_only",
        "promotion_rule": "perfect_targeted_diagnostic_authorizes_fresh_all_36_only",
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v66_external_blocker.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v65_fresh_structured.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v64_reference_freeze.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
            _record(runtime_dir / "app_server_judge_v5_fixture.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
        ],
        "frozen_inputs": {
            "full": _record(input_path),
            "truth": _record(truth_path),
            "roles": _record(role_path),
            "pointwise_turn": {
                "input": _record(pointwise_paths["input"]),
                "prompt": _record(pointwise_paths["prompt"]),
                "schema": _record(pointwise_paths["schema"]),
            },
        },
        "root_turn_freeze_rule": "freeze_once_after_measured_pointwise_receipt_before_root_thread_start",
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "layered-diagnostic-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v67 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV67Error("immutable v67 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
        "truth": truth,
        "roles": roles,
        "pointwise": {
            "input": value,
            "prompt": pointwise_prompt,
            "schema": pointwise_schema,
            "paths": pointwise_paths,
        },
        "capacity_policy": capacity["policy"],
    }


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = _attempt_records(root)
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    incomplete = False
    for attempt in attempts:
        sidecar_record = attempt.get("sidecar")
        if not isinstance(sidecar_record, Mapping):
            if attempt.get("capacity") is not None or attempt.get("output") is not None:
                incomplete = True
            continue
        try:
            usage = _validate_usage(_load_json(Path(sidecar_record["path"]), "v67 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = not incomplete and unknown == 0
    failure = {
        "schema_version": V67_FAILURE_VERSION,
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
        "schema_version": V67_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "fresh_all_36_authorized": False,
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


async def run_v67(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v66_root: Path = DEFAULT_V66_ROOT,
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v67 terminal")
    frozen = freeze_v67(
        output_dir=root, v66_root=v66_root, timeout_seconds=timeout_seconds
    )
    factory = client_factory or _client_factory
    sidecars = []
    adopted = {}
    current_turn = None
    try:
        async with factory(frozen["capacity_policy"]) as client:
            current_turn = POINTWISE_TURN
            pointwise_output, sidecar, was_adopted = await _get_or_run_turn(
                client=client,
                turn_name=current_turn,
                paths=frozen["pointwise"]["paths"],
                prompt=frozen["pointwise"]["prompt"],
                schema=frozen["pointwise"]["schema"],
                base_instructions=pointwise_base_instructions(),
                model=POINTWISE_MODEL,
                effort=POINTWISE_EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=12,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda value: validate_pointwise_output(value, frozen["value"]),
            )
            sidecars.append(sidecar)
            adopted[current_turn] = was_adopted
            receipts = freeze_pointwise_receipts(pointwise_output, frozen["value"])
            receipt_path = root / "pointwise-field-receipts.private.json"
            _write_immutable(receipt_path, receipts)
            root_input = build_root_input(frozen["value"], receipts)
            root_prompt = build_root_prompt(root_input)
            root_schema = root_output_schema(root_input)
            root_paths = _freeze_turn_request(
                root=root,
                turn_name=ROOT_TURN,
                input_value=root_input,
                prompt=root_prompt,
                schema=root_schema,
            )
            transition = {
                "schema_version": "pif_app_server_judge_v5_4_v67_transition_v1",
                "created_at": now_iso(),
                "pointwise_receipts": _record(receipt_path),
                "pointwise_sidecar": _record(frozen["pointwise"]["paths"]["sidecar"]),
                "root_turn": {
                    "input": _record(root_paths["input"]),
                    "prompt": _record(root_paths["prompt"]),
                    "schema": _record(root_paths["schema"]),
                },
                "root_thread_started_at_freeze": False,
            }
            transition_path = root / "pointwise-to-root-transition.json"
            _write_immutable(transition_path, transition)
            current_turn = ROOT_TURN
            root_output, sidecar, was_adopted = await _get_or_run_turn(
                client=client,
                turn_name=current_turn,
                paths=root_paths,
                prompt=root_prompt,
                schema=root_schema,
                base_instructions=root_base_instructions(),
                model=ROOT_MODEL,
                effort=ROOT_EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=12,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda value: validate_root_output(
                    project_root_contract(value, root_input)[0], root_input
                ),
            )
            sidecars.append(sidecar)
            adopted[current_turn] = was_adopted
        projected, operations = project_root_contract(root_output, root_input)
        errors = validate_root_output(projected, root_input)
        if errors:
            raise JudgeV5CalibrationV67Error("projected root output invalid: " + "; ".join(errors))
        output_path = root / "layered-output.private.json"
        _write_immutable(output_path, projected)
        audit = {
            "schema_version": V67_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "semantic_field_source": ROOT_MODEL,
            "evidence_source": "deterministic_exact_evidence_receipt",
            "unsupported_source": "frozen_llm_proposition_support_receipt",
            "new_semantic_decisions_from_deterministic_code": False,
            "privacy": "opaque_ids_enums_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v67(projected, frozen["truth"], frozen["roles"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "layered-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V67_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v67_layered_residual_passed_fresh_all_36_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v67_layered_residual_passed_fresh_all_36_authorized"
                if passed
                else "v67_layered_residual_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "layered_residual_diagnostic_passed": passed,
            "fresh_all_36_authorized": passed,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(output_path),
            "projection_audit": _record(audit_path),
            "pointwise_receipts": _record(root / "pointwise-field-receipts.private.json"),
            "transition": _record(root / "pointwise-to-root-transition.json"),
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
    parser = argparse.ArgumentParser(description="Run v67 layered residual diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v66-root", default=str(DEFAULT_V66_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v67(
            output_dir=Path(args.output_dir),
            v66_root=Path(args.v66_root),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "layered_residual_diagnostic_passed": terminal.get(
                    "layered_residual_diagnostic_passed", False
                ),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
