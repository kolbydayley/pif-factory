from __future__ import annotations

"""Explicit value-relation versus independent-root diagnostic over v61 errors."""

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
from .app_server_judge_v5_calibration_v61_adjudication import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V61_ROOT,
    _validate_predecessors as _validate_v61_predecessors,
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


V62_INPUT_VERSION = "pif_app_server_judge_v5_4_v62_root_status_input_v1"
V62_TRUTH_VERSION = "pif_app_server_judge_v5_4_v62_root_status_truth_v1"
V62_SPEC_VERSION = "pif_app_server_judge_v5_4_v62_root_status_spec_v1"
V62_SCORE_VERSION = "pif_app_server_judge_v5_4_v62_root_status_score_v1"
V62_AUDIT_VERSION = "pif_app_server_judge_v5_4_v62_contract_projection_audit_v1"
V62_FAILURE_VERSION = "pif_app_server_judge_v5_4_v62_root_status_failure_v1"
V62_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v62_root_status_terminal_v1"
V62_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V62_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V62_PHASE_ID = "judge_v5_4_v62_explicit_root_status_diagnostic"
TURN_NAME = "explicit_root_status_diagnostic"
VALUE_RELATIONS = ("same", "different", "abstain")
ROOT_STATUSES = ("root", "derivative", "not_different", "abstain")
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V61_ROOT.parent / "judge-calibration-v5_4-v62-explicit-root-status"
).resolve()


class JudgeV5CalibrationV62RootStatusError(RuntimeError):
    """The explicit root-status diagnostic cannot preserve its contract."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _validate_v61(v61_root: Path) -> dict[str, Any]:
    paths = {
        "v61_terminal": v61_root / "terminal.json",
        "v61_spec": v61_root / "adjudication-spec.json",
        "v61_score": v61_root / "adjudication-score.json",
        "v61_input": v61_root / "adjudication-input.private.json",
        "v61_truth": v61_root / "diagnostic-truth.private.json",
        "v61_taxonomy": v61_root / "error-taxonomy.json",
        "v61_merged_output": v61_root / "merged-output.private.json",
        "v61_adjudicated_output": v61_root / "adjudicated-output.private.json",
        "v61_audit": v61_root / "adjudication-audit.json",
        "v61_turn_input": v61_root / "turns/whole-unit-disagreement-adjudication/input.private.json",
        "v61_turn_output": v61_root / "turns/whole-unit-disagreement-adjudication/output.private.json",
        "v61_turn_capacity": v61_root / "turns/whole-unit-disagreement-adjudication/capacity.json",
        "v61_turn_sidecar": v61_root / "turns/whole-unit-disagreement-adjudication/sidecar.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v61_terminal"]
    score = values["v61_score"]
    spec = values["v61_spec"]
    sidecar = values["v61_turn_sidecar"]
    capacity = values["v61_turn_capacity"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v61_adjudication_quality_gate_not_passed"
        or terminal.get("adjudication_passed") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or not _record_matches(terminal.get("score"), paths["v61_score"])
        or not _record_matches(terminal.get("merged_output"), paths["v61_merged_output"])
        or not _record_matches(terminal.get("adjudicated_output"), paths["v61_adjudicated_output"])
        or not _record_matches(terminal.get("adjudication_audit"), paths["v61_audit"])
        or score.get("passed") is not False
        or spec.get("model") != "gpt-5.5"
        or spec.get("retry_count_per_turn") != 0
        or sidecar.get("model") != "gpt-5.5"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("state") != "completed"
        or sidecar.get("usage_status") != "measured"
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("rate_limit_reached_type") is not None
    ):
        raise JudgeV5CalibrationV62RootStatusError("v61 predecessor is inadmissible")
    usage = _validate_usage(sidecar)
    if usage != terminal.get("usage"):
        raise JudgeV5CalibrationV62RootStatusError("v61 usage drifted")
    attempts = terminal.get("attempts") or []
    if (
        len(attempts) != 1
        or not _record_matches(attempts[0].get("sidecar"), paths["v61_turn_sidecar"])
        or not _record_matches(attempts[0].get("capacity"), paths["v61_turn_capacity"])
        or not _record_matches(attempts[0].get("output"), paths["v61_turn_output"])
    ):
        raise JudgeV5CalibrationV62RootStatusError("v61 attempt binding drifted")
    upstream = _validate_v61_predecessors(
        Path(spec["predecessor"]["v59_terminal"]["path"]).parent,
        Path(spec["predecessor"]["v60_terminal"]["path"]).parent,
    )
    if spec.get("predecessor") != upstream:
        raise JudgeV5CalibrationV62RootStatusError("v61 upstream binding drifted")
    return {name: _record(path) for name, path in paths.items()}


def build_v62_cohort(
    base_input: Mapping[str, Any], output: Mapping[str, Any], truth: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    sources = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in base_input.get("units") or []
    }
    rows = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in output.get("units") or []
    }
    expected = {
        (str(case_id), str(witness_id)): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    failures = []
    empty_controls = []
    nonempty_controls = []
    for key in sorted(rows):
        observed = {
            item["field"] for item in rows[key]["checklist"] if item["decision"] == "different"
        }
        if observed != expected[key]:
            failures.append(key)
        elif expected[key]:
            nonempty_controls.append(key)
        else:
            empty_controls.append(key)
    if len(failures) != 8 or not empty_controls or not nonempty_controls:
        raise JudgeV5CalibrationV62RootStatusError("v62 cohort invariant drifted")
    selected = [*failures, empty_controls[0], nonempty_controls[0]]
    units = []
    roles = []
    for key in selected:
        source = sources[key]
        prior = rows[key]
        units.append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "source_excerpt": source["source_excerpt"],
                "structured_event": deepcopy(source["structured_event"]),
                "frozen_proposition_verdict": source["frozen_proposition_verdict"],
                "prior_value_differences": [
                    item["field"] for item in prior["checklist"] if item["decision"] == "different"
                ],
            }
        )
        roles.append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "role": "failure" if key in failures else "control",
            }
        )
    truth_cases: dict[str, Any] = {}
    for case_id, witness_id in selected:
        source_case = truth["cases"][case_id]
        case = truth_cases.setdefault(case_id, {"field_issues": {}, "structured_fields": {}})
        case["field_issues"][witness_id] = source_case["field_issues"][witness_id]
        case["structured_fields"][witness_id] = source_case["structured_fields"][witness_id]
    return (
        {
            "schema_version": V62_INPUT_VERSION,
            "units": units,
            "checklist_field_order": list(CHECKLIST_FIELDS),
            "side_labels_present": False,
            "system_identity_present": False,
            "prior_output_is_fallible": True,
        },
        {
            "schema_version": V62_TRUTH_VERSION,
            "witness_count": 10,
            "cases": truth_cases,
        },
        {"units": roles},
    )


def v62_base_instructions() -> str:
    return (
        "You are a side-free structured-event verifier. For every field, separate value_relation "
        "from independent_root_status. value_relation asks whether that field's populated value is "
        "licensed by the source. If different, mark independent_root_status=root only when the field "
        "remains materially wrong after substituting source-supported values for every other wrong "
        "field. Mark derivative when its apparent conflict disappears after those substitutions, and "
        "name the root dependency fields. same requires not_different; abstain requires abstain. actor "
        "or speaker identity differences often make other value relations appear different, but only "
        "the independently persistent fields are root. attribution is the reporting/source relation, "
        "not participant identity. evidence is independently root only when the populated evidence is "
        "itself inexact or fails the frozen proposition after substitutions. event_type is root only "
        "when the action/category remains wrong. target is root only when the semantic object/recipient "
        "remains wrong. unsupported_inference follows the frozen proposition receipt exactly and is "
        "root when unsupported. event_boundary must be same/not_different. The prior differences are "
        "fallible hints, not truth. Use exact source spans or []. Do not infer origin, vote, use "
        "confidence, regex, keywords, overlap, or embeddings."
    )


def build_v62_prompt(value: Mapping[str, Any]) -> str:
    fixture = load_fixture_truth_audit()
    packet = {
        "rubric": fixture["mismatch_checklist"],
        "mismatch_precedence": fixture["mismatch_precedence"],
        "units": value["units"],
    }
    return (
        "Return each opaque unit once and all 15 fields in supplied order. For every different "
        "value_relation, choose root or derivative; derivative requires one or more dependency_fields, "
        "while root requires none. same requires not_different and no dependencies. structured_field_verdict "
        "is incorrect iff any row is root, abstain iff no row is root and any row abstains, otherwise "
        "correct. Every nonempty span must be an exact substring.\n\n# Explicit root-status units\n"
        + _canonical_json(packet)
        + "\n"
    )


def v62_output_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    units = value["units"]
    row = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "field",
            "value_relation",
            "independent_root_status",
            "dependency_fields",
            "source_evidence_spans",
            "rationale",
        ],
        "properties": {
            "field": {"type": "string", "enum": list(CHECKLIST_FIELDS)},
            "value_relation": {"type": "string", "enum": list(VALUE_RELATIONS)},
            "independent_root_status": {"type": "string", "enum": list(ROOT_STATUSES)},
            "dependency_fields": {
                "type": "array",
                "maxItems": 4,
                "items": {"type": "string", "enum": list(CHECKLIST_FIELDS)},
            },
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
            "case_id": {"type": "string", "enum": sorted({x["case_id"] for x in units})},
            "witness_id": {"type": "string", "enum": sorted(x["witness_id"] for x in units)},
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
        raise JudgeV5CalibrationV62RootStatusError("v62 schema exceeds supported subset")
    return schema


def project_v62_contract(
    output: Mapping[str, Any], value: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    projected = deepcopy(output)
    sources = {
        (row["case_id"], row["witness_id"]): row for row in value.get("units") or []
    }
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
        checklist = {
            row.get("field"): row for row in unit.get("checklist") or [] if isinstance(row, Mapping)
        }
        unsupported = checklist.get("unsupported_inference")
        if isinstance(unsupported, Mapping):
            expected = {
                "supported": ("same", "not_different"),
                "unsupported": ("different", "root"),
                "abstain": ("abstain", "abstain"),
            }[source["frozen_proposition_verdict"]]
            prior = (
                unsupported.get("value_relation"),
                unsupported.get("independent_root_status"),
            )
            if prior != expected or unsupported.get("dependency_fields"):
                unsupported["value_relation"] = expected[0]
                unsupported["independent_root_status"] = expected[1]
                unsupported["dependency_fields"] = []
                operations.append(
                    {
                        "operation_type": "project_frozen_support_receipt",
                        "case_id": key[0],
                        "witness_id": key[1],
                        "field": "unsupported_inference",
                        "prior": list(prior),
                        "projected": list(expected),
                    }
                )
        statuses = {
            row.get("independent_root_status")
            for row in unit.get("checklist") or []
            if isinstance(row, Mapping)
        }
        unit["structured_field_verdict"] = (
            "incorrect" if "root" in statuses else "abstain" if "abstain" in statuses else "correct"
        )
    return projected, operations


def validate_v62_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping) or set(output) != {"units"} or not isinstance(output.get("units"), list):
        return ["invalid_v62_output_root"]
    expected = {
        (row["case_id"], row["witness_id"]): row for row in value.get("units") or []
    }
    seen = set()
    errors = []
    for index, unit in enumerate(output["units"]):
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
            errors.append(prefix + "_checklist_order")
            continue
        source = expected[key]["source_excerpt"]
        for row_index, row in enumerate(checklist):
            rp = f"{prefix}_row_{row_index}"
            if not isinstance(row, Mapping) or set(row) != {
                "field", "value_relation", "independent_root_status", "dependency_fields", "source_evidence_spans", "rationale"
            }:
                errors.append(rp + "_shape")
                continue
            relation = row.get("value_relation")
            status = row.get("independent_root_status")
            deps = row.get("dependency_fields")
            spans = row.get("source_evidence_spans")
            if relation not in VALUE_RELATIONS or status not in ROOT_STATUSES:
                errors.append(rp + "_enum")
            valid_pair = (
                (relation == "same" and status == "not_different")
                or (relation == "different" and status in {"root", "derivative"})
                or (relation == "abstain" and status == "abstain")
            )
            if not valid_pair:
                errors.append(rp + "_relation_status")
            if (
                not isinstance(deps, list)
                or len(deps) != len(set(deps))
                or any(dep not in CHECKLIST_FIELDS or dep == row.get("field") for dep in deps)
                or (status == "derivative" and not deps)
                or (status != "derivative" and deps)
            ):
                errors.append(rp + "_dependencies")
            if (
                not isinstance(spans, list)
                or len(spans) > 2
                or len(spans) != len(set(spans))
                or any(not isinstance(span, str) or not span or span not in source for span in spans)
            ):
                errors.append(rp + "_evidence")
        rows = {row.get("field"): row for row in checklist if isinstance(row, Mapping)}
        boundary = rows.get("event_boundary") or {}
        if (
            boundary.get("value_relation") != "same"
            or boundary.get("independent_root_status") != "not_different"
        ):
            errors.append(prefix + "_event_boundary_scope")
        unsupported = rows.get("unsupported_inference") or {}
        expected_unsupported = {
            "supported": ("same", "not_different"),
            "unsupported": ("different", "root"),
            "abstain": ("abstain", "abstain"),
        }[expected[key]["frozen_proposition_verdict"]]
        if (
            unsupported.get("value_relation"),
            unsupported.get("independent_root_status"),
        ) != expected_unsupported:
            errors.append(prefix + "_unsupported_projection")
        statuses = {row.get("independent_root_status") for row in checklist if isinstance(row, Mapping)}
        projected_verdict = "incorrect" if "root" in statuses else "abstain" if "abstain" in statuses else "correct"
        if unit.get("structured_field_verdict") != projected_verdict:
            errors.append(prefix + "_verdict_projection")
    if seen != set(expected):
        errors.append("v62_output_coverage")
    return errors


def score_v62(
    output: Mapping[str, Any], truth: Mapping[str, Any], roles: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    role = {(row["case_id"], row["witness_id"]): row["role"] for row in roles["units"]}
    rows = {(row["case_id"], row["witness_id"]): row for row in output["units"]}
    if set(rows) != set(expected):
        raise JudgeV5CalibrationV62RootStatusError("v62 score coverage drifted")
    tp = fp = fn = exact = control_exact = verdict_correct = abstentions = 0
    for key, fields in expected.items():
        observed = {
            row["field"] for row in rows[key]["checklist"] if row["independent_root_status"] == "root"
        }
        tp += len(fields & observed)
        fp += len(observed - fields)
        fn += len(fields - observed)
        is_exact = observed == fields
        exact += int(is_exact)
        if role[key] == "control":
            control_exact += int(is_exact)
        expected_verdict = "incorrect" if fields else "correct"
        verdict_correct += int(rows[key]["structured_field_verdict"] == expected_verdict)
        abstentions += sum(
            row["independent_root_status"] == "abstain" for row in rows[key]["checklist"]
        )
    den = 2 * tp + fp + fn
    metrics = {
        "witness_count": 10,
        "structured_field_accuracy": round(verdict_correct / 10, 6),
        "root_field_f1": round((2 * tp) / den, 6) if den else 0.0,
        "exact_case_rate": round(exact / 10, 6),
        "control_exact_rate": round(control_exact / 2, 6),
        "abstention_count": abstentions,
    }
    checks = {
        "structured_field_accuracy": metrics["structured_field_accuracy"] >= 0.95,
        "root_field_f1": metrics["root_field_f1"] >= 0.95,
        "exact_case_rate": metrics["exact_case_rate"] >= 0.95,
        "control_exact_rate": metrics["control_exact_rate"] == 1.0,
        "abstention_count": metrics["abstention_count"] == 0,
    }
    return {
        "schema_version": V62_SCORE_VERSION,
        "passed": all(checks.values()),
        "metrics": metrics,
        "checks": checks,
        "failed_checks": sorted(key for key, passed in checks.items() if not passed),
        "gates_frozen_before_semantic_calls": True,
        "next_authorization_if_passed": "fresh_all_36_witness_structured_protocol_diagnostic_only",
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V62_CAPACITY_AUDIT_VERSION,
        "phase_id": V62_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v62 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV62RootStatusError("immutable v62 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V62_CAPACITY_POLICY_VERSION,
        "phase_id": V62_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [TURN_NAME],
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        "projected_phase_quota_points": math.ceil(
            MAX_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    if policy_path.exists():
        prior = _load_json(policy_path, "v62 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV62RootStatusError("immutable v62 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v62(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v61_root: Path = DEFAULT_V61_ROOT,
    model: str = "gpt-5.5",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v61(v61_root.resolve())
    base_input = _load_json(v61_root / "adjudication-input.private.json", "v61 input")
    # Reconstruct all 18 source units from v61 plus its five carried agreement units' v60 sources.
    v60_root = Path(_load_json(v61_root / "adjudication-spec.json", "v61 spec")["predecessor"]["v60_terminal"]["path"]).parent
    all_source = {
        "units": [
            row
            for index in range(2)
            for row in _load_json(
                v60_root / "turns" / f"root-projection-shard-{index:02d}" / "input.private.json",
                f"v60 shard{index:02d} input",
            )["units"]
        ]
    }
    output = _load_json(v61_root / "merged-output.private.json", "v61 merged output")
    full_truth = _load_json(v61_root / "diagnostic-truth.private.json", "v61 truth")
    value, truth, roles = build_v62_cohort(all_source, output, full_truth)
    input_path = root / "root-status-input.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    roles_path = root / "cohort-roles.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(roles_path, roles)
    prompt = build_v62_prompt(value)
    schema = v62_output_schema(value)
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=value, prompt=prompt, schema=schema
    )
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V62_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "explicit_value_relation_and_independent_root_status",
        "failure_unit_count": 8,
        "control_unit_count": 2,
        "retry_count_per_turn": 0,
        "promotion_rule": "pass_authorizes_fresh_all_36_witness_structured_protocol_diagnostic_only",
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v61_adjudication.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v60_gpt54.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "truth": _record(truth_path),
            "roles": _record(roles_path),
            "turn_input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "root-status-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v62 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV62RootStatusError("immutable v62 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
        "truth": truth,
        "roles": roles,
        "prompt": prompt,
        "schema": schema,
        "paths": paths,
        "capacity_policy": capacity["policy"],
    }


def _write_failure(root: Path, error_class: str) -> dict[str, Any]:
    attempts = _attempt_records(root)
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            continue
        try:
            usage = _validate_usage(_load_json(Path(record["path"]), "v62 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = unknown == 0
    failure = {
        "schema_version": V62_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
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
        "schema_version": V62_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "all_36_diagnostic_authorized": False,
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


async def run_v62(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v61_root: Path = DEFAULT_V61_ROOT,
    model: str = "gpt-5.5",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v62 terminal")
    frozen = freeze_v62(
        output_dir=root,
        v61_root=v61_root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
    )
    factory = client_factory or _client_factory
    try:
        async with factory(frozen["capacity_policy"]) as client:
            output, sidecar, was_adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=v62_base_instructions(),
                model=model,
                effort=reasoning_effort,
                timeout_seconds=timeout_seconds,
                batch_size=10,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda value: validate_v62_output(
                    project_v62_contract(value, frozen["value"])[0], frozen["value"]
                ),
            )
        projected, operations = project_v62_contract(output, frozen["value"])
        if validate_v62_output(projected, frozen["value"]):
            raise JudgeV5CalibrationV62RootStatusError("projected v62 output remained invalid")
        output_path = root / "root-status-output.private.json"
        _write_immutable(output_path, projected)
        audit = {
            "schema_version": V62_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "root_field_projection_source": "llm_independent_root_status_only",
            "new_semantic_decisions_from_deterministic_code": False,
            "privacy": "opaque_ids_enums_counts_and_span_hashes_only",
        }
        audit_path = root / "contract-projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v62(projected, frozen["truth"], frozen["roles"])
        score["contract_projection_audit"] = _record(audit_path)
        score_path = root / "root-status-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage([sidecar])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V62_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v62_root_status_passed_all_36_diagnostic_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v62_root_status_passed_all_36_diagnostic_authorized"
                if passed
                else "v62_root_status_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "root_status_diagnostic_passed": passed,
            "all_36_diagnostic_authorized": passed,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(output_path),
            "contract_projection_audit": _record(audit_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": {TURN_NAME: was_adopted},
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.error_class)
    except Exception as exc:
        return _write_failure(root, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v62 explicit root-status diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v61-root", default=str(DEFAULT_V61_ROOT))
    parser.add_argument("--model", default="gpt-5.5")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v62(
            output_dir=Path(args.output_dir),
            v61_root=Path(args.v61_root),
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
                "root_status_diagnostic_passed": terminal.get("root_status_diagnostic_passed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
