from __future__ import annotations

"""Independent field-microtask evaluator diagnostic over the v70 cohort."""

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
from .app_server_judge_v5_calibration_v70_blind_reference_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V70_ROOT,
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


V71_INPUT_VERSION = "pif_app_server_judge_v5_4_v71_field_microtask_input_v1"
V71_SPEC_VERSION = "pif_app_server_judge_v5_4_v71_field_microtask_spec_v1"
V71_AUDIT_VERSION = "pif_app_server_judge_v5_4_v71_projection_audit_v1"
V71_SCORE_VERSION = "pif_app_server_judge_v5_4_v71_score_v1"
V71_FAILURE_VERSION = "pif_app_server_judge_v5_4_v71_failure_v1"
V71_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v71_terminal_v1"
V71_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V71_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V71_PHASE_ID = "judge_v5_4_v71_field_microtask_diagnostic"
TURN_NAME = "field_microtask_diagnostic"
MODEL = "gpt-5.4-mini"
EFFORT = "high"
ROOT_STATUSES = ("root", "not_root", "abstain")
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V70_ROOT.parent / "judge-calibration-v5_4-v71-field-microtasks"
).resolve()


class JudgeV5CalibrationV71Error(RuntimeError):
    """The v71 field-microtask diagnostic cannot preserve its contract."""


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


def _validate_v70(v70_root: Path) -> dict[str, Any]:
    paths = {
        "v70_terminal": v70_root / "terminal.json",
        "v70_spec": v70_root / "blind-reference-spec.json",
        "v70_input": v70_root / "blind-reference-input.private.json",
        "v70_truth": v70_root / "diagnostic-truth.private.json",
        "v70_roles": v70_root / "cohort-roles.json",
        "v70_proposal": v70_root / "reference-proposal.json",
        "v70_output": v70_root / "blind-reference-output.private.json",
        "v70_audit": v70_root / "projection-audit.json",
        "v70_capacity": v70_root / "turns/blind-reference-audit/capacity.json",
        "v70_sidecar": v70_root / "turns/blind-reference-audit/sidecar.json",
        "v70_raw_output": v70_root / "turns/blind-reference-audit/output.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v70_terminal"]
    spec = values["v70_spec"]
    proposal = values["v70_proposal"]
    sidecar = values["v70_sidecar"]
    attempts = terminal.get("attempts") or []
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v70_blind_reference_audit_gate_not_passed"
        or terminal.get("reference_audit_valid") is not False
        or terminal.get("reference_freeze_decision_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or not _record_matches(terminal.get("reference_proposal"), paths["v70_proposal"])
        or not _record_matches(terminal.get("output"), paths["v70_output"])
        or not _record_matches(terminal.get("projection_audit"), paths["v70_audit"])
        or proposal.get("reference_audit_valid") is not False
        or proposal.get("control_exact_count") != 1
        or proposal.get("abstention_count") != 13
        or proposal.get("reference_patch_proposal_authorized") is not False
        or spec.get("retry_count_per_turn") != 0
        or not all(_verify_record(record) for record in spec.get("runtime_files") or [])
        or sidecar.get("model") != "gpt-5.6-luna"
        or sidecar.get("effort") != "high"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("usage_status") != "measured"
        or _validate_usage(sidecar) != terminal.get("usage")
        or len(attempts) != 1
        or not _record_matches(attempts[0].get("capacity"), paths["v70_capacity"])
        or not _record_matches(attempts[0].get("sidecar"), paths["v70_sidecar"])
        or not _record_matches(attempts[0].get("output"), paths["v70_raw_output"])
    ):
        raise JudgeV5CalibrationV71Error("v70 predecessor is inadmissible")
    return {name: _record(path) for name, path in paths.items()}


def _task_id(case_id: str, witness_id: str, field: str) -> str:
    return "ftask_" + sha256_text(f"{case_id}|{witness_id}|{field}")[:24]


def build_v71_input(value: Mapping[str, Any]) -> dict[str, Any]:
    fixture = load_fixture_truth_audit()
    contracts = {row["field"]: deepcopy(row) for row in fixture["mismatch_checklist"]}
    if set(contracts) != set(CHECKLIST_FIELDS):
        raise JudgeV5CalibrationV71Error("v71 field contracts drifted")
    units = [deepcopy(row) for row in value["units"]]
    tasks = [
        {
            "task_id": _task_id(unit["case_id"], unit["witness_id"], field),
            "case_id": unit["case_id"],
            "witness_id": unit["witness_id"],
            "field": field,
        }
        for unit in units
        for field in CHECKLIST_FIELDS
    ]
    if len(tasks) != 90 or len({row["task_id"] for row in tasks}) != 90:
        raise JudgeV5CalibrationV71Error("v71 task identity drifted")
    return {
        "schema_version": V71_INPUT_VERSION,
        "units": units,
        "field_contracts": [contracts[field] for field in CHECKLIST_FIELDS],
        "tasks": tasks,
        "task_count": 90,
        "prior_labels_present": False,
        "candidate_outputs_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }


def base_instructions() -> str:
    return (
        "You are a side-free field-microtask verifier. Each task asks about exactly one structured field; "
        "decide it independently after mentally correcting all other event fields. root means this field's "
        "own populated value materially conflicts with the selected source proposition or a material required "
        "value is omitted. not_root means the field itself is source-correct or not applicable. Do not "
        "propagate another field's error. Evidence follows exact source grounding; unsupported_inference "
        "follows the frozen proposition receipt. Use exact source substrings or []. Do not infer system "
        "origin, vote, use confidence, regex, keywords, overlap, or embeddings."
    )


def build_prompt(value: Mapping[str, Any]) -> str:
    return (
        "Return exactly one decision for every opaque task_id. Process tasks independently even when they "
        "share a witness. Do not emit a whole-event verdict. Every nonempty source span must be an exact "
        "substring of that task's unit source.\n\n# Field microtasks\n"
        + _canonical_json(value)
        + "\n"
    )


def output_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    task_ids = [row["task_id"] for row in value["tasks"]]
    decision = {
        "type": "object",
        "additionalProperties": False,
        "required": ["task_id", "root_status", "source_evidence_spans", "rationale"],
        "properties": {
            "task_id": {"type": "string", "enum": task_ids},
            "root_status": {"type": "string", "enum": list(ROOT_STATUSES)},
            "source_evidence_spans": {
                "type": "array",
                "maxItems": 2,
                "items": {"type": "string", "minLength": 1, "maxLength": 1000},
            },
            "rationale": {"type": "string", "minLength": 1, "maxLength": 220},
        },
    }
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["decisions"],
        "properties": {
            "decisions": {
                "type": "array",
                "minItems": len(task_ids),
                "maxItems": len(task_ids),
                "items": decision,
            }
        },
    }
    errors = validate_app_server_output_schema_subset(schema)
    if errors:
        raise JudgeV5CalibrationV71Error("v71 schema exceeds supported subset")
    return schema


def validate_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping) or set(output) != {"decisions"}:
        return ["invalid_microtask_root"]
    tasks = {row["task_id"]: row for row in value["tasks"]}
    sources = {(row["case_id"], row["witness_id"]): row["source_excerpt"] for row in value["units"]}
    seen = set()
    errors = []
    for index, row in enumerate(output.get("decisions") or []):
        prefix = f"decision_{index}"
        if not isinstance(row, Mapping) or set(row) != {
            "task_id", "root_status", "source_evidence_spans", "rationale"
        }:
            errors.append(prefix + "_shape")
            continue
        task_id = row.get("task_id")
        if task_id not in tasks or task_id in seen:
            errors.append(prefix + "_identity")
            continue
        seen.add(task_id)
        task = tasks[task_id]
        source = sources[(task["case_id"], task["witness_id"])]
        spans = row.get("source_evidence_spans")
        if row.get("root_status") not in ROOT_STATUSES:
            errors.append(prefix + "_status")
        if (
            not isinstance(spans, list)
            or len(spans) > 2
            or len(spans) != len(set(spans))
            or any(not isinstance(span, str) or not span or span not in source for span in spans)
        ):
            errors.append(prefix + "_evidence")
        if not isinstance(row.get("rationale"), str) or not row["rationale"]:
            errors.append(prefix + "_rationale")
    if seen != set(tasks):
        errors.append("microtask_coverage")
    return errors


def project_output(
    output: Mapping[str, Any], value: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    projected = deepcopy(output)
    tasks = {row["task_id"]: row for row in value["tasks"]}
    units = {(row["case_id"], row["witness_id"]): row for row in value["units"]}
    operations = []
    for row in projected.get("decisions") or []:
        task = tasks.get(row.get("task_id"))
        if task is None:
            continue
        key = (task["case_id"], task["witness_id"])
        unit = units[key]
        spans = row.get("source_evidence_spans")
        if isinstance(spans, list):
            retained = []
            for span in spans:
                if isinstance(span, str) and span and span in unit["source_excerpt"]:
                    retained.append(span)
                else:
                    rendered = span if isinstance(span, str) else json.dumps(span, sort_keys=True)
                    operations.append(
                        {
                            "operation_type": "drop_nonexact_microtask_span",
                            "task_id": row.get("task_id"),
                            "field": task["field"],
                            "span_sha256": sha256_text(rendered),
                            "span_size_bytes": len(rendered.encode("utf-8")),
                        }
                    )
            row["source_evidence_spans"] = retained
        expected = None
        if task["field"] == "evidence":
            expected = "not_root" if unit["exact_evidence_receipt"] else "root"
        elif task["field"] == "unsupported_inference":
            expected = {
                "supported": "not_root",
                "unsupported": "root",
                "abstain": "abstain",
            }[unit["frozen_proposition_verdict"]]
        if expected is not None and row.get("root_status") != expected:
            operations.append(
                {
                    "operation_type": (
                        "project_exact_evidence_receipt"
                        if task["field"] == "evidence"
                        else "project_frozen_support_receipt"
                    ),
                    "task_id": row.get("task_id"),
                    "field": task["field"],
                    "prior": row.get("root_status"),
                    "projected": expected,
                }
            )
            row["root_status"] = expected
    return projected, operations


def assemble_checklist(output: Mapping[str, Any], value: Mapping[str, Any]) -> dict[str, Any]:
    tasks = {row["task_id"]: row for row in value["tasks"]}
    decisions = {row["task_id"]: row for row in output["decisions"]}
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for task_id, task in tasks.items():
        key = (task["case_id"], task["witness_id"])
        grouped.setdefault(key, {})[task["field"]] = decisions[task_id]
    units = []
    for unit in value["units"]:
        key = (unit["case_id"], unit["witness_id"])
        checklist = [
            {
                "field": field,
                "root_status": grouped[key][field]["root_status"],
                "source_evidence_spans": grouped[key][field]["source_evidence_spans"],
                "rationale": grouped[key][field]["rationale"],
            }
            for field in CHECKLIST_FIELDS
        ]
        statuses = {row["root_status"] for row in checklist}
        verdict = "incorrect" if "root" in statuses else "abstain" if "abstain" in statuses else "correct"
        units.append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "structured_field_verdict": verdict,
                "checklist": checklist,
            }
        )
    return {"units": units}


def score_v71(
    checklist: Mapping[str, Any], truth: Mapping[str, Any], roles: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
        if any(
            row["case_id"] == case_id and row["witness_id"] == witness_id
            for row in roles["units"]
        )
    }
    role_map = {(row["case_id"], row["witness_id"]): row["role"] for row in roles["units"]}
    rows = {(row["case_id"], row["witness_id"]): row for row in checklist["units"]}
    if set(rows) != set(expected) or set(role_map) != set(expected):
        raise JudgeV5CalibrationV71Error("v71 score coverage drifted")
    tp = fp = fn = exact = cells = abstentions = 0
    role_exact = {role: 0 for role in set(role_map.values())}
    for key, fields in expected.items():
        observed = {row["field"] for row in rows[key]["checklist"] if row["root_status"] == "root"}
        tp += len(fields & observed)
        fp += len(observed - fields)
        fn += len(fields - observed)
        match = observed == fields
        exact += int(match)
        role_exact[role_map[key]] += int(match)
        cells += sum(
            (row["root_status"] == "root") == (row["field"] in fields)
            for row in rows[key]["checklist"]
        )
        abstentions += sum(row["root_status"] == "abstain" for row in rows[key]["checklist"])
    denominator = 2 * tp + fp + fn
    metrics = {
        "witness_count": 6,
        "root_field_f1": round((2 * tp) / denominator, 6) if denominator else 0.0,
        "root_checklist_cell_accuracy": round(cells / (6 * len(CHECKLIST_FIELDS)), 6),
        "exact_case_rate": round(exact / 6, 6),
        "reference_disagreement_exact_rate": round(role_exact["reference_disagreement"] / 4, 6),
        "control_exact_rate": round(
            (role_exact["empty_control"] + role_exact["nonempty_control"]) / 2, 6
        ),
        "abstention_count": abstentions,
    }
    checks = {
        "root_field_f1": metrics["root_field_f1"] == 1.0,
        "root_checklist_cell_accuracy": metrics["root_checklist_cell_accuracy"] == 1.0,
        "exact_case_rate": metrics["exact_case_rate"] == 1.0,
        "reference_disagreement_exact_rate": metrics["reference_disagreement_exact_rate"] == 1.0,
        "control_exact_rate": metrics["control_exact_rate"] == 1.0,
        "abstention_count": metrics["abstention_count"] == 0,
    }
    return {
        "schema_version": V71_SCORE_VERSION,
        "passed": all(checks.values()),
        "metrics": metrics,
        "checks": checks,
        "failed_checks": sorted(key for key, passed in checks.items() if not passed),
        "gates_frozen_before_semantic_calls": True,
        "next_authorization_if_passed": "one_fresh_12_witness_field_microtask_diagnostic_only",
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V71_CAPACITY_AUDIT_VERSION,
        "phase_id": V71_PHASE_ID,
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
        prior = _load_json(audit_path, "v71 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV71Error("immutable v71 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V71_CAPACITY_POLICY_VERSION,
        "phase_id": V71_PHASE_ID,
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
        prior = _load_json(policy_path, "v71 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV71Error("immutable v71 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v71(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v70_root: Path = DEFAULT_V70_ROOT,
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v70(v70_root.resolve())
    source = _load_json(v70_root / "blind-reference-input.private.json", "v70 input")
    truth = _load_json(v70_root / "diagnostic-truth.private.json", "v70 truth")
    roles = _load_json(v70_root / "cohort-roles.json", "v70 roles")
    value = build_v71_input(source)
    input_path = root / "field-microtask-input.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    roles_path = root / "cohort-roles.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(roles_path, roles)
    prompt = build_prompt(value)
    schema = output_schema(value)
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=value, prompt=prompt, schema=schema
    )
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V71_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "ninety_independent_field_microtasks_single_turn",
        "witness_count": 6,
        "task_count": 90,
        "turn_plan": [TURN_NAME],
        "prior_labels_in_model_input": False,
        "candidate_outputs_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "perfect_six_witness_result_authorizes_fresh_12_only",
        "fresh_12_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v70_blind_reference_audit.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "truth": _record(truth_path),
            "roles": _record(roles_path),
            "turn": {
                "input": _record(paths["input"]),
                "prompt": _record(paths["prompt"]),
                "schema": _record(paths["schema"]),
            },
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "field-microtask-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v71 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV71Error("immutable v71 spec drifted")
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
    complete = True
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            complete = False
            continue
        try:
            usage = _validate_usage(_load_json(Path(record["path"]), "v71 sidecar"))
        except Exception:
            complete = False
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    failure = {
        "schema_version": V71_FAILURE_VERSION,
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
        "schema_version": V71_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "fresh_12_authorized": False,
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


async def run_v71(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v70_root: Path = DEFAULT_V70_ROOT,
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v71 terminal")
    frozen = freeze_v71(
        output_dir=root, v70_root=v70_root, timeout_seconds=timeout_seconds
    )
    factory = client_factory or _client_factory
    try:
        async with factory(frozen["capacity_policy"]) as client:
            output, sidecar, adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=base_instructions(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=90,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda value: validate_output(
                    project_output(value, frozen["value"])[0], frozen["value"]
                ),
            )
        projected, operations = project_output(output, frozen["value"])
        errors = validate_output(projected, frozen["value"])
        if errors:
            raise JudgeV5CalibrationV71Error("projected v71 output is invalid")
        raw_path = root / "field-microtask-output.private.json"
        _write_immutable(raw_path, projected)
        checklist = assemble_checklist(projected, frozen["value"])
        checklist_path = root / "assembled-checklist.private.json"
        _write_immutable(checklist_path, checklist)
        audit = {
            "schema_version": V71_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "semantic_field_source": MODEL,
            "assembly_is_structural_only": True,
            "new_semantic_decisions_from_deterministic_code": False,
            "privacy": "opaque_task_ids_enums_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v71(checklist, frozen["truth"], frozen["roles"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "field-microtask-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage([sidecar])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V71_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v71_field_microtask_passed_fresh_12_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v71_field_microtask_passed_fresh_12_authorized"
                if passed
                else "v71_field_microtask_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "field_microtask_diagnostic_passed": passed,
            "fresh_12_authorized": passed,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(raw_path),
            "assembled_checklist": _record(checklist_path),
            "projection_audit": _record(audit_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": {TURN_NAME: adopted},
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.error_class)
    except Exception as exc:
        return _write_failure(root, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v71 field-microtask diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v70-root", default=str(DEFAULT_V70_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v71(
            output_dir=Path(args.output_dir),
            v70_root=Path(args.v70_root),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "field_microtask_diagnostic_passed": terminal.get(
                    "field_microtask_diagnostic_passed", False
                ),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
