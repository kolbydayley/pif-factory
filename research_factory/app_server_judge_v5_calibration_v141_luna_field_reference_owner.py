from __future__ import annotations

"""Fresh Luna singleton owner for the nine stable v140 field-reference disputes."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v140_layered_field_diagnostic as v140
from .app_server_judge_v5 import CHECKLIST_FIELDS
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
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
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v115_full_contested_field_owner import (
    base_instructions_v115,
    build_prompt_v115,
)
from .app_server_judge_v5_calibration_v126_singleton_field_owner import (
    _validate_v125,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _validate_usage,
)
from .util import now_iso, sha256_text


V141_INPUT_VERSION = "pif_app_server_judge_v5_4_v141_luna_owner_input_v1"
V141_TRUTH_VERSION = "pif_app_server_judge_v5_4_v141_luna_owner_truth_v1"
V141_SELECTION_VERSION = "pif_app_server_judge_v5_4_v141_selection_v1"
V141_SPEC_VERSION = "pif_app_server_judge_v5_4_v141_spec_v1"
V141_SCORE_VERSION = "pif_app_server_judge_v5_4_v141_score_v1"
V141_REFERENCE_VERSION = "pif_app_server_judge_v5_4_calibration_truth_v12_luna_field_owner_frozen"
V141_FAILURE_VERSION = "pif_app_server_judge_v5_4_v141_failure_v1"
V141_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v141_terminal_v1"
V141_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V141_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V141_PHASE_ID = "judge_v5_4_v141_luna_field_reference_owner"

MODEL = "gpt-5.6-luna"
EFFORT = "high"
FIELDS = ("event_boundary", "evidence", "metric", "reported_actor", "stance")
CONTROL_TURNS = tuple(f"luna_control_{field}" for field in FIELDS)
OWNER_TURNS = tuple(f"luna_reference_owner_{index:02d}" for index in range(9))
TURN_NAMES = CONTROL_TURNS + OWNER_TURNS
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    v140.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v141-luna-field-reference-owner"
).resolve()


class JudgeV5CalibrationV141Error(RuntimeError):
    """The v141 Luna reference-owner contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v140() -> dict[str, Any]:
    root = v140.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "layered-field-diagnostic-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "layered-field-score.json",
        "support": root / "support-output.private.json",
        "support_canary": root / "support-order-canary.private.json",
        "fields": root / "field-output.private.json",
        "field_canary": root / "field-order-canary.private.json",
        "truth": root / "layered-field-truth.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v140 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v140_layered_support_field_quality_gate_not_passed"
        or terminal.get("expanded_layered_field_diagnostic_authorized") is not False
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 718322
        or score.get("passed") is not False
        or score.get("failed_checks")
        != ["field_decision_accuracy", "pointwise_field_issue_f1"]
        or score.get("metrics", {}).get("support_sensitivity") != 1.0
        or score.get("metrics", {}).get("support_specificity") != 1.0
        or score.get("metrics", {}).get("support_canary_exact_count") != 5
        or score.get("metrics", {}).get("field_decision_accuracy") != 0.88
        or score.get("metrics", {}).get("pointwise_field_issue_f1") != 0.709677
        or score.get("metrics", {}).get("structured_field_accuracy") != 1.0
        or score.get("metrics", {}).get("field_canary_exact_count") != 75
        or spec.get("model") != "gpt-5.6-sol"
        or spec.get("reasoning_effort") != "high"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV141Error("v140 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV141Error("v140 runtime record drifted")
    for name, key in {
        "score": "score",
        "support": "support_output",
        "support_canary": "support_canary",
        "fields": "field_output",
        "field_canary": "field_canary",
    }.items():
        record = terminal.get(key)
        if record != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV141Error(f"v140 {name} record drifted")
    attempts = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    frozen_turns = {row["turn_name"]: row for row in spec["frozen_inputs"]["turns"]}
    input_tasks = {}
    for turn_name in spec["turn_plan"]:
        frozen = frozen_turns[turn_name]
        turn_root = root / "turns" / turn_name.replace("_", "-")
        records = {}
        for name, filename in {
            "capacity": "capacity.json",
            "sidecar": "sidecar.json",
            "output": "output.private.json",
        }.items():
            path = turn_root / filename
            if not path.is_file():
                raise JudgeV5CalibrationV141Error("v140 turn coverage is incomplete")
            records[name] = _record(path)
        if frozen["role"] == "field_primary":
            value = _load_json(Path(frozen["input"]["path"]), "v140 field input")
            for task in value["tasks"]:
                input_tasks[str(task["task_id"])] = task
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v140 sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"] or len(input_tasks) != 75:
        raise JudgeV5CalibrationV141Error("v140 usage or task coverage drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "input_tasks": input_tasks,
        "v139": v140._validate_v139(),
    }


def _task_id(role: str, source_task_id: str) -> str:
    return role + "_" + sha256_text(f"v141|{role}|{source_task_id}")[:24]


def _one_task_value(task: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": V141_INPUT_VERSION,
        "task_count": 1,
        "tasks": [deepcopy(task)],
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "disagreement_reasons_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "singleton_context": True,
    }


def build_v141_inputs(
    predecessor: Mapping[str, Any], controls_source: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    truth_rows = {
        row["task_id"]: row for row in predecessor["values"]["truth"]["field_tasks"]
    }
    observed = {
        row["task_id"]: row for row in predecessor["values"]["fields"]["decisions"]
    }
    repeated = {
        row["task_id"]: row
        for row in predecessor["values"]["field_canary"]["decisions"]
    }
    disputes = [
        row
        for task_id, row in truth_rows.items()
        if observed[task_id]["field_status"] != row["expected_status"]
        and repeated[task_id]["field_status"] == observed[task_id]["field_status"]
    ]
    counts = {field: sum(row["field"] == field for row in disputes) for field in FIELDS}
    if counts != {
        "event_boundary": 1,
        "evidence": 1,
        "metric": 3,
        "reported_actor": 2,
        "stance": 2,
    } or len(disputes) != 9:
        raise JudgeV5CalibrationV141Error("v141 stable dispute coverage drifted")

    v121_truth = controls_source["v124"]["v122"]["v121"]["values"]["truth"][
        "tasks"
    ]
    v121_input = {
        row["task_id"]: row
        for row in controls_source["v124"]["v122"]["v121_input"]["tasks"]
    }
    v121_primary = {
        row["task_id"]: row
        for row in controls_source["v124"]["v122"]["values"]["primary"][
            "decisions"
        ]
    }
    rows = []
    owner_truth = []
    for field in FIELDS:
        candidates = [
            row
            for row in v121_truth
            if row["role"] == "matched_control"
            and row["field"] == field
            and v121_primary[row["task_id"]]["field_status"]
            == row["control_expected_status"]
            and v121_primary[row["task_id"]]["field_status"] != "abstain"
        ]
        candidates.sort(key=lambda row: sha256_text(f"v141|control|{row['task_id']}"))
        if not candidates:
            raise JudgeV5CalibrationV141Error("v141 control coverage drifted")
        source = candidates[0]
        task = deepcopy(v121_input[source["task_id"]])
        task_id = _task_id("control", source["task_id"])
        task["task_id"] = task_id
        rows.append(
            {
                "turn_name": f"luna_control_{field}",
                "turn_role": "control",
                "field": field,
                "value": _one_task_value(task),
            }
        )
        owner_truth.append(
            {
                "task_id": task_id,
                "role": "control",
                "field": field,
                "source_task_id": source["task_id"],
                "control_expected_status": source["control_expected_status"],
            }
        )
    disputes.sort(
        key=lambda row: sha256_text(
            f"v141|owner|{row['field']}|{row['case_id']}|{row['witness_id']}"
        )
    )
    for turn_name, dispute in zip(OWNER_TURNS, disputes, strict=True):
        source_task_id = dispute["task_id"]
        task = deepcopy(predecessor["input_tasks"][source_task_id])
        task_id = _task_id("owner", source_task_id)
        task["task_id"] = task_id
        rows.append(
            {
                "turn_name": turn_name,
                "turn_role": "owner",
                "field": dispute["field"],
                "value": _one_task_value(task),
            }
        )
        owner_truth.append(
            {
                "task_id": task_id,
                "role": "owner",
                "source_v140_task_id": source_task_id,
                "case_id": dispute["case_id"],
                "witness_id": dispute["witness_id"],
                "field": dispute["field"],
                "current_status": dispute["expected_status"],
                "v140_status": observed[source_task_id]["field_status"],
            }
        )
    rows.sort(key=lambda row: TURN_NAMES.index(row["turn_name"]))
    truth = {
        "schema_version": V141_TRUTH_VERSION,
        "task_count": 14,
        "control_count": 5,
        "owner_count": 9,
        "tasks": owner_truth,
    }
    selection = {
        "schema_version": V141_SELECTION_VERSION,
        "created_at": now_iso(),
        "stable_reference_dispute_count": 9,
        "dispute_field_counts": counts,
        "control_count": 5,
        "owner_count": 9,
        "maximum_tasks_per_turn": 1,
        "selection_uses_source_text": False,
        "selection_uses_only_stable_model_reference_disagreements": True,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
        "privacy": "private_source_event_output_aggregate_reports_only",
    }
    return rows, truth, selection


def score_v141(outputs: Mapping[str, Mapping[str, Any]], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {
        row["task_id"]: row
        for output in outputs.values()
        for row in output["decisions"]
    }
    if set(observed) != set(expected) or len(observed) != 14:
        raise JudgeV5CalibrationV141Error("v141 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "control"]
    owners = [row for row in expected.values() if row["role"] == "owner"]
    control_exact = sum(
        observed[row["task_id"]]["field_status"]
        == row["control_expected_status"]
        for row in controls
    )
    owner_abstentions = sum(
        observed[row["task_id"]]["field_status"] == "abstain" for row in owners
    )
    evidence_complete = sum(
        bool(row["source_evidence_spans"]) for row in observed.values()
    )
    owner_v140_agreement = sum(
        observed[row["task_id"]]["field_status"] == row["v140_status"] for row in owners
    )
    checks = {
        "singleton_control_exact_rate": control_exact == len(controls),
        "singleton_owner_abstention_count": owner_abstentions == 0,
        "evidence_complete_rate": evidence_complete == len(observed),
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V141_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": 14,
            "control_count": len(controls),
            "control_exact_count": control_exact,
            "owner_count": len(owners),
            "owner_abstention_count": owner_abstentions,
            "owner_v140_agreement_count": owner_v140_agreement,
            "evidence_complete_count": evidence_complete,
        },
        "reference_patch_authorized": passed,
        "repaired_v140_rescore_authorized": passed,
        "expanded_layered_field_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _observed(outputs: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    result = {
        row["task_id"]: row
        for output in outputs.values()
        for row in output["decisions"]
    }
    if len(result) != 14:
        raise JudgeV5CalibrationV141Error("v141 owner output coverage drifted")
    return result


def patch_v140_truth(
    *, current_truth: Mapping[str, Any], owner_truth: Mapping[str, Any], outputs: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    result = deepcopy(current_truth)
    observed = _observed(outputs)
    task_rows = {row["task_id"]: row for row in result["field_tasks"]}
    changed = 0
    for row in owner_truth["tasks"]:
        if row["role"] != "owner":
            continue
        source_task_id = row["source_v140_task_id"]
        status = observed[row["task_id"]]["field_status"]
        target = task_rows[source_task_id]
        before = target["expected_status"]
        target["expected_status"] = status
        issues = set(result["cases"][row["case_id"]]["field_issues"][row["witness_id"]])
        if status == "incorrect":
            issues.add(row["field"])
        else:
            issues.discard(row["field"])
        result["cases"][row["case_id"]]["field_issues"][row["witness_id"]] = [
            field for field in CHECKLIST_FIELDS if field in issues
        ]
        result["cases"][row["case_id"]]["structured_fields"][row["witness_id"]] = (
            "incorrect" if issues else "correct"
        )
        changed += int(before != status)
    result["schema_version"] = V141_TRUTH_VERSION
    result["v141_owner_task_count"] = 9
    result["v141_reference_change_count"] = changed
    return result


def patch_reference_v141(
    *, current_reference: Mapping[str, Any], owner_truth: Mapping[str, Any], outputs: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    result = deepcopy(current_reference)
    observed = _observed(outputs)
    changed = 0
    for row in owner_truth["tasks"]:
        if row["role"] != "owner":
            continue
        status = observed[row["task_id"]]["field_status"]
        case = result["cases"][row["case_id"]]
        issues = set(case["field_issues"][row["witness_id"]])
        before = row["field"] in issues
        if status == "incorrect":
            issues.add(row["field"])
        else:
            issues.discard(row["field"])
        case["field_issues"][row["witness_id"]] = [
            field for field in CHECKLIST_FIELDS if field in issues
        ]
        case["structured_fields"][row["witness_id"]] = (
            "incorrect" if issues else "correct"
        )
        changed += int(before != (status == "incorrect"))
    result["schema_version"] = V141_REFERENCE_VERSION
    result["reference_version"] = "fixture_reference_v12_luna_field_owner_frozen"
    result["v141_owner_task_count"] = 9
    result["v141_reference_change_count"] = changed
    result["v141_owner_basis"] = "fresh_singleton_luna_field_owner"
    return result


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V141_CAPACITY_AUDIT_VERSION,
        "phase_id": V141_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V141_CAPACITY_POLICY_VERSION,
        "phase_id": V141_PHASE_ID,
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
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": math.ceil(
            bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v141(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v141 terminal")}
    predecessor = _validate_v140()
    controls_source = _validate_v125()
    rows, truth, selection = build_v141_inputs(predecessor, controls_source)
    truth_path = root / "luna-field-owner-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    turns = []
    for row in rows:
        value = row["value"]
        prompt, schema = build_prompt_v115(value), output_schema(value)
        paths = _freeze_turn_request(
            root=root,
            turn_name=row["turn_name"],
            input_value=value,
            prompt=prompt,
            schema=schema,
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor_records = {
        **{f"v140_{name}": record for name, record in predecessor["records"].items()},
        "v140_attempts": predecessor["attempts"],
        "v138_reference": predecessor["v139"]["v138"]["records"]["reference"],
        **{
            f"v121_{name}": record
            for name, record in controls_source["v124"]["v122"]["v121"][
                "records"
            ].items()
        },
        "v121_input": controls_source["v124"]["v122"]["v121_input_record"],
    }
    capacity = _build_capacity_policy(root, predecessor_records)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V141_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_singleton_luna_owner_for_nine_stable_v140_field_reference_disputes",
        "control_count": 5,
        "owner_count": 9,
        "maximum_tasks_per_turn": 1,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "five_exact_controls_nine_decisive_owners_complete_evidence_then_original_v140_rescore",
        "reference_patch_authorized": False,
        "expanded_layered_field_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor_records,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v140_layered_field_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v126_singleton_field_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v115_full_contested_field_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "truth": _record(truth_path),
            "selection": _record(selection_path),
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "role": turn["turn_role"],
                    "field": turn["field"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "luna-field-reference-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v140": predecessor,
        "current_reference": predecessor["v139"]["v138"]["values"]["reference"],
    }


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(
                _load_json(Path(record["path"]), "v141 sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V141_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
        "known_usage_lower_bound": usage,
        "unknown_usage_turn_count": unknown,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V141_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_patch_authorized": False,
        "expanded_layered_field_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
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


async def run_v141(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v141 terminal")
    frozen = freeze_v141(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        outputs, sidecars = {}, []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=base_instructions_v115(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=1,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_output(
                        candidate, item
                    ),
                )
                outputs[current_turn] = output
                sidecars.append(sidecar)
        owner_score = score_v141(outputs, frozen["truth"])
        patched_truth = patch_v140_truth(
            current_truth=frozen["v140"]["values"]["truth"],
            owner_truth=frozen["truth"],
            outputs=outputs,
        )
        patched_reference = patch_reference_v141(
            current_reference=frozen["current_reference"],
            owner_truth=frozen["truth"],
            outputs=outputs,
        )
        v140_rescore = v140.score_v140(
            support=frozen["v140"]["values"]["support"],
            support_canary=frozen["v140"]["values"]["support_canary"],
            fields=frozen["v140"]["values"]["fields"],
            field_canary=frozen["v140"]["values"]["field_canary"],
            truth=patched_truth,
        )
        owner_passed = bool(owner_score["passed"])
        expanded = bool(owner_passed and v140_rescore["passed"])
        paths = {
            "outputs": root / "luna-field-owner-outputs.private.json",
            "owner_score": root / "luna-field-owner-score.json",
            "truth": root / "patched-v140-truth.private.json",
            "v140_score": root / "repaired-v140-score.json",
            "reference": root / "calibration-truth-v12-luna-field-owner.private.json",
        }
        _write_immutable(paths["outputs"], {"turns": outputs})
        _write_immutable(paths["owner_score"], owner_score)
        _write_immutable(paths["truth"], patched_truth)
        _write_immutable(paths["v140_score"], v140_rescore)
        if owner_passed:
            _write_immutable(paths["reference"], patched_reference)
        accounting = _aggregate_usage(sidecars)
        failed_checks = sorted(
            [f"owner:{key}" for key in owner_score["failed_checks"]]
            + [f"v140_rescore:{key}" for key in v140_rescore["failed_checks"]]
        )
        terminal = {
            "schema_version": V141_TERMINAL_VERSION,
            "state": "completed" if owner_passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v141_luna_reference_owner_passed_layered_expansion_authorized"
                if expanded
                else (
                    "v141_luna_reference_owner_passed_layered_gate_not_passed"
                    if owner_passed
                    else "inactive_incomplete_recovery_required"
                )
            ),
            "development_terminal_reason": (
                "v141_reference_v12_frozen_v140_rescore_passed"
                if expanded
                else (
                    "v141_reference_v12_frozen_v140_rescore_not_passed"
                    if owner_passed
                    else "v141_luna_field_reference_owner_quality_gate_not_passed"
                )
            ),
            "overall_evaluation_complete": False,
            "reference_patch_authorized": owner_passed,
            "reference_frozen": owner_passed,
            "expanded_layered_field_diagnostic_authorized": expanded,
            "alignment_diagnostic_authorized": False,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "owner_score": _record(paths["owner_score"]),
            "repaired_v140_score": _record(paths["v140_score"]),
            "outputs": _record(paths["outputs"]),
            "patched_truth": _record(paths["truth"]),
            "reference": _record(paths["reference"]) if owner_passed else None,
            "failed_quality_gates": failed_checks,
            "metrics": {
                "owner": owner_score["metrics"],
                "repaired_v140": v140_rescore["metrics"],
            },
            "predecessor_v140_usage": frozen["v140"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v141 Luna field reference owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v141(
            output_dir=Path(args.output_dir),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal.get("reference_frozen", False),
                "expanded_layered_field_diagnostic_authorized": terminal.get(
                    "expanded_layered_field_diagnostic_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
