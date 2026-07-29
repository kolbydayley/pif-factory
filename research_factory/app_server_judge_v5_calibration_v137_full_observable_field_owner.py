from __future__ import annotations

"""Expand the validated field-specific owner over every observable v134 dispute."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v108_layered_diagnostic as v108
from . import app_server_judge_v5_calibration_v135_dominant_field_owner_diagnostic as v135
from . import app_server_judge_v5_calibration_v136_dominant_field_owner_recovery as v136
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
    _field_contracts,
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v99_refined_luna_diagnostic import (
    _general_requested_field_value,
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
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .util import now_iso, sha256_text


V137_INPUT_VERSION = "pif_app_server_judge_v5_4_v137_field_owner_input_v1"
V137_TRUTH_VERSION = "pif_app_server_judge_v5_4_v137_field_owner_truth_v1"
V137_SELECTION_VERSION = "pif_app_server_judge_v5_4_v137_selection_v1"
V137_SPEC_VERSION = "pif_app_server_judge_v5_4_v137_spec_v1"
V137_SCORE_VERSION = "pif_app_server_judge_v5_4_v137_score_v1"
V137_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v137_output_v1"
V137_REFERENCE_VERSION = "pif_app_server_judge_v5_4_calibration_truth_v11_observable_field_owner_frozen"
V137_FAILURE_VERSION = "pif_app_server_judge_v5_4_v137_failure_v1"
V137_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v137_terminal_v1"
V137_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V137_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V137_PHASE_ID = "judge_v5_4_v137_full_observable_field_owner"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
FIELDS = (
    "actor",
    "attribution",
    "certainty",
    "evidence",
    "metric",
    "speaker",
    "stance",
    "target",
    "temporal_horizon",
)
OBSERVABLE_COUNTS = {
    "actor": 2,
    "attribution": 12,
    "certainty": 11,
    "evidence": 5,
    "metric": 4,
    "speaker": 9,
    "stance": 2,
    "target": 2,
    "temporal_horizon": 1,
}
REMAINING_COUNTS = {
    "actor": 2,
    "attribution": 10,
    "certainty": 9,
    "evidence": 5,
    "metric": 4,
    "speaker": 7,
    "stance": 2,
    "target": 2,
    "temporal_horizon": 1,
}
OWNER_TASKS_PER_SHARD = 5
PRIMARY_PLAN = tuple(
    (field, shard)
    for field in FIELDS
    for shard in range(math.ceil(REMAINING_COUNTS[field] / OWNER_TASKS_PER_SHARD))
)
PRIMARY_TURNS = tuple(
    f"expanded_{field}_primary_{shard:02d}" for field, shard in PRIMARY_PLAN
)
CANARY_TURNS = tuple(f"expanded_{field}_order_canary" for field in FIELDS)
TURN_NAMES = PRIMARY_TURNS + CANARY_TURNS
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    v136.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v137-full-observable-field-owner"
).resolve()


class JudgeV5CalibrationV137Error(RuntimeError):
    """The v137 full observable field-owner contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v136() -> dict[str, Any]:
    root = v136.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "dominant-field-owner-recovery-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "recovered-dominant-field-owner-score.json",
        "primary": root / "recovered-dominant-field-owner-output.private.json",
        "canary": root / "recovered-dominant-field-owner-canary.private.json",
    }
    values = {name: _load_json(path, f"v136 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v136_recovered_field_owner_passed_expansion_authorized"
        or terminal.get("expanded_field_owner_diagnostic_authorized") is not True
        or terminal.get("reference_patch_authorized") is not False
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_attempt_started") is not False
        or terminal.get("usage_status") != "no_new_semantic_usage"
        or terminal.get("usage", {}).get("total_tokens") != 0
        or terminal.get("reused_predecessor_usage", {}).get("total_tokens") != 133874
        or score.get("passed") is not True
        or score.get("failed_checks") != []
        or score.get("metrics", {}).get("matched_control_exact_count") != 6
        or score.get("metrics", {}).get("order_canary_exact_count") != 12
        or score.get("metrics", {}).get("owner_task_count") != 6
        or spec.get("semantic_turn_count") != 0
        or spec.get("semantic_retry_count") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV137Error("v136 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV137Error("v136 runtime record drifted")
    for name, key in {
        "spec": "spec",
        "score": "score",
        "primary": "primary_output",
        "canary": "canary_output",
    }.items():
        record = terminal.get(key)
        if record != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV137Error(f"v136 {name} record drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "v135": v136._validate_v135(),
        "v134": v135._validate_v134(),
    }


def _owner_task_id(case_id: str, witness_id: str, field: str) -> str:
    return "owner_" + sha256_text(f"v137|{case_id}|{witness_id}|{field}")[:24]


def _control_task_id(source_task_id: str, field: str, shard: int) -> str:
    return "control_" + sha256_text(
        f"v137|{source_task_id}|{field}|{shard}"
    )[:24]


def _input_value(field: str, tasks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": V137_INPUT_VERSION,
        "field_focus": field,
        "task_count": len(tasks),
        "tasks": deepcopy(list(tasks)),
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "disagreement_reasons_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }


def _observable_candidates(v134_source: Mapping[str, Any]) -> list[dict[str, Any]]:
    truth = v134_source["values"]["truth"]
    units = {
        str(row["witness_id"]): row
        for row in v134_source["values"]["input"]["units"]
    }
    primary = {
        str(row["witness_id"]): set(row["field_issue_fields"])
        for row in v134_source["values"]["pointwise"]["units"]
    }
    canary = {
        str(row["witness_id"]): set(row["field_issue_fields"])
        for row in v134_source["values"]["canary"]["units"]
    }
    contracts = _field_contracts()
    result = []
    for case_id, case in truth["cases"].items():
        for witness_id, expected_fields in case["field_issues"].items():
            current = set(expected_fields)
            disputed = (current ^ primary[witness_id]) | (
                current ^ canary.get(witness_id, current)
            )
            event = compact_empty_event_fields(
                deepcopy(units[witness_id]["structured_event"])
            )
            for field in sorted(disputed):
                result.append(
                    {
                        "task_id": _owner_task_id(case_id, witness_id, field),
                        "case_id": case_id,
                        "witness_id": witness_id,
                        "field": field,
                        "current_status": "incorrect" if field in current else "correct",
                        "task": {
                            "task_id": _owner_task_id(case_id, witness_id, field),
                            "field": field,
                            "field_contract": deepcopy(contracts[field]),
                            "requested_field_value": _general_requested_field_value(
                                field, event
                            ),
                            "source_excerpt": units[witness_id]["source_excerpt"],
                            "structured_event": event,
                        },
                    }
                )
    counts = {field: sum(row["field"] == field for row in result) for field in FIELDS}
    if counts != OBSERVABLE_COUNTS or len(result) != 48:
        raise JudgeV5CalibrationV137Error("v137 observable dispute coverage drifted")
    return result


def build_v137_inputs(
    *,
    v134_source: Mapping[str, Any],
    v136_source: Mapping[str, Any],
    controls_source: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, Any]]:
    candidates = _observable_candidates(v134_source)
    candidate_by_key = {
        (row["case_id"], row["witness_id"], row["field"]): row for row in candidates
    }
    v135_truth = {
        row["task_id"]: row
        for row in v136_source["v135"]["values"]["truth"]["tasks"]
    }
    v136_primary = {
        row["task_id"]: row for row in v136_source["values"]["primary"]["decisions"]
    }
    inherited = {}
    inherited_keys = set()
    for task_id, row in v135_truth.items():
        if row["role"] != "owner":
            continue
        key = (row["case_id"], row["witness_id"], row["field"])
        candidate = candidate_by_key.get(key)
        decision = v136_primary[task_id]
        if candidate is None or decision["field_status"] == "abstain":
            raise JudgeV5CalibrationV137Error("v136 inherited owner coverage drifted")
        inherited_keys.add(key)
        inherited[candidate["task_id"]] = {
            "task_id": candidate["task_id"],
            "field_status": decision["field_status"],
            "source_evidence_spans": deepcopy(decision["source_evidence_spans"]),
            "rationale": "Inherited from the validated v135 decisive owner.",
        }
    if len(inherited) != 6:
        raise JudgeV5CalibrationV137Error("v137 inherited owner count drifted")

    remaining = [
        row
        for row in candidates
        if (row["case_id"], row["witness_id"], row["field"]) not in inherited_keys
    ]
    remaining_counts = {
        field: sum(row["field"] == field for row in remaining) for field in FIELDS
    }
    if remaining_counts != REMAINING_COUNTS or len(remaining) != 42:
        raise JudgeV5CalibrationV137Error("v137 remaining owner coverage drifted")

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
    truth_rows = []
    first_primary_by_field = {}
    for field, shard in PRIMARY_PLAN:
        field_candidates = sorted(
            [row for row in remaining if row["field"] == field],
            key=lambda row: sha256_text(
                f"v137|owner-order|{field}|{row['case_id']}|{row['witness_id']}"
            ),
        )
        selected = field_candidates[
            shard * OWNER_TASKS_PER_SHARD : (shard + 1) * OWNER_TASKS_PER_SHARD
        ]
        controls = [
            row
            for row in v121_truth
            if row["role"] == "matched_control"
            and row["field"] == field
            and v121_primary[row["task_id"]]["field_status"]
            == row["control_expected_status"]
            and v121_primary[row["task_id"]]["field_status"] != "abstain"
        ]
        controls.sort(key=lambda row: sha256_text(f"v137|control-order|{row['task_id']}"))
        if not selected or shard >= len(controls):
            raise JudgeV5CalibrationV137Error("v137 control or shard coverage drifted")
        control = controls[shard]
        model_tasks = [deepcopy(row["task"]) for row in selected]
        for row in selected:
            truth_rows.append(
                {
                    "task_id": row["task_id"],
                    "role": "owner",
                    "case_id": row["case_id"],
                    "witness_id": row["witness_id"],
                    "field": field,
                    "current_status": row["current_status"],
                }
            )
        control_task = deepcopy(v121_input[control["task_id"]])
        control_task_id = _control_task_id(control["task_id"], field, shard)
        control_task["task_id"] = control_task_id
        model_tasks.append(control_task)
        truth_rows.append(
            {
                "task_id": control_task_id,
                "role": "control",
                "source_task_id": control["task_id"],
                "field": field,
                "control_expected_status": control["control_expected_status"],
            }
        )
        model_tasks.sort(
            key=lambda task: sha256_text(
                f"v137|primary-task-order|{field}|{shard}|{task['task_id']}"
            )
        )
        turn_name = f"expanded_{field}_primary_{shard:02d}"
        value = _input_value(field, model_tasks)
        row = {
            "turn_name": turn_name,
            "turn_role": "primary",
            "field": field,
            "shard": shard,
            "value": value,
        }
        rows.append(row)
        if shard == 0:
            first_primary_by_field[field] = row
    canary_task_ids = []
    for field, turn_name in zip(FIELDS, CANARY_TURNS, strict=True):
        source = first_primary_by_field[field]
        value = _input_value(field, list(reversed(source["value"]["tasks"])))
        canary_task_ids.extend(str(task["task_id"]) for task in value["tasks"])
        rows.append(
            {
                "turn_name": turn_name,
                "turn_role": "order_canary",
                "field": field,
                "shard": 0,
                "value": value,
            }
        )
    rows.sort(key=lambda row: TURN_NAMES.index(row["turn_name"]))
    truth_value = {
        "schema_version": V137_TRUTH_VERSION,
        "observable_owner_task_count": 48,
        "inherited_owner_task_count": 6,
        "new_owner_task_count": 42,
        "matched_control_task_count": len(PRIMARY_PLAN),
        "tasks": truth_rows,
        "inherited_owner_decisions": list(inherited.values()),
        "inherited_owner_tasks": [
            {
                "task_id": row["task_id"],
                "role": "owner",
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "field": row["field"],
                "current_status": row["current_status"],
            }
            for row in candidates
            if row["task_id"] in inherited
        ],
        "canary_task_ids": canary_task_ids,
    }
    selection = {
        "schema_version": V137_SELECTION_VERSION,
        "created_at": now_iso(),
        "observable_disagreement_counts": OBSERVABLE_COUNTS,
        "remaining_disagreement_counts": REMAINING_COUNTS,
        "observable_owner_task_count": 48,
        "inherited_v136_owner_task_count": 6,
        "new_owner_task_count": 42,
        "primary_turn_count": len(PRIMARY_TURNS),
        "order_canary_turn_count": len(CANARY_TURNS),
        "maximum_owner_tasks_per_primary_turn": OWNER_TASKS_PER_SHARD,
        "one_matched_control_per_primary_turn": True,
        "order_canary_identical_membership_and_ids": True,
        "order_canary_only_difference": "reversed_task_array_order",
        "canary_marker_in_model_input": False,
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "semantic_pruning_performed": False,
        "majority_voting_used": False,
        "privacy": "private_source_event_output_aggregate_reports_only",
    }
    return rows, truth_value, selection, inherited


def _decision_map(output: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    decisions = {str(row["task_id"]): deepcopy(row) for row in output["decisions"]}
    if len(decisions) != len(output["decisions"]):
        raise JudgeV5CalibrationV137Error("v137 output task IDs overlap")
    return decisions


def combined_owner_decisions(
    *, primary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    observed = _decision_map(primary)
    inherited = {
        str(row["task_id"]): deepcopy(row)
        for row in truth["inherited_owner_decisions"]
    }
    owner_ids = {
        str(row["task_id"])
        for row in truth["tasks"]
        if row["role"] == "owner"
    }
    owner_ids.update(str(row["task_id"]) for row in truth["inherited_owner_tasks"])
    combined = {task_id: observed[task_id] for task_id in owner_ids if task_id in observed}
    combined.update(inherited)
    if set(combined) != owner_ids or len(combined) != 48:
        raise JudgeV5CalibrationV137Error("v137 combined owner coverage drifted")
    return combined


def score_v137(
    *, primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {str(row["task_id"]): row for row in truth["tasks"]}
    observed = _decision_map(primary)
    repeated = _decision_map(canary)
    canary_ids = set(str(task_id) for task_id in truth["canary_task_ids"])
    if set(observed) != set(expected) or set(repeated) != canary_ids:
        raise JudgeV5CalibrationV137Error("v137 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "control"]
    new_owners = [row for row in expected.values() if row["role"] == "owner"]
    inherited_tasks = {
        str(row["task_id"]): row for row in truth["inherited_owner_tasks"]
    }
    inherited_decisions = {
        str(row["task_id"]): row for row in truth["inherited_owner_decisions"]
    }
    control_exact = sum(
        observed[row["task_id"]]["field_status"]
        == row["control_expected_status"]
        for row in controls
    )
    canary_exact = sum(
        observed[task_id]["field_status"] == repeated[task_id]["field_status"]
        for task_id in canary_ids
    )
    new_owner_abstentions = sum(
        observed[row["task_id"]]["field_status"] == "abstain" for row in new_owners
    )
    inherited_abstentions = sum(
        row["field_status"] == "abstain" for row in inherited_decisions.values()
    )
    all_decisions = (
        list(observed.values())
        + list(repeated.values())
        + list(inherited_decisions.values())
    )
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in all_decisions)
    owner = combined_owner_decisions(primary=primary, truth=truth)
    owner_tasks = {
        **{row["task_id"]: row for row in new_owners},
        **inherited_tasks,
    }
    change_count = sum(
        owner[task_id]["field_status"] != row["current_status"]
        for task_id, row in owner_tasks.items()
    )
    checks = {
        "matched_control_exact_rate": control_exact == len(controls),
        "order_canary_exact_rate": canary_exact == len(canary_ids),
        "new_owner_abstention_count": new_owner_abstentions == 0,
        "inherited_owner_abstention_count": inherited_abstentions == 0,
        "evidence_complete_rate": evidence_complete == len(all_decisions),
        "field_specific_turn_contract": True,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V137_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "observable_owner_task_count": 48,
            "inherited_owner_task_count": len(inherited_tasks),
            "new_owner_task_count": len(new_owners),
            "matched_control_count": len(controls),
            "matched_control_exact_count": control_exact,
            "order_canary_decision_count": len(canary_ids),
            "order_canary_exact_count": canary_exact,
            "new_owner_abstention_count": new_owner_abstentions,
            "inherited_owner_abstention_count": inherited_abstentions,
            "evidence_complete_count": evidence_complete,
            "owner_reference_change_count": change_count,
        },
        "reference_patch_authorized": passed,
        "fresh_pointwise_diagnostic_authorized": passed,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
    }


def build_reference_v137(
    *, current_reference: Mapping[str, Any], primary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    result = deepcopy(current_reference)
    owner = combined_owner_decisions(primary=primary, truth=truth)
    owner_tasks = {
        row["task_id"]: row
        for row in list(truth["tasks"]) + list(truth["inherited_owner_tasks"])
        if row["role"] == "owner"
    }
    changed = 0
    for task_id, task in owner_tasks.items():
        case = result["cases"][task["case_id"]]
        issues = set(case["field_issues"][task["witness_id"]])
        before = task["field"] in issues
        after = owner[task_id]["field_status"] == "incorrect"
        if after:
            issues.add(task["field"])
        else:
            issues.discard(task["field"])
        case["field_issues"][task["witness_id"]] = [
            field for field in v108.CHECKLIST_FIELDS if field in issues
        ]
        case["structured_fields"][task["witness_id"]] = (
            "incorrect" if issues else "correct"
        )
        changed += int(before != after)
    if len(owner_tasks) != 48:
        raise JudgeV5CalibrationV137Error("v137 reference owner coverage drifted")
    result["schema_version"] = V137_REFERENCE_VERSION
    result["reference_version"] = "fixture_reference_v11_observable_field_owner_frozen"
    result["v137_observable_owner_task_count"] = 48
    result["v137_reference_change_count"] = changed
    result["v137_owner_basis"] = "single_decisive_field_specific_sol_owner"
    return result


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V137_CAPACITY_AUDIT_VERSION,
        "phase_id": V137_PHASE_ID,
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
        "schema_version": V137_CAPACITY_POLICY_VERSION,
        "phase_id": V137_PHASE_ID,
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


def freeze_v137(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v137 terminal")}
    v136_source = _validate_v136()
    v134_source = v136_source["v134"]
    controls_source = _validate_v125()
    rows, truth, selection, inherited = build_v137_inputs(
        v134_source=v134_source,
        v136_source=v136_source,
        controls_source=controls_source,
    )
    truth_path = root / "full-observable-field-owner-truth.private.json"
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
    predecessor = {
        **{f"v136_{name}": record for name, record in v136_source["records"].items()},
        **{f"v134_{name}": record for name, record in v134_source["records"].items()},
        "v134_attempts": v134_source["attempts"],
        "v132_reference": v134_source["v133"]["v132"]["records"]["reference"],
        **{
            f"v121_{name}": record
            for name, record in controls_source["v124"]["v122"]["v121"][
                "records"
            ].items()
        },
        "v121_input": controls_source["v124"]["v122"]["v121_input_record"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V137_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "field_specific_sol_owner_for_all_remaining_observable_v134_reference_disputes",
        "observable_owner_task_count": 48,
        "inherited_owner_task_count": 6,
        "new_owner_task_count": 42,
        "primary_turn_count": len(PRIMARY_TURNS),
        "order_canary_turn_count": len(CANARY_TURNS),
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "canary_marker_in_model_input": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_controls_all_repeated_decisions_no_abstentions_complete_exact_evidence_and_full_48_owner_coverage",
        "reference_patch_authorized": False,
        "fresh_pointwise_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v136_dominant_field_owner_recovery.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v135_dominant_field_owner_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v134_gpt55_singleton_pointwise.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v126_singleton_field_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v115_full_contested_field_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_fixture.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "truth": _record(truth_path),
            "selection": _record(selection_path),
            "inherited_owner_decisions_sha256": sha256_text(
                json.dumps(inherited, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            ),
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "role": turn["turn_role"],
                    "field": turn["field"],
                    "shard": turn["shard"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "full-observable-field-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v134": v134_source,
        "v136": v136_source,
        "current_reference": v134_source["v133"]["v132"]["values"]["reference"],
    }


def _merge_turn_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    task_ids = [str(row["task_id"]) for row in decisions]
    if len(task_ids) != len(set(task_ids)):
        raise JudgeV5CalibrationV137Error("v137 turn outputs overlap")
    return {"schema_version": V137_OUTPUT_VERSION, "decisions": decisions}


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
                _load_json(Path(record["path"]), "v137 sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V137_FAILURE_VERSION,
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
        "schema_version": V137_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_patch_authorized": False,
        "fresh_pointwise_diagnostic_authorized": False,
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


async def run_v137(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v137 terminal")
    frozen = freeze_v137(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        primary_outputs, canary_outputs, sidecars = [], [], []
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
                    batch_size=len(turn["value"]["tasks"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_output(
                        candidate, item
                    ),
                )
                sidecars.append(sidecar)
                target = (
                    primary_outputs
                    if turn["turn_role"] == "primary"
                    else canary_outputs
                )
                target.append(output)
        primary = _merge_turn_outputs(primary_outputs)
        canary = _merge_turn_outputs(canary_outputs)
        score = score_v137(primary=primary, canary=canary, truth=frozen["truth"])
        owner = {
            "schema_version": V137_OUTPUT_VERSION,
            "decisions": list(
                combined_owner_decisions(primary=primary, truth=frozen["truth"]).values()
            ),
        }
        paths = {
            "primary": root / "full-observable-field-owner-output.private.json",
            "canary": root / "full-observable-field-owner-canary.private.json",
            "owner": root / "combined-observable-field-owner.private.json",
            "score": root / "full-observable-field-owner-score.json",
            "reference": root / "calibration-truth-v11-observable-field-owner.private.json",
        }
        _write_immutable(paths["primary"], primary)
        _write_immutable(paths["canary"], canary)
        _write_immutable(paths["owner"], owner)
        _write_immutable(paths["score"], score)
        if score["passed"]:
            reference = build_reference_v137(
                current_reference=frozen["current_reference"],
                primary=primary,
                truth=frozen["truth"],
            )
            _write_immutable(paths["reference"], reference)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V137_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v137_full_observable_field_owner_passed_reference_patch_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v137_observable_field_reference_patch_authorized"
                if passed
                else "v137_full_observable_field_owner_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reference_patch_authorized": passed,
            "reference_frozen": passed,
            "fresh_pointwise_diagnostic_authorized": passed,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(paths["score"]),
            "primary_output": _record(paths["primary"]),
            "canary_output": _record(paths["canary"]),
            "combined_owner_output": _record(paths["owner"]),
            "reference": _record(paths["reference"]) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v135_usage": frozen["v136"]["v135"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v137 full observable field owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v137(
            output_dir=Path(args.output_dir),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_patch_authorized": terminal.get(
                    "reference_patch_authorized", False
                ),
                "fresh_pointwise_diagnostic_authorized": terminal.get(
                    "fresh_pointwise_diagnostic_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
