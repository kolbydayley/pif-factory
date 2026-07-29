from __future__ import annotations

"""Terra owner pass over the 95 nonunanimous retained-case field decisions."""

import argparse
import asyncio
import json
import math
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

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
    _merge_outputs,
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
from .app_server_judge_v5_calibration_v106_full_development import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V106_ROOT,
)
from .app_server_judge_v5_calibration_v107_recovery_receipt import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V107_ROOT,
)
from .app_server_judge_v5_calibration_v115_full_contested_field_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V115_ROOT,
    base_instructions_v115,
    build_prompt_v115,
)
from .app_server_judge_v5_calibration_v118_capped_alignment_repair import _iter_records
from .app_server_judge_v5_calibration_v120_retained_case_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V120_ROOT,
    _validate_v119,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .util import now_iso, sha256_text


V121_INPUT_VERSION = "pif_app_server_judge_v5_4_v121_retained_field_owner_input_v1"
V121_TRUTH_VERSION = "pif_app_server_judge_v5_4_v121_retained_field_owner_truth_v1"
V121_SELECTION_VERSION = "pif_app_server_judge_v5_4_v121_selection_v1"
V121_SPEC_VERSION = "pif_app_server_judge_v5_4_v121_spec_v1"
V121_SCORE_VERSION = "pif_app_server_judge_v5_4_v121_score_v1"
V121_REFERENCE_VERSION = "pif_app_server_judge_v5_4_calibration_truth_v10_retained_field_candidate"
V121_FAILURE_VERSION = "pif_app_server_judge_v5_4_v121_failure_v1"
V121_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v121_terminal_v1"
V121_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V121_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V121_PHASE_ID = "judge_v5_4_v121_retained_field_owner"

MODEL = "gpt-5.6-terra"
EFFORT = "high"
TASKS_PER_TURN = 24
PRIMARY_TURNS = tuple(f"retained_field_owner_shard_{index:02d}" for index in range(5))
CANARY_TURN = "retained_field_owner_canary"
TURN_NAMES = PRIMARY_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V120_ROOT.parent / "judge-calibration-v5_4-v121-retained-field-owner"
).resolve()


class JudgeV5CalibrationV121Error(RuntimeError):
    """The v121 retained field-owner contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v120() -> dict[str, Any]:
    root = DEFAULT_V120_ROOT
    paths = {
        "spec": root / "retained-diagnostic-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "retained-diagnostic-score.json",
        "truth": root / "retained-diagnostic-truth.private.json",
        "pointwise_input": root / "pointwise-input-full.private.json",
        "checklist": root / "pointwise-checklist-full.private.json",
        "pointwise": root / "pointwise-output-full.private.json",
        "alignment": root / "reconciled-alignment.private.json",
        "disagreements": root / "observable-disagreements.private.json",
    }
    values = {name: _load_json(path, f"v120 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v120_retained_case_diagnostic_quality_gate_not_passed"
        or terminal.get("diagnostic_passed") is not False
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 363090
        or len(terminal.get("attempts") or []) != 9
        or score.get("passed") is not False
        or score.get("metrics", {}).get("case_count") != 18
        or score.get("metrics", {}).get("witness_count") != 52
        or score.get("metrics", {}).get("structured_field_accuracy") != 0.730769
        or score.get("metrics", {}).get("pointwise_field_issue_f1") != 0.579882
        or spec.get("pointwise_model") != "gpt-5.6-luna"
        or spec.get("alignment_model") != "gpt-5.6-terra"
        or spec.get("adjudicator_model") != "gpt-5.4"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV121Error("v120 predecessor contract drifted")
    if not all(_verify_record(record) for record in _iter_records(spec)):
        raise JudgeV5CalibrationV121Error("v120 frozen record drifted")
    usage = {field: 0 for field in USAGE_FIELDS}
    attempt_records = {}
    for attempt in terminal["attempts"]:
        turn_name = str(attempt["turn_name"])
        for key in ("capacity", "sidecar", "output"):
            if not isinstance(attempt.get(key), Mapping) or not _verify_record(attempt[key]):
                raise JudgeV5CalibrationV121Error("v120 attempt record drifted")
        measured = _validate_usage(_load_json(Path(attempt["sidecar"]["path"]), "v120 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempt_records[turn_name] = attempt
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV121Error("v120 usage aggregate drifted")
    checklist_errors = v108_validate_checklist(values["checklist"], values["pointwise_input"])
    if checklist_errors:
        raise JudgeV5CalibrationV121Error("v120 pointwise checklist drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempt_records": attempt_records,
        "usage": usage,
    }


def v108_validate_checklist(output: Mapping[str, Any], source: Mapping[str, Any]) -> list[str]:
    from .app_server_judge_v5_calibration_v108_layered_diagnostic import (
        validate_pointwise_checklist_output,
    )

    return validate_pointwise_checklist_output(output, source)


def _validate_v106() -> dict[str, Any]:
    root = DEFAULT_V106_ROOT
    recovery_root = DEFAULT_V107_ROOT
    paths = {
        "spec": root / "full-calibration-spec.json",
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "pointwise": root / "pointwise-output-full.private.json",
        "recovery_terminal": recovery_root / "terminal.json",
        "recovery_receipt": recovery_root / "recovery-receipt.json",
    }
    values = {name: _load_json(path, f"v106 {name}") for name, path in paths.items()}
    terminal, failure, spec, recovery = (
        values["terminal"],
        values["failure"],
        values["spec"],
        values["recovery_terminal"],
    )
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 897380
        or terminal.get("production_mutated") is not False
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != "disagreement_adjudication"
        or failure.get("error_class") != "KeyError"
        or failure.get("usage_status") != "complete"
        or failure.get("unknown_usage_turn_count") != 0
        or len(failure.get("attempts") or []) != 25
        or spec.get("primary_model") != "gpt-5.5"
        or spec.get("retry_count_per_turn") != 0
        or recovery.get("state") != "inactive"
        or recovery.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or recovery.get("fresh_full_calibration_authorized") is not False
        or recovery.get("selection_authorized") is not False
        or recovery.get("holdout_authorized") is not False
        or recovery.get("production_mutated") is not False
    ):
        raise JudgeV5CalibrationV121Error("v106 independent evidence drifted")
    usage = {field: 0 for field in USAGE_FIELDS}
    pointwise_units = []
    attempt_records = {}
    for attempt in failure["attempts"]:
        turn_name = str(attempt["turn_name"])
        for key in ("capacity", "sidecar", "output"):
            if not isinstance(attempt.get(key), Mapping) or not _verify_record(attempt[key]):
                raise JudgeV5CalibrationV121Error("v106 attempt record drifted")
        measured = _validate_usage(_load_json(Path(attempt["sidecar"]["path"]), "v106 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempt_records[turn_name] = attempt
        if turn_name.startswith("pointwise_support_shard_"):
            pointwise_units.extend(
                _load_json(Path(attempt["output"]["path"]), "v106 pointwise shard")["units"]
            )
    if usage != terminal["usage"] or len(pointwise_units) != 182:
        raise JudgeV5CalibrationV121Error("v106 usage or pointwise coverage drifted")
    merged = {"units": pointwise_units}
    if merged != values["pointwise"]:
        raise JudgeV5CalibrationV121Error("v106 aggregate pointwise output drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempt_records": attempt_records,
        "usage": usage,
    }


def _task_id(case_id: str, witness_id: str, field: str, role: str) -> str:
    return "field_" + sha256_text(f"v121|{role}|{case_id}|{witness_id}|{field}")[:24]


def _canary_id(owner_task_id: str) -> str:
    return "perm_" + sha256_text(f"v121|canary|{owner_task_id}")[:24]


def _status_map(pointwise: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    result = {}
    for row in pointwise["units"]:
        issues = set(row["field_issue_fields"])
        result[str(row["witness_id"])] = {
            field: "incorrect" if field in issues else "correct"
            for field in CHECKLIST_FIELDS
        }
    return result


def _select_canaries(rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    chosen = []
    used_witnesses = set()
    for role in ("consensus_reference_dispute", "model_disagreement"):
        candidates = sorted(
            [row for row in rows if row["role"] == role],
            key=lambda row: sha256_text(
                f"v121-canary|{role}|{row['field']}|{row['witness_id']}"
            ),
        )
        role_chosen = []
        used_fields = set()
        for row in candidates:
            if row["witness_id"] in used_witnesses or row["field"] in used_fields:
                continue
            role_chosen.append(row)
            used_witnesses.add(row["witness_id"])
            used_fields.add(row["field"])
            if len(role_chosen) == 6:
                break
        if len(role_chosen) != 6:
            raise JudgeV5CalibrationV121Error("v121 canary diversity coverage drifted")
        chosen.extend(role_chosen)
    return chosen


def build_v121_inputs(
    v119: Mapping[str, Any], v120: Mapping[str, Any], v106: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    truth = v120["values"]["truth"]
    units = {
        str(row["witness_id"]): row for row in v120["values"]["pointwise_input"]["units"]
    }
    luna = _status_map(v120["values"]["pointwise"])
    gpt = _status_map(v106["values"]["pointwise"])
    contracts = _field_contracts()
    v115_tasks = _load_json(
        DEFAULT_V115_ROOT / "contested-field-input.private.json", "v115 contracts"
    )["tasks"]
    for row in v115_tasks:
        contracts[row["field"]] = deepcopy(row["field_contract"])

    contested = []
    control_pool = []
    for case_id, case in truth["cases"].items():
        for witness_id in case["proposition"]:
            event = compact_empty_event_fields(deepcopy(units[witness_id]["structured_event"]))
            current_issues = set(case["field_issues"][witness_id])
            for field in CHECKLIST_FIELDS:
                current_status = "incorrect" if field in current_issues else "correct"
                row = {
                    "case_id": case_id,
                    "witness_id": witness_id,
                    "field": field,
                    "current_status": current_status,
                    "luna_status": luna[witness_id][field],
                    "gpt55_status": gpt[witness_id][field],
                    "proposition_status": case["proposition"][witness_id],
                    "event": event,
                    "source_excerpt": units[witness_id]["source_excerpt"],
                }
                if current_status == row["luna_status"] == row["gpt55_status"]:
                    control_pool.append(row)
                else:
                    row["role"] = (
                        "consensus_reference_dispute"
                        if row["luna_status"] == row["gpt55_status"]
                        else "model_disagreement"
                    )
                    contested.append(row)
    role_counts = Counter(row["role"] for row in contested)
    field_counts = Counter(row["field"] for row in contested)
    if len(contested) != 95 or role_counts != {
        "consensus_reference_dispute": 30,
        "model_disagreement": 65,
    }:
        raise JudgeV5CalibrationV121Error("v121 contested field coverage drifted")

    controls = []
    for field in CHECKLIST_FIELDS:
        candidates = sorted(
            [row for row in control_pool if row["field"] == field],
            key=lambda row: (
                row["current_status"] != "incorrect",
                sha256_text(f"v121-control|{field}|{row['witness_id']}"),
            ),
        )
        controls.append(candidates[0])
    extra_fields = [field for field, _ in field_counts.most_common(5)]
    used = {(row["witness_id"], row["field"]) for row in controls}
    for field in extra_fields:
        candidate = min(
            (
                row
                for row in control_pool
                if row["field"] == field
                and (row["witness_id"], row["field"]) not in used
                and row["current_status"] == "correct"
            ),
            key=lambda row: sha256_text(f"v121-extra-control|{field}|{row['witness_id']}"),
        )
        controls.append(candidate)
        used.add((candidate["witness_id"], candidate["field"]))
    if len(controls) != 20 or len(used) != 20:
        raise JudgeV5CalibrationV121Error("v121 matched control coverage drifted")

    def task(row: Mapping[str, Any], role: str) -> dict[str, Any]:
        return {
            "task_id": _task_id(row["case_id"], row["witness_id"], row["field"], role),
            "field": row["field"],
            "field_contract": deepcopy(contracts[row["field"]]),
            "requested_field_value": _general_requested_field_value(row["field"], row["event"]),
            "source_excerpt": row["source_excerpt"],
            "structured_event": deepcopy(row["event"]),
        }

    task_rows = []
    truth_rows = []
    for row in contested + controls:
        role = row.get("role") or "matched_control"
        item = task(row, role)
        task_rows.append(item)
        truth_rows.append(
            {
                "task_id": item["task_id"],
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "field": row["field"],
                "role": role,
                "current_status": row["current_status"],
                "luna_status": row["luna_status"],
                "gpt55_status": row["gpt55_status"],
                "control_expected_status": row["current_status"] if role == "matched_control" else None,
                "proposition_status": row["proposition_status"],
            }
        )
    task_rows.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    task_by_id = {row["task_id"]: row for row in task_rows}
    canary_map = []
    canary_tasks = []
    for row in reversed(_select_canaries(truth_rows)):
        canary_id = _canary_id(row["task_id"])
        item = deepcopy(task_by_id[row["task_id"]])
        item["task_id"] = canary_id
        canary_tasks.append(item)
        canary_map.append({"owner_task_id": row["task_id"], "canary_task_id": canary_id})
    value = {
        "schema_version": V121_INPUT_VERSION,
        "task_count": 115,
        "tasks": task_rows,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V121_TRUTH_VERSION,
        "task_count": 115,
        "contested_task_count": 95,
        "matched_control_count": 20,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V121_INPUT_VERSION,
        "task_count": 12,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V121_SELECTION_VERSION,
        "created_at": now_iso(),
        "source_field_decision_count": 780,
        "unanimous_decision_count": 685,
        "contested_task_count": 95,
        "contested_role_counts": dict(sorted(role_counts.items())),
        "contested_field_counts": dict(sorted(field_counts.items())),
        "matched_control_count": 20,
        "permutation_canary_count": 12,
        "selection_uses_source_text": False,
        "selection_uses_only_prior_llm_decisions_reference_status_field_enums_and_opaque_ids": True,
        "semantic_pruning_performed": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "privacy": "private_source_event_output_aggregate_reports_only",
    }
    return value, truth_value, canary, selection


def _shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value["tasks"]
    result = []
    for index in range(0, len(tasks), TASKS_PER_TURN):
        result.append(
            {
                **{key: deepcopy(child) for key, child in value.items() if key != "tasks"},
                "task_count": len(tasks[index : index + TASKS_PER_TURN]),
                "tasks": deepcopy(tasks[index : index + TASKS_PER_TURN]),
                "shard_ordinal": index // TASKS_PER_TURN,
                "shard_count": math.ceil(len(tasks) / TASKS_PER_TURN),
            }
        )
    if len(result) != 5:
        raise JudgeV5CalibrationV121Error("v121 primary shard coverage drifted")
    return result


def score_v121(
    primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {str(row["task_id"]): row for row in truth["tasks"]}
    observed = {str(row["task_id"]): row for row in primary["decisions"]}
    canary_rows = {str(row["task_id"]): row for row in canary["decisions"]}
    if set(observed) != set(expected):
        raise JudgeV5CalibrationV121Error("v121 primary score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "matched_control"]
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    abstentions = sum(row["field_status"] == "abstain" for row in observed.values())
    canary_exact = 0
    triggers: dict[str, set[str]] = defaultdict(set)
    for mapping in truth["canary_map"]:
        owner_id, canary_id = mapping["owner_task_id"], mapping["canary_task_id"]
        if observed[owner_id]["field_status"] == canary_rows[canary_id]["field_status"]:
            canary_exact += 1
        else:
            triggers[owner_id].add("canary_disagreement")
    for row in controls:
        if observed[row["task_id"]]["field_status"] != row["control_expected_status"]:
            triggers[row["task_id"]].add("matched_control_mismatch")
    for task_id, row in observed.items():
        if row["field_status"] == "abstain":
            triggers[task_id].add("primary_abstention")
    unsupported_conflicts = 0
    for task_id, row in expected.items():
        if row["field"] != "unsupported_inference":
            continue
        wanted = {"supported": "correct", "unsupported": "incorrect", "abstain": "abstain"}[
            row["proposition_status"]
        ]
        if observed[task_id]["field_status"] != wanted:
            unsupported_conflicts += 1
            triggers[task_id].add("unsupported_consistency")
    checks = {
        "matched_control_exact_rate": control_exact == 20,
        "primary_abstention_count": abstentions == 0,
        "permutation_canary_exact_rate": canary_exact == 12,
        "unsupported_inference_consistency": unsupported_conflicts == 0,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V121_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": 115,
            "contested_task_count": 95,
            "matched_control_count": 20,
            "matched_control_exact_count": control_exact,
            "primary_abstention_count": abstentions,
            "permutation_canary_count": 12,
            "permutation_canary_exact_count": canary_exact,
            "unsupported_inference_conflict_count": unsupported_conflicts,
            "observable_repair_trigger_count": len(triggers),
        },
        "observable_repair_triggers": [
            {"task_id": task_id, "reasons": sorted(reasons)}
            for task_id, reasons in sorted(triggers.items())
        ],
        "retained_field_reference_patch_authorized": passed,
        "capped_repair_authorized": not passed and 0 < len(triggers) <= 12,
        "proposition_reference_frozen": False,
        "alignment_reference_frozen": False,
        "fresh_diagnostic_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "majority_voting_used": False,
    }


def build_reference_candidate(
    current_reference: Mapping[str, Any], truth: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    candidate = deepcopy(current_reference)
    observed = {str(row["task_id"]): row for row in output["decisions"]}
    changed = 0
    for row in truth["tasks"]:
        if row["role"] == "matched_control":
            continue
        case = candidate["cases"][row["case_id"]]
        issues = set(case["field_issues"][row["witness_id"]])
        before = row["field"] in issues
        after = observed[row["task_id"]]["field_status"] == "incorrect"
        if after:
            issues.add(row["field"])
        else:
            issues.discard(row["field"])
        case["field_issues"][row["witness_id"]] = sorted(issues)
        case["structured_fields"][row["witness_id"]] = "incorrect" if issues else "correct"
        changed += int(before != after)
    candidate["schema_version"] = V121_REFERENCE_VERSION
    candidate["reference_version"] = "fixture_reference_v10_retained_field_owner_candidate"
    candidate["retained_field_owner_change_count"] = changed
    candidate["retained_field_reference_patch_authorized"] = True
    candidate["proposition_reference_frozen"] = False
    candidate["alignment_reference_frozen"] = False
    candidate["fresh_diagnostic_authorized"] = False
    return candidate


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V121_CAPACITY_AUDIT_VERSION,
        "phase_id": V121_PHASE_ID,
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
        "schema_version": V121_CAPACITY_POLICY_VERSION,
        "phase_id": V121_PHASE_ID,
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
        "projected_phase_quota_points": math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v121(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v121 terminal")}
    v119, v120, v106 = _validate_v119(), _validate_v120(), _validate_v106()
    value, truth, canary, selection = build_v121_inputs(v119, v120, v106)
    input_path = root / "retained-field-input.private.json"
    truth_path = root / "retained-field-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_time(selection_path, selection, "created_at")
    primary_shards = _shards(value)
    turns = []
    for turn_name, shard in zip(PRIMARY_TURNS, primary_shards, strict=True):
        prompt, schema = build_prompt_v115(shard), output_schema(shard)
        paths = _freeze_turn_request(root=root, turn_name=turn_name, input_value=shard, prompt=prompt, schema=schema)
        turns.append({"turn_name": turn_name, "value": shard, "prompt": prompt, "schema": schema, "paths": paths})
    canary_prompt, canary_schema = build_prompt_v115(canary), output_schema(canary)
    canary_paths = _freeze_turn_request(
        root=root,
        turn_name=CANARY_TURN,
        input_value=canary,
        prompt=canary_prompt,
        schema=canary_schema,
    )
    turns.append({"turn_name": CANARY_TURN, "value": canary, "prompt": canary_prompt, "schema": canary_schema, "paths": canary_paths})
    predecessor = {
        **{f"v119_{name}": record for name, record in v119["records"].items()},
        **{f"v120_{name}": record for name, record in v120["records"].items()},
        "v120_attempts": v120["attempt_records"],
        **{f"v106_{name}": record for name, record in v106["records"].items()},
        "v106_attempts": v106["attempt_records"],
        "v115_contracts": _record(DEFAULT_V115_ROOT / "contested-field-input.private.json"),
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V121_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "terra_side_free_owner_for_95_nonunanimous_retained_fields_plus_20_controls_and_12_canaries",
        "task_count": 115,
        "contested_task_count": 95,
        "matched_control_count": 20,
        "permutation_canary_count": 12,
        "tasks_per_turn": TASKS_PER_TURN,
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "promotion_rule": "twenty_exact_controls_zero_abstentions_twelve_exact_canaries_and_zero_unsupported_conflicts",
        "retained_field_reference_patch_authorized": False,
        "proposition_reference_frozen": False,
        "alignment_reference_frozen": False,
        "fresh_diagnostic_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v120_retained_case_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v115_full_contested_field_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "truth": _record(truth_path),
            "canary": _record(canary_path),
            "selection": _record(selection_path),
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_outputs_no_source_text_in_reports",
    }
    spec_path = root / "retained-field-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v119": v119,
    }


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v121 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V121_FAILURE_VERSION,
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
        "schema_version": V121_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "retained_field_reference_patch_authorized": False,
        "fresh_diagnostic_authorized": False,
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


async def run_v121(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v121 terminal")
    frozen = freeze_v121(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        outputs, sidecars = [], []
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
                    batch_size=turn["value"]["task_count"],
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_output(candidate, item),
                )
                outputs.append(output)
                sidecars.append(sidecar)
        primary = _merge_outputs(outputs[:-1], "decisions")
        canary = outputs[-1]
        primary_path = root / "retained-field-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(primary_path, primary)
        _write_immutable(canary_path, canary)
        score = score_v121(primary, canary, frozen["truth"])
        score_path = root / "retained-field-owner-score.json"
        _write_immutable(score_path, score)
        reference_path = root / "calibration-truth-v10-retained-field-candidate.private.json"
        if score["passed"]:
            reference = build_reference_candidate(
                frozen["v119"]["values"]["truth"], frozen["truth"], primary
            )
            _write_immutable(reference_path, reference)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V121_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v121_retained_field_owner_passed_patch_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v121_retained_field_reference_patch_authorized"
                if passed
                else "v121_retained_field_owner_repair_or_recovery_required"
            ),
            "overall_evaluation_complete": False,
            "retained_field_reference_patch_authorized": passed,
            "capped_repair_authorized": score["capped_repair_authorized"],
            "proposition_reference_frozen": False,
            "alignment_reference_frozen": False,
            "fresh_diagnostic_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "reference_candidate": _record(reference_path) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v120_usage": frozen["spec"]["predecessor"]["v120_terminal"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v121 retained field owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v121(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "retained_field_reference_patch_authorized": terminal.get(
                    "retained_field_reference_patch_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
