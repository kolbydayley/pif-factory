from __future__ import annotations

"""GPT-5.4 owner for corrected proposition-only truth migration in the retained cohort."""

import argparse
import asyncio
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

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
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import _record, _verify_record
from .app_server_judge_v5_calibration_v117_alignment_reference_owner import _validate_v116
from .app_server_judge_v5_calibration_v121_retained_field_owner import _validate_v106, _validate_v120
from .app_server_judge_v5_calibration_v127_singleton_proposition_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V127_ROOT,
    _validate_v126,
    proposition_instructions,
    proposition_output_schema,
    proposition_prompt,
    validate_proposition_output,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V128_INPUT_VERSION = "pif_app_server_judge_v5_4_v128_proposition_migration_input_v1"
V128_TRUTH_VERSION = "pif_app_server_judge_v5_4_v128_proposition_migration_truth_v1"
V128_SELECTION_VERSION = "pif_app_server_judge_v5_4_v128_selection_v1"
V128_SPEC_VERSION = "pif_app_server_judge_v5_4_v128_spec_v1"
V128_SCORE_VERSION = "pif_app_server_judge_v5_4_v128_score_v1"
V128_REFERENCE_VERSION = (
    "pif_app_server_judge_v5_4_calibration_truth_v10_retained_proposition_migration_frozen"
)
V128_FAILURE_VERSION = "pif_app_server_judge_v5_4_v128_failure_v1"
V128_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v128_terminal_v1"
V128_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V128_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V128_PHASE_ID = "judge_v5_4_v128_proposition_migration_owner"

MODEL = "gpt-5.4"
EFFORT = "high"
PRIMARY_TURN = "proposition_migration_owner"
CANARY_TURN = "proposition_migration_owner_canary"
TURN_NAMES = (PRIMARY_TURN, CANARY_TURN)
TARGET_COUNT = 6
CONTROL_COUNT = 4
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V127_ROOT.parent / "judge-calibration-v5_4-v128-proposition-migration-owner"
).resolve()


class JudgeV5CalibrationV128Error(RuntimeError):
    """The v128 proposition migration contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v127() -> dict[str, Any]:
    root = DEFAULT_V127_ROOT
    paths = {
        "spec": root / "singleton-proposition-owner-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "singleton-proposition-score.json",
        "outputs": root / "singleton-proposition-outputs.private.json",
        "truth": root / "singleton-proposition-truth.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v127 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v127_singleton_proposition_owner_quality_gate_not_passed"
        or terminal.get("retained_proposition_reference_patch_authorized") is not False
        or terminal.get("proposition_reference_frozen") is not False
        or terminal.get("alignment_reference_frozen") is not False
        or terminal.get("fresh_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 125097
        or score.get("passed") is not False
        or score.get("failed_checks") != ["singleton_control_exact_rate"]
        or score.get("metrics", {}).get("singleton_control_exact_count") != 2
        or score.get("metrics", {}).get("singleton_owner_abstention_count") != 0
        or spec.get("model") != "gpt-5.6-sol"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV128Error("v127 predecessor contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5CalibrationV128Error("v127 runtime record drifted")
    for name, key in {"score": "score", "outputs": "outputs"}.items():
        record = terminal.get(key)
        if not isinstance(record, Mapping) or dict(record) != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV128Error(f"v127 {name} record drifted")
    attempts = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_name in spec["turn_plan"]:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        records = {}
        for name, filename in {
            "capacity": "capacity.json",
            "sidecar": "sidecar.json",
            "output": "output.private.json",
        }.items():
            path = turn_root / filename
            if not path.is_file():
                raise JudgeV5CalibrationV128Error("v127 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(_load_json(Path(records["sidecar"]["path"]), "v127 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV128Error("v127 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "v126": _validate_v126(),
    }


def _status_map(pointwise: Mapping[str, Any]) -> dict[str, str]:
    return {str(row["witness_id"]): str(row["proposition_verdict"]) for row in pointwise["units"]}


def _task_id(role: str, witness_id: str) -> str:
    return role + "_" + sha256_text(f"v128|{role}|{witness_id}")[:24]


def _canary_id(owner_task_id: str) -> str:
    return "canary_" + sha256_text(f"v128|canary|{owner_task_id}")[:24]


def _task(unit: Mapping[str, Any], task_id: str) -> dict[str, Any]:
    claim_text = unit["structured_event"].get("claim_text")
    if not isinstance(claim_text, str) or not claim_text:
        raise JudgeV5CalibrationV128Error("v128 proposition text is missing")
    return {
        "task_id": task_id,
        "source_excerpt": unit["source_excerpt"],
        "proposition_text": claim_text,
    }


def build_v128_inputs(
    v127: Mapping[str, Any], v120: Mapping[str, Any], v106: Mapping[str, Any], v116: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    reference = v127["v126"]["values"]["reference"]
    retained_cases = set(v120["values"]["truth"]["cases"])
    full_input_record = v106["values"]["spec"]["frozen_inputs"]["pointwise_full"]
    if not isinstance(full_input_record, Mapping) or not _verify_record(full_input_record):
        raise JudgeV5CalibrationV128Error("v106 pointwise input record drifted")
    full_input = _load_json(Path(full_input_record["path"]), "v106 pointwise input")
    units = {str(row["witness_id"]): row for row in full_input["units"]}
    luna = _status_map(v120["values"]["pointwise"])
    gpt55 = _status_map(v106["values"]["pointwise"])

    retained = []
    for case_id in sorted(retained_cases):
        for witness_id, current in reference["cases"][case_id]["proposition"].items():
            statuses = (current, luna[witness_id], gpt55[witness_id])
            if len(set(statuses)) > 1:
                role = (
                    "consensus_reference_dispute"
                    if statuses[1] == statuses[2] != statuses[0]
                    else "model_disagreement"
                )
            elif current == "unsupported":
                role = "legacy_unsupported_definition_migration"
            else:
                continue
            retained.append(
                {
                    "case_id": case_id,
                    "witness_id": witness_id,
                    "migration_role": role,
                }
            )
    if len(retained) != TARGET_COUNT or Counter(row["migration_role"] for row in retained) != {
        "legacy_unsupported_definition_migration": 3,
        "model_disagreement": 2,
        "consensus_reference_dispute": 1,
    }:
        raise JudgeV5CalibrationV128Error("v128 migration target coverage drifted")

    audited_reference = v116["values"]["reference"]
    controls = []
    for status in ("supported", "unsupported"):
        candidates = []
        for case_id, case in audited_reference["cases"].items():
            for witness_id, observed in case["proposition"].items():
                if observed == status:
                    candidates.append({"case_id": case_id, "witness_id": witness_id, "status": status})
        candidates.sort(key=lambda row: sha256_text(f"v128|control|{status}|{row['witness_id']}"))
        used_cases = {row["case_id"] for row in controls}
        for row in candidates:
            if row["case_id"] in used_cases:
                continue
            controls.append(row)
            used_cases.add(row["case_id"])
            if sum(item["status"] == status for item in controls) == 2:
                break
    if len(controls) != CONTROL_COUNT or Counter(row["status"] for row in controls) != {
        "supported": 2,
        "unsupported": 2,
    }:
        raise JudgeV5CalibrationV128Error("v128 audited control coverage drifted")

    tasks, truth_rows = [], []
    for row in retained:
        task_id = _task_id("owner", row["witness_id"])
        tasks.append(_task(units[row["witness_id"]], task_id))
        truth_rows.append(
            {
                "task_id": task_id,
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "role": "migration_owner",
                "migration_role": row["migration_role"],
            }
        )
    for row in controls:
        task_id = _task_id("control", row["witness_id"])
        tasks.append(_task(units[row["witness_id"]], task_id))
        truth_rows.append(
            {
                "task_id": task_id,
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "role": "audited_control",
                "control_expected_status": row["status"],
            }
        )
    tasks.sort(key=lambda row: sha256_text(f"v128|primary-order|{row['task_id']}"))
    truth_rows.sort(key=lambda row: row["task_id"])
    owner_tasks = {
        row["task_id"]: row
        for row in tasks
        if any(item["task_id"] == row["task_id"] and item["role"] == "migration_owner" for item in truth_rows)
    }
    canary_tasks, canary_map = [], []
    for owner_id in sorted(owner_tasks, key=lambda value: sha256_text(f"v128|canary-order|{value}")):
        canary_id = _canary_id(owner_id)
        task = deepcopy(owner_tasks[owner_id])
        task["task_id"] = canary_id
        canary_tasks.append(task)
        canary_map.append({"owner_task_id": owner_id, "canary_task_id": canary_id})
    value = {
        "schema_version": V128_INPUT_VERSION,
        "task_count": 10,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    canary = {
        "schema_version": V128_INPUT_VERSION,
        "task_count": TARGET_COUNT,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    truth = {
        "schema_version": V128_TRUTH_VERSION,
        "task_count": 10,
        "migration_owner_count": TARGET_COUNT,
        "audited_control_count": CONTROL_COUNT,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    selection = {
        "schema_version": V128_SELECTION_VERSION,
        "created_at": now_iso(),
        "migration_owner_count": TARGET_COUNT,
        "migration_role_counts": dict(sorted(Counter(row["migration_role"] for row in retained).items())),
        "audited_control_count": CONTROL_COUNT,
        "audited_control_status_counts": {"supported": 2, "unsupported": 2},
        "permutation_canary_count": TARGET_COUNT,
        "corrected_proposition_only_rubric": True,
        "legacy_unsupported_labels_treated_as_migration_targets_not_controls": True,
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "migration_roles_in_model_input": False,
        "owner_is_decisive": True,
        "majority_voting_used": False,
        "privacy": "aggregate_counts_only",
    }
    return value, truth, canary, selection


def score_v128(
    primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in primary["decisions"]}
    repeated = {row["task_id"]: row for row in canary["decisions"]}
    if set(expected) != set(observed) or len(repeated) != TARGET_COUNT:
        raise JudgeV5CalibrationV128Error("v128 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "audited_control"]
    owners = [row for row in expected.values() if row["role"] == "migration_owner"]
    control_exact = sum(
        observed[row["task_id"]]["proposition_status"] == row["control_expected_status"]
        for row in controls
    )
    owner_abstentions = sum(
        observed[row["task_id"]]["proposition_status"] == "abstain" for row in owners
    )
    canary_exact = sum(
        observed[row["owner_task_id"]]["proposition_status"]
        == repeated[row["canary_task_id"]]["proposition_status"]
        for row in truth["canary_map"]
    )
    canary_abstentions = sum(row["proposition_status"] == "abstain" for row in repeated.values())
    evidence_complete = sum(
        bool(row["source_evidence_spans"])
        for row in list(observed.values()) + list(repeated.values())
    )
    checks = {
        "audited_control_exact_rate": control_exact == CONTROL_COUNT,
        "migration_owner_abstention_count": owner_abstentions == 0,
        "permutation_canary_exact_rate": canary_exact == TARGET_COUNT,
        "permutation_canary_abstention_count": canary_abstentions == 0,
        "evidence_complete_rate": evidence_complete == 16,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V128_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": 10,
            "migration_owner_count": TARGET_COUNT,
            "audited_control_count": CONTROL_COUNT,
            "audited_control_exact_count": control_exact,
            "migration_owner_abstention_count": owner_abstentions,
            "permutation_canary_count": TARGET_COUNT,
            "permutation_canary_exact_count": canary_exact,
            "permutation_canary_abstention_count": canary_abstentions,
            "evidence_complete_count": evidence_complete,
        },
        "retained_proposition_reference_patch_authorized": passed,
        "proposition_reference_frozen": passed,
        "alignment_reference_frozen": False,
        "fresh_diagnostic_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "majority_voting_used": False,
    }


def build_reference_candidate_v128(
    *, current_reference: Mapping[str, Any], truth: Mapping[str, Any], primary: Mapping[str, Any]
) -> dict[str, Any]:
    candidate = deepcopy(current_reference)
    observed = {row["task_id"]: row for row in primary["decisions"]}
    changed = unsupported_changes = 0
    for row in truth["tasks"]:
        if row["role"] != "migration_owner":
            continue
        decision = observed[row["task_id"]]["proposition_status"]
        case = candidate["cases"][row["case_id"]]
        witness_id = row["witness_id"]
        changed += int(case["proposition"][witness_id] != decision)
        case["proposition"][witness_id] = decision
        issues = set(case["field_issues"][witness_id])
        before = "unsupported_inference" in issues
        if decision == "unsupported":
            issues.add("unsupported_inference")
        else:
            issues.discard("unsupported_inference")
        case["field_issues"][witness_id] = sorted(issues)
        case["structured_fields"][witness_id] = "incorrect" if issues else "correct"
        unsupported_changes += int(before != ("unsupported_inference" in issues))
    candidate["schema_version"] = V128_REFERENCE_VERSION
    candidate["reference_version"] = "fixture_reference_v10_retained_proposition_migration_frozen"
    candidate["retained_proposition_migration_change_count"] = changed
    candidate["unsupported_inference_projection_change_count"] = unsupported_changes
    candidate["retained_field_reference_patch_authorized"] = True
    candidate["retained_proposition_reference_patch_authorized"] = True
    candidate["proposition_reference_frozen"] = True
    candidate["alignment_reference_frozen"] = False
    candidate["fresh_diagnostic_authorized"] = False
    return candidate


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path, policy_path = root / "capacity-policy-audit.json", root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V128_CAPACITY_AUDIT_VERSION,
        "phase_id": V128_PHASE_ID,
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
        "schema_version": V128_CAPACITY_POLICY_VERSION,
        "phase_id": V128_PHASE_ID,
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


def freeze_v128(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v128 terminal")}
    v127, v120, v106, v116 = _validate_v127(), _validate_v120(), _validate_v106(), _validate_v116()
    value, truth, canary, selection = build_v128_inputs(v127, v120, v106, v116)
    input_path = root / "proposition-migration-input.private.json"
    truth_path = root / "proposition-migration-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_time(selection_path, selection, "created_at")
    turns = []
    for turn_name, item in ((PRIMARY_TURN, value), (CANARY_TURN, canary)):
        prompt, schema = proposition_prompt(item), proposition_output_schema(item)
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=item, prompt=prompt, schema=schema
        )
        turns.append({"turn_name": turn_name, "value": item, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor = {
        **{f"v127_{name}": record for name, record in v127["records"].items()},
        "v127_attempts": v127["attempts"],
        **{f"v126_{name}": record for name, record in v127["v126"]["records"].items()},
        **{f"v116_{name}": record for name, record in v116["records"].items()},
        **{f"v120_{name}": record for name, record in v120["records"].items()},
        **{f"v106_{name}": record for name, record in v106["records"].items()},
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V128_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "gpt54_corrected_proposition_only_owner_for_six_migration_targets_four_audited_controls_and_six_case_canary",
        "migration_owner_count": TARGET_COUNT,
        "audited_control_count": CONTROL_COUNT,
        "permutation_canary_count": TARGET_COUNT,
        "turn_plan": list(TURN_NAMES),
        "corrected_proposition_only_rubric": True,
        "legacy_unsupported_labels_are_targets_not_controls": True,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "migration_roles_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "four_exact_audited_controls_six_decisive_migration_targets_six_exact_canaries_and_complete_exact_evidence",
        "retained_proposition_reference_patch_authorized": False,
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
            _record(runtime_dir / "app_server_judge_v5_calibration_v127_singleton_proposition_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v126_singleton_field_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v117_alignment_reference_owner.py"),
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
        "privacy": "private_source_proposition_output_no_source_text_in_reports",
    }
    spec_path = root / "proposition-migration-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v127": v127,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v128 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V128_FAILURE_VERSION,
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
        "schema_version": V128_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "retained_proposition_reference_patch_authorized": False,
        "proposition_reference_frozen": False,
        "alignment_reference_frozen": False,
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


async def run_v128(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v128 terminal")
    frozen = freeze_v128(output_dir=root, timeout_seconds=timeout_seconds)
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
                    base_instructions=proposition_instructions(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=turn["value"]["task_count"],
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_proposition_output(candidate, item),
                )
                outputs.append(output)
                sidecars.append(sidecar)
        primary, canary = outputs
        primary_path = root / "proposition-migration-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(primary_path, primary)
        _write_immutable(canary_path, canary)
        score = score_v128(primary, canary, frozen["truth"])
        score_path = root / "proposition-migration-score.json"
        _write_immutable(score_path, score)
        candidate_path = root / "calibration-truth-v10-retained-proposition-migration.private.json"
        if score["passed"]:
            candidate = build_reference_candidate_v128(
                current_reference=frozen["v127"]["v126"]["values"]["reference"],
                truth=frozen["truth"],
                primary=primary,
            )
            _write_immutable(candidate_path, candidate)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V128_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v128_proposition_migration_owner_passed_patch_authorized"
                if passed else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v128_retained_proposition_reference_patch_authorized"
                if passed else "v128_proposition_migration_owner_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "retained_proposition_reference_patch_authorized": passed,
            "proposition_reference_frozen": passed,
            "alignment_reference_frozen": False,
            "fresh_diagnostic_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "primary_output": _record(primary_path),
            "canary_output": _record(canary_path),
            "reference_candidate": _record(candidate_path) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v127_usage": frozen["v127"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v128 proposition migration owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v128(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({"state": terminal["state"], "terminal_reason": terminal["terminal_reason"], "retained_proposition_reference_patch_authorized": terminal.get("retained_proposition_reference_patch_authorized", False), "usage_status": terminal.get("usage_status")}, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
