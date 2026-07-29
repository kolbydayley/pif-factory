from __future__ import annotations

"""Decisive Sol owner for the six observable v124 retained-field disputes."""

import argparse
import asyncio
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

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
)
from .app_server_judge_v5_calibration_v123_capped_field_repair import (
    _validate_v119,
    _validate_v122,
    build_prompt_v123,
)
from .app_server_judge_v5_calibration_v124_scoreable_recovery import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V124_ROOT,
    _validate_v123,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V125_INPUT_VERSION = "pif_app_server_judge_v5_4_v125_final_field_owner_input_v1"
V125_TRUTH_VERSION = "pif_app_server_judge_v5_4_v125_final_field_owner_truth_v1"
V125_SELECTION_VERSION = "pif_app_server_judge_v5_4_v125_selection_v1"
V125_SPEC_VERSION = "pif_app_server_judge_v5_4_v125_spec_v1"
V125_SCORE_VERSION = "pif_app_server_judge_v5_4_v125_score_v1"
V125_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v125_reconciled_output_v1"
V125_REFERENCE_VERSION = (
    "pif_app_server_judge_v5_4_calibration_truth_v10_retained_field_owner_frozen"
)
V125_FAILURE_VERSION = "pif_app_server_judge_v5_4_v125_failure_v1"
V125_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v125_terminal_v1"
V125_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V125_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V125_PHASE_ID = "judge_v5_4_v125_final_retained_field_owner"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
PRIMARY_TURN = "final_retained_field_owner"
CANARY_TURN = "final_retained_field_owner_canary"
TURN_NAMES = (PRIMARY_TURN, CANARY_TURN)
DISPUTE_COUNT = 6
CONTROL_COUNT = 6
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V124_ROOT.parent / "judge-calibration-v5_4-v125-final-field-owner"
).resolve()


class JudgeV5CalibrationV125Error(RuntimeError):
    """The v125 final-owner contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v124() -> dict[str, Any]:
    root = DEFAULT_V124_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "recovered": root / "scoreable-v123-output.private.json",
        "score": root / "recovered-v123-score.json",
        "full_score": root / "recomputed-full-field-owner-score.json",
        "reconciled": root / "reconciled-retained-field-output.private.json",
        "audit": root / "exact-identity-recovery-audit.json",
        "taxonomy": root / "sanitized-quality-taxonomy.json",
    }
    values = {name: _load_json(path, f"v124 {name}") for name, path in paths.items()}
    terminal, score, taxonomy = values["terminal"], values["score"], values["taxonomy"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v124_scoreable_v123_quality_disagreement_final_owner_required"
        or terminal.get("quality_result_scoreable") is not True
        or terminal.get("quality_gate_passed") is not False
        or terminal.get("final_owner_authorized") is not True
        or terminal.get("retained_field_reference_patch_authorized") is not False
        or terminal.get("fresh_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_attempt_started") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != {field: 0 for field in USAGE_FIELDS}
        or terminal.get("predecessor_v123_usage", {}).get("total_tokens") != 26894
        or score.get("passed") is not False
        or score.get("metrics", {}).get("matched_control_exact_count") != 3
        or score.get("metrics", {}).get("repair_gate_exact_count") != 0
        or taxonomy.get("observable_final_owner_dispute_count") != DISPUTE_COUNT
        or taxonomy.get("final_owner_authorized") is not True
    ):
        raise JudgeV5CalibrationV125Error("v124 recovery contract drifted")
    terminal_records = {
        "recovered": "recovered_output",
        "score": "score",
        "full_score": "full_field_owner_score",
        "reconciled": "reconciled_output",
        "audit": "recovery_audit",
        "taxonomy": "sanitized_taxonomy",
    }
    for name, terminal_key in terminal_records.items():
        record = terminal.get(terminal_key)
        if (
            not isinstance(record, Mapping)
            or dict(record) != _record(paths[name])
            or not _verify_record(record)
        ):
            raise JudgeV5CalibrationV125Error(f"v124 {name} record drifted")
    v123 = _validate_v123()
    v122 = _validate_v122()
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "v123": v123,
        "v122": v122,
    }


def _owner_task_id(source_task_id: str) -> str:
    return "owner_" + sha256_text(f"v125|owner|{source_task_id}")[:24]


def _control_task_id(source_task_id: str) -> str:
    return "control_" + sha256_text(f"v125|control|{source_task_id}")[:24]


def _canary_task_id(owner_task_id: str) -> str:
    return "canary_" + sha256_text(f"v125|canary|{owner_task_id}")[:24]


def build_v125_inputs(
    v124: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    v123_input = {
        str(row["task_id"]): row for row in v124["v123"]["values"]["input"]["tasks"]
    }
    v123_truth = {
        str(row["task_id"]): row for row in v124["v123"]["values"]["truth"]["tasks"]
    }
    v123_output = {
        str(row["task_id"]): row for row in v124["values"]["recovered"]["decisions"]
    }
    disputed_v123_ids = sorted(
        task_id
        for task_id, row in v123_truth.items()
        if v123_output[task_id]["field_status"]
        != (row.get("required_gate_status") or row.get("control_expected_status"))
    )
    if len(disputed_v123_ids) != DISPUTE_COUNT:
        raise JudgeV5CalibrationV125Error("v125 dispute coverage drifted")

    tasks = []
    truth_rows = []
    for v123_task_id in disputed_v123_ids:
        source_truth = v123_truth[v123_task_id]
        source_task_id = str(source_truth["source_task_id"])
        task_id = _owner_task_id(source_task_id)
        task = deepcopy(v123_input[v123_task_id])
        task["task_id"] = task_id
        tasks.append(task)
        truth_rows.append(
            {
                "task_id": task_id,
                "source_task_id": source_task_id,
                "v123_task_id": v123_task_id,
                "role": "final_owner_dispute",
                "field": source_truth["field"],
            }
        )

    v121_truth = {
        str(row["task_id"]): row
        for row in v124["v122"]["v121"]["values"]["truth"]["tasks"]
    }
    v121_input = {
        str(row["task_id"]): row for row in v124["v122"]["v121_input"]["tasks"]
    }
    v121_primary = {
        str(row["task_id"]): row for row in v124["v122"]["values"]["primary"]["decisions"]
    }
    excluded_source_ids = {str(row["source_task_id"]) for row in v123_truth.values()}
    eligible_controls = [
        row
        for row in v121_truth.values()
        if row["role"] == "matched_control"
        and row["task_id"] not in excluded_source_ids
        and v121_primary[row["task_id"]]["field_status"] == row["control_expected_status"]
        and v121_primary[row["task_id"]]["field_status"] != "abstain"
    ]
    eligible_controls.sort(
        key=lambda row: sha256_text(f"v125|control-rank|{row['field']}|{row['task_id']}")
    )
    controls = []
    used_fields = set()
    for row in eligible_controls:
        if row["field"] in used_fields:
            continue
        controls.append(row)
        used_fields.add(row["field"])
        if len(controls) == CONTROL_COUNT:
            break
    if len(controls) != CONTROL_COUNT:
        raise JudgeV5CalibrationV125Error("v125 matched control coverage drifted")
    for row in controls:
        source_task_id = str(row["task_id"])
        task_id = _control_task_id(source_task_id)
        task = deepcopy(v121_input[source_task_id])
        task["task_id"] = task_id
        tasks.append(task)
        truth_rows.append(
            {
                "task_id": task_id,
                "source_task_id": source_task_id,
                "role": "matched_control",
                "field": row["field"],
                "control_expected_status": row["control_expected_status"],
            }
        )

    tasks.sort(key=lambda row: sha256_text(f"v125|primary-order|{row['task_id']}"))
    truth_rows.sort(key=lambda row: row["task_id"])
    owner_tasks = {
        row["task_id"]: row
        for row in tasks
        if any(
            truth["task_id"] == row["task_id"] and truth["role"] == "final_owner_dispute"
            for truth in truth_rows
        )
    }
    canary_tasks = []
    canary_map = []
    for owner_id in sorted(owner_tasks, key=lambda value: sha256_text(f"v125|canary-order|{value}")):
        canary_id = _canary_task_id(owner_id)
        task = deepcopy(owner_tasks[owner_id])
        task["task_id"] = canary_id
        canary_tasks.append(task)
        canary_map.append({"owner_task_id": owner_id, "canary_task_id": canary_id})
    if len(tasks) != 12 or len(canary_tasks) != DISPUTE_COUNT:
        raise JudgeV5CalibrationV125Error("v125 primary or canary coverage drifted")
    value = {
        "schema_version": V125_INPUT_VERSION,
        "task_count": 12,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth = {
        "schema_version": V125_TRUTH_VERSION,
        "task_count": 12,
        "final_owner_dispute_count": DISPUTE_COUNT,
        "matched_control_count": CONTROL_COUNT,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V125_INPUT_VERSION,
        "task_count": DISPUTE_COUNT,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V125_SELECTION_VERSION,
        "created_at": now_iso(),
        "final_owner_dispute_count": DISPUTE_COUNT,
        "matched_control_count": CONTROL_COUNT,
        "permutation_canary_count": DISPUTE_COUNT,
        "dispute_field_counts": dict(
            sorted(Counter(row["field"] for row in truth_rows if row["role"] == "final_owner_dispute").items())
        ),
        "control_field_count": len(used_fields),
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "dispute_reasons_in_model_input": False,
        "final_owner_is_decisive": True,
        "majority_voting_used": False,
        "semantic_pruning_performed": False,
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return value, truth, canary, selection


def score_v125(
    primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {str(row["task_id"]): row for row in truth["tasks"]}
    observed = {str(row["task_id"]): row for row in primary["decisions"]}
    repeated = {str(row["task_id"]): row for row in canary["decisions"]}
    if set(expected) != set(observed) or len(repeated) != DISPUTE_COUNT:
        raise JudgeV5CalibrationV125Error("v125 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "matched_control"]
    disputes = [row for row in expected.values() if row["role"] == "final_owner_dispute"]
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    dispute_abstentions = sum(
        observed[row["task_id"]]["field_status"] == "abstain" for row in disputes
    )
    canary_exact = sum(
        observed[row["owner_task_id"]]["field_status"]
        == repeated[row["canary_task_id"]]["field_status"]
        for row in truth["canary_map"]
    )
    canary_abstentions = sum(row["field_status"] == "abstain" for row in repeated.values())
    all_rows = list(observed.values()) + list(repeated.values())
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in all_rows)
    checks = {
        "matched_control_exact_rate": control_exact == CONTROL_COUNT,
        "final_owner_dispute_abstention_count": dispute_abstentions == 0,
        "permutation_canary_exact_rate": canary_exact == DISPUTE_COUNT,
        "permutation_canary_abstention_count": canary_abstentions == 0,
        "evidence_complete_rate": evidence_complete == 18,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V125_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": 12,
            "final_owner_dispute_count": DISPUTE_COUNT,
            "matched_control_count": CONTROL_COUNT,
            "matched_control_exact_count": control_exact,
            "final_owner_dispute_abstention_count": dispute_abstentions,
            "permutation_canary_count": DISPUTE_COUNT,
            "permutation_canary_exact_count": canary_exact,
            "permutation_canary_abstention_count": canary_abstentions,
            "evidence_complete_count": evidence_complete,
        },
        "retained_field_reference_patch_authorized": passed,
        "proposition_reference_frozen": False,
        "alignment_reference_frozen": False,
        "fresh_diagnostic_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "final_owner_is_decisive": True,
        "majority_voting_used": False,
    }


def reconcile_v125(
    *, base_primary: Mapping[str, Any], truth: Mapping[str, Any], owner: Mapping[str, Any]
) -> dict[str, Any]:
    final = deepcopy(base_primary)
    final_rows = {str(row["task_id"]): row for row in final["decisions"]}
    owner_rows = {str(row["task_id"]): row for row in owner["decisions"]}
    changed = 0
    for row in truth["tasks"]:
        if row["role"] != "final_owner_dispute":
            continue
        source_id = str(row["source_task_id"])
        decision = owner_rows[str(row["task_id"])]
        final_rows[source_id]["field_status"] = decision["field_status"]
        final_rows[source_id]["source_evidence_spans"] = deepcopy(
            decision["source_evidence_spans"]
        )
        final_rows[source_id]["rationale"] = "Decisive side-free Sol owner decision."
        changed += 1
    if changed != DISPUTE_COUNT:
        raise JudgeV5CalibrationV125Error("v125 reconciliation coverage drifted")
    final["schema_version"] = V125_OUTPUT_VERSION
    final["final_owner_change_count"] = changed
    final["final_owner_basis"] = "v125_side_free_sol_owner_with_controls_and_canary"
    return final


def build_reference_candidate_v125(
    *, current_reference: Mapping[str, Any], v121_truth: Mapping[str, Any],
    reconciled: Mapping[str, Any], owner_truth: Mapping[str, Any]
) -> dict[str, Any]:
    candidate = deepcopy(current_reference)
    observed = {str(row["task_id"]): row for row in reconciled["decisions"]}
    owner_source_ids = {
        str(row["source_task_id"])
        for row in owner_truth["tasks"]
        if row["role"] == "final_owner_dispute"
    }
    patch_rows = [
        row
        for row in v121_truth["tasks"]
        if row["role"] != "matched_control" or row["task_id"] in owner_source_ids
    ]
    if len(patch_rows) != 97:
        raise JudgeV5CalibrationV125Error("v125 reference patch coverage drifted")
    changed = 0
    for row in patch_rows:
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
    candidate["schema_version"] = V125_REFERENCE_VERSION
    candidate["reference_version"] = "fixture_reference_v10_retained_field_owner_frozen"
    candidate["retained_field_owner_change_count"] = changed
    candidate["retained_field_final_owner_case_count"] = DISPUTE_COUNT
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
        "schema_version": V125_CAPACITY_AUDIT_VERSION,
        "phase_id": V125_PHASE_ID,
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
        "schema_version": V125_CAPACITY_POLICY_VERSION,
        "phase_id": V125_PHASE_ID,
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


def freeze_v125(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v125 terminal")}
    v124 = _validate_v124()
    v119 = _validate_v119()
    value, truth, canary, selection = build_v125_inputs(v124)
    input_path = root / "final-owner-input.private.json"
    truth_path = root / "final-owner-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_time(selection_path, selection, "created_at")
    turns = []
    for turn_name, item in ((PRIMARY_TURN, value), (CANARY_TURN, canary)):
        prompt, schema = build_prompt_v123(item), output_schema(item)
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=item, prompt=prompt, schema=schema
        )
        turns.append(
            {"turn_name": turn_name, "value": item, "prompt": prompt, "schema": schema, "paths": paths}
        )
    predecessor = {
        **{f"v124_{name}": record for name, record in v124["records"].items()},
        **{f"v123_{name}": record for name, record in v124["v123"]["records"].items()},
        **{f"v122_{name}": record for name, record in v124["v122"]["records"].items()},
        "v121_input": v124["v122"]["v121_input_record"],
        **{f"v119_{name}": record for name, record in v119["records"].items()},
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V125_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "decisive_side_free_sol_owner_for_six_disputes_six_controls_and_six_case_canary",
        "final_owner_dispute_count": DISPUTE_COUNT,
        "matched_control_count": CONTROL_COUNT,
        "permutation_canary_count": DISPUTE_COUNT,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "dispute_reasons_in_model_input": False,
        "final_owner_is_decisive": True,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "six_exact_controls_six_decisive_disputes_six_exact_canaries_and_complete_exact_evidence",
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
            _record(runtime_dir / "app_server_judge_v5_calibration_v124_scoreable_recovery.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v123_capped_field_repair.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v122_field_owner_recovery.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v121_retained_field_owner.py"),
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
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "final-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v124": v124,
        "v119": v119,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v125 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V125_FAILURE_VERSION,
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
        "schema_version": V125_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "retained_field_reference_patch_authorized": False,
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


async def run_v125(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v125 terminal")
    frozen = freeze_v125(output_dir=root, timeout_seconds=timeout_seconds)
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
        primary, canary = outputs
        primary_path = root / "final-owner-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(primary_path, primary)
        _write_immutable(canary_path, canary)
        score = score_v125(primary, canary, frozen["truth"])
        score_path = root / "final-owner-score.json"
        _write_immutable(score_path, score)
        reconciled_path = root / "reconciled-retained-field-output.private.json"
        candidate_path = root / "calibration-truth-v10-retained-field-owner.private.json"
        if score["passed"]:
            reconciled = reconcile_v125(
                base_primary=frozen["v124"]["v122"]["values"]["primary"],
                truth=frozen["truth"],
                owner=primary,
            )
            _write_immutable(reconciled_path, reconciled)
            candidate = build_reference_candidate_v125(
                current_reference=frozen["v119"]["values"]["truth"],
                v121_truth=frozen["v124"]["v122"]["v121"]["values"]["truth"],
                reconciled=reconciled,
                owner_truth=frozen["truth"],
            )
            _write_immutable(candidate_path, candidate)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V125_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v125_final_owner_passed_retained_field_patch_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v125_retained_field_reference_patch_authorized"
                if passed
                else "v125_final_field_owner_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "retained_field_reference_patch_authorized": passed,
            "proposition_reference_frozen": False,
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
            "reconciled_output": _record(reconciled_path) if passed else None,
            "reference_candidate": _record(candidate_path) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v123_usage": frozen["v124"]["v123"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v125 final retained-field owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v125(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
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
