from __future__ import annotations

"""Field-specific Sol owner diagnostic for the dominant v134 reference disputes."""

import argparse
import asyncio
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v134_gpt55_singleton_pointwise as v134
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


V135_INPUT_VERSION = "pif_app_server_judge_v5_4_v135_field_owner_input_v1"
V135_TRUTH_VERSION = "pif_app_server_judge_v5_4_v135_field_owner_truth_v1"
V135_SELECTION_VERSION = "pif_app_server_judge_v5_4_v135_selection_v1"
V135_SPEC_VERSION = "pif_app_server_judge_v5_4_v135_spec_v1"
V135_SCORE_VERSION = "pif_app_server_judge_v5_4_v135_score_v1"
V135_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v135_output_v1"
V135_FAILURE_VERSION = "pif_app_server_judge_v5_4_v135_failure_v1"
V135_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v135_terminal_v1"
V135_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V135_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V135_PHASE_ID = "judge_v5_4_v135_dominant_field_owner_diagnostic"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
FIELDS = ("attribution", "certainty", "speaker")
OWNER_TASKS_PER_FIELD = 2
CONTROL_TASKS_PER_FIELD = 2
TASKS_PER_TURN = OWNER_TASKS_PER_FIELD + CONTROL_TASKS_PER_FIELD
PRIMARY_TURNS = tuple(f"dominant_{field}_primary" for field in FIELDS)
CANARY_TURNS = tuple(f"dominant_{field}_order_canary" for field in FIELDS)
TURN_NAMES = PRIMARY_TURNS + CANARY_TURNS
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    v134.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v135-dominant-field-owner-diagnostic"
).resolve()


class JudgeV5CalibrationV135Error(RuntimeError):
    """The v135 field-owner diagnostic contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v134() -> dict[str, Any]:
    root = v134.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "gpt55-singleton-pointwise-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "gpt55-singleton-pointwise-score.json",
        "pointwise": root / "gpt55-singleton-pointwise-output.private.json",
        "canary": root / "gpt55-singleton-canary-output.private.json",
        "truth": root / "gpt55-singleton-truth.private.json",
        "input": root / "pointwise-input.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v134 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v134_gpt55_singleton_pointwise_quality_gate_not_passed"
        or terminal.get("pointwise_diagnostic_passed") is not False
        or terminal.get("expanded_singleton_pointwise_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 254135
        or score.get("passed") is not False
        or score.get("failed_checks")
        != ["order_bias", "pointwise_field_issue_f1", "structured_field_accuracy"]
        or score.get("metrics", {}).get("support_sensitivity") != 1.0
        or score.get("metrics", {}).get("support_specificity") != 1.0
        or score.get("metrics", {}).get("structured_field_accuracy") != 0.555556
        or score.get("metrics", {}).get("pointwise_field_issue_f1") != 0.5
        or score.get("metrics", {}).get("canary_case_exact_count") != 0
        or spec.get("model") != "gpt-5.5"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV135Error("v134 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV135Error("v134 runtime record drifted")
    for name, key in {
        "score": "score",
        "pointwise": "pointwise_output",
        "canary": "canary_output",
    }.items():
        record = terminal.get(key)
        if (
            not isinstance(record, Mapping)
            or dict(record) != _record(paths[name])
            or not _verify_record(record)
        ):
            raise JudgeV5CalibrationV135Error(f"v134 {name} record drifted")
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
                raise JudgeV5CalibrationV135Error("v134 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v134 sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV135Error("v134 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "v133": v134._validate_v133(),
    }


def _task_id(role: str, case_id: str, witness_id: str, field: str) -> str:
    return role + "_" + sha256_text(
        f"v135|{role}|{case_id}|{witness_id}|{field}"
    )[:24]


def _input_value(field: str, tasks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": V135_INPUT_VERSION,
        "field_focus": field,
        "task_count": len(tasks),
        "tasks": deepcopy(list(tasks)),
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "disagreement_reasons_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }


def build_v135_inputs(
    predecessor: Mapping[str, Any], controls_source: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    truth = predecessor["values"]["truth"]
    units = {
        str(row["witness_id"]): row
        for row in predecessor["values"]["input"]["units"]
    }
    observed = {
        str(row["witness_id"]): set(row["field_issue_fields"])
        for row in predecessor["values"]["pointwise"]["units"]
    }
    contracts = _field_contracts()
    owner_candidates: dict[str, list[dict[str, Any]]] = {field: [] for field in FIELDS}
    for case_id, case in truth["cases"].items():
        for witness_id, expected_fields in case["field_issues"].items():
            current = set(expected_fields)
            for field in FIELDS:
                if (field in current) == (field in observed[witness_id]):
                    continue
                unit = units[witness_id]
                event = compact_empty_event_fields(deepcopy(unit["structured_event"]))
                owner_candidates[field].append(
                    {
                        "case_id": case_id,
                        "witness_id": witness_id,
                        "current_status": "incorrect" if field in current else "correct",
                        "v134_status": (
                            "incorrect" if field in observed[witness_id] else "correct"
                        ),
                        "task": {
                            "task_id": _task_id("owner", case_id, witness_id, field),
                            "field": field,
                            "field_contract": deepcopy(contracts[field]),
                            "requested_field_value": _general_requested_field_value(
                                field, event
                            ),
                            "source_excerpt": unit["source_excerpt"],
                            "structured_event": event,
                        },
                    }
                )
    expected_counts = {"attribution": 8, "certainty": 11, "speaker": 9}
    if {field: len(rows) for field, rows in owner_candidates.items()} != expected_counts:
        raise JudgeV5CalibrationV135Error("v134 dominant disagreement coverage drifted")

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
    rows: list[dict[str, Any]] = []
    truth_rows: list[dict[str, Any]] = []
    control_status_counts: Counter[str] = Counter()
    for field, primary_turn, canary_turn in zip(
        FIELDS, PRIMARY_TURNS, CANARY_TURNS, strict=True
    ):
        candidates = sorted(
            owner_candidates[field],
            key=lambda row: sha256_text(
                f"v135|owner-rank|{field}|{row['case_id']}|{row['witness_id']}"
            ),
        )[:OWNER_TASKS_PER_FIELD]
        controls = [
            row
            for row in v121_truth
            if row["role"] == "matched_control"
            and row["field"] == field
            and v121_primary[row["task_id"]]["field_status"]
            == row["control_expected_status"]
            and v121_primary[row["task_id"]]["field_status"] != "abstain"
        ]
        controls.sort(key=lambda row: sha256_text(f"v135|control-rank|{row['task_id']}"))
        controls = controls[:CONTROL_TASKS_PER_FIELD]
        if len(candidates) != OWNER_TASKS_PER_FIELD or len(controls) != CONTROL_TASKS_PER_FIELD:
            raise JudgeV5CalibrationV135Error("v135 field-specific coverage drifted")

        model_tasks = []
        for candidate in candidates:
            model_tasks.append(deepcopy(candidate["task"]))
            truth_rows.append(
                {
                    "task_id": candidate["task"]["task_id"],
                    "role": "owner",
                    "case_id": candidate["case_id"],
                    "witness_id": candidate["witness_id"],
                    "field": field,
                    "current_status": candidate["current_status"],
                    "v134_status": candidate["v134_status"],
                }
            )
        for control in controls:
            task = deepcopy(v121_input[control["task_id"]])
            task_id = _task_id("control", control["task_id"], control["task_id"], field)
            task["task_id"] = task_id
            model_tasks.append(task)
            truth_rows.append(
                {
                    "task_id": task_id,
                    "role": "control",
                    "source_task_id": control["task_id"],
                    "field": field,
                    "control_expected_status": control["control_expected_status"],
                }
            )
            control_status_counts[f"{field}:{control['control_expected_status']}"] += 1
        model_tasks.sort(
            key=lambda task: sha256_text(f"v135|primary-order|{field}|{task['task_id']}")
        )
        primary_value = _input_value(field, model_tasks)
        canary_value = _input_value(field, list(reversed(model_tasks)))
        rows.extend(
            [
                {
                    "turn_name": primary_turn,
                    "turn_role": "primary",
                    "field": field,
                    "value": primary_value,
                },
                {
                    "turn_name": canary_turn,
                    "turn_role": "order_canary",
                    "field": field,
                    "value": canary_value,
                },
            ]
        )
    rows.sort(key=lambda row: TURN_NAMES.index(row["turn_name"]))
    truth_value = {
        "schema_version": V135_TRUTH_VERSION,
        "owner_task_count": len(FIELDS) * OWNER_TASKS_PER_FIELD,
        "control_task_count": len(FIELDS) * CONTROL_TASKS_PER_FIELD,
        "tasks": truth_rows,
    }
    selection = {
        "schema_version": V135_SELECTION_VERSION,
        "created_at": now_iso(),
        "fields": list(FIELDS),
        "candidate_disagreement_counts": expected_counts,
        "selected_owner_count": len(FIELDS) * OWNER_TASKS_PER_FIELD,
        "matched_control_count": len(FIELDS) * CONTROL_TASKS_PER_FIELD,
        "control_status_counts": dict(sorted(control_status_counts.items())),
        "tasks_per_turn": TASKS_PER_TURN,
        "primary_turn_count": len(PRIMARY_TURNS),
        "order_canary_turn_count": len(CANARY_TURNS),
        "order_canary_identical_membership_and_ids": True,
        "order_canary_only_difference": "reversed_task_array_order",
        "canary_marker_in_model_input": False,
        "selection_uses_source_text": False,
        "selection_uses_only_frozen_labels_outputs_field_enums_and_opaque_ids": True,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "semantic_pruning_performed": False,
        "majority_voting_used": False,
        "privacy": "private_source_event_output_aggregate_reports_only",
    }
    return rows, truth_value, selection


def score_v135(
    *, primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {str(row["task_id"]): row for row in truth["tasks"]}
    observed = {str(row["task_id"]): row for row in primary["decisions"]}
    repeated = {str(row["task_id"]): row for row in canary["decisions"]}
    if set(observed) != set(expected) or set(repeated) != set(expected):
        raise JudgeV5CalibrationV135Error("v135 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "control"]
    owners = [row for row in expected.values() if row["role"] == "owner"]
    control_exact = sum(
        observed[row["task_id"]]["field_status"]
        == row["control_expected_status"]
        for row in controls
    )
    canary_exact = sum(
        observed[task_id]["field_status"] == repeated[task_id]["field_status"]
        for task_id in expected
    )
    all_decisions = list(observed.values()) + list(repeated.values())
    abstentions = sum(row["field_status"] == "abstain" for row in all_decisions)
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in all_decisions)
    owner_v134_agreement = sum(
        observed[row["task_id"]]["field_status"] == row["v134_status"]
        for row in owners
    )
    owner_current_reference_agreement = sum(
        observed[row["task_id"]]["field_status"] == row["current_status"]
        for row in owners
    )
    checks = {
        "matched_control_exact_rate": control_exact == len(controls),
        "order_canary_exact_rate": canary_exact == len(expected),
        "abstention_count": abstentions == 0,
        "evidence_complete_rate": evidence_complete == len(all_decisions),
        "field_specific_turn_contract": True,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V135_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "field_count": len(FIELDS),
            "owner_task_count": len(owners),
            "matched_control_count": len(controls),
            "matched_control_exact_count": control_exact,
            "order_canary_decision_count": len(expected),
            "order_canary_exact_count": canary_exact,
            "abstention_count": abstentions,
            "evidence_complete_count": evidence_complete,
            "owner_v134_agreement_count": owner_v134_agreement,
            "owner_current_reference_agreement_count": owner_current_reference_agreement,
        },
        "expanded_field_owner_diagnostic_authorized": passed,
        "reference_patch_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V135_CAPACITY_AUDIT_VERSION,
        "phase_id": V135_PHASE_ID,
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
        "schema_version": V135_CAPACITY_POLICY_VERSION,
        "phase_id": V135_PHASE_ID,
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


def freeze_v135(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v135 terminal")}
    predecessor = _validate_v134()
    controls_source = _validate_v125()
    rows, truth, selection = build_v135_inputs(predecessor, controls_source)
    truth_path = root / "dominant-field-owner-truth.private.json"
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
        **{f"v134_{name}": record for name, record in predecessor["records"].items()},
        "v134_attempts": predecessor["attempts"],
        **{
            f"v121_{name}": record
            for name, record in controls_source["v124"]["v122"]["v121"][
                "records"
            ].items()
        },
        "v121_input": controls_source["v124"]["v122"]["v121_input_record"],
        "v121_attempts": controls_source["v124"]["v122"]["v121"]["attempts"],
    }
    capacity = _build_capacity_policy(root, predecessor_records)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V135_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "three_field_specific_sol_owner_pairs_with_unmarked_reversed_membership_canaries",
        "fields": list(FIELDS),
        "owner_task_count": len(FIELDS) * OWNER_TASKS_PER_FIELD,
        "matched_control_count": len(FIELDS) * CONTROL_TASKS_PER_FIELD,
        "tasks_per_turn": TASKS_PER_TURN,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "canary_marker_in_model_input": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_controls_all_repeated_decisions_no_abstentions_and_complete_exact_evidence",
        "expanded_field_owner_diagnostic_authorized": False,
        "reference_patch_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor_records,
        "runtime_files": [
            _record(Path(__file__)),
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
    spec_path = root / "dominant-field-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "predecessor": predecessor,
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
                _load_json(Path(record["path"]), "v135 sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V135_FAILURE_VERSION,
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
        "schema_version": V135_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "expanded_field_owner_diagnostic_authorized": False,
        "reference_patch_authorized": False,
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


async def run_v135(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v135 terminal")
    frozen = freeze_v135(output_dir=root, timeout_seconds=timeout_seconds)
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
                    batch_size=TASKS_PER_TURN,
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
        primary = _merge_outputs(primary_outputs, "decisions")
        canary = _merge_outputs(canary_outputs, "decisions")
        primary["schema_version"] = V135_OUTPUT_VERSION
        canary["schema_version"] = V135_OUTPUT_VERSION
        paths = {
            "primary": root / "dominant-field-owner-output.private.json",
            "canary": root / "dominant-field-owner-canary.private.json",
            "score": root / "dominant-field-owner-score.json",
        }
        _write_immutable(paths["primary"], primary)
        _write_immutable(paths["canary"], canary)
        score = score_v135(primary=primary, canary=canary, truth=frozen["truth"])
        _write_immutable(paths["score"], score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V135_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v135_dominant_field_owner_passed_expansion_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v135_field_specific_owner_protocol_passed"
                if passed
                else "v135_field_specific_owner_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "expanded_field_owner_diagnostic_authorized": passed,
            "reference_patch_authorized": False,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(paths["score"]),
            "primary_output": _record(paths["primary"]),
            "canary_output": _record(paths["canary"]),
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v134_usage": frozen["predecessor"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run v135 dominant field-specific owner diagnostic"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v135(
            output_dir=Path(args.output_dir),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "expanded_field_owner_diagnostic_authorized": terminal.get(
                    "expanded_field_owner_diagnostic_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
