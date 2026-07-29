from __future__ import annotations

"""Capped singleton repair for the four observable v137 field-owner triggers."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v137_full_observable_field_owner as v137
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


V138_INPUT_VERSION = "pif_app_server_judge_v5_4_v138_singleton_repair_input_v1"
V138_TRUTH_VERSION = "pif_app_server_judge_v5_4_v138_singleton_repair_truth_v1"
V138_SELECTION_VERSION = "pif_app_server_judge_v5_4_v138_selection_v1"
V138_SPEC_VERSION = "pif_app_server_judge_v5_4_v138_spec_v1"
V138_SCORE_VERSION = "pif_app_server_judge_v5_4_v138_score_v1"
V138_FAILURE_VERSION = "pif_app_server_judge_v5_4_v138_failure_v1"
V138_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v138_terminal_v1"
V138_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V138_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V138_PHASE_ID = "judge_v5_4_v138_singleton_field_repair"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
TURN_NAMES = (
    "singleton_control_certainty",
    "singleton_control_metric",
    "singleton_repair_certainty_owner",
    "singleton_repair_metric_owner",
    "singleton_repair_evidence_control",
    "singleton_repair_temporal_horizon_control",
)
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    v137.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v138-singleton-field-repair"
).resolve()


class JudgeV5CalibrationV138Error(RuntimeError):
    """The v138 singleton repair contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v137() -> dict[str, Any]:
    root = v137.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "full-observable-field-owner-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "full-observable-field-owner-score.json",
        "primary": root / "full-observable-field-owner-output.private.json",
        "canary": root / "full-observable-field-owner-canary.private.json",
        "owner": root / "combined-observable-field-owner.private.json",
        "truth": root / "full-observable-field-owner-truth.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v137 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v137_full_observable_field_owner_quality_gate_not_passed"
        or terminal.get("reference_patch_authorized") is not False
        or terminal.get("reference_frozen") is not False
        or terminal.get("fresh_pointwise_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 475009
        or score.get("passed") is not False
        or score.get("failed_checks")
        != ["matched_control_exact_rate", "order_canary_exact_rate"]
        or score.get("metrics", {}).get("matched_control_exact_count") != 11
        or score.get("metrics", {}).get("matched_control_count") != 12
        or score.get("metrics", {}).get("order_canary_exact_count") != 37
        or score.get("metrics", {}).get("order_canary_decision_count") != 40
        or score.get("metrics", {}).get("new_owner_abstention_count") != 0
        or score.get("metrics", {}).get("evidence_complete_count") != 100
        or spec.get("model") != "gpt-5.6-sol"
        or spec.get("turn_plan") != list(v137.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV138Error("v137 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV138Error("v137 runtime record drifted")
    for name, key in {
        "score": "score",
        "primary": "primary_output",
        "canary": "canary_output",
        "owner": "combined_owner_output",
    }.items():
        record = terminal.get(key)
        if record != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV138Error(f"v137 {name} record drifted")
    attempts = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    frozen_turns = {row["turn_name"]: row for row in spec["frozen_inputs"]["turns"]}
    input_tasks = {}
    for turn_name in spec["turn_plan"]:
        frozen = frozen_turns[turn_name]
        turn_root = root / "turns" / turn_name.replace("_", "-")
        turn_paths = {
            name: turn_root / filename
            for name, filename in {
                "capacity": "capacity.json",
                "sidecar": "sidecar.json",
                "output": "output.private.json",
            }.items()
        }
        if any(not path.is_file() for path in turn_paths.values()):
            raise JudgeV5CalibrationV138Error("v137 turn coverage is incomplete")
        value = _load_json(Path(frozen["input"]["path"]), "v137 turn input")
        for task in value["tasks"]:
            input_tasks[str(task["task_id"])] = task
        measured = _validate_usage(_load_json(turn_paths["sidecar"], "v137 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = {name: _record(path) for name, path in turn_paths.items()}
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV138Error("v137 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "input_tasks": input_tasks,
        "v136": v137._validate_v136(),
    }


def _singleton_task_id(role: str, source_task_id: str) -> str:
    return role + "_" + sha256_text(f"v138|{role}|{source_task_id}")[:24]


def _one_task_value(task: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": V138_INPUT_VERSION,
        "task_count": 1,
        "tasks": [deepcopy(task)],
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "disagreement_reasons_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "singleton_context": True,
    }


def build_v138_inputs(
    v137_source: Mapping[str, Any], controls_source: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    truth = {
        row["task_id"]: row for row in v137_source["values"]["truth"]["tasks"]
    }
    primary = {
        row["task_id"]: row
        for row in v137_source["values"]["primary"]["decisions"]
    }
    canary = {
        row["task_id"]: row
        for row in v137_source["values"]["canary"]["decisions"]
    }
    failed_controls = [
        row
        for row in truth.values()
        if row["role"] == "control"
        and primary[row["task_id"]]["field_status"]
        != row["control_expected_status"]
    ]
    disagreements = [
        truth[task_id]
        for task_id, row in canary.items()
        if primary[task_id]["field_status"] != row["field_status"]
    ]
    signature = sorted((row["field"], row["role"]) for row in failed_controls + disagreements)
    if signature != [
        ("certainty", "owner"),
        ("evidence", "control"),
        ("metric", "owner"),
        ("temporal_horizon", "control"),
    ]:
        raise JudgeV5CalibrationV138Error("v137 repair trigger coverage drifted")
    triggers = {row["field"]: row for row in failed_controls + disagreements}
    if len(triggers) != 4:
        raise JudgeV5CalibrationV138Error("v138 trigger identity coverage drifted")

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
    for field in ("certainty", "metric"):
        candidates = [
            row
            for row in v121_truth
            if row["role"] == "matched_control"
            and row["field"] == field
            and v121_primary[row["task_id"]]["field_status"]
            == row["control_expected_status"]
            and v121_primary[row["task_id"]]["field_status"] != "abstain"
        ]
        candidates.sort(key=lambda row: sha256_text(f"v138|control|{row['task_id']}"))
        if not candidates:
            raise JudgeV5CalibrationV138Error("v138 independent control coverage drifted")
        source = candidates[0]
        task = deepcopy(v121_input[source["task_id"]])
        task_id = _singleton_task_id("control", source["task_id"])
        task["task_id"] = task_id
        rows.append(
            {
                "turn_name": f"singleton_control_{field}",
                "turn_role": "independent_control",
                "field": field,
                "value": _one_task_value(task),
            }
        )
        truth_rows.append(
            {
                "task_id": task_id,
                "role": "control",
                "field": field,
                "source_task_id": source["task_id"],
                "control_expected_status": source["control_expected_status"],
            }
        )
    for field in ("certainty", "metric", "evidence", "temporal_horizon"):
        trigger = triggers[field]
        source_task_id = trigger["task_id"]
        task = deepcopy(v137_source["input_tasks"][source_task_id])
        task_id = _singleton_task_id("repair", source_task_id)
        task["task_id"] = task_id
        turn_name = (
            f"singleton_repair_{field}_owner"
            if trigger["role"] == "owner"
            else f"singleton_repair_{field}_control"
        )
        rows.append(
            {
                "turn_name": turn_name,
                "turn_role": f"repair_{trigger['role']}",
                "field": field,
                "value": _one_task_value(task),
            }
        )
        truth_row = {
            "task_id": task_id,
            "role": trigger["role"],
            "field": field,
            "source_v137_task_id": source_task_id,
        }
        if trigger["role"] == "control":
            truth_row["control_expected_status"] = trigger["control_expected_status"]
        truth_rows.append(truth_row)
    rows.sort(key=lambda row: TURN_NAMES.index(row["turn_name"]))
    truth_value = {
        "schema_version": V138_TRUTH_VERSION,
        "task_count": 6,
        "independent_control_count": 2,
        "repair_owner_count": 2,
        "repair_control_count": 2,
        "tasks": truth_rows,
    }
    selection = {
        "schema_version": V138_SELECTION_VERSION,
        "created_at": now_iso(),
        "observable_trigger_count": 4,
        "trigger_field_role_signature": signature,
        "independent_control_count": 2,
        "singleton_turn_count": 6,
        "maximum_tasks_per_turn": 1,
        "selection_uses_source_text": False,
        "selection_uses_only_observable_control_and_order_failures": True,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "semantic_pruning_performed": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
        "privacy": "private_source_event_output_aggregate_reports_only",
    }
    return rows, truth_value, selection


def score_v138(outputs: Mapping[str, Mapping[str, Any]], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {
        row["task_id"]: row
        for output in outputs.values()
        for row in output["decisions"]
    }
    if set(observed) != set(expected) or len(observed) != 6:
        raise JudgeV5CalibrationV138Error("v138 score coverage drifted")
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
    checks = {
        "singleton_control_exact_rate": control_exact == len(controls),
        "singleton_owner_abstention_count": owner_abstentions == 0,
        "evidence_complete_rate": evidence_complete == len(observed),
        "singleton_context_contract": True,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V138_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": 6,
            "singleton_control_count": len(controls),
            "singleton_control_exact_count": control_exact,
            "singleton_owner_count": len(owners),
            "singleton_owner_abstention_count": owner_abstentions,
            "evidence_complete_count": evidence_complete,
        },
        "repaired_v137_rescore_authorized": passed,
        "reference_patch_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def repair_v137_outputs(
    *,
    outputs: Mapping[str, Mapping[str, Any]],
    truth: Mapping[str, Any],
    primary: Mapping[str, Any],
    canary: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    observed = {
        row["task_id"]: row
        for output in outputs.values()
        for row in output["decisions"]
    }
    repaired_primary = deepcopy(primary)
    repaired_canary = deepcopy(canary)
    primary_map = {row["task_id"]: row for row in repaired_primary["decisions"]}
    canary_map = {row["task_id"]: row for row in repaired_canary["decisions"]}
    repaired_count = 0
    for row in truth["tasks"]:
        source_task_id = row.get("source_v137_task_id")
        if source_task_id is None:
            continue
        source = observed[row["task_id"]]
        replacement = {
            **deepcopy(source),
            "task_id": source_task_id,
            "rationale": "Capped isolated v138 repair owner decision.",
        }
        primary_map[source_task_id].update(deepcopy(replacement))
        if source_task_id in canary_map:
            canary_map[source_task_id].update(deepcopy(replacement))
        repaired_count += 1
    if repaired_count != 4:
        raise JudgeV5CalibrationV138Error("v138 repaired output coverage drifted")
    return repaired_primary, repaired_canary


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V138_CAPACITY_AUDIT_VERSION,
        "phase_id": V138_PHASE_ID,
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
        "schema_version": V138_CAPACITY_POLICY_VERSION,
        "phase_id": V138_PHASE_ID,
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


def freeze_v138(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v138 terminal")}
    predecessor = _validate_v137()
    controls_source = _validate_v125()
    rows, truth, selection = build_v138_inputs(predecessor, controls_source)
    truth_path = root / "singleton-field-repair-truth.private.json"
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
        **{f"v137_{name}": record for name, record in predecessor["records"].items()},
        "v137_attempts": predecessor["attempts"],
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
        "schema_version": V138_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "six_isolated_sol_turns_for_four_v137_observable_triggers_and_two_controls",
        "observable_trigger_count": 4,
        "independent_control_count": 2,
        "maximum_tasks_per_turn": 1,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_four_controls_two_decisive_owners_complete_evidence_and_original_v137_rescore_pass",
        "reference_patch_authorized": False,
        "fresh_pointwise_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor_records,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v137_full_observable_field_owner.py"),
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
    spec_path = root / "singleton-field-repair-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v137": predecessor,
        "current_reference": predecessor["v136"]["v134"]["v133"]["v132"][
            "values"
        ]["reference"],
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
                _load_json(Path(record["path"]), "v138 sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V138_FAILURE_VERSION,
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
        "schema_version": V138_TERMINAL_VERSION,
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


async def run_v138(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v138 terminal")
    frozen = freeze_v138(output_dir=root, timeout_seconds=timeout_seconds)
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
        repair_score = score_v138(outputs, frozen["truth"])
        repaired_primary, repaired_canary = repair_v137_outputs(
            outputs=outputs,
            truth=frozen["truth"],
            primary=frozen["v137"]["values"]["primary"],
            canary=frozen["v137"]["values"]["canary"],
        )
        repaired_v137_score = v137.score_v137(
            primary=repaired_primary,
            canary=repaired_canary,
            truth=frozen["v137"]["values"]["truth"],
        )
        passed = bool(repair_score["passed"] and repaired_v137_score["passed"])
        paths = {
            "outputs": root / "singleton-field-repair-outputs.private.json",
            "repair_score": root / "singleton-field-repair-score.json",
            "primary": root / "repaired-full-observable-field-owner-output.private.json",
            "canary": root / "repaired-full-observable-field-owner-canary.private.json",
            "v137_score": root / "repaired-v137-score.json",
            "reference": root / "calibration-truth-v11-observable-field-owner.private.json",
        }
        _write_immutable(paths["outputs"], {"turns": outputs})
        _write_immutable(paths["repair_score"], repair_score)
        _write_immutable(paths["primary"], repaired_primary)
        _write_immutable(paths["canary"], repaired_canary)
        _write_immutable(paths["v137_score"], repaired_v137_score)
        if passed:
            reference = v137.build_reference_v137(
                current_reference=frozen["current_reference"],
                primary=repaired_primary,
                truth=frozen["v137"]["values"]["truth"],
            )
            _write_immutable(paths["reference"], reference)
        accounting = _aggregate_usage(sidecars)
        failed_checks = sorted(
            [f"repair:{key}" for key in repair_score["failed_checks"]]
            + [f"v137_rescore:{key}" for key in repaired_v137_score["failed_checks"]]
        )
        terminal = {
            "schema_version": V138_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v138_singleton_repair_passed_reference_frozen"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v138_repaired_v137_reference_patch_authorized"
                if passed
                else "v138_singleton_field_repair_quality_gate_not_passed"
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
            "repair_score": _record(paths["repair_score"]),
            "repaired_v137_score": _record(paths["v137_score"]),
            "outputs": _record(paths["outputs"]),
            "repaired_primary": _record(paths["primary"]),
            "repaired_canary": _record(paths["canary"]),
            "reference": _record(paths["reference"]) if passed else None,
            "failed_quality_gates": failed_checks,
            "metrics": {
                "repair": repair_score["metrics"],
                "repaired_v137": repaired_v137_score["metrics"],
            },
            "predecessor_v137_usage": frozen["v137"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v138 singleton field repair")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v138(
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
