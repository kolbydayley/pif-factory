from __future__ import annotations

"""Independent owner audit for the two v88 failed control labels."""

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
from .app_server_judge_v5_calibration_v75_exact_span_remaining_shard import project_exact_spans
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v84_metric_target_reference_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V84_ROOT,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record_matches,
    _verify_record,
    _write_stable_created,
)
from .app_server_judge_v5_calibration_v88_residual_reference_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V88_ROOT,
    base_instructions_v88,
    build_prompt_v88,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _validate_usage,
)
from .util import now_iso, sha256_text


V89_INPUT_VERSION = "pif_app_server_judge_v5_4_v89_control_truth_input_v1"
V89_TRUTH_VERSION = "pif_app_server_judge_v5_4_v89_control_truth_truth_v1"
V89_SELECTION_VERSION = "pif_app_server_judge_v5_4_v89_selection_v1"
V89_SPEC_VERSION = "pif_app_server_judge_v5_4_v89_control_truth_spec_v1"
V89_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v89_control_truth_output_v1"
V89_SCORE_VERSION = "pif_app_server_judge_v5_4_v89_control_truth_score_v1"
V89_AUDIT_VERSION = "pif_app_server_judge_v5_4_v89_projection_audit_v1"
V89_FAILURE_VERSION = "pif_app_server_judge_v5_4_v89_failure_v1"
V89_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v89_terminal_v1"
V89_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V89_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V89_PHASE_ID = "judge_v5_4_v89_control_truth_audit"
MODEL = "gpt-5.5"
EFFORT = "high"
PRIMARY_TURNS = ("control_truth_shard_00", "control_truth_shard_01")
CANARY_TURN = "control_truth_permutation_canary"
TURN_NAMES = PRIMARY_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V88_ROOT.parent / "judge-calibration-v5_4-v89-control-truth-audit"
).resolve()


class JudgeV5CalibrationV89Error(RuntimeError):
    """The v89 control-truth audit cannot preserve its contract."""


def _validate_predecessors(*, v88_root: Path, v84_root: Path) -> dict[str, Any]:
    paths = {
        "v88_terminal": v88_root / "terminal.json",
        "v88_spec": v88_root / "residual-reference-spec.json",
        "v88_score": v88_root / "residual-reference-score.json",
        "v88_input": v88_root / "residual-reference-input.private.json",
        "v88_truth": v88_root / "residual-reference-truth.private.json",
        "v88_output": v88_root / "residual-reference-output.private.json",
        "v88_canary_output": v88_root / "permutation-canary-output.private.json",
        "v84_terminal": v84_root / "terminal.json",
        "v84_spec": v84_root / "metric-target-spec.json",
        "v84_input": v84_root / "metric-target-input.private.json",
        "v84_truth": v84_root / "metric-target-truth.private.json",
        "v84_output": v84_root / "metric-target-output.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v88_terminal"]
    v88_spec = values["v88_spec"]
    v88_score = values["v88_score"]
    v84_terminal = values["v84_terminal"]
    v84_spec = values["v84_spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v88_residual_reference_quality_gate_not_passed"
        or terminal.get("reference_patch_authorized") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or v88_score.get("failed_checks") != ["matched_control_exact_rate"]
        or v88_score.get("metrics", {}).get("matched_control_exact_count") != 4
        or v88_score.get("metrics", {}).get("permutation_canary_exact_count") != 3
        or v88_score.get("metrics", {}).get("abstention_count") != 0
        or not _record_matches(terminal.get("score"), paths["v88_score"])
        or not _record_matches(terminal.get("output"), paths["v88_output"])
        or not _record_matches(terminal.get("canary_output"), paths["v88_canary_output"])
        or not _record_matches(v88_spec.get("frozen_inputs", {}).get("input"), paths["v88_input"])
        or not _record_matches(v88_spec.get("frozen_inputs", {}).get("truth"), paths["v88_truth"])
        or not all(_verify_record(row) for row in v88_spec.get("runtime_files") or [])
        or v84_terminal.get("state") != "completed"
        or v84_terminal.get("reference_freeze_authorized") is not True
        or v84_terminal.get("usage_status") != "complete"
        or v84_terminal.get("production_mutated") is not False
        or not _record_matches(v84_spec.get("frozen_inputs", {}).get("input"), paths["v84_input"])
        or not _record_matches(v84_spec.get("frozen_inputs", {}).get("truth"), paths["v84_truth"])
        or not all(_verify_record(row) for row in v84_spec.get("runtime_files") or [])
    ):
        raise JudgeV5CalibrationV89Error("v84/v88 control-truth contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def _task_id(source_task_id: str, role: str) -> str:
    return "truth_" + sha256_text(f"v89|{role}|{source_task_id}")[:24]


def _canary_task_id(owner_task_id: str) -> str:
    return "perm_" + sha256_text(f"v89|canary|{owner_task_id}")[:24]


def build_v89_inputs(
    *,
    v88_input: Mapping[str, Any],
    v88_truth: Mapping[str, Any],
    v88_output: Mapping[str, Any],
    v84_input: Mapping[str, Any],
    v84_truth: Mapping[str, Any],
    v84_output: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    v88_task = {row["task_id"]: row for row in v88_input.get("tasks") or []}
    v88_observed = {row["task_id"]: row for row in v88_output.get("decisions") or []}
    failed_controls = [
        row
        for row in v88_truth.get("tasks") or []
        if row.get("role") == "matched_control"
        and v88_observed[row["task_id"]]["field_status"] != row["control_expected_status"]
    ]
    if len(failed_controls) != 2 or sorted(row["field"] for row in failed_controls) != [
        "certainty",
        "target",
    ]:
        raise JudgeV5CalibrationV89Error("v89 failed-control coverage drifted")
    selected: list[tuple[str, Mapping[str, Any], Mapping[str, Any], str]] = []
    for row in sorted(failed_controls, key=lambda child: child["task_id"]):
        selected.append(("truth_challenge", row, v88_task[row["task_id"]], row["prior_status"]))

    v84_task = {row["task_id"]: row for row in v84_input.get("tasks") or []}
    v84_observed = {row["task_id"]: row for row in v84_output.get("decisions") or []}
    target_controls = [
        row
        for row in v84_truth.get("tasks") or []
        if row.get("role") == "matched_control" and row.get("field") == "target"
    ]
    if len(target_controls) != 2 or any(
        v84_observed[row["task_id"]]["field_status"] != row["control_expected_status"]
        for row in target_controls
    ):
        raise JudgeV5CalibrationV89Error("v84 settled target controls drifted")
    for row in sorted(target_controls, key=lambda child: child["task_id"]):
        selected.append(("settled_control", row, v84_task[row["task_id"]], row["control_expected_status"]))

    passed_v88_controls = [
        row
        for row in v88_truth.get("tasks") or []
        if row.get("role") == "matched_control"
        and v88_observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        and (
            (row["field"] == "certainty" and row["control_expected_status"] == "incorrect")
            or (row["field"] == "temporal_horizon" and row["control_expected_status"] == "correct")
        )
    ]
    if len(passed_v88_controls) != 2:
        raise JudgeV5CalibrationV89Error("v88 settled control coverage drifted")
    for row in sorted(passed_v88_controls, key=lambda child: child["task_id"]):
        selected.append(("settled_control", row, v88_task[row["task_id"]], row["control_expected_status"]))

    tasks = []
    truth_rows = []
    for role, source_truth, source_task, status in selected:
        task_id = _task_id(source_truth["task_id"], role)
        task = deepcopy(source_task)
        task["task_id"] = task_id
        tasks.append(task)
        truth_rows.append(
            {
                "task_id": task_id,
                "role": role,
                "case_id": source_truth["case_id"],
                "witness_id": source_truth["witness_id"],
                "field": source_truth["field"],
                "prior_status": status,
                "control_expected_status": status if role == "settled_control" else None,
            }
        )
    if len(tasks) != 6 or len({row["task_id"] for row in tasks}) != 6:
        raise JudgeV5CalibrationV89Error("v89 task coverage drifted")
    tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    task_by_id = {row["task_id"]: row for row in tasks}
    challenges = [row["task_id"] for row in truth_rows if row["role"] == "truth_challenge"]
    canary_tasks = []
    canary_map = []
    for owner_task_id in reversed(sorted(challenges)):
        task = deepcopy(task_by_id[owner_task_id])
        canary_task_id = _canary_task_id(owner_task_id)
        task["task_id"] = canary_task_id
        canary_tasks.append(task)
        canary_map.append({"canary_task_id": canary_task_id, "owner_task_id": owner_task_id})
    value = {
        "schema_version": V89_INPUT_VERSION,
        "task_count": 6,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth = {
        "schema_version": V89_TRUTH_VERSION,
        "task_count": 6,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V89_INPUT_VERSION,
        "task_count": 2,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V89_SELECTION_VERSION,
        "created_at": now_iso(),
        "task_count": 6,
        "role_counts": {"truth_challenge": 2, "settled_control": 4},
        "field_counts": dict(sorted(Counter(row["field"] for row in truth_rows).items())),
        "permutation_canary_count": 2,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return value, truth, canary, selection


def primary_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value.get("tasks") or []
    if len(tasks) != 6:
        raise JudgeV5CalibrationV89Error("v89 shard coverage drifted")
    return [
        {
            **{key: deepcopy(child) for key, child in value.items() if key != "tasks"},
            "task_count": 3,
            "tasks": deepcopy(tasks[index : index + 3]),
            "shard_ordinal": index // 3,
            "shard_count": 2,
        }
        for index in range(0, 6, 3)
    ]


def merge_outputs(outputs: Sequence[Mapping[str, Any]], expected_count: int) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(decisions) != expected_count or len({row["task_id"] for row in decisions}) != expected_count:
        raise JudgeV5CalibrationV89Error("v89 output coverage drifted")
    return {"schema_version": V89_OUTPUT_VERSION, "decisions": decisions}


def score_v89(
    owner: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in owner["decisions"]}
    repeated = {row["task_id"]: row for row in canary["decisions"]}
    if set(expected) != set(observed) or len(repeated) != 2:
        raise JudgeV5CalibrationV89Error("v89 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "settled_control"]
    challenges = [row for row in expected.values() if row["role"] == "truth_challenge"]
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    canary_exact = sum(
        repeated[row["canary_task_id"]]["field_status"]
        == observed[row["owner_task_id"]]["field_status"]
        for row in truth["canary_map"]
    )
    abstentions = sum(row["field_status"] == "abstain" for row in observed.values())
    abstentions += sum(row["field_status"] == "abstain" for row in repeated.values())
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    evidence_complete += sum(bool(row["source_evidence_spans"]) for row in repeated.values())
    checks = {
        "settled_control_exact_rate": control_exact == 4,
        "permutation_canary_exact_rate": canary_exact == 2,
        "abstention_count": abstentions == 0,
        "evidence_complete_rate": evidence_complete == 8,
    }
    passed = all(checks.values())
    proposal = [
        {
            "case_id": row["case_id"],
            "witness_id": row["witness_id"],
            "field": row["field"],
            "prior_status": row["prior_status"],
            "owner_status": observed[row["task_id"]]["field_status"],
            "reference_change": observed[row["task_id"]]["field_status"] != row["prior_status"],
        }
        for row in challenges
    ]
    return {
        "schema_version": V89_SCORE_VERSION,
        "passed": passed,
        "metrics": {
            "task_count": 6,
            "truth_challenge_count": 2,
            "settled_control_count": 4,
            "settled_control_exact_count": control_exact,
            "settled_control_exact_rate": round(control_exact / 4, 6),
            "permutation_canary_count": 2,
            "permutation_canary_exact_count": canary_exact,
            "permutation_canary_exact_rate": round(canary_exact / 2, 6),
            "abstention_count": abstentions,
            "evidence_complete_count": evidence_complete,
            "evidence_complete_rate": round(evidence_complete / 8, 6),
            "reference_change_count": sum(row["reference_change"] for row in proposal),
        },
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "control_truth_patch_proposal": proposal if passed else [],
        "v88_reconciliation_authorized": passed,
        "reference_freeze_authorized": False,
        "fresh_diagnostic_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "gates_frozen_before_semantic_calls": True,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V89_CAPACITY_AUDIT_VERSION,
        "phase_id": V89_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_created(audit_path, audit, "v89 capacity audit")
    policy = {
        "schema_version": V89_CAPACITY_POLICY_VERSION,
        "phase_id": V89_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v89 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v89(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v88_root: Path = DEFAULT_V88_ROOT,
    v84_root: Path = DEFAULT_V84_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors(v88_root=v88_root.resolve(), v84_root=v84_root.resolve())
    values = predecessor["values"]
    value, truth, canary, selection = build_v89_inputs(
        v88_input=values["v88_input"],
        v88_truth=values["v88_truth"],
        v88_output=values["v88_output"],
        v84_input=values["v84_input"],
        v84_truth=values["v84_truth"],
        v84_output=values["v84_output"],
    )
    input_path = root / "control-truth-input.private.json"
    truth_path = root / "control-truth-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_created(selection_path, selection, "v89 selection audit")
    turn_values = primary_shards(value) + [canary]
    turns = []
    for turn_name, turn_value in zip(TURN_NAMES, turn_values, strict=True):
        prompt = build_prompt_v88(turn_value)
        schema = output_schema(turn_value)
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=turn_value, prompt=prompt, schema=schema
        )
        turns.append(
            {
                "turn_name": turn_name,
                "value": turn_value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    capacity = _build_capacity_policy(root, predecessor["records"])
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V89_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "independent_control_truth_owner_with_settled_controls_and_permutation_canaries",
        "task_count": 6,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_four_settled_controls_both_canaries_all_eight_evidence_receipts_and_zero_abstentions",
        "v88_reconciliation_authorized": False,
        "reference_freeze_authorized": False,
        "fresh_diagnostic_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v88_residual_reference_audit.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v84_metric_target_reference_audit.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v75_exact_span_remaining_shard.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "truth": _record(truth_path),
            "canary": _record(canary_path),
            "selection_audit": _record(selection_path),
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
    spec_path = root / "control-truth-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v89 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV89Error("immutable v89 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "truth": truth,
        "turns": turns,
        "capacity_policy": capacity["policy"],
    }


def _real_attempts(root: Path) -> list[dict[str, Any]]:
    return [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = _real_attempts(root)
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v89 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V89_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "attempts": attempts,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V89_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "v88_reconciliation_authorized": False,
        "reference_freeze_authorized": False,
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


async def run_v89(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v89 terminal")
    frozen = freeze_v89(output_dir=root, timeout_seconds=timeout_seconds)
    factory = client_factory or _client_factory
    primary_outputs = []
    canary_outputs = []
    sidecars = []
    operations = []
    adoptions = {}
    current_turn = None
    try:
        async with factory(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=base_instructions_v88(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(turn["value"]["tasks"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, value=turn["value"]: validate_output(
                        project_exact_spans(candidate, value)[0], value
                    ),
                )
                projected, turn_operations = project_exact_spans(output, turn["value"])
                if validate_output(projected, turn["value"]):
                    raise JudgeV5CalibrationV89Error("projected v89 output is invalid")
                if current_turn == CANARY_TURN:
                    canary_outputs.append(projected)
                else:
                    primary_outputs.append(projected)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
                operations.extend([{**row, "turn_name": current_turn} for row in turn_operations])
        owner = merge_outputs(primary_outputs, 6)
        canary = merge_outputs(canary_outputs, 2)
        owner_path = root / "control-truth-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(owner_path, owner)
        _write_immutable(canary_path, canary)
        audit = {
            "schema_version": V89_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "projection_scope": "nonexact_source_span_removal_only",
            "semantic_status_changed": False,
            "privacy": "opaque_task_ids_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v89(owner, canary, frozen["truth"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "control-truth-score.json"
        _write_immutable(score_path, score)
        proposal_path = root / "control-truth-patch-proposal.json"
        if score["passed"]:
            _write_immutable(
                proposal_path,
                {
                    "schema_version": "pif_app_server_judge_v5_4_v89_control_truth_patch_v1",
                    "created_at": now_iso(),
                    "changes": score["control_truth_patch_proposal"],
                    "reference_freeze_authorized": False,
                    "v88_reconciliation_authorized": True,
                },
            )
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V89_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v89_control_truth_audit_passed_v88_reconciliation_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v89_control_truth_audit_passed_v88_reconciliation_authorized"
                if passed
                else "v89_control_truth_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "v88_reconciliation_authorized": passed,
            "reference_freeze_authorized": False,
            "fresh_diagnostic_authorized": False,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(owner_path),
            "canary_output": _record(canary_path),
            "patch_proposal": _record(proposal_path) if passed else None,
            "projection_audit": _record(audit_path),
            "attempts": _real_attempts(root),
            "completed_checkpoint_adoptions": adoptions,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v89 control-truth audit")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v89(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "v88_reconciliation_authorized": terminal.get(
                    "v88_reconciliation_authorized", False
                ),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
