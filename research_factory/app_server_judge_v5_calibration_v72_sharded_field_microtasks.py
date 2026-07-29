from __future__ import annotations

"""Sharded recovery for the immutable v71 field-microtask timeout."""

import argparse
import asyncio
import json
import math
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
from .app_server_judge_v5_calibration_v71_field_microtasks import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V71_ROOT,
    EFFORT,
    MODEL,
    assemble_checklist,
    base_instructions,
    build_prompt,
    output_schema,
    project_output,
    score_v71,
    validate_output,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso


V72_SPEC_VERSION = "pif_app_server_judge_v5_4_v72_sharded_field_microtask_spec_v1"
V72_AUDIT_VERSION = "pif_app_server_judge_v5_4_v72_projection_audit_v1"
V72_FAILURE_VERSION = "pif_app_server_judge_v5_4_v72_failure_v1"
V72_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v72_terminal_v1"
V72_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V72_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V72_PHASE_ID = "judge_v5_4_v72_sharded_field_microtask_diagnostic"
TURN_NAMES = tuple(f"field_microtask_shard_{index:02d}" for index in range(6))
TASKS_PER_SHARD = 15
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V71_ROOT.parent / "judge-calibration-v5_4-v72-sharded-field-microtasks"
).resolve()


class JudgeV5CalibrationV72Error(RuntimeError):
    """The v72 recovery cannot preserve its frozen predecessor or shard contract."""


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


def _validate_v71(v71_root: Path) -> dict[str, Any]:
    paths = {
        "v71_terminal": v71_root / "terminal.json",
        "v71_failure": v71_root / "failure.json",
        "v71_spec": v71_root / "field-microtask-spec.json",
        "v71_input": v71_root / "field-microtask-input.private.json",
        "v71_truth": v71_root / "diagnostic-truth.private.json",
        "v71_roles": v71_root / "cohort-roles.json",
        "v71_capacity": v71_root / "turns/field-microtask-diagnostic/capacity.json",
        "v71_sidecar": v71_root / "turns/field-microtask-diagnostic/sidecar.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v71_terminal"]
    failure = values["v71_failure"]
    spec = values["v71_spec"]
    sidecar = values["v71_sidecar"]
    attempts = failure.get("attempts") or []
    output_path = v71_root / "turns/field-microtask-diagnostic/output.private.json"
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("overall_evaluation_complete") is not False
        or terminal.get("usage_status") != "unknown"
        or terminal.get("accounting_complete") is not False
        or terminal.get("fresh_12_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or not _record_matches(terminal.get("failure"), paths["v71_failure"])
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != "field_microtask_diagnostic"
        or failure.get("error_class") != "AppServerTurnTimeout"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("usage_status") != "unknown"
        or failure.get("accounting_complete") is not False
        or len(attempts) != 1
        or not _record_matches(attempts[0].get("capacity"), paths["v71_capacity"])
        or not _record_matches(attempts[0].get("sidecar"), paths["v71_sidecar"])
        or attempts[0].get("output") is not None
        or output_path.exists()
        or spec.get("model") != MODEL
        or spec.get("reasoning_effort") != EFFORT
        or spec.get("task_count") != 90
        or spec.get("retry_count_per_turn") != 0
        or not all(_verify_record(record) for record in spec.get("runtime_files") or [])
        or sidecar.get("state") != "interrupted"
        or sidecar.get("status") != "timeout"
        or sidecar.get("error_class") != "turn_timeout"
        or sidecar.get("usage_status") != "unknown"
        or sidecar.get("usage_complete") is not False
        or sidecar.get("usage") is not None
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("batch_size") != 90
        or sidecar.get("thread_mode") != "new_thread"
        or sidecar.get("recovery_reran_model") is not False
        or float(sidecar.get("wall_elapsed_seconds") or 0.0) < 1190.0
    ):
        raise JudgeV5CalibrationV72Error("v71 predecessor is inadmissible")
    return {name: _record(path) for name, path in paths.items()}


def build_v72_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    units = value.get("units")
    tasks = value.get("tasks")
    if not isinstance(units, list) or len(units) != 6 or not isinstance(tasks, list):
        raise JudgeV5CalibrationV72Error("v71 input shape drifted")
    shards = []
    seen = set()
    for index, unit in enumerate(units):
        identity = (unit["case_id"], unit["witness_id"])
        shard_tasks = [
            deepcopy(task)
            for task in tasks
            if (task["case_id"], task["witness_id"]) == identity
        ]
        if len(shard_tasks) != TASKS_PER_SHARD:
            raise JudgeV5CalibrationV72Error("v72 shard task count drifted")
        task_ids = {task["task_id"] for task in shard_tasks}
        if seen & task_ids:
            raise JudgeV5CalibrationV72Error("v72 shards overlap")
        seen.update(task_ids)
        shards.append(
            {
                "schema_version": value["schema_version"],
                "units": [deepcopy(unit)],
                "field_contracts": deepcopy(value["field_contracts"]),
                "tasks": shard_tasks,
                "task_count": TASKS_PER_SHARD,
                "prior_labels_present": False,
                "candidate_outputs_present": False,
                "system_identity_present": False,
                "tasks_are_independent": True,
                "shard_ordinal": index,
                "shard_count": len(units),
            }
        )
    if len(seen) != 90 or seen != {task["task_id"] for task in tasks}:
        raise JudgeV5CalibrationV72Error("v72 shard coverage drifted")
    return shards


def merge_shard_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    identities = [row.get("task_id") for row in decisions]
    if len(decisions) != 90 or len(set(identities)) != 90:
        raise JudgeV5CalibrationV72Error("v72 output coverage drifted")
    return {"decisions": decisions}


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    phase_bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V72_CAPACITY_AUDIT_VERSION,
        "phase_id": V72_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": phase_bound,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v72 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV72Error("immutable v72 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V72_CAPACITY_POLICY_VERSION,
        "phase_id": V72_PHASE_ID,
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
        "phase_total_token_bound": phase_bound,
        "projected_phase_quota_points": math.ceil(
            phase_bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    if policy_path.exists():
        prior = _load_json(policy_path, "v72 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV72Error("immutable v72 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v72(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v71_root: Path = DEFAULT_V71_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v71(v71_root.resolve())
    value = _load_json(v71_root / "field-microtask-input.private.json", "v71 input")
    truth = _load_json(v71_root / "diagnostic-truth.private.json", "v71 truth")
    roles = _load_json(v71_root / "cohort-roles.json", "v71 roles")
    shards = build_v72_shards(value)
    input_path = root / "field-microtask-input.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    roles_path = root / "cohort-roles.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(roles_path, roles)
    turn_records = []
    turn_values = []
    for turn_name, shard in zip(TURN_NAMES, shards, strict=True):
        prompt = build_prompt(shard)
        schema = output_schema(shard)
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=shard,
            prompt=prompt,
            schema=schema,
        )
        turn_values.append(
            {
                "turn_name": turn_name,
                "value": shard,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
        turn_records.append(
            {
                "turn_name": turn_name,
                "input": _record(paths["input"]),
                "prompt": _record(paths["prompt"]),
                "schema": _record(paths["schema"]),
            }
        )
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V72_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds_per_turn": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "six_one_witness_shards_over_identical_v71_microtasks",
        "witness_count": 6,
        "task_count": 90,
        "tasks_per_shard": TASKS_PER_SHARD,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "candidate_outputs_in_model_input": False,
        "retry_count_per_turn": 0,
        "v71_turn_replayed": False,
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
            _record(runtime_dir / "app_server_judge_v5_calibration_v71_field_microtasks.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "truth": _record(truth_path),
            "roles": _record(roles_path),
            "turns": turn_records,
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "sharded-field-microtask-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v72 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV72Error("immutable v72 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
        "truth": truth,
        "roles": roles,
        "turns": turn_values,
        "capacity_policy": capacity["policy"],
    }


def _failure_accounting(root: Path) -> dict[str, Any]:
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    attempts = _attempt_records(root)
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            usage = _validate_usage(_load_json(Path(record["path"]), "v72 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = unknown == 0 and bool(attempts)
    return {
        "attempts": attempts,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": known if complete else None,
        "unknown_usage_attempt_count": unknown,
    }


def _write_failure(root: Path, *, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    accounting = _failure_accounting(root)
    failure = {
        "schema_version": V72_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        **accounting,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V72_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "field_microtask_diagnostic_passed": False,
        "fresh_12_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": accounting["accounting_complete"],
        "usage_status": accounting["usage_status"],
        "usage": accounting["usage"],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v72(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v71_root: Path = DEFAULT_V71_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v72 terminal")
    frozen = freeze_v72(
        output_dir=root, v71_root=v71_root, timeout_seconds=timeout_seconds
    )
    factory = client_factory or _client_factory
    outputs = []
    sidecars = []
    adoptions = {}
    operations = []
    try:
        async with factory(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                value = turn["value"]
                output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=turn["turn_name"],
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=base_instructions(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=TASKS_PER_SHARD,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, shard=value: validate_output(
                        project_output(candidate, shard)[0], shard
                    ),
                )
                projected, shard_operations = project_output(output, value)
                errors = validate_output(projected, value)
                if errors:
                    raise JudgeV5CalibrationV72Error("projected v72 shard is invalid")
                outputs.append(projected)
                sidecars.append(sidecar)
                adoptions[turn["turn_name"]] = adopted
                operations.extend(shard_operations)
        merged = merge_shard_outputs(outputs)
        merged_path = root / "field-microtask-output.private.json"
        _write_immutable(merged_path, merged)
        checklist = assemble_checklist(merged, frozen["value"])
        checklist_path = root / "assembled-checklist.private.json"
        _write_immutable(checklist_path, checklist)
        audit = {
            "schema_version": V72_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "semantic_field_source": MODEL,
            "assembly_is_structural_only": True,
            "new_semantic_decisions_from_deterministic_code": False,
            "v71_turn_replayed": False,
            "privacy": "opaque_task_ids_enums_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v71(checklist, frozen["truth"], frozen["roles"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "field-microtask-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V72_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v72_sharded_field_microtask_passed_fresh_12_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v72_sharded_field_microtask_passed_fresh_12_authorized"
                if passed
                else "v72_sharded_field_microtask_quality_gate_not_passed"
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
            "output": _record(merged_path),
            "assembled_checklist": _record(checklist_path),
            "projection_audit": _record(audit_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": adoptions,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, turn_name=exc.turn_name, error_class=exc.error_class)
    except Exception as exc:
        turn_name = frozen["turns"][len(outputs)]["turn_name"] if len(outputs) < 6 else None
        return _write_failure(root, turn_name=turn_name, error_class=type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v72 sharded field microtasks")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v71-root", default=str(DEFAULT_V71_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v72(
            output_dir=Path(args.output_dir),
            v71_root=Path(args.v71_root),
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
