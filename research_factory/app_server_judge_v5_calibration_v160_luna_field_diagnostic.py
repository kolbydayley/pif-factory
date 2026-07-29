from __future__ import annotations

"""Fresh Luna field-only diagnostic after the immutable v159 quality failure."""

import argparse
import asyncio
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from . import app_server_judge_v5_calibration_v149_fresh_corrected_field_diagnostic as v149
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v159_replacement_model_diagnostic as v159
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    output_schema as field_output_schema,
    validate_output as validate_field_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V160_SPEC_VERSION = "pif_app_server_judge_v5_4_v160_spec_v1"
V160_SCORE_VERSION = "pif_app_server_judge_v5_4_v160_score_v1"
V160_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v160_luna_field_protocol_v1"
V160_FAILURE_VERSION = "pif_app_server_judge_v5_4_v160_failure_v1"
V160_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v160_terminal_v1"
V160_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V160_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V160_PHASE_ID = "judge_v5_4_v160_luna_field_diagnostic"

FIELD_MODEL = "gpt-5.6-luna"
EFFORT = "high"
TURN_NAMES = tuple(f"luna_field_singleton_{index:02d}" for index in range(12))
MAXIMUM_TOTAL_TOKENS_PER_TURN = 45000
TIMEOUT_SECONDS = v159.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v159.DEFAULT_OUTPUT_ROOT.parent / "judge-calibration-v5_4-v160-luna-field-diagnostic"
).resolve()


class JudgeV5CalibrationV160Error(RuntimeError):
    """The immutable v160 field diagnostic contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v159() -> dict[str, Any]:
    root = v159.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "score": root / "replacement-model-diagnostic-score.json",
        "spec": root / "replacement-model-diagnostic-spec.json",
        "field_output": root / "field-output.private.json",
    }
    values = {name: _load_json(path, f"v159 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    sidecars = []
    for row in attempts:
        if not isinstance(row.get("sidecar"), Mapping):
            raise JudgeV5CalibrationV160Error("v159 measured sidecar coverage drifted")
        _verify_record(row["sidecar"])
        measured = _validate_usage(
            _load_json(Path(row["sidecar"]["path"]), "v159 measured sidecar")
        )
        if measured["total_tokens"] <= 0:
            raise JudgeV5CalibrationV160Error("v159 measured usage drifted")
        sidecars.append(row["sidecar"])
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v159_replacement_model_diagnostic_quality_gate_not_passed"
        or terminal.get("replacement_diagnostic_passed") is not False
        or terminal.get("fresh_full_replacement_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 419537
        or terminal.get("cumulative_calibration_usage", {}).get("total_tokens")
        != 1926427
        or score.get("passed") is not False
        or score.get("metrics", {}).get("field_residual_exact_count") != 1
        or score.get("metrics", {}).get("field_control_exact_count") != 5
        or len(attempts) != 16
        or len(sidecars) != 16
        or spec.get("maximum_turn_count") != 16
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5CalibrationV160Error("v159 quality terminal drifted")
    for record in spec["runtime_files"]:
        _verify_record(record)
    for turn in spec["frozen_inputs"]["turns"]:
        for key in ("input", "prompt", "schema"):
            _verify_record(turn[key])
    source = v159._validate_v158()
    data = v159.build_v159_inputs(source)
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "sidecars": sidecars,
        "source": source,
        "data": data,
        "cumulative_usage": terminal["cumulative_calibration_usage"],
    }


def build_v160_inputs(source: Mapping[str, Any]) -> dict[str, Any]:
    old_turns = {
        row["input"]["path"]: row
        for row in source["values"]["spec"]["frozen_inputs"]["turns"]
        if row["role"] == "field_singleton"
    }
    values_by_task = {}
    for path, record_row in old_turns.items():
        _verify_record(record_row["input"])
        value = _load_json(Path(path), "v159 field input")
        tasks = value.get("tasks")
        if not isinstance(tasks, list) or len(tasks) != 1:
            raise JudgeV5CalibrationV160Error("v159 singleton field input drifted")
        values_by_task[str(tasks[0]["task_id"])] = value
    selected = source["data"]["selected_field_ids"]
    turns = []
    for turn_name, task_id in zip(TURN_NAMES, selected, strict=True):
        expected = next(
            row["value"]
            for row in source["data"]["field_turns"]
            if row["task_id"] == task_id
        )
        value = values_by_task.get(task_id)
        if value != expected:
            raise JudgeV5CalibrationV160Error("v159 field input reuse drifted")
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "field_singleton",
                "task_id": task_id,
                "value": value,
            }
        )
    if len(turns) != 12 or len(values_by_task) != 12:
        raise JudgeV5CalibrationV160Error("v160 field coverage drifted")
    return {
        "turns": turns,
        "selected_field_ids": list(selected),
        "truth": source["data"]["truth"],
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    audit = {
        "schema_version": V160_CAPACITY_AUDIT_VERSION,
        "phase_id": V160_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V160_CAPACITY_POLICY_VERSION,
        "phase_id": V160_PHASE_ID,
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
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": math.ceil(
            bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v160(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v160 terminal")}
    source = _validate_v159()
    data = build_v160_inputs(source)
    turns = []
    for row in data["turns"]:
        prompt = v149.field_prompt_v149(row["value"])
        schema = field_output_schema(row["value"])
        paths = _freeze_turn_request(
            root=root,
            turn_name=row["turn_name"],
            input_value=row["value"],
            prompt=prompt,
            schema=schema,
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor = {
        **{f"v159_{name}": record for name, record in source["records"].items()},
        "v159_sidecars": source["sidecars"],
        "cumulative_usage": source["cumulative_usage"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    inherited_runtime = source["values"]["spec"]["runtime_files"]
    spec = {
        "schema_version": V160_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "field_model": FIELD_MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_luna_singleton_field_diagnostic_six_residuals_plus_six_controls",
        "turn_count": len(TURN_NAMES),
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": len(TURN_NAMES),
        "maximum_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "truth_labels_exposed_to_model": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [_record(Path(__file__)), *inherited_runtime],
        "frozen_instructions": {
            "field_sha256": sha256_text(v146.base_instructions_v146())
        },
        "frozen_inputs": {
            "truth": source["values"]["spec"]["frozen_inputs"]["truth"],
            "v159_spec": source["records"]["spec"],
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "role": turn["turn_role"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_truth_and_mapping_sanitized_terminal_only",
    }
    spec_path = root / "luna-field-diagnostic-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "data": data,
        "source": source,
    }


def score_v160(
    *, field_output: Mapping[str, Any], data: Mapping[str, Any], source: Mapping[str, Any]
) -> dict[str, Any]:
    truth = {str(row["task_id"]): row for row in data["truth"]["field_tasks"]}
    observed = {str(row["task_id"]): row for row in field_output["decisions"]}
    selected = set(data["selected_field_ids"])
    if set(observed) != selected:
        raise JudgeV5CalibrationV160Error("v160 field score coverage drifted")
    exact = {
        task_id: observed[task_id]["field_status"] == truth[task_id]["expected_status"]
        for task_id in selected
    }
    diagnostic = source["source"]["values"]["diagnostic"]
    residuals = set(diagnostic["field_error_task_ids"])
    controls = set(diagnostic["field_control_task_ids"])
    metrics = {
        "field_overall_exact_count": sum(exact.values()),
        "field_decision_count": len(exact),
        "field_residual_exact_count": sum(exact[key] for key in residuals),
        "field_residual_count": len(residuals),
        "field_control_exact_count": sum(exact[key] for key in controls),
        "field_control_count": len(controls),
        "field_abstention_count": sum(
            observed[key]["field_status"] == "abstain" for key in selected
        ),
    }
    checks = {
        "field_overall_exact_count": metrics["field_overall_exact_count"] >= 11,
        "field_residual_exact_count": metrics["field_residual_exact_count"] >= 5,
        "field_control_exact_count": metrics["field_control_exact_count"] == 6,
        "field_abstention_count": metrics["field_abstention_count"] == 0,
    }
    passed = all(checks.values())
    return {
        "schema_version": V160_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": metrics,
        "bounded_alignment_verifier_diagnostic_authorized": passed,
        "fresh_full_replacement_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
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
                _load_json(Path(record["path"]), "v160 measured sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    predecessor = _validate_v159()["cumulative_usage"]
    cumulative = _sum_usage(predecessor, usage)
    failure = {
        "schema_version": V160_FAILURE_VERSION,
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
        "predecessor_cumulative_usage": predecessor,
        "cumulative_known_usage_lower_bound": cumulative,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V160_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "luna_field_diagnostic_passed": False,
        "bounded_alignment_verifier_diagnostic_authorized": False,
        "fresh_full_replacement_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "predecessor_cumulative_usage": predecessor,
        "cumulative_known_usage_lower_bound": cumulative,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v160(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v160 terminal")
    frozen = freeze_v160(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    sidecars = []
    outputs = []
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v146.base_instructions_v146(),
                    model=FIELD_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=1,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_field_output(
                        candidate, item
                    ),
                )
                outputs.append(output)
                sidecars.append(sidecar)
        field_output = v155._merge_outputs(outputs, "decisions")
        _write_immutable(root / "field-output.private.json", field_output)
        score = score_v160(
            field_output=field_output, data=frozen["data"], source=frozen["source"]
        )
        score_path = root / "luna-field-diagnostic-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        protocol_path = root / "luna-field-protocol-v160.json"
        if passed:
            _write_immutable(
                protocol_path,
                {
                    "schema_version": V160_PROTOCOL_VERSION,
                    "frozen_at": now_iso(),
                    "field_model": FIELD_MODEL,
                    "reasoning_effort": EFFORT,
                    "field_context_mode": "singleton",
                    "field_instructions_sha256": sha256_text(
                        v146.base_instructions_v146()
                    ),
                    "truth_labels_exposed_to_model": False,
                    "quality_gates_unchanged": True,
                    "bounded_alignment_verifier_diagnostic_authorized": True,
                    "fresh_full_replacement_calibration_authorized": False,
                    "selection_authorized": False,
                    "holdout_authorized": False,
                    "production_mutation_allowed": False,
                },
            )
        accounting = _aggregate_usage(sidecars)
        predecessor = frozen["source"]["cumulative_usage"]
        cumulative = _sum_usage(predecessor, accounting["usage"])
        terminal = {
            "schema_version": V160_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": "v160_luna_field_diagnostic_passed_alignment_verifier_authorized"
            if passed
            else "inactive_incomplete_recovery_required",
            "development_terminal_reason": "v160_luna_field_diagnostic_passed"
            if passed
            else "v160_luna_field_diagnostic_quality_gate_not_passed",
            "overall_evaluation_complete": False,
            "luna_field_diagnostic_passed": passed,
            "bounded_alignment_verifier_diagnostic_authorized": passed,
            "fresh_full_replacement_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "score": _record(score_path),
            "protocol": _record(protocol_path) if passed else None,
            "predecessor_cumulative_usage": predecessor,
            "cumulative_calibration_usage": cumulative,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v160 Luna field diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v160(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "luna_field_diagnostic_passed": terminal.get(
                    "luna_field_diagnostic_passed", False
                ),
                "bounded_alignment_verifier_diagnostic_authorized": terminal.get(
                    "bounded_alignment_verifier_diagnostic_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
