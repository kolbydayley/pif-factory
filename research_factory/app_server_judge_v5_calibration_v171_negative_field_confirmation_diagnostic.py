from __future__ import annotations

"""Diagnose a side-free confirmation pass for every negative field decision."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from . import app_server_judge_v5_calibration_v149_fresh_corrected_field_diagnostic as v149
from . import app_server_judge_v5_calibration_v168_capacity_recovery as v168
from . import app_server_judge_v5_calibration_v170_verifier_continuation as v170
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


V171_SPEC_VERSION = "pif_app_server_judge_v5_4_v171_spec_v1"
V171_SCORE_VERSION = "pif_app_server_judge_v5_4_v171_score_v1"
V171_FAILURE_VERSION = "pif_app_server_judge_v5_4_v171_failure_v1"
V171_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v171_terminal_v1"
V171_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V171_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V171_PHASE_ID = "judge_v5_4_v171_negative_field_confirmation_diagnostic"

PRIMARY_TURNS = tuple(f"negative_confirmation_primary_{index:02d}" for index in range(12))
REPEAT_TURNS = tuple(f"negative_confirmation_repeat_{index:02d}" for index in range(4))
TURN_NAMES = PRIMARY_TURNS + REPEAT_TURNS
MODEL = "gpt-5.4"
EFFORT = "high"
MAXIMUM_TOTAL_TOKENS_PER_TURN = 28_000
TIMEOUT_SECONDS = v170.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v170.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v171-negative-field-confirmation-diagnostic"
).resolve()


class JudgeV5CalibrationV171Error(RuntimeError):
    """The immutable v171 diagnostic contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v170_quality_terminal() -> dict[str, Any]:
    root = v170.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "score": root / "fresh-full-verifier-continuation-score.json",
        "spec": root / "verifier-continuation-spec.json",
        "policy": root / "capacity-policy.json",
        "audit": root / "capacity-policy-audit.json",
        "truth": root / "full-calibration-truth.private.json",
        "fields": root / "field-output.private.json",
        "field_repeats": root / "field-repeat-output.private.json",
        "support": root / "support-output-full.private.json",
        "final_base": root / "alignment-final-base.private.json",
        "final_canary": root / "alignment-final-canary.private.json",
    }
    values = {name: _load_json(path, f"v170 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    metrics = score.get("metrics") or {}
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v170_full_development_calibration_quality_gate_not_passed"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("development_judge_frozen") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("production_mutated") is not False
        or score.get("passed") is not False
        or score.get("failed_checks")
        != ["field_diagnostic_f1", "structured_field_accuracy"]
        or metrics.get("field_diagnostic_f1") != 0.933333
        or metrics.get("structured_field_accuracy") != 0.933333
        or metrics.get("support_sensitivity") != 0.994118
        or metrics.get("support_specificity") != 1.0
        or metrics.get("alignment_f1") != 0.993197
        or metrics.get("order_bias") != 0.0
        or terminal.get("usage", {}).get("total_tokens") != 467096
        or terminal.get("cumulative_calibration_usage", {}).get("total_tokens") != 4981094
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5CalibrationV171Error("v170 quality terminal contract drifted")
    attempts = _attempt_records(root)
    if len(attempts) != 15:
        raise JudgeV5CalibrationV171Error("v170 attempt coverage drifted")
    for attempt in attempts:
        for key in ("capacity", "sidecar", "output"):
            record = attempt.get(key)
            if not isinstance(record, Mapping):
                raise JudgeV5CalibrationV171Error(f"v170 {key} record is missing")
            _verify_record(record)
        _validate_usage(_load_json(Path(attempt["sidecar"]["path"]), "v170 sidecar"))
    for record in spec["runtime_files"]:
        _verify_record(record)
    for record in spec["frozen_inputs"]["turns"]:
        for key in ("input", "prompt", "schema"):
            _verify_record(record[key])
    for path in paths.values():
        if not path.is_file():
            raise JudgeV5CalibrationV171Error("v170 predecessor artifact disappeared")
    cumulative = terminal["cumulative_calibration_usage"]
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "cumulative_usage": {field: int(cumulative[field]) for field in USAGE_FIELDS},
    }


def _field_input_map() -> dict[str, dict[str, Any]]:
    mapping = {}
    for name in v168.FIELD_PRIMARY_TURNS:
        path = v168.DEFAULT_OUTPUT_ROOT / "turns" / name.replace("_", "-") / "input.private.json"
        value = _load_json(path, f"v168 {name} input")
        tasks = value.get("tasks")
        if not isinstance(tasks, list) or len(tasks) != 1:
            raise JudgeV5CalibrationV171Error("v168 singleton field input drifted")
        task_id = str(tasks[0]["task_id"])
        if task_id in mapping:
            raise JudgeV5CalibrationV171Error("v168 field task identity is duplicated")
        mapping[task_id] = value
    if len(mapping) != 28:
        raise JudgeV5CalibrationV171Error("v168 field input coverage drifted")
    return mapping


def build_v171_selection(source: Mapping[str, Any]) -> dict[str, Any]:
    truth = {str(row["task_id"]): row for row in source["values"]["truth"]["field_tasks"]}
    observed = {
        str(row["task_id"]): row
        for row in source["values"]["fields"]["decisions"]
    }
    negative = [
        {**deepcopy(truth[task_id]), "observed_status": row["field_status"]}
        for task_id, row in observed.items()
        if row["field_status"] == "incorrect"
    ]
    false_positives = sorted(
        (row for row in negative if row["expected_status"] == "correct"),
        key=lambda row: sha256_text(f"v171|target|{row['task_id']}"),
    )
    true_negatives = [row for row in negative if row["expected_status"] == "incorrect"]
    controls = []
    for field in sorted({str(row["field"]) for row in true_negatives}):
        candidates = [row for row in true_negatives if row["field"] == field]
        candidates.sort(key=lambda row: sha256_text(f"v171|control|{field}|{row['task_id']}"))
        controls.append(candidates[0])
    if (
        len(negative) != 15
        or len(false_positives) != 2
        or {row["field"] for row in false_positives} != {"certainty", "speaker"}
        or len(controls) != 10
        or {row["field"] for row in controls}
        != {
            "actor",
            "attribution",
            "causal_mechanism",
            "certainty",
            "event_boundary",
            "metric",
            "reported_actor",
            "speaker",
            "stance",
            "temporal_horizon",
        }
    ):
        raise JudgeV5CalibrationV171Error("v171 diagnostic selection topology drifted")
    primary = false_positives + controls
    primary.sort(key=lambda row: sha256_text(f"v171|primary-order|{row['task_id']}"))
    paired_fields = {"certainty", "speaker"}
    repeats = [row for row in primary if row["field"] in paired_fields]
    repeats.sort(key=lambda row: sha256_text(f"v171|repeat-order|{row['task_id']}"))
    if len(primary) != 12 or len(repeats) != 4:
        raise JudgeV5CalibrationV171Error("v171 primary or repeat coverage drifted")
    return {
        "negative_task_count": len(negative),
        "primary": primary,
        "repeats": repeats,
        "target_task_ids": sorted(row["task_id"] for row in false_positives),
        "control_task_ids": sorted(row["task_id"] for row in controls),
    }


def _build_capacity_policy(root: Path, source: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V171_CAPACITY_AUDIT_VERSION,
        "phase_id": V171_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v170_terminal": source["records"]["terminal"],
        "v170_score": source["records"]["score"],
        "measured_basis": {
            "prior_gpt54_singleton_turn_count": 6,
            "prior_gpt54_singleton_maximum_total_tokens": 22163,
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V171_CAPACITY_POLICY_VERSION,
        "phase_id": V171_PHASE_ID,
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
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v171(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v171 terminal")}
    source = _validate_v170_quality_terminal()
    selection = build_v171_selection(source)
    field_inputs = _field_input_map()
    rows = []
    for turn_name, row in zip(PRIMARY_TURNS, selection["primary"], strict=True):
        rows.append((turn_name, "primary", row))
    for turn_name, row in zip(REPEAT_TURNS, selection["repeats"], strict=True):
        rows.append((turn_name, "repeat", row))
    turns = []
    for turn_name, role, row in rows:
        value = deepcopy(field_inputs[str(row["task_id"])])
        prompt = v149.field_prompt_v149(value)
        schema = field_output_schema(value)
        request_paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": role,
                "task_id": str(row["task_id"]),
                "field": str(row["field"]),
                "expected_status": str(row["expected_status"]),
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": request_paths,
            }
        )
    selection_path = root / "negative-field-confirmation-selection-audit.json"
    _write_stable_time(
        selection_path,
        {
            "schema_version": V171_SPEC_VERSION,
            "created_at": now_iso(),
            "observed_negative_task_count": selection["negative_task_count"],
            "primary_task_count": len(selection["primary"]),
            "repeat_task_count": len(selection["repeats"]),
            "target_count": len(selection["target_task_ids"]),
            "control_count": len(selection["control_task_ids"]),
            "target_fields": ["certainty", "speaker"],
            "control_field_count": 10,
            "selection_uses_source_text": False,
            "selection_uses_only_frozen_truth_observed_status_and_opaque_ids": True,
            "production_rule_routes_every_observed_incorrect_field_decision": True,
            "full_confirmation_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
        },
        "created_at",
    )
    truth_path = root / "negative-field-confirmation-truth.private.json"
    _write_immutable(
        truth_path,
        {
            "schema_version": V171_SPEC_VERSION,
            "primary": [
                {
                    "task_id": row["task_id"],
                    "field": row["field"],
                    "expected_status": row["expected_status"],
                }
                for row in selection["primary"]
            ],
            "repeat_task_ids": [row["task_id"] for row in selection["repeats"]],
        },
    )
    capacity = _build_capacity_policy(root, source)
    spec = {
        "schema_version": V171_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V171_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "confirm_every_observable_negative_field_decision_with_side_free_gpt54_owner",
        "diagnostic_primary_task_count": 12,
        "diagnostic_repeat_task_count": 4,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": len(TURN_NAMES),
        "maximum_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "promotion_requires_primary_exact_count": 12,
        "promotion_requires_repeat_exact_count": 4,
        "promotion_requires_evidence_complete_count": 16,
        "full_confirmation_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "reference_truth_exposed_to_model": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": source["records"],
        "runtime_files": [_record(Path(__file__)), *source["values"]["spec"]["runtime_files"]],
        "frozen_instructions": {"field_sha256": sha256_text(v146.base_instructions_v146())},
        "frozen_inputs": {
            "selection": _record(selection_path),
            "truth": _record(truth_path),
            "field_protocol": source["values"]["spec"]["frozen_inputs"]["field_protocol"],
            "reference": source["values"]["spec"]["frozen_inputs"]["reference"],
            "turns": [
                {
                    "turn_name": row["turn_name"],
                    "role": row["turn_role"],
                    "input": _record(row["paths"]["input"]),
                    "prompt": _record(row["paths"]["prompt"]),
                    "schema": _record(row["paths"]["schema"]),
                }
                for row in turns
            ],
        },
        "privacy": "private_source_event_output_truth_and_mapping_sanitized_terminal_only",
    }
    spec_path = root / "negative-field-confirmation-diagnostic-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "selection": selection,
        "source": source,
    }


def score_v171(
    *, turns: Sequence[Mapping[str, Any]], outputs: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    primary = {}
    repeats = {}
    evidence_complete = 0
    truth = {}
    for turn, output in zip(turns, outputs, strict=True):
        decisions = output.get("decisions")
        if not isinstance(decisions, list) or len(decisions) != 1:
            raise JudgeV5CalibrationV171Error("v171 output coverage drifted")
        decision = decisions[0]
        task_id = str(turn["task_id"])
        if str(decision.get("task_id")) != task_id:
            raise JudgeV5CalibrationV171Error("v171 output identity drifted")
        evidence_complete += int(bool(decision.get("source_evidence_spans")))
        if turn["turn_role"] == "primary":
            primary[task_id] = str(decision["field_status"])
            truth[task_id] = str(turn["expected_status"])
        else:
            repeats[task_id] = str(decision["field_status"])
    primary_exact = sum(primary[key] == truth[key] for key in truth)
    repeat_exact = sum(primary[key] == repeats[key] for key in repeats)
    target_ids = {
        str(row["task_id"])
        for row in turns
        if row["turn_role"] == "primary" and row["expected_status"] == "correct"
    }
    target_exact = sum(primary[key] == "correct" for key in target_ids)
    metrics = {
        "primary_task_count": len(primary),
        "primary_exact_count": primary_exact,
        "target_task_count": len(target_ids),
        "target_exact_count": target_exact,
        "control_task_count": len(primary) - len(target_ids),
        "control_exact_count": sum(
            primary[key] == truth[key] for key in truth if key not in target_ids
        ),
        "repeat_task_count": len(repeats),
        "repeat_exact_count": repeat_exact,
        "abstention_count": sum(value == "abstain" for value in [*primary.values(), *repeats.values()]),
        "evidence_complete_count": evidence_complete,
        "evidence_expected_count": len(turns),
    }
    checks = {
        "primary_exact": primary_exact == 12,
        "target_exact": target_exact == 2,
        "control_exact": metrics["control_exact_count"] == 10,
        "repeat_exact": repeat_exact == 4,
        "no_abstentions": metrics["abstention_count"] == 0,
        "evidence_complete": evidence_complete == 16,
    }
    return {
        "schema_version": V171_SCORE_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(key for key, passed in checks.items() if not passed),
        "metrics": metrics,
    }


def _write_failure(
    root: Path, source: Mapping[str, Any], turn_name: Optional[str], error_class: str
) -> dict[str, Any]:
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v171 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    predecessor = source["cumulative_usage"]
    cumulative = _sum_usage(predecessor, usage)
    failure = {
        "schema_version": V171_FAILURE_VERSION,
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
        "schema_version": V171_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "full_confirmation_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v171(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v171 terminal")
    frozen = freeze_v171(output_dir=root, timeout_seconds=timeout_seconds)
    source = frozen["source"]
    current_turn: Optional[str] = None
    outputs, sidecars = [], []
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
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=1,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_field_output(candidate, item),
                )
                outputs.append(output)
                sidecars.append(sidecar)
        score = score_v171(turns=frozen["turns"], outputs=outputs)
        score_path = root / "negative-field-confirmation-diagnostic-score.json"
        _write_immutable(score_path, score)
        output_path = root / "negative-field-confirmation-output.private.json"
        _write_immutable(
            output_path,
            {
                "schema_version": V171_SPEC_VERSION,
                "turn_outputs": {
                    turn["turn_name"]: output
                    for turn, output in zip(frozen["turns"], outputs, strict=True)
                },
            },
        )
        passed = bool(score["passed"])
        accounting = _aggregate_usage(sidecars)
        cumulative = _sum_usage(source["cumulative_usage"], accounting["usage"])
        terminal = {
            "schema_version": V171_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v171_negative_field_confirmation_diagnostic_passed_full_confirmation_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v171_negative_field_confirmation_diagnostic_passed"
                if passed
                else "v171_negative_field_confirmation_diagnostic_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "full_confirmation_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "score": _record(score_path),
            "output": _record(output_path),
            "predecessor_cumulative_usage": source["cumulative_usage"],
            "cumulative_calibration_usage": cumulative,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, source, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, source, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v171 negative-field confirmation diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v171(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "full_confirmation_authorized": terminal.get("full_confirmation_authorized", False),
                "selection_authorized": terminal.get("selection_authorized", False),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
