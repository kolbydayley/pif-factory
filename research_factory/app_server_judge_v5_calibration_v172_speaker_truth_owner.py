from __future__ import annotations

"""Side-free owner adjudication for the remaining speaker truth dispute."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v137_full_observable_field_owner as v137
from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from . import app_server_judge_v5_calibration_v149_fresh_corrected_field_diagnostic as v149
from . import app_server_judge_v5_calibration_v168_capacity_recovery as v168
from . import app_server_judge_v5_calibration_v171_negative_field_confirmation_diagnostic as v171
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


V172_SPEC_VERSION = "pif_app_server_judge_v5_4_v172_spec_v1"
V172_SCORE_VERSION = "pif_app_server_judge_v5_4_v172_score_v1"
V172_REFERENCE_VERSION = "pif_app_server_judge_v5_4_fixture_reference_v16"
V172_TRUTH_VERSION = "pif_app_server_judge_v5_4_full_truth_v172"
V172_FAILURE_VERSION = "pif_app_server_judge_v5_4_v172_failure_v1"
V172_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v172_terminal_v1"
V172_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V172_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V172_PHASE_ID = "judge_v5_4_v172_speaker_truth_owner"

PRIMARY_TURN = "speaker_truth_owner_primary"
CANARY_TURN = "speaker_truth_owner_balanced_canary"
TURN_NAMES = (PRIMARY_TURN, CANARY_TURN)
MODEL = "gpt-5.6-terra"
EFFORT = "high"
MAXIMUM_TOTAL_TOKENS_PER_TURN = 45_000
TIMEOUT_SECONDS = v171.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v171.DEFAULT_OUTPUT_ROOT.parent / "judge-calibration-v5_4-v172-speaker-truth-owner"
).resolve()

TARGET_TASK_ID = "fullfield_9f8a800b8246aaa42152da33"
NEGATIVE_CONTROL_ID = "control_75bb6528a7fe1fc4a6ff40ca"
POSITIVE_CONTROL_ID = "control_f6b9ac3cdc33f690835ea848"
CONTROL_TRUTH = {NEGATIVE_CONTROL_ID: "incorrect", POSITIVE_CONTROL_ID: "correct"}
SPEAKER_CONTRACT = {
    "field": "speaker",
    "definition": "The direct discourse source or source voice that presents the aligned proposition in the excerpt.",
    "decision_rule": (
        "Keep speaker independent from actor, stance holder, and reported actor. A third-person "
        "narrative assertion that a person supports or believes something does not by itself make "
        "that person the direct discourse source. A quote or an explicit speech/source verb can "
        "establish the speaker. For a merged proposition, one speaker must voice every material "
        "conjunct; an adjacent or partial speaker is insufficient."
    ),
}


class JudgeV5CalibrationV172Error(RuntimeError):
    """The immutable v172 truth-owner contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v171_terminal() -> dict[str, Any]:
    root = v171.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "score": root / "negative-field-confirmation-diagnostic-score.json",
        "spec": root / "negative-field-confirmation-diagnostic-spec.json",
        "policy": root / "capacity-policy.json",
        "audit": root / "capacity-policy-audit.json",
        "truth": root / "negative-field-confirmation-truth.private.json",
        "output": root / "negative-field-confirmation-output.private.json",
    }
    values = {name: _load_json(path, f"v171 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v171_negative_field_confirmation_diagnostic_quality_gate_not_passed"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("full_confirmation_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("production_mutated") is not False
        or score.get("passed") is not False
        or score.get("failed_checks") != ["primary_exact", "target_exact"]
        or score.get("metrics", {}).get("primary_exact_count") != 11
        or score.get("metrics", {}).get("target_exact_count") != 1
        or score.get("metrics", {}).get("control_exact_count") != 10
        or score.get("metrics", {}).get("repeat_exact_count") != 4
        or terminal.get("usage", {}).get("total_tokens") != 346445
        or terminal.get("cumulative_calibration_usage", {}).get("total_tokens") != 5327539
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5CalibrationV172Error("v171 terminal contract drifted")
    attempts = _attempt_records(root)
    if len(attempts) != 16:
        raise JudgeV5CalibrationV172Error("v171 attempt coverage drifted")
    for attempt in attempts:
        for key in ("capacity", "sidecar", "output"):
            record = attempt.get(key)
            if not isinstance(record, Mapping):
                raise JudgeV5CalibrationV172Error(f"v171 {key} record is missing")
            _verify_record(record)
        _validate_usage(_load_json(Path(attempt["sidecar"]["path"]), "v171 sidecar"))
    for record in spec["runtime_files"]:
        _verify_record(record)
    for record in spec["frozen_inputs"]["turns"]:
        for key in ("input", "prompt", "schema"):
            _verify_record(record[key])
    for path in paths.values():
        if not path.is_file():
            raise JudgeV5CalibrationV172Error("v171 predecessor artifact disappeared")
    v170 = v171._validate_v170_quality_terminal()
    cumulative = terminal["cumulative_calibration_usage"]
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "v170": v170,
        "cumulative_usage": {field: int(cumulative[field]) for field in USAGE_FIELDS},
    }


def _find_task(path: Path, task_id: str) -> dict[str, Any]:
    value = _load_json(path, f"speaker source {task_id}")
    tasks = value.get("tasks")
    if not isinstance(tasks, list):
        raise JudgeV5CalibrationV172Error("speaker source task list drifted")
    matches = [deepcopy(row) for row in tasks if str(row.get("task_id")) == task_id]
    if len(matches) != 1:
        raise JudgeV5CalibrationV172Error("speaker source task identity drifted")
    task = matches[0]
    task["field_contract"] = deepcopy(SPEAKER_CONTRACT)
    return task


def build_v172_input() -> dict[str, Any]:
    target = _find_task(
        v168.DEFAULT_OUTPUT_ROOT
        / "turns/recovery-field-singleton-12/input.private.json",
        TARGET_TASK_ID,
    )
    negative = _find_task(
        v137.DEFAULT_OUTPUT_ROOT
        / "turns/expanded-speaker-primary-00/input.private.json",
        NEGATIVE_CONTROL_ID,
    )
    positive = _find_task(
        v137.DEFAULT_OUTPUT_ROOT
        / "turns/expanded-speaker-primary-01/input.private.json",
        POSITIVE_CONTROL_ID,
    )
    tasks = [target, negative, positive]
    tasks.sort(key=lambda row: sha256_text(f"v172|primary-order|{row['task_id']}"))
    base = v149._field_value(tasks[0])
    base["task_count"] = len(tasks)
    base["tasks"] = tasks
    base["singleton_context"] = False
    base["tasks_are_independent"] = True
    return base


def _build_capacity_policy(root: Path, source: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V172_CAPACITY_AUDIT_VERSION,
        "phase_id": V172_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v171_terminal": source["records"]["terminal"],
        "v171_score": source["records"]["score"],
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "task_count_per_turn": 3,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V172_CAPACITY_POLICY_VERSION,
        "phase_id": V172_PHASE_ID,
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


def freeze_v172(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v172 terminal")}
    source = _validate_v171_terminal()
    primary = build_v172_input()
    canary = deepcopy(primary)
    canary["tasks"] = list(reversed(canary["tasks"]))
    turns = []
    for turn_name, role, value in (
        (PRIMARY_TURN, "primary", primary),
        (CANARY_TURN, "balanced_canary", canary),
    ):
        prompt = v149.field_prompt_v149(value)
        schema = field_output_schema(value)
        request_paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": role,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": request_paths,
            }
        )
    truth_path = root / "speaker-truth-owner-controls.private.json"
    _write_immutable(
        truth_path,
        {
            "schema_version": V172_SPEC_VERSION,
            "target_task_id": TARGET_TASK_ID,
            "target_expected_status": None,
            "control_truth": CONTROL_TRUTH,
        },
    )
    capacity = _build_capacity_policy(root, source)
    v137_paths = [
        v137.DEFAULT_OUTPUT_ROOT / "full-observable-field-owner-spec.json",
        v137.DEFAULT_OUTPUT_ROOT / "full-observable-field-owner-truth.private.json",
        v137.DEFAULT_OUTPUT_ROOT / "turns/expanded-speaker-primary-00/input.private.json",
        v137.DEFAULT_OUTPUT_ROOT / "turns/expanded-speaker-primary-01/input.private.json",
    ]
    spec = {
        "schema_version": V172_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V172_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "side_free_terra_speaker_truth_owner_with_positive_negative_controls_and_balanced_order",
        "task_count": 3,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": 2,
        "maximum_turn_count": 2,
        "retry_count_per_turn": 0,
        "target_current_truth_exposed_to_model": False,
        "target_prior_model_outputs_exposed_to_model": False,
        "reference_freeze_requires_control_exact_count": 4,
        "reference_freeze_requires_target_order_agreement": True,
        "full_confirmation_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": source["records"],
        "runtime_files": [_record(Path(__file__)), *source["values"]["spec"]["runtime_files"]],
        "frozen_instructions": {"field_sha256": sha256_text(v146.base_instructions_v146())},
        "frozen_inputs": {
            "controls": _record(truth_path),
            "v137_sources": [_record(path) for path in v137_paths],
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
    spec_path = root / "speaker-truth-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "source": source,
    }


def score_v172(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if len(outputs) != 2:
        raise JudgeV5CalibrationV172Error("v172 output count drifted")
    mapped = []
    evidence_complete = 0
    abstentions = 0
    for output in outputs:
        decisions = output.get("decisions")
        if not isinstance(decisions, list) or len(decisions) != 3:
            raise JudgeV5CalibrationV172Error("v172 decision coverage drifted")
        rows = {str(row["task_id"]): str(row["field_status"]) for row in decisions}
        if set(rows) != {TARGET_TASK_ID, NEGATIVE_CONTROL_ID, POSITIVE_CONTROL_ID}:
            raise JudgeV5CalibrationV172Error("v172 decision identity drifted")
        evidence_complete += sum(bool(row.get("source_evidence_spans")) for row in decisions)
        abstentions += sum(str(row["field_status"]) == "abstain" for row in decisions)
        mapped.append(rows)
    control_exact = sum(
        rows[task_id] == wanted for rows in mapped for task_id, wanted in CONTROL_TRUTH.items()
    )
    target_statuses = [rows[TARGET_TASK_ID] for rows in mapped]
    target_agrees = target_statuses[0] == target_statuses[1]
    target_status = target_statuses[0] if target_agrees else "abstain"
    metrics = {
        "orientation_count": 2,
        "control_decision_count": 4,
        "control_exact_count": control_exact,
        "target_decision_count": 2,
        "target_order_agreement": target_agrees,
        "target_adjudicated_status": target_status,
        "abstention_count": abstentions,
        "evidence_complete_count": evidence_complete,
        "evidence_expected_count": 6,
    }
    checks = {
        "controls_exact": control_exact == 4,
        "target_order_agreement": target_agrees,
        "target_not_abstain": target_status in {"correct", "incorrect"},
        "no_abstentions": abstentions == 0,
        "evidence_complete": evidence_complete == 6,
    }
    return {
        "schema_version": V172_SCORE_VERSION,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v172 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    cumulative = _sum_usage(source["cumulative_usage"], usage)
    failure = {
        "schema_version": V172_FAILURE_VERSION,
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
        "predecessor_cumulative_usage": source["cumulative_usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V172_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_frozen": False,
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


async def run_v172(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v172 terminal")
    frozen = freeze_v172(output_dir=root, timeout_seconds=timeout_seconds)
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
                    batch_size=3,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_field_output(candidate, item),
                )
                outputs.append(output)
                sidecars.append(sidecar)
        score = score_v172(outputs)
        score_path = root / "speaker-truth-owner-score.json"
        _write_immutable(score_path, score)
        output_path = root / "speaker-truth-owner-output.private.json"
        _write_immutable(
            output_path,
            {
                "schema_version": V172_SPEC_VERSION,
                "primary": outputs[0],
                "balanced_canary": outputs[1],
            },
        )
        passed = bool(score["passed"])
        reference_path = root / "fixture-reference-v16-v172.private.json"
        truth_path = root / "full-calibration-truth-v172.private.json"
        patch_path = root / "speaker-truth-patch-audit.json"
        if passed:
            status = str(score["metrics"]["target_adjudicated_status"])
            prior_truth = source["v170"]["values"]["truth"]
            truth = deepcopy(prior_truth)
            changed = 0
            for row in truth["field_tasks"]:
                if str(row["task_id"]) == TARGET_TASK_ID:
                    changed = int(str(row["expected_status"]) != status)
                    row["expected_status"] = status
                    break
            else:
                raise JudgeV5CalibrationV172Error("speaker truth target disappeared")
            truth["schema_version"] = V172_TRUTH_VERSION
            _write_immutable(truth_path, truth)
            _write_immutable(
                patch_path,
                {
                    "schema_version": V172_REFERENCE_VERSION,
                    "target_task_id": TARGET_TASK_ID,
                    "prior_expected_status": next(
                        row["expected_status"]
                        for row in prior_truth["field_tasks"]
                        if str(row["task_id"]) == TARGET_TASK_ID
                    ),
                    "adjudicated_expected_status": status,
                    "reference_change_count": changed,
                    "control_exact_count": score["metrics"]["control_exact_count"],
                    "target_order_agreement": score["metrics"]["target_order_agreement"],
                    "source_text_in_audit": False,
                },
            )
            _write_immutable(
                reference_path,
                {
                    "schema_version": V172_REFERENCE_VERSION,
                    "frozen_at": now_iso(),
                    "model": MODEL,
                    "reasoning_effort": EFFORT,
                    "target_task_id": TARGET_TASK_ID,
                    "target_expected_status": status,
                    "truth": _record(truth_path),
                    "patch_audit": _record(patch_path),
                    "owner_output": _record(output_path),
                    "owner_score": _record(score_path),
                    "full_confirmation_authorized": True,
                    "selection_authorized": False,
                    "holdout_authorized": False,
                },
            )
        accounting = _aggregate_usage(sidecars)
        cumulative = _sum_usage(source["cumulative_usage"], accounting["usage"])
        terminal = {
            "schema_version": V172_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v172_speaker_truth_owner_passed_reference_v16_frozen_full_confirmation_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v172_speaker_truth_owner_passed"
                if passed
                else "v172_speaker_truth_owner_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reference_frozen": passed,
            "full_confirmation_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "score": _record(score_path),
            "output": _record(output_path),
            "reference": _record(reference_path) if passed else None,
            "truth": _record(truth_path) if passed else None,
            "patch_audit": _record(patch_path) if passed else None,
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
    parser = argparse.ArgumentParser(description="Run v172 speaker truth owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v172(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal.get("reference_frozen", False),
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
