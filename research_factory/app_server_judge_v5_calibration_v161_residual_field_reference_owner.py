from __future__ import annotations

"""Blinded reference-owner audit for the six persistent field residuals."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v145_comprehensive_field_reference_owner as v145
from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v160_luna_field_diagnostic as v160
from .app_server_judge_v5 import CHECKLIST_FIELDS
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


V161_SPEC_VERSION = "pif_app_server_judge_v5_4_v161_spec_v1"
V161_SCORE_VERSION = "pif_app_server_judge_v5_4_v161_score_v1"
V161_REFERENCE_VERSION = "pif_app_server_fixture_reference_v14_v161"
V161_TRUTH_VERSION = "pif_app_server_judge_v5_4_v161_patched_truth_v1"
V161_FAILURE_VERSION = "pif_app_server_judge_v5_4_v161_failure_v1"
V161_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v161_terminal_v1"
V161_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V161_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V161_PHASE_ID = "judge_v5_4_v161_residual_field_reference_owner"

MODEL = "gpt-5.6-terra"
EFFORT = "high"
PRIMARY_TURNS = tuple(f"field_reference_primary_{index:02d}" for index in range(12))
REPEAT_TURNS = tuple(f"field_reference_repeat_{index:02d}" for index in range(6))
TURN_NAMES = PRIMARY_TURNS + REPEAT_TURNS
MAXIMUM_TOTAL_TOKENS_PER_TURN = 45000
TIMEOUT_SECONDS = v160.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v160.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v161-residual-field-reference-owner"
).resolve()


class JudgeV5CalibrationV161Error(RuntimeError):
    """The immutable v161 reference-owner contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v160() -> dict[str, Any]:
    root = v160.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "score": root / "luna-field-diagnostic-score.json",
        "spec": root / "luna-field-diagnostic-spec.json",
        "field_output": root / "field-output.private.json",
    }
    values = {name: _load_json(path, f"v160 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    sidecars = []
    for row in attempts:
        if not isinstance(row.get("sidecar"), Mapping):
            raise JudgeV5CalibrationV161Error("v160 measured sidecar coverage drifted")
        _verify_record(row["sidecar"])
        _validate_usage(_load_json(Path(row["sidecar"]["path"]), "v160 sidecar"))
        sidecars.append(row["sidecar"])
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v160_luna_field_diagnostic_quality_gate_not_passed"
        or terminal.get("luna_field_diagnostic_passed") is not False
        or terminal.get("bounded_alignment_verifier_diagnostic_authorized") is not False
        or terminal.get("fresh_full_replacement_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 239995
        or terminal.get("cumulative_calibration_usage", {}).get("total_tokens")
        != 2166422
        or score.get("passed") is not False
        or score.get("metrics", {}).get("field_residual_exact_count") != 2
        or score.get("metrics", {}).get("field_control_exact_count") != 5
        or len(attempts) != 12
        or len(sidecars) != 12
        or spec.get("maximum_turn_count") != 12
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5CalibrationV161Error("v160 quality terminal drifted")
    for record in spec["runtime_files"]:
        _verify_record(record)
    for turn in spec["frozen_inputs"]["turns"]:
        for key in ("input", "prompt", "schema"):
            _verify_record(turn[key])
    source = v160._validate_v159()
    data = v160.build_v160_inputs(source)
    v155_source = v155._validate_v154()
    reference = v155._reference(v155_source)
    reference_record = v155._reference_record(v155_source)
    _verify_record(reference_record)
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "sidecars": sidecars,
        "source": source,
        "data": data,
        "reference": reference,
        "reference_record": reference_record,
        "cumulative_usage": terminal["cumulative_calibration_usage"],
    }


def build_v161_inputs(source: Mapping[str, Any]) -> dict[str, Any]:
    diagnostic = source["source"]["source"]["values"]["diagnostic"]
    residual_ids = list(diagnostic["field_error_task_ids"])
    control_ids = list(diagnostic["field_control_task_ids"])
    values = {row["task_id"]: row["value"] for row in source["data"]["turns"]}
    primary_ids = residual_ids + control_ids
    primary_ids.sort(key=lambda value: sha256_text(f"v161|primary|{value}"))
    repeat_ids = sorted(
        residual_ids, key=lambda value: sha256_text(f"v161|repeat|{value}")
    )
    turns = []
    for turn_name, task_id in zip(PRIMARY_TURNS, primary_ids, strict=True):
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "reference_primary",
                "task_id": task_id,
                "value": deepcopy(values[task_id]),
            }
        )
    for turn_name, task_id in zip(REPEAT_TURNS, repeat_ids, strict=True):
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "reference_repeat",
                "task_id": task_id,
                "value": deepcopy(values[task_id]),
            }
        )
    if (
        len(turns) != 18
        or len(set(primary_ids)) != 12
        or len(set(repeat_ids)) != 6
        or set(repeat_ids) != set(residual_ids)
    ):
        raise JudgeV5CalibrationV161Error("v161 reference coverage drifted")
    return {
        "turns": turns,
        "primary_ids": primary_ids,
        "repeat_ids": repeat_ids,
        "residual_ids": residual_ids,
        "control_ids": control_ids,
        "truth": source["data"]["truth"],
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    audit = {
        "schema_version": V161_CAPACITY_AUDIT_VERSION,
        "phase_id": V161_PHASE_ID,
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
        "schema_version": V161_CAPACITY_POLICY_VERSION,
        "phase_id": V161_PHASE_ID,
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


def freeze_v161(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v161 terminal")}
    source = _validate_v160()
    data = build_v161_inputs(source)
    turns = []
    for row in data["turns"]:
        prompt = v146.build_prompt_v146(row["value"])
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
        **{f"v160_{name}": record for name, record in source["records"].items()},
        "v160_sidecars": source["sidecars"],
        "current_reference": source["reference_record"],
        "cumulative_usage": source["cumulative_usage"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V161_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "designated_terra_reference_owner_six_residuals_six_controls_and_six_residual_repeats",
        "primary_turn_count": 12,
        "repeat_turn_count": 6,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": len(TURN_NAMES),
        "maximum_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "majority_voting_used": False,
        "truth_labels_exposed_to_model": False,
        "prior_model_decisions_exposed_to_model": False,
        "repeat_marker_exposed_to_model": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            *source["values"]["spec"]["runtime_files"],
            _record(Path(v145.__file__)),
        ],
        "frozen_instructions": {
            "reference_owner_sha256": sha256_text(v146.base_instructions_v146())
        },
        "frozen_inputs": {
            "current_reference": source["reference_record"],
            "v160_spec": source["records"]["spec"],
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
    spec_path = root / "residual-field-reference-owner-spec.json"
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


def score_v161(
    *, outputs: Mapping[str, Mapping[str, Any]], data: Mapping[str, Any]
) -> dict[str, Any]:
    primary = {}
    repeated = {}
    for turn in data["turns"]:
        row = outputs[turn["turn_name"]]["decisions"][0]
        target = primary if turn["turn_role"] == "reference_primary" else repeated
        target[str(row["task_id"])] = row
    if set(primary) != set(data["primary_ids"]) or set(repeated) != set(
        data["repeat_ids"]
    ):
        raise JudgeV5CalibrationV161Error("v161 output coverage drifted")
    truth = {str(row["task_id"]): row for row in data["truth"]["field_tasks"]}
    controls = set(data["control_ids"])
    residuals = set(data["residual_ids"])
    metrics = {
        "primary_decision_count": len(primary),
        "control_count": len(controls),
        "control_exact_count": sum(
            primary[key]["field_status"] == truth[key]["expected_status"]
            for key in controls
        ),
        "owner_count": len(residuals),
        "owner_abstention_count": sum(
            primary[key]["field_status"] == "abstain" for key in residuals
        ),
        "repeat_count": len(repeated),
        "repeat_exact_count": sum(
            primary[key]["field_status"] == repeated[key]["field_status"]
            for key in residuals
        ),
        "evidence_complete_count": sum(
            bool(row["source_evidence_spans"])
            for row in list(primary.values()) + list(repeated.values())
        ),
        "current_reference_agreement_count": sum(
            primary[key]["field_status"] == truth[key]["expected_status"]
            for key in residuals
        ),
    }
    checks = {
        "control_exact_rate": metrics["control_exact_count"] == 6,
        "owner_abstention_count": metrics["owner_abstention_count"] == 0,
        "repeat_exact_rate": metrics["repeat_exact_count"] == 6,
        "evidence_complete_rate": metrics["evidence_complete_count"] == 18,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V161_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": metrics,
        "reference_patch_authorized": passed,
        "fresh_field_diagnostic_authorized": passed,
        "fresh_full_replacement_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def patch_truth_and_reference(
    *,
    outputs: Mapping[str, Mapping[str, Any]],
    data: Mapping[str, Any],
    current_reference: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], int]:
    truth = deepcopy(data["truth"])
    reference = deepcopy(current_reference)
    primary = {}
    for turn in data["turns"]:
        if turn["turn_role"] != "reference_primary":
            continue
        primary[turn["task_id"]] = outputs[turn["turn_name"]]["decisions"][0]
    truth_rows = {str(row["task_id"]): row for row in truth["field_tasks"]}
    changed = 0
    for task_id in data["residual_ids"]:
        target = truth_rows[task_id]
        status = primary[task_id]["field_status"]
        if status == "abstain":
            raise JudgeV5CalibrationV161Error("v161 cannot patch an abstaining owner")
        before = target["expected_status"]
        target["expected_status"] = status
        case = reference["cases"][target["case_id"]]
        issues = set(case["field_issues"][target["witness_id"]])
        if status == "incorrect":
            issues.add(target["field"])
        else:
            issues.discard(target["field"])
        case["field_issues"][target["witness_id"]] = [
            field for field in CHECKLIST_FIELDS if field in issues
        ]
        case["structured_fields"][target["witness_id"]] = (
            "incorrect" if issues else "correct"
        )
        changed += int(before != status)
    truth.update(
        {
            "schema_version": V161_TRUTH_VERSION,
            "v161_reference_owner_task_count": 6,
            "v161_reference_change_count": changed,
        }
    )
    reference.update(
        {
            "schema_version": V161_REFERENCE_VERSION,
            "reference_version": "fixture_reference_v14_v161_residual_field_owner_frozen",
            "v161_reference_owner_task_count": 6,
            "v161_reference_change_count": changed,
            "v161_owner_basis": "fresh_terra_singleton_reference_owner_with_controls_and_independent_repeats",
            "reference_frozen": True,
            "selection_authorized": False,
            "holdout_authorized": False,
        }
    )
    return truth, reference, changed


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
                _load_json(Path(record["path"]), "v161 measured sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    predecessor = _validate_v160()["cumulative_usage"]
    cumulative = _sum_usage(predecessor, usage)
    failure = {
        "schema_version": V161_FAILURE_VERSION,
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
        "schema_version": V161_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_frozen": False,
        "fresh_field_diagnostic_authorized": False,
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


async def run_v161(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v161 terminal")
    frozen = freeze_v161(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    sidecars = []
    outputs = {}
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
                    output_validator=lambda candidate, item=turn["value"]: validate_field_output(
                        candidate, item
                    ),
                )
                outputs[current_turn] = output
                sidecars.append(sidecar)
        score = score_v161(outputs=outputs, data=frozen["data"])
        score_path = root / "residual-field-reference-owner-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        truth_path = root / "full-calibration-truth-v161.private.json"
        reference_path = root / "fixture-reference-v14-v161.private.json"
        change_count = 0
        if passed:
            truth, reference, change_count = patch_truth_and_reference(
                outputs=outputs,
                data=frozen["data"],
                current_reference=frozen["source"]["reference"],
            )
            _write_immutable(truth_path, truth)
            _write_immutable(reference_path, reference)
        _write_immutable(
            root / "reference-owner-output.private.json",
            {"schema_version": V161_SCORE_VERSION, "turn_outputs": outputs},
        )
        accounting = _aggregate_usage(sidecars)
        predecessor = frozen["source"]["cumulative_usage"]
        cumulative = _sum_usage(predecessor, accounting["usage"])
        terminal = {
            "schema_version": V161_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": "v161_residual_field_reference_frozen_fresh_field_diagnostic_authorized"
            if passed
            else "inactive_incomplete_recovery_required",
            "development_terminal_reason": "v161_residual_field_reference_owner_passed"
            if passed
            else "v161_residual_field_reference_owner_quality_gate_not_passed",
            "overall_evaluation_complete": False,
            "reference_frozen": passed,
            "reference_change_count": change_count,
            "fresh_field_diagnostic_authorized": passed,
            "fresh_full_replacement_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "score": _record(score_path),
            "patched_truth": _record(truth_path) if passed else None,
            "reference": _record(reference_path) if passed else None,
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
    parser = argparse.ArgumentParser(description="Run v161 residual field reference owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v161(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal.get("reference_frozen", False),
                "fresh_field_diagnostic_authorized": terminal.get(
                    "fresh_field_diagnostic_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
