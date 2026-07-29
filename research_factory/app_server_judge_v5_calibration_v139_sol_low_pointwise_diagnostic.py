from __future__ import annotations

"""Fresh Sol-low pointwise diagnostic against the repaired v11 reference."""

import argparse
import asyncio
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v108_layered_diagnostic as v108
from . import app_server_judge_v5_calibration_v138_singleton_field_repair as v138
from .app_server_judge_v5_calibration import pointwise_input_subset
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
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v120_retained_case_diagnostic import (
    _balanced_ids,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _validate_usage,
)
from .util import now_iso, sha256_text


V139_SELECTION_VERSION = "pif_app_server_judge_v5_4_v139_selection_v1"
V139_TRUTH_VERSION = "pif_app_server_judge_v5_4_v139_truth_v1"
V139_SPEC_VERSION = "pif_app_server_judge_v5_4_v139_spec_v1"
V139_SCORE_VERSION = "pif_app_server_judge_v5_4_v139_score_v1"
V139_FAILURE_VERSION = "pif_app_server_judge_v5_4_v139_failure_v1"
V139_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v139_terminal_v1"
V139_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V139_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V139_PHASE_ID = "judge_v5_4_v139_sol_low_pointwise_diagnostic"

MODEL = "gpt-5.6-sol"
EFFORT = "low"
CASE_COUNT = 12
CANARY_CASE_COUNT = 4
PRIMARY_TURNS = tuple(f"sol_low_pointwise_{index:02d}" for index in range(CASE_COUNT))
CANARY_TURNS = tuple(f"sol_low_order_canary_{index:02d}" for index in range(CANARY_CASE_COUNT))
TURN_NAMES = PRIMARY_TURNS + CANARY_TURNS
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    v138.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v139-sol-low-pointwise-diagnostic"
).resolve()


class JudgeV5CalibrationV139Error(RuntimeError):
    """The v139 Sol-low pointwise diagnostic contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v138() -> dict[str, Any]:
    root = v138.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "singleton-field-repair-spec.json",
        "terminal": root / "terminal.json",
        "repair_score": root / "singleton-field-repair-score.json",
        "v137_score": root / "repaired-v137-score.json",
        "primary": root / "repaired-full-observable-field-owner-output.private.json",
        "canary": root / "repaired-full-observable-field-owner-canary.private.json",
        "reference": root / "calibration-truth-v11-observable-field-owner.private.json",
        "truth": root / "singleton-field-repair-truth.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v138 {name}") for name, path in paths.items()}
    terminal, repair_score, v137_score, spec = (
        values["terminal"],
        values["repair_score"],
        values["v137_score"],
        values["spec"],
    )
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason") != "v138_singleton_repair_passed_reference_frozen"
        or terminal.get("reference_patch_authorized") is not True
        or terminal.get("reference_frozen") is not True
        or terminal.get("fresh_pointwise_diagnostic_authorized") is not True
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 127260
        or repair_score.get("passed") is not True
        or repair_score.get("failed_checks") != []
        or repair_score.get("metrics", {}).get("singleton_control_exact_count") != 4
        or v137_score.get("passed") is not True
        or v137_score.get("failed_checks") != []
        or v137_score.get("metrics", {}).get("matched_control_exact_count") != 12
        or v137_score.get("metrics", {}).get("order_canary_exact_count") != 40
        or v137_score.get("metrics", {}).get("owner_reference_change_count") != 19
        or spec.get("model") != "gpt-5.6-sol"
        or spec.get("reasoning_effort") != "high"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV139Error("v138 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV139Error("v138 runtime record drifted")
    for name, key in {
        "repair_score": "repair_score",
        "v137_score": "repaired_v137_score",
        "primary": "repaired_primary",
        "canary": "repaired_canary",
        "reference": "reference",
    }.items():
        record = terminal.get(key)
        if record != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV139Error(f"v138 {name} record drifted")
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
                raise JudgeV5CalibrationV139Error("v138 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v138 sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV139Error("v138 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "v137": v138._validate_v137(),
    }


def build_v139_selection(
    predecessor: Mapping[str, Any], source: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    reference = predecessor["values"]["reference"]
    previously_targeted = set(
        predecessor["v137"]["v136"]["v134"]["v133"]["values"]["truth"][
            "cases"
        ]
    )
    candidates = sorted(set(reference["cases"]) - previously_targeted)
    if len(candidates) != 48:
        raise JudgeV5CalibrationV139Error("v139 fresh candidate coverage drifted")
    selected = _balanced_ids(candidates, reference, CASE_COUNT, "v139-selected")
    canary = _balanced_ids(selected, reference, CANARY_CASE_COUNT, "v139-canary")
    expected = deepcopy(reference)
    expected["schema_version"] = V139_TRUTH_VERSION
    expected["cases"] = {
        case_id: deepcopy(reference["cases"][case_id]) for case_id in selected
    }
    expected["canary_case_ids"] = list(canary)
    expected["diagnostic_source"] = (
        "v11_cases_excluding_all_18_v133_targeted_development_cases"
    )
    pointwise = pointwise_input_subset(
        source["values"]["v106_pointwise_input"], selected
    )
    if len(expected["cases"]) != CASE_COUNT:
        raise JudgeV5CalibrationV139Error("v139 selected truth coverage drifted")
    selection = {
        "schema_version": V139_SELECTION_VERSION,
        "created_at": now_iso(),
        "candidate_case_count": len(candidates),
        "excluded_previously_targeted_case_count": len(previously_targeted),
        "selected_case_count": CASE_COUNT,
        "canary_case_count": CANARY_CASE_COUNT,
        "selected": list(selected),
        "canary": list(canary),
        "selected_shape_counts": dict(
            sorted(Counter(reference["cases"][case_id]["shape"] for case_id in selected).items())
        ),
        "canary_shape_counts": dict(
            sorted(Counter(reference["cases"][case_id]["shape"] for case_id in canary).items())
        ),
        "maximum_cases_per_turn": 1,
        "selection_uses_source_text": False,
        "selection_uses_only_frozen_reference_labels_and_provenance": True,
        "model_outputs_used_for_selection": False,
        "privacy": "opaque_case_ids_and_aggregate_counts_only",
    }
    return selection, expected, pointwise


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _f1(tp: int, fp: int, fn: int) -> float:
    denominator = 2 * tp + fp + fn
    return round(2 * tp / denominator, 6) if denominator else 1.0


def score_v139(
    *, pointwise: Mapping[str, Any], canary: Mapping[str, Any], expected: Mapping[str, Any]
) -> dict[str, Any]:
    observed = {str(row["witness_id"]): row for row in pointwise["units"]}
    repeated = {str(row["witness_id"]): row for row in canary["units"]}
    tp = fn = tn = fp = structured = field_tp = field_fp = field_fn = abstain = total = 0
    for case in expected["cases"].values():
        for witness_id, wanted_support in case["proposition"].items():
            row = observed[witness_id]
            total += 1
            got = row["proposition_verdict"]
            abstain += int(got == "abstain")
            if wanted_support == "supported" and got == "supported":
                tp += 1
            elif wanted_support == "supported":
                fn += 1
            elif got == "unsupported":
                tn += 1
            else:
                fp += 1
            structured += int(
                row["structured_field_verdict"]
                == case["structured_fields"][witness_id]
            )
            wanted_fields = set(case["field_issues"][witness_id])
            got_fields = set(row["field_issue_fields"])
            field_tp += len(wanted_fields & got_fields)
            field_fp += len(got_fields - wanted_fields)
            field_fn += len(wanted_fields - got_fields)
    canary_exact = sum(
        all(
            (
                observed[witness_id]["proposition_verdict"],
                observed[witness_id]["structured_field_verdict"],
                sorted(observed[witness_id]["field_issue_fields"]),
            )
            == (
                repeated[witness_id]["proposition_verdict"],
                repeated[witness_id]["structured_field_verdict"],
                sorted(repeated[witness_id]["field_issue_fields"]),
            )
            for witness_id in expected["cases"][case_id]["proposition"]
        )
        for case_id in expected["canary_case_ids"]
    )
    metrics = {
        "case_count": CASE_COUNT,
        "witness_count": total,
        "support_sensitivity": _ratio(tp, tp + fn),
        "support_specificity": _ratio(tn, tn + fp),
        "structured_field_accuracy": _ratio(structured, total),
        "pointwise_field_issue_f1": _f1(field_tp, field_fp, field_fn),
        "abstention_count": abstain,
        "canary_case_count": CANARY_CASE_COUNT,
        "canary_case_exact_count": canary_exact,
        "order_bias": _ratio(CANARY_CASE_COUNT - canary_exact, CANARY_CASE_COUNT),
    }
    checks = {
        "case_count": True,
        "support_sensitivity": metrics["support_sensitivity"] >= 0.95,
        "support_specificity": metrics["support_specificity"] >= 0.95,
        "structured_field_accuracy": metrics["structured_field_accuracy"] >= 0.95,
        "pointwise_field_issue_f1": metrics["pointwise_field_issue_f1"] >= 0.95,
        "abstention_count": abstain == 0,
        "canary_case_count": True,
        "order_bias": metrics["order_bias"] == 0.0,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V139_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": metrics,
        "expanded_sol_low_pointwise_diagnostic_authorized": passed,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V139_CAPACITY_AUDIT_VERSION,
        "phase_id": V139_PHASE_ID,
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
        "schema_version": V139_CAPACITY_POLICY_VERSION,
        "phase_id": V139_PHASE_ID,
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


def freeze_v139(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v139 terminal")}
    predecessor = _validate_v138()
    source = v108._validate_predecessors()
    selection, expected, selected_input = build_v139_selection(predecessor, source)
    selection_path = root / "selection-audit.json"
    truth_path = root / "sol-low-pointwise-truth.private.json"
    input_path = root / "pointwise-input.private.json"
    _write_stable_time(selection_path, selection, "created_at")
    _write_immutable(truth_path, expected)
    _write_immutable(input_path, selected_input)
    turns = []
    for turn_name, case_id in zip(PRIMARY_TURNS, selection["selected"], strict=True):
        value = pointwise_input_subset(selected_input, [case_id])
        prompt = v108.build_pointwise_checklist_prompt(value)
        schema = v108.pointwise_checklist_schema(value)
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=value,
            prompt=prompt,
            schema=schema,
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "primary",
                "case_id": case_id,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    for turn_name, case_id in zip(CANARY_TURNS, selection["canary"], strict=True):
        value = pointwise_input_subset(selected_input, [case_id])
        value["units"] = list(reversed(value["units"]))
        prompt = v108.build_pointwise_checklist_prompt(value)
        schema = v108.pointwise_checklist_schema(value)
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=value,
            prompt=prompt,
            schema=schema,
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "order_canary",
                "case_id": case_id,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    predecessor_records = {
        **{f"v138_{name}": record for name, record in predecessor["records"].items()},
        "v138_attempts": predecessor["attempts"],
        "v106_pointwise_input": source["records"]["v106_pointwise_input"],
        "v106_pool": source["records"]["v106_pool"],
        "v106_truth": source["records"]["v106_truth"],
    }
    capacity = _build_capacity_policy(root, predecessor_records)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V139_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "twelve_fresh_single_case_sol_low_pointwise_turns_with_four_unmarked_order_canaries",
        "case_count": CASE_COUNT,
        "canary_case_count": CANARY_CASE_COUNT,
        "maximum_cases_per_turn": 1,
        "turn_plan": list(TURN_NAMES),
        "prior_model_outputs_reused": False,
        "reference_truth_exposed_to_model": False,
        "canary_marker_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_support_field_abstention_exact_evidence_and_zero_order_bias_gates",
        "expanded_sol_low_pointwise_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor_records,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v138_singleton_field_repair.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v108_layered_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v107_recovery_receipt.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v106_full_development.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_instruction_hash": sha256_text(v108.pointwise_checklist_instructions()),
        "frozen_inputs": {
            "selection": _record(selection_path),
            "truth": _record(truth_path),
            "pointwise": _record(input_path),
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "role": turn["turn_role"],
                    "case_id": turn["case_id"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "sol-low-pointwise-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "expected": expected,
    }


def _merge_checklists(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    units = [deepcopy(row) for output in outputs for row in output["units"]]
    witness_ids = [str(row["witness_id"]) for row in units]
    if len(witness_ids) != len(set(witness_ids)):
        raise JudgeV5CalibrationV139Error("v139 checklist outputs overlap")
    return {"units": units}


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
                _load_json(Path(record["path"]), "v139 sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V139_FAILURE_VERSION,
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
        "schema_version": V139_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "expanded_sol_low_pointwise_diagnostic_authorized": False,
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


async def run_v139(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v139 terminal")
    frozen = freeze_v139(output_dir=root, timeout_seconds=timeout_seconds)
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
                    base_instructions=v108.pointwise_checklist_instructions(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(turn["value"]["units"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: v108.validate_pointwise_checklist_output(
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
        primary_checklist = _merge_checklists(primary_outputs)
        canary_checklist = _merge_checklists(canary_outputs)
        pointwise = v108.project_pointwise_checklist(primary_checklist)
        canary = v108.project_pointwise_checklist(canary_checklist)
        score = score_v139(
            pointwise=pointwise,
            canary=canary,
            expected=frozen["expected"],
        )
        paths = {
            "pointwise": root / "sol-low-pointwise-output.private.json",
            "canary": root / "sol-low-order-canary.private.json",
            "score": root / "sol-low-pointwise-score.json",
        }
        _write_immutable(paths["pointwise"], pointwise)
        _write_immutable(paths["canary"], canary)
        _write_immutable(paths["score"], score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V139_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v139_sol_low_pointwise_passed_expansion_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v139_sol_low_pointwise_quality_gates_passed"
                if passed
                else "v139_sol_low_pointwise_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "expanded_sol_low_pointwise_diagnostic_authorized": passed,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(paths["score"]),
            "pointwise_output": _record(paths["pointwise"]),
            "canary_output": _record(paths["canary"]),
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v139 Sol-low pointwise diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v139(
            output_dir=Path(args.output_dir),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "expanded_sol_low_pointwise_diagnostic_authorized": terminal.get(
                    "expanded_sol_low_pointwise_diagnostic_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
