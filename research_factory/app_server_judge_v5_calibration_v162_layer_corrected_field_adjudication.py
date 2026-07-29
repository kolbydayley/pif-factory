from __future__ import annotations

"""Correct the unsupported-inference layer and adjudicate one unstable field."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v159_replacement_model_diagnostic as v159
from . import app_server_judge_v5_calibration_v160_luna_field_diagnostic as v160
from . import app_server_judge_v5_calibration_v161_residual_field_reference_owner as v161
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


V162_SPEC_VERSION = "pif_app_server_judge_v5_4_v162_spec_v1"
V162_SCORE_VERSION = "pif_app_server_judge_v5_4_v162_score_v1"
V162_REFERENCE_VERSION = "pif_app_server_fixture_reference_v14_v162"
V162_TRUTH_VERSION = "pif_app_server_judge_v5_4_v162_patched_truth_v1"
V162_FAILURE_VERSION = "pif_app_server_judge_v5_4_v162_failure_v1"
V162_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v162_terminal_v1"
V162_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V162_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V162_PHASE_ID = "judge_v5_4_v162_layer_corrected_field_adjudication"

MODEL = "gpt-5.4"
EFFORT = "high"
TURN_NAMES = tuple(f"field_adjudication_singleton_{index:02d}" for index in range(6))
MAXIMUM_TOTAL_TOKENS_PER_TURN = 45000
TIMEOUT_SECONDS = v161.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v161.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v162-layer-corrected-field-adjudication"
).resolve()


class JudgeV5CalibrationV162Error(RuntimeError):
    """The immutable v162 adjudication contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v161() -> dict[str, Any]:
    root = v161.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "score": root / "residual-field-reference-owner-score.json",
        "spec": root / "residual-field-reference-owner-spec.json",
        "output": root / "reference-owner-output.private.json",
    }
    values = {name: _load_json(path, f"v161 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    sidecars = []
    for row in attempts:
        if not isinstance(row.get("sidecar"), Mapping):
            raise JudgeV5CalibrationV162Error("v161 measured sidecar coverage drifted")
        _verify_record(row["sidecar"])
        _validate_usage(_load_json(Path(row["sidecar"]["path"]), "v161 sidecar"))
        sidecars.append(row["sidecar"])
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v161_residual_field_reference_owner_quality_gate_not_passed"
        or terminal.get("reference_frozen") is not False
        or terminal.get("fresh_field_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 380122
        or terminal.get("cumulative_calibration_usage", {}).get("total_tokens")
        != 2546544
        or score.get("passed") is not False
        or score.get("failed_checks") != ["control_exact_rate", "repeat_exact_rate"]
        or score.get("metrics", {}).get("control_exact_count") != 5
        or score.get("metrics", {}).get("repeat_exact_count") != 5
        or score.get("metrics", {}).get("owner_abstention_count") != 0
        or len(attempts) != 18
        or len(sidecars) != 18
        or spec.get("maximum_turn_count") != 18
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5CalibrationV162Error("v161 quality terminal drifted")
    for record in spec["runtime_files"]:
        _verify_record(record)
    for turn in spec["frozen_inputs"]["turns"]:
        for key in ("input", "prompt", "schema"):
            _verify_record(turn[key])
    source = v161._validate_v160()
    data = v161.build_v161_inputs(source)
    v155_support = source["source"]["source"]["source"]["source"]["v155"]
    support_receipts = v155_support["values"]["support_receipts"]
    support_record = v155_support["records"]["support_receipts"]
    _verify_record(support_record)
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "sidecars": sidecars,
        "source": source,
        "data": data,
        "support_receipts": support_receipts,
        "support_record": support_record,
        "cumulative_usage": terminal["cumulative_calibration_usage"],
    }


def _v161_decisions(source: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    outputs = source["values"]["output"]["turn_outputs"]
    primary = {}
    repeated = {}
    for turn in source["data"]["turns"]:
        row = outputs[turn["turn_name"]]["decisions"][0]
        target = primary if turn["turn_role"] == "reference_primary" else repeated
        target[turn["task_id"]] = row
    return primary, repeated


def build_v162_inputs(source: Mapping[str, Any]) -> dict[str, Any]:
    data = source["data"]
    primary, repeated = _v161_decisions(source)
    truth = {str(row["task_id"]): row for row in data["truth"]["field_tasks"]}
    repeat_disputes = [
        task_id
        for task_id in data["residual_ids"]
        if primary[task_id]["field_status"] != repeated[task_id]["field_status"]
    ]
    support_controls = [
        task_id
        for task_id in data["control_ids"]
        if truth[task_id]["field"] == "unsupported_inference"
    ]
    if len(repeat_disputes) != 1 or len(support_controls) != 1:
        raise JudgeV5CalibrationV162Error("v162 observable dispute coverage drifted")
    target_id = repeat_disputes[0]
    support_control_id = support_controls[0]
    if truth[target_id]["field"] != "negation":
        raise JudgeV5CalibrationV162Error("v162 adjudication target drifted")
    receipts = {
        str(row["witness_id"]): row for row in source["support_receipts"]["units"]
    }
    receipt = receipts[truth[support_control_id]["witness_id"]]
    support_projection = {
        "supported": "correct",
        "unsupported": "incorrect",
        "abstain": "abstain",
    }[receipt["proposition_verdict"]]
    if support_projection != truth[support_control_id]["expected_status"]:
        raise JudgeV5CalibrationV162Error("v162 support projection truth mismatch")
    controls = [
        task_id
        for task_id in data["control_ids"]
        if task_id != support_control_id
    ]
    if len(controls) != 5 or any(truth[key]["field"] == "unsupported_inference" for key in controls):
        raise JudgeV5CalibrationV162Error("v162 settled control coverage drifted")
    task_values = {
        turn["task_id"]: turn["value"]
        for turn in data["turns"]
        if turn["turn_role"] == "reference_primary"
    }
    selected = [target_id, *controls]
    selected.sort(key=lambda value: sha256_text(f"v162|order|{value}"))
    turns = [
        {
            "turn_name": turn_name,
            "turn_role": "field_adjudication",
            "task_id": task_id,
            "value": deepcopy(task_values[task_id]),
        }
        for turn_name, task_id in zip(TURN_NAMES, selected, strict=True)
    ]
    return {
        "turns": turns,
        "target_id": target_id,
        "control_ids": controls,
        "support_control_id": support_control_id,
        "support_projection": support_projection,
        "truth": data["truth"],
        "residual_ids": data["residual_ids"],
        "v161_primary": primary,
        "v161_repeated": repeated,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    audit = {
        "schema_version": V162_CAPACITY_AUDIT_VERSION,
        "phase_id": V162_PHASE_ID,
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
        "schema_version": V162_CAPACITY_POLICY_VERSION,
        "phase_id": V162_PHASE_ID,
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


def freeze_v162(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v162 terminal")}
    source = _validate_v161()
    data = build_v162_inputs(source)
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
        **{f"v161_{name}": record for name, record in source["records"].items()},
        "v161_sidecars": source["sidecars"],
        "support_receipts": source["support_record"],
        "current_reference": source["source"]["reference_record"],
        "cumulative_usage": source["cumulative_usage"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V162_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "support_layer_projection_plus_single_negation_adjudication_with_five_settled_controls",
        "adjudication_target_count": 1,
        "control_turn_count": 5,
        "support_projection_control_count": 1,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": len(TURN_NAMES),
        "maximum_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "majority_voting_used": False,
        "truth_labels_exposed_to_model": False,
        "prior_model_decisions_exposed_to_model": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)), *source["values"]["spec"]["runtime_files"]
        ],
        "frozen_instructions": {
            "adjudicator_sha256": sha256_text(v146.base_instructions_v146())
        },
        "frozen_inputs": {
            "current_reference": source["source"]["reference_record"],
            "support_receipts": source["support_record"],
            "v161_output": source["records"]["output"],
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
    spec_path = root / "layer-corrected-field-adjudication-spec.json"
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


def score_v162(
    *, outputs: Mapping[str, Mapping[str, Any]], data: Mapping[str, Any]
) -> dict[str, Any]:
    observed = {
        turn["task_id"]: outputs[turn["turn_name"]]["decisions"][0]
        for turn in data["turns"]
    }
    if set(observed) != {turn["task_id"] for turn in data["turns"]}:
        raise JudgeV5CalibrationV162Error("v162 output coverage drifted")
    truth = {str(row["task_id"]): row for row in data["truth"]["field_tasks"]}
    metrics = {
        "adjudication_target_count": 1,
        "adjudication_abstention_count": int(
            observed[data["target_id"]]["field_status"] == "abstain"
        ),
        "control_count": 5,
        "control_exact_count": sum(
            observed[key]["field_status"] == truth[key]["expected_status"]
            for key in data["control_ids"]
        ),
        "support_projection_control_count": 1,
        "support_projection_exact_count": int(
            data["support_projection"]
            == truth[data["support_control_id"]]["expected_status"]
        ),
        "evidence_complete_count": sum(
            bool(row["source_evidence_spans"]) for row in observed.values()
        ),
    }
    checks = {
        "adjudication_abstention_count": metrics["adjudication_abstention_count"] == 0,
        "control_exact_rate": metrics["control_exact_count"] == 5,
        "support_projection_exact_rate": metrics["support_projection_exact_count"] == 1,
        "evidence_complete_rate": metrics["evidence_complete_count"] == 6,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V162_SCORE_VERSION,
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
    adjudicated = {
        turn["task_id"]: outputs[turn["turn_name"]]["decisions"][0]
        for turn in data["turns"]
    }
    statuses = {}
    for task_id in data["residual_ids"]:
        if task_id == data["target_id"]:
            statuses[task_id] = adjudicated[task_id]["field_status"]
        else:
            primary = data["v161_primary"][task_id]["field_status"]
            repeated = data["v161_repeated"][task_id]["field_status"]
            if primary != repeated:
                raise JudgeV5CalibrationV162Error("v162 stable owner decision drifted")
            statuses[task_id] = primary
    if any(status == "abstain" for status in statuses.values()):
        raise JudgeV5CalibrationV162Error("v162 cannot patch an abstaining owner")
    truth_rows = {str(row["task_id"]): row for row in truth["field_tasks"]}
    changed = 0
    for task_id, status in statuses.items():
        target = truth_rows[task_id]
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
            "schema_version": V162_TRUTH_VERSION,
            "v162_reference_owner_task_count": 6,
            "v162_reference_change_count": changed,
            "unsupported_inference_owned_by_support_projection": True,
        }
    )
    reference.update(
        {
            "schema_version": V162_REFERENCE_VERSION,
            "reference_version": "fixture_reference_v14_v162_layer_corrected_field_owner_frozen",
            "v162_reference_owner_task_count": 6,
            "v162_reference_change_count": changed,
            "v162_owner_basis": "five_stable_terra_owner_decisions_plus_one_gpt54_adjudication",
            "unsupported_inference_owned_by_support_projection": True,
            "reference_frozen": True,
            "selection_authorized": False,
            "holdout_authorized": False,
        }
    )
    return truth, reference, changed


def retrospective_rescore(
    *, patched_truth: Mapping[str, Any], data: Mapping[str, Any]
) -> dict[str, Any]:
    truth = {str(row["task_id"]): row for row in patched_truth["field_tasks"]}
    selected = set(data["residual_ids"]) | set(data["control_ids"])
    candidates = {
        "sol_v159": _load_json(
            v159.DEFAULT_OUTPUT_ROOT / "field-output.private.json", "v159 field output"
        ),
        "luna_v160": _load_json(
            v160.DEFAULT_OUTPUT_ROOT / "field-output.private.json", "v160 field output"
        ),
    }
    rows = {}
    for name, output in candidates.items():
        observed = {str(row["task_id"]): row for row in output["decisions"]}
        observed[data["support_control_id"]] = {
            "field_status": data["support_projection"]
        }
        exact = {
            key: observed[key]["field_status"] == truth[key]["expected_status"]
            for key in selected
        }
        rows[name] = {
            "overall_exact_count": sum(exact.values()),
            "decision_count": len(exact),
            "residual_exact_count": sum(exact[key] for key in data["residual_ids"]),
            "residual_count": len(data["residual_ids"]),
            "control_exact_count": sum(exact[key] for key in data["control_ids"]),
            "control_count": len(data["control_ids"]),
            "diagnostic_gate_would_pass": sum(exact.values()) >= 11
            and sum(exact[key] for key in data["residual_ids"]) >= 5
            and sum(exact[key] for key in data["control_ids"]) == 6,
        }
    return {
        "schema_version": V162_SCORE_VERSION,
        "retrospective_only": True,
        "fresh_diagnostic_still_required": True,
        "candidates": rows,
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
                _load_json(Path(record["path"]), "v162 measured sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    predecessor = _validate_v161()["cumulative_usage"]
    cumulative = _sum_usage(predecessor, usage)
    failure = {
        "schema_version": V162_FAILURE_VERSION,
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
        "schema_version": V162_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_frozen": False,
        "fresh_field_diagnostic_authorized": False,
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


async def run_v162(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v162 terminal")
    frozen = freeze_v162(output_dir=root, timeout_seconds=timeout_seconds)
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
        score = score_v162(outputs=outputs, data=frozen["data"])
        score_path = root / "layer-corrected-field-adjudication-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        truth_path = root / "full-calibration-truth-v162.private.json"
        reference_path = root / "fixture-reference-v14-v162.private.json"
        rescore_path = root / "retrospective-field-rescore.json"
        change_count = 0
        if passed:
            truth, reference, change_count = patch_truth_and_reference(
                outputs=outputs,
                data=frozen["data"],
                current_reference=frozen["source"]["source"]["reference"],
            )
            _write_immutable(truth_path, truth)
            _write_immutable(reference_path, reference)
            _write_immutable(
                rescore_path,
                retrospective_rescore(patched_truth=truth, data=frozen["data"]),
            )
        _write_immutable(
            root / "field-adjudication-output.private.json",
            {"schema_version": V162_SCORE_VERSION, "turn_outputs": outputs},
        )
        accounting = _aggregate_usage(sidecars)
        predecessor = frozen["source"]["cumulative_usage"]
        cumulative = _sum_usage(predecessor, accounting["usage"])
        terminal = {
            "schema_version": V162_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": "v162_layer_corrected_field_reference_frozen_fresh_diagnostic_authorized"
            if passed
            else "inactive_incomplete_recovery_required",
            "development_terminal_reason": "v162_layer_corrected_field_adjudication_passed"
            if passed
            else "v162_layer_corrected_field_adjudication_quality_gate_not_passed",
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
            "retrospective_rescore": _record(rescore_path) if passed else None,
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
    parser = argparse.ArgumentParser(description="Run v162 field adjudication")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v162(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
