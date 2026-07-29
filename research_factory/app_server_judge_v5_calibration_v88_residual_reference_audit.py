from __future__ import annotations

"""Blinded, control-calibrated reference audit for the three v87 residuals."""

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
    V23_ROOT,
    _field_contracts,
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v85_reference_v4_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V85_ROOT,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V86_ROOT,
    _record_matches,
    _verify_record,
    _write_stable_created,
)
from .app_server_judge_v5_calibration_v87_observable_repair import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V87_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _validate_usage,
)
from .util import now_iso, sha256_text


V88_INPUT_VERSION = "pif_app_server_judge_v5_4_v88_residual_reference_input_v1"
V88_TRUTH_VERSION = "pif_app_server_judge_v5_4_v88_residual_reference_truth_v1"
V88_SELECTION_VERSION = "pif_app_server_judge_v5_4_v88_selection_v1"
V88_SPEC_VERSION = "pif_app_server_judge_v5_4_v88_residual_reference_spec_v1"
V88_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v88_residual_reference_output_v1"
V88_SCORE_VERSION = "pif_app_server_judge_v5_4_v88_residual_reference_score_v1"
V88_AUDIT_VERSION = "pif_app_server_judge_v5_4_v88_projection_audit_v1"
V88_FAILURE_VERSION = "pif_app_server_judge_v5_4_v88_failure_v1"
V88_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v88_terminal_v1"
V88_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V88_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V88_PHASE_ID = "judge_v5_4_v88_residual_reference_audit"
MODEL = "gpt-5.6-luna"
EFFORT = "high"
PRIMARY_TURNS = tuple(f"residual_reference_shard_{index:02d}" for index in range(3))
CANARY_TURN = "residual_reference_permutation_canary"
TURN_NAMES = PRIMARY_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V87_ROOT.parent / "judge-calibration-v5_4-v88-residual-reference-audit"
).resolve()


class JudgeV5CalibrationV88Error(RuntimeError):
    """The v88 reference-owner audit cannot preserve its contract."""


def _validate_predecessors(
    *, v87_root: Path, v86_root: Path, v85_root: Path, v23_root: Path
) -> dict[str, Any]:
    paths = {
        "v87_terminal": v87_root / "terminal.json",
        "v87_spec": v87_root / "observable-repair-spec.json",
        "v87_audit": v87_root / "reconciliation-audit.json",
        "v87_score": v87_root / "observable-repair-score.json",
        "v86_spec": v86_root / "fresh-enhanced-spec.json",
        "v86_input": v86_root / "fresh-enhanced-input.private.json",
        "v86_truth": v86_root / "fresh-enhanced-truth.private.json",
        "v85_truth": v85_root / "calibration-truth-v4.private.json",
        "v23_pointwise": v23_root / "pointwise-input-full.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v87_terminal"]
    v87_spec = values["v87_spec"]
    audit = values["v87_audit"]
    v86_spec = values["v86_spec"]
    residuals = audit.get("residual_mismatches") or []
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v87_repair_completed_residual_reference_audit_required"
        or terminal.get("repair_completed") is not True
        or terminal.get("diagnostic_passed") is not False
        or terminal.get("residual_reference_audit_authorized") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or audit.get("residual_mismatch_count") != 3
        or len(residuals) != 3
        or sorted(row.get("field") for row in residuals)
        != ["certainty", "target", "temporal_horizon"]
        or not _record_matches(terminal.get("reconciliation_audit"), paths["v87_audit"])
        or not _record_matches(terminal.get("score"), paths["v87_score"])
        or not all(_verify_record(row) for row in v87_spec.get("runtime_files") or [])
        or not _record_matches(v86_spec.get("frozen_inputs", {}).get("input"), paths["v86_input"])
        or not _record_matches(v86_spec.get("frozen_inputs", {}).get("truth"), paths["v86_truth"])
        or not _record_matches(v86_spec.get("predecessor", {}).get("v85_truth"), paths["v85_truth"])
        or not _record_matches(
            v86_spec.get("predecessor", {}).get("v23_pointwise"), paths["v23_pointwise"]
        )
        or not all(_verify_record(row) for row in v86_spec.get("runtime_files") or [])
    ):
        raise JudgeV5CalibrationV88Error("v85/v86/v87 reference-audit contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def _owner_task_id(case_id: str, witness_id: str, field: str, role: str) -> str:
    return "ref_" + sha256_text(f"v88|{role}|{case_id}|{witness_id}|{field}")[:24]


def _canary_task_id(owner_task_id: str) -> str:
    return "perm_" + sha256_text(f"v88|canary|{owner_task_id}")[:24]


def build_v88_inputs(
    *,
    pointwise: Mapping[str, Any],
    reference: Mapping[str, Any],
    v86_input: Mapping[str, Any],
    v86_truth: Mapping[str, Any],
    residuals: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    contracts = _field_contracts()
    source_by_identity = {
        (row["case_id"], row["witness_id"]): row for row in pointwise.get("units") or []
    }
    v86_truth_by_task = {row["task_id"]: row for row in v86_truth.get("tasks") or []}
    v86_task_by_id = {row["task_id"]: row for row in v86_input.get("tasks") or []}
    excluded = {
        (row["case_id"], row["witness_id"]) for row in v86_truth.get("tasks") or []
    }
    selected: list[
        tuple[str, str, Optional[str], Mapping[str, Any], Mapping[str, Any]]
    ] = []
    used = set(excluded)
    for residual in sorted(residuals, key=lambda row: row["task_id"]):
        truth_row = v86_truth_by_task.get(residual["task_id"])
        task = v86_task_by_id.get(residual["task_id"])
        if not truth_row or not task or truth_row["field"] != residual["field"]:
            raise JudgeV5CalibrationV88Error("v88 residual identity drifted")
        selected.append(("reference_dispute", truth_row["field"], None, truth_row, task))
    for field in ("certainty", "target", "temporal_horizon"):
        for status in ("correct", "incorrect"):
            candidates = []
            for identity, unit in source_by_identity.items():
                if identity in used:
                    continue
                issues = set(reference["cases"][identity[0]]["field_issues"].get(identity[1], []))
                expected = "incorrect" if field in issues else "correct"
                if expected == status:
                    candidates.append(unit)
            candidates.sort(
                key=lambda row: sha256_text(
                    f"v88-control|{field}|{status}|{row['case_id']}|{row['witness_id']}"
                )
            )
            if not candidates:
                raise JudgeV5CalibrationV88Error("v88 matched-control slot unavailable")
            unit = candidates[0]
            identity = (unit["case_id"], unit["witness_id"])
            used.add(identity)
            selected.append(("matched_control", field, status, unit, unit))
    tasks = []
    truth_rows = []
    for role, field, control_status, identity_row, source_row in selected:
        case_id = identity_row["case_id"]
        witness_id = identity_row["witness_id"]
        task_id = _owner_task_id(case_id, witness_id, field, role)
        tasks.append(
            {
                "task_id": task_id,
                "field": field,
                "field_contract": deepcopy(contracts[field]),
                "source_excerpt": source_row["source_excerpt"],
                "structured_event": deepcopy(source_row["structured_event"]),
            }
        )
        prior_status = (
            identity_row["expected_status"] if role == "reference_dispute" else control_status
        )
        truth_rows.append(
            {
                "task_id": task_id,
                "role": role,
                "case_id": case_id,
                "witness_id": witness_id,
                "field": field,
                "prior_status": prior_status,
                "control_expected_status": control_status,
            }
        )
    if len(tasks) != 9 or len({row["task_id"] for row in tasks}) != 9:
        raise JudgeV5CalibrationV88Error("v88 task coverage drifted")
    tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    task_by_id = {row["task_id"]: row for row in tasks}
    dispute_ids = [row["task_id"] for row in truth_rows if row["role"] == "reference_dispute"]
    canary_tasks = []
    canary_map = []
    for owner_task_id in reversed(sorted(dispute_ids)):
        task = deepcopy(task_by_id[owner_task_id])
        canary_task_id = _canary_task_id(owner_task_id)
        task["task_id"] = canary_task_id
        canary_tasks.append(task)
        canary_map.append({"canary_task_id": canary_task_id, "owner_task_id": owner_task_id})
    value = {
        "schema_version": V88_INPUT_VERSION,
        "task_count": 9,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth = {
        "schema_version": V88_TRUTH_VERSION,
        "task_count": 9,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V88_INPUT_VERSION,
        "task_count": 3,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V88_SELECTION_VERSION,
        "created_at": now_iso(),
        "task_count": 9,
        "role_counts": {"reference_dispute": 3, "matched_control": 6},
        "field_counts": dict(sorted(Counter(row["field"] for row in truth_rows).items())),
        "control_status_counts": {"correct": 3, "incorrect": 3},
        "permutation_canary_count": 3,
        "control_selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return value, truth, canary, selection


def base_instructions_v88() -> str:
    return (
        "You are the neutral reference owner for independent source-to-field verification. Align the event to "
        "the exact proposition expressed by its evidence, claim, and target, then judge only the requested "
        "field while mentally correcting every other field. For target, harmless paraphrase or coreference "
        "that preserves referent and scope is correct; changed scope is incorrect. For certainty, judge the "
        "source's actual epistemic commitment: unqualified assertions, hedges, estimates, and modal language "
        "can differ materially. For temporal_horizon, interpret the proposition's semantic time reference, "
        "including tense, aspect, explicit dates, durations, and conditional timing; do not borrow timing from "
        "an adjacent proposition. The first evidence span must exactly express the aligned proposition. Abstain "
        "only when the source genuinely cannot determine the requested field. Do not use regex, keywords, "
        "overlap, embeddings, prior labels, system identity, confidence, or voting."
    )


def build_prompt_v88(value: Mapping[str, Any]) -> str:
    return (
        "Return one field decision for every opaque task_id. Do not compare tasks or emit whole-event verdicts. "
        "Every source_evidence_span must be an exact substring of that task's source_excerpt.\n\n"
        + json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    )


def primary_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value.get("tasks") or []
    if len(tasks) != 9:
        raise JudgeV5CalibrationV88Error("v88 shard coverage drifted")
    return [
        {
            **{key: deepcopy(child) for key, child in value.items() if key != "tasks"},
            "task_count": 3,
            "tasks": deepcopy(tasks[index : index + 3]),
            "shard_ordinal": index // 3,
            "shard_count": 3,
        }
        for index in range(0, 9, 3)
    ]


def merge_outputs(outputs: Sequence[Mapping[str, Any]], expected_count: int) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(decisions) != expected_count or len({row["task_id"] for row in decisions}) != expected_count:
        raise JudgeV5CalibrationV88Error("v88 output coverage drifted")
    return {"schema_version": V88_OUTPUT_VERSION, "decisions": decisions}


def score_v88(
    owner: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in owner["decisions"]}
    repeated = {row["task_id"]: row for row in canary["decisions"]}
    if set(expected) != set(observed) or len(repeated) != 3:
        raise JudgeV5CalibrationV88Error("v88 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "matched_control"]
    disputes = [row for row in expected.values() if row["role"] == "reference_dispute"]
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
        "matched_control_exact_rate": control_exact == 6,
        "permutation_canary_exact_rate": canary_exact == 3,
        "abstention_count": abstentions == 0,
        "evidence_complete_rate": evidence_complete == 12,
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
        for row in disputes
    ]
    return {
        "schema_version": V88_SCORE_VERSION,
        "passed": passed,
        "metrics": {
            "task_count": 9,
            "reference_dispute_count": 3,
            "matched_control_count": 6,
            "matched_control_exact_count": control_exact,
            "matched_control_exact_rate": round(control_exact / 6, 6),
            "permutation_canary_count": 3,
            "permutation_canary_exact_count": canary_exact,
            "permutation_canary_exact_rate": round(canary_exact / 3, 6),
            "abstention_count": abstentions,
            "evidence_complete_count": evidence_complete,
            "evidence_complete_rate": round(evidence_complete / 12, 6),
            "reference_change_count": sum(row["reference_change"] for row in proposal),
        },
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "reference_patch_proposal": proposal if passed else [],
        "reference_patch_authorized": passed,
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
        "schema_version": V88_CAPACITY_AUDIT_VERSION,
        "phase_id": V88_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_created(audit_path, audit, "v88 capacity audit")
    policy = {
        "schema_version": V88_CAPACITY_POLICY_VERSION,
        "phase_id": V88_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v88 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v88(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v87_root: Path = DEFAULT_V87_ROOT,
    v86_root: Path = DEFAULT_V86_ROOT,
    v85_root: Path = DEFAULT_V85_ROOT,
    v23_root: Path = V23_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors(
        v87_root=v87_root.resolve(),
        v86_root=v86_root.resolve(),
        v85_root=v85_root.resolve(),
        v23_root=v23_root.resolve(),
    )
    values = predecessor["values"]
    value, truth, canary, selection = build_v88_inputs(
        pointwise=values["v23_pointwise"],
        reference=values["v85_truth"],
        v86_input=values["v86_input"],
        v86_truth=values["v86_truth"],
        residuals=values["v87_audit"]["residual_mismatches"],
    )
    input_path = root / "residual-reference-input.private.json"
    truth_path = root / "residual-reference-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_created(selection_path, selection, "v88 selection audit")
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
        "schema_version": V88_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "blinded_residual_reference_owner_with_balanced_same_field_controls_and_permutation_canaries",
        "task_count": 9,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_six_controls_all_three_canaries_all_twelve_evidence_receipts_and_zero_abstentions",
        "reference_patch_authorized": False,
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
            _record(runtime_dir / "app_server_judge_v5_calibration_v87_observable_repair.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v85_reference_v4_freeze.py"),
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
    spec_path = root / "residual-reference-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v88 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV88Error("immutable v88 spec drifted")
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v88 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V88_FAILURE_VERSION,
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
        "schema_version": V88_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_patch_authorized": False,
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


async def run_v88(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v88 terminal")
    frozen = freeze_v88(output_dir=root, timeout_seconds=timeout_seconds)
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
                    raise JudgeV5CalibrationV88Error("projected v88 output is invalid")
                if current_turn == CANARY_TURN:
                    canary_outputs.append(projected)
                else:
                    primary_outputs.append(projected)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
                operations.extend([{**row, "turn_name": current_turn} for row in turn_operations])
        owner = merge_outputs(primary_outputs, 9)
        canary = merge_outputs(canary_outputs, 3)
        owner_path = root / "residual-reference-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(owner_path, owner)
        _write_immutable(canary_path, canary)
        audit = {
            "schema_version": V88_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "projection_scope": "nonexact_source_span_removal_only",
            "semantic_status_changed": False,
            "privacy": "opaque_task_ids_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v88(owner, canary, frozen["truth"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "residual-reference-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V88_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v88_residual_reference_audit_passed_patch_freeze_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v88_residual_reference_audit_passed_patch_freeze_authorized"
                if passed
                else "v88_residual_reference_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reference_patch_authorized": passed,
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
    parser = argparse.ArgumentParser(description="Run v88 residual reference audit")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v88(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_patch_authorized": terminal.get("reference_patch_authorized", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
