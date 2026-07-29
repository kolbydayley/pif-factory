from __future__ import annotations

"""Blind GPT-5.5 reference-owner audit for v77/v78 disputed field labels."""

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
from .app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V75_ROOT,
    project_exact_spans,
)
from .app_server_judge_v5_calibration_v76_neutral_contested_adjudication import (
    base_instructions,
    build_prompt,
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v77_reconcile_or_abstain_design import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V77_ROOT,
)
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V78_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso, sha256_text


V79_INPUT_VERSION = "pif_app_server_judge_v5_4_v79_reference_owner_input_v1"
V79_TRUTH_VERSION = "pif_app_server_judge_v5_4_v79_reference_owner_truth_v1"
V79_SELECTION_VERSION = "pif_app_server_judge_v5_4_v79_reference_owner_selection_v1"
V79_SPEC_VERSION = "pif_app_server_judge_v5_4_v79_reference_owner_spec_v1"
V79_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v79_reference_owner_output_v1"
V79_PROPOSAL_VERSION = "pif_app_server_judge_v5_4_v79_reference_patch_proposal_v1"
V79_AUDIT_VERSION = "pif_app_server_judge_v5_4_v79_projection_audit_v1"
V79_SCORE_VERSION = "pif_app_server_judge_v5_4_v79_reference_owner_score_v1"
V79_FAILURE_VERSION = "pif_app_server_judge_v5_4_v79_failure_v1"
V79_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v79_terminal_v1"
V79_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V79_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V79_PHASE_ID = "judge_v5_4_v79_blind_reference_owner"
MODEL = "gpt-5.5"
EFFORT = "high"
TASKS_PER_SHARD = 3
OWNER_TURNS = tuple(f"reference_owner_shard_{index:02d}" for index in range(5))
CANARY_TURN = "reference_owner_permutation_canary"
TURN_NAMES = OWNER_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V78_ROOT.parent / "judge-calibration-v5_4-v79-blind-reference-owner"
).resolve()


class JudgeV5CalibrationV79Error(RuntimeError):
    """The v79 reference-owner contract cannot be preserved."""


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


def _validate_predecessors(
    *, v75_root: Path, v77_root: Path, v78_root: Path
) -> dict[str, Any]:
    paths = {
        "v75_spec": v75_root / "exact-span-remaining-shard-spec.json",
        "v75_input": v75_root / "direct-field-input.private.json",
        "v77_terminal": v77_root / "terminal.json",
        "v77_receipt": v77_root / "reconciliation-receipt.json",
        "v77_delta": v77_root / "reconciled-delta.private.json",
        "v78_terminal": v78_root / "terminal.json",
        "v78_spec": v78_root / "fresh-reconcile-spec.json",
        "v78_input": v78_root / "fresh-input.private.json",
        "v78_truth": v78_root / "fresh-truth.private.json",
        "v78_primary": v78_root / "primary-output.private.json",
        "v78_adjudicator": v78_root / "adjudicator-output.private.json",
        "v78_reconciled": v78_root / "reconciled-output.private.json",
        "v78_score": v78_root / "fresh-reconcile-score.json",
        "v78_projection": v78_root / "projection-audit.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    v75_spec = values["v75_spec"]
    v77_terminal = values["v77_terminal"]
    v77_receipt = values["v77_receipt"]
    v78_terminal = values["v78_terminal"]
    v78_spec = values["v78_spec"]
    v78_score = values["v78_score"]
    if (
        not _record_matches(v75_spec.get("frozen_inputs", {}).get("input"), paths["v75_input"])
        or not all(_verify_record(row) for row in v75_spec.get("runtime_files") or [])
        or v77_terminal.get("state") != "completed"
        or v77_terminal.get("fresh_15_development_diagnostic_authorized") is not True
        or v77_terminal.get("semantic_attempt_started") is not False
        or v77_terminal.get("production_mutated") is not False
        or not _record_matches(v77_terminal.get("reconciliation_receipt"), paths["v77_receipt"])
        or not _record_matches(v77_terminal.get("reconciled_delta"), paths["v77_delta"])
        or not all(_verify_record(row) for row in (v77_terminal.get("predecessors") or {}).values())
        or v77_receipt.get("settled_change_count") != 4
        or v77_receipt.get("abstention_count") != 3
        or v77_receipt.get("majority_voting_used") is not False
        or v78_terminal.get("state") != "inactive"
        or v78_terminal.get("development_terminal_reason")
        != "v78_fresh_reconcile_quality_gate_not_passed"
        or v78_terminal.get("usage_status") != "complete"
        or v78_terminal.get("accounting_complete") is not True
        or v78_terminal.get("semantic_retry_count") != 0
        or v78_terminal.get("diagnostic_passed") is not False
        or v78_terminal.get("production_mutated") is not False
        or len(v78_terminal.get("attempts") or []) != 10
        or not all(
            _verify_record(record)
            for attempt in v78_terminal.get("attempts") or []
            for record in (attempt.get("capacity"), attempt.get("sidecar"), attempt.get("output"))
        )
        or not _record_matches(v78_terminal.get("primary_output"), paths["v78_primary"])
        or not _record_matches(v78_terminal.get("adjudicator_output"), paths["v78_adjudicator"])
        or not _record_matches(v78_terminal.get("reconciled_output"), paths["v78_reconciled"])
        or not _record_matches(v78_terminal.get("score"), paths["v78_score"])
        or not _record_matches(v78_terminal.get("projection_audit"), paths["v78_projection"])
        or not _record_matches(v78_spec.get("frozen_inputs", {}).get("input"), paths["v78_input"])
        or not _record_matches(v78_spec.get("frozen_inputs", {}).get("truth"), paths["v78_truth"])
        or not all(_verify_record(row) for row in v78_spec.get("runtime_files") or [])
        or v78_score.get("metrics", {}).get("exact_count") != 13
        or v78_score.get("metrics", {}).get("task_count") != 15
        or v78_score.get("metrics", {}).get("independent_agreement_count") != 14
        or v78_score.get("metrics", {}).get("incorrect_sensitivity") != 1.0
    ):
        raise JudgeV5CalibrationV79Error("v75/v77/v78 predecessor contract drifted")
    return {name: _record(path) for name, path in paths.items()}


def _owner_task_id(source_version: str, original_task_id: str) -> str:
    return "owner_" + sha256_text(f"v79|{source_version}|{original_task_id}")[:24]


def _canary_task_id(owner_task_id: str) -> str:
    return "perm_" + sha256_text(f"v79|canary|{owner_task_id}")[:24]


def build_v79_inputs(
    *,
    v75_input: Mapping[str, Any],
    v77_delta: Mapping[str, Any],
    v78_input: Mapping[str, Any],
    v78_truth: Mapping[str, Any],
    v78_primary: Mapping[str, Any],
    v78_adjudicator: Mapping[str, Any],
    v78_reconciled: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    source75 = {row["task_id"]: row for row in v75_input.get("tasks") or []}
    source78 = {row["task_id"]: row for row in v78_input.get("tasks") or []}
    truth78 = {row["task_id"]: row for row in v78_truth.get("tasks") or []}
    primary78 = {row["task_id"]: row for row in v78_primary.get("decisions") or []}
    adjudicator78 = {row["task_id"]: row for row in v78_adjudicator.get("decisions") or []}
    reconciled78 = {row["task_id"]: row for row in v78_reconciled.get("decisions") or []}
    if not source75 or set(source78) != set(truth78) or set(source78) != set(primary78):
        raise JudgeV5CalibrationV79Error("reference-owner source coverage drifted")
    if set(source78) != set(adjudicator78) or set(source78) != set(reconciled78):
        raise JudgeV5CalibrationV79Error("reference-owner output coverage drifted")

    selected: list[dict[str, Any]] = []
    for row in v77_delta.get("rows") or []:
        original_task_id = row["original_task_id"]
        if original_task_id not in source75:
            raise JudgeV5CalibrationV79Error("v77 source task is absent")
        if row["reconciled_status"] == "abstain":
            role = "unresolved"
        elif row["prior_status"] != row["reconciled_status"]:
            role = "proposed_change"
        else:
            continue
        selected.append(
            {
                "source_version": "v75_v77",
                "original_task_id": original_task_id,
                "role": role,
                "prior_status": row["prior_status"],
                "primary_status": row["primary_status"],
                "adjudicator_status": row["adjudicated_status"],
                "proposed_status": row["reconciled_status"],
                "task": source75[original_task_id],
            }
        )

    v78_failures = []
    for task_id, truth_row in truth78.items():
        reconciled_status = reconciled78[task_id]["reconciled_status"]
        if reconciled_status == truth_row["expected_status"]:
            continue
        primary_status = primary78[task_id]["field_status"]
        adjudicator_status = adjudicator78[task_id]["field_status"]
        role = (
            "proposed_change"
            if primary_status == adjudicator_status and primary_status in {"correct", "incorrect"}
            else "unresolved"
        )
        v78_failures.append((task_id, role))
        selected.append(
            {
                "source_version": "v78",
                "original_task_id": task_id,
                "role": role,
                "prior_status": truth_row["expected_status"],
                "primary_status": primary_status,
                "adjudicator_status": adjudicator_status,
                "proposed_status": reconciled_status,
                "task": source78[task_id],
            }
        )
    role_counts = {
        role: sum(row["role"] == role for row in selected)
        for role in ("proposed_change", "unresolved")
    }
    if role_counts != {"proposed_change": 5, "unresolved": 4} or len(v78_failures) != 2:
        raise JudgeV5CalibrationV79Error("reference-owner disputed-row selection drifted")

    exact_controls = []
    for task_id, truth_row in truth78.items():
        expected = truth_row["expected_status"]
        if (
            reconciled78[task_id]["reconciled_status"] == expected
            and primary78[task_id]["field_status"] == expected
            and adjudicator78[task_id]["field_status"] == expected
        ):
            exact_controls.append((task_id, expected))
    controls = []
    for expected in ("correct", "incorrect"):
        candidates = sorted(
            (task_id for task_id, status in exact_controls if status == expected),
            key=lambda task_id: sha256_text(f"v79|control|{task_id}"),
        )
        if len(candidates) < 3:
            raise JudgeV5CalibrationV79Error("reference-owner control pool is too small")
        for task_id in candidates[:3]:
            controls.append(
                {
                    "source_version": "v78",
                    "original_task_id": task_id,
                    "role": "matched_control",
                    "prior_status": expected,
                    "primary_status": expected,
                    "adjudicator_status": expected,
                    "proposed_status": expected,
                    "task": source78[task_id],
                }
            )
    selected.extend(controls)
    if len(selected) != 15 or len({row["original_task_id"] for row in selected}) != 15:
        raise JudgeV5CalibrationV79Error("reference-owner selection is not 15 distinct tasks")

    model_tasks = []
    truth_rows = []
    for row in selected:
        owner_task_id = _owner_task_id(row["source_version"], row["original_task_id"])
        task = row["task"]
        model_tasks.append(
            {
                "task_id": owner_task_id,
                "field": task["field"],
                "field_contract": deepcopy(task["field_contract"]),
                "source_excerpt": task["source_excerpt"],
                "structured_event": deepcopy(task["structured_event"]),
            }
        )
        truth_rows.append(
            {
                "task_id": owner_task_id,
                "source_version": row["source_version"],
                "original_task_id": row["original_task_id"],
                "field": task["field"],
                "role": row["role"],
                "prior_status": row["prior_status"],
                "primary_status": row["primary_status"],
                "adjudicator_status": row["adjudicator_status"],
                "proposed_status": row["proposed_status"],
                "control_expected_status": (
                    row["prior_status"] if row["role"] == "matched_control" else None
                ),
            }
        )
    model_tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    task_by_id = {row["task_id"]: row for row in model_tasks}
    audit_ids = [row["task_id"] for row in truth_rows if row["role"] != "matched_control"]
    canary_owner_ids = sorted(audit_ids, key=lambda task_id: sha256_text(f"v79|perm|{task_id}"))[:3]
    canary_tasks = []
    canary_map = []
    for owner_task_id in reversed(canary_owner_ids):
        task = deepcopy(task_by_id[owner_task_id])
        canary_task_id = _canary_task_id(owner_task_id)
        task["task_id"] = canary_task_id
        canary_tasks.append(task)
        canary_map.append({"canary_task_id": canary_task_id, "owner_task_id": owner_task_id})

    value = {
        "schema_version": V79_INPUT_VERSION,
        "task_count": 15,
        "tasks": model_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V79_TRUTH_VERSION,
        "task_count": 15,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V79_INPUT_VERSION,
        "task_count": 3,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V79_SELECTION_VERSION,
        "created_at": now_iso(),
        "task_count": 15,
        "role_counts": {"matched_control": 6, **role_counts},
        "control_status_counts": {"correct": 3, "incorrect": 3},
        "permutation_canary_count": 3,
        "selection_uses_source_text": False,
        "reference_owner_model_input_contains_prior_labels": False,
        "reference_owner_model_input_contains_prior_model_decisions": False,
        "majority_voting_used": False,
        "privacy": "aggregate_counts_only",
    }
    return value, truth_value, canary, selection


def _owner_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value.get("tasks") or []
    if len(tasks) != 15:
        raise JudgeV5CalibrationV79Error("reference-owner task count drifted")
    return [
        {
            **{key: deepcopy(child) for key, child in value.items() if key != "tasks"},
            "task_count": TASKS_PER_SHARD,
            "tasks": deepcopy(tasks[index : index + TASKS_PER_SHARD]),
            "shard_ordinal": index // TASKS_PER_SHARD,
            "shard_count": 5,
        }
        for index in range(0, 15, TASKS_PER_SHARD)
    ]


def merge_outputs(outputs: Sequence[Mapping[str, Any]], expected_count: int) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(decisions) != expected_count or len({row["task_id"] for row in decisions}) != expected_count:
        raise JudgeV5CalibrationV79Error("reference-owner output coverage drifted")
    return {"schema_version": V79_OUTPUT_VERSION, "decisions": decisions}


def score_v79(
    owner_output: Mapping[str, Any],
    canary_output: Mapping[str, Any],
    truth: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in owner_output["decisions"]}
    canary = {row["task_id"]: row for row in canary_output["decisions"]}
    if set(expected) != set(observed) or len(canary) != 3:
        raise JudgeV5CalibrationV79Error("reference-owner score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "matched_control"]
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    canary_exact = sum(
        canary[row["canary_task_id"]]["field_status"]
        == observed[row["owner_task_id"]]["field_status"]
        for row in truth["canary_map"]
    )
    owner_abstentions = sum(row["field_status"] == "abstain" for row in observed.values())
    canary_abstentions = sum(row["field_status"] == "abstain" for row in canary.values())
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    evidence_complete += sum(bool(row["source_evidence_spans"]) for row in canary.values())
    checks = {
        "matched_control_exact_rate": control_exact == 6,
        "permutation_canary_exact_rate": canary_exact == 3,
        "owner_abstention_count": owner_abstentions == 0,
        "canary_abstention_count": canary_abstentions == 0,
        "evidence_complete_rate": evidence_complete == 18,
    }
    passed = all(checks.values())
    proposal = [
        {
            "source_version": row["source_version"],
            "original_task_id": row["original_task_id"],
            "field": row["field"],
            "prior_status": row["prior_status"],
            "owner_status": observed[row["task_id"]]["field_status"],
            "reference_change": observed[row["task_id"]]["field_status"] != row["prior_status"],
        }
        for row in expected.values()
        if row["role"] != "matched_control"
    ]
    return {
        "schema_version": V79_SCORE_VERSION,
        "passed": passed,
        "metrics": {
            "task_count": 15,
            "audited_reference_row_count": 9,
            "matched_control_count": 6,
            "matched_control_exact_count": control_exact,
            "matched_control_exact_rate": round(control_exact / 6, 6),
            "permutation_canary_count": 3,
            "permutation_canary_exact_count": canary_exact,
            "permutation_canary_exact_rate": round(canary_exact / 3, 6),
            "owner_abstention_count": owner_abstentions,
            "canary_abstention_count": canary_abstentions,
            "evidence_complete_count": evidence_complete,
            "evidence_complete_rate": round(evidence_complete / 18, 6),
            "reference_change_count": sum(row["reference_change"] for row in proposal),
        },
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "reference_patch_proposal": proposal if passed else [],
        "reference_freeze_authorized": passed,
        "fresh_primary_repair_diagnostic_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "majority_voting_used": False,
        "gates_frozen_before_semantic_calls": True,
    }


def _write_stable_created(path: Path, value: Mapping[str, Any], purpose: str) -> dict[str, Any]:
    candidate = deepcopy(value)
    if path.exists():
        prior = _load_json(path, purpose)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV79Error(f"immutable {purpose} drifted")
        return prior
    _write_immutable(path, candidate)
    return candidate


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    phase_bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V79_CAPACITY_AUDIT_VERSION,
        "phase_id": V79_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": phase_bound,
        },
    }
    _write_stable_created(audit_path, audit, "v79 capacity audit")
    policy = {
        "schema_version": V79_CAPACITY_POLICY_VERSION,
        "phase_id": V79_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v79 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v79(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v75_root: Path = DEFAULT_V75_ROOT,
    v77_root: Path = DEFAULT_V77_ROOT,
    v78_root: Path = DEFAULT_V78_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors(
        v75_root=v75_root.resolve(), v77_root=v77_root.resolve(), v78_root=v78_root.resolve()
    )
    value, truth, canary, selection = build_v79_inputs(
        v75_input=_load_json(v75_root / "direct-field-input.private.json", "v75 input"),
        v77_delta=_load_json(v77_root / "reconciled-delta.private.json", "v77 delta"),
        v78_input=_load_json(v78_root / "fresh-input.private.json", "v78 input"),
        v78_truth=_load_json(v78_root / "fresh-truth.private.json", "v78 truth"),
        v78_primary=_load_json(v78_root / "primary-output.private.json", "v78 primary"),
        v78_adjudicator=_load_json(v78_root / "adjudicator-output.private.json", "v78 adjudicator"),
        v78_reconciled=_load_json(v78_root / "reconciled-output.private.json", "v78 reconciled"),
    )
    input_path = root / "reference-owner-input.private.json"
    truth_path = root / "reference-owner-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_created(selection_path, selection, "v79 selection audit")
    turns = []
    turn_values = _owner_shards(value) + [canary]
    for turn_name, turn_value in zip(TURN_NAMES, turn_values, strict=True):
        prompt = build_prompt(turn_value)
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
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V79_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "blind_reference_owner_with_matched_controls_and_small_permutation_canary",
        "task_count": 15,
        "audited_reference_row_count": 9,
        "matched_control_count": 6,
        "permutation_canary_count": 3,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "pass_authorizes_deterministic_versioned_reference_freeze_only",
        "reference_freeze_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v77_reconcile_or_abstain_design.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v76_neutral_contested_adjudication.py"),
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
    spec_path = root / "reference-owner-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v79 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV79Error("immutable v79 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
        "truth": truth,
        "canary": canary,
        "turns": turns,
        "capacity_policy": capacity["policy"],
    }


def _real_attempts(root: Path) -> list[dict[str, Any]]:
    return [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]


def _write_failure(root: Path, *, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = _real_attempts(root)
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v79 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V79_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "attempts": attempts,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
        "unknown_usage_attempt_count": unknown,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V79_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_freeze_authorized": False,
        "fresh_primary_repair_diagnostic_authorized": False,
        "full_calibration_authorized": False,
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


async def run_v79(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v79 terminal")
    frozen = freeze_v79(output_dir=root, timeout_seconds=timeout_seconds)
    factory = client_factory or _client_factory
    owner_outputs = []
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
                    base_instructions=base_instructions(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=TASKS_PER_SHARD,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, value=turn["value"]: validate_output(
                        project_exact_spans(candidate, value)[0], value
                    ),
                )
                projected, turn_operations = project_exact_spans(output, turn["value"])
                errors = validate_output(projected, turn["value"])
                if errors:
                    raise JudgeV5CalibrationV79Error("projected reference-owner output is invalid")
                if current_turn == CANARY_TURN:
                    canary_outputs.append(projected)
                else:
                    owner_outputs.append(projected)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
                operations.extend(
                    [{**row, "turn_name": current_turn} for row in turn_operations]
                )
        owner = merge_outputs(owner_outputs, 15)
        canary = merge_outputs(canary_outputs, 3)
        owner_path = root / "reference-owner-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(owner_path, owner)
        _write_immutable(canary_path, canary)
        audit = {
            "schema_version": V79_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "projection_scope": "nonexact_source_span_removal_only",
            "semantic_status_changed": False,
            "majority_voting_used": False,
            "privacy": "opaque_task_ids_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v79(owner, canary, frozen["truth"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "reference-owner-score.json"
        _write_immutable(score_path, score)
        proposal = {
            "schema_version": V79_PROPOSAL_VERSION,
            "created_at": now_iso(),
            "state": "authorized" if score["passed"] else "suppressed",
            "reference_owner_model": MODEL,
            "audited_reference_row_count": 9,
            "rows": score["reference_patch_proposal"],
            "reference_freeze_authorized": score["reference_freeze_authorized"],
            "fresh_diagnostic_required_after_freeze": True,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "majority_voting_used": False,
        }
        proposal_path = root / "reference-patch-proposal.json"
        _write_immutable(proposal_path, proposal)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V79_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v79_reference_owner_passed_reference_freeze_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v79_reference_owner_passed_reference_freeze_authorized"
                if passed
                else "v79_reference_owner_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reference_owner_passed": passed,
            "reference_freeze_authorized": passed,
            "fresh_primary_repair_diagnostic_authorized": False,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "reference_patch_proposal": _record(proposal_path),
            "owner_output": _record(owner_path),
            "canary_output": _record(canary_path),
            "projection_audit": _record(audit_path),
            "attempts": _real_attempts(root),
            "completed_checkpoint_adoptions": adoptions,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, turn_name=exc.turn_name, error_class=exc.error_class)
    except Exception as exc:
        return _write_failure(root, turn_name=current_turn, error_class=type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v79 blind reference-owner audit")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v79(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_owner_passed": terminal.get("reference_owner_passed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
