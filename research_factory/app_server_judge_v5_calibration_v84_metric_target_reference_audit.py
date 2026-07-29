from __future__ import annotations

"""Blind metric/target reference audit for the three v83 semantic misses."""

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
    DEFAULT_OUTPUT_ROOT as DEFAULT_V78_ROOT,
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v82_reference_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V82_ROOT,
)
from .app_server_judge_v5_calibration_v83_fresh_alignment_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V83_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _canonical_json,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso, sha256_text


V84_INPUT_VERSION = "pif_app_server_judge_v5_4_v84_metric_target_input_v1"
V84_TRUTH_VERSION = "pif_app_server_judge_v5_4_v84_metric_target_truth_v1"
V84_SELECTION_VERSION = "pif_app_server_judge_v5_4_v84_selection_v1"
V84_SPEC_VERSION = "pif_app_server_judge_v5_4_v84_metric_target_spec_v1"
V84_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v84_metric_target_output_v1"
V84_SCORE_VERSION = "pif_app_server_judge_v5_4_v84_metric_target_score_v1"
V84_PROPOSAL_VERSION = "pif_app_server_judge_v5_4_v84_reference_proposal_v1"
V84_AUDIT_VERSION = "pif_app_server_judge_v5_4_v84_projection_audit_v1"
V84_FAILURE_VERSION = "pif_app_server_judge_v5_4_v84_failure_v1"
V84_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v84_terminal_v1"
V84_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V84_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V84_PHASE_ID = "judge_v5_4_v84_metric_target_reference_audit"
MODEL = "gpt-5.5"
EFFORT = "high"
OWNER_TURNS = tuple(f"metric_target_reference_shard_{index:02d}" for index in range(3))
CANARY_TURN = "metric_target_reference_permutation_canary"
TURN_NAMES = OWNER_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V83_ROOT.parent / "judge-calibration-v5_4-v84-metric-target-reference-audit"
).resolve()


class JudgeV5CalibrationV84Error(RuntimeError):
    """The v84 metric/target reference audit cannot preserve its boundary."""


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


def _validate_predecessors(v83_root: Path, v82_root: Path, v78_root: Path) -> dict[str, Any]:
    paths = {
        "v83_terminal": v83_root / "terminal.json",
        "v83_spec": v83_root / "fresh-alignment-spec.json",
        "v83_input": v83_root / "fresh-alignment-input.private.json",
        "v83_truth": v83_root / "fresh-alignment-truth.private.json",
        "v83_output": v83_root / "fresh-alignment-output.private.json",
        "v83_canary": v83_root / "permutation-canary-output.private.json",
        "v83_score": v83_root / "fresh-alignment-score.json",
        "v83_projection": v83_root / "projection-audit.json",
        "v82_terminal": v82_root / "terminal.json",
        "v82_truth": v82_root / "calibration-truth-v3.private.json",
        "v82_receipt": v82_root / "reference-receipt.json",
        "v78_spec": v78_root / "fresh-reconcile-spec.json",
        "v78_input": v78_root / "fresh-input.private.json",
        "v78_truth": v78_root / "fresh-truth.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v83_terminal"]
    spec = values["v83_spec"]
    score = values["v83_score"]
    v82_terminal = values["v82_terminal"]
    v78_spec = values["v78_spec"]
    metrics = score.get("metrics") or {}
    if (
        terminal.get("state") != "inactive"
        or terminal.get("development_terminal_reason")
        != "v83_fresh_alignment_quality_gate_not_passed"
        or terminal.get("diagnostic_passed") is not False
        or terminal.get("bounded_observable_repair_authorized") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or len(terminal.get("attempts") or []) != 6
        or not all(
            _verify_record(record)
            for attempt in terminal.get("attempts") or []
            for record in (attempt.get("capacity"), attempt.get("sidecar"), attempt.get("output"))
        )
        or not _record_matches(terminal.get("output"), paths["v83_output"])
        or not _record_matches(terminal.get("canary_output"), paths["v83_canary"])
        or not _record_matches(terminal.get("score"), paths["v83_score"])
        or not _record_matches(terminal.get("projection_audit"), paths["v83_projection"])
        or metrics.get("exact_count") != 12
        or metrics.get("incorrect_sensitivity") != 0.714286
        or metrics.get("correct_specificity") != 0.875
        or metrics.get("permutation_canary_exact_rate") != 1.0
        or metrics.get("observable_repair_trigger_count") != 0
        or spec.get("model") != "gpt-5.6-terra"
        or not _record_matches(spec.get("frozen_inputs", {}).get("input"), paths["v83_input"])
        or not _record_matches(spec.get("frozen_inputs", {}).get("truth"), paths["v83_truth"])
        or not all(_verify_record(row) for row in spec.get("runtime_files") or [])
        or v82_terminal.get("reference_frozen") is not True
        or not _record_matches(v82_terminal.get("truth"), paths["v82_truth"])
        or not _record_matches(v82_terminal.get("reference_receipt"), paths["v82_receipt"])
        or not _record_matches(v78_spec.get("frozen_inputs", {}).get("input"), paths["v78_input"])
        or not _record_matches(v78_spec.get("frozen_inputs", {}).get("truth"), paths["v78_truth"])
    ):
        raise JudgeV5CalibrationV84Error("v83/v82/v78 predecessor contract drifted")
    return {name: _record(path) for name, path in paths.items()}


def _task_id(source: str, source_task_id: str) -> str:
    return "audit_" + sha256_text(f"v84|{source}|{source_task_id}")[:24]


def _canary_task_id(owner_task_id: str) -> str:
    return "perm_" + sha256_text(f"v84|canary|{owner_task_id}")[:24]


def build_v84_inputs(
    *,
    v83_input: Mapping[str, Any],
    v83_truth: Mapping[str, Any],
    v83_output: Mapping[str, Any],
    v78_input: Mapping[str, Any],
    v78_truth: Mapping[str, Any],
    reference: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    source83 = {row["task_id"]: row for row in v83_input.get("tasks") or []}
    truth83 = {row["task_id"]: row for row in v83_truth.get("tasks") or []}
    output83 = {row["task_id"]: row for row in v83_output.get("decisions") or []}
    if set(source83) != set(truth83) or set(source83) != set(output83):
        raise JudgeV5CalibrationV84Error("v83 source coverage drifted")
    disputed = [
        task_id
        for task_id, row in truth83.items()
        if output83[task_id]["field_status"] != row["expected_status"]
    ]
    if len(disputed) != 3 or Counter(truth83[row]["field"] for row in disputed) != {
        "metric": 2,
        "target": 1,
    }:
        raise JudgeV5CalibrationV84Error("v83 disputed field set drifted")
    selected = [
        {
            "source": "v83",
            "source_task_id": task_id,
            "role": "reference_dispute",
            "field": truth83[task_id]["field"],
            "prior_status": truth83[task_id]["expected_status"],
            "terra_status": output83[task_id]["field_status"],
            "control_expected_status": None,
            "task": source83[task_id],
            "case_id": truth83[task_id]["case_id"],
            "witness_id": truth83[task_id]["witness_id"],
        }
        for task_id in disputed
    ]
    source78 = {row["task_id"]: row for row in v78_input.get("tasks") or []}
    truth78 = {row["task_id"]: row for row in v78_truth.get("tasks") or []}
    control_slots = (
        ("metric", "correct"),
        ("metric", "incorrect"),
        ("target", "correct"),
        ("target", "incorrect"),
        ("actor", "correct"),
        ("actor", "incorrect"),
    )
    for field, status in control_slots:
        candidates = []
        for task_id, row in truth78.items():
            if row["field"] != field:
                continue
            issues = set(
                reference["cases"][row["case_id"]]["field_issues"].get(row["witness_id"], [])
            )
            expected = "incorrect" if field in issues else "correct"
            if expected == status:
                candidates.append(task_id)
        candidates.sort(key=lambda task_id: sha256_text(f"v84|control|{field}|{status}|{task_id}"))
        if not candidates:
            raise JudgeV5CalibrationV84Error("v84 field-matched control is unavailable")
        task_id = candidates[0]
        row = truth78[task_id]
        selected.append(
            {
                "source": "v78",
                "source_task_id": task_id,
                "role": "matched_control",
                "field": field,
                "prior_status": status,
                "terra_status": None,
                "control_expected_status": status,
                "task": source78[task_id],
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
            }
        )
    if len(selected) != 9 or len({(row["source"], row["source_task_id"]) for row in selected}) != 9:
        raise JudgeV5CalibrationV84Error("v84 selection coverage drifted")
    tasks = []
    truth_rows = []
    for row in selected:
        owner_task_id = _task_id(row["source"], row["source_task_id"])
        task = deepcopy(row["task"])
        task["task_id"] = owner_task_id
        tasks.append(task)
        truth_rows.append(
            {
                "task_id": owner_task_id,
                "source": row["source"],
                "source_task_id": row["source_task_id"],
                "role": row["role"],
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "field": row["field"],
                "prior_status": row["prior_status"],
                "terra_status": row["terra_status"],
                "control_expected_status": row["control_expected_status"],
            }
        )
    tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    task_by_id = {row["task_id"]: row for row in tasks}
    disputed_owner_ids = [row["task_id"] for row in truth_rows if row["role"] == "reference_dispute"]
    canary_tasks = []
    canary_map = []
    for owner_task_id in reversed(sorted(disputed_owner_ids)):
        task = deepcopy(task_by_id[owner_task_id])
        canary_task_id = _canary_task_id(owner_task_id)
        task["task_id"] = canary_task_id
        canary_tasks.append(task)
        canary_map.append({"canary_task_id": canary_task_id, "owner_task_id": owner_task_id})
    value = {
        "schema_version": V84_INPUT_VERSION,
        "task_count": 9,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V84_TRUTH_VERSION,
        "task_count": 9,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V84_INPUT_VERSION,
        "task_count": 3,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V84_SELECTION_VERSION,
        "created_at": now_iso(),
        "task_count": 9,
        "role_counts": {"reference_dispute": 3, "matched_control": 6},
        "field_control_counts": {"actor": 2, "metric": 2, "target": 2},
        "control_status_counts": {"correct": 3, "incorrect": 3},
        "permutation_canary_count": 3,
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "privacy": "aggregate_counts_only",
    }
    return value, truth_value, canary, selection


def base_instructions() -> str:
    return (
        "You are the designated neutral reference owner for isolated source-to-field tasks. First align the "
        "event to the exact source proposition expressed by its evidence, claim, and target. Judge only the "
        "requested field for that proposition and never transfer semantics from an adjacent proposition. For "
        "metric, value, unit, comparator, direction, and measurement scope are material; a qualitative direction "
        "such as heavier, lower, widens, or narrows can be material even without a numeral. For target, a harmless "
        "paraphrase or coreference that preserves the same referent and scope is correct; added or changed scope is "
        "incorrect. Mentally correct every other field. The first evidence span must exactly express the aligned "
        "proposition. Abstain only when the source cannot determine the field. Do not vote, use confidence, regex, "
        "keywords, overlap, embeddings, prior labels, or system identity."
    )


def build_prompt(value: Mapping[str, Any]) -> str:
    return (
        "Return one field decision for every opaque task_id. Do not compare tasks or emit whole-event verdicts. "
        "Every source_evidence_span must be an exact substring of that task's source_excerpt.\n\n"
        + _canonical_json(value)
        + "\n"
    )


def owner_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value.get("tasks") or []
    if len(tasks) != 9:
        raise JudgeV5CalibrationV84Error("v84 task count drifted")
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
        raise JudgeV5CalibrationV84Error("v84 output coverage drifted")
    return {"schema_version": V84_OUTPUT_VERSION, "decisions": decisions}


def score_v84(owner: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in owner["decisions"]}
    repeated = {row["task_id"]: row for row in canary["decisions"]}
    if set(expected) != set(observed) or len(repeated) != 3:
        raise JudgeV5CalibrationV84Error("v84 score coverage drifted")
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
        "schema_version": V84_SCORE_VERSION,
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
        "reference_freeze_authorized": passed,
        "fresh_diagnostic_authorized": False,
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
            raise JudgeV5CalibrationV84Error(f"immutable {purpose} drifted")
        return prior
    _write_immutable(path, candidate)
    return candidate


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    phase_bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V84_CAPACITY_AUDIT_VERSION,
        "phase_id": V84_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": phase_bound,
        },
    }
    _write_stable_created(audit_path, audit, "v84 capacity audit")
    policy = {
        "schema_version": V84_CAPACITY_POLICY_VERSION,
        "phase_id": V84_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v84 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v84(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v83_root: Path = DEFAULT_V83_ROOT,
    v82_root: Path = DEFAULT_V82_ROOT,
    v78_root: Path = DEFAULT_V78_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors(v83_root.resolve(), v82_root.resolve(), v78_root.resolve())
    value, truth, canary, selection = build_v84_inputs(
        v83_input=_load_json(v83_root / "fresh-alignment-input.private.json", "v83 input"),
        v83_truth=_load_json(v83_root / "fresh-alignment-truth.private.json", "v83 truth"),
        v83_output=_load_json(v83_root / "fresh-alignment-output.private.json", "v83 output"),
        v78_input=_load_json(v78_root / "fresh-input.private.json", "v78 input"),
        v78_truth=_load_json(v78_root / "fresh-truth.private.json", "v78 truth"),
        reference=_load_json(v82_root / "calibration-truth-v3.private.json", "v82 reference"),
    )
    input_path = root / "metric-target-input.private.json"
    truth_path = root / "metric-target-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_created(selection_path, selection, "v84 selection audit")
    turn_values = owner_shards(value) + [canary]
    turns = []
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
        "schema_version": V84_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "blind_metric_target_reference_owner_with_field_matched_controls_and_canary",
        "task_count": 9,
        "reference_dispute_count": 3,
        "matched_control_count": 6,
        "permutation_canary_count": 3,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "pass_authorizes_deterministic_versioned_reference_freeze_only",
        "reference_freeze_authorized": False,
        "fresh_diagnostic_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v83_fresh_alignment_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v82_reference_freeze.py"),
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
    spec_path = root / "metric-target-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v84 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV84Error("immutable v84 spec drifted")
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v84 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V84_FAILURE_VERSION,
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
        "schema_version": V84_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_freeze_authorized": False,
        "fresh_diagnostic_authorized": False,
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


async def run_v84(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v84 terminal")
    frozen = freeze_v84(output_dir=root, timeout_seconds=timeout_seconds)
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
                    batch_size=len(turn["value"]["tasks"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, value=turn["value"]: validate_output(
                        project_exact_spans(candidate, value)[0], value
                    ),
                )
                projected, turn_operations = project_exact_spans(output, turn["value"])
                if validate_output(projected, turn["value"]):
                    raise JudgeV5CalibrationV84Error("projected v84 output is invalid")
                if current_turn == CANARY_TURN:
                    canary_outputs.append(projected)
                else:
                    owner_outputs.append(projected)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
                operations.extend([{**row, "turn_name": current_turn} for row in turn_operations])
        owner = merge_outputs(owner_outputs, 9)
        canary = merge_outputs(canary_outputs, 3)
        owner_path = root / "metric-target-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(owner_path, owner)
        _write_immutable(canary_path, canary)
        audit = {
            "schema_version": V84_AUDIT_VERSION,
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
        score = score_v84(owner, canary, frozen["truth"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "metric-target-score.json"
        _write_immutable(score_path, score)
        proposal = {
            "schema_version": V84_PROPOSAL_VERSION,
            "created_at": now_iso(),
            "state": "authorized" if score["passed"] else "suppressed",
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
            "schema_version": V84_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v84_metric_target_reference_audit_passed_reference_freeze_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v84_metric_target_reference_audit_passed_reference_freeze_authorized"
                if passed
                else "v84_metric_target_reference_audit_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reference_audit_passed": passed,
            "reference_freeze_authorized": passed,
            "fresh_diagnostic_authorized": False,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "reference_patch_proposal": _record(proposal_path),
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
    parser = argparse.ArgumentParser(description="Run v84 metric/target reference audit")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v84(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_audit_passed": terminal.get("reference_audit_passed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
