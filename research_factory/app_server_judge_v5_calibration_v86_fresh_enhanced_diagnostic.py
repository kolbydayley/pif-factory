from __future__ import annotations

"""Fresh enhanced-rubric diagnostic against reference v4."""

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
from .app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V75_ROOT,
    project_exact_spans,
)
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V78_ROOT,
    V23_ROOT,
    _field_contracts,
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v83_fresh_alignment_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V83_ROOT,
    MAX_REPAIR_TRIGGER_COUNT,
    score_v83,
)
from .app_server_judge_v5_calibration_v84_metric_target_reference_audit import (
    base_instructions,
    build_prompt,
)
from .app_server_judge_v5_calibration_v85_reference_v4_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V85_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso, sha256_text


V86_INPUT_VERSION = "pif_app_server_judge_v5_4_v86_fresh_enhanced_input_v1"
V86_TRUTH_VERSION = "pif_app_server_judge_v5_4_v86_fresh_enhanced_truth_v1"
V86_SELECTION_VERSION = "pif_app_server_judge_v5_4_v86_selection_v1"
V86_SPEC_VERSION = "pif_app_server_judge_v5_4_v86_fresh_enhanced_spec_v1"
V86_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v86_fresh_enhanced_output_v1"
V86_SCORE_VERSION = "pif_app_server_judge_v5_4_v86_fresh_enhanced_score_v1"
V86_AUDIT_VERSION = "pif_app_server_judge_v5_4_v86_projection_audit_v1"
V86_FAILURE_VERSION = "pif_app_server_judge_v5_4_v86_failure_v1"
V86_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v86_terminal_v1"
V86_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V86_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V86_PHASE_ID = "judge_v5_4_v86_fresh_enhanced_diagnostic"
MODEL = "gpt-5.6-terra"
EFFORT = "high"
PRIMARY_TURNS = tuple(f"fresh_enhanced_shard_{index:02d}" for index in range(5))
CANARY_TURN = "fresh_enhanced_permutation_canary"
TURN_NAMES = PRIMARY_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 600.0
V86_FIELD_POLARITY_SLOTS = (
    ("metric", "incorrect"),
    ("evidence", "incorrect"),
    ("temporal_horizon", "incorrect"),
    ("target", "incorrect"),
    ("certainty", "incorrect"),
    ("attribution", "incorrect"),
    ("actor", "incorrect"),
    ("actor", "correct"),
    ("attribution", "correct"),
    ("certainty", "correct"),
    ("event_boundary", "correct"),
    ("metric", "correct"),
    ("speaker", "correct"),
    ("target", "correct"),
    ("unsupported_inference", "correct"),
)
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V85_ROOT.parent / "judge-calibration-v5_4-v86-fresh-enhanced-diagnostic"
).resolve()


class JudgeV5CalibrationV86Error(RuntimeError):
    """The v86 fresh enhanced diagnostic cannot preserve its contract."""


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
    *, v85_root: Path, v23_root: Path, v75_root: Path, v78_root: Path, v83_root: Path
) -> dict[str, Any]:
    paths = {
        "v85_terminal": v85_root / "terminal.json",
        "v85_receipt": v85_root / "reference-receipt.json",
        "v85_truth": v85_root / "calibration-truth-v4.private.json",
        "v85_audit": v85_root / "reference-patch-audit.json",
        "v23_spec": v23_root / "calibration-spec.json",
        "v23_pointwise": v23_root / "pointwise-input-full.private.json",
        "v75_spec": v75_root / "exact-span-remaining-shard-spec.json",
        "v75_truth": v75_root / "selected-truth.private.json",
        "v78_spec": v78_root / "fresh-reconcile-spec.json",
        "v78_truth": v78_root / "fresh-truth.private.json",
        "v83_spec": v83_root / "fresh-alignment-spec.json",
        "v83_truth": v83_root / "fresh-alignment-truth.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v85_terminal"]
    receipt = values["v85_receipt"]
    v23_spec = values["v23_spec"]
    v75_spec = values["v75_spec"]
    v78_spec = values["v78_spec"]
    v83_spec = values["v83_spec"]
    if (
        terminal.get("state") != "completed"
        or terminal.get("reference_frozen") is not True
        or terminal.get("fresh_enhanced_diagnostic_authorized") is not True
        or terminal.get("full_calibration_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_attempt_started") is not False
        or terminal.get("usage", {}).get("total_tokens") != 0
        or not _record_matches(terminal.get("truth"), paths["v85_truth"])
        or not _record_matches(terminal.get("reference_receipt"), paths["v85_receipt"])
        or not _record_matches(terminal.get("patch_audit"), paths["v85_audit"])
        or receipt.get("case_count") != 66
        or receipt.get("witness_count") != 182
        or receipt.get("reference_change_count") != 2
        or receipt.get("fresh_enhanced_diagnostic_authorized") is not True
        or not _record_matches(v23_spec.get("frozen_inputs", {}).get("pointwise_full"), paths["v23_pointwise"])
        or not _record_matches(v75_spec.get("frozen_inputs", {}).get("selected_truth"), paths["v75_truth"])
        or not _record_matches(v78_spec.get("frozen_inputs", {}).get("truth"), paths["v78_truth"])
        or not _record_matches(v83_spec.get("frozen_inputs", {}).get("truth"), paths["v83_truth"])
        or not all(_verify_record(row) for row in v75_spec.get("runtime_files") or [])
        or not all(_verify_record(row) for row in v78_spec.get("runtime_files") or [])
        or not all(_verify_record(row) for row in v83_spec.get("runtime_files") or [])
    ):
        raise JudgeV5CalibrationV86Error("v85/v23/v75/v78/v83 predecessor contract drifted")
    return {name: _record(path) for name, path in paths.items()}


def _task_id(case_id: str, witness_id: str, field: str) -> str:
    return "enh_" + sha256_text(f"v86|{case_id}|{witness_id}|{field}")[:24]


def _canary_task_id(primary_task_id: str) -> str:
    return "perm_" + sha256_text(f"v86|canary|{primary_task_id}")[:24]


def build_v86_inputs(
    *,
    pointwise: Mapping[str, Any],
    reference: Mapping[str, Any],
    prior_truths: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    contracts = _field_contracts()
    excluded = {
        (row["case_id"], row["witness_id"])
        for truth in prior_truths
        for row in truth.get("tasks") or []
    }
    units = [
        row
        for row in pointwise.get("units") or []
        if (row["case_id"], row["witness_id"]) not in excluded
    ]
    selected = []
    used = set()
    for field, status in V86_FIELD_POLARITY_SLOTS:
        candidates = []
        for unit in units:
            identity = (unit["case_id"], unit["witness_id"])
            issues = set(reference["cases"][identity[0]]["field_issues"].get(identity[1], []))
            expected = "incorrect" if field in issues else "correct"
            if expected == status and identity not in used:
                candidates.append(unit)
        candidates.sort(
            key=lambda row: sha256_text(
                f"v86-select|{field}|{status}|{row['case_id']}|{row['witness_id']}"
            )
        )
        if not candidates:
            raise JudgeV5CalibrationV86Error("v86 field-polarity slot has no fresh witness")
        chosen = candidates[0]
        used.add((chosen["case_id"], chosen["witness_id"]))
        selected.append((field, status, chosen))
    if len(selected) != 15 or len(used) != 15:
        raise JudgeV5CalibrationV86Error("v86 selection coverage drifted")
    tasks = []
    truth_rows = []
    for field, status, unit in selected:
        task_id = _task_id(unit["case_id"], unit["witness_id"], field)
        tasks.append(
            {
                "task_id": task_id,
                "field": field,
                "field_contract": deepcopy(contracts[field]),
                "source_excerpt": unit["source_excerpt"],
                "structured_event": deepcopy(unit["structured_event"]),
            }
        )
        truth_rows.append(
            {
                "task_id": task_id,
                "case_id": unit["case_id"],
                "witness_id": unit["witness_id"],
                "field": field,
                "expected_status": status,
            }
        )
    tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    task_by_id = {row["task_id"]: row for row in tasks}
    canary_owner_ids = sorted(
        task_by_id, key=lambda task_id: sha256_text(f"v86|perm|{task_id}")
    )[:4]
    canary_tasks = []
    canary_map = []
    for primary_task_id in reversed(canary_owner_ids):
        task = deepcopy(task_by_id[primary_task_id])
        canary_task_id = _canary_task_id(primary_task_id)
        task["task_id"] = canary_task_id
        canary_tasks.append(task)
        canary_map.append(
            {"canary_task_id": canary_task_id, "primary_task_id": primary_task_id}
        )
    value = {
        "schema_version": V86_INPUT_VERSION,
        "task_count": 15,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V86_TRUTH_VERSION,
        "reference_version": reference["reference_version"],
        "task_count": 15,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V86_INPUT_VERSION,
        "task_count": 4,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V86_SELECTION_VERSION,
        "created_at": now_iso(),
        "source_witness_count": len(pointwise.get("units") or []),
        "excluded_prior_witness_count": len(excluded),
        "selected_task_count": 15,
        "selected_distinct_witness_count": len(used),
        "expected_correct_count": sum(status == "correct" for _, status, _ in selected),
        "expected_incorrect_count": sum(status == "incorrect" for _, status, _ in selected),
        "field_counts": dict(sorted(Counter(field for field, _, _ in selected).items())),
        "permutation_canary_count": 4,
        "selection_uses_source_text": False,
        "selection_rule": "fixed_slots_hash_ranked_distinct_witnesses_excluding_v75_v78_v83",
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return value, truth_value, canary, selection


def primary_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value.get("tasks") or []
    if len(tasks) != 15:
        raise JudgeV5CalibrationV86Error("v86 task count drifted")
    return [
        {
            **{key: deepcopy(child) for key, child in value.items() if key != "tasks"},
            "task_count": 3,
            "tasks": deepcopy(tasks[index : index + 3]),
            "shard_ordinal": index // 3,
            "shard_count": 5,
        }
        for index in range(0, 15, 3)
    ]


def merge_outputs(outputs: Sequence[Mapping[str, Any]], expected_count: int) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(decisions) != expected_count or len({row["task_id"] for row in decisions}) != expected_count:
        raise JudgeV5CalibrationV86Error("v86 output coverage drifted")
    return {"schema_version": V86_OUTPUT_VERSION, "decisions": decisions}


def score_v86(
    primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    score = score_v83(primary, canary, truth)
    score["schema_version"] = V86_SCORE_VERSION
    return score


def _write_stable_created(path: Path, value: Mapping[str, Any], purpose: str) -> dict[str, Any]:
    candidate = deepcopy(value)
    if path.exists():
        prior = _load_json(path, purpose)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV86Error(f"immutable {purpose} drifted")
        return prior
    _write_immutable(path, candidate)
    return candidate


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    phase_bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V86_CAPACITY_AUDIT_VERSION,
        "phase_id": V86_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": phase_bound,
        },
    }
    _write_stable_created(audit_path, audit, "v86 capacity audit")
    policy = {
        "schema_version": V86_CAPACITY_POLICY_VERSION,
        "phase_id": V86_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v86 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v86(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v85_root: Path = DEFAULT_V85_ROOT,
    v23_root: Path = V23_ROOT,
    v75_root: Path = DEFAULT_V75_ROOT,
    v78_root: Path = DEFAULT_V78_ROOT,
    v83_root: Path = DEFAULT_V83_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors(
        v85_root=v85_root.resolve(),
        v23_root=v23_root.resolve(),
        v75_root=v75_root.resolve(),
        v78_root=v78_root.resolve(),
        v83_root=v83_root.resolve(),
    )
    value, truth, canary, selection = build_v86_inputs(
        pointwise=_load_json(v23_root / "pointwise-input-full.private.json", "v23 pointwise"),
        reference=_load_json(v85_root / "calibration-truth-v4.private.json", "v85 reference"),
        prior_truths=[
            _load_json(v75_root / "selected-truth.private.json", "v75 truth"),
            _load_json(v78_root / "fresh-truth.private.json", "v78 truth"),
            _load_json(v83_root / "fresh-alignment-truth.private.json", "v83 truth"),
        ],
    )
    input_path = root / "fresh-enhanced-input.private.json"
    truth_path = root / "fresh-enhanced-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_created(selection_path, selection, "v86 selection audit")
    turn_values = primary_shards(value) + [canary]
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
        "schema_version": V86_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_unseen_enhanced_metric_target_alignment_with_permutation_canary",
        "task_count": 15,
        "distinct_witness_count": 15,
        "permutation_canary_count": 4,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "retry_count_per_turn": 0,
        "observable_repair_triggers": [
            "primary_abstention",
            "primary_missing_exact_evidence",
            "canary_abstention",
            "canary_missing_exact_evidence",
            "permutation_status_disagreement",
        ],
        "maximum_observable_repair_trigger_count": MAX_REPAIR_TRIGGER_COUNT,
        "promotion_rule": "pass_authorizes_one_fresh_full_development_calibration_only",
        "fresh_full_development_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v85_reference_v4_freeze.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v84_metric_target_reference_audit.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v83_fresh_alignment_diagnostic.py"),
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
    spec_path = root / "fresh-enhanced-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v86 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV86Error("immutable v86 spec drifted")
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v86 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V86_FAILURE_VERSION,
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
        "schema_version": V86_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "diagnostic_passed": False,
        "bounded_observable_repair_authorized": False,
        "fresh_full_development_calibration_authorized": False,
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


async def run_v86(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v86 terminal")
    frozen = freeze_v86(output_dir=root, timeout_seconds=timeout_seconds)
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
                    raise JudgeV5CalibrationV86Error("projected v86 output is invalid")
                if current_turn == CANARY_TURN:
                    canary_outputs.append(projected)
                else:
                    primary_outputs.append(projected)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
                operations.extend([{**row, "turn_name": current_turn} for row in turn_operations])
        primary = merge_outputs(primary_outputs, 15)
        canary = merge_outputs(canary_outputs, 4)
        primary_path = root / "fresh-enhanced-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(primary_path, primary)
        _write_immutable(canary_path, canary)
        audit = {
            "schema_version": V86_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "projection_scope": "nonexact_source_span_removal_only",
            "semantic_status_changed": False,
            "privacy": "opaque_task_ids_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v86(primary, canary, frozen["truth"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "fresh-enhanced-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        repair = bool(score["bounded_observable_repair_authorized"])
        terminal = {
            "schema_version": V86_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v86_fresh_enhanced_passed_full_development_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v86_fresh_enhanced_passed_full_development_calibration_authorized"
                if passed
                else (
                    "v86_observable_repair_required"
                    if repair
                    else "v86_fresh_enhanced_quality_gate_not_passed"
                )
            ),
            "overall_evaluation_complete": False,
            "diagnostic_passed": passed,
            "bounded_observable_repair_authorized": repair,
            "fresh_full_development_calibration_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(primary_path),
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
    parser = argparse.ArgumentParser(description="Run v86 fresh enhanced diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v86(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "diagnostic_passed": terminal.get("diagnostic_passed", False),
                "bounded_observable_repair_authorized": terminal.get(
                    "bounded_observable_repair_authorized", False
                ),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
