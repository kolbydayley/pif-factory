from __future__ import annotations

"""Fresh Luna diagnostic against the corrected reference v6."""

import argparse
import asyncio
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

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
from .app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V75_ROOT,
    project_exact_spans,
)
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    V23_ROOT,
    _field_contracts,
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v83_fresh_alignment_diagnostic import (
    MAX_REPAIR_TRIGGER_COUNT,
    score_v83,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record_matches,
    _verify_record,
    _write_stable_created,
)
from .app_server_judge_v5_calibration_v88_residual_reference_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V88_ROOT,
)
from .app_server_judge_v5_calibration_v91_fresh_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V91_ROOT,
)
from .app_server_judge_v5_calibration_v94_systematic_field_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V94_ROOT,
    base_instructions_v94,
)
from .app_server_judge_v5_calibration_v95_reference_v6_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V95_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _validate_usage,
)
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .util import now_iso, sha256_text


V96_INPUT_VERSION = "pif_app_server_judge_v5_4_v96_fresh_luna_input_v1"
V96_TRUTH_VERSION = "pif_app_server_judge_v5_4_v96_fresh_luna_truth_v1"
V96_SELECTION_VERSION = "pif_app_server_judge_v5_4_v96_selection_v1"
V96_SPEC_VERSION = "pif_app_server_judge_v5_4_v96_fresh_luna_spec_v1"
V96_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v96_fresh_luna_output_v1"
V96_SCORE_VERSION = "pif_app_server_judge_v5_4_v96_fresh_luna_score_v1"
V96_AUDIT_VERSION = "pif_app_server_judge_v5_4_v96_projection_audit_v1"
V96_FAILURE_VERSION = "pif_app_server_judge_v5_4_v96_failure_v1"
V96_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v96_terminal_v1"
V96_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V96_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V96_PHASE_ID = "judge_v5_4_v96_fresh_luna_diagnostic"

MODEL = "gpt-5.6-luna"
EFFORT = "high"
FIELD_POLARITY_SLOTS = (
    ("target", "incorrect"),
    ("certainty", "incorrect"),
    ("actor", "incorrect"),
    ("attribution", "incorrect"),
    ("event_type", "incorrect"),
    ("evidence", "incorrect"),
    ("metric", "incorrect"),
    ("target", "correct"),
    ("certainty", "correct"),
    ("speaker", "correct"),
    ("temporal_horizon", "correct"),
    ("stance", "correct"),
    ("causal_mechanism", "correct"),
    ("reported_actor", "correct"),
    ("unsupported_inference", "correct"),
)
PRIMARY_TURNS = tuple(f"fresh_luna_v6_shard_{index:02d}" for index in range(5))
CANARY_TURN = "fresh_luna_v6_permutation_canary"
TURN_NAMES = PRIMARY_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V95_ROOT.parent / "judge-calibration-v5_4-v96-fresh-luna-diagnostic"
).resolve()


class JudgeV5CalibrationV96Error(RuntimeError):
    """The v96 fresh Luna diagnostic cannot preserve its frozen contract."""


def _validate_predecessors(
    *,
    v95_root: Path,
    v94_root: Path,
    v23_root: Path,
    v75_root: Path,
    v88_root: Path,
    v91_root: Path,
) -> dict[str, Any]:
    paths = {
        "v95_terminal": v95_root / "terminal.json",
        "v95_receipt": v95_root / "reference-receipt.json",
        "v95_truth": v95_root / "calibration-truth-v6.private.json",
        "v95_audit": v95_root / "reference-patch-audit.json",
        "v94_terminal": v94_root / "terminal.json",
        "v94_spec": v94_root / "systematic-field-spec.json",
        "v94_rubric": v94_root / "field-rubric.json",
        "v23_pointwise": v23_root / "pointwise-input-full.private.json",
        "v75_terminal": v75_root / "terminal.json",
        "v75_spec": v75_root / "exact-span-remaining-shard-spec.json",
        "v75_truth": v75_root / "selected-truth.private.json",
        "v88_terminal": v88_root / "terminal.json",
        "v88_spec": v88_root / "residual-reference-spec.json",
        "v88_truth": v88_root / "residual-reference-truth.private.json",
        "v91_terminal": v91_root / "terminal.json",
        "v91_spec": v91_root / "fresh-luna-spec.json",
        "v91_truth": v91_root / "fresh-luna-truth.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t95 = values["v95_terminal"]
    r95 = values["v95_receipt"]
    t94 = values["v94_terminal"]
    prior_specs = (values["v75_spec"], values["v88_spec"], values["v91_spec"])
    if (
        t95.get("state") != "completed"
        or t95.get("reference_frozen") is not True
        or t95.get("fresh_diagnostic_authorized") is not True
        or t95.get("full_calibration_authorized") is not False
        or t95.get("selection_authorized") is not False
        or t95.get("holdout_authorized") is not False
        or t95.get("production_mutated") is not False
        or t95.get("usage", {}).get("total_tokens") != 0
        or not _record_matches(t95.get("truth"), paths["v95_truth"])
        or not _record_matches(t95.get("reference_receipt"), paths["v95_receipt"])
        or not _record_matches(t95.get("patch_audit"), paths["v95_audit"])
        or r95.get("case_count") != 66
        or r95.get("witness_count") != 182
        or r95.get("reference_change_count") != 14
        or r95.get("normalization_operation_count") != 2
        or t94.get("state") != "completed"
        or t94.get("reference_patch_authorized") is not True
        or t94.get("usage_status") != "complete"
        or t94.get("production_mutated") is not False
        or not _record_matches(
            values["v94_spec"].get("frozen_inputs", {}).get("rubric"),
            paths["v94_rubric"],
        )
        or not all(_verify_record(row) for row in values["v94_spec"].get("runtime_files") or [])
        or len(values["v23_pointwise"].get("units") or []) != 182
        or any(spec.get("model") != MODEL for spec in prior_specs)
        or not all(all(_verify_record(row) for row in spec.get("runtime_files") or []) for spec in prior_specs)
        or values["v75_terminal"].get("usage_status") != "complete"
        or values["v88_terminal"].get("usage_status") != "complete"
        or values["v91_terminal"].get("usage_status") != "complete"
        or any(
            terminal.get("production_mutated") is not False
            for terminal in (
                values["v75_terminal"],
                values["v88_terminal"],
                values["v91_terminal"],
            )
        )
    ):
        raise JudgeV5CalibrationV96Error("v95/reference/Luna predecessor contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def _task_id(case_id: str, witness_id: str, field: str) -> str:
    return "fresh_" + sha256_text(f"v96|{case_id}|{witness_id}|{field}")[:24]


def _canary_task_id(primary_task_id: str) -> str:
    return "perm_" + sha256_text(f"v96|canary|{primary_task_id}")[:24]


def build_v96_inputs(
    *,
    pointwise: Mapping[str, Any],
    reference: Mapping[str, Any],
    rubric: Mapping[str, Any],
    prior_luna_truths: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    contracts = _field_contracts()
    contracts.update(deepcopy(rubric["field_contracts"]))
    excluded = {
        (row["case_id"], row["witness_id"])
        for truth in prior_luna_truths
        for row in truth.get("tasks") or []
        if row.get("case_id") and row.get("witness_id")
    }
    units = [
        row
        for row in pointwise.get("units") or []
        if (row["case_id"], row["witness_id"]) not in excluded
    ]
    selected = []
    used = set()
    for field, status in FIELD_POLARITY_SLOTS:
        candidates = []
        for unit in units:
            identity = (unit["case_id"], unit["witness_id"])
            issues = set(reference["cases"][identity[0]]["field_issues"].get(identity[1], []))
            expected = "incorrect" if field in issues else "correct"
            if expected == status and identity not in used:
                candidates.append(unit)
        candidates.sort(
            key=lambda row: sha256_text(
                f"v96-select|{field}|{status}|{row['case_id']}|{row['witness_id']}"
            )
        )
        if not candidates:
            raise JudgeV5CalibrationV96Error("v96 field-polarity slot has no fresh Luna witness")
        chosen = candidates[0]
        used.add((chosen["case_id"], chosen["witness_id"]))
        selected.append((field, status, chosen))
    if len(selected) != 15 or len(used) != 15:
        raise JudgeV5CalibrationV96Error("v96 selection coverage drifted")
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
                "structured_event": compact_empty_event_fields(deepcopy(unit["structured_event"])),
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
        task_by_id, key=lambda task_id: sha256_text(f"v96|perm|{task_id}")
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
        "schema_version": V96_INPUT_VERSION,
        "task_count": 15,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth = {
        "schema_version": V96_TRUTH_VERSION,
        "reference_version": reference["reference_version"],
        "task_count": 15,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V96_INPUT_VERSION,
        "task_count": 4,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V96_SELECTION_VERSION,
        "created_at": now_iso(),
        "source_witness_count": len(pointwise.get("units") or []),
        "excluded_prior_luna_witness_count": len(excluded),
        "eligible_luna_fresh_witness_count": len(units),
        "selected_task_count": 15,
        "selected_distinct_witness_count": len(used),
        "expected_correct_count": 8,
        "expected_incorrect_count": 7,
        "field_counts": dict(sorted(Counter(field for field, _, _ in selected).items())),
        "permutation_canary_count": 4,
        "selection_uses_source_text": False,
        "selection_rule": "fixed_slots_hash_ranked_excluding_all_v75_v88_v91_luna_witnesses",
        "empty_event_fields_omitted_only": True,
        "semantic_pruning_performed": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return value, truth, canary, selection


def primary_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value.get("tasks") or []
    if len(tasks) != 15:
        raise JudgeV5CalibrationV96Error("v96 task count drifted")
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
        raise JudgeV5CalibrationV96Error("v96 output coverage drifted")
    return {"schema_version": V96_OUTPUT_VERSION, "decisions": decisions}


def build_prompt_v96(value: Mapping[str, Any]) -> str:
    return (
        "Return one field decision for every opaque task_id. Judge only the requested existing field "
        "under its supplied contract; do not emit a whole-event verdict or compare tasks. Every "
        "source_evidence_span must be an exact substring of that task's source_excerpt.\n\n"
        + json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    )


def score_v96(
    primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    score = score_v83(primary, canary, truth)
    score["schema_version"] = V96_SCORE_VERSION
    return score


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V96_CAPACITY_AUDIT_VERSION,
        "phase_id": V96_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_created(audit_path, audit, "v96 capacity audit")
    policy = {
        "schema_version": V96_CAPACITY_POLICY_VERSION,
        "phase_id": V96_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v96 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v96(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v95_root: Path = DEFAULT_V95_ROOT,
    v94_root: Path = DEFAULT_V94_ROOT,
    v23_root: Path = V23_ROOT,
    v75_root: Path = DEFAULT_V75_ROOT,
    v88_root: Path = DEFAULT_V88_ROOT,
    v91_root: Path = DEFAULT_V91_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors(
        v95_root=v95_root.resolve(),
        v94_root=v94_root.resolve(),
        v23_root=v23_root.resolve(),
        v75_root=v75_root.resolve(),
        v88_root=v88_root.resolve(),
        v91_root=v91_root.resolve(),
    )
    values = predecessor["values"]
    value, truth, canary, selection = build_v96_inputs(
        pointwise=values["v23_pointwise"],
        reference=values["v95_truth"],
        rubric=values["v94_rubric"],
        prior_luna_truths=[values["v75_truth"], values["v88_truth"], values["v91_truth"]],
    )
    input_path = root / "fresh-luna-v6-input.private.json"
    truth_path = root / "fresh-luna-v6-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_created(selection_path, selection, "v96 selection audit")
    turn_values = primary_shards(value) + [canary]
    turns = []
    for turn_name, turn_value in zip(TURN_NAMES, turn_values, strict=True):
        prompt = build_prompt_v96(turn_value)
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
    runtime_files = [
        Path(__file__),
        runtime_dir / "app_server_judge_v5_calibration_v95_reference_v6_freeze.py",
        runtime_dir / "app_server_judge_v5_calibration_v94_systematic_field_audit.py",
        runtime_dir / "app_server_judge_v5_calibration_v91_fresh_luna_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v88_residual_reference_audit.py",
        runtime_dir / "app_server_judge_v5_calibration_v83_fresh_alignment_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v75_exact_span_remaining_shard.py",
        runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py",
        runtime_dir / "app_server_capacity_reserve.py",
        runtime_dir / "codex_app_server.py",
    ]
    spec = {
        "schema_version": V96_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_luna_field_diagnostic_against_reference_v6_with_corrected_rubric",
        "task_count": 15,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "15_of_15_all_sensitivity_specificity_evidence_canaries_zero_abstentions",
        "bounded_observable_repair_limit": MAX_REPAIR_TRIGGER_COUNT,
        "fresh_full_development_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [_record(path) for path in runtime_files],
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
    spec_path = root / "fresh-luna-v6-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v96 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV96Error("immutable v96 spec drifted")
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v96 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V96_FAILURE_VERSION,
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
        "schema_version": V96_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
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


async def run_v96(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v96 terminal")
    frozen = freeze_v96(output_dir=root, timeout_seconds=timeout_seconds)
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
                    base_instructions=base_instructions_v94(),
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
                    raise JudgeV5CalibrationV96Error("projected v96 output is invalid")
                if current_turn == CANARY_TURN:
                    canary_outputs.append(projected)
                else:
                    primary_outputs.append(projected)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
                operations.extend(
                    [{**row, "turn_name": current_turn} for row in turn_operations]
                )
        primary = merge_outputs(primary_outputs, 15)
        canary = merge_outputs(canary_outputs, 4)
        primary_path = root / "fresh-luna-v6-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(primary_path, primary)
        _write_immutable(canary_path, canary)
        audit = {
            "schema_version": V96_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "projection_scope": "nonexact_source_span_removal_only",
            "semantic_status_changed": False,
            "privacy": "opaque_task_ids_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v96(primary, canary, frozen["truth"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "fresh-luna-v6-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V96_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v96_fresh_luna_diagnostic_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v96_fresh_luna_diagnostic_passed_full_calibration_authorized"
                if passed
                else "v96_fresh_luna_diagnostic_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "fresh_full_development_calibration_authorized": passed,
            "bounded_observable_repair_authorized": score[
                "bounded_observable_repair_authorized"
            ],
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
    parser = argparse.ArgumentParser(description="Run v96 fresh Luna diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v96(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "fresh_full_development_calibration_authorized": terminal.get(
                    "fresh_full_development_calibration_authorized", False
                ),
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
