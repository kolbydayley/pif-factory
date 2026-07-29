from __future__ import annotations

"""Refined Luna diagnostic with v96 regression cases and reference v7."""

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
)
from .app_server_judge_v5_calibration_v96_fresh_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V96_ROOT,
)
from .app_server_judge_v5_calibration_v97_residual_field_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V97_ROOT,
    _requested_field_value,
    base_instructions_v97,
)
from .app_server_judge_v5_calibration_v98_reference_v7_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V98_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _validate_usage,
)
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .util import now_iso, sha256_text


V99_INPUT_VERSION = "pif_app_server_judge_v5_4_v99_refined_luna_input_v1"
V99_TRUTH_VERSION = "pif_app_server_judge_v5_4_v99_refined_luna_truth_v1"
V99_SELECTION_VERSION = "pif_app_server_judge_v5_4_v99_selection_v1"
V99_SPEC_VERSION = "pif_app_server_judge_v5_4_v99_refined_luna_spec_v1"
V99_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v99_refined_luna_output_v1"
V99_SCORE_VERSION = "pif_app_server_judge_v5_4_v99_refined_luna_score_v1"
V99_AUDIT_VERSION = "pif_app_server_judge_v5_4_v99_projection_audit_v1"
V99_FAILURE_VERSION = "pif_app_server_judge_v5_4_v99_failure_v1"
V99_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v99_terminal_v1"
V99_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V99_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V99_PHASE_ID = "judge_v5_4_v99_refined_luna_diagnostic"

MODEL = "gpt-5.6-luna"
EFFORT = "high"
TARGETED_FIELDS = ("evidence", "metric", "reported_actor", "speaker")
FRESH_FIELD_POLARITY_SLOTS = (
    ("target", "incorrect"),
    ("actor", "incorrect"),
    ("attribution", "incorrect"),
    ("event_type", "incorrect"),
    ("unsupported_inference", "incorrect"),
    ("certainty", "correct"),
    ("target", "correct"),
    ("speaker", "correct"),
    ("temporal_horizon", "correct"),
    ("stance", "correct"),
    ("causal_mechanism", "correct"),
)
PRIMARY_TURNS = tuple(f"refined_luna_shard_{index:02d}" for index in range(5))
CANARY_TURN = "refined_luna_permutation_canary"
TURN_NAMES = PRIMARY_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V98_ROOT.parent / "judge-calibration-v5_4-v99-refined-luna-diagnostic"
).resolve()


class JudgeV5CalibrationV99Error(RuntimeError):
    """The v99 refined Luna diagnostic cannot preserve its frozen contract."""


def _validate_predecessors(
    *,
    v98_root: Path,
    v97_root: Path,
    v96_root: Path,
    v94_root: Path,
    v23_root: Path,
    v75_root: Path,
    v88_root: Path,
    v91_root: Path,
) -> dict[str, Any]:
    paths = {
        "v98_terminal": v98_root / "terminal.json",
        "v98_receipt": v98_root / "reference-receipt.json",
        "v98_truth": v98_root / "calibration-truth-v7.private.json",
        "v97_terminal": v97_root / "terminal.json",
        "v97_spec": v97_root / "residual-field-spec.json",
        "v97_rubric": v97_root / "field-rubric.json",
        "v96_terminal": v96_root / "terminal.json",
        "v96_spec": v96_root / "fresh-luna-v6-spec.json",
        "v96_input": v96_root / "fresh-luna-v6-input.private.json",
        "v96_truth": v96_root / "fresh-luna-v6-truth.private.json",
        "v96_output": v96_root / "fresh-luna-v6-output.private.json",
        "v94_rubric": v94_root / "field-rubric.json",
        "v23_pointwise": v23_root / "pointwise-input-full.private.json",
        "v75_spec": v75_root / "exact-span-remaining-shard-spec.json",
        "v75_truth": v75_root / "selected-truth.private.json",
        "v88_spec": v88_root / "residual-reference-spec.json",
        "v88_truth": v88_root / "residual-reference-truth.private.json",
        "v91_spec": v91_root / "fresh-luna-spec.json",
        "v91_truth": v91_root / "fresh-luna-truth.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t98 = values["v98_terminal"]
    r98 = values["v98_receipt"]
    t97 = values["v97_terminal"]
    t96 = values["v96_terminal"]
    luna_specs = (values["v75_spec"], values["v88_spec"], values["v91_spec"], values["v96_spec"])
    if (
        t98.get("state") != "completed"
        or t98.get("reference_frozen") is not True
        or t98.get("fresh_diagnostic_authorized") is not True
        or t98.get("full_calibration_authorized") is not False
        or t98.get("selection_authorized") is not False
        or t98.get("holdout_authorized") is not False
        or t98.get("production_mutated") is not False
        or t98.get("usage", {}).get("total_tokens") != 0
        or not _record_matches(t98.get("truth"), paths["v98_truth"])
        or not _record_matches(t98.get("reference_receipt"), paths["v98_receipt"])
        or r98.get("reference_change_count") != 2
        or r98.get("field_change_counts") != {"metric": 1, "speaker": 1}
        or t97.get("state") != "completed"
        or t97.get("reference_patch_authorized") is not True
        or t97.get("usage_status") != "complete"
        or t97.get("production_mutated") is not False
        or not _record_matches(
            values["v97_spec"].get("frozen_inputs", {}).get("rubric"), paths["v97_rubric"]
        )
        or not all(_verify_record(row) for row in values["v97_spec"].get("runtime_files") or [])
        or t96.get("state") != "inactive"
        or t96.get("development_terminal_reason")
        != "v96_fresh_luna_diagnostic_quality_gate_not_passed"
        or t96.get("usage_status") != "complete"
        or t96.get("production_mutated") is not False
        or not _record_matches(values["v96_spec"].get("frozen_inputs", {}).get("input"), paths["v96_input"])
        or not _record_matches(values["v96_spec"].get("frozen_inputs", {}).get("truth"), paths["v96_truth"])
        or len(values["v23_pointwise"].get("units") or []) != 182
        or any(spec.get("model") != MODEL for spec in luna_specs)
        or not all(all(_verify_record(row) for row in spec.get("runtime_files") or []) for spec in luna_specs)
    ):
        raise JudgeV5CalibrationV99Error("v98/refined-Luna predecessor contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def _general_requested_field_value(field: str, event: Mapping[str, Any]) -> dict[str, Any]:
    if field in TARGETED_FIELDS:
        return _requested_field_value(field, event)
    keys = {
        "actor": ("actor_name", "actor_type", "actor_role", "actor_affiliation"),
        "attribution": (
            "source_context_kind",
            "speaker_name",
            "reported_actor_name",
        ),
        "causal_mechanism": ("causal_mechanism",),
        "certainty": ("certainty",),
        "event_boundary": ("claim_text", "evidence"),
        "event_type": ("event_type", "event_subtype"),
        "negation": ("claim_text", "counterclaim"),
        "stance": ("stance",),
        "target": ("target_name", "target_concept"),
        "temporal_horizon": ("temporal_horizon",),
        "unsupported_inference": ("claim_text", "evidence"),
    }[field]
    values = {key: event.get(key) for key in keys if event.get(key) not in (None, "", [], {})}
    return {"presence": "present" if values else "absent", "values": values}


def _task_id(role: str, case_id: str, witness_id: str, field: str) -> str:
    return "fresh_" + sha256_text(f"v99|{role}|{case_id}|{witness_id}|{field}")[:24]


def _canary_task_id(primary_task_id: str) -> str:
    return "perm_" + sha256_text(f"v99|canary|{primary_task_id}")[:24]


def build_v99_inputs(
    *,
    pointwise: Mapping[str, Any],
    reference: Mapping[str, Any],
    v94_rubric: Mapping[str, Any],
    v97_rubric: Mapping[str, Any],
    v96_input: Mapping[str, Any],
    v96_truth: Mapping[str, Any],
    v96_output: Mapping[str, Any],
    prior_luna_truths: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    contracts = _field_contracts()
    contracts.update(deepcopy(v94_rubric["field_contracts"]))
    contracts.update(deepcopy(v97_rubric["field_contracts"]))
    pointwise_by_identity = {
        (row["case_id"], row["witness_id"]): row for row in pointwise.get("units") or []
    }
    tasks96 = {row["task_id"]: row for row in v96_input["tasks"]}
    observed96 = {row["task_id"]: row for row in v96_output["decisions"]}
    targeted = []
    targeted_identities = set()
    for row in v96_truth["tasks"]:
        if observed96[row["task_id"]]["field_status"] == row["expected_status"]:
            continue
        if row["field"] not in TARGETED_FIELDS:
            raise JudgeV5CalibrationV99Error("v99 targeted regression field drifted")
        identity = (row["case_id"], row["witness_id"])
        source_task = tasks96[row["task_id"]]
        issues = set(reference["cases"][identity[0]]["field_issues"][identity[1]])
        expected = "incorrect" if row["field"] in issues else "correct"
        task_id = _task_id("regression", identity[0], identity[1], row["field"])
        event = compact_empty_event_fields(deepcopy(source_task["structured_event"]))
        targeted.append(
            (
                {
                    "task_id": task_id,
                    "field": row["field"],
                    "field_contract": deepcopy(contracts[row["field"]]),
                    "requested_field_value": _general_requested_field_value(row["field"], event),
                    "source_excerpt": source_task["source_excerpt"],
                    "structured_event": event,
                },
                {
                    "task_id": task_id,
                    "case_id": identity[0],
                    "witness_id": identity[1],
                    "field": row["field"],
                    "expected_status": expected,
                    "cohort_role": "v96_targeted_regression",
                },
            )
        )
        targeted_identities.add(identity)
    if len(targeted) != 4 or Counter(task["field"] for task, _ in targeted) != Counter(TARGETED_FIELDS):
        raise JudgeV5CalibrationV99Error("v99 targeted regression coverage drifted")

    excluded = {
        (row["case_id"], row["witness_id"])
        for truth in prior_luna_truths
        for row in truth.get("tasks") or []
        if row.get("case_id") and row.get("witness_id")
    }
    used = set(targeted_identities)
    fresh = []
    units = [row for row in pointwise.get("units") or [] if (row["case_id"], row["witness_id"]) not in excluded]
    for field, status in FRESH_FIELD_POLARITY_SLOTS:
        candidates = []
        for unit in units:
            identity = (unit["case_id"], unit["witness_id"])
            if identity in used:
                continue
            issues = set(reference["cases"][identity[0]]["field_issues"][identity[1]])
            expected = "incorrect" if field in issues else "correct"
            if expected == status:
                candidates.append(unit)
        candidates.sort(
            key=lambda row: sha256_text(
                f"v99-select|{field}|{status}|{row['case_id']}|{row['witness_id']}"
            )
        )
        if not candidates:
            raise JudgeV5CalibrationV99Error("v99 fresh field-polarity slot is empty")
        unit = candidates[0]
        identity = (unit["case_id"], unit["witness_id"])
        used.add(identity)
        task_id = _task_id("fresh", identity[0], identity[1], field)
        event = compact_empty_event_fields(deepcopy(unit["structured_event"]))
        fresh.append(
            (
                {
                    "task_id": task_id,
                    "field": field,
                    "field_contract": deepcopy(contracts[field]),
                    "requested_field_value": _general_requested_field_value(field, event),
                    "source_excerpt": unit["source_excerpt"],
                    "structured_event": event,
                },
                {
                    "task_id": task_id,
                    "case_id": identity[0],
                    "witness_id": identity[1],
                    "field": field,
                    "expected_status": status,
                    "cohort_role": "fresh_to_luna",
                },
            )
        )
    rows = targeted + fresh
    if len(rows) != 15 or len(used) != 15:
        raise JudgeV5CalibrationV99Error("v99 diagnostic coverage drifted")
    tasks = sorted((task for task, _ in rows), key=lambda row: row["task_id"])
    truth_rows = sorted((truth for _, truth in rows), key=lambda row: row["task_id"])
    if Counter(row["expected_status"] for row in truth_rows) != Counter({"incorrect": 7, "correct": 8}):
        raise JudgeV5CalibrationV99Error("v99 polarity balance drifted")
    task_by_id = {row["task_id"]: row for row in tasks}
    targeted_by_field = {
        row["field"]: row["task_id"]
        for row in truth_rows
        if row["cohort_role"] == "v96_targeted_regression"
    }
    canary_owner_ids = [targeted_by_field["evidence"], targeted_by_field["reported_actor"]]
    canary_owner_ids.extend(
        sorted(
            [row["task_id"] for row in truth_rows if row["task_id"] not in canary_owner_ids],
            key=lambda task_id: sha256_text(f"v99|perm|{task_id}"),
        )[:2]
    )
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
        "schema_version": V99_INPUT_VERSION,
        "task_count": 15,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth = {
        "schema_version": V99_TRUTH_VERSION,
        "reference_version": reference["reference_version"],
        "task_count": 15,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V99_INPUT_VERSION,
        "task_count": 4,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V99_SELECTION_VERSION,
        "created_at": now_iso(),
        "source_witness_count": len(pointwise.get("units") or []),
        "targeted_regression_count": 4,
        "targeted_regression_fields": list(TARGETED_FIELDS),
        "fresh_to_luna_count": 11,
        "selected_distinct_witness_count": len(used),
        "expected_correct_count": 8,
        "expected_incorrect_count": 7,
        "permutation_canary_count": 4,
        "canary_includes_prompt_fix_regressions": ["evidence", "reported_actor"],
        "selection_uses_source_text": False,
        "selection_rule": "four_v96_disagreements_plus_hash_ranked_fresh_slots_excluding_prior_luna_witnesses",
        "requested_field_presence_explicit": True,
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
        raise JudgeV5CalibrationV99Error("v99 task count drifted")
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
        raise JudgeV5CalibrationV99Error("v99 output coverage drifted")
    return {"schema_version": V99_OUTPUT_VERSION, "decisions": decisions}


def build_prompt_v99(value: Mapping[str, Any]) -> str:
    return (
        "Return one field decision for every opaque task_id. requested_field_value explicitly describes "
        "the existing requested field, including absence; never substitute another event field. Judge only "
        "that field under its contract and do not compare tasks. Every source_evidence_span must be an exact "
        "substring of that task's source_excerpt.\n\n"
        + json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    )


def score_v99(
    primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    score = score_v83(primary, canary, truth)
    score["schema_version"] = V99_SCORE_VERSION
    return score


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V99_CAPACITY_AUDIT_VERSION,
        "phase_id": V99_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_created(audit_path, audit, "v99 capacity audit")
    policy = {
        "schema_version": V99_CAPACITY_POLICY_VERSION,
        "phase_id": V99_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v99 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v99(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v98_root: Path = DEFAULT_V98_ROOT,
    v97_root: Path = DEFAULT_V97_ROOT,
    v96_root: Path = DEFAULT_V96_ROOT,
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
        v98_root=v98_root.resolve(),
        v97_root=v97_root.resolve(),
        v96_root=v96_root.resolve(),
        v94_root=v94_root.resolve(),
        v23_root=v23_root.resolve(),
        v75_root=v75_root.resolve(),
        v88_root=v88_root.resolve(),
        v91_root=v91_root.resolve(),
    )
    values = predecessor["values"]
    value, truth, canary, selection = build_v99_inputs(
        pointwise=values["v23_pointwise"],
        reference=values["v98_truth"],
        v94_rubric=values["v94_rubric"],
        v97_rubric=values["v97_rubric"],
        v96_input=values["v96_input"],
        v96_truth=values["v96_truth"],
        v96_output=values["v96_output"],
        prior_luna_truths=[
            values["v75_truth"],
            values["v88_truth"],
            values["v91_truth"],
            values["v96_truth"],
        ],
    )
    input_path = root / "refined-luna-input.private.json"
    truth_path = root / "refined-luna-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_created(selection_path, selection, "v99 selection audit")
    turn_values = primary_shards(value) + [canary]
    turns = []
    for turn_name, turn_value in zip(TURN_NAMES, turn_values, strict=True):
        prompt = build_prompt_v99(turn_value)
        schema = output_schema(turn_value)
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=turn_value, prompt=prompt, schema=schema
        )
        turns.append(
            {"turn_name": turn_name, "value": turn_value, "prompt": prompt, "schema": schema, "paths": paths}
        )
    capacity = _build_capacity_policy(root, predecessor["records"])
    runtime_dir = Path(__file__).resolve().parent
    runtime_files = [
        Path(__file__),
        runtime_dir / "app_server_judge_v5_calibration_v98_reference_v7_freeze.py",
        runtime_dir / "app_server_judge_v5_calibration_v97_residual_field_audit.py",
        runtime_dir / "app_server_judge_v5_calibration_v96_fresh_luna_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v94_systematic_field_audit.py",
        runtime_dir / "app_server_judge_v5_calibration_v83_fresh_alignment_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v75_exact_span_remaining_shard.py",
        runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py",
        runtime_dir / "app_server_capacity_reserve.py",
        runtime_dir / "codex_app_server.py",
    ]
    spec = {
        "schema_version": V99_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "refined_luna_diagnostic_with_four_v96_regressions_explicit_field_presence_and_reference_v7",
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
    spec_path = root / "refined-luna-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v99 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV99Error("immutable v99 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {"root": root, "spec": spec, "truth": truth, "turns": turns, "capacity_policy": capacity["policy"]}


def _real_attempts(root: Path) -> list[dict[str, Any]]:
    return [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]


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
            measured = _validate_usage(_load_json(Path(record["path"]), "v99 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V99_FAILURE_VERSION,
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
        "schema_version": V99_TERMINAL_VERSION,
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


async def run_v99(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v99 terminal")
    frozen = freeze_v99(output_dir=root, timeout_seconds=timeout_seconds)
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
                    base_instructions=base_instructions_v97(),
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
                    raise JudgeV5CalibrationV99Error("projected v99 output is invalid")
                if current_turn == CANARY_TURN:
                    canary_outputs.append(projected)
                else:
                    primary_outputs.append(projected)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
                operations.extend([{**row, "turn_name": current_turn} for row in turn_operations])
        primary = merge_outputs(primary_outputs, 15)
        canary = merge_outputs(canary_outputs, 4)
        primary_path = root / "refined-luna-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(primary_path, primary)
        _write_immutable(canary_path, canary)
        audit = {
            "schema_version": V99_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "projection_scope": "nonexact_source_span_removal_only",
            "semantic_status_changed": False,
            "privacy": "opaque_task_ids_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v99(primary, canary, frozen["truth"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "refined-luna-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V99_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v99_refined_luna_diagnostic_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v99_refined_luna_diagnostic_passed_full_calibration_authorized"
                if passed
                else "v99_refined_luna_diagnostic_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "fresh_full_development_calibration_authorized": passed,
            "bounded_observable_repair_authorized": score["bounded_observable_repair_authorized"],
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
    parser = argparse.ArgumentParser(description="Run v99 refined Luna diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v99(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
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
