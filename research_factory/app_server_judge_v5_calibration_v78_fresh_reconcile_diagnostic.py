from __future__ import annotations

"""Fresh 15-task development diagnostic for reconcile-or-abstain judging."""

import argparse
import asyncio
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import validate_app_server_output_schema_subset
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
from .app_server_judge_v5_calibration_v77_reconcile_or_abstain_design import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V77_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _canonical_json,
    _record,
    _sha256_file,
    _validate_usage,
)
from .app_server_judge_v5_fixture import load_fixture_truth_audit
from .util import now_iso, sha256_text


V78_INPUT_VERSION = "pif_app_server_judge_v5_4_v78_fresh_input_v1"
V78_TRUTH_VERSION = "pif_app_server_judge_v5_4_v78_fresh_truth_v1"
V78_SELECTION_VERSION = "pif_app_server_judge_v5_4_v78_selection_audit_v1"
V78_SPEC_VERSION = "pif_app_server_judge_v5_4_v78_fresh_reconcile_spec_v1"
V78_RECONCILED_VERSION = "pif_app_server_judge_v5_4_v78_reconciled_output_v1"
V78_AUDIT_VERSION = "pif_app_server_judge_v5_4_v78_projection_audit_v1"
V78_SCORE_VERSION = "pif_app_server_judge_v5_4_v78_score_v1"
V78_FAILURE_VERSION = "pif_app_server_judge_v5_4_v78_failure_v1"
V78_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v78_terminal_v1"
V78_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V78_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V78_PHASE_ID = "judge_v5_4_v78_fresh_reconcile_diagnostic"
PRIMARY_MODEL = "gpt-5.6-luna"
ADJUDICATOR_MODEL = "gpt-5.6-sol"
EFFORT = "high"
SHARD_COUNT = 5
TASKS_PER_SHARD = 3
PRIMARY_TURNS = tuple(f"primary_direct_field_shard_{index:02d}" for index in range(SHARD_COUNT))
ADJUDICATOR_TURNS = tuple(
    f"adjudicator_direct_field_shard_{index:02d}" for index in range(SHARD_COUNT)
)
TURN_NAMES = PRIMARY_TURNS + ADJUDICATOR_TURNS
TIMEOUT_SECONDS = 600.0
STATUSES = ("correct", "incorrect", "abstain")
FIELD_POLARITY_SLOTS = (
    ("actor", "incorrect"),
    ("actor", "correct"),
    ("attribution", "incorrect"),
    ("attribution", "correct"),
    ("event_type", "incorrect"),
    ("event_type", "correct"),
    ("metric", "incorrect"),
    ("metric", "correct"),
    ("speaker", "incorrect"),
    ("speaker", "correct"),
    ("stance", "incorrect"),
    ("stance", "correct"),
    ("target", "incorrect"),
    ("target", "correct"),
    ("event_boundary", "correct"),
)
V23_ROOT = (
    DEFAULT_V77_ROOT.parent
    / "judge-calibration-v5_4-reference-v2-capacity-v23/fresh-attempt"
).resolve()
V24_ROOT = (
    DEFAULT_V77_ROOT.parent
    / "judge-calibration-v5_4-reference-v2-scoreable-recovery-v24"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V77_ROOT.parent / "judge-calibration-v5_4-v78-fresh-reconcile-diagnostic"
).resolve()


class JudgeV5CalibrationV78Error(RuntimeError):
    """The fresh reconcile diagnostic cannot preserve its evidence boundary."""


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


def _validate_sources(
    *, v77_root: Path, v23_root: Path, v24_root: Path
) -> dict[str, Any]:
    paths = {
        "v77_terminal": v77_root / "terminal.json",
        "v77_receipt": v77_root / "reconciliation-receipt.json",
        "v77_delta": v77_root / "reconciled-delta.private.json",
        "v23_spec": v23_root / "calibration-spec.json",
        "v23_pool": v23_root / "shared-witness-pool.private.json",
        "v23_truth": v23_root / "calibration-truth.private.json",
        "v23_pointwise": v23_root / "pointwise-input-full.private.json",
        "v23_mapping": v23_root / "witness-mapping.private.json",
        "v24_terminal": v24_root / "terminal.json",
        "v24_adoption": v24_root / "v23-adoption-receipt.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    v77_terminal = values["v77_terminal"]
    v77_receipt = values["v77_receipt"]
    v23_spec = values["v23_spec"]
    v24_terminal = values["v24_terminal"]
    v24_adoption = values["v24_adoption"]
    frozen = v23_spec.get("frozen_inputs") or {}
    if (
        v77_terminal.get("state") != "completed"
        or v77_terminal.get("fresh_15_development_diagnostic_authorized") is not True
        or v77_terminal.get("semantic_attempt_started") is not False
        or v77_terminal.get("production_mutated") is not False
        or not _record_matches(v77_terminal.get("reconciliation_receipt"), paths["v77_receipt"])
        or not _record_matches(v77_terminal.get("reconciled_delta"), paths["v77_delta"])
        or v77_receipt.get("settled_change_count") != 4
        or v77_receipt.get("abstention_count") != 3
        or v77_receipt.get("majority_voting_used") is not False
        or v23_spec.get("state") != "frozen_before_calibration_model_calls"
        or v23_spec.get("case_count") != 66
        or v23_spec.get("witness_count") != 182
        or not _record_matches(frozen.get("pool"), paths["v23_pool"])
        or not _record_matches(frozen.get("truth"), paths["v23_truth"])
        or not _record_matches(frozen.get("pointwise_full"), paths["v23_pointwise"])
        or not _record_matches(frozen.get("mapping"), paths["v23_mapping"])
        or v24_terminal.get("state") != "completed"
        or v24_terminal.get("terminal_reason")
        != "judge_full_calibration_quality_gate_not_passed"
        or v24_terminal.get("calibration_passed") is not False
        or v24_terminal.get("selection_authorized") is not False
        or v24_terminal.get("production_mutated") is not False
        or v24_terminal.get("usage_status") != "complete"
        or v24_terminal.get("accounting_complete") is not True
        or v24_adoption.get("all_completed_outputs_adopted") is not True
        or v24_adoption.get("source_semantic_turn_replay_allowed") is not False
        or v24_adoption.get("source_unknown_usage_turn_count") != 0
    ):
        raise JudgeV5CalibrationV78Error("v77/v23/v24 source contract drifted")
    return {name: _record(path) for name, path in paths.items()}


def _field_contracts() -> dict[str, dict[str, Any]]:
    fixture = load_fixture_truth_audit()
    return {row["field"]: deepcopy(row) for row in fixture["mismatch_checklist"]}


def _fresh_task_id(case_id: str, witness_id: str, field: str) -> str:
    return "fresh_" + sha256_text(f"v78|{case_id}|{witness_id}|{field}")[:24]


def build_v78_inputs(
    pointwise: Mapping[str, Any],
    truth: Mapping[str, Any],
    prior_selected_truth: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    contracts = _field_contracts()
    prior_witnesses = {
        (row["case_id"], row["witness_id"])
        for row in prior_selected_truth["tasks"]
    }
    units = [
        row
        for row in pointwise["units"]
        if (row["case_id"], row["witness_id"]) not in prior_witnesses
    ]
    selected = []
    used_witnesses = set()
    for field, status in FIELD_POLARITY_SLOTS:
        candidates = []
        for unit in units:
            identity = (unit["case_id"], unit["witness_id"])
            issues = set(
                truth["cases"][identity[0]]["field_issues"].get(identity[1], [])
            )
            expected_status = "incorrect" if field in issues else "correct"
            if expected_status == status and identity not in used_witnesses:
                candidates.append(unit)
        candidates.sort(
            key=lambda row: sha256_text(
                f"v78-select|{field}|{status}|{row['case_id']}|{row['witness_id']}"
            )
        )
        if not candidates:
            raise JudgeV5CalibrationV78Error("fresh slot has no distinct witness")
        chosen = candidates[0]
        used_witnesses.add((chosen["case_id"], chosen["witness_id"]))
        selected.append((field, status, chosen))
    if len(selected) != 15 or len(used_witnesses) != 15:
        raise JudgeV5CalibrationV78Error("fresh selection coverage drifted")
    model_tasks = []
    truth_rows = []
    for field, status, unit in selected:
        task_id = _fresh_task_id(unit["case_id"], unit["witness_id"], field)
        model_tasks.append(
            {
                "task_id": task_id,
                "field": field,
                "field_contract": contracts[field],
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
    model_tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    value = {
        "schema_version": V78_INPUT_VERSION,
        "task_count": 15,
        "tasks": model_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V78_TRUTH_VERSION,
        "task_count": 15,
        "tasks": truth_rows,
    }
    audit = {
        "schema_version": V78_SELECTION_VERSION,
        "created_at": now_iso(),
        "source_witness_count": len(pointwise["units"]),
        "excluded_prior_witness_count": len(prior_witnesses),
        "selected_task_count": 15,
        "selected_distinct_witness_count": len(used_witnesses),
        "expected_correct_count": sum(status == "correct" for _, status, _ in selected),
        "expected_incorrect_count": sum(status == "incorrect" for _, status, _ in selected),
        "field_counts": dict(sorted(Counter(field for field, _, _ in selected).items())),
        "selection_uses_source_text": False,
        "selection_rule": "fixed_field_polarity_slots_hash_ranked_distinct_witnesses_excluding_v75_cohort",
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return value, truth_value, audit


def build_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value.get("tasks")
    if not isinstance(tasks, list) or len(tasks) != 15:
        raise JudgeV5CalibrationV78Error("v78 task count drifted")
    return [
        {
            "schema_version": value["schema_version"],
            "task_count": TASKS_PER_SHARD,
            "tasks": deepcopy(tasks[index : index + TASKS_PER_SHARD]),
            "prior_labels_present": False,
            "prior_model_decisions_present": False,
            "system_identity_present": False,
            "tasks_are_independent": True,
            "shard_ordinal": index // TASKS_PER_SHARD,
            "shard_count": SHARD_COUNT,
        }
        for index in range(0, 15, TASKS_PER_SHARD)
    ]


def base_instructions(role: str) -> str:
    return (
        f"You are the {role} in a side-free source-to-field evaluation. Each task contains one source, one "
        "structured event, and one field contract. Decide independently with no knowledge of truth labels or "
        "other model outputs. Mentally correct every other field. incorrect means this field's value materially "
        "conflicts with the source or a materially required value is omitted; correct means this field alone is "
        "source-correct or not applicable. Actor and speaker are distinct roles. Cite exact source substrings. "
        "Abstain only when the supplied source cannot determine the field. Do not vote, use confidence, regex, "
        "keywords, overlap, embeddings, or infer system identity."
    )


def build_prompt(value: Mapping[str, Any]) -> str:
    return (
        "Return one field decision for every opaque task_id. Do not compare tasks or emit a whole-event verdict. "
        "Every source_evidence_span must be an exact substring of that task's source_excerpt.\n\n"
        + _canonical_json(value)
        + "\n"
    )


def output_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    task_ids = [row["task_id"] for row in value["tasks"]]
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["decisions"],
        "properties": {
            "decisions": {
                "type": "array",
                "minItems": len(task_ids),
                "maxItems": len(task_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["task_id", "field_status", "source_evidence_spans", "rationale"],
                    "properties": {
                        "task_id": {"type": "string", "enum": task_ids},
                        "field_status": {"type": "string", "enum": list(STATUSES)},
                        "source_evidence_spans": {
                            "type": "array",
                            "maxItems": 2,
                            "items": {"type": "string", "minLength": 1, "maxLength": 1000},
                        },
                        "rationale": {"type": "string", "minLength": 1, "maxLength": 260},
                    },
                },
            }
        },
    }
    if validate_app_server_output_schema_subset(schema):
        raise JudgeV5CalibrationV78Error("v78 schema exceeds supported subset")
    return schema


def validate_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping) or set(output) != {"decisions"}:
        return ["invalid_fresh_root"]
    tasks = {row["task_id"]: row for row in value["tasks"]}
    seen = set()
    errors = []
    for index, row in enumerate(output.get("decisions") or []):
        prefix = f"decision_{index}"
        if not isinstance(row, Mapping) or set(row) != {
            "task_id", "field_status", "source_evidence_spans", "rationale"
        }:
            errors.append(prefix + "_shape")
            continue
        task_id = row.get("task_id")
        if task_id not in tasks or task_id in seen:
            errors.append(prefix + "_identity")
            continue
        seen.add(task_id)
        spans = row.get("source_evidence_spans")
        source_text = tasks[task_id]["source_excerpt"]
        if row.get("field_status") not in STATUSES:
            errors.append(prefix + "_status")
        if (
            not isinstance(spans, list)
            or len(spans) > 2
            or len(spans) != len(set(spans))
            or any(not isinstance(span, str) or not span or span not in source_text for span in spans)
        ):
            errors.append(prefix + "_evidence")
        if not isinstance(row.get("rationale"), str) or not row["rationale"]:
            errors.append(prefix + "_rationale")
    if seen != set(tasks):
        errors.append("fresh_coverage")
    return errors


def merge_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [deepcopy(row) for output in outputs for row in output["decisions"]]
    ids = [row.get("task_id") for row in rows]
    if len(rows) != 15 or len(set(ids)) != 15:
        raise JudgeV5CalibrationV78Error("v78 output coverage drifted")
    return {"decisions": rows}


def reconcile_outputs(
    primary: Mapping[str, Any], adjudicator: Mapping[str, Any]
) -> dict[str, Any]:
    left = {row["task_id"]: row for row in primary["decisions"]}
    right = {row["task_id"]: row for row in adjudicator["decisions"]}
    if set(left) != set(right):
        raise JudgeV5CalibrationV78Error("v78 reconciliation coverage drifted")
    rows = []
    for task_id in sorted(left):
        left_status = left[task_id]["field_status"]
        right_status = right[task_id]["field_status"]
        reconciled = (
            left_status
            if left_status in {"correct", "incorrect"} and left_status == right_status
            else "abstain"
        )
        rows.append(
            {
                "task_id": task_id,
                "primary_status": left_status,
                "adjudicator_status": right_status,
                "reconciled_status": reconciled,
                "basis": (
                    "independent_llm_agreement"
                    if reconciled != "abstain"
                    else "independent_llm_disagreement_or_abstention"
                ),
            }
        )
    return {"schema_version": V78_RECONCILED_VERSION, "decisions": rows}


def score_v78(
    reconciled: Mapping[str, Any],
    truth: Mapping[str, Any],
    primary: Mapping[str, Any],
    adjudicator: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    final = {row["task_id"]: row for row in reconciled["decisions"]}
    left = {row["task_id"]: row for row in primary["decisions"]}
    right = {row["task_id"]: row for row in adjudicator["decisions"]}
    if not (set(expected) == set(final) == set(left) == set(right)):
        raise JudgeV5CalibrationV78Error("v78 score coverage drifted")
    incorrect = [row for row in expected.values() if row["expected_status"] == "incorrect"]
    correct = [row for row in expected.values() if row["expected_status"] == "correct"]
    exact = sum(
        final[task_id]["reconciled_status"] == row["expected_status"]
        for task_id, row in expected.items()
    )
    sensitivity = sum(
        final[row["task_id"]]["reconciled_status"] == "incorrect" for row in incorrect
    )
    specificity = sum(
        final[row["task_id"]]["reconciled_status"] == "correct" for row in correct
    )
    abstentions = sum(row["reconciled_status"] == "abstain" for row in final.values())
    agreement = sum(
        left[task_id]["field_status"] == right[task_id]["field_status"]
        and left[task_id]["field_status"] in {"correct", "incorrect"}
        for task_id in expected
    )
    primary_evidence = sum(bool(row["source_evidence_spans"]) for row in left.values())
    adjudicator_evidence = sum(bool(row["source_evidence_spans"]) for row in right.values())
    metrics = {
        "task_count": 15,
        "incorrect_count": len(incorrect),
        "correct_count": len(correct),
        "exact_count": exact,
        "exact_rate": round(exact / 15, 6),
        "incorrect_sensitivity": round(sensitivity / len(incorrect), 6),
        "correct_specificity": round(specificity / len(correct), 6),
        "independent_agreement_count": agreement,
        "independent_agreement_rate": round(agreement / 15, 6),
        "reconciled_abstention_count": abstentions,
        "primary_evidence_complete_rate": round(primary_evidence / 15, 6),
        "adjudicator_evidence_complete_rate": round(adjudicator_evidence / 15, 6),
    }
    checks = {
        "exact_rate": metrics["exact_rate"] == 1.0,
        "incorrect_sensitivity": metrics["incorrect_sensitivity"] == 1.0,
        "correct_specificity": metrics["correct_specificity"] == 1.0,
        "independent_agreement_rate": metrics["independent_agreement_rate"] == 1.0,
        "reconciled_abstention_count": abstentions == 0,
        "primary_evidence_complete_rate": metrics["primary_evidence_complete_rate"] == 1.0,
        "adjudicator_evidence_complete_rate": metrics["adjudicator_evidence_complete_rate"] == 1.0,
    }
    return {
        "schema_version": V78_SCORE_VERSION,
        "passed": all(checks.values()),
        "metrics": metrics,
        "checks": checks,
        "failed_checks": sorted(key for key, ok in checks.items() if not ok),
        "fresh_full_development_calibration_authorized": all(checks.values()),
        "selection_authorized": False,
        "holdout_authorized": False,
        "gates_frozen_before_semantic_calls": True,
    }


def _write_stable_created(path: Path, value: Mapping[str, Any], purpose: str) -> dict[str, Any]:
    candidate = deepcopy(value)
    if path.exists():
        prior = _load_json(path, purpose)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV78Error(f"immutable {purpose} drifted")
        return prior
    _write_immutable(path, candidate)
    return candidate


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    phase_bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V78_CAPACITY_AUDIT_VERSION,
        "phase_id": V78_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": phase_bound,
        },
    }
    _write_stable_created(audit_path, audit, "v78 capacity audit")
    policy = {
        "schema_version": V78_CAPACITY_POLICY_VERSION,
        "phase_id": V78_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v78 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v78(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v77_root: Path = DEFAULT_V77_ROOT,
    v23_root: Path = V23_ROOT,
    v24_root: Path = V24_ROOT,
    v75_root: Path = DEFAULT_V75_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    sources = _validate_sources(
        v77_root=v77_root.resolve(), v23_root=v23_root.resolve(), v24_root=v24_root.resolve()
    )
    value, truth, selection = build_v78_inputs(
        _load_json(v23_root / "pointwise-input-full.private.json", "v23 pointwise"),
        _load_json(v23_root / "calibration-truth.private.json", "v23 truth"),
        _load_json(v75_root / "selected-truth.private.json", "v75 selected truth"),
    )
    input_path = root / "fresh-input.private.json"
    truth_path = root / "fresh-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_stable_created(selection_path, selection, "v78 selection audit")
    shards = build_shards(value)
    turns = []
    turn_records = []
    for role, model, names in (
        ("primary field auditor", PRIMARY_MODEL, PRIMARY_TURNS),
        ("independent neutral adjudicator", ADJUDICATOR_MODEL, ADJUDICATOR_TURNS),
    ):
        instructions = base_instructions(role)
        for turn_name, shard in zip(names, shards, strict=True):
            prompt = build_prompt(shard)
            schema = output_schema(shard)
            paths = _freeze_turn_request(
                root=root, turn_name=turn_name, input_value=shard, prompt=prompt, schema=schema
            )
            turns.append(
                {
                    "turn_name": turn_name,
                    "role": role,
                    "model": model,
                    "value": shard,
                    "prompt": prompt,
                    "schema": schema,
                    "base_instructions": instructions,
                    "paths": paths,
                }
            )
            turn_records.append(
                {
                    "turn_name": turn_name,
                    "role": role,
                    "model": model,
                    "input": _record(paths["input"]),
                    "prompt": _record(paths["prompt"]),
                    "schema": _record(paths["schema"]),
                }
            )
    capacity = _build_capacity_policy(root, sources)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V78_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "primary_model": PRIMARY_MODEL,
        "adjudicator_model": ADJUDICATOR_MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds_per_turn": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "independent_direct_field_passes_reconcile_agreement_else_abstain",
        "task_count": 15,
        "distinct_witness_count": 15,
        "tasks_per_shard": TASKS_PER_SHARD,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "perfect_fresh_15_exact_sensitivity_specificity_agreement_evidence_no_abstention_authorizes_full_development_calibration_only",
        "fresh_full_development_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "sources": sources,
        "runtime_files": [
            _record(Path(__file__)),
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
            "selection_audit": _record(selection_path),
            "turns": turn_records,
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "fresh-reconcile-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v78 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV78Error("immutable v78 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
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


def _failure_accounting(root: Path) -> dict[str, Any]:
    attempts = _real_attempts(root)
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            usage = _validate_usage(_load_json(Path(record["path"]), "v78 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = unknown == 0
    return {
        "attempts": attempts,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": known if complete else None,
        "unknown_usage_attempt_count": unknown,
    }


def _write_failure(root: Path, *, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    accounting = _failure_accounting(root)
    failure = {
        "schema_version": V78_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        **accounting,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V78_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "diagnostic_passed": False,
        "fresh_full_development_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": accounting["accounting_complete"],
        "usage_status": accounting["usage_status"],
        "usage": accounting["usage"],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v78(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v78 terminal")
    frozen = freeze_v78(output_dir=root, timeout_seconds=timeout_seconds)
    factory = client_factory or _client_factory
    outputs: dict[str, list[dict[str, Any]]] = {"primary": [], "adjudicator": []}
    sidecars = []
    adoptions = {}
    operations = []
    try:
        async with factory(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=turn["turn_name"],
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=turn["base_instructions"],
                    model=turn["model"],
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=TASKS_PER_SHARD,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, shard=turn["value"]: validate_output(
                        project_exact_spans(candidate, shard)[0], shard
                    ),
                )
                projected, turn_operations = project_exact_spans(output, turn["value"])
                if validate_output(projected, turn["value"]):
                    raise JudgeV5CalibrationV78Error("projected v78 output is invalid")
                lane = "primary" if turn["turn_name"] in PRIMARY_TURNS else "adjudicator"
                outputs[lane].append(projected)
                sidecars.append(sidecar)
                adoptions[turn["turn_name"]] = adopted
                operations.extend(
                    [{**row, "lane": lane, "turn_name": turn["turn_name"]} for row in turn_operations]
                )
        primary = merge_outputs(outputs["primary"])
        adjudicator = merge_outputs(outputs["adjudicator"])
        primary_path = root / "primary-output.private.json"
        adjudicator_path = root / "adjudicator-output.private.json"
        _write_immutable(primary_path, primary)
        _write_immutable(adjudicator_path, adjudicator)
        reconciled = reconcile_outputs(primary, adjudicator)
        reconciled_path = root / "reconciled-output.private.json"
        _write_immutable(reconciled_path, reconciled)
        audit = {
            "schema_version": V78_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "projection_scope": "nonexact_source_span_removal_only",
            "semantic_status_changed": False,
            "reconciliation_is_status_agreement_or_abstain_only": True,
            "majority_voting_used": False,
            "privacy": "opaque_task_ids_field_enums_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v78(reconciled, frozen["truth"], primary, adjudicator)
        score["projection_audit"] = _record(audit_path)
        score_path = root / "fresh-reconcile-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V78_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v78_fresh_reconcile_passed_full_development_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v78_fresh_reconcile_passed_full_development_calibration_authorized"
                if passed
                else "v78_fresh_reconcile_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "diagnostic_passed": passed,
            "fresh_full_development_calibration_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "primary_output": _record(primary_path),
            "adjudicator_output": _record(adjudicator_path),
            "reconciled_output": _record(reconciled_path),
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
        completed = sum(len(rows) for rows in outputs.values())
        turn_name = frozen["turns"][completed]["turn_name"] if completed < len(TURN_NAMES) else None
        return _write_failure(root, turn_name=turn_name, error_class=type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v78 fresh reconcile diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v78(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "diagnostic_passed": terminal.get("diagnostic_passed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
