from __future__ import annotations

"""Neutral Sol adjudication for v75 contested fields and matched canaries."""

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


V76_INPUT_VERSION = "pif_app_server_judge_v5_4_v76_neutral_adjudication_input_v1"
V76_TRUTH_VERSION = "pif_app_server_judge_v5_4_v76_neutral_adjudication_truth_v1"
V76_SELECTION_VERSION = "pif_app_server_judge_v5_4_v76_selection_audit_v1"
V76_SPEC_VERSION = "pif_app_server_judge_v5_4_v76_neutral_adjudication_spec_v1"
V76_SCORE_VERSION = "pif_app_server_judge_v5_4_v76_neutral_adjudication_score_v1"
V76_FAILURE_VERSION = "pif_app_server_judge_v5_4_v76_failure_v1"
V76_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v76_terminal_v1"
V76_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V76_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V76_PHASE_ID = "judge_v5_4_v76_neutral_contested_adjudication"
MODEL = "gpt-5.6-sol"
EFFORT = "high"
TURN_NAMES = tuple(f"neutral_adjudication_shard_{index:02d}" for index in range(5))
TASKS_PER_SHARD = 2
TIMEOUT_SECONDS = 600.0
STATUSES = ("correct", "incorrect", "abstain")
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V75_ROOT.parent / "judge-calibration-v5_4-v76-neutral-contested-adjudication"
).resolve()


class JudgeV5CalibrationV76Error(RuntimeError):
    """The v76 neutral adjudication contract cannot be preserved."""


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


def _validate_v75(v75_root: Path) -> dict[str, Any]:
    paths = {
        "v75_terminal": v75_root / "terminal.json",
        "v75_spec": v75_root / "exact-span-remaining-shard-spec.json",
        "v75_input": v75_root / "direct-field-input.private.json",
        "v75_truth": v75_root / "selected-truth.private.json",
        "v75_output": v75_root / "direct-field-output.private.json",
        "v75_score": v75_root / "direct-field-score.json",
        "v75_projection": v75_root / "final-projection-audit.json",
        "v75_capacity": v75_root / "turns/direct-field-shard-04-recovery/capacity.json",
        "v75_sidecar": v75_root / "turns/direct-field-shard-04-recovery/sidecar.json",
        "v75_turn_output": v75_root / "turns/direct-field-shard-04-recovery/output.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v75_terminal"]
    spec = values["v75_spec"]
    score = values["v75_score"]
    attempts = terminal.get("attempts") or []
    metrics = score.get("metrics") or {}
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v75_direct_field_audit_quality_gate_not_passed"
        or terminal.get("diagnostic_passed") is not False
        or terminal.get("proposal_verification_authorized") is not False
        or terminal.get("reference_patch_authorized") is not False
        or terminal.get("fresh_12_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("v74_semantic_turn_replayed") is not False
        or not _record_matches(terminal.get("output"), paths["v75_output"])
        or not _record_matches(terminal.get("score"), paths["v75_score"])
        or not _record_matches(terminal.get("projection_audit"), paths["v75_projection"])
        or metrics.get("task_count") != 15
        or metrics.get("control_exact_count") != 3
        or metrics.get("control_exact_rate") != 0.75
        or metrics.get("abstention_count") != 1
        or metrics.get("evidence_complete_rate") != 1.0
        or metrics.get("proposed_change_count") != 5
        or score.get("proposed_changes") != []
        or spec.get("v74_semantic_turn_replayed") is not False
        or spec.get("retry_count_per_turn") != 0
        or not all(_verify_record(record) for record in spec.get("runtime_files") or [])
        or len(attempts) != 1
        or not _record_matches(attempts[0].get("capacity"), paths["v75_capacity"])
        or not _record_matches(attempts[0].get("sidecar"), paths["v75_sidecar"])
        or not _record_matches(attempts[0].get("output"), paths["v75_turn_output"])
    ):
        raise JudgeV5CalibrationV76Error("v75 predecessor is inadmissible")
    sidecar = values["v75_sidecar"]
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
    ):
        raise JudgeV5CalibrationV76Error("v75 measured sidecar drifted")
    _validate_usage(sidecar)
    return {name: _record(path) for name, path in paths.items()}


def _adjudication_task_id(original_task_id: str) -> str:
    return "adjud_" + sha256_text(f"v76|{original_task_id}")[:24]


def build_v76_inputs(
    source: Mapping[str, Any],
    truth: Mapping[str, Any],
    primary_output: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    tasks = {row["task_id"]: row for row in source["tasks"]}
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in primary_output["decisions"]}
    if set(tasks) != set(expected) or set(tasks) != set(observed):
        raise JudgeV5CalibrationV76Error("v76 source coverage drifted")
    selected = []
    for task_id in sorted(tasks):
        truth_row = expected[task_id]
        primary_status = observed[task_id]["field_status"]
        if primary_status == "abstain":
            role = "primary_abstention"
        elif primary_status != truth_row["expected_status"]:
            role = (
                "primary_control_disagreement"
                if truth_row["role"] == "matched_control"
                else "primary_truth_disagreement"
            )
        elif truth_row["role"] == "matched_control":
            role = "matched_canary"
        else:
            continue
        selected.append((task_id, role))
    role_counts = Counter(role for _, role in selected)
    expected_counts = {
        "matched_canary": 3,
        "primary_abstention": 1,
        "primary_control_disagreement": 1,
        "primary_truth_disagreement": 5,
    }
    if dict(sorted(role_counts.items())) != expected_counts or len(selected) != 10:
        raise JudgeV5CalibrationV76Error("v76 selection drifted")
    model_tasks = []
    private_truth = []
    for original_task_id, role in selected:
        task = tasks[original_task_id]
        adjudication_id = _adjudication_task_id(original_task_id)
        model_tasks.append(
            {
                "task_id": adjudication_id,
                "field": task["field"],
                "field_contract": deepcopy(task["field_contract"]),
                "source_excerpt": task["source_excerpt"],
                "structured_event": deepcopy(task["structured_event"]),
            }
        )
        private_truth.append(
            {
                "task_id": adjudication_id,
                "original_task_id": original_task_id,
                "field": task["field"],
                "role": role,
                "expected_status": expected[original_task_id]["expected_status"],
                "primary_status": observed[original_task_id]["field_status"],
            }
        )
    model_tasks.sort(key=lambda row: row["task_id"])
    private_truth.sort(key=lambda row: row["task_id"])
    value = {
        "schema_version": V76_INPUT_VERSION,
        "task_count": 10,
        "tasks": model_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V76_TRUTH_VERSION,
        "task_count": 10,
        "tasks": private_truth,
    }
    audit = {
        "schema_version": V76_SELECTION_VERSION,
        "created_at": now_iso(),
        "source_version": "v75",
        "task_count": 10,
        "role_counts": expected_counts,
        "selection_uses_source_text": False,
        "selection_rule": "all_primary_truth_or_control_disagreements_all_primary_abstentions_and_all_remaining_matched_controls",
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "privacy": "aggregate_counts_only",
    }
    return value, truth_value, audit


def build_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value.get("tasks")
    if not isinstance(tasks, list) or len(tasks) != 10:
        raise JudgeV5CalibrationV76Error("v76 task count drifted")
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
            "shard_count": len(TURN_NAMES),
        }
        for index in range(0, 10, TASKS_PER_SHARD)
    ]


def base_instructions() -> str:
    return (
        "You are the neutral final adjudicator for isolated source-to-field tasks. Each task contains one "
        "source, one structured event, and one field contract. Decide independently with no knowledge of "
        "prior labels or systems. Mentally correct every other field. incorrect means this field's own value "
        "materially conflicts with the source or a materially required value is omitted; correct means this "
        "field alone is source-correct or not applicable. Actor and speaker are distinct roles. Cite exact "
        "source substrings for every decision. Abstain only when the supplied source cannot determine the "
        "field. Do not vote, use confidence, regex, keywords, overlap, embeddings, or infer system identity."
    )


def build_prompt(value: Mapping[str, Any]) -> str:
    return (
        "Return one final field decision for every opaque task_id. Do not compare tasks or emit a whole-event "
        "verdict. Every source_evidence_span must be an exact substring of that task's source_excerpt.\n\n"
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
        raise JudgeV5CalibrationV76Error("v76 schema exceeds supported subset")
    return schema


def validate_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping) or set(output) != {"decisions"}:
        return ["invalid_adjudication_root"]
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
        errors.append("adjudication_coverage")
    return errors


def merge_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [deepcopy(row) for output in outputs for row in output["decisions"]]
    ids = [row.get("task_id") for row in rows]
    if len(rows) != 10 or len(set(ids)) != 10:
        raise JudgeV5CalibrationV76Error("v76 output coverage drifted")
    return {"decisions": rows}


def score_v76(output: Mapping[str, Any], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in output["decisions"]}
    if set(expected) != set(observed):
        raise JudgeV5CalibrationV76Error("v76 score coverage drifted")
    canaries = [row for row in expected.values() if row["role"] == "matched_canary"]
    proposal_rows = [
        row
        for row in expected.values()
        if row["role"] in {"primary_truth_disagreement", "primary_control_disagreement"}
    ]
    abstention_rows = [row for row in expected.values() if row["role"] == "primary_abstention"]
    canary_exact = sum(
        observed[row["task_id"]]["field_status"] == row["expected_status"]
        for row in canaries
    )
    proposal_agreement = sum(
        observed[row["task_id"]]["field_status"] == row["primary_status"]
        for row in proposal_rows
    )
    abstentions = sum(row["field_status"] == "abstain" for row in observed.values())
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    checks = {
        "canary_exact_rate": canary_exact == 3,
        "primary_proposal_agreement_rate": proposal_agreement == 6,
        "primary_abstention_resolved": all(
            observed[row["task_id"]]["field_status"] in {"correct", "incorrect"}
            for row in abstention_rows
        ),
        "abstention_count": abstentions == 0,
        "evidence_complete_rate": evidence_complete == 10,
    }
    passed = all(checks.values())
    proposed_changes = [
        {
            "original_task_id": row["original_task_id"],
            "field": row["field"],
            "prior_status": row["expected_status"],
            "adjudicated_status": observed[row["task_id"]]["field_status"],
        }
        for row in expected.values()
        if row["role"] != "matched_canary"
        and observed[row["task_id"]]["field_status"] in {"correct", "incorrect"}
        and observed[row["task_id"]]["field_status"] != row["expected_status"]
    ]
    metrics = {
        "task_count": 10,
        "canary_count": 3,
        "canary_exact_count": canary_exact,
        "canary_exact_rate": round(canary_exact / 3, 6),
        "primary_proposal_count": 6,
        "primary_proposal_agreement_count": proposal_agreement,
        "primary_proposal_agreement_rate": round(proposal_agreement / 6, 6),
        "primary_abstention_count": 1,
        "adjudicator_abstention_count": abstentions,
        "evidence_complete_count": evidence_complete,
        "evidence_complete_rate": round(evidence_complete / 10, 6),
        "proposed_reference_change_count": len(proposed_changes),
    }
    return {
        "schema_version": V76_SCORE_VERSION,
        "passed": passed,
        "metrics": metrics,
        "checks": checks,
        "failed_checks": sorted(key for key, ok in checks.items() if not ok),
        "proposed_reference_changes": proposed_changes if passed else [],
        "reference_freeze_authorized": passed,
        "fresh_12_authorized": False,
        "gates_frozen_before_semantic_calls": True,
    }


def _write_stable_created(path: Path, value: Mapping[str, Any], purpose: str) -> dict[str, Any]:
    candidate = deepcopy(value)
    if path.exists():
        prior = _load_json(path, purpose)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV76Error(f"immutable {purpose} drifted")
        return prior
    _write_immutable(path, candidate)
    return candidate


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    phase_bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V76_CAPACITY_AUDIT_VERSION,
        "phase_id": V76_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": phase_bound,
        },
    }
    _write_stable_created(audit_path, audit, "v76 capacity audit")
    policy = {
        "schema_version": V76_CAPACITY_POLICY_VERSION,
        "phase_id": V76_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v76 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v76(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v75_root: Path = DEFAULT_V75_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v75(v75_root.resolve())
    source = _load_json(v75_root / "direct-field-input.private.json", "v75 input")
    truth = _load_json(v75_root / "selected-truth.private.json", "v75 truth")
    primary = _load_json(v75_root / "direct-field-output.private.json", "v75 output")
    value, private_truth, selection = build_v76_inputs(source, truth, primary)
    input_path = root / "neutral-adjudication-input.private.json"
    truth_path = root / "neutral-adjudication-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, private_truth)
    _write_stable_created(selection_path, selection, "v76 selection audit")
    turns = []
    turn_records = []
    for turn_name, shard in zip(TURN_NAMES, build_shards(value), strict=True):
        prompt = build_prompt(shard)
        schema = output_schema(shard)
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=shard, prompt=prompt, schema=schema
        )
        turns.append(
            {"turn_name": turn_name, "value": shard, "prompt": prompt, "schema": schema, "paths": paths}
        )
        turn_records.append(
            {
                "turn_name": turn_name,
                "input": _record(paths["input"]),
                "prompt": _record(paths["prompt"]),
                "schema": _record(paths["schema"]),
            }
        )
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V76_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds_per_turn": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "neutral_source_only_adjudication_over_seven_contested_tasks_plus_three_canaries",
        "task_count": 10,
        "contested_task_count": 7,
        "canary_count": 3,
        "tasks_per_shard": TASKS_PER_SHARD,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "perfect_canaries_full_primary_proposal_agreement_resolved_abstention_exact_evidence_authorizes_reference_freeze_only",
        "reference_freeze_authorized": False,
        "fresh_12_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v75_exact_span_remaining_shard.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v74_stable_direct_field_audit.py"),
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
    spec_path = root / "neutral-adjudication-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v76 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV76Error("immutable v76 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
        "truth": private_truth,
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
            usage = _validate_usage(_load_json(Path(record["path"]), "v76 sidecar"))
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
        "schema_version": V76_FAILURE_VERSION,
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
        "schema_version": V76_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "adjudication_passed": False,
        "reference_freeze_authorized": False,
        "fresh_12_authorized": False,
        "full_calibration_authorized": False,
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


async def run_v76(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v75_root: Path = DEFAULT_V75_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v76 terminal")
    frozen = freeze_v76(output_dir=root, v75_root=v75_root, timeout_seconds=timeout_seconds)
    factory = client_factory or _client_factory
    outputs = []
    sidecars = []
    adoptions = {}
    try:
        async with factory(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=turn["turn_name"],
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=base_instructions(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=TASKS_PER_SHARD,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, shard=turn["value"]: validate_output(candidate, shard),
                )
                outputs.append(output)
                sidecars.append(sidecar)
                adoptions[turn["turn_name"]] = adopted
        merged = merge_outputs(outputs)
        output_path = root / "neutral-adjudication-output.private.json"
        _write_immutable(output_path, merged)
        score = score_v76(merged, frozen["truth"])
        score_path = root / "neutral-adjudication-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V76_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v76_neutral_adjudication_passed_reference_freeze_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v76_neutral_adjudication_passed_reference_freeze_authorized"
                if passed
                else "v76_neutral_adjudication_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "adjudication_passed": passed,
            "reference_freeze_authorized": passed,
            "fresh_12_authorized": False,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(output_path),
            "attempts": _real_attempts(root),
            "completed_checkpoint_adoptions": adoptions,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, turn_name=exc.turn_name, error_class=exc.error_class)
    except Exception as exc:
        turn_name = frozen["turns"][len(outputs)]["turn_name"] if len(outputs) < 5 else None
        return _write_failure(root, turn_name=turn_name, error_class=type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v76 neutral contested adjudication")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v75-root", default=str(DEFAULT_V75_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v76(
            output_dir=Path(args.output_dir),
            v75_root=Path(args.v75_root),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "adjudication_passed": terminal.get("adjudication_passed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
