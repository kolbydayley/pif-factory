from __future__ import annotations

"""Direct side-free field audit over v72 disagreements and matched controls."""

import argparse
import asyncio
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import CHECKLIST_FIELDS, validate_app_server_output_schema_subset
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
from .app_server_judge_v5_calibration_v72_sharded_field_microtasks import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V72_ROOT,
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


V73_INPUT_VERSION = "pif_app_server_judge_v5_4_v73_direct_field_input_v1"
V73_TRUTH_VERSION = "pif_app_server_judge_v5_4_v73_direct_field_truth_v1"
V73_TAXONOMY_VERSION = "pif_app_server_judge_v5_4_v73_error_taxonomy_v1"
V73_SPEC_VERSION = "pif_app_server_judge_v5_4_v73_direct_field_spec_v1"
V73_SCORE_VERSION = "pif_app_server_judge_v5_4_v73_direct_field_score_v1"
V73_FAILURE_VERSION = "pif_app_server_judge_v5_4_v73_failure_v1"
V73_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v73_terminal_v1"
V73_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V73_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V73_PHASE_ID = "judge_v5_4_v73_direct_field_audit"
MODEL = "gpt-5.6-luna"
EFFORT = "high"
TURN_NAMES = tuple(f"direct_field_shard_{index:02d}" for index in range(5))
TASKS_PER_SHARD = 3
TIMEOUT_SECONDS = 600.0
STATUSES = ("correct", "incorrect", "abstain")
EXCLUDED_CONTROL_FIELDS = {"evidence", "unsupported_inference"}
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V72_ROOT.parent / "judge-calibration-v5_4-v73-direct-field-audit"
).resolve()


class JudgeV5CalibrationV73Error(RuntimeError):
    """The direct-field audit cannot preserve its evidence contract."""


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


def _validate_v72(v72_root: Path) -> dict[str, Any]:
    paths = {
        "v72_terminal": v72_root / "terminal.json",
        "v72_spec": v72_root / "sharded-field-microtask-spec.json",
        "v72_input": v72_root / "field-microtask-input.private.json",
        "v72_truth": v72_root / "diagnostic-truth.private.json",
        "v72_roles": v72_root / "cohort-roles.json",
        "v72_output": v72_root / "field-microtask-output.private.json",
        "v72_checklist": v72_root / "assembled-checklist.private.json",
        "v72_score": v72_root / "field-microtask-score.json",
        "v72_audit": v72_root / "projection-audit.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v72_terminal"]
    spec = values["v72_spec"]
    score = values["v72_score"]
    attempts = terminal.get("attempts") or []
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v72_sharded_field_microtask_quality_gate_not_passed"
        or terminal.get("field_microtask_diagnostic_passed") is not False
        or terminal.get("fresh_12_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("turn_count") != 6
        or not _record_matches(terminal.get("output"), paths["v72_output"])
        or not _record_matches(terminal.get("assembled_checklist"), paths["v72_checklist"])
        or not _record_matches(terminal.get("score"), paths["v72_score"])
        or not _record_matches(terminal.get("projection_audit"), paths["v72_audit"])
        or score.get("passed") is not False
        or score.get("metrics", {}).get("root_field_f1") != 0.645161
        or score.get("metrics", {}).get("root_checklist_cell_accuracy") != 0.877778
        or score.get("metrics", {}).get("exact_case_rate") != 0.166667
        or score.get("metrics", {}).get("control_exact_rate") != 0.5
        or spec.get("retry_count_per_turn") != 0
        or spec.get("v71_turn_replayed") is not False
        or not all(_verify_record(record) for record in spec.get("runtime_files") or [])
        or len(attempts) != 6
    ):
        raise JudgeV5CalibrationV73Error("v72 predecessor is inadmissible")
    for attempt in attempts:
        if (
            attempt.get("state") != "completed"
            or attempt.get("status") != "completed"
            or attempt.get("usage_status") != "measured"
            or not _verify_record(attempt.get("capacity"))
            or not _verify_record(attempt.get("sidecar"))
            or not _verify_record(attempt.get("output"))
        ):
            raise JudgeV5CalibrationV73Error("v72 measured attempt drifted")
        sidecar = _load_json(Path(attempt["sidecar"]["path"]), "v72 sidecar")
        if (
            sidecar.get("auth_type") != "chatgpt"
            or sidecar.get("plan_type") != "pro"
            or sidecar.get("usage_status") != "measured"
            or sidecar.get("usage_complete") is not True
            or _validate_usage(sidecar) is None
        ):
            raise JudgeV5CalibrationV73Error("v72 sidecar drifted")
    return {name: _record(path) for name, path in paths.items()}


def _field_contracts() -> dict[str, dict[str, Any]]:
    fixture = load_fixture_truth_audit()
    contracts = {row["field"]: deepcopy(row) for row in fixture["mismatch_checklist"]}
    if set(contracts) != set(CHECKLIST_FIELDS):
        raise JudgeV5CalibrationV73Error("field contracts drifted")
    return contracts


def _task_id(case_id: str, witness_id: str, field: str) -> str:
    return "direct_" + sha256_text(f"v73|{case_id}|{witness_id}|{field}")[:24]


def build_v73_inputs(
    value: Mapping[str, Any],
    truth: Mapping[str, Any],
    checklist: Mapping[str, Any],
    roles: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    contracts = _field_contracts()
    units = {(row["case_id"], row["witness_id"]): row for row in value["units"]}
    role_map = {(row["case_id"], row["witness_id"]): row["role"] for row in roles["units"]}
    observed = {
        (row["case_id"], row["witness_id"]): {
            cell["field"] for cell in row["checklist"] if cell["root_status"] == "root"
        }
        for row in checklist["units"]
    }
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
        if (case_id, witness_id) in units
    }
    if set(units) != set(role_map) or set(units) != set(observed) or set(units) != set(expected):
        raise JudgeV5CalibrationV73Error("v73 witness coverage drifted")
    disagreements = []
    matched_root = []
    matched_not_root = []
    for identity in units:
        for field in CHECKLIST_FIELDS:
            expected_status = "incorrect" if field in expected[identity] else "correct"
            observed_status = "incorrect" if field in observed[identity] else "correct"
            row = {
                "identity": identity,
                "field": field,
                "expected_status": expected_status,
                "observed_status": observed_status,
            }
            if expected_status != observed_status:
                disagreements.append(row)
            elif field not in EXCLUDED_CONTROL_FIELDS:
                (matched_root if expected_status == "incorrect" else matched_not_root).append(row)
    if len(disagreements) != 11:
        raise JudgeV5CalibrationV73Error("v73 disagreement count drifted")
    rank = lambda row: sha256_text(  # noqa: E731 - fixed deterministic rank
        f"v73-control|{row['identity'][0]}|{row['identity'][1]}|{row['field']}"
    )
    controls = sorted(matched_root, key=rank)[:2] + sorted(matched_not_root, key=rank)[:2]
    selected = disagreements + controls
    if len(selected) != 15:
        raise JudgeV5CalibrationV73Error("v73 selected task count drifted")
    tasks = []
    truth_rows = []
    for row in selected:
        identity = row["identity"]
        unit = units[identity]
        task_id = _task_id(identity[0], identity[1], row["field"])
        tasks.append(
            {
                "task_id": task_id,
                "field": row["field"],
                "field_contract": contracts[row["field"]],
                "source_excerpt": unit["source_excerpt"],
                "structured_event": deepcopy(unit["structured_event"]),
            }
        )
        truth_rows.append(
            {
                "task_id": task_id,
                "case_id": identity[0],
                "witness_id": identity[1],
                "field": row["field"],
                "role": "disagreement" if row in disagreements else "matched_control",
                "control_polarity": (
                    row["expected_status"] if row not in disagreements else None
                ),
                "expected_status": row["expected_status"],
                "v72_observed_status": row["observed_status"],
                "witness_role": role_map[identity],
            }
        )
    tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    if len({row["task_id"] for row in tasks}) != 15:
        raise JudgeV5CalibrationV73Error("v73 task identities drifted")
    input_value = {
        "schema_version": V73_INPUT_VERSION,
        "task_count": 15,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V73_TRUTH_VERSION,
        "task_count": 15,
        "tasks": truth_rows,
    }
    fp = Counter(row["field"] for row in disagreements if row["observed_status"] == "incorrect")
    fn = Counter(row["field"] for row in disagreements if row["observed_status"] == "correct")
    taxonomy = {
        "schema_version": V73_TAXONOMY_VERSION,
        "created_at": now_iso(),
        "source_version": "v72",
        "disagreement_count": len(disagreements),
        "matched_control_count": len(controls),
        "false_positive_field_counts": dict(sorted(fp.items())),
        "false_negative_field_counts": dict(sorted(fn.items())),
        "false_positive_count": sum(fp.values()),
        "false_negative_count": sum(fn.values()),
        "selection_uses_source_text": False,
        "selection_rule": "all_v72_truth_disagreements_plus_two_hash_ranked_root_and_two_hash_ranked_nonroot_matches",
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return input_value, truth_value, taxonomy


def build_v73_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value.get("tasks")
    if not isinstance(tasks, list) or len(tasks) != 15:
        raise JudgeV5CalibrationV73Error("v73 task count drifted")
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
        for index in range(0, 15, TASKS_PER_SHARD)
    ]


def base_instructions() -> str:
    return (
        "You are a side-free source-to-field auditor. Each task is independent and contains one source, "
        "one structured event, and exactly one field contract. Mentally correct every other event field. "
        "Return incorrect only if this field's populated value materially conflicts with the source or a "
        "materially required value for this field is omitted. Return correct when this field alone is "
        "source-correct or not applicable. Actor and speaker are separate roles and must be evaluated only "
        "under their own definitions. Do not propagate another field's error, infer system identity, vote, "
        "use confidence, regex, keywords, overlap, or embeddings. Cite exact source substrings for every "
        "correct or incorrect decision; abstain only when the supplied source truly cannot decide the field."
    )


def build_prompt(value: Mapping[str, Any]) -> str:
    return (
        "Return exactly one decision per opaque task_id. Do not compare tasks or emit a whole-event verdict. "
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
        raise JudgeV5CalibrationV73Error("v73 schema exceeds supported subset")
    return schema


def validate_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping) or set(output) != {"decisions"}:
        return ["invalid_direct_field_root"]
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
        source = tasks[task_id]["source_excerpt"]
        if row.get("field_status") not in STATUSES:
            errors.append(prefix + "_status")
        if (
            not isinstance(spans, list)
            or len(spans) > 2
            or len(spans) != len(set(spans))
            or any(not isinstance(span, str) or not span or span not in source for span in spans)
        ):
            errors.append(prefix + "_evidence")
        if not isinstance(row.get("rationale"), str) or not row["rationale"]:
            errors.append(prefix + "_rationale")
    if seen != set(tasks):
        errors.append("direct_field_coverage")
    return errors


def merge_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [deepcopy(row) for output in outputs for row in output["decisions"]]
    ids = [row.get("task_id") for row in rows]
    if len(rows) != 15 or len(set(ids)) != 15:
        raise JudgeV5CalibrationV73Error("v73 output coverage drifted")
    return {"decisions": rows}


def score_v73(output: Mapping[str, Any], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in output["decisions"]}
    if set(expected) != set(observed):
        raise JudgeV5CalibrationV73Error("v73 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "matched_control"]
    disagreements = [row for row in expected.values() if row["role"] == "disagreement"]
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["expected_status"]
        for row in controls
    )
    abstentions = sum(row["field_status"] == "abstain" for row in observed.values())
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    proposed_changes = [
        {
            "task_id": row["task_id"],
            "field": row["field"],
            "prior_status": row["expected_status"],
            "proposed_status": observed[row["task_id"]]["field_status"],
        }
        for row in disagreements
        if observed[row["task_id"]]["field_status"] in {"correct", "incorrect"}
        and observed[row["task_id"]]["field_status"] != row["expected_status"]
    ]
    metrics = {
        "task_count": 15,
        "disagreement_task_count": len(disagreements),
        "matched_control_count": len(controls),
        "control_exact_count": control_exact,
        "control_exact_rate": round(control_exact / len(controls), 6),
        "abstention_count": abstentions,
        "evidence_complete_count": evidence_complete,
        "evidence_complete_rate": round(evidence_complete / 15, 6),
        "proposed_change_count": len(proposed_changes),
    }
    checks = {
        "control_exact_rate": metrics["control_exact_rate"] == 1.0,
        "abstention_count": abstentions == 0,
        "evidence_complete_rate": metrics["evidence_complete_rate"] == 1.0,
        "disagreement_task_count": len(disagreements) == 11,
    }
    passed = all(checks.values())
    return {
        "schema_version": V73_SCORE_VERSION,
        "passed": passed,
        "metrics": metrics,
        "checks": checks,
        "failed_checks": sorted(key for key, ok in checks.items() if not ok),
        "proposed_changes": proposed_changes if passed else [],
        "proposals_authorized_for_neutral_verification": passed,
        "reference_patch_authorized": False,
        "fresh_12_authorized": False,
        "gates_frozen_before_semantic_calls": True,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    phase_bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V73_CAPACITY_AUDIT_VERSION,
        "phase_id": V73_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": phase_bound,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v73 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV73Error("immutable v73 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V73_CAPACITY_POLICY_VERSION,
        "phase_id": V73_PHASE_ID,
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
    if policy_path.exists():
        prior = _load_json(policy_path, "v73 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV73Error("immutable v73 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v73(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v72_root: Path = DEFAULT_V72_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v72(v72_root.resolve())
    source = _load_json(v72_root / "field-microtask-input.private.json", "v72 input")
    truth = _load_json(v72_root / "diagnostic-truth.private.json", "v72 truth")
    checklist = _load_json(v72_root / "assembled-checklist.private.json", "v72 checklist")
    roles = _load_json(v72_root / "cohort-roles.json", "v72 roles")
    value, selected_truth, taxonomy = build_v73_inputs(source, truth, checklist, roles)
    input_path = root / "direct-field-input.private.json"
    truth_path = root / "selected-truth.private.json"
    taxonomy_path = root / "error-taxonomy.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, selected_truth)
    _write_immutable(taxonomy_path, taxonomy)
    turns = []
    turn_records = []
    for turn_name, shard in zip(TURN_NAMES, build_v73_shards(value), strict=True):
        prompt = build_prompt(shard)
        schema = output_schema(shard)
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=shard,
            prompt=prompt,
            schema=schema,
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
        "schema_version": V73_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds_per_turn": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "direct_isolated_field_audit_over_all_v72_disagreements_plus_four_matched_controls",
        "task_count": 15,
        "disagreement_task_count": 11,
        "matched_control_count": 4,
        "tasks_per_shard": TASKS_PER_SHARD,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "perfect_controls_no_abstention_exact_evidence_authorizes_neutral_proposal_verification_only",
        "reference_patch_authorized": False,
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
            _record(runtime_dir / "app_server_judge_v5_calibration_v72_sharded_field_microtasks.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "selected_truth": _record(truth_path),
            "error_taxonomy": _record(taxonomy_path),
            "turns": turn_records,
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "direct-field-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v73 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV73Error("immutable v73 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
        "truth": selected_truth,
        "turns": turns,
        "capacity_policy": capacity["policy"],
    }


def _failure_accounting(root: Path) -> dict[str, Any]:
    attempts = _attempt_records(root)
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            usage = _validate_usage(_load_json(Path(record["path"]), "v73 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = bool(attempts) and unknown == 0
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
        "schema_version": V73_FAILURE_VERSION,
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
        "schema_version": V73_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "diagnostic_passed": False,
        "proposal_verification_authorized": False,
        "reference_patch_authorized": False,
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


async def run_v73(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v72_root: Path = DEFAULT_V72_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v73 terminal")
    frozen = freeze_v73(output_dir=root, v72_root=v72_root, timeout_seconds=timeout_seconds)
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
        output_path = root / "direct-field-output.private.json"
        _write_immutable(output_path, merged)
        score = score_v73(merged, frozen["truth"])
        score_path = root / "direct-field-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V73_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v73_direct_field_audit_passed_proposal_verification_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v73_direct_field_audit_passed_proposal_verification_authorized"
                if passed
                else "v73_direct_field_audit_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "diagnostic_passed": passed,
            "proposal_verification_authorized": passed,
            "reference_patch_authorized": False,
            "fresh_12_authorized": False,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(output_path),
            "attempts": _attempt_records(root),
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
    parser = argparse.ArgumentParser(description="Run v73 direct field audit")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v72-root", default=str(DEFAULT_V72_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v73(
            output_dir=Path(args.output_dir),
            v72_root=Path(args.v72_root),
            timeout_seconds=args.timeout_seconds,
        )
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
