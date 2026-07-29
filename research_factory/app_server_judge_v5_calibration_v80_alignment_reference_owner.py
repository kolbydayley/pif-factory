from __future__ import annotations

"""Alignment-first Terra reference owner after the v79 control truth challenge."""

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
from .app_server_judge_v5_calibration_v75_exact_span_remaining_shard import project_exact_spans
from .app_server_judge_v5_calibration_v76_neutral_contested_adjudication import (
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V78_ROOT,
)
from .app_server_judge_v5_calibration_v79_reference_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V79_ROOT,
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


V80_INPUT_VERSION = "pif_app_server_judge_v5_4_v80_alignment_owner_input_v1"
V80_TRUTH_VERSION = "pif_app_server_judge_v5_4_v80_alignment_owner_truth_v1"
V80_SELECTION_VERSION = "pif_app_server_judge_v5_4_v80_alignment_owner_selection_v1"
V80_SPEC_VERSION = "pif_app_server_judge_v5_4_v80_alignment_owner_spec_v1"
V80_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v80_alignment_owner_output_v1"
V80_PROPOSAL_VERSION = "pif_app_server_judge_v5_4_v80_reference_patch_proposal_v1"
V80_AUDIT_VERSION = "pif_app_server_judge_v5_4_v80_projection_audit_v1"
V80_SCORE_VERSION = "pif_app_server_judge_v5_4_v80_alignment_owner_score_v1"
V80_FAILURE_VERSION = "pif_app_server_judge_v5_4_v80_failure_v1"
V80_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v80_terminal_v1"
V80_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V80_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V80_PHASE_ID = "judge_v5_4_v80_alignment_first_reference_owner"
MODEL = "gpt-5.6-terra"
EFFORT = "high"
TASKS_PER_OWNER_SHARD = 4
OWNER_TURNS = tuple(f"alignment_reference_owner_shard_{index:02d}" for index in range(4))
CANARY_TURN = "alignment_reference_owner_permutation_canary"
TURN_NAMES = OWNER_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V79_ROOT.parent / "judge-calibration-v5_4-v80-alignment-reference-owner"
).resolve()


class JudgeV5CalibrationV80Error(RuntimeError):
    """The v80 alignment-first reference-owner contract cannot be preserved."""


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


def _validate_predecessors(v79_root: Path, v78_root: Path) -> dict[str, Any]:
    paths = {
        "v79_terminal": v79_root / "terminal.json",
        "v79_spec": v79_root / "reference-owner-spec.json",
        "v79_input": v79_root / "reference-owner-input.private.json",
        "v79_truth": v79_root / "reference-owner-truth.private.json",
        "v79_output": v79_root / "reference-owner-output.private.json",
        "v79_canary": v79_root / "permutation-canary-output.private.json",
        "v79_score": v79_root / "reference-owner-score.json",
        "v79_proposal": v79_root / "reference-patch-proposal.json",
        "v79_projection": v79_root / "projection-audit.json",
        "v78_input": v78_root / "fresh-input.private.json",
        "v78_truth": v78_root / "fresh-truth.private.json",
        "v78_primary": v78_root / "primary-output.private.json",
        "v78_adjudicator": v78_root / "adjudicator-output.private.json",
        "v78_reconciled": v78_root / "reconciled-output.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v79_terminal"]
    spec = values["v79_spec"]
    score = values["v79_score"]
    proposal = values["v79_proposal"]
    metrics = score.get("metrics") or {}
    if (
        terminal.get("state") != "inactive"
        or terminal.get("development_terminal_reason")
        != "v79_reference_owner_quality_gate_not_passed"
        or terminal.get("reference_owner_passed") is not False
        or terminal.get("reference_freeze_authorized") is not False
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
        or not _record_matches(terminal.get("owner_output"), paths["v79_output"])
        or not _record_matches(terminal.get("canary_output"), paths["v79_canary"])
        or not _record_matches(terminal.get("score"), paths["v79_score"])
        or not _record_matches(terminal.get("reference_patch_proposal"), paths["v79_proposal"])
        or not _record_matches(terminal.get("projection_audit"), paths["v79_projection"])
        or metrics.get("matched_control_count") != 6
        or metrics.get("matched_control_exact_count") != 5
        or metrics.get("permutation_canary_exact_count") != 3
        or metrics.get("owner_abstention_count") != 0
        or metrics.get("evidence_complete_rate") != 1.0
        or score.get("failed_checks") != ["matched_control_exact_rate"]
        or proposal.get("state") != "suppressed"
        or proposal.get("rows") != []
        or spec.get("model") != "gpt-5.5"
        or spec.get("majority_voting_used") is not False
        or not _record_matches(spec.get("frozen_inputs", {}).get("input"), paths["v79_input"])
        or not _record_matches(spec.get("frozen_inputs", {}).get("truth"), paths["v79_truth"])
        or not all(_verify_record(row) for row in spec.get("runtime_files") or [])
    ):
        raise JudgeV5CalibrationV80Error("v79 predecessor contract drifted")
    return {name: _record(path) for name, path in paths.items()}


def _terra_task_id(original_owner_task_id: str) -> str:
    return "terra_" + sha256_text(f"v80|{original_owner_task_id}")[:24]


def _canary_task_id(terra_task_id: str) -> str:
    return "perm_" + sha256_text(f"v80|canary|{terra_task_id}")[:24]


def build_v80_inputs(
    *,
    v79_input: Mapping[str, Any],
    v79_truth: Mapping[str, Any],
    v79_output: Mapping[str, Any],
    v78_input: Mapping[str, Any],
    v78_truth: Mapping[str, Any],
    v78_primary: Mapping[str, Any],
    v78_adjudicator: Mapping[str, Any],
    v78_reconciled: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    source79 = {row["task_id"]: row for row in v79_input.get("tasks") or []}
    truth79 = {row["task_id"]: row for row in v79_truth.get("tasks") or []}
    output79 = {row["task_id"]: row for row in v79_output.get("decisions") or []}
    if set(source79) != set(truth79) or set(source79) != set(output79) or len(source79) != 15:
        raise JudgeV5CalibrationV80Error("v79 source coverage drifted")
    failed_controls = [
        row
        for row in truth79.values()
        if row["role"] == "matched_control"
        and output79[row["task_id"]]["field_status"] != row["control_expected_status"]
    ]
    if (
        len(failed_controls) != 1
        or failed_controls[0]["field"] != "stance"
        or failed_controls[0]["control_expected_status"] != "incorrect"
        or output79[failed_controls[0]["task_id"]]["field_status"] != "correct"
    ):
        raise JudgeV5CalibrationV80Error("v79 truth challenge drifted")
    challenge_id = failed_controls[0]["task_id"]

    selected = []
    for task_id, row in truth79.items():
        if row["role"] != "matched_control" or task_id == challenge_id:
            selected.append(
                {
                    "source_task_id": task_id,
                    "role": "truth_challenge" if task_id == challenge_id else row["role"],
                    "source_version": row["source_version"],
                    "original_task_id": row["original_task_id"],
                    "field": row["field"],
                    "prior_status": row["prior_status"],
                    "v79_owner_status": output79[task_id]["field_status"],
                    "control_expected_status": None,
                    "task": source79[task_id],
                }
            )
    if len(selected) != 10:
        raise JudgeV5CalibrationV80Error("v80 audited reference row count drifted")

    source78 = {row["task_id"]: row for row in v78_input.get("tasks") or []}
    truth78 = {row["task_id"]: row for row in v78_truth.get("tasks") or []}
    primary78 = {row["task_id"]: row for row in v78_primary.get("decisions") or []}
    adjudicator78 = {row["task_id"]: row for row in v78_adjudicator.get("decisions") or []}
    reconciled78 = {row["task_id"]: row for row in v78_reconciled.get("decisions") or []}
    if not source78 or set(source78) != set(truth78) or set(source78) != set(primary78):
        raise JudgeV5CalibrationV80Error("v78 source coverage drifted")
    original_selected = {row["original_task_id"] for row in selected}
    control_pool = []
    for task_id, truth_row in truth78.items():
        expected = truth_row["expected_status"]
        if (
            task_id not in original_selected
            and reconciled78[task_id]["reconciled_status"] == expected
            and primary78[task_id]["field_status"] == expected
            and adjudicator78[task_id]["field_status"] == expected
            and task_id != failed_controls[0]["original_task_id"]
        ):
            control_pool.append((task_id, expected))
    controls = []
    for expected in ("correct", "incorrect"):
        candidates = sorted(
            (task_id for task_id, status in control_pool if status == expected),
            key=lambda task_id: sha256_text(f"v80|control|{task_id}"),
        )
        if len(candidates) < 3:
            raise JudgeV5CalibrationV80Error("v80 replacement control pool is too small")
        for task_id in candidates[:3]:
            controls.append(
                {
                    "source_task_id": task_id,
                    "role": "matched_control",
                    "source_version": "v78",
                    "original_task_id": task_id,
                    "field": source78[task_id]["field"],
                    "prior_status": expected,
                    "v79_owner_status": None,
                    "control_expected_status": expected,
                    "task": source78[task_id],
                }
            )
    selected.extend(controls)
    if len(selected) != 16 or len({row["original_task_id"] for row in selected}) != 16:
        raise JudgeV5CalibrationV80Error("v80 selection is not 16 distinct tasks")

    model_tasks = []
    truth_rows = []
    for row in selected:
        terra_task_id = _terra_task_id(row["source_task_id"])
        source_task = row["task"]
        model_tasks.append(
            {
                "task_id": terra_task_id,
                "field": source_task["field"],
                "field_contract": deepcopy(source_task["field_contract"]),
                "source_excerpt": source_task["source_excerpt"],
                "structured_event": deepcopy(source_task["structured_event"]),
            }
        )
        truth_rows.append(
            {
                "task_id": terra_task_id,
                "source_version": row["source_version"],
                "original_task_id": row["original_task_id"],
                "field": row["field"],
                "role": row["role"],
                "prior_status": row["prior_status"],
                "v79_owner_status": row["v79_owner_status"],
                "control_expected_status": row["control_expected_status"],
            }
        )
    model_tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    task_by_id = {row["task_id"]: row for row in model_tasks}
    audit_ids = [row["task_id"] for row in truth_rows if row["role"] != "matched_control"]
    canary_owner_ids = sorted(audit_ids, key=lambda task_id: sha256_text(f"v80|perm|{task_id}"))[:4]
    canary_tasks = []
    canary_map = []
    for terra_task_id in reversed(canary_owner_ids):
        task = deepcopy(task_by_id[terra_task_id])
        canary_task_id = _canary_task_id(terra_task_id)
        task["task_id"] = canary_task_id
        canary_tasks.append(task)
        canary_map.append({"canary_task_id": canary_task_id, "owner_task_id": terra_task_id})
    value = {
        "schema_version": V80_INPUT_VERSION,
        "task_count": 16,
        "tasks": model_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V80_TRUTH_VERSION,
        "task_count": 16,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V80_INPUT_VERSION,
        "task_count": 4,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V80_SELECTION_VERSION,
        "created_at": now_iso(),
        "task_count": 16,
        "role_counts": {
            "matched_control": 6,
            "proposed_change": 5,
            "unresolved": 4,
            "truth_challenge": 1,
        },
        "control_status_counts": {"correct": 3, "incorrect": 3},
        "permutation_canary_count": 4,
        "selection_uses_source_text": False,
        "truth_challenge_trigger": "single_v79_control_disagreement_with_full_evidence_and_stable_canary",
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "privacy": "aggregate_counts_only",
    }
    return value, truth_value, canary, selection


def base_instructions() -> str:
    return (
        "You are the designated neutral reference owner for isolated source-to-field tasks. First align the "
        "structured event to the exact source proposition expressed by its evidence, claim, and target. Judge "
        "only the requested field for that aligned proposition. Never transfer actor, speaker, attribution, "
        "stance, metric, or other semantics from an adjacent proposition about a different target or event. "
        "Mentally correct every other field. incorrect means the requested field materially conflicts with "
        "the aligned source proposition or omits a materially required value; correct means it is source-correct "
        "or not applicable. The first evidence span must be an exact substring expressing the aligned proposition; "
        "a second exact span may show a conflict. Abstain only when the supplied source cannot determine the field. "
        "Do not vote, use confidence, regex, keywords, overlap, embeddings, or infer prior labels or system identity."
    )


def build_prompt(value: Mapping[str, Any]) -> str:
    return (
        "Return one alignment-first field decision for every opaque task_id. Do not compare tasks or emit a "
        "whole-event verdict. Every source_evidence_span must be an exact substring of that task's source_excerpt.\n\n"
        + _canonical_json(value)
        + "\n"
    )


def _owner_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value.get("tasks") or []
    if len(tasks) != 16:
        raise JudgeV5CalibrationV80Error("v80 owner task count drifted")
    return [
        {
            **{key: deepcopy(child) for key, child in value.items() if key != "tasks"},
            "task_count": TASKS_PER_OWNER_SHARD,
            "tasks": deepcopy(tasks[index : index + TASKS_PER_OWNER_SHARD]),
            "shard_ordinal": index // TASKS_PER_OWNER_SHARD,
            "shard_count": 4,
        }
        for index in range(0, 16, TASKS_PER_OWNER_SHARD)
    ]


def merge_outputs(outputs: Sequence[Mapping[str, Any]], expected_count: int) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(decisions) != expected_count or len({row["task_id"] for row in decisions}) != expected_count:
        raise JudgeV5CalibrationV80Error("v80 output coverage drifted")
    return {"schema_version": V80_OUTPUT_VERSION, "decisions": decisions}


def score_v80(
    owner_output: Mapping[str, Any],
    canary_output: Mapping[str, Any],
    truth: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in owner_output["decisions"]}
    canary = {row["task_id"]: row for row in canary_output["decisions"]}
    if set(expected) != set(observed) or len(canary) != 4:
        raise JudgeV5CalibrationV80Error("v80 score coverage drifted")
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
        "permutation_canary_exact_rate": canary_exact == 4,
        "owner_abstention_count": owner_abstentions == 0,
        "canary_abstention_count": canary_abstentions == 0,
        "evidence_complete_rate": evidence_complete == 20,
    }
    passed = all(checks.values())
    proposal = [
        {
            "source_version": row["source_version"],
            "original_task_id": row["original_task_id"],
            "field": row["field"],
            "role": row["role"],
            "prior_status": row["prior_status"],
            "owner_status": observed[row["task_id"]]["field_status"],
            "reference_change": observed[row["task_id"]]["field_status"] != row["prior_status"],
        }
        for row in expected.values()
        if row["role"] != "matched_control"
    ]
    return {
        "schema_version": V80_SCORE_VERSION,
        "passed": passed,
        "metrics": {
            "task_count": 16,
            "audited_reference_row_count": 10,
            "matched_control_count": 6,
            "matched_control_exact_count": control_exact,
            "matched_control_exact_rate": round(control_exact / 6, 6),
            "permutation_canary_count": 4,
            "permutation_canary_exact_count": canary_exact,
            "permutation_canary_exact_rate": round(canary_exact / 4, 6),
            "owner_abstention_count": owner_abstentions,
            "canary_abstention_count": canary_abstentions,
            "evidence_complete_count": evidence_complete,
            "evidence_complete_rate": round(evidence_complete / 20, 6),
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
            raise JudgeV5CalibrationV80Error(f"immutable {purpose} drifted")
        return prior
    _write_immutable(path, candidate)
    return candidate


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    phase_bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V80_CAPACITY_AUDIT_VERSION,
        "phase_id": V80_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": phase_bound,
        },
    }
    _write_stable_created(audit_path, audit, "v80 capacity audit")
    policy = {
        "schema_version": V80_CAPACITY_POLICY_VERSION,
        "phase_id": V80_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v80 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v80(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v79_root: Path = DEFAULT_V79_ROOT,
    v78_root: Path = DEFAULT_V78_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors(v79_root.resolve(), v78_root.resolve())
    value, truth, canary, selection = build_v80_inputs(
        v79_input=_load_json(v79_root / "reference-owner-input.private.json", "v79 input"),
        v79_truth=_load_json(v79_root / "reference-owner-truth.private.json", "v79 truth"),
        v79_output=_load_json(v79_root / "reference-owner-output.private.json", "v79 output"),
        v78_input=_load_json(v78_root / "fresh-input.private.json", "v78 input"),
        v78_truth=_load_json(v78_root / "fresh-truth.private.json", "v78 truth"),
        v78_primary=_load_json(v78_root / "primary-output.private.json", "v78 primary"),
        v78_adjudicator=_load_json(v78_root / "adjudicator-output.private.json", "v78 adjudicator"),
        v78_reconciled=_load_json(v78_root / "reconciled-output.private.json", "v78 reconciled"),
    )
    input_path = root / "alignment-owner-input.private.json"
    truth_path = root / "alignment-owner-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_created(selection_path, selection, "v80 selection audit")
    turn_values = _owner_shards(value) + [canary]
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
        "schema_version": V80_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "alignment_first_reference_owner_with_truth_challenge_balanced_controls_and_canary",
        "task_count": 16,
        "audited_reference_row_count": 10,
        "matched_control_count": 6,
        "permutation_canary_count": 4,
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
            _record(runtime_dir / "app_server_judge_v5_calibration_v79_reference_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
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
    spec_path = root / "alignment-owner-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v80 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV80Error("immutable v80 spec drifted")
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v80 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V80_FAILURE_VERSION,
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
        "schema_version": V80_TERMINAL_VERSION,
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


async def run_v80(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v80 terminal")
    frozen = freeze_v80(output_dir=root, timeout_seconds=timeout_seconds)
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
                    raise JudgeV5CalibrationV80Error("projected v80 output is invalid")
                if current_turn == CANARY_TURN:
                    canary_outputs.append(projected)
                else:
                    owner_outputs.append(projected)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
                operations.extend([{**row, "turn_name": current_turn} for row in turn_operations])
        owner = merge_outputs(owner_outputs, 16)
        canary = merge_outputs(canary_outputs, 4)
        owner_path = root / "alignment-owner-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(owner_path, owner)
        _write_immutable(canary_path, canary)
        audit = {
            "schema_version": V80_AUDIT_VERSION,
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
        score = score_v80(owner, canary, frozen["truth"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "alignment-owner-score.json"
        _write_immutable(score_path, score)
        proposal = {
            "schema_version": V80_PROPOSAL_VERSION,
            "created_at": now_iso(),
            "state": "authorized" if score["passed"] else "suppressed",
            "reference_owner_model": MODEL,
            "audited_reference_row_count": 10,
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
            "schema_version": V80_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v80_alignment_reference_owner_passed_reference_freeze_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v80_alignment_reference_owner_passed_reference_freeze_authorized"
                if passed
                else "v80_alignment_reference_owner_quality_gate_not_passed"
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
    parser = argparse.ArgumentParser(description="Run v80 alignment-first reference owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v80(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
