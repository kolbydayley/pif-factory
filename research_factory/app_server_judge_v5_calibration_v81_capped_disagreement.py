from __future__ import annotations

"""Single capped side-free adjudication for the v80 permutation disagreement."""

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
from .app_server_judge_v5_calibration_v80_alignment_reference_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V80_ROOT,
    base_instructions,
    build_prompt,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso, sha256_text


V81_INPUT_VERSION = "pif_app_server_judge_v5_4_v81_capped_disagreement_input_v1"
V81_TRUTH_VERSION = "pif_app_server_judge_v5_4_v81_capped_disagreement_truth_v1"
V81_SELECTION_VERSION = "pif_app_server_judge_v5_4_v81_selection_v1"
V81_SPEC_VERSION = "pif_app_server_judge_v5_4_v81_capped_disagreement_spec_v1"
V81_SCORE_VERSION = "pif_app_server_judge_v5_4_v81_capped_disagreement_score_v1"
V81_RECONCILIATION_VERSION = "pif_app_server_judge_v5_4_v81_reconciliation_v1"
V81_AUDIT_VERSION = "pif_app_server_judge_v5_4_v81_projection_audit_v1"
V81_FAILURE_VERSION = "pif_app_server_judge_v5_4_v81_failure_v1"
V81_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v81_terminal_v1"
V81_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V81_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V81_PHASE_ID = "judge_v5_4_v81_capped_side_free_disagreement"
MODEL = "gpt-5.4"
EFFORT = "high"
TURN_NAME = "capped_side_free_disagreement"
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V80_ROOT.parent / "judge-calibration-v5_4-v81-capped-disagreement"
).resolve()


class JudgeV5CalibrationV81Error(RuntimeError):
    """The v81 capped adjudication contract cannot be preserved."""


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


def _validate_v80(v80_root: Path) -> dict[str, Any]:
    paths = {
        "terminal": v80_root / "terminal.json",
        "spec": v80_root / "alignment-owner-spec.json",
        "input": v80_root / "alignment-owner-input.private.json",
        "truth": v80_root / "alignment-owner-truth.private.json",
        "output": v80_root / "alignment-owner-output.private.json",
        "canary": v80_root / "permutation-canary-output.private.json",
        "score": v80_root / "alignment-owner-score.json",
        "proposal": v80_root / "reference-patch-proposal.json",
        "projection": v80_root / "projection-audit.json",
    }
    values = {name: _load_json(path, f"v80 {name}") for name, path in paths.items()}
    terminal = values["terminal"]
    spec = values["spec"]
    score = values["score"]
    proposal = values["proposal"]
    metrics = score.get("metrics") or {}
    if (
        terminal.get("state") != "inactive"
        or terminal.get("development_terminal_reason")
        != "v80_alignment_reference_owner_quality_gate_not_passed"
        or terminal.get("reference_owner_passed") is not False
        or terminal.get("reference_freeze_authorized") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or len(terminal.get("attempts") or []) != 5
        or not all(
            _verify_record(record)
            for attempt in terminal.get("attempts") or []
            for record in (attempt.get("capacity"), attempt.get("sidecar"), attempt.get("output"))
        )
        or not _record_matches(terminal.get("owner_output"), paths["output"])
        or not _record_matches(terminal.get("canary_output"), paths["canary"])
        or not _record_matches(terminal.get("score"), paths["score"])
        or not _record_matches(terminal.get("reference_patch_proposal"), paths["proposal"])
        or not _record_matches(terminal.get("projection_audit"), paths["projection"])
        or metrics.get("matched_control_exact_count") != 6
        or metrics.get("permutation_canary_exact_count") != 3
        or metrics.get("permutation_canary_count") != 4
        or metrics.get("owner_abstention_count") != 0
        or metrics.get("evidence_complete_rate") != 1.0
        or score.get("failed_checks") != ["permutation_canary_exact_rate"]
        or proposal.get("state") != "suppressed"
        or proposal.get("rows") != []
        or spec.get("model") != "gpt-5.6-terra"
        or spec.get("majority_voting_used") is not False
        or not _record_matches(spec.get("frozen_inputs", {}).get("input"), paths["input"])
        or not _record_matches(spec.get("frozen_inputs", {}).get("truth"), paths["truth"])
        or not all(_verify_record(row) for row in spec.get("runtime_files") or [])
    ):
        raise JudgeV5CalibrationV81Error("v80 predecessor contract drifted")
    return {name: _record(path) for name, path in paths.items()}


def _task_id(source_id: str) -> str:
    return "capped_" + sha256_text(f"v81|{source_id}")[:24]


def build_v81_inputs(
    *,
    v80_input: Mapping[str, Any],
    v80_truth: Mapping[str, Any],
    v80_output: Mapping[str, Any],
    v80_canary: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    source = {row["task_id"]: row for row in v80_input.get("tasks") or []}
    truth = {row["task_id"]: row for row in v80_truth.get("tasks") or []}
    owner = {row["task_id"]: row for row in v80_output.get("decisions") or []}
    canary = {row["task_id"]: row for row in v80_canary.get("decisions") or []}
    if set(source) != set(truth) or set(source) != set(owner) or len(source) != 16:
        raise JudgeV5CalibrationV81Error("v80 source coverage drifted")
    unstable = []
    for row in v80_truth.get("canary_map") or []:
        if owner[row["owner_task_id"]]["field_status"] != canary[row["canary_task_id"]]["field_status"]:
            unstable.append(row["owner_task_id"])
    if len(unstable) != 1 or truth[unstable[0]]["field"] != "event_boundary":
        raise JudgeV5CalibrationV81Error("v80 observable disagreement drifted")
    disputed_id = unstable[0]
    controls = []
    for expected in ("correct", "incorrect"):
        candidates = sorted(
            (
                row["task_id"]
                for row in truth.values()
                if row["role"] == "matched_control" and row["control_expected_status"] == expected
            ),
            key=lambda task_id: sha256_text(f"v81|control|{task_id}"),
        )
        if len(candidates) < 2:
            raise JudgeV5CalibrationV81Error("v81 control pool is too small")
        controls.extend((task_id, expected) for task_id in candidates[:2])
    selected = [(disputed_id, "observable_disagreement", None)] + [
        (task_id, "matched_control", expected) for task_id, expected in controls
    ]
    model_tasks = []
    truth_rows = []
    for source_id, role, expected in selected:
        target_id = _task_id(source_id)
        task = deepcopy(source[source_id])
        task["task_id"] = target_id
        model_tasks.append(task)
        truth_rows.append(
            {
                "task_id": target_id,
                "source_task_id": source_id,
                "role": role,
                "field": truth[source_id]["field"],
                "control_expected_status": expected,
                "prior_status": truth[source_id]["prior_status"],
                "v80_owner_status": owner[source_id]["field_status"],
                "v80_canary_status": (
                    next(
                        canary[row["canary_task_id"]]["field_status"]
                        for row in v80_truth["canary_map"]
                        if row["owner_task_id"] == source_id
                    )
                    if role == "observable_disagreement"
                    else None
                ),
                "source_version": truth[source_id]["source_version"],
                "original_task_id": truth[source_id]["original_task_id"],
            }
        )
    model_tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    value = {
        "schema_version": V81_INPUT_VERSION,
        "task_count": 5,
        "tasks": model_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V81_TRUTH_VERSION,
        "task_count": 5,
        "tasks": truth_rows,
    }
    selection = {
        "schema_version": V81_SELECTION_VERSION,
        "created_at": now_iso(),
        "task_count": 5,
        "role_counts": {"observable_disagreement": 1, "matched_control": 4},
        "control_status_counts": {"correct": 2, "incorrect": 2},
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "capped_adjudication_turn_count": 1,
        "privacy": "aggregate_counts_only",
    }
    return value, truth_value, selection


def score_v81(output: Mapping[str, Any], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in output["decisions"]}
    if set(expected) != set(observed):
        raise JudgeV5CalibrationV81Error("v81 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "matched_control"]
    disputed = [row for row in expected.values() if row["role"] == "observable_disagreement"]
    if len(controls) != 4 or len(disputed) != 1:
        raise JudgeV5CalibrationV81Error("v81 role coverage drifted")
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    abstentions = sum(row["field_status"] == "abstain" for row in observed.values())
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    adjudicated_status = observed[disputed[0]["task_id"]]["field_status"]
    checks = {
        "matched_control_exact_rate": control_exact == 4,
        "adjudicated_status_nonabstain": adjudicated_status in {"correct", "incorrect"},
        "abstention_count": abstentions == 0,
        "evidence_complete_rate": evidence_complete == 5,
    }
    passed = all(checks.values())
    return {
        "schema_version": V81_SCORE_VERSION,
        "passed": passed,
        "metrics": {
            "task_count": 5,
            "matched_control_count": 4,
            "matched_control_exact_count": control_exact,
            "matched_control_exact_rate": round(control_exact / 4, 6),
            "abstention_count": abstentions,
            "evidence_complete_count": evidence_complete,
            "evidence_complete_rate": round(evidence_complete / 5, 6),
        },
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "adjudicated_status": adjudicated_status if passed else None,
        "reference_freeze_authorized": passed,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "majority_voting_used": False,
        "gates_frozen_before_semantic_calls": True,
    }


def build_reconciliation(
    *,
    v80_truth: Mapping[str, Any],
    v80_output: Mapping[str, Any],
    v81_truth: Mapping[str, Any],
    v81_score: Mapping[str, Any],
) -> dict[str, Any]:
    owner = {row["task_id"]: row for row in v80_output["decisions"]}
    disputed = next(row for row in v81_truth["tasks"] if row["role"] == "observable_disagreement")
    rows = []
    for row in v80_truth["tasks"]:
        if row["role"] == "matched_control":
            continue
        status = (
            v81_score["adjudicated_status"]
            if row["task_id"] == disputed["source_task_id"]
            else owner[row["task_id"]]["field_status"]
        )
        rows.append(
            {
                "source_version": row["source_version"],
                "original_task_id": row["original_task_id"],
                "field": row["field"],
                "prior_status": row["prior_status"],
                "final_status": status,
                "reference_change": status != row["prior_status"],
                "basis": (
                    "capped_side_free_adjudication"
                    if row["task_id"] == disputed["source_task_id"]
                    else "v80_stable_alignment_owner"
                ),
            }
        )
    if len(rows) != 10:
        raise JudgeV5CalibrationV81Error("v81 reconciliation row count drifted")
    return {
        "schema_version": V81_RECONCILIATION_VERSION,
        "created_at": now_iso(),
        "state": "authorized",
        "row_count": 10,
        "reference_change_count": sum(row["reference_change"] for row in rows),
        "rows": rows,
        "majority_voting_used": False,
        "reference_freeze_authorized": True,
        "fresh_diagnostic_required_after_freeze": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _write_stable_created(path: Path, value: Mapping[str, Any], purpose: str) -> dict[str, Any]:
    candidate = deepcopy(value)
    if path.exists():
        prior = _load_json(path, purpose)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV81Error(f"immutable {purpose} drifted")
        return prior
    _write_immutable(path, candidate)
    return candidate


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V81_CAPACITY_AUDIT_VERSION,
        "phase_id": V81_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        },
    }
    _write_stable_created(audit_path, audit, "v81 capacity audit")
    policy = {
        "schema_version": V81_CAPACITY_POLICY_VERSION,
        "phase_id": V81_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [TURN_NAME],
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        "projected_phase_quota_points": math.ceil(
            MAX_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_created(policy_path, policy, "v81 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v81(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v80_root: Path = DEFAULT_V80_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v80(v80_root.resolve())
    value, truth, selection = build_v81_inputs(
        v80_input=_load_json(v80_root / "alignment-owner-input.private.json", "v80 input"),
        v80_truth=_load_json(v80_root / "alignment-owner-truth.private.json", "v80 truth"),
        v80_output=_load_json(v80_root / "alignment-owner-output.private.json", "v80 output"),
        v80_canary=_load_json(v80_root / "permutation-canary-output.private.json", "v80 canary"),
    )
    input_path = root / "capped-adjudication-input.private.json"
    truth_path = root / "capped-adjudication-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_stable_created(selection_path, selection, "v81 selection audit")
    prompt = build_prompt(value)
    schema = output_schema(value)
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=value, prompt=prompt, schema=schema
    )
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V81_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_capped_side_free_adjudication_for_observable_permutation_disagreement",
        "task_count": 5,
        "observable_disagreement_count": 1,
        "matched_control_count": 4,
        "turn_plan": [TURN_NAME],
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
            _record(runtime_dir / "app_server_judge_v5_calibration_v80_alignment_reference_owner.py"),
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
            "turn_input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "capped-adjudication-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v81 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV81Error("immutable v81 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
        "truth": truth,
        "prompt": prompt,
        "schema": schema,
        "paths": paths,
        "capacity_policy": capacity["policy"],
    }


def _write_failure(root: Path, error_class: str) -> dict[str, Any]:
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v81 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V81_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
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
        "schema_version": V81_TERMINAL_VERSION,
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


async def run_v81(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v81 terminal")
    frozen = freeze_v81(output_dir=root, timeout_seconds=timeout_seconds)
    factory = client_factory or _client_factory
    try:
        async with factory(frozen["capacity_policy"]) as client:
            output, sidecar, adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=base_instructions(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=5,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: validate_output(
                    project_exact_spans(candidate, frozen["value"])[0], frozen["value"]
                ),
            )
        projected, operations = project_exact_spans(output, frozen["value"])
        if validate_output(projected, frozen["value"]):
            raise JudgeV5CalibrationV81Error("projected v81 output is invalid")
        output_path = root / "capped-adjudication-output.private.json"
        _write_immutable(output_path, projected)
        audit = {
            "schema_version": V81_AUDIT_VERSION,
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
        score = score_v81(projected, frozen["truth"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "capped-adjudication-score.json"
        _write_immutable(score_path, score)
        reconciliation_path = root / "reference-reconciliation.private.json"
        if score["passed"]:
            reconciliation = build_reconciliation(
                v80_truth=_load_json(DEFAULT_V80_ROOT / "alignment-owner-truth.private.json", "v80 truth"),
                v80_output=_load_json(DEFAULT_V80_ROOT / "alignment-owner-output.private.json", "v80 output"),
                v81_truth=frozen["truth"],
                v81_score=score,
            )
            _write_immutable(reconciliation_path, reconciliation)
        accounting = _aggregate_usage([sidecar])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V81_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v81_capped_adjudication_passed_reference_freeze_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v81_capped_adjudication_passed_reference_freeze_authorized"
                if passed
                else "v81_capped_adjudication_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "capped_adjudication_passed": passed,
            "reference_freeze_authorized": passed,
            "fresh_primary_repair_diagnostic_authorized": False,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(output_path),
            "projection_audit": _record(audit_path),
            "reconciliation": _record(reconciliation_path) if passed else None,
            "attempts": _attempt_records(root),
            "completed_checkpoint_adopted": adopted,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.error_class)
    except Exception as exc:
        return _write_failure(root, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v81 capped disagreement adjudication")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v81(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "capped_adjudication_passed": terminal.get("capped_adjudication_passed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
