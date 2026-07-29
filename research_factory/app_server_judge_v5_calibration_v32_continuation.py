from __future__ import annotations

"""Versioned v32 continuation after the v31 repair-output validation failure."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import (
    PROTOCOL_VERSION,
    adjudication_alignment_input,
    build_disagreement_adjudication_prompt,
    build_neutral_alignment_input,
    build_neutral_alignment_prompt,
    neutral_alignment_base_instructions,
    neutral_alignment_output_schema,
)
from .app_server_judge_v5_calibration import CALIBRATION_GATES
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    DEFAULT_PIPELINE_ROOT,
    MAX_TOKENS_PER_TURN,
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _build_scoreable_adjudication_input,
    _client_factory,
    _find_scoreable_disagreements,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _merge_outputs,
    _ratio,
    _reconcile_scoreable_alignment,
    _score_subset,
    _validate_scoreable_alignment_output,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v28_diagnostic import (
    ALIGNMENT_CASES_PER_SHARD,
    V28_CANARY_CASE_IDS,
    V28_CASE_IDS,
    _chunked,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _canonical_json,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso


V32_CONTINUATION_SPEC_VERSION = "pif_app_server_judge_v5_4_v32_repair_prompt_continuation_spec_v1"
V32_CONTINUATION_SCORE_VERSION = "pif_app_server_judge_v5_4_v32_repair_prompt_continuation_score_v1"
V32_CONTINUATION_TERMINAL_VERSION = (
    "pif_app_server_judge_v5_4_v32_repair_prompt_continuation_terminal_v1"
)
V32_CONTINUATION_FAILURE_VERSION = (
    "pif_app_server_judge_v5_4_v32_repair_prompt_continuation_failure_v1"
)
V32_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V32_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V32_PHASE_ID = "judge_v5_4_v32_repair_prompt_continuation"

DEFAULT_V31_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v31-repair-pass-diagnostic"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v32-repair-prompt-continuation"
).resolve()


class JudgeV5CalibrationV32ContinuationError(RuntimeError):
    """The v32 continuation cannot continue without violating its frozen contract."""


def _turn_names() -> list[str]:
    names = [
        f"neutral_alignment_repair_shard_{index:02d}"
        for index in range(math.ceil(len(V28_CASE_IDS) / ALIGNMENT_CASES_PER_SHARD))
    ]
    names.extend(
        f"neutral_alignment_canary_shard_{index:02d}"
        for index in range(math.ceil(len(V28_CANARY_CASE_IDS) / ALIGNMENT_CASES_PER_SHARD))
    )
    names.append("disagreement_adjudication")
    return names


def _validate_v31_failure(v31_root: Path) -> dict[str, Any]:
    terminal_path = v31_root / "terminal.json"
    failure_path = v31_root / "failure.json"
    terminal = _load_json(terminal_path, "v31 terminal")
    failure = _load_json(failure_path, "v31 failure")
    failed_turn = v31_root / "turns" / "neutral-alignment-repair-shard-00"
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != "neutral_alignment_repair_shard_00"
        or failure.get("usage_status") != "complete"
        or failure.get("error_class") != "JudgeV5CalibrationV26DiagnosticError"
    ):
        raise JudgeV5CalibrationV32ContinuationError("v31 predecessor is not the expected validator failure")
    required = {
        "pool": v31_root / "shared-witness-pool.private.json",
        "truth": v31_root / "diagnostic-truth.private.json",
        "pointwise_output": v31_root / "pointwise-output-full.private.json",
        "support_receipts": v31_root / "support-receipts.private.json",
        "base_alignment_initial": v31_root / "base-alignment-initial.private.json",
        "failed_repair_output": failed_turn / "output.private.json",
        "failed_repair_sidecar": failed_turn / "sidecar.json",
    }
    for path in required.values():
        if not path.is_file():
            raise JudgeV5CalibrationV32ContinuationError("v31 predecessor artifact is missing")
    failed_input = _load_json(failed_turn / "input.private.json", "v31 failed repair input")
    failed_output = _load_json(failed_turn / "output.private.json", "v31 failed repair output")
    errors = _validate_scoreable_alignment_output(failed_output, failed_input)
    if not errors or not all(
        error.endswith("relation_precedence_mismatch")
        or error.endswith("partial_without_boundary_evidence")
        for error in errors
    ):
        raise JudgeV5CalibrationV32ContinuationError("v31 failure is not the expected relation-precedence validator failure")
    return {
        "terminal": _record(terminal_path),
        "failure": _record(failure_path),
        "required_inputs": {label: _record(path) for label, path in required.items()},
        "validator_errors": errors,
    }


def _build_capacity_policy(*, root: Path, v31_root: Path) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    turn_names = _turn_names()
    predecessor = _validate_v31_failure(v31_root)
    audit = {
        "schema_version": V32_CAPACITY_AUDIT_VERSION,
        "phase_id": V32_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v31_predecessor": predecessor,
        "measured_basis": {
            "v31_known_usage_total_tokens": int(
                (_load_json(v31_root / "terminal.json", "v31 terminal").get("usage") or {}).get(
                    "total_tokens"
                )
                or 0
            ),
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": len(turn_names) * MAX_TOKENS_PER_TURN,
        },
        "precommitted_experiment": "full_rubric_relation_precedence_repair_continuation",
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v32 capacity audit")
        stable = deepcopy(audit)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV32ContinuationError("immutable v32 capacity audit drifted")
        audit = prior
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V32_CAPACITY_POLICY_VERSION,
        "phase_id": V32_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": turn_names,
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": len(turn_names) * MAX_TOKENS_PER_TURN,
        "projected_phase_quota_points": math.ceil(
            len(turn_names) * MAX_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    if policy_path.exists():
        prior = _load_json(policy_path, "v32 capacity policy")
        stable = deepcopy(policy)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV32ContinuationError("immutable v32 capacity policy drifted")
        policy = prior
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def _alignment_output_subset(
    output: Mapping[str, Any], case_ids: Sequence[str]
) -> dict[str, Any]:
    wanted = set(case_ids)
    rows = [deepcopy(row) for row in output["cases"] if row["case_id"] in wanted]
    if {row["case_id"] for row in rows} != wanted:
        raise JudgeV5CalibrationV32ContinuationError("alignment repair subset coverage drifted")
    return {"cases": rows}


def _alignment_repair_instructions_v32() -> str:
    return (
        neutral_alignment_base_instructions()
        + " V32 repair contract: relation must be the deterministic projection of the "
        "15-row checklist. If all rows are same, relation=equivalent. If the only "
        "different rows are exactly event_boundary and evidence, relation=partial. If "
        "any other row is different, relation=non_equivalent. If any row is abstain, "
        "relation=abstain. Do not return partial for a direct material field conflict."
    )


def _build_alignment_repair_prompt(
    *, alignment_input: Mapping[str, Any], original_output: Mapping[str, Any]
) -> str:
    base_prompt = build_neutral_alignment_prompt(alignment_input)
    repair = {
        "repair_goal": (
            "Verify the original alignment candidate and return a complete replacement. "
            "The candidate is not truth. Enforce relation precedence from the checklist."
        ),
        "relation_precedence": {
            "equivalent": "all checklist rows same",
            "partial": "exactly event_boundary and evidence different; no other different rows",
            "non_equivalent": "one or more direct material rows different other than merge/split-only",
            "abstain": "one or more checklist rows abstain",
        },
        "original_alignment": original_output,
    }
    return base_prompt + "\n# V32 original alignment candidate\n" + _canonical_json(repair) + "\n"


def _sum_usage(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, int]:
    return {
        field: int((left.get(field) or 0)) + int((right.get(field) or 0))
        for field in USAGE_FIELDS
    }


def freeze_v32_repair_prompt_continuation(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v31_root: Path = DEFAULT_V31_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v31_failure(v31_root)
    pool = _load_json(v31_root / "shared-witness-pool.private.json", "v31 pool")
    truth = _load_json(v31_root / "diagnostic-truth.private.json", "v31 truth")
    pointwise_output = _load_json(v31_root / "pointwise-output-full.private.json", "v31 pointwise")
    support_receipts = _load_json(v31_root / "support-receipts.private.json", "v31 support")
    base_alignment = _load_json(v31_root / "base-alignment-initial.private.json", "v31 base")
    for name, value in (
        ("shared-witness-pool.private.json", pool),
        ("diagnostic-truth.private.json", truth),
        ("pointwise-output-full.private.json", pointwise_output),
        ("support-receipts.private.json", support_receipts),
        ("base-alignment-initial.private.json", base_alignment),
    ):
        _write_immutable(root / name, value)
    capacity = _build_capacity_policy(root=root, v31_root=v31_root)
    spec = {
        "schema_version": V32_CONTINUATION_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "protocol_version": PROTOCOL_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "continuation_from_v31_completed_pointwise_and_base_alignment_with_full_rubric_repair",
        "case_count": len(V28_CASE_IDS),
        "canary_case_count": len(V28_CANARY_CASE_IDS),
        "case_ids": list(V28_CASE_IDS),
        "canary_case_ids": list(V28_CANARY_CASE_IDS),
        "turn_plan": _turn_names(),
        "retry_count_per_turn": 0,
        "predecessor_v31_replayed": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "v31_predecessor": predecessor,
        "frozen_inputs": {
            "pool": _record(root / "shared-witness-pool.private.json"),
            "truth": _record(root / "diagnostic-truth.private.json"),
            "pointwise_output": _record(root / "pointwise-output-full.private.json"),
            "support_receipts": _record(root / "support-receipts.private.json"),
            "base_alignment_initial": _record(root / "base-alignment-initial.private.json"),
        },
        "gates": CALIBRATION_GATES,
        "privacy": "private_inputs_prompts_outputs_no_sanitized_text_in_terminal",
    }
    spec_path = root / "repair-prompt-continuation-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v32 continuation spec")
        stable = deepcopy(spec)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV32ContinuationError("immutable v32 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "pool": pool,
        "truth": truth,
        "pointwise_output": pointwise_output,
        "support_receipts": support_receipts,
        "base_alignment": base_alignment,
        "capacity_policy": capacity["policy"],
    }


def _score_v32(
    *,
    pointwise_output: Mapping[str, Any],
    reconciled_alignment: Mapping[str, Any],
    expected: Mapping[str, Any],
    observable_disagreements: Mapping[str, Any],
) -> dict[str, Any]:
    score = _score_subset(
        pointwise_output=pointwise_output,
        reconciled_alignment=reconciled_alignment,
        expected=expected,
        observable_disagreements=observable_disagreements,
    )
    disagreement_count = int(observable_disagreements.get("disagreement_case_count") or 0)
    score["schema_version"] = V32_CONTINUATION_SCORE_VERSION
    score["metrics"]["order_bias"] = _ratio(disagreement_count, len(V28_CANARY_CASE_IDS))
    score["metrics"]["canary_case_count"] = len(V28_CANARY_CASE_IDS)
    score["checks"]["order_bias"] = (
        score["metrics"]["order_bias"] <= CALIBRATION_GATES["order_bias_max"]
    )
    score["checks"]["canary_case_count"] = len(V28_CANARY_CASE_IDS) == 6
    score["passed"] = all(score["checks"].values())
    score["failed_checks"] = sorted(key for key, passed in score["checks"].items() if not passed)
    return score


def _write_failure_terminal(
    *, root: Path, spec_path: Path, v31_root: Path, turn_name: Optional[str], error_class: str
) -> dict[str, Any]:
    attempts = _attempt_records(root)
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    partial = False
    for attempt in attempts:
        sidecar_record = attempt.get("sidecar")
        if not isinstance(sidecar_record, dict):
            if attempt.get("capacity") is not None or attempt.get("output") is not None:
                partial = True
            continue
        sidecar = _load_json(Path(sidecar_record["path"]), "v32 failed sidecar")
        try:
            usage = _validate_usage(sidecar)
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    v31_usage = _load_json(v31_root / "terminal.json", "v31 terminal").get("usage") or {}
    accounting_complete = not partial and unknown == 0
    failure = {
        "schema_version": V32_CONTINUATION_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "full_calibration_authorized": False,
        "accounting_complete": accounting_complete,
        "usage_status": "complete" if accounting_complete else "unknown",
        "usage": known if accounting_complete else None,
        "predecessor_v31_usage": v31_usage,
        "end_to_end_usage_including_v31": _sum_usage(v31_usage, known),
        "known_usage_lower_bound": known,
        "unknown_usage_turn_count": unknown,
        "partial_attempt_without_sidecar": partial,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V32_CONTINUATION_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "spec_sha256": _sha256_file(spec_path),
        "failure": _record(failure_path),
        "diagnostic_passed": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_allowed": False,
        "semantic_retry_count": 0,
        "accounting_complete": accounting_complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "predecessor_v31_usage": v31_usage,
        "end_to_end_usage_including_v31": failure["end_to_end_usage_including_v31"],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v32_repair_prompt_continuation(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v31_root: Path = DEFAULT_V31_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v32 terminal")
    frozen = freeze_v32_repair_prompt_continuation(
        output_dir=root,
        v31_root=v31_root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
    )
    policy_path = frozen["capacity_policy"]
    factory = client_factory or _client_factory
    sidecars: list[dict[str, Any]] = []
    adopted: dict[str, bool] = {}
    current_turn: Optional[str] = None
    try:
        async with factory(policy_path) as client:
            base_input = build_neutral_alignment_input(
                frozen["pool"], frozen["support_receipts"]
            )
            repaired_outputs = []
            for index, case_ids in enumerate(_chunked(V28_CASE_IDS, ALIGNMENT_CASES_PER_SHARD)):
                shard_input = build_neutral_alignment_input(
                    frozen["pool"], frozen["support_receipts"], case_ids=case_ids
                )
                original_subset = _alignment_output_subset(frozen["base_alignment"], case_ids)
                prompt = _build_alignment_repair_prompt(
                    alignment_input=shard_input, original_output=original_subset
                )
                schema = neutral_alignment_output_schema(shard_input)
                current_turn = f"neutral_alignment_repair_shard_{index:02d}"
                paths = _freeze_turn_request(
                    root=root,
                    turn_name=current_turn,
                    input_value=shard_input,
                    prompt=prompt,
                    schema=schema,
                )
                output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=_alignment_repair_instructions_v32(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard_input["cases"]),
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard_input: _validate_scoreable_alignment_output(
                        value, item
                    ),
                )
                repaired_outputs.append(output)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
            base_output = _merge_outputs(repaired_outputs, "cases")
            base_output_path = root / "base-alignment-full.private.json"
            _write_immutable(base_output_path, base_output)

            canary_input = build_neutral_alignment_input(
                frozen["pool"],
                frozen["support_receipts"],
                case_ids=V28_CANARY_CASE_IDS,
                permutation="balanced_canary",
            )
            canary_outputs = []
            for index, case_ids in enumerate(
                _chunked(V28_CANARY_CASE_IDS, ALIGNMENT_CASES_PER_SHARD)
            ):
                shard_input = build_neutral_alignment_input(
                    frozen["pool"],
                    frozen["support_receipts"],
                    case_ids=case_ids,
                    permutation="balanced_canary",
                )
                prompt = build_neutral_alignment_prompt(shard_input)
                schema = neutral_alignment_output_schema(shard_input)
                current_turn = f"neutral_alignment_canary_shard_{index:02d}"
                paths = _freeze_turn_request(
                    root=root,
                    turn_name=current_turn,
                    input_value=shard_input,
                    prompt=prompt,
                    schema=schema,
                )
                output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=neutral_alignment_base_instructions(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard_input["cases"]),
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard_input: _validate_scoreable_alignment_output(
                        value, item
                    ),
                )
                canary_outputs.append(output)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
            canary_output = _merge_outputs(canary_outputs, "cases")
            canary_output_path = root / "canary-alignment-full.private.json"
            _write_immutable(canary_output_path, canary_output)
            disagreements = _find_scoreable_disagreements(
                base_output=base_output,
                base_input=base_input,
                canary_output=canary_output,
                canary_input=canary_input,
            )
            disagreements_path = root / "observable-disagreements.private.json"
            _write_immutable(disagreements_path, disagreements)
            adjudication_output = None
            adjudication_input = None
            if disagreements["adjudication_required"]:
                packet = _build_scoreable_adjudication_input(
                    base_input=base_input,
                    base_output=base_output,
                    canary_input=canary_input,
                    canary_output=canary_output,
                )
                adjudication_input = adjudication_alignment_input(
                    base_input=base_input, adjudication_input=packet
                )
                prompt = build_disagreement_adjudication_prompt(
                    adjudication_input=packet,
                    adjudication_alignment=adjudication_input,
                )
                schema = neutral_alignment_output_schema(adjudication_input)
                current_turn = "disagreement_adjudication"
                paths = _freeze_turn_request(
                    root=root,
                    turn_name=current_turn,
                    input_value={
                        "adjudication_packet": packet,
                        "alignment_input": adjudication_input,
                    },
                    prompt=prompt,
                    schema=schema,
                )
                adjudication_output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=_alignment_repair_instructions_v32(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(adjudication_input["cases"]),
                    policy_path=policy_path,
                    output_validator=lambda value: _validate_scoreable_alignment_output(
                        value, adjudication_input
                    ),
                )
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
        reconciled = _reconcile_scoreable_alignment(
            base_output=base_output,
            base_input=base_input,
            adjudication_output=adjudication_output,
            adjudication_input=adjudication_input,
        )
        reconciled_path = root / "reconciled-alignment.private.json"
        _write_immutable(reconciled_path, reconciled)
        score = _score_v32(
            pointwise_output=frozen["pointwise_output"],
            reconciled_alignment=reconciled,
            expected=frozen["truth"],
            observable_disagreements=disagreements,
        )
        score_path = root / "diagnostic-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        predecessor_usage = _load_json(v31_root / "terminal.json", "v31 terminal").get("usage") or {}
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V32_CONTINUATION_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v32_repair_prompt_continuation_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v32_repair_prompt_continuation_passed_full_calibration_authorized"
                if passed
                else "v32_repair_prompt_continuation_quality_gate_not_passed"
            ),
            "babysitter_status": (
                "v32_repair_prompt_continuation_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "spec_sha256": _sha256_file(frozen["spec_path"]),
            "diagnostic_passed": passed,
            "full_calibration_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_allowed": False,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "observable_disagreements": _record(disagreements_path),
            "reconciled_alignment": _record(reconciled_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": adopted,
            "predecessor_v31_usage": predecessor_usage,
            "end_to_end_usage_including_v31": _sum_usage(predecessor_usage, accounting["usage"]),
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure_terminal(
            root=root,
            spec_path=frozen["spec_path"],
            v31_root=v31_root,
            turn_name=exc.turn_name,
            error_class=exc.error_class,
        )
    except Exception as exc:
        return _write_failure_terminal(
            root=root,
            spec_path=frozen["spec_path"],
            v31_root=v31_root,
            turn_name=current_turn,
            error_class=type(exc).__name__,
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v32 repair-prompt continuation")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v31-root", default=str(DEFAULT_V31_ROOT))
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v32_repair_prompt_continuation(
            output_dir=Path(args.output_dir),
            v31_root=Path(args.v31_root),
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal.get("state"),
                "terminal_reason": terminal.get("terminal_reason"),
                "diagnostic_passed": terminal.get("diagnostic_passed"),
                "full_calibration_authorized": terminal.get("full_calibration_authorized"),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
