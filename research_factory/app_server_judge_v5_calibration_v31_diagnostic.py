from __future__ import annotations

"""Versioned v31 repair-pass diagnostic after the v30 quality failure."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_capacity_reserve import RESERVE_CAPACITY_CHECKPOINT_VERSION
from .app_server_judge_v5 import (
    ADJUDICATION_INPUT_VERSION,
    PROTOCOL_VERSION,
    adjudication_alignment_input,
    build_disagreement_adjudication_prompt,
    build_neutral_alignment_input,
    build_neutral_alignment_prompt,
    build_pointwise_support_input,
    build_pointwise_support_prompt,
    freeze_support_receipts,
    neutral_alignment_base_instructions,
    neutral_alignment_output_schema,
    pointwise_support_base_instructions,
    pointwise_support_output_schema,
    validate_pointwise_support_output,
)
from .app_server_judge_v5_calibration import CALIBRATION_GATES, pointwise_input_subset
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    DEFAULT_PIPELINE_ROOT,
    DEFAULT_V23_ROOT,
    MAX_PROMPT_BYTES,
    MAX_SCHEMA_BYTES,
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
    _normalize_scoreable_alignment_output,
    _ratio,
    _reconcile_scoreable_alignment,
    _score_subset,
    _subset_pool,
    _subset_truth,
    _validate_scoreable_alignment_output,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v28_diagnostic import (
    ALIGNMENT_CASES_PER_SHARD,
    POINTWISE_CASES_PER_SHARD,
    V28_CANARY_CASE_IDS,
    V28_CASE_IDS,
    _alignment_instructions_v28,
    _chunked,
    _pointwise_instructions_v28,
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


V31_DIAGNOSTIC_SPEC_VERSION = "pif_app_server_judge_v5_4_v31_repair_pass_diagnostic_spec_v1"
V31_DIAGNOSTIC_SCORE_VERSION = "pif_app_server_judge_v5_4_v31_repair_pass_diagnostic_score_v1"
V31_DIAGNOSTIC_TERMINAL_VERSION = (
    "pif_app_server_judge_v5_4_v31_repair_pass_diagnostic_terminal_v1"
)
V31_DIAGNOSTIC_FAILURE_VERSION = (
    "pif_app_server_judge_v5_4_v31_repair_pass_diagnostic_failure_v1"
)
V31_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V31_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V31_PHASE_ID = "judge_v5_4_v31_repair_pass_diagnostic"

DEFAULT_V30_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v30-protocol-retry-after-preturn-failure"
).resolve()
DEFAULT_V31_DESIGN_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v31-repair-pass-design"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v31-repair-pass-diagnostic"
).resolve()


class JudgeV5CalibrationV31DiagnosticError(RuntimeError):
    """The v31 diagnostic cannot continue without violating its frozen contract."""


def _turn_names() -> list[str]:
    names = [
        f"pointwise_support_shard_{index:02d}"
        for index in range(math.ceil(len(V28_CASE_IDS) / POINTWISE_CASES_PER_SHARD))
    ]
    names.extend(
        f"pointwise_repair_shard_{index:02d}"
        for index in range(math.ceil(len(V28_CASE_IDS) / POINTWISE_CASES_PER_SHARD))
    )
    names.extend(
        f"neutral_alignment_base_shard_{index:02d}"
        for index in range(math.ceil(len(V28_CASE_IDS) / ALIGNMENT_CASES_PER_SHARD))
    )
    names.extend(
        f"neutral_alignment_repair_shard_{index:02d}"
        for index in range(math.ceil(len(V28_CASE_IDS) / ALIGNMENT_CASES_PER_SHARD))
    )
    names.extend(
        f"neutral_alignment_canary_shard_{index:02d}"
        for index in range(math.ceil(len(V28_CANARY_CASE_IDS) / ALIGNMENT_CASES_PER_SHARD))
    )
    names.append("disagreement_adjudication")
    return names


def _pointwise_initial_instructions_v31() -> str:
    return (
        _pointwise_instructions_v28()
        + " V31 guard: favor abstain over inventing support, but do not turn "
        "source-supported paraphrase, role alias, or resolved coreference into an "
        "unsupported proposition. Preserve the proposition verdict and structured-field "
        "verdict as independent decisions."
    )


def _pointwise_repair_instructions_v31() -> str:
    return (
        pointwise_support_base_instructions()
        + " V31 repair pass: you are verifying a prior pointwise receipt for the same "
        "opaque unit. Return a complete replacement receipt. Keep the prior receipt only "
        "when it is fully entailed by the source and uses the minimal field_issue_fields. "
        "Patch false unsupported/support labels, harmless coreference or role-alias "
        "mistakes, and overbroad field issues. Do not use confidence, voting, regex, "
        "keyword matching, token overlap, or embeddings."
    )


def _alignment_initial_instructions_v31() -> str:
    return _alignment_instructions_v28()


def _alignment_repair_instructions_v31() -> str:
    return (
        neutral_alignment_base_instructions()
        + " V31 residual repair pass: verify a prior neutral alignment for the same "
        "opaque cases. Return a complete replacement alignment. Leave legitimate residual "
        "events unpaired, repair forced equivalent pairs, repair missing non-equivalent "
        "field diagnostics, and preserve partial only for true merge/split boundary "
        "differences with evidence. Use the frozen pointwise receipts as constraints, "
        "but make no side or system inference."
    )


def _validate_v31_predecessors(
    *, design_root: Path, v30_root: Path
) -> dict[str, Any]:
    design_terminal_path = design_root / "terminal.json"
    design_path = design_root / "repair-pass-design.json"
    v30_terminal_path = v30_root / "terminal.json"
    v30_score_path = v30_root / "diagnostic-score.json"
    design_terminal = _load_json(design_terminal_path, "v31 design terminal")
    design = _load_json(design_path, "v31 repair-pass design")
    v30_terminal = _load_json(v30_terminal_path, "v30 terminal")
    if (
        design_terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or design_terminal.get("semantic_attempt_started") is not False
        or design_terminal.get("production_mutated") is not False
        or design_terminal.get("next_required_artifact")
        != "versioned_v31_repair_pass_implementation_with_tests_then_single_managed_app_server_diagnostic"
        or design.get("state") != "frozen_presemantic_design"
        or design.get("semantic_attempt_authorized_by_this_artifact") is not False
        or design.get("production_mutated") is not False
    ):
        raise JudgeV5CalibrationV31DiagnosticError("v31 design predecessor is not admissible")
    design_record = design_terminal.get("design")
    if (
        not isinstance(design_record, Mapping)
        or design_record.get("sha256") != _sha256_file(design_path)
        or design_record.get("size_bytes") != design_path.stat().st_size
    ):
        raise JudgeV5CalibrationV31DiagnosticError("v31 design terminal does not bind design")
    if (
        v30_terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or v30_terminal.get("development_terminal_reason")
        != "v28_field_guarded_diagnostic_quality_gate_not_passed"
        or v30_terminal.get("diagnostic_passed") is not False
        or v30_terminal.get("accounting_complete") is not True
        or v30_terminal.get("usage_status") != "complete"
        or v30_terminal.get("production_mutated") is not False
        or v30_terminal.get("selection_authorized") is not False
        or v30_terminal.get("holdout_authorized") is not False
    ):
        raise JudgeV5CalibrationV31DiagnosticError("v30 predecessor is not the expected quality failure")
    return {
        "v31_design_terminal": _record(design_terminal_path),
        "v31_design": _record(design_path),
        "v30_terminal": _record(v30_terminal_path),
        "v30_score": _record(v30_score_path),
    }


def _build_capacity_policy(
    *, root: Path, design_root: Path, v30_root: Path
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    turn_names = _turn_names()
    predecessors = _validate_v31_predecessors(design_root=design_root, v30_root=v30_root)
    v30_terminal = _load_json(v30_root / "terminal.json", "v30 terminal")
    audit = {
        "schema_version": V31_CAPACITY_AUDIT_VERSION,
        "phase_id": V31_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessors": predecessors,
        "measured_basis": {
            "v30_total_tokens": int((v30_terminal.get("usage") or {}).get("total_tokens") or 0),
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": len(turn_names) * MAX_TOKENS_PER_TURN,
        },
        "precommitted_experiment": "pointwise_repair_then_residual_alignment_repair",
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v31 capacity audit")
        stable = deepcopy(audit)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV31DiagnosticError("immutable v31 capacity audit drifted")
        audit = prior
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V31_CAPACITY_POLICY_VERSION,
        "phase_id": V31_PHASE_ID,
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
        prior = _load_json(policy_path, "v31 capacity policy")
        stable = deepcopy(policy)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV31DiagnosticError("immutable v31 capacity policy drifted")
        policy = prior
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def _pointwise_output_subset(
    output: Mapping[str, Any], case_ids: Sequence[str]
) -> dict[str, Any]:
    wanted = set(case_ids)
    rows = [deepcopy(row) for row in output["units"] if row["case_id"] in wanted]
    if {row["case_id"] for row in rows} != wanted:
        raise JudgeV5CalibrationV31DiagnosticError("pointwise repair subset coverage drifted")
    return {"units": rows}


def _alignment_output_subset(
    output: Mapping[str, Any], case_ids: Sequence[str]
) -> dict[str, Any]:
    wanted = set(case_ids)
    rows = [deepcopy(row) for row in output["cases"] if row["case_id"] in wanted]
    if {row["case_id"] for row in rows} != wanted:
        raise JudgeV5CalibrationV31DiagnosticError("alignment repair subset coverage drifted")
    return {"cases": rows}


def _build_pointwise_repair_prompt(
    *, pointwise_input: Mapping[str, Any], original_output: Mapping[str, Any]
) -> str:
    original_by_id = {row["witness_id"]: row for row in original_output["units"]}
    repair_units = []
    for unit in pointwise_input["units"]:
        receipt = original_by_id.get(unit["witness_id"])
        if receipt is None:
            raise JudgeV5CalibrationV31DiagnosticError("pointwise repair receipt coverage drifted")
        repair_units.append({**deepcopy(unit), "original_receipt": deepcopy(receipt)})
    instructions = (
        "Return every case_id/witness_id exactly once as a complete replacement receipt. "
        "The original_receipt is not truth; it is a candidate to verify. Correct only when "
        "the source excerpt entails the change. For harmless paraphrase, role alias, title "
        "variation, or resolved coreference, prefer supported/correct when all material "
        "claims are licensed. Keep field_issue_fields minimal and independently justified."
    )
    packet = {"units": repair_units}
    return instructions + "\n\n# V31 pointwise repair units\n" + _canonical_json(packet) + "\n"


def _build_alignment_repair_prompt(
    *, alignment_input: Mapping[str, Any], original_output: Mapping[str, Any]
) -> str:
    instructions = (
        "Return every case exactly once as a complete replacement alignment. "
        "original_alignment is not truth; it is a candidate to verify. Keep it only if the "
        "equivalence partition, pairs, unpaired witness IDs, relation, and every checklist "
        "row follow the frozen rubric. Leave true residual events unpaired. Do not infer "
        "origin or use side identity."
    )
    packet = {
        "rubric_and_cases": {
            "checklist_field_order": alignment_input["checklist_field_order"],
            "checklist_decision_order": alignment_input["checklist_decision_order"],
            "cases": alignment_input["cases"],
        },
        "original_alignment": original_output,
    }
    return instructions + "\n\n# V31 residual alignment repair packet\n" + _canonical_json(packet) + "\n"


def freeze_v31_repair_pass_diagnostic(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v23_root: Path = DEFAULT_V23_ROOT,
    v30_root: Path = DEFAULT_V30_ROOT,
    design_root: Path = DEFAULT_V31_DESIGN_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessors = _validate_v31_predecessors(design_root=design_root, v30_root=v30_root)
    pool = _subset_pool(
        _load_json(v23_root / "shared-witness-pool.private.json", "v23 witness pool"),
        V28_CASE_IDS,
    )
    truth = _subset_truth(
        _load_json(v23_root / "calibration-truth.private.json", "v23 truth"),
        V28_CASE_IDS,
    )
    pool_path = root / "shared-witness-pool.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    _write_immutable(pool_path, pool)
    _write_immutable(truth_path, truth)
    pointwise_input = build_pointwise_support_input(pool)
    pointwise_full_path = root / "pointwise-input-full.private.json"
    _write_immutable(pointwise_full_path, pointwise_input)
    pointwise_shards = []
    for index, case_ids in enumerate(_chunked(V28_CASE_IDS, POINTWISE_CASES_PER_SHARD)):
        shard_input = pointwise_input_subset(pointwise_input, case_ids)
        prompt = build_pointwise_support_prompt(shard_input)
        schema = pointwise_support_output_schema(shard_input)
        turn_name = f"pointwise_support_shard_{index:02d}"
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=shard_input,
            prompt=prompt,
            schema=schema,
        )
        pointwise_shards.append(
            {
                "index": index,
                "case_ids": case_ids,
                "input": shard_input,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
                "turn_name": turn_name,
            }
        )
    capacity = _build_capacity_policy(root=root, design_root=design_root, v30_root=v30_root)
    spec = {
        "schema_version": V31_DIAGNOSTIC_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "protocol_version": PROTOCOL_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "pointwise_repair_then_residual_alignment_repair",
        "case_count": len(V28_CASE_IDS),
        "canary_case_count": len(V28_CANARY_CASE_IDS),
        "witness_count": len(pointwise_input["units"]),
        "case_ids": list(V28_CASE_IDS),
        "canary_case_ids": list(V28_CANARY_CASE_IDS),
        "turn_plan": _turn_names(),
        "retry_count_per_turn": 0,
        "semantic_model_calls_performed_during_freeze": 0,
        "full_calibration_authorized_before_diagnostic_pass": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessors": predecessors,
        "frozen_inputs": {
            "pool": _record(pool_path),
            "truth": _record(truth_path),
            "pointwise_full": _record(pointwise_full_path),
            "pointwise_shards": [
                {
                    "index": shard["index"],
                    "case_ids": shard["case_ids"],
                    "input": _record(shard["paths"]["input"]),
                    "prompt": _record(shard["paths"]["prompt"]),
                    "schema": _record(shard["paths"]["schema"]),
                }
                for shard in pointwise_shards
            ],
        },
        "gates": CALIBRATION_GATES,
        "request_byte_caps": {"prompt": MAX_PROMPT_BYTES, "output_schema": MAX_SCHEMA_BYTES},
        "privacy": "private_inputs_prompts_outputs_no_sanitized_text_in_terminal",
    }
    spec_path = root / "repair-pass-diagnostic-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v31 repair-pass diagnostic spec")
        stable = deepcopy(spec)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV31DiagnosticError("immutable v31 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "pool": pool,
        "truth": truth,
        "pointwise_input": pointwise_input,
        "pointwise_shards": pointwise_shards,
        "capacity_policy": capacity["policy"],
    }


def _score_v31(
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
    score["schema_version"] = V31_DIAGNOSTIC_SCORE_VERSION
    score["metrics"]["order_bias"] = _ratio(disagreement_count, len(V28_CANARY_CASE_IDS))
    score["metrics"]["canary_case_count"] = len(V28_CANARY_CASE_IDS)
    score["checks"]["order_bias"] = (
        score["metrics"]["order_bias"] <= CALIBRATION_GATES["order_bias_max"]
    )
    score["checks"]["canary_case_count"] = len(V28_CANARY_CASE_IDS) == 6
    score["passed"] = all(score["checks"].values())
    score["failed_checks"] = sorted(key for key, passed in score["checks"].items() if not passed)
    score["v31_canary_case_ids"] = list(V28_CANARY_CASE_IDS)
    return score


def _write_failure_terminal(
    *, root: Path, spec_path: Path, turn_name: Optional[str], error_class: str
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
        sidecar = _load_json(Path(sidecar_record["path"]), "v31 failed sidecar")
        try:
            usage = _validate_usage(sidecar)
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    accounting_complete = not partial and unknown == 0
    failure = {
        "schema_version": V31_DIAGNOSTIC_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "full_calibration_authorized": False,
        "accounting_complete": accounting_complete,
        "usage_status": "complete" if accounting_complete else "unknown",
        "usage": known if accounting_complete else None,
        "known_usage_lower_bound": known,
        "unknown_usage_turn_count": unknown,
        "partial_attempt_without_sidecar": partial,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V31_DIAGNOSTIC_TERMINAL_VERSION,
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
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v31_repair_pass_diagnostic(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v23_root: Path = DEFAULT_V23_ROOT,
    v30_root: Path = DEFAULT_V30_ROOT,
    design_root: Path = DEFAULT_V31_DESIGN_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v31 terminal")
    frozen = freeze_v31_repair_pass_diagnostic(
        output_dir=root,
        v23_root=v23_root,
        v30_root=v30_root,
        design_root=design_root,
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
            pointwise_initial_outputs = []
            for shard in frozen["pointwise_shards"]:
                current_turn = shard["turn_name"]
                output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=shard["paths"],
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                    base_instructions=_pointwise_initial_instructions_v31(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["units"]),
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard: validate_pointwise_support_output(
                        value, item["input"]
                    ),
                )
                pointwise_initial_outputs.append(output)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
            pointwise_initial = _merge_outputs(pointwise_initial_outputs, "units")
            pointwise_initial_path = root / "pointwise-output-initial.private.json"
            _write_immutable(pointwise_initial_path, pointwise_initial)

            pointwise_repair_outputs = []
            for shard in frozen["pointwise_shards"]:
                index = int(shard["index"])
                current_turn = f"pointwise_repair_shard_{index:02d}"
                original_subset = _pointwise_output_subset(pointwise_initial, shard["case_ids"])
                prompt = _build_pointwise_repair_prompt(
                    pointwise_input=shard["input"], original_output=original_subset
                )
                schema = pointwise_support_output_schema(shard["input"])
                paths = _freeze_turn_request(
                    root=root,
                    turn_name=current_turn,
                    input_value=shard["input"],
                    prompt=prompt,
                    schema=schema,
                )
                output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=_pointwise_repair_instructions_v31(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["units"]),
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard: validate_pointwise_support_output(
                        value, item["input"]
                    ),
                )
                pointwise_repair_outputs.append(output)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
            pointwise_output = _merge_outputs(pointwise_repair_outputs, "units")
            pointwise_output_path = root / "pointwise-output-full.private.json"
            _write_immutable(pointwise_output_path, pointwise_output)
            support_receipts = freeze_support_receipts(pointwise_output, frozen["pointwise_input"])
            support_path = root / "support-receipts.private.json"
            _write_immutable(support_path, support_receipts)

            base_input = build_neutral_alignment_input(frozen["pool"], support_receipts)
            base_outputs = []
            base_shards: list[dict[str, Any]] = []
            for index, case_ids in enumerate(_chunked(V28_CASE_IDS, ALIGNMENT_CASES_PER_SHARD)):
                shard_input = build_neutral_alignment_input(
                    frozen["pool"], support_receipts, case_ids=case_ids
                )
                prompt = build_neutral_alignment_prompt(shard_input)
                schema = neutral_alignment_output_schema(shard_input)
                current_turn = f"neutral_alignment_base_shard_{index:02d}"
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
                    base_instructions=_alignment_initial_instructions_v31(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard_input["cases"]),
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard_input: _validate_scoreable_alignment_output(
                        value, item
                    ),
                )
                base_outputs.append(output)
                base_shards.append({"index": index, "case_ids": case_ids, "input": shard_input})
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
            base_output = _merge_outputs(base_outputs, "cases")
            base_output_path = root / "base-alignment-initial.private.json"
            _write_immutable(base_output_path, base_output)

            repaired_alignment_outputs = []
            for shard in base_shards:
                index = int(shard["index"])
                current_turn = f"neutral_alignment_repair_shard_{index:02d}"
                original_subset = _alignment_output_subset(base_output, shard["case_ids"])
                prompt = _build_alignment_repair_prompt(
                    alignment_input=shard["input"], original_output=original_subset
                )
                schema = neutral_alignment_output_schema(shard["input"])
                paths = _freeze_turn_request(
                    root=root,
                    turn_name=current_turn,
                    input_value=shard["input"],
                    prompt=prompt,
                    schema=schema,
                )
                output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=_alignment_repair_instructions_v31(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["cases"]),
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard["input"]: _validate_scoreable_alignment_output(
                        value, item
                    ),
                )
                repaired_alignment_outputs.append(output)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
            base_output = _merge_outputs(repaired_alignment_outputs, "cases")
            base_output_path = root / "base-alignment-full.private.json"
            _write_immutable(base_output_path, base_output)

            canary_input = build_neutral_alignment_input(
                frozen["pool"],
                support_receipts,
                case_ids=V28_CANARY_CASE_IDS,
                permutation="balanced_canary",
            )
            canary_outputs = []
            for index, case_ids in enumerate(
                _chunked(V28_CANARY_CASE_IDS, ALIGNMENT_CASES_PER_SHARD)
            ):
                shard_input = build_neutral_alignment_input(
                    frozen["pool"],
                    support_receipts,
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
                    base_instructions=_alignment_initial_instructions_v31(),
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
                    base_instructions=_alignment_repair_instructions_v31(),
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
        score = _score_v31(
            pointwise_output=pointwise_output,
            reconciled_alignment=reconciled,
            expected=frozen["truth"],
            observable_disagreements=disagreements,
        )
        score_path = root / "diagnostic-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V31_DIAGNOSTIC_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v31_repair_pass_diagnostic_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v31_repair_pass_diagnostic_passed_full_calibration_authorized"
                if passed
                else "v31_repair_pass_diagnostic_quality_gate_not_passed"
            ),
            "babysitter_status": (
                "v31_repair_pass_diagnostic_passed_full_calibration_authorized"
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
            "support_receipts": _record(support_path),
            "observable_disagreements": _record(disagreements_path),
            "reconciled_alignment": _record(reconciled_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": adopted,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure_terminal(
            root=root,
            spec_path=frozen["spec_path"],
            turn_name=exc.turn_name,
            error_class=exc.error_class,
        )
    except Exception as exc:
        return _write_failure_terminal(
            root=root,
            spec_path=frozen["spec_path"],
            turn_name=current_turn,
            error_class=type(exc).__name__,
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v31 repair-pass judge diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v23-root", default=str(DEFAULT_V23_ROOT))
    parser.add_argument("--v30-root", default=str(DEFAULT_V30_ROOT))
    parser.add_argument("--design-root", default=str(DEFAULT_V31_DESIGN_ROOT))
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v31_repair_pass_diagnostic(
            output_dir=Path(args.output_dir),
            v23_root=Path(args.v23_root),
            v30_root=Path(args.v30_root),
            design_root=Path(args.design_root),
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
