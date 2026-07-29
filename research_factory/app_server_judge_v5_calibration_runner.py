from __future__ import annotations

"""Fail-closed sharded calibration and independent fixture audit for judge-v5.4."""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_capacity import CapacityGatedCodexAppServerClient
from .app_server_judge_v5 import (
    PROTOCOL_VERSION,
    adjudication_alignment_input,
    build_disagreement_adjudication_input,
    build_disagreement_adjudication_prompt,
    build_neutral_alignment_input,
    build_neutral_alignment_prompt,
    build_pointwise_support_input,
    build_pointwise_support_prompt,
    find_observable_alignment_disagreements,
    freeze_support_receipts,
    neutral_alignment_base_instructions,
    neutral_alignment_output_schema,
    pointwise_support_base_instructions,
    pointwise_support_output_schema,
    reconcile_neutral_alignment,
    validate_neutral_alignment_output,
    validate_pointwise_support_output,
)
from .app_server_judge_v5_calibration import (
    CALIBRATION_CASES_PER_SHARD,
    CALIBRATION_GATES,
    calibration_case_shards,
    make_v5_calibration_pool,
    pointwise_input_subset,
    score_v5_calibration,
)
from .app_server_judge_v5_diagnostic import (
    JudgeV5DiagnosticAttemptFailed,
    _aggregate_usage,
    _attempt_records,
    _freeze_turn_request,
    _get_or_run_turn,
    _record,
    _sha256_file,
    _validate_usage,
    _write_immutable_json,
)
from .util import now_iso


CALIBRATION_RUN_VERSION = "pif_app_server_judge_v5_4_calibration_run_v3"
CALIBRATION_TERMINAL_VERSION = "pif_app_server_judge_v5_4_calibration_terminal_v3"
CALIBRATION_FAILURE_VERSION = "pif_app_server_judge_v5_4_calibration_failure_v3"
EXECUTION_PURPOSES = ("calibration", "fixture_truth_audit")
DEFAULT_PIPELINE_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
DEFAULT_JUDGE_FREEZE_RECEIPT = DEFAULT_PIPELINE_ROOT / "judge-v5_4-freeze-receipt-v1.json"
DEFAULT_V1_AUDIT_RECEIPT = (
    DEFAULT_PIPELINE_ROOT / "calibration-v1-failure-audit-receipt-v1.json"
)
DEFAULT_FIXTURE_TRUTH_AUDIT_RECEIPT = (
    DEFAULT_PIPELINE_ROOT / "fixture-truth-audit-receipt-v1.json"
)
DEFAULT_OUTPUT_ROOT = DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v2"


class JudgeV5CalibrationRunnerError(RuntimeError):
    """The full calibration cannot continue without violating its frozen contract."""


def _load_json(path: Path, *, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JudgeV5CalibrationRunnerError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise JudgeV5CalibrationRunnerError(f"{purpose} is not an object")
    return value


def _verify_record(record: Mapping[str, Any]) -> Path:
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise JudgeV5CalibrationRunnerError("frozen judge record drifted")
    return path


def verify_judge_freeze_receipt(path: Path) -> dict[str, Any]:
    receipt = _load_json(path.expanduser().resolve(), purpose="judge-v5.4 freeze receipt")
    if (
        receipt.get("schema_version") != "pif_app_server_judge_v5_4_freeze_receipt_v1"
        or receipt.get("status") != "first_passing_judge_frozen"
        or receipt.get("protocol_version") != PROTOCOL_VERSION
        or receipt.get("diagnostic_passed") is not True
        or receipt.get("all_frozen_gates_passed") is not True
        or receipt.get("diagnostic_usage_total_tokens") != 122896
        or receipt.get("diagnostic_semantic_retry_count") != 0
        or receipt.get("prompt_protocol_changes_after_freeze_allowed") is not False
        or receipt.get("gate_threshold_changes_after_freeze_allowed") is not False
        or receipt.get("diagnostic_output_reuse_for_calibration_allowed") is not False
        or receipt.get("full_calibration_authorized") is not True
        or receipt.get("selection_authorized") is not False
    ):
        raise JudgeV5CalibrationRunnerError("judge-v5.4 freeze receipt is unsafe")
    records = receipt.get("records")
    if not isinstance(records, Mapping) or len(records) != 10:
        raise JudgeV5CalibrationRunnerError("judge-v5.4 freeze coverage drifted")
    for record in records.values():
        _verify_record(record)
    protocol_path = _verify_record(records["protocol"])
    if protocol_path != Path(__file__).resolve().with_name("app_server_judge_v5.py"):
        raise JudgeV5CalibrationRunnerError("frozen judge protocol path drifted")
    return receipt


def verify_calibration_v1_audit_receipt(path: Path) -> dict[str, Any]:
    receipt = _load_json(path.expanduser().resolve(), purpose="calibration-v1 audit receipt")
    if (
        receipt.get("schema_version")
        != "pif_app_server_judge_v5_4_calibration_v1_audit_receipt_v1"
        or receipt.get("corrected_classification")
        != "complete_semantic_output_must_be_scored"
        or receipt.get("hard_validator_rule_removed")
        != "unsupported_without_specific_root"
        or receipt.get("structural_schema_validation_retained") is not True
        or receipt.get("support_receipt_consistency_validation_retained") is not True
        or receipt.get("relation_projection_validation_retained") is not True
        or receipt.get("semantic_root_omission_remains_scoreable") is not True
        or receipt.get("frozen_judge_prompt_changed") is not False
        or receipt.get("frozen_rubric_changed") is not False
        or receipt.get("calibration_gates_changed") is not False
        or receipt.get("calibration_v1_replay_allowed") is not False
        or receipt.get("calibration_v1_output_reuse_allowed") is not False
        or receipt.get("calibration_v2_all_turns_must_run_fresh") is not True
        or receipt.get("selection_authorized") is not False
    ):
        raise JudgeV5CalibrationRunnerError("calibration-v1 audit receipt is unsafe")
    for key in ("terminal_receipt", "failed_input", "failed_output", "failed_sidecar"):
        _verify_record(receipt[key])
    return receipt


def verify_fixture_truth_audit_receipt(path: Path) -> dict[str, Any]:
    receipt = _load_json(path.expanduser().resolve(), purpose="fixture truth audit receipt")
    if (
        receipt.get("schema_version") != "pif_judge_fixture_truth_audit_receipt_v1"
        or receipt.get("status") != "verified_before_pipeline_v5_semantic_calls"
        or receipt.get("legacy_joint_support_labels_admissible") is not False
        or receipt.get("semantic_model_calls_performed") != 0
        or receipt.get("production_mutation_performed") is not False
        or receipt.get("material_field_reclassification_count") != 10
        or receipt.get("language_tutor_case_reclassification_count") != 6
        or receipt.get("mismatch_checklist_row_count") != 15
        or receipt.get("serialization_measurement")
        != {
            "prompt_count": 22,
            "before_prompt_bytes": 401660,
            "after_prompt_bytes": 241992,
            "reduction_fraction": 0.3975,
            "empty_values_only": True,
            "semantic_pruning_performed": False,
        }
    ):
        raise JudgeV5CalibrationRunnerError("fixture truth audit receipt is unsafe")
    for key in (
        "fixture_audit",
        "source_fixture",
        "pipeline_v4_calibration_report",
        "pipeline_v4_terminal_receipt",
        "pipeline_v4_terminal",
    ):
        record = receipt.get(key)
        if not isinstance(record, Mapping):
            raise JudgeV5CalibrationRunnerError("fixture truth audit coverage drifted")
        _verify_record(record)
    return receipt


def validate_scoreable_calibration_alignment_output(
    output: Any, alignment_input: Mapping[str, Any]
) -> list[str]:
    errors = validate_neutral_alignment_output(output, alignment_input)
    return [
        error
        for error in errors
        if not error.endswith("_unsupported_without_specific_root")
    ]


def _merge_outputs(outputs: Sequence[Mapping[str, Any]], key: str) -> dict[str, Any]:
    rows = [row for output in outputs for row in output[key]]
    ids = [
        (str(row["case_id"]), str(row.get("witness_id") or "")) for row in rows
    ]
    if len(ids) != len(set(ids)):
        raise JudgeV5CalibrationRunnerError("calibration shard outputs overlap")
    return {key: rows}


def freeze_v5_full_calibration(
    *,
    output_dir: Path,
    model: str,
    reasoning_effort: str,
    timeout_seconds: float,
    execution_purpose: str = "calibration",
    judge_freeze_receipt_path: Path = DEFAULT_JUDGE_FREEZE_RECEIPT,
    v1_audit_receipt_path: Path = DEFAULT_V1_AUDIT_RECEIPT,
    fixture_truth_audit_receipt_path: Path = DEFAULT_FIXTURE_TRUTH_AUDIT_RECEIPT,
) -> dict[str, Any]:
    if execution_purpose not in EXECUTION_PURPOSES:
        raise JudgeV5CalibrationRunnerError("unsupported calibration execution purpose")
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    freeze_path = judge_freeze_receipt_path.expanduser().resolve()
    audit_path = v1_audit_receipt_path.expanduser().resolve()
    fixture_audit_path = fixture_truth_audit_receipt_path.expanduser().resolve()
    verify_judge_freeze_receipt(freeze_path)
    verify_calibration_v1_audit_receipt(audit_path)
    verify_fixture_truth_audit_receipt(fixture_audit_path)
    pool, mapping, expected = make_v5_calibration_pool()
    shards = calibration_case_shards(pool)
    pointwise_input = build_pointwise_support_input(pool)
    pool_path = root / "shared-witness-pool.private.json"
    mapping_path = root / "witness-mapping.private.json"
    truth_path = root / (
        "provisional-calibration-truth.private.json"
        if execution_purpose == "fixture_truth_audit"
        else "calibration-truth.private.json"
    )
    pointwise_full_path = root / "pointwise-input-full.private.json"
    _write_immutable_json(pool_path, pool)
    _write_immutable_json(mapping_path, mapping)
    _write_immutable_json(truth_path, expected)
    _write_immutable_json(pointwise_full_path, pointwise_input)
    pointwise_shards = []
    for index, case_ids in enumerate(shards):
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
    spec = {
        "schema_version": CALIBRATION_RUN_VERSION,
        "state": (
            "frozen_before_fixture_truth_audit_model_calls"
            if execution_purpose == "fixture_truth_audit"
            else "frozen_before_calibration_model_calls"
        ),
        "created_at": now_iso(),
        "execution_purpose": execution_purpose,
        "protocol_version": PROTOCOL_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "case_count": 66,
        "witness_count": len(pointwise_input["units"]),
        "pointwise_shard_count": len(shards),
        "base_alignment_shard_count": len(shards),
        "cases_per_shard": CALIBRATION_CASES_PER_SHARD,
        "canary_case_count": 12,
        "minimum_turn_count": len(shards) * 2 + 1,
        "maximum_turn_count": len(shards) * 2 + 2,
        "retry_count_per_turn": 0,
        "all_turns_fresh": True,
        "diagnostic_output_reuse_allowed": False,
        "calibration_v1_output_reuse_allowed": False,
        "provisional_truth_exposed_to_model": False,
        "provisional_truth_can_authorize_reference_freeze": False,
        "fixture_truth_proposal_can_authorize_selection": False,
        "separate_observable_disagreement_adjudication_required": (
            execution_purpose == "fixture_truth_audit"
        ),
        "complete_semantic_outputs_are_scored": True,
        "scoreable_semantic_root_omission": "unsupported_without_specific_root",
        "support_is_side_free": True,
        "alignment_is_origin_neutral": True,
        "adjudication_call_cap": 1,
        "calibration_gates": CALIBRATION_GATES,
        "judge_freeze_receipt": _record(freeze_path),
        "calibration_v1_failure_audit_receipt": _record(audit_path),
        "fixture_truth_audit_receipt": _record(fixture_audit_path),
        "frozen_sources": {
            "calibration_protocol": _record(Path(__file__).resolve()),
            "judge_protocol": _record(
                Path(__file__).resolve().with_name("app_server_judge_v5.py")
            ),
            "calibration_fixture": _record(
                Path(__file__).resolve().with_name("app_server_judge_v5_calibration.py")
            ),
        },
        "frozen_inputs": {
            "pool": _record(pool_path),
            "mapping": _record(mapping_path),
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
        "production_mutation_allowed": False,
        "semantic_model_calls_performed_during_freeze": 0,
    }
    spec_path = root / (
        "fixture-audit-spec.json"
        if execution_purpose == "fixture_truth_audit"
        else "calibration-spec.json"
    )
    if spec_path.exists():
        prior = _load_json(spec_path, purpose="full calibration specification")
        stable = dict(spec)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationRunnerError("frozen calibration spec drifted")
        spec = prior
    else:
        _write_immutable_json(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "pool": pool,
        "mapping": mapping,
        "expected": expected,
        "shards": shards,
        "pointwise_input": pointwise_input,
        "pointwise_shards": pointwise_shards,
    }


def _failure_terminal(
    *,
    root: Path,
    spec_path: Path,
    turn_name: Optional[str],
    error_class: str,
    execution_purpose: str,
) -> dict[str, Any]:
    attempts = _attempt_records(root)
    usage_fields = (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )
    known = {field: 0 for field in usage_fields}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            continue
        sidecar = _load_json(Path(record["path"]), purpose="calibration attempt sidecar")
        try:
            usage = _validate_usage(sidecar)
        except Exception:
            unknown += 1
            continue
        for field in usage_fields:
            known[field] += usage[field]
    accounting_complete = unknown == 0
    failure = {
        "schema_version": CALIBRATION_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "execution_purpose": execution_purpose,
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "score_authorized": False,
        "selection_authorized": False,
        "accounting_complete": accounting_complete,
        "usage_status": "complete" if accounting_complete else "unknown",
        "usage": known if accounting_complete else None,
        "known_usage_lower_bound": known,
        "unknown_usage_turn_count": unknown,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable_json(failure_path, failure)
    terminal = {
        "schema_version": CALIBRATION_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "spec_sha256": _sha256_file(spec_path),
        "failure": _record(failure_path),
        "accounting_complete": accounting_complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "calibration_passed": False,
        "fixture_truth_proposal_completed": False,
        "selection_authorized": False,
        "production_mutated": False,
        "semantic_retry_allowed": False,
    }
    _write_immutable_json(root / "terminal.json", terminal)
    return terminal


async def run_v5_full_calibration(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    execution_purpose: str = "calibration",
    judge_freeze_receipt_path: Path = DEFAULT_JUDGE_FREEZE_RECEIPT,
    v1_audit_receipt_path: Path = DEFAULT_V1_AUDIT_RECEIPT,
    fixture_truth_audit_receipt_path: Path = DEFAULT_FIXTURE_TRUTH_AUDIT_RECEIPT,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, purpose="full calibration terminal")
    frozen = freeze_v5_full_calibration(
        output_dir=root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
        execution_purpose=execution_purpose,
        judge_freeze_receipt_path=judge_freeze_receipt_path,
        v1_audit_receipt_path=v1_audit_receipt_path,
        fixture_truth_audit_receipt_path=fixture_truth_audit_receipt_path,
    )
    sidecars: list[dict[str, Any]] = []
    adopted: dict[str, bool] = {}
    current_turn: Optional[str] = None
    try:
        async with client_factory() as client:
            pointwise_outputs = []
            for shard in frozen["pointwise_shards"]:
                current_turn = shard["turn_name"]
                output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=shard["paths"],
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                    base_instructions=pointwise_support_base_instructions(),
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["units"]),
                    output_validator=lambda value, item=shard: validate_pointwise_support_output(
                        value, item["input"]
                    ),
                )
                pointwise_outputs.append(output)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
            pointwise_output = _merge_outputs(pointwise_outputs, "units")
            errors = validate_pointwise_support_output(
                pointwise_output, frozen["pointwise_input"]
            )
            if errors:
                raise JudgeV5CalibrationRunnerError(
                    "aggregate pointwise validation failed: %s" % "; ".join(errors)
                )
            pointwise_output_path = root / "pointwise-output-full.private.json"
            _write_immutable_json(pointwise_output_path, pointwise_output)
            support_receipts = freeze_support_receipts(
                pointwise_output, frozen["pointwise_input"]
            )
            support_path = root / "support-receipts.private.json"
            _write_immutable_json(support_path, support_receipts)

            base_input = build_neutral_alignment_input(
                frozen["pool"], support_receipts
            )
            base_outputs = []
            for index, case_ids in enumerate(frozen["shards"]):
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
                    base_instructions=neutral_alignment_base_instructions(),
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard_input["cases"]),
                    output_validator=lambda value, item=shard_input: validate_scoreable_calibration_alignment_output(
                        value, item
                    ),
                )
                base_outputs.append(output)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
            base_output = _merge_outputs(base_outputs, "cases")
            errors = validate_scoreable_calibration_alignment_output(
                base_output, base_input
            )
            if errors:
                raise JudgeV5CalibrationRunnerError(
                    "aggregate alignment validation failed: %s" % "; ".join(errors)
                )
            _write_immutable_json(root / "base-alignment-full.private.json", base_output)

            canary_input = build_neutral_alignment_input(
                frozen["pool"],
                support_receipts,
                case_ids=frozen["expected"]["canary_case_ids"],
                permutation="balanced_canary",
            )
            canary_prompt = build_neutral_alignment_prompt(canary_input)
            canary_schema = neutral_alignment_output_schema(canary_input)
            current_turn = "neutral_alignment_canary"
            canary_paths = _freeze_turn_request(
                root=root,
                turn_name=current_turn,
                input_value=canary_input,
                prompt=canary_prompt,
                schema=canary_schema,
            )
            canary_output, canary_sidecar, was_adopted = await _get_or_run_turn(
                client=client,
                turn_name=current_turn,
                paths=canary_paths,
                prompt=canary_prompt,
                schema=canary_schema,
                base_instructions=neutral_alignment_base_instructions(),
                model=model,
                reasoning_effort=reasoning_effort,
                timeout_seconds=timeout_seconds,
                batch_size=len(canary_input["cases"]),
                output_validator=lambda value: validate_scoreable_calibration_alignment_output(
                    value, canary_input
                ),
            )
            sidecars.append(canary_sidecar)
            adopted[current_turn] = was_adopted
            disagreements = find_observable_alignment_disagreements(
                base_output=base_output,
                base_input=base_input,
                canary_output=canary_output,
                canary_input=canary_input,
                support_receipts=support_receipts,
            )
            disagreement_path = root / "observable-disagreements.private.json"
            _write_immutable_json(disagreement_path, disagreements)
            adjudication_output = None
            adjudication_input = None
            if disagreements["adjudication_required"]:
                packet = build_disagreement_adjudication_input(
                    base_input=base_input,
                    base_output=base_output,
                    canary_input=canary_input,
                    canary_output=canary_output,
                    support_receipts=support_receipts,
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
                    base_instructions=neutral_alignment_base_instructions(),
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(adjudication_input["cases"]),
                    output_validator=lambda value: validate_scoreable_calibration_alignment_output(
                        value, adjudication_input
                    ),
                )
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted

        reconciled = reconcile_neutral_alignment(
            base_output=base_output,
            base_input=base_input,
            canary_output=canary_output,
            canary_input=canary_input,
            support_receipts=support_receipts,
            adjudication_output=adjudication_output,
            adjudication_input=adjudication_input,
        )
        reconciled_path = root / "reconciled-alignment.private.json"
        _write_immutable_json(reconciled_path, reconciled)
        score = score_v5_calibration(
            pointwise_output=pointwise_output,
            reconciled_alignment=reconciled,
            expected=frozen["expected"],
            observable_disagreements=disagreements,
        )
        score_path = root / (
            "fixture-audit-comparison-vs-provisional.json"
            if execution_purpose == "fixture_truth_audit"
            else "calibration-score.json"
        )
        _write_immutable_json(score_path, score)
        accounting = _aggregate_usage(sidecars)
        audit_mode = execution_purpose == "fixture_truth_audit"
        terminal = {
            "schema_version": CALIBRATION_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "fixture_truth_proposal_completed_reference_adjudication_required"
                if audit_mode
                else "judge_full_calibration_passed_selection_authorized"
                if score["passed"]
                else "judge_full_calibration_quality_gate_not_passed"
            ),
            "execution_purpose": execution_purpose,
            "spec_sha256": _sha256_file(frozen["spec_path"]),
            "calibration_passed": False if audit_mode else score["passed"],
            "fixture_truth_proposal_completed": audit_mode,
            "provisional_reference_comparison_passed": score["passed"],
            "reference_freeze_authorized": False,
            "selection_authorized": False if audit_mode else score["passed"],
            "score": _record(score_path),
            "support_receipts": _record(root / "support-receipts.private.json"),
            "observable_disagreements": _record(
                root / "observable-disagreements.private.json"
            ),
            "reconciled_alignment": _record(reconciled_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": adopted,
            "semantic_retry_count": 0,
            "production_mutated": False,
            "semantic_retry_allowed": False,
            **accounting,
        }
        _write_immutable_json(terminal_path, terminal)
        return terminal
    except JudgeV5DiagnosticAttemptFailed as exc:
        return _failure_terminal(
            root=root,
            spec_path=frozen["spec_path"],
            turn_name=exc.turn_name,
            error_class=exc.error_class,
            execution_purpose=execution_purpose,
        )
    except Exception as exc:
        return _failure_terminal(
            root=root,
            spec_path=frozen["spec_path"],
            turn_name=current_turn,
            error_class=type(exc).__name__,
            execution_purpose=execution_purpose,
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run frozen judge-v5.4 calibration")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--judge-freeze-receipt", default=str(DEFAULT_JUDGE_FREEZE_RECEIPT))
    parser.add_argument("--v1-audit-receipt", default=str(DEFAULT_V1_AUDIT_RECEIPT))
    parser.add_argument(
        "--fixture-truth-audit-receipt",
        default=str(DEFAULT_FIXTURE_TRUTH_AUDIT_RECEIPT),
    )
    parser.add_argument("--execution-purpose", choices=EXECUTION_PURPOSES, default="calibration")
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v5_full_calibration(
            output_dir=Path(args.output_dir),
            judge_freeze_receipt_path=Path(args.judge_freeze_receipt),
            v1_audit_receipt_path=Path(args.v1_audit_receipt),
            fixture_truth_audit_receipt_path=Path(args.fixture_truth_audit_receipt),
            execution_purpose=args.execution_purpose,
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(json.dumps({
        "state": terminal["state"],
        "terminal_reason": terminal["terminal_reason"],
        "calibration_passed": terminal.get("calibration_passed", False),
        "selection_authorized": terminal.get("selection_authorized", False),
        "fixture_truth_proposal_completed": terminal.get(
            "fixture_truth_proposal_completed", False
        ),
    }, sort_keys=True))
    return 0 if terminal.get("state") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
