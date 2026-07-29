from __future__ import annotations

"""Versioned v28 field-guarded diagnostic after the v27 timeout."""

import argparse
import asyncio
import json
import math
from collections import Counter
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
    DEFAULT_V26_PRESEMANTIC_DESIGN_ROOT,
    PINNED_CODEX_0_144_1,
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
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso


V28_DIAGNOSTIC_SPEC_VERSION = "pif_app_server_judge_v5_4_v28_field_guarded_diagnostic_spec_v1"
V28_DIAGNOSTIC_TERMINAL_VERSION = (
    "pif_app_server_judge_v5_4_v28_field_guarded_diagnostic_terminal_v1"
)
V28_DIAGNOSTIC_FAILURE_VERSION = (
    "pif_app_server_judge_v5_4_v28_field_guarded_diagnostic_failure_v1"
)
V28_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V28_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V28_ERROR_TAXONOMY_VERSION = "pif_app_server_judge_v5_4_v28_error_taxonomy_v1"

DEFAULT_V27_ROOT = (
    DEFAULT_PIPELINE_ROOT
    / "judge-calibration-v5_4-v27-staged-diagnostic-validator-repair"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v28-field-guarded-diagnostic"
).resolve()
DEFAULT_V28_ROOT = DEFAULT_OUTPUT_ROOT
DEFAULT_V29_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v29-capacity-contract-repair"
).resolve()

V28_CASE_IDS = [
    "jcase_4d5dff9aae0f62a79e8e1d50",
    "jcase_ddf03786960ff7583b95eb84",
    "jcase_1153895f992c3ca36fecd004",
    "jcase_27edbb38a94ae1eb60da4295",
    "jcase_486d2d05929d68d8a8a34262",
    "jcase_616ca00172aad1a9ac7836db",
    "jcase_b498a98ede5a7e127f358a5e",
    "jcase_1124b8b6032c8ed35dada2f0",
    "jcase_43f1212ec1d286cde0eaf89d",
    "jcase_a2a565c42526b84767bf4b1d",
    "jcase_f5aac851a010eb910fac98c5",
    "jcase_35112ff09a798d991065fe76",
]
V28_CANARY_CASE_IDS = [
    "jcase_ddf03786960ff7583b95eb84",
    "jcase_4d5dff9aae0f62a79e8e1d50",
    "jcase_1124b8b6032c8ed35dada2f0",
    "jcase_43f1212ec1d286cde0eaf89d",
    "jcase_a2a565c42526b84767bf4b1d",
    "jcase_27edbb38a94ae1eb60da4295",
]
POINTWISE_CASES_PER_SHARD = 4
ALIGNMENT_CASES_PER_SHARD = 3


class JudgeV5CalibrationV28DiagnosticError(RuntimeError):
    """The v28 diagnostic cannot continue without violating its frozen contract."""


def _chunked(values: Sequence[str], size: int) -> list[list[str]]:
    return [list(values[index : index + size]) for index in range(0, len(values), size)]


def _turn_names() -> list[str]:
    names = [
        f"pointwise_support_shard_{index:02d}"
        for index in range(math.ceil(len(V28_CASE_IDS) / POINTWISE_CASES_PER_SHARD))
    ]
    names.extend(
        f"neutral_alignment_base_shard_{index:02d}"
        for index in range(math.ceil(len(V28_CASE_IDS) / ALIGNMENT_CASES_PER_SHARD))
    )
    names.extend(
        f"neutral_alignment_canary_shard_{index:02d}"
        for index in range(math.ceil(len(V28_CANARY_CASE_IDS) / ALIGNMENT_CASES_PER_SHARD))
    )
    names.append("disagreement_adjudication")
    return names


def _pointwise_instructions_v28() -> str:
    return (
        pointwise_support_base_instructions()
        + " V28 guard: keep proposition/source support and structured-event field "
        "correctness separate. Mark structured_field_verdict=incorrect only for a "
        "material field conflict entailed by the source or for a material witness claim "
        "not entailed by the source. Do not mark actor, speaker, attribution, stance, "
        "target, or event_type different for harmless paraphrase, role aliases, pronoun "
        "coreference, title variation, or because a source-supported residual is merely "
        "unpaired. Unsupported_inference requires a material unentailed claim, not just "
        "different wording."
    )


def _alignment_instructions_v28() -> str:
    return (
        neutral_alignment_base_instructions()
        + " V28 guard: unpaired is a valid outcome. Do not force a witness into a pair "
        "when it describes a separate residual event or has no one-to-one counterpart. "
        "Use frozen support receipts as constraints when judging unsupported_inference, "
        "but still evaluate equivalence from the source and witness semantics rather than "
        "from exact text or side position. When support receipts say a witness has field "
        "issues, compare the specific issue fields; do not convert every field issue into "
        "event_boundary or attribution differences. Prefer the minimal root field set."
    )


def _validate_v27_predecessor(v27_root: Path) -> dict[str, Any]:
    terminal_path = v27_root / "terminal.json"
    failure_path = v27_root / "failure.json"
    terminal = _load_json(terminal_path, "v27 predecessor terminal")
    failure = _load_json(failure_path, "v27 predecessor failure")
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("failed_turn_name") != "neutral_alignment_canary_shard_02"
        or failure.get("usage_status") != "unknown"
    ):
        raise JudgeV5CalibrationV28DiagnosticError("v27 predecessor is not the expected fail-closed timeout")
    return {"terminal": _record(terminal_path), "failure": _record(failure_path)}


def _late_predecessor_records(output_root: Path) -> dict[str, Any]:
    records: dict[str, Any] = {}
    for label, predecessor_root in (
        ("v28_field_guarded_preprobe_failure", DEFAULT_V28_ROOT),
        ("v29_capacity_contract_repair_failure", DEFAULT_V29_ROOT),
    ):
        resolved = predecessor_root.expanduser().resolve()
        if resolved == output_root.expanduser().resolve():
            continue
        terminal_path = resolved / "terminal.json"
        if not terminal_path.exists():
            continue
        item: dict[str, Any] = {"terminal": _record(terminal_path)}
        failure_path = resolved / "failure.json"
        if failure_path.exists():
            item["failure"] = _record(failure_path)
        records[label] = item
    return records


def _build_v27_error_taxonomy(v27_root: Path) -> dict[str, Any]:
    truth = _load_json(v27_root / "diagnostic-truth.private.json", "v27 truth")
    pointwise = _load_json(v27_root / "pointwise-output-full.private.json", "v27 pointwise output")
    base = _load_json(v27_root / "base-alignment-full.private.json", "v27 base alignment")
    base_cases = []
    for index in range(3):
        base_input = _load_json(
            v27_root / "turns" / f"neutral-alignment-base-shard-{index:02d}" / "input.private.json",
            "v27 base input shard",
        )
        base_cases.extend(base_input["cases"])
    merged_base_input = {key: value for key, value in base_input.items() if key != "cases"} | {
        "cases": base_cases
    }
    base_only = _score_subset(
        pointwise_output=pointwise,
        reconciled_alignment=_normalize_scoreable_alignment_output(base, merged_base_input),
        expected=truth,
        observable_disagreements={"disagreement_case_count": 0},
    )
    pointwise_rows = {row["witness_id"]: row for row in pointwise["units"]}
    support_confusion: Counter[str] = Counter()
    structured_confusion: Counter[str] = Counter()
    pointwise_field_issues: Counter[str] = Counter()
    support_miss_cases: Counter[str] = Counter()
    structured_miss_cases: Counter[str] = Counter()
    for case_id, case_truth in truth["cases"].items():
        for witness_id, expected_support in case_truth["proposition"].items():
            observed = pointwise_rows[witness_id]["proposition_verdict"]
            support_confusion[f"{expected_support}->{observed}"] += 1
            if observed != expected_support:
                support_miss_cases[case_id] += 1
            expected_structured = case_truth["structured_fields"][witness_id]
            observed_structured = pointwise_rows[witness_id]["structured_field_verdict"]
            structured_confusion[f"{expected_structured}->{observed_structured}"] += 1
            if observed_structured != expected_structured:
                structured_miss_cases[case_id] += 1
            wanted = set(case_truth["field_issues"][witness_id])
            got = set(pointwise_rows[witness_id]["field_issue_fields"])
            for field in wanted - got:
                pointwise_field_issues[f"fn:{field}"] += 1
            for field in got - wanted:
                pointwise_field_issues[f"fp:{field}"] += 1
    return {
        "schema_version": V28_ERROR_TAXONOMY_VERSION,
        "created_at": now_iso(),
        "source": {
            "v27_terminal": _record(v27_root / "terminal.json"),
            "v27_failure": _record(v27_root / "failure.json"),
            "v27_pointwise": _record(v27_root / "pointwise-output-full.private.json"),
            "v27_base_alignment": _record(v27_root / "base-alignment-full.private.json"),
        },
        "privacy": "sanitized_opaque_ids_and_counts_only",
        "failure_class": "v27_timeout_plus_base_quality_gap",
        "v27_timeout": {
            "failed_turn_name": "neutral_alignment_canary_shard_02",
            "usage_status": "unknown",
            "timeout_seconds": 1200,
        },
        "base_only_metrics": base_only["metrics"],
        "base_only_failed_checks": base_only["failed_checks"],
        "support_confusion": dict(sorted(support_confusion.items())),
        "structured_confusion": dict(sorted(structured_confusion.items())),
        "pointwise_field_issue_error_counts": dict(sorted(pointwise_field_issues.items())),
        "support_miss_case_counts": dict(sorted(support_miss_cases.items())),
        "structured_miss_case_counts": dict(sorted(structured_miss_cases.items())),
        "v28_interventions": [
            "respect_six_case_origin_neutral_canary",
            "reduce_alignment_shards_to_three_cases",
            "add_pointwise_harmless_paraphrase_and_coreference_guard",
            "add_alignment_unpaired_residual_and_minimal_root_field_guard",
        ],
    }


def _build_capacity_policy(root: Path, v27_root: Path) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    turn_names = _turn_names()
    audit = {
        "schema_version": V28_CAPACITY_AUDIT_VERSION,
        "phase_id": "judge_v5_4_v28_field_guarded_diagnostic",
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v27_predecessor": _validate_v27_predecessor(v27_root),
        "late_predecessor_attempts": _late_predecessor_records(root),
        "measured_basis": {
            "v27_known_usage_lower_bound_total_tokens": 0,
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": len(turn_names) * MAX_TOKENS_PER_TURN,
        },
    }
    v27_failure = _load_json(v27_root / "failure.json", "v27 failure")
    known = v27_failure.get("known_usage_lower_bound")
    if isinstance(known, dict):
        audit["measured_basis"]["v27_known_usage_lower_bound_total_tokens"] = int(
            known.get("total_tokens") or 0
        )
    if audit_path.exists():
        prior = _load_json(audit_path, "v28 capacity audit")
        stable = deepcopy(audit)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV28DiagnosticError("immutable v28 capacity audit drifted")
        audit = prior
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V28_CAPACITY_POLICY_VERSION,
        "phase_id": "judge_v5_4_v28_field_guarded_diagnostic",
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
        prior = _load_json(policy_path, "v28 capacity policy")
        stable = deepcopy(policy)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV28DiagnosticError("immutable v28 capacity policy drifted")
        policy = prior
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def _validate_capacity_checkpoint(path: Path, policy_path: Path) -> None:
    value = _load_json(path, "v28 reserve capacity checkpoint")
    if (
        value.get("schema_version") != RESERVE_CAPACITY_CHECKPOINT_VERSION
        or value.get("policy_sha256") != _sha256_file(policy_path)
        or value.get("cleared_for_semantic_turn") is not True
        or value.get("managed_chatgpt_auth_verified") is not True
        or value.get("rate_limit_reached_type") is not None
        or value.get("retry_checkpoint_reuse_allowed") is not False
    ):
        raise JudgeV5CalibrationV28DiagnosticError("v28 reserve capacity checkpoint drifted")


def freeze_v28_field_guarded_diagnostic(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v23_root: Path = DEFAULT_V23_ROOT,
    v27_root: Path = DEFAULT_V27_ROOT,
    design_root: Path = DEFAULT_V26_PRESEMANTIC_DESIGN_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    _validate_v27_predecessor(v27_root)
    design_terminal = _load_json(design_root / "terminal.json", "v26 presemantic terminal")
    if (
        design_terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or design_terminal.get("semantic_attempt_authorized") is not False
        or design_terminal.get("production_mutated") is not False
    ):
        raise JudgeV5CalibrationV28DiagnosticError("v26 design terminal is not admissible")
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
    taxonomy_path = root / "v27-sanitized-error-taxonomy.json"
    _write_immutable(pool_path, pool)
    _write_immutable(truth_path, truth)
    taxonomy = _build_v27_error_taxonomy(v27_root)
    if taxonomy_path.exists():
        prior_taxonomy = _load_json(taxonomy_path, "v28 sanitized taxonomy")
        stable_taxonomy = deepcopy(taxonomy)
        stable_taxonomy["created_at"] = prior_taxonomy.get("created_at")
        if prior_taxonomy != stable_taxonomy:
            raise JudgeV5CalibrationV28DiagnosticError("immutable v28 taxonomy drifted")
        taxonomy = prior_taxonomy
    else:
        _write_immutable(taxonomy_path, taxonomy)
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
    capacity = _build_capacity_policy(root, v27_root.expanduser().resolve())
    spec = {
        "schema_version": V28_DIAGNOSTIC_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "protocol_version": PROTOCOL_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "field_guarded_pointwise_then_three_case_alignment_shards",
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
        "v27_predecessor": _validate_v27_predecessor(v27_root),
        "late_predecessor_attempts": _late_predecessor_records(root),
        "v26_presemantic_terminal": _record(design_root / "terminal.json"),
        "v27_sanitized_error_taxonomy": _record(taxonomy_path),
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
    spec_path = root / "field-guarded-diagnostic-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v28 field-guarded diagnostic spec")
        stable = deepcopy(spec)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV28DiagnosticError("immutable v28 spec drifted")
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
        sidecar = _load_json(Path(sidecar_record["path"]), "v28 failed sidecar")
        try:
            usage = _validate_usage(sidecar)
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    accounting_complete = not partial and unknown == 0
    failure = {
        "schema_version": V28_DIAGNOSTIC_FAILURE_VERSION,
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
        "schema_version": V28_DIAGNOSTIC_TERMINAL_VERSION,
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
        "accounting_complete": accounting_complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


def _score_v28(
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
    score["metrics"]["order_bias"] = _ratio(disagreement_count, len(V28_CANARY_CASE_IDS))
    score["metrics"]["canary_case_count"] = len(V28_CANARY_CASE_IDS)
    score["checks"]["order_bias"] = (
        score["metrics"]["order_bias"] <= CALIBRATION_GATES["order_bias_max"]
    )
    score["checks"]["canary_case_count"] = len(V28_CANARY_CASE_IDS) == 6
    score["passed"] = all(score["checks"].values())
    score["failed_checks"] = sorted(key for key, passed in score["checks"].items() if not passed)
    score["v28_canary_case_ids"] = list(V28_CANARY_CASE_IDS)
    return score


async def run_v28_field_guarded_diagnostic(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v23_root: Path = DEFAULT_V23_ROOT,
    v27_root: Path = DEFAULT_V27_ROOT,
    design_root: Path = DEFAULT_V26_PRESEMANTIC_DESIGN_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v28 terminal")
    frozen = freeze_v28_field_guarded_diagnostic(
        output_dir=root,
        v23_root=v23_root,
        v27_root=v27_root,
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
            pointwise_outputs = []
            for shard in frozen["pointwise_shards"]:
                current_turn = shard["turn_name"]
                output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=shard["paths"],
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                    base_instructions=_pointwise_instructions_v28(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["units"]),
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard: validate_pointwise_support_output(
                        value, item["input"]
                    ),
                )
                pointwise_outputs.append(output)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
            pointwise_output = _merge_outputs(pointwise_outputs, "units")
            pointwise_output_path = root / "pointwise-output-full.private.json"
            _write_immutable(pointwise_output_path, pointwise_output)
            support_receipts = freeze_support_receipts(pointwise_output, frozen["pointwise_input"])
            support_path = root / "support-receipts.private.json"
            _write_immutable(support_path, support_receipts)
            base_input = build_neutral_alignment_input(frozen["pool"], support_receipts)
            base_outputs = []
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
                    base_instructions=_alignment_instructions_v28(),
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
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
            base_output = _merge_outputs(base_outputs, "cases")
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
                    base_instructions=_alignment_instructions_v28(),
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
                    base_instructions=_alignment_instructions_v28(),
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
        score = _score_v28(
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
            "schema_version": V28_DIAGNOSTIC_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v28_field_guarded_diagnostic_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v28_field_guarded_diagnostic_passed_full_calibration_authorized"
                if passed
                else "v28_field_guarded_diagnostic_quality_gate_not_passed"
            ),
            "babysitter_status": (
                "v28_field_guarded_diagnostic_passed_full_calibration_authorized"
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
    parser = argparse.ArgumentParser(description="Run v28 field-guarded judge diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v23-root", default=str(DEFAULT_V23_ROOT))
    parser.add_argument("--v27-root", default=str(DEFAULT_V27_ROOT))
    parser.add_argument("--design-root", default=str(DEFAULT_V26_PRESEMANTIC_DESIGN_ROOT))
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v28_field_guarded_diagnostic(
            output_dir=Path(args.output_dir),
            v23_root=Path(args.v23_root),
            v27_root=Path(args.v27_root),
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
