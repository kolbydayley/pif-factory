from __future__ import annotations

"""Targeted anonymous-bipartite alignment diagnostic after the v46 failure."""

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
    _ratio,
    _reconcile_scoreable_alignment,
    _score_subset,
    _subset_pool,
    _subset_truth,
    _validate_scoreable_alignment_output,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v28_diagnostic import _chunked
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso


V48_SPEC_VERSION = "pif_app_server_judge_v5_4_v48_bipartite_diagnostic_spec_v1"
V48_SCORE_VERSION = "pif_app_server_judge_v5_4_v48_bipartite_diagnostic_score_v1"
V48_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v48_bipartite_diagnostic_terminal_v1"
V48_FAILURE_VERSION = "pif_app_server_judge_v5_4_v48_bipartite_diagnostic_failure_v1"
V48_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V48_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V48_PHASE_ID = "judge_v5_4_v48_bipartite_alignment_diagnostic"

TARGET_CASE_IDS = (
    "jcase_1124b8b6032c8ed35dada2f0",
    "jcase_1153895f992c3ca36fecd004",
    "jcase_27edbb38a94ae1eb60da4295",
    "jcase_43f1212ec1d286cde0eaf89d",
    "jcase_a2a565c42526b84767bf4b1d",
    "jcase_ddf03786960ff7583b95eb84",
)
CASES_PER_SHARD = 3
ANONYMOUS_SET_LABELS = ("set_kappa", "set_sigma")

DEFAULT_V46_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v46-full-development-diagnostic"
).resolve()
DEFAULT_V47_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v47-reference-consistency-freeze"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v48-bipartite-targeted-diagnostic"
).resolve()


class JudgeV5CalibrationV48DiagnosticError(RuntimeError):
    """The v48 diagnostic cannot continue without violating its frozen contract."""


def _turn_names() -> list[str]:
    names = [
        f"bipartite_alignment_base_shard_{index:02d}"
        for index in range(math.ceil(len(TARGET_CASE_IDS) / CASES_PER_SHARD))
    ]
    names.extend(
        f"bipartite_alignment_canary_shard_{index:02d}"
        for index in range(math.ceil(len(TARGET_CASE_IDS) / CASES_PER_SHARD))
    )
    names.append("disagreement_adjudication")
    return names


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _write_or_adopt_timestamped(path: Path, value: Mapping[str, Any], purpose: str) -> dict[str, Any]:
    candidate = deepcopy(dict(value))
    if path.exists():
        prior = _load_json(path, purpose)
        candidate["created_at"] = prior.get("created_at")
        if prior != candidate:
            raise JudgeV5CalibrationV48DiagnosticError(f"immutable {purpose} drifted")
        return prior
    _write_immutable(path, candidate)
    return candidate


def _validate_presemantic_predecessor(root: Path) -> dict[str, Any]:
    terminal_path = root / "terminal.json"
    failure_path = root / "failure.json"
    terminal = _load_json(terminal_path, "presemantic predecessor terminal")
    failure = _load_json(failure_path, "presemantic predecessor failure")
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("semantic_attempt_started") is not False
        or terminal.get("usage_status") != "not_applicable_no_semantic_turn_started"
        or terminal.get("production_mutated") is not False
        or failure.get("error_class") != "ImmutableFreezeIdempotencyError"
        or failure.get("semantic_attempt_started") is not False
        or failure.get("capacity_checkpoint_count") != 0
        or failure.get("sidecar_count") != 0
        or failure.get("output_count") != 0
    ):
        raise JudgeV5CalibrationV48DiagnosticError(
            "presemantic recovery predecessor is not admissible"
        )
    if not _record_matches(terminal.get("failure"), failure_path):
        raise JudgeV5CalibrationV48DiagnosticError(
            "presemantic recovery terminal does not bind failure"
        )
    return {
        "terminal": _record(terminal_path),
        "failure": _record(failure_path),
    }


def _validate_predecessors(v46_root: Path, v47_root: Path) -> dict[str, Any]:
    v46_terminal_path = v46_root / "terminal.json"
    v46_spec_path = v46_root / "full-development-diagnostic-spec.json"
    v46_terminal = _load_json(v46_terminal_path, "v46 terminal")
    v46_spec = _load_json(v46_spec_path, "v46 spec")
    v47_terminal_path = v47_root / "terminal.json"
    v47_receipt_path = v47_root / "reference-freeze-receipt.json"
    v47_truth_path = v47_root / "frozen-diagnostic-truth.private.json"
    v47_terminal = _load_json(v47_terminal_path, "v47 terminal")
    v47_receipt = _load_json(v47_receipt_path, "v47 receipt")
    if (
        v46_terminal.get("state") != "inactive"
        or v46_terminal.get("development_terminal_reason")
        != "v46_full_development_diagnostic_quality_gate_not_passed"
        or v46_terminal.get("accounting_complete") is not True
        or v46_terminal.get("usage_status") != "complete"
        or v46_terminal.get("production_mutated") is not False
        or v46_terminal.get("full_calibration_authorized") is not False
    ):
        raise JudgeV5CalibrationV48DiagnosticError("v46 predecessor is not admissible")
    if (
        v47_terminal.get("state") != "completed"
        or v47_terminal.get("terminal_reason")
        != "v47_reference_consistency_freeze_completed"
        or v47_terminal.get("targeted_diagnostic_authorized") is not True
        or v47_terminal.get("production_mutated") is not False
        or v47_receipt.get("reference_freeze_authorized") is not True
        or not _record_matches(v47_receipt.get("frozen_truth"), v47_truth_path)
    ):
        raise JudgeV5CalibrationV48DiagnosticError("v47 reference is not admissible")
    frozen_inputs = v46_spec.get("frozen_inputs")
    if not isinstance(frozen_inputs, Mapping):
        raise JudgeV5CalibrationV48DiagnosticError("v46 frozen inputs are missing")
    input_paths = {
        "pool": v46_root / "shared-witness-pool.private.json",
        "support_receipts": v46_root / "support-receipts.private.json",
        "pointwise_output": v46_root / "pointwise-output-full.private.json",
    }
    for name, path in input_paths.items():
        if not _record_matches(frozen_inputs.get(name), path):
            raise JudgeV5CalibrationV48DiagnosticError(f"v46 {name} drifted")
    return {
        "v46_terminal": _record(v46_terminal_path),
        "v46_spec": _record(v46_spec_path),
        "v46_pool": _record(input_paths["pool"]),
        "v46_support_receipts": _record(input_paths["support_receipts"]),
        "v46_pointwise_output": _record(input_paths["pointwise_output"]),
        "v47_terminal": _record(v47_terminal_path),
        "v47_receipt": _record(v47_receipt_path),
        "v47_truth": _record(v47_truth_path),
    }


def annotate_anonymous_comparison_sets(
    alignment_input: Mapping[str, Any],
    pool: Mapping[str, Any],
    *,
    permutation: str,
) -> dict[str, Any]:
    if permutation not in {"base", "balanced_canary"}:
        raise JudgeV5CalibrationV48DiagnosticError("unsupported v48 permutation")
    rendered = deepcopy(alignment_input)
    pool_by_case = {str(case["case_id"]): case for case in pool.get("cases") or []}
    left_label, right_label = ANONYMOUS_SET_LABELS
    if permutation == "balanced_canary":
        left_label, right_label = right_label, left_label
    for case in rendered.get("cases") or []:
        source_case = pool_by_case.get(str(case.get("case_id")))
        if source_case is None:
            raise JudgeV5CalibrationV48DiagnosticError("alignment case is outside pool")
        membership: dict[str, str] = {}
        for witness in source_case.get("event_set_a") or []:
            membership[str(witness["witness_id"])] = left_label
        for witness in source_case.get("event_set_b") or []:
            witness_id = str(witness["witness_id"])
            if witness_id in membership:
                raise JudgeV5CalibrationV48DiagnosticError("witness appears in both sets")
            membership[witness_id] = right_label
        for witness in case.get("witnesses") or []:
            witness_id = str(witness.get("witness_id"))
            if witness_id not in membership:
                raise JudgeV5CalibrationV48DiagnosticError("witness set membership is missing")
            witness["comparison_set"] = membership[witness_id]
    rendered["anonymous_comparison_sets_present"] = True
    rendered["comparison_set_labels"] = sorted(ANONYMOUS_SET_LABELS)
    rendered["comparison_set_labels_swapped_for_canary"] = permutation == "balanced_canary"
    rendered["comparison_sets_reveal_system_identity"] = False
    return rendered


def build_bipartite_alignment_input(
    pool: Mapping[str, Any],
    support_receipts: Mapping[str, Any],
    *,
    case_ids: Sequence[str],
    permutation: str,
) -> dict[str, Any]:
    generic = build_neutral_alignment_input(
        pool,
        support_receipts,
        case_ids=case_ids,
        permutation=permutation,
    )
    return annotate_anonymous_comparison_sets(generic, pool, permutation=permutation)


def bipartite_alignment_instructions() -> str:
    return (
        neutral_alignment_base_instructions()
        + " Anonymous bipartite correspondence guard: comparison_set labels are arbitrary "
        "and reveal neither system identity nor quality. First pair only across the two "
        "comparison sets by the underlying source-event correspondence, even when a material "
        "field mutation makes the paired witnesses non-equivalent. Every alignment pair must "
        "contain exactly one witness from each comparison set. A witness is unpaired exactly "
        "when no witness in the opposite set represents the same underlying source event or "
        "lens. Do not pair two witnesses merely because they are the closest wording, and do "
        "not leave a material mutation unpaired when its cross-set counterpart is identifiable. "
        "After correspondence is fixed, determine equivalence groups and the 15-row checklist. "
        "The unsupported_inference row remains the exact projection of the frozen pointwise "
        "proposition verdicts; do not add another semantic field unless that field itself "
        "changes truth conditions."
    )


def build_bipartite_alignment_prompt(alignment_input: Mapping[str, Any]) -> str:
    prefix = (
        "Use anonymous comparison-set membership only to make one-to-one correspondence "
        "identifiable. Never infer which set is baseline or candidate. Pair across sets first; "
        "then classify relation and checklist fields. unpaired_witness_ids must contain every "
        "witness without a cross-set source-event counterpart and no paired witness.\n\n"
    )
    return prefix + build_neutral_alignment_prompt(alignment_input)


def validate_bipartite_alignment_output(
    output: Any, alignment_input: Mapping[str, Any]
) -> list[str]:
    errors = list(_validate_scoreable_alignment_output(output, alignment_input))
    errors.extend(bipartite_pair_errors(output, alignment_input))
    return errors


def bipartite_pair_errors(
    output: Any, alignment_input: Mapping[str, Any]
) -> list[str]:
    errors: list[str] = []
    memberships = {
        str(case["case_id"]): {
            str(witness["witness_id"]): str(witness["comparison_set"])
            for witness in case.get("witnesses") or []
        }
        for case in alignment_input.get("cases") or []
    }
    if not isinstance(output, Mapping):
        return errors
    for case in output.get("cases") or []:
        case_id = str(case.get("case_id"))
        case_membership = memberships.get(case_id, {})
        for pair in case.get("alignment_pairs") or []:
            left = str(pair.get("witness_id_1"))
            right = str(pair.get("witness_id_2"))
            if (
                left not in case_membership
                or right not in case_membership
                or case_membership.get(left) == case_membership.get(right)
            ):
                errors.append(f"{case_id}_alignment_pair_not_cross_set")
    return errors


def _support_subset(receipts: Mapping[str, Any], case_ids: Sequence[str]) -> dict[str, Any]:
    wanted = set(case_ids)
    value = deepcopy(receipts)
    value["units"] = [
        row for row in value.get("units") or [] if str(row.get("case_id")) in wanted
    ]
    if {str(row["case_id"]) for row in value["units"]} != wanted:
        raise JudgeV5CalibrationV48DiagnosticError("support subset coverage drifted")
    return value


def _pointwise_subset(output: Mapping[str, Any], case_ids: Sequence[str]) -> dict[str, Any]:
    wanted = set(case_ids)
    value = deepcopy(output)
    value["units"] = [
        row for row in value.get("units") or [] if str(row.get("case_id")) in wanted
    ]
    if {str(row["case_id"]) for row in value["units"]} != wanted:
        raise JudgeV5CalibrationV48DiagnosticError("pointwise subset coverage drifted")
    return value


def _build_capacity_policy(
    *, root: Path, v46_root: Path, predecessors: Mapping[str, Any]
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    turn_names = _turn_names()
    v46_terminal = _load_json(v46_root / "terminal.json", "v46 terminal")
    audit = {
        "schema_version": V48_CAPACITY_AUDIT_VERSION,
        "phase_id": f"{V48_PHASE_ID}:{root.name}",
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessors": predecessors,
        "measured_basis": {
            "v46_total_tokens": int((v46_terminal.get("usage") or {}).get("total_tokens") or 0),
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": len(turn_names) * MAX_TOKENS_PER_TURN,
        },
        "precommitted_experiment": "anonymous_bipartite_targeted_alignment_diagnostic",
    }
    audit = _write_or_adopt_timestamped(audit_path, audit, "v48 capacity audit")
    policy = {
        "schema_version": V48_CAPACITY_POLICY_VERSION,
        "phase_id": f"{V48_PHASE_ID}:{root.name}",
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
            len(turn_names)
            * MAX_TOKENS_PER_TURN
            * QUOTA_POINTS_PER_MILLION_TOKENS
            / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    policy = _write_or_adopt_timestamped(policy_path, policy, "v48 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v48_bipartite_diagnostic(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v46_root: Path = DEFAULT_V46_ROOT,
    v47_root: Path = DEFAULT_V47_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    presemantic_predecessor_root: Optional[Path] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessors = _validate_predecessors(v46_root, v47_root)
    if presemantic_predecessor_root is not None:
        predecessors["presemantic_recovery"] = _validate_presemantic_predecessor(
            presemantic_predecessor_root.expanduser().resolve()
        )
    pool = _subset_pool(
        _load_json(v46_root / "shared-witness-pool.private.json", "v46 pool"),
        TARGET_CASE_IDS,
    )
    support = _support_subset(
        _load_json(v46_root / "support-receipts.private.json", "v46 support receipts"),
        TARGET_CASE_IDS,
    )
    pointwise = _pointwise_subset(
        _load_json(v46_root / "pointwise-output-full.private.json", "v46 pointwise output"),
        TARGET_CASE_IDS,
    )
    truth = _subset_truth(
        _load_json(v47_root / "frozen-diagnostic-truth.private.json", "v47 truth"),
        TARGET_CASE_IDS,
    )
    pool_path = root / "shared-witness-pool.private.json"
    support_path = root / "support-receipts.private.json"
    pointwise_path = root / "pointwise-output-full.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    _write_immutable(pool_path, pool)
    _write_immutable(support_path, support)
    _write_immutable(pointwise_path, pointwise)
    _write_immutable(truth_path, truth)
    frozen_shards: list[dict[str, Any]] = []
    for permutation, prefix in (
        ("base", "bipartite_alignment_base_shard"),
        ("balanced_canary", "bipartite_alignment_canary_shard"),
    ):
        for index, case_ids in enumerate(_chunked(TARGET_CASE_IDS, CASES_PER_SHARD)):
            alignment_input = build_bipartite_alignment_input(
                pool,
                support,
                case_ids=case_ids,
                permutation=permutation,
            )
            prompt = build_bipartite_alignment_prompt(alignment_input)
            schema = neutral_alignment_output_schema(alignment_input)
            turn_name = f"{prefix}_{index:02d}"
            paths = _freeze_turn_request(
                root=root,
                turn_name=turn_name,
                input_value=alignment_input,
                prompt=prompt,
                schema=schema,
            )
            frozen_shards.append(
                {
                    "turn_name": turn_name,
                    "permutation": permutation,
                    "case_ids": list(case_ids),
                    "input": alignment_input,
                    "prompt": prompt,
                    "schema": schema,
                    "paths": paths,
                }
            )
    capacity = _build_capacity_policy(
        root=root, v46_root=v46_root, predecessors=predecessors
    )
    spec = {
        "schema_version": V48_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "protocol_version": PROTOCOL_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "anonymous_bipartite_correspondence_then_field_alignment",
        "case_count": len(TARGET_CASE_IDS),
        "case_ids": list(TARGET_CASE_IDS),
        "case_selection_basis": "all_five_v46_error_cases_plus_one_passing_multilens_control",
        "cases_per_shard": CASES_PER_SHARD,
        "turn_plan": _turn_names(),
        "retry_count_per_turn": 0,
        "semantic_model_calls_performed_during_freeze": 0,
        "promotion_rule": (
            "all_frozen_threshold_checks_pass_and_alignment_partition_relation_field_"
            "unpaired_metrics_are_exact_and_order_bias_is_zero"
        ),
        "stop_rule": "any_failed_or_unknown_usage_turn_or_any_targeted_gate_failure",
        "full_development_diagnostic_authorized_before_pass": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessors": predecessors,
        "frozen_inputs": {
            "pool": _record(pool_path),
            "support_receipts": _record(support_path),
            "pointwise_output": _record(pointwise_path),
            "truth": _record(truth_path),
            "turn_requests": [
                {
                    "turn_name": shard["turn_name"],
                    "permutation": shard["permutation"],
                    "case_ids": shard["case_ids"],
                    "input": _record(shard["paths"]["input"]),
                    "prompt": _record(shard["paths"]["prompt"]),
                    "schema": _record(shard["paths"]["schema"]),
                }
                for shard in frozen_shards
            ],
        },
        "gates": CALIBRATION_GATES,
        "request_byte_caps": {"prompt": MAX_PROMPT_BYTES, "output_schema": MAX_SCHEMA_BYTES},
        "privacy": "private_inputs_prompts_outputs_no_source_text_in_terminal",
    }
    spec_path = root / "bipartite-diagnostic-spec.json"
    spec = _write_or_adopt_timestamped(spec_path, spec, "v48 diagnostic spec")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "pool": pool,
        "support": support,
        "pointwise": pointwise,
        "truth": truth,
        "shards": frozen_shards,
        "capacity_policy": capacity["policy"],
    }


def _score_v48(
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
    score["schema_version"] = V48_SCORE_VERSION
    score["metrics"]["order_bias"] = _ratio(disagreement_count, len(TARGET_CASE_IDS))
    score["metrics"]["canary_case_count"] = len(TARGET_CASE_IDS)
    score["checks"]["order_bias"] = score["metrics"]["order_bias"] == 0.0
    score["checks"]["canary_case_count"] = len(TARGET_CASE_IDS) == 6
    exact_metrics = (
        "alignment_f1",
        "equivalence_partition_exact_case_rate",
        "equivalent_sensitivity",
        "equivalent_specificity",
        "field_diagnostic_f1",
        "relation_accuracy",
        "unpaired_exact_case_rate",
    )
    score["targeted_exact_checks"] = {
        name: score["metrics"].get(name) == 1.0 for name in exact_metrics
    }
    score["passed"] = all(score["checks"].values()) and all(
        score["targeted_exact_checks"].values()
    )
    score["failed_checks"] = sorted(
        [name for name, passed in score["checks"].items() if not passed]
        + [
            f"targeted_exact_{name}"
            for name, passed in score["targeted_exact_checks"].items()
            if not passed
        ]
    )
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
        if not isinstance(sidecar_record, Mapping):
            if attempt.get("capacity") is not None or attempt.get("output") is not None:
                partial = True
            continue
        sidecar = _load_json(Path(sidecar_record["path"]), "v48 failed sidecar")
        try:
            usage = _validate_usage(sidecar)
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    accounting_complete = not partial and unknown == 0
    failure = {
        "schema_version": V48_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "full_development_diagnostic_authorized": False,
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
        "schema_version": V48_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "spec_sha256": _sha256_file(spec_path),
        "failure": _record(failure_path),
        "diagnostic_passed": False,
        "full_development_diagnostic_authorized": False,
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


async def run_v48_bipartite_diagnostic(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v46_root: Path = DEFAULT_V46_ROOT,
    v47_root: Path = DEFAULT_V47_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
    presemantic_predecessor_root: Optional[Path] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v48 terminal")
    frozen = freeze_v48_bipartite_diagnostic(
        output_dir=root,
        v46_root=v46_root,
        v47_root=v47_root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
        presemantic_predecessor_root=presemantic_predecessor_root,
    )
    policy_path = frozen["capacity_policy"]
    factory = client_factory or _client_factory
    sidecars: list[dict[str, Any]] = []
    adopted: dict[str, bool] = {}
    current_turn: Optional[str] = None
    try:
        async with factory(policy_path) as client:
            outputs: dict[str, list[Mapping[str, Any]]] = {"base": [], "balanced_canary": []}
            inputs: dict[str, list[Mapping[str, Any]]] = {"base": [], "balanced_canary": []}
            for shard in frozen["shards"]:
                current_turn = shard["turn_name"]
                output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=shard["paths"],
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                    base_instructions=bipartite_alignment_instructions(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["cases"]),
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard: validate_bipartite_alignment_output(
                        value, item["input"]
                    ),
                )
                outputs[shard["permutation"]].append(output)
                inputs[shard["permutation"]].append(shard["input"])
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
            base_output = _merge_outputs(outputs["base"], "cases")
            canary_output = _merge_outputs(outputs["balanced_canary"], "cases")
            base_input = build_bipartite_alignment_input(
                frozen["pool"],
                frozen["support"],
                case_ids=TARGET_CASE_IDS,
                permutation="base",
            )
            canary_input = build_bipartite_alignment_input(
                frozen["pool"],
                frozen["support"],
                case_ids=TARGET_CASE_IDS,
                permutation="balanced_canary",
            )
            base_path = root / "base-alignment-full.private.json"
            canary_path = root / "canary-alignment-full.private.json"
            _write_immutable(base_path, base_output)
            _write_immutable(canary_path, canary_output)
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
                    base_input=base_input,
                    adjudication_input=packet,
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
                    base_instructions=bipartite_alignment_instructions(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(adjudication_input["cases"]),
                    policy_path=policy_path,
                    output_validator=lambda value: validate_bipartite_alignment_output(
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
        score = _score_v48(
            pointwise_output=frozen["pointwise"],
            reconciled_alignment=reconciled,
            expected=frozen["truth"],
            observable_disagreements=disagreements,
        )
        score_path = root / "diagnostic-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V48_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v48_bipartite_targeted_diagnostic_passed"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v48_bipartite_targeted_diagnostic_passed_full_development_diagnostic_authorized"
                if passed
                else "v48_bipartite_targeted_diagnostic_quality_gate_not_passed"
            ),
            "diagnostic_passed": passed,
            "full_development_diagnostic_authorized": passed,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_allowed": False,
            "semantic_retry_count": 0,
            "spec_sha256": _sha256_file(frozen["spec_path"]),
            "score": _record(score_path),
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
    parser = argparse.ArgumentParser(description="Run the v48 bipartite diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v46-root", default=str(DEFAULT_V46_ROOT))
    parser.add_argument("--v47-root", default=str(DEFAULT_V47_ROOT))
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    parser.add_argument("--presemantic-predecessor-root")
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v48_bipartite_diagnostic(
            output_dir=Path(args.output_dir),
            v46_root=Path(args.v46_root),
            v47_root=Path(args.v47_root),
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
            presemantic_predecessor_root=(
                Path(args.presemantic_predecessor_root)
                if args.presemantic_predecessor_root
                else None
            ),
        )
    )
    print(json.dumps({
        "state": terminal.get("state"),
        "terminal_reason": terminal.get("terminal_reason"),
        "diagnostic_passed": terminal.get("diagnostic_passed"),
        "full_development_diagnostic_authorized": terminal.get(
            "full_development_diagnostic_authorized"
        ),
        "usage_status": terminal.get("usage_status"),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
