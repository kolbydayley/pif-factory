from __future__ import annotations

"""Fresh 12-case diagnostic against the restored canonical v5 reference."""

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
    build_pointwise_support_input,
    build_pointwise_support_prompt,
    freeze_support_receipts,
    neutral_alignment_output_schema,
    pointwise_support_base_instructions,
    pointwise_support_output_schema,
    validate_pointwise_support_output,
)
from .app_server_judge_v5_calibration import CALIBRATION_GATES, pointwise_input_subset
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
from .app_server_judge_v5_calibration_v26_diagnostic import (
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
    _reconcile_scoreable_alignment,
    _score_subset,
    _subset_pool,
    _subset_truth,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v48_diagnostic import (
    TARGET_CASE_IDS as RESIDUAL_CASE_IDS,
    build_bipartite_alignment_input,
    bipartite_alignment_instructions,
    build_bipartite_alignment_prompt,
    validate_bipartite_alignment_output,
)
from .app_server_judge_v5_calibration_v50_reference_restore import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V50_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso


V51_SPEC_VERSION = "pif_app_server_judge_v5_4_v51_fresh_diagnostic_spec_v1"
V51_SCORE_VERSION = "pif_app_server_judge_v5_4_v51_fresh_diagnostic_score_v1"
V51_FAILURE_VERSION = "pif_app_server_judge_v5_4_v51_fresh_diagnostic_failure_v1"
V51_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v51_fresh_diagnostic_terminal_v1"
V51_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V51_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V51_PHASE_ID = "judge_v5_4_v51_fresh_canonical_diagnostic"
CASES_PER_SHARD = 6

CONTROL_CASE_IDS = (
    "jcase_bfb849451c42ae17e8da679e",  # identity paraphrase
    "jcase_0fc6ec2bd31814bf6a20756a",  # material field error
    "jcase_a6388cf9b869f73abc71ea98",  # merge/split boundary
    "jcase_6ed97bbc7e5220914a0ed899",  # reversed multi-lens
    "jcase_0537a122213719690fee5ba5",  # supported one-sided
    "jcase_2323410cfa22406f0532cd70",  # unsupported one-sided
)
DIAGNOSTIC_CASE_IDS = tuple(RESIDUAL_CASE_IDS) + CONTROL_CASE_IDS

DEFAULT_V49_ROOT = (
    DEFAULT_V50_ROOT.parent / "judge-calibration-v5_4-v49-bipartite-targeted-diagnostic-recovery"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V50_ROOT.parent / "judge-calibration-v5_4-v51-fresh-canonical-diagnostic"
).resolve()


class JudgeV5CalibrationV51DiagnosticError(RuntimeError):
    """The v51 diagnostic cannot continue without violating its frozen contract."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _shards(case_ids: Sequence[str] = DIAGNOSTIC_CASE_IDS) -> list[list[str]]:
    values = list(case_ids)
    shards = [values[index : index + CASES_PER_SHARD] for index in range(0, len(values), CASES_PER_SHARD)]
    if len(shards) != 2 or any(len(shard) != CASES_PER_SHARD for shard in shards):
        raise JudgeV5CalibrationV51DiagnosticError("v51 shard layout drifted")
    return shards


def _turn_names() -> list[str]:
    return [
        "pointwise_support_shard_00",
        "pointwise_support_shard_01",
        "bipartite_alignment_base_shard_00",
        "bipartite_alignment_base_shard_01",
        "bipartite_alignment_canary_shard_00",
        "bipartite_alignment_canary_shard_01",
        "disagreement_adjudication",
    ]


def v51_pointwise_instructions() -> str:
    return (
        pointwise_support_base_instructions()
        + " Apply these scope rules before deciding. Proposition support is evaluated over "
        "the entire source excerpt, not only one evidence sentence. A single event may "
        "compress multiple source propositions; it remains proposition-supported when every "
        "material clause is entailed somewhere in the excerpt. Merge versus split is an "
        "alignment event-boundary question and is not by itself unsupported inference. "
        "Resolve ordinary paraphrase, pronouns, negation, and discourse roles semantically. "
        "For structured fields, judge only populated fields, use the whole excerpt, and report "
        "minimal independent root fields. A wrong actor or speaker does not also make "
        "attribution different unless the attribution relation itself is wrong. A supported "
        "paraphrase does not make evidence different merely because its wording is not copied."
    )


def build_v51_pointwise_prompt(pointwise_input: Mapping[str, Any]) -> str:
    return (
        "Apply proposition support before structured-field correctness. For a compound claim, "
        "test each material clause against the full excerpt and mark supported when all pass. "
        "Do not convert merge/split scope into unsupported_inference. Return only minimal "
        "independent field_issue_fields; do not add derivative attribution or evidence errors.\n\n"
        + build_pointwise_support_prompt(pointwise_input)
    )


def v51_alignment_instructions() -> str:
    return (
        bipartite_alignment_instructions()
        + " Correspondence is semantic event identity, not nearest wording. For each witness, "
        "first identify its claim target, stance/negation, event type, and source-event lens; "
        "then pair it with the opposite-set witness representing that same lens. An extra "
        "unsupported assertion is normally an unpaired residual when the opposite set has no "
        "event for that assertion; never consume a supported event as its closest match. "
        "A merged event may correspond to one split event while the other split event remains "
        "unpaired; classify that pair partial with exactly event_boundary and evidence when all "
        "merged clauses are source-supported. Ordinary negated paraphrases such as opposition "
        "versus lack of support preserve the same stance when their truth conditions match. "
        "Use minimal root checklist differences and do not duplicate an actor/speaker error as "
        "attribution unless the reporting relation itself changes."
    )


def build_v51_alignment_prompt(alignment_input: Mapping[str, Any]) -> str:
    return (
        "Pair by the same underlying source-event lens before judging relation. Check target, "
        "stance/negation, event type, and proposition scope together; lexical closeness is not "
        "correspondence. Preserve unsupported extra assertions as residuals when they lack a "
        "cross-set counterpart.\n\n"
        + build_bipartite_alignment_prompt(alignment_input)
    )


def _validate_predecessors(v50_root: Path, v49_root: Path) -> dict[str, Any]:
    paths = {
        "v50_terminal": v50_root / "terminal.json",
        "v50_receipt": v50_root / "reference-restoration-receipt.json",
        "canonical_pool": v50_root / "canonical-shared-witness-pool.private.json",
        "canonical_mapping": v50_root / "canonical-witness-mapping.private.json",
        "canonical_truth": v50_root / "canonical-calibration-truth.private.json",
        "restoration_audit": v50_root / "reference-restoration-audit.json",
        "v49_terminal": v49_root / "terminal.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    v50 = values["v50_terminal"]
    if (
        v50.get("state") != "completed"
        or v50.get("terminal_reason") != "v50_canonical_reference_restoration_completed"
        or v50.get("reference_frozen") is not True
        or v50.get("production_mutated") is not False
        or not _record_matches(v50.get("reference_receipt"), paths["v50_receipt"])
        or not _record_matches(v50.get("canonical_truth"), paths["canonical_truth"])
        or not _record_matches(v50.get("restoration_audit"), paths["restoration_audit"])
    ):
        raise JudgeV5CalibrationV51DiagnosticError("v50 reference is not admissible")
    receipt = values["v50_receipt"]
    for key in ("canonical_pool", "canonical_mapping", "canonical_truth", "restoration_audit"):
        if not _record_matches(receipt.get(key), paths[key]):
            raise JudgeV5CalibrationV51DiagnosticError(f"v50 {key} drifted")
    v49 = values["v49_terminal"]
    if (
        v49.get("state") != "inactive"
        or v49.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or v49.get("accounting_complete") is not True
        or v49.get("usage_status") != "complete"
        or v49.get("production_mutated") is not False
        or v49.get("semantic_retry_count") != 0
    ):
        raise JudgeV5CalibrationV51DiagnosticError("v49 attempt is not admissible")
    return {name: _record(path) for name, path in paths.items()}


def _build_capacity_policy(
    *, root: Path, predecessors: Mapping[str, Any], v49_root: Path
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    v49_terminal = _load_json(v49_root / "terminal.json", "v49 terminal")
    turn_names = _turn_names()
    audit = {
        "schema_version": V51_CAPACITY_AUDIT_VERSION,
        "phase_id": V51_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessors": predecessors,
        "measured_basis": {
            "v49_turn_count": v49_terminal.get("turn_count"),
            "v49_total_tokens": (v49_terminal.get("usage") or {}).get("total_tokens"),
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": len(turn_names) * MAX_TOKENS_PER_TURN,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v51 capacity audit")
        stable = deepcopy(audit)
        stable["created_at"] = prior.get("created_at")
        if stable != prior:
            raise JudgeV5CalibrationV51DiagnosticError("immutable v51 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V51_CAPACITY_POLICY_VERSION,
        "phase_id": V51_PHASE_ID,
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
        prior = _load_json(policy_path, "v51 capacity policy")
        stable = deepcopy(policy)
        stable["created_at"] = prior.get("created_at")
        if stable != prior:
            raise JudgeV5CalibrationV51DiagnosticError("immutable v51 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v51_diagnostic(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v50_root: Path = DEFAULT_V50_ROOT,
    v49_root: Path = DEFAULT_V49_ROOT,
    model: str = "gpt-5.6-luna",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessors = _validate_predecessors(v50_root.resolve(), v49_root.resolve())
    canonical_pool = _load_json(v50_root / "canonical-shared-witness-pool.private.json", "canonical pool")
    canonical_truth = _load_json(v50_root / "canonical-calibration-truth.private.json", "canonical truth")
    pool = _subset_pool(canonical_pool, DIAGNOSTIC_CASE_IDS)
    truth = _subset_truth(canonical_truth, DIAGNOSTIC_CASE_IDS)
    if len(set(DIAGNOSTIC_CASE_IDS)) != 12 or set(RESIDUAL_CASE_IDS) & set(CONTROL_CASE_IDS):
        raise JudgeV5CalibrationV51DiagnosticError("v51 case selection drifted")
    pool_path = root / "shared-witness-pool.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    _write_immutable(pool_path, pool)
    _write_immutable(truth_path, truth)
    pointwise_input = build_pointwise_support_input(pool)
    pointwise_input_path = root / "pointwise-input-full.private.json"
    _write_immutable(pointwise_input_path, pointwise_input)
    pointwise_shards = []
    for index, case_ids in enumerate(_shards()):
        shard_input = pointwise_input_subset(pointwise_input, case_ids)
        prompt = build_v51_pointwise_prompt(shard_input)
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
                "case_ids": list(case_ids),
                "input": shard_input,
                "prompt": prompt,
                "schema": schema,
                "turn_name": turn_name,
                "paths": paths,
            }
        )
    capacity = _build_capacity_policy(root=root, predecessors=predecessors, v49_root=v49_root)
    runtime_dir = Path(__file__).resolve().parent
    runtime_files = [
        Path(__file__).resolve(),
        runtime_dir / "app_server_judge_v5.py",
        runtime_dir / "app_server_judge_v5_calibration.py",
        runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v48_diagnostic.py",
        runtime_dir / "app_server_capacity_reserve.py",
        runtime_dir / "codex_app_server.py",
    ]
    spec = {
        "schema_version": V51_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "protocol_version": PROTOCOL_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_pointwise_then_anonymous_bipartite_alignment",
        "case_count": len(DIAGNOSTIC_CASE_IDS),
        "case_ids": list(DIAGNOSTIC_CASE_IDS),
        "residual_case_ids": list(RESIDUAL_CASE_IDS),
        "control_case_ids": list(CONTROL_CASE_IDS),
        "witness_count": len(pointwise_input["units"]),
        "turn_plan": _turn_names(),
        "retry_count_per_turn": 0,
        "semantic_model_calls_performed_during_freeze": 0,
        "promotion_rule": (
            "all_frozen_subset_gates_and_all_residual_exact_gates_must_pass"
        ),
        "full_calibration_authorized_before_diagnostic_pass": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessors": predecessors,
        "runtime_files": [_record(path) for path in runtime_files],
        "frozen_inputs": {
            "pool": _record(pool_path),
            "truth": _record(truth_path),
            "pointwise_full": _record(pointwise_input_path),
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
        "privacy": "private_inputs_prompts_outputs_no_source_text_in_terminal",
    }
    spec_path = root / "fresh-diagnostic-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v51 spec")
        stable = deepcopy(spec)
        stable["created_at"] = prior.get("created_at")
        if stable != prior:
            raise JudgeV5CalibrationV51DiagnosticError("immutable v51 spec drifted")
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


def _subset_pointwise(output: Mapping[str, Any], case_ids: Sequence[str]) -> dict[str, Any]:
    wanted = set(case_ids)
    rows = [deepcopy(row) for row in output.get("units") or [] if str(row.get("case_id")) in wanted]
    if {str(row.get("case_id")) for row in rows} != wanted:
        raise JudgeV5CalibrationV51DiagnosticError("target pointwise coverage drifted")
    return {"units": rows}


def _subset_alignment(output: Mapping[str, Any], case_ids: Sequence[str]) -> dict[str, Any]:
    wanted = set(case_ids)
    cases = [deepcopy(row) for row in output.get("cases") or [] if str(row.get("case_id")) in wanted]
    if {str(row.get("case_id")) for row in cases} != wanted:
        raise JudgeV5CalibrationV51DiagnosticError("target alignment coverage drifted")
    return {**{key: deepcopy(value) for key, value in output.items() if key != "cases"}, "cases": cases}


def _subset_disagreements(value: Mapping[str, Any], case_ids: Sequence[str]) -> dict[str, Any]:
    wanted = set(case_ids)
    rows = [deepcopy(row) for row in value.get("disagreements") or [] if str(row.get("case_id")) in wanted]
    return {
        "disagreement_case_count": len(rows),
        "disagreements": rows,
        "adjudication_required": bool(rows),
        "adjudication_call_cap": 1,
        "majority_voting_used": False,
    }


def _score_v51(
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
    residual_score = _score_subset(
        pointwise_output=_subset_pointwise(pointwise_output, RESIDUAL_CASE_IDS),
        reconciled_alignment=_subset_alignment(reconciled_alignment, RESIDUAL_CASE_IDS),
        expected=_subset_truth(expected, RESIDUAL_CASE_IDS),
        observable_disagreements=_subset_disagreements(
            observable_disagreements, RESIDUAL_CASE_IDS
        ),
    )
    exact_one = (
        "support_sensitivity",
        "support_specificity",
        "structured_field_accuracy",
        "pointwise_field_issue_f1",
        "alignment_f1",
        "equivalent_sensitivity",
        "equivalent_specificity",
        "field_diagnostic_f1",
        "relation_accuracy",
        "equivalence_partition_exact_case_rate",
        "unpaired_exact_case_rate",
    )
    residual_checks = {
        **{name: residual_score["metrics"].get(name) == 1.0 for name in exact_one},
        "abstention_rate": residual_score["metrics"].get("abstention_rate") == 0.0,
        "order_bias": residual_score["metrics"].get("order_bias") == 0.0,
    }
    score["schema_version"] = V51_SCORE_VERSION
    score["residual_metrics"] = residual_score["metrics"]
    score["residual_exact_checks"] = residual_checks
    score["passed"] = all(score["checks"].values()) and all(residual_checks.values())
    score["failed_checks"] = sorted(
        [name for name, passed in score["checks"].items() if not passed]
        + [f"residual_exact_{name}" for name, passed in residual_checks.items() if not passed]
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
        sidecar = _load_json(Path(sidecar_record["path"]), "v51 failed sidecar")
        try:
            usage = _validate_usage(sidecar)
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    accounting_complete = not partial and unknown == 0
    failure = {
        "schema_version": V51_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
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
        "schema_version": V51_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
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


async def run_v51_diagnostic(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v50_root: Path = DEFAULT_V50_ROOT,
    v49_root: Path = DEFAULT_V49_ROOT,
    model: str = "gpt-5.6-luna",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v51 terminal")
    frozen = freeze_v51_diagnostic(
        output_dir=root,
        v50_root=v50_root,
        v49_root=v49_root,
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
                    base_instructions=v51_pointwise_instructions(),
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
            pointwise_path = root / "pointwise-output-full.private.json"
            _write_immutable(pointwise_path, pointwise_output)
            support_receipts = freeze_support_receipts(
                pointwise_output, frozen["pointwise_input"]
            )
            support_path = root / "support-receipts.private.json"
            _write_immutable(support_path, support_receipts)

            outputs: dict[str, list[Mapping[str, Any]]] = {
                "base": [],
                "balanced_canary": [],
            }
            for permutation, label in (("base", "base"), ("balanced_canary", "canary")):
                for index, case_ids in enumerate(_shards()):
                    shard_input = build_bipartite_alignment_input(
                        frozen["pool"],
                        support_receipts,
                        case_ids=case_ids,
                        permutation=permutation,
                    )
                    prompt = build_v51_alignment_prompt(shard_input)
                    schema = neutral_alignment_output_schema(shard_input)
                    current_turn = f"bipartite_alignment_{label}_shard_{index:02d}"
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
                        base_instructions=v51_alignment_instructions(),
                        model=model,
                        effort=reasoning_effort,
                        timeout_seconds=timeout_seconds,
                        batch_size=len(shard_input["cases"]),
                        policy_path=policy_path,
                        output_validator=lambda value, item=shard_input: validate_bipartite_alignment_output(
                            value, item
                        ),
                    )
                    outputs[permutation].append(output)
                    sidecars.append(sidecar)
                    adopted[current_turn] = was_adopted
            base_output = _merge_outputs(outputs["base"], "cases")
            canary_output = _merge_outputs(outputs["balanced_canary"], "cases")
            base_input = build_bipartite_alignment_input(
                frozen["pool"], support_receipts, case_ids=DIAGNOSTIC_CASE_IDS, permutation="base"
            )
            canary_input = build_bipartite_alignment_input(
                frozen["pool"],
                support_receipts,
                case_ids=DIAGNOSTIC_CASE_IDS,
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
                    base_instructions=v51_alignment_instructions(),
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
        score = _score_v51(
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
            "schema_version": V51_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v51_fresh_diagnostic_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v51_fresh_diagnostic_passed_full_calibration_authorized"
                if passed
                else "v51_fresh_diagnostic_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "diagnostic_passed": passed,
            "full_calibration_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_allowed": False,
            "semantic_retry_count": 0,
            "spec_sha256": _sha256_file(frozen["spec_path"]),
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
    parser = argparse.ArgumentParser(description="Run the v51 fresh canonical diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v50-root", default=str(DEFAULT_V50_ROOT))
    parser.add_argument("--v49-root", default=str(DEFAULT_V49_ROOT))
    parser.add_argument("--model", default="gpt-5.6-luna")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v51_diagnostic(
            output_dir=Path(args.output_dir),
            v50_root=Path(args.v50_root),
            v49_root=Path(args.v49_root),
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "diagnostic_passed": terminal["diagnostic_passed"],
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
