from __future__ import annotations

"""Fresh diagnostic against the v53 scoped structured reference."""

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
    freeze_support_receipts,
    neutral_alignment_output_schema,
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
    _subset_pool,
    _subset_truth,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v48_diagnostic import (
    build_bipartite_alignment_input,
    validate_bipartite_alignment_output,
)
from .app_server_judge_v5_calibration_v50_reference_restore import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V50_ROOT,
)
from .app_server_judge_v5_calibration_v51_diagnostic import (
    CONTROL_CASE_IDS,
    DIAGNOSTIC_CASE_IDS,
    RESIDUAL_CASE_IDS,
    _score_v51,
    _shards,
    build_v51_alignment_prompt,
    build_v51_pointwise_prompt,
    v51_alignment_instructions,
    v51_pointwise_instructions,
)
from .app_server_judge_v5_calibration_v53_reference import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V53_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso


V54_SPEC_VERSION = "pif_app_server_judge_v5_4_v54_fresh_diagnostic_spec_v1"
V54_SCORE_VERSION = "pif_app_server_judge_v5_4_v54_fresh_diagnostic_score_v1"
V54_FAILURE_VERSION = "pif_app_server_judge_v5_4_v54_fresh_diagnostic_failure_v1"
V54_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v54_fresh_diagnostic_terminal_v1"
V54_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V54_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V54_PHASE_ID = "judge_v5_4_v54_fresh_scoped_reference_diagnostic"

DEFAULT_V51_ROOT = (
    DEFAULT_V50_ROOT.parent / "judge-calibration-v5_4-v51-fresh-canonical-diagnostic"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V50_ROOT.parent / "judge-calibration-v5_4-v54-fresh-scoped-diagnostic"
).resolve()


class JudgeV5CalibrationV54DiagnosticError(RuntimeError):
    """The v54 diagnostic cannot continue without violating its frozen contract."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


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


def v54_pointwise_instructions() -> str:
    return (
        v51_pointwise_instructions()
        + " For an unsupported proposition, unsupported_inference captures the ungrounded "
        "assertion. Add certainty or temporal_horizon only when the source independently "
        "contradicts that populated value; do not list them merely because the proposition "
        "itself is unsupported. Keep target only when the event asserts a source-unsupported "
        "target as an independent material field."
    )


def build_v54_pointwise_prompt(value: Mapping[str, Any]) -> str:
    return (
        "Use only minimal independent root fields. Inherited lack of support belongs to "
        "unsupported_inference and must not be duplicated into certainty or temporal_horizon "
        "without a separate source conflict. Every returned evidence span must be copied "
        "verbatim; use [] rather than a paraphrase.\n\n"
        + build_v51_pointwise_prompt(value)
    )


def v54_alignment_instructions() -> str:
    return (
        v51_alignment_instructions()
        + " Serialization is fail-closed: every source_evidence_span must be an exact verbatim "
        "substring of source_excerpt. If an exact useful span cannot be copied confidently, "
        "return [] for that row; never normalize, shorten with ellipses, or paraphrase."
    )


def build_v54_alignment_prompt(value: Mapping[str, Any]) -> str:
    return (
        "Before returning, verify every nonempty source_evidence_span by exact character "
        "containment in source_excerpt. Prefer [] over any nonexact quotation.\n\n"
        + build_v51_alignment_prompt(value)
    )


def _validate_predecessors(
    v50_root: Path, v51_root: Path, v53_root: Path
) -> dict[str, Any]:
    paths = {
        "v50_terminal": v50_root / "terminal.json",
        "canonical_pool": v50_root / "canonical-shared-witness-pool.private.json",
        "v51_terminal": v51_root / "terminal.json",
        "v51_failure": v51_root / "failure.json",
        "v53_terminal": v53_root / "terminal.json",
        "v53_receipt": v53_root / "reference-receipt.json",
        "v53_truth": v53_root / "scoped-calibration-truth.private.json",
        "v53_audit": v53_root / "reference-scope-audit.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    if (
        values["v50_terminal"].get("reference_frozen") is not True
        or values["v50_terminal"].get("production_mutated") is not False
    ):
        raise JudgeV5CalibrationV54DiagnosticError("v50 pool predecessor drifted")
    v51 = values["v51_terminal"]
    if (
        v51.get("state") != "failed"
        or v51.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or v51.get("accounting_complete") is not True
        or v51.get("usage_status") != "complete"
        or v51.get("production_mutated") is not False
        or not _record_matches(v51.get("failure"), paths["v51_failure"])
    ):
        raise JudgeV5CalibrationV54DiagnosticError("v51 failure is not admissible")
    v53 = values["v53_terminal"]
    if (
        v53.get("state") != "completed"
        or v53.get("reference_frozen") is not True
        or v53.get("production_mutated") is not False
        or not _record_matches(v53.get("reference_receipt"), paths["v53_receipt"])
        or not _record_matches(v53.get("scoped_truth"), paths["v53_truth"])
        or not _record_matches(v53.get("scope_audit"), paths["v53_audit"])
    ):
        raise JudgeV5CalibrationV54DiagnosticError("v53 reference is not admissible")
    return {name: _record(path) for name, path in paths.items()}


def _build_capacity_policy(
    root: Path, predecessors: Mapping[str, Any], v51_root: Path
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    v51 = _load_json(v51_root / "terminal.json", "v51 terminal")
    turn_names = _turn_names()
    audit = {
        "schema_version": V54_CAPACITY_AUDIT_VERSION,
        "phase_id": V54_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessors": predecessors,
        "measured_basis": {
            "v51_completed_turn_count": len(v51.get("attempts") or []),
            "v51_total_tokens": (v51.get("usage") or {}).get("total_tokens"),
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": len(turn_names) * MAX_TOKENS_PER_TURN,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v54 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV54DiagnosticError("immutable v54 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V54_CAPACITY_POLICY_VERSION,
        "phase_id": V54_PHASE_ID,
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
        prior = _load_json(policy_path, "v54 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV54DiagnosticError("immutable v54 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v54_diagnostic(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v50_root: Path = DEFAULT_V50_ROOT,
    v51_root: Path = DEFAULT_V51_ROOT,
    v53_root: Path = DEFAULT_V53_ROOT,
    model: str = "gpt-5.6-luna",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessors = _validate_predecessors(v50_root.resolve(), v51_root.resolve(), v53_root.resolve())
    pool = _subset_pool(
        _load_json(v50_root / "canonical-shared-witness-pool.private.json", "canonical pool"),
        DIAGNOSTIC_CASE_IDS,
    )
    truth = _subset_truth(
        _load_json(v53_root / "scoped-calibration-truth.private.json", "v53 truth"),
        DIAGNOSTIC_CASE_IDS,
    )
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
        prompt = build_v54_pointwise_prompt(shard_input)
        schema = pointwise_support_output_schema(shard_input)
        turn_name = f"pointwise_support_shard_{index:02d}"
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=shard_input, prompt=prompt, schema=schema
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
    capacity = _build_capacity_policy(root, predecessors, v51_root)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V54_SPEC_VERSION,
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
        "v51_semantic_outputs_reused_for_promotion": False,
        "promotion_rule": "all_frozen_subset_gates_and_all_residual_exact_gates_must_pass",
        "full_calibration_authorized_before_diagnostic_pass": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessors": predecessors,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v48_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v51_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
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
        prior = _load_json(spec_path, "v54 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV54DiagnosticError("immutable v54 spec drifted")
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
        if not isinstance(sidecar_record, Mapping):
            if attempt.get("capacity") is not None or attempt.get("output") is not None:
                partial = True
            continue
        sidecar = _load_json(Path(sidecar_record["path"]), "v54 failed sidecar")
        try:
            usage = _validate_usage(sidecar)
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = not partial and unknown == 0
    failure = {
        "schema_version": V54_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": known if complete else None,
        "known_usage_lower_bound": known,
        "unknown_usage_turn_count": unknown,
        "partial_attempt_without_sidecar": partial,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V54_TERMINAL_VERSION,
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
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v54_diagnostic(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v50_root: Path = DEFAULT_V50_ROOT,
    v51_root: Path = DEFAULT_V51_ROOT,
    v53_root: Path = DEFAULT_V53_ROOT,
    model: str = "gpt-5.6-luna",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v54 terminal")
    frozen = freeze_v54_diagnostic(
        output_dir=root,
        v50_root=v50_root,
        v51_root=v51_root,
        v53_root=v53_root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
    )
    factory = client_factory or _client_factory
    sidecars: list[dict[str, Any]] = []
    adopted: dict[str, bool] = {}
    current_turn: Optional[str] = None
    try:
        async with factory(frozen["capacity_policy"]) as client:
            pointwise_outputs = []
            for shard in frozen["pointwise_shards"]:
                current_turn = shard["turn_name"]
                output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=shard["paths"],
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                    base_instructions=v54_pointwise_instructions(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["units"]),
                    policy_path=frozen["capacity_policy"],
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
            support = freeze_support_receipts(pointwise_output, frozen["pointwise_input"])
            support_path = root / "support-receipts.private.json"
            _write_immutable(support_path, support)
            outputs: dict[str, list[Mapping[str, Any]]] = {"base": [], "balanced_canary": []}
            for permutation, label in (("base", "base"), ("balanced_canary", "canary")):
                for index, case_ids in enumerate(_shards()):
                    shard_input = build_bipartite_alignment_input(
                        frozen["pool"], support, case_ids=case_ids, permutation=permutation
                    )
                    prompt = build_v54_alignment_prompt(shard_input)
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
                        base_instructions=v54_alignment_instructions(),
                        model=model,
                        effort=reasoning_effort,
                        timeout_seconds=timeout_seconds,
                        batch_size=len(shard_input["cases"]),
                        policy_path=frozen["capacity_policy"],
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
                frozen["pool"], support, case_ids=DIAGNOSTIC_CASE_IDS, permutation="base"
            )
            canary_input = build_bipartite_alignment_input(
                frozen["pool"], support, case_ids=DIAGNOSTIC_CASE_IDS, permutation="balanced_canary"
            )
            _write_immutable(root / "base-alignment-full.private.json", base_output)
            _write_immutable(root / "canary-alignment-full.private.json", canary_output)
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
                    adjudication_input=packet, adjudication_alignment=adjudication_input
                )
                schema = neutral_alignment_output_schema(adjudication_input)
                current_turn = "disagreement_adjudication"
                paths = _freeze_turn_request(
                    root=root,
                    turn_name=current_turn,
                    input_value={"adjudication_packet": packet, "alignment_input": adjudication_input},
                    prompt=prompt,
                    schema=schema,
                )
                adjudication_output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=v54_alignment_instructions(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(adjudication_input["cases"]),
                    policy_path=frozen["capacity_policy"],
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
        score["schema_version"] = V54_SCORE_VERSION
        score_path = root / "diagnostic-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V54_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v54_fresh_diagnostic_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v54_fresh_diagnostic_passed_full_calibration_authorized"
                if passed
                else "v54_fresh_diagnostic_quality_gate_not_passed"
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
            root=root, spec_path=frozen["spec_path"], turn_name=exc.turn_name, error_class=exc.error_class
        )
    except Exception as exc:
        return _write_failure_terminal(
            root=root,
            spec_path=frozen["spec_path"],
            turn_name=current_turn,
            error_class=type(exc).__name__,
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v54 fresh scoped diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v50-root", default=str(DEFAULT_V50_ROOT))
    parser.add_argument("--v51-root", default=str(DEFAULT_V51_ROOT))
    parser.add_argument("--v53-root", default=str(DEFAULT_V53_ROOT))
    parser.add_argument("--model", default="gpt-5.6-luna")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v54_diagnostic(
            output_dir=Path(args.output_dir),
            v50_root=Path(args.v50_root),
            v51_root=Path(args.v51_root),
            v53_root=Path(args.v53_root),
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
