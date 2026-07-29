from __future__ import annotations

"""Versioned v26 staged diagnostic after the v25 repair diagnostic failure."""

import argparse
import asyncio
import json
import math
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_capacity_reserve import (
    RESERVE_CAPACITY_CHECKPOINT_VERSION,
    ReserveCapacityGatedCodexAppServerClient,
)
from .app_server_judge_v5 import (
    ADJUDICATION_INPUT_VERSION,
    ALIGNMENT_OUTPUT_VERSION,
    CHECKLIST_FIELDS,
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
    project_mismatch_fields,
    validate_neutral_alignment_output,
    validate_pointwise_support_output,
)
from .app_server_judge_v5_calibration import CALIBRATION_GATES, pointwise_input_subset
from .app_server_judge_v5_calibration_v25 import (
    DIAGNOSTIC_CASE_IDS,
)
from .app_server_judge_v5_calibration_v25_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V25_ROOT,
    DEFAULT_V26_PRESEMANTIC_DESIGN_ROOT,
    PINNED_CODEX_0_144_1,
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _canonical_json,
    _record,
    _sha256_file,
    _turn_paths,
    _validate_usage,
    _write_immutable_json,
    _write_immutable_text,
)
from .codex_app_server import APP_SERVER_CLIENT_VERSION, CodexAppServerClient
from .util import now_iso, sha256_text


V26_DIAGNOSTIC_SPEC_VERSION = "pif_app_server_judge_v5_4_v26_staged_diagnostic_spec_v1"
V26_DIAGNOSTIC_SCORE_VERSION = "pif_app_server_judge_v5_4_v26_staged_diagnostic_score_v1"
V26_DIAGNOSTIC_TERMINAL_VERSION = (
    "pif_app_server_judge_v5_4_v26_staged_diagnostic_terminal_v1"
)
V26_DIAGNOSTIC_FAILURE_VERSION = (
    "pif_app_server_judge_v5_4_v26_staged_diagnostic_failure_v1"
)
V26_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V26_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"

DEFAULT_PIPELINE_ROOT = Path("work/app-server-development-v2/unattended-pipeline-v5").resolve()
DEFAULT_V23_ROOT = (
    DEFAULT_PIPELINE_ROOT
    / "judge-calibration-v5_4-reference-v2-capacity-v23/fresh-attempt"
)
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v26-staged-diagnostic"
).resolve()
MAX_PROMPT_BYTES = 96 * 1024
MAX_SCHEMA_BYTES = 64 * 1024
MAX_TOKENS_PER_TURN = 70_000
CASES_PER_SHARD = 6


class JudgeV5CalibrationV26DiagnosticError(RuntimeError):
    """The v26 diagnostic cannot continue without violating its frozen contract."""


class JudgeV5CalibrationV26DiagnosticAttemptFailed(
    JudgeV5CalibrationV26DiagnosticError
):
    def __init__(self, *, turn_name: Optional[str], error_class: str) -> None:
        self.turn_name = turn_name
        self.error_class = error_class
        super().__init__(f"{turn_name or 'unknown_turn'} failed: {error_class}")


def _load_json(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JudgeV5CalibrationV26DiagnosticError(
            f"{purpose} is missing or invalid"
        ) from exc
    if not isinstance(value, dict):
        raise JudgeV5CalibrationV26DiagnosticError(f"{purpose} is not an object")
    return value


def _write_immutable(path: Path, value: Any) -> None:
    rendered = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    target = path.expanduser().resolve()
    if target.exists():
        if target.read_text(encoding="utf-8") != rendered:
            raise JudgeV5CalibrationV26DiagnosticError(
                f"immutable v26 artifact drifted: {target}"
            )
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    _write_immutable_text(target, rendered)


def _turn_names() -> list[str]:
    names: list[str] = []
    for stage in ("pointwise_support", "neutral_alignment_base", "neutral_alignment_canary"):
        for index in range(3):
            names.append(f"{stage}_shard_{index:02d}")
    names.append("disagreement_adjudication")
    return names


def _shards() -> list[list[str]]:
    ids = list(DIAGNOSTIC_CASE_IDS)
    shards = [ids[index : index + CASES_PER_SHARD] for index in range(0, len(ids), CASES_PER_SHARD)]
    if len(shards) != 3 or any(len(shard) != CASES_PER_SHARD for shard in shards):
        raise JudgeV5CalibrationV26DiagnosticError("v26 diagnostic shard layout drifted")
    return shards


def _subset_pool(pool: Mapping[str, Any], case_ids: Sequence[str]) -> dict[str, Any]:
    wanted = set(case_ids)
    cases = [deepcopy(case) for case in pool["cases"] if case["case_id"] in wanted]
    if {case["case_id"] for case in cases} != wanted:
        raise JudgeV5CalibrationV26DiagnosticError("v26 pool subset coverage drifted")
    return {
        key: deepcopy(value)
        for key, value in pool.items()
        if key != "cases"
    } | {"cases": cases}


def _subset_truth(truth: Mapping[str, Any], case_ids: Sequence[str]) -> dict[str, Any]:
    cases = {
        case_id: deepcopy(truth["cases"][case_id])
        for case_id in case_ids
        if case_id in truth["cases"]
    }
    if set(cases) != set(case_ids):
        raise JudgeV5CalibrationV26DiagnosticError("v26 truth subset coverage drifted")
    witness_count = sum(len(row["proposition"]) for row in cases.values())
    return {
        **{key: deepcopy(value) for key, value in truth.items() if key != "cases"},
        "case_count": len(cases),
        "witness_count": witness_count,
        "canary_case_ids": list(case_ids),
        "cases": cases,
    }


def _freeze_turn_request(
    *,
    root: Path,
    turn_name: str,
    input_value: Mapping[str, Any],
    prompt: str,
    schema: Mapping[str, Any],
) -> dict[str, Path]:
    if len(prompt.encode("utf-8")) > MAX_PROMPT_BYTES:
        raise JudgeV5CalibrationV26DiagnosticError("v26 prompt exceeds byte cap")
    if len(_canonical_json(schema).encode("utf-8")) > MAX_SCHEMA_BYTES:
        raise JudgeV5CalibrationV26DiagnosticError("v26 schema exceeds byte cap")
    paths = _turn_paths(root, turn_name)
    _write_immutable_json(paths["input"], input_value)
    _write_immutable_text(paths["prompt"], prompt)
    _write_immutable_json(paths["schema"], schema)
    return paths


def _build_capacity_policy(root: Path, design_root: Path) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    design_terminal = design_root / "terminal.json"
    turn_names = _turn_names()
    audit = {
        "schema_version": V26_CAPACITY_AUDIT_VERSION,
        "phase_id": "judge_v5_4_v26_staged_diagnostic",
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v26_presemantic_terminal": _record(design_terminal),
        "v25_non_acceptance": _record(DEFAULT_V25_ROOT / "non-acceptance.json"),
        "measured_basis": {
            "v25_diagnostic_total_tokens": 265849,
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": len(turn_names) * MAX_TOKENS_PER_TURN,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v26 capacity audit")
        stable = deepcopy(audit)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV26DiagnosticError("immutable v26 capacity audit drifted")
        audit = prior
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V26_CAPACITY_POLICY_VERSION,
        "phase_id": "judge_v5_4_v26_staged_diagnostic",
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
        prior = _load_json(policy_path, "v26 capacity policy")
        stable = deepcopy(policy)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV26DiagnosticError("immutable v26 capacity policy drifted")
        policy = prior
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v26_staged_diagnostic(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v23_root: Path = DEFAULT_V23_ROOT,
    design_root: Path = DEFAULT_V26_PRESEMANTIC_DESIGN_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    design_terminal = _load_json(design_root / "terminal.json", "v26 presemantic terminal")
    if (
        design_terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or design_terminal.get("semantic_attempt_authorized") is not False
        or design_terminal.get("production_mutated") is not False
    ):
        raise JudgeV5CalibrationV26DiagnosticError("v26 design terminal is not admissible")
    pool = _subset_pool(
        _load_json(v23_root / "shared-witness-pool.private.json", "v23 witness pool"),
        DIAGNOSTIC_CASE_IDS,
    )
    truth = _subset_truth(
        _load_json(v23_root / "calibration-truth.private.json", "v23 truth"),
        DIAGNOSTIC_CASE_IDS,
    )
    pool_path = root / "shared-witness-pool.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    _write_immutable(pool_path, pool)
    _write_immutable(truth_path, truth)
    pointwise_input = build_pointwise_support_input(pool)
    pointwise_full_path = root / "pointwise-input-full.private.json"
    _write_immutable(pointwise_full_path, pointwise_input)
    pointwise_shards = []
    for index, case_ids in enumerate(_shards()):
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
    capacity = _build_capacity_policy(root, design_root.expanduser().resolve())
    spec = {
        "schema_version": V26_DIAGNOSTIC_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "protocol_version": PROTOCOL_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "staged_pointwise_support_then_alignment",
        "case_count": len(DIAGNOSTIC_CASE_IDS),
        "witness_count": len(pointwise_input["units"]),
        "turn_plan": _turn_names(),
        "retry_count_per_turn": 0,
        "semantic_model_calls_performed_during_freeze": 0,
        "full_calibration_authorized_before_diagnostic_pass": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "v26_presemantic_terminal": _record(design_root / "terminal.json"),
        "v25_non_acceptance": _record(DEFAULT_V25_ROOT / "non-acceptance.json"),
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
    spec_path = root / "staged-diagnostic-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v26 staged diagnostic spec")
        stable = deepcopy(spec)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV26DiagnosticError("immutable v26 spec drifted")
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


def _validate_capacity_checkpoint(path: Path, policy_path: Path) -> None:
    value = _load_json(path, "v26 reserve capacity checkpoint")
    if (
        value.get("schema_version") != RESERVE_CAPACITY_CHECKPOINT_VERSION
        or value.get("policy_sha256") != _sha256_file(policy_path)
        or value.get("cleared_for_semantic_turn") is not True
        or value.get("managed_chatgpt_auth_verified") is not True
        or value.get("rate_limit_reached_type") is not None
        or value.get("retry_checkpoint_reuse_allowed") is not False
    ):
        raise JudgeV5CalibrationV26DiagnosticError("v26 reserve capacity checkpoint drifted")


def _checkpoint_state(paths: Mapping[str, Path]) -> str:
    present = {key: paths[key].exists() for key in ("output", "sidecar", "capacity")}
    if not any(present.values()):
        return "absent"
    if all(present.values()):
        sidecar = _load_json(paths["sidecar"], "v26 existing sidecar")
        return "completed" if sidecar.get("state") == "completed" else "terminal_noncomplete"
    return "partial"


def _validate_completed_turn(
    *,
    paths: Mapping[str, Path],
    prompt: str,
    schema: Mapping[str, Any],
    base_instructions: str,
    model: str,
    effort: str,
    policy_path: Path,
    output_validator: Callable[[Any], Sequence[str]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    output = _load_json(paths["output"], "v26 turn output")
    sidecar = _load_json(paths["sidecar"], "v26 turn sidecar")
    _validate_capacity_checkpoint(paths["capacity"], policy_path)
    required = {
        "state": "completed",
        "status": "completed",
        "client_version": APP_SERVER_CLIENT_VERSION,
        "transport": "stdio",
        "auth_type": "chatgpt",
        "thread_mode": "new_thread",
        "model": model,
        "effort": effort,
        "prompt_sha256": sha256_text(prompt),
        "base_instructions_sha256": sha256_text(base_instructions),
        "output_schema_sha256": sha256_text(_canonical_json(schema)),
    }
    for key, expected in required.items():
        if sidecar.get(key) != expected:
            raise JudgeV5CalibrationV26DiagnosticError(f"v26 sidecar mismatch: {key}")
    _validate_usage(sidecar)
    output_text = paths["output"].read_text(encoding="utf-8")
    acceptable = {sha256_text(output_text)}
    if output_text.endswith("\n"):
        acceptable.add(sha256_text(output_text[:-1]))
    if sidecar.get("output_sha256") not in acceptable:
        raise JudgeV5CalibrationV26DiagnosticError("v26 output hash mismatch")
    errors = list(output_validator(output))
    if errors:
        raise JudgeV5CalibrationV26DiagnosticError(
            "v26 output invalid: " + "; ".join(errors)
        )
    return output, sidecar


async def _get_or_run_turn(
    *,
    client: Any,
    turn_name: str,
    paths: Mapping[str, Path],
    prompt: str,
    schema: Mapping[str, Any],
    base_instructions: str,
    model: str,
    effort: str,
    timeout_seconds: float,
    batch_size: int,
    policy_path: Path,
    output_validator: Callable[[Any], Sequence[str]],
) -> tuple[dict[str, Any], dict[str, Any], bool]:
    state = _checkpoint_state(paths)
    if state == "completed":
        try:
            output, sidecar = _validate_completed_turn(
                paths=paths,
                prompt=prompt,
                schema=schema,
                base_instructions=base_instructions,
                model=model,
                effort=effort,
                policy_path=policy_path,
                output_validator=output_validator,
            )
        except Exception as exc:
            raise JudgeV5CalibrationV26DiagnosticAttemptFailed(
                turn_name=turn_name, error_class=type(exc).__name__
            ) from exc
        return output, sidecar, True
    if state != "absent":
        raise JudgeV5CalibrationV26DiagnosticAttemptFailed(
            turn_name=turn_name, error_class=f"immutable_{state}_checkpoint"
        )
    try:
        result = await client.run_ephemeral_structured_turn(
            model=model,
            effort=effort,
            base_instructions=base_instructions,
            prompt=prompt,
            output_schema=dict(schema),
            cwd=Path.cwd(),
            sidecar_path=paths["sidecar"],
            capacity_checkpoint_path=paths["capacity"],
            output_path=paths["output"],
            batch_size=batch_size,
            thread_mode="new_thread",
            timeout_seconds=timeout_seconds,
        )
    except Exception as exc:
        raise JudgeV5CalibrationV26DiagnosticAttemptFailed(
            turn_name=turn_name, error_class=type(exc).__name__
        ) from exc
    if not result.status_ok or not isinstance(result.output, dict):
        raise JudgeV5CalibrationV26DiagnosticAttemptFailed(
            turn_name=turn_name,
            error_class=str(result.error_class or result.status or "turn_failed"),
        )
    return _validate_completed_turn(
        paths=paths,
        prompt=prompt,
        schema=schema,
        base_instructions=base_instructions,
        model=model,
        effort=effort,
        policy_path=policy_path,
        output_validator=output_validator,
    ) + (False,)


def _merge_outputs(outputs: Sequence[Mapping[str, Any]], key: str) -> dict[str, Any]:
    rows = [row for output in outputs for row in output[key]]
    ids = [(str(row["case_id"]), str(row.get("witness_id") or "")) for row in rows]
    if len(ids) != len(set(ids)):
        raise JudgeV5CalibrationV26DiagnosticError("v26 shard outputs overlap")
    return {key: rows}


def _validate_scoreable_alignment_output(
    output: Any, alignment_input: Mapping[str, Any]
) -> list[str]:
    return [
        error
        for error in validate_neutral_alignment_output(output, alignment_input)
        if not error.endswith("_unsupported_inference_support_inconsistent")
        and not error.endswith("_unsupported_without_specific_root")
    ]


def _normalize_scoreable_alignment_output(
    output: Mapping[str, Any], alignment_input: Mapping[str, Any]
) -> dict[str, Any]:
    errors = _validate_scoreable_alignment_output(output, alignment_input)
    if errors:
        raise JudgeV5CalibrationV26DiagnosticError(
            "invalid scoreable alignment output: " + "; ".join(errors)
        )
    normalized_cases = []
    for row in output["cases"]:
        pairs = []
        for pair in row["alignment_pairs"]:
            ids = sorted((pair["witness_id_1"], pair["witness_id_2"]))
            checklist_by_field = {item["field"]: item for item in pair["checklist"]}
            pairs.append(
                {
                    "witness_ids": ids,
                    "relation": pair["relation"],
                    "mismatch_fields": project_mismatch_fields(pair["checklist"]),
                    "checklist_decisions": {
                        field: checklist_by_field[field]["decision"]
                        for field in CHECKLIST_FIELDS
                    },
                }
            )
        normalized_cases.append(
            {
                "case_id": row["case_id"],
                "equivalence_groups": sorted(
                    (sorted(group["witness_ids"]) for group in row["equivalence_groups"]),
                    key=lambda values: tuple(values),
                ),
                "alignment_pairs": sorted(pairs, key=lambda item: tuple(item["witness_ids"])),
                "unpaired_witness_ids": sorted(row["unpaired_witness_ids"]),
            }
        )
    return {
        "schema_version": ALIGNMENT_OUTPUT_VERSION,
        "cases": sorted(normalized_cases, key=lambda item: item["case_id"]),
        "mismatch_fields_projected_from_checklists": True,
        "origin_neutral": True,
    }


def _find_scoreable_disagreements(
    *,
    base_output: Mapping[str, Any],
    base_input: Mapping[str, Any],
    canary_output: Mapping[str, Any],
    canary_input: Mapping[str, Any],
) -> dict[str, Any]:
    base = _normalize_scoreable_alignment_output(base_output, base_input)
    canary = _normalize_scoreable_alignment_output(canary_output, canary_input)
    base_by_case = {item["case_id"]: item for item in base["cases"]}
    canary_by_case = {item["case_id"]: item for item in canary["cases"]}
    disagreements = [
        {"case_id": case_id, "reasons": ["permutation_output_changed"]}
        for case_id, canary_case in sorted(canary_by_case.items())
        if base_by_case.get(case_id) != canary_case
    ]
    return {
        "disagreement_case_count": len(disagreements),
        "disagreements": disagreements,
        "adjudication_required": bool(disagreements),
        "adjudication_call_cap": 1,
        "majority_voting_used": False,
    }


def _build_scoreable_adjudication_input(
    *,
    base_input: Mapping[str, Any],
    base_output: Mapping[str, Any],
    canary_input: Mapping[str, Any],
    canary_output: Mapping[str, Any],
) -> dict[str, Any]:
    disagreement = _find_scoreable_disagreements(
        base_output=base_output,
        base_input=base_input,
        canary_output=canary_output,
        canary_input=canary_input,
    )
    if not disagreement["disagreements"]:
        return {
            "schema_version": ADJUDICATION_INPUT_VERSION,
            "cases": [],
            "adjudication_required": False,
            "call_cap": 1,
        }
    base_cases = {item["case_id"]: item for item in base_input["cases"]}
    normalized_base = {
        item["case_id"]: item
        for item in _normalize_scoreable_alignment_output(base_output, base_input)["cases"]
    }
    normalized_canary = {
        item["case_id"]: item
        for item in _normalize_scoreable_alignment_output(canary_output, canary_input)["cases"]
    }
    cases = []
    for item in disagreement["disagreements"]:
        case_id = item["case_id"]
        cases.append(
            {
                **deepcopy(base_cases[case_id]),
                "observed_disagreement_reasons": item["reasons"],
                "anonymous_candidate_1": normalized_base[case_id],
                "anonymous_candidate_2": normalized_canary[case_id],
            }
        )
    return {
        "schema_version": ADJUDICATION_INPUT_VERSION,
        "cases": cases,
        "adjudication_required": True,
        "call_cap": 1,
        "candidate_order_has_no_vote_meaning": True,
    }


def _reconcile_scoreable_alignment(
    *,
    base_output: Mapping[str, Any],
    base_input: Mapping[str, Any],
    adjudication_output: Optional[Mapping[str, Any]],
    adjudication_input: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    base = _normalize_scoreable_alignment_output(base_output, base_input)
    if adjudication_output is None or adjudication_input is None:
        return base
    adjudicated = _normalize_scoreable_alignment_output(
        adjudication_output, adjudication_input
    )
    by_case = {item["case_id"]: item for item in base["cases"]}
    for item in adjudicated["cases"]:
        by_case[item["case_id"]] = item
    return {
        **base,
        "cases": sorted(by_case.values(), key=lambda item: item["case_id"]),
        "adjudication_applied": True,
    }


def _score_subset(
    *,
    pointwise_output: Mapping[str, Any],
    reconciled_alignment: Mapping[str, Any],
    expected: Mapping[str, Any],
    observable_disagreements: Mapping[str, Any],
) -> dict[str, Any]:
    expected_cases = expected["cases"]
    pointwise_rows = {str(row["witness_id"]): row for row in pointwise_output["units"]}
    expected_witnesses = {
        witness_id for truth in expected_cases.values() for witness_id in truth["proposition"]
    }
    if set(pointwise_rows) != expected_witnesses:
        raise JudgeV5CalibrationV26DiagnosticError("v26 pointwise output coverage drifted")
    support_tp = support_fn = support_tn = support_fp = 0
    structured_correct = structured_total = 0
    pointwise_field_tp = pointwise_field_fp = pointwise_field_fn = 0
    abstentions = 0
    support_by_shape: dict[str, Counter[str]] = defaultdict(Counter)
    for truth in expected_cases.values():
        for witness_id, expected_verdict in truth["proposition"].items():
            row = pointwise_rows[witness_id]
            observed = row["proposition_verdict"]
            if observed == "abstain":
                abstentions += 1
            if expected_verdict == "supported" and observed == "supported":
                support_tp += 1
                support_by_shape[truth["shape"]]["tp"] += 1
            elif expected_verdict == "supported":
                support_fn += 1
                support_by_shape[truth["shape"]]["fn"] += 1
            elif observed == "unsupported":
                support_tn += 1
                support_by_shape[truth["shape"]]["tn"] += 1
            else:
                support_fp += 1
                support_by_shape[truth["shape"]]["fp"] += 1
            structured_total += 1
            structured_correct += int(
                row["structured_field_verdict"] == truth["structured_fields"][witness_id]
            )
            wanted = set(truth["field_issues"][witness_id])
            observed_issues = set(row["field_issue_fields"])
            pointwise_field_tp += len(wanted & observed_issues)
            pointwise_field_fp += len(observed_issues - wanted)
            pointwise_field_fn += len(wanted - observed_issues)
    alignment_rows = {str(row["case_id"]): row for row in reconciled_alignment["cases"]}
    if set(alignment_rows) != set(expected_cases):
        raise JudgeV5CalibrationV26DiagnosticError("v26 alignment coverage drifted")
    alignment_tp = alignment_fp = alignment_fn = 0
    relation_correct = relation_total = 0
    field_tp = field_fp = field_fn = 0
    equivalent_tp = equivalent_fn = equivalent_tn = equivalent_fp = 0
    partition_exact = unpaired_exact = 0
    for case_id, truth in expected_cases.items():
        row = alignment_rows[case_id]
        observed_pairs = {
            tuple(sorted(pair["witness_ids"])): pair
            for pair in row.get("alignment_pairs") or []
        }
        expected_pairs = {
            tuple(sorted(pair["witness_ids"])): pair for pair in truth["pairs"]
        }
        alignment_tp += len(set(observed_pairs) & set(expected_pairs))
        alignment_fp += len(set(observed_pairs) - set(expected_pairs))
        alignment_fn += len(set(expected_pairs) - set(observed_pairs))
        for pair_ids, expected_pair in expected_pairs.items():
            observed_pair = observed_pairs.get(pair_ids)
            observed_relation = observed_pair.get("relation") if observed_pair else None
            if observed_relation == "abstain":
                abstentions += 1
            relation_total += 1
            relation_correct += int(observed_relation == expected_pair["relation"])
            expected_equivalent = expected_pair["relation"] == "equivalent"
            if expected_equivalent and observed_relation == "equivalent":
                equivalent_tp += 1
            elif expected_equivalent:
                equivalent_fn += 1
            elif observed_relation == "equivalent" or observed_relation in {None, "abstain"}:
                equivalent_fp += 1
            else:
                equivalent_tn += 1
            wanted = set(expected_pair["mismatch_fields"])
            observed_fields = set(observed_pair.get("mismatch_fields") if observed_pair else [])
            field_tp += len(wanted & observed_fields)
            field_fp += len(observed_fields - wanted)
            field_fn += len(wanted - observed_fields)
        observed_partition = {
            tuple(sorted(group)) for group in row.get("equivalence_groups") or []
        }
        expected_partition = {tuple(sorted(group)) for group in truth["equivalence_groups"]}
        partition_exact += int(observed_partition == expected_partition)
        unpaired_exact += int(sorted(row.get("unpaired_witness_ids") or []) == truth["unpaired_witness_ids"])
    metrics = {
        "case_count": len(expected_cases),
        "witness_count": structured_total,
        "support_sensitivity": _ratio(support_tp, support_tp + support_fn),
        "support_specificity": _ratio(support_tn, support_tn + support_fp),
        "structured_field_accuracy": _ratio(structured_correct, structured_total),
        "pointwise_field_issue_f1": _f1(pointwise_field_tp, pointwise_field_fp, pointwise_field_fn),
        "alignment_f1": _f1(alignment_tp, alignment_fp, alignment_fn),
        "equivalent_sensitivity": _ratio(equivalent_tp, equivalent_tp + equivalent_fn),
        "equivalent_specificity": _ratio(equivalent_tn, equivalent_tn + equivalent_fp),
        "field_diagnostic_f1": _f1(field_tp, field_fp, field_fn),
        "relation_accuracy": _ratio(relation_correct, relation_total),
        "equivalence_partition_exact_case_rate": _ratio(partition_exact, len(expected_cases)),
        "unpaired_exact_case_rate": _ratio(unpaired_exact, len(expected_cases)),
        "abstention_count": abstentions,
        "abstention_rate": _ratio(abstentions, structured_total + relation_total + len(expected_cases)),
        "order_bias": _ratio(
            int(observable_disagreements.get("disagreement_case_count") or 0),
            len(expected_cases),
        ),
    }
    checks = {
        "support_sensitivity": metrics["support_sensitivity"] >= CALIBRATION_GATES["support_sensitivity_min"],
        "support_specificity": metrics["support_specificity"] >= CALIBRATION_GATES["support_specificity_min"],
        "structured_field_accuracy": metrics["structured_field_accuracy"] >= CALIBRATION_GATES["structured_field_accuracy_min"],
        "alignment_f1": metrics["alignment_f1"] >= CALIBRATION_GATES["alignment_f1_min"],
        "equivalent_sensitivity": metrics["equivalent_sensitivity"] >= CALIBRATION_GATES["equivalent_sensitivity_min"],
        "equivalent_specificity": metrics["equivalent_specificity"] >= CALIBRATION_GATES["equivalent_specificity_min"],
        "field_diagnostic_f1": metrics["field_diagnostic_f1"] >= CALIBRATION_GATES["field_diagnostic_f1_min"],
        "relation_accuracy": metrics["relation_accuracy"] >= CALIBRATION_GATES["relation_accuracy_min"],
        "equivalence_partition_exact_case_rate": metrics["equivalence_partition_exact_case_rate"] >= CALIBRATION_GATES["equivalence_partition_exact_case_rate_min"],
        "unpaired_exact_case_rate": metrics["unpaired_exact_case_rate"] >= CALIBRATION_GATES["unpaired_exact_case_rate_min"],
        "abstention_rate": metrics["abstention_rate"] <= CALIBRATION_GATES["abstention_rate_max"],
        "order_bias": metrics["order_bias"] <= CALIBRATION_GATES["order_bias_max"],
    }
    return {
        "schema_version": V26_DIAGNOSTIC_SCORE_VERSION,
        "passed": all(checks.values()),
        "metrics": metrics,
        "checks": checks,
        "failed_checks": sorted(key for key, passed in checks.items() if not passed),
        "gates": CALIBRATION_GATES,
        "support_by_shape": {shape: dict(sorted(counts.items())) for shape, counts in sorted(support_by_shape.items())},
        "subset_minimum_cases_gate_not_applicable": True,
    }


def _ratio(num: int, den: int) -> float:
    return round(num / den, 6) if den else 0.0


def _f1(tp: int, fp: int, fn: int) -> float:
    den = (2 * tp) + fp + fn
    return round((2 * tp) / den, 6) if den else 0.0


def _aggregate_usage(sidecars: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total = {field: 0 for field in USAGE_FIELDS}
    for sidecar in sidecars:
        usage = _validate_usage(sidecar)
        for field in USAGE_FIELDS:
            total[field] += usage[field]
    return {
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": total,
        "turn_count": len(sidecars),
        "wall_elapsed_seconds_sum": round(
            sum(float(sidecar.get("wall_elapsed_seconds") or 0.0) for sidecar in sidecars),
            3,
        ),
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
        sidecar = _load_json(Path(sidecar_record["path"]), "v26 failed sidecar")
        try:
            usage = _validate_usage(sidecar)
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    accounting_complete = not partial and unknown == 0
    failure = {
        "schema_version": V26_DIAGNOSTIC_FAILURE_VERSION,
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
        "schema_version": V26_DIAGNOSTIC_TERMINAL_VERSION,
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


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    def inner() -> CodexAppServerClient:
        return CodexAppServerClient(
            command=[str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"]
        )

    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=inner
    )


async def run_v26_staged_diagnostic(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v23_root: Path = DEFAULT_V23_ROOT,
    design_root: Path = DEFAULT_V26_PRESEMANTIC_DESIGN_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v26 terminal")
    frozen = freeze_v26_staged_diagnostic(
        output_dir=root,
        v23_root=v23_root,
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
                    base_instructions=pointwise_support_base_instructions(),
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
            for index, case_ids in enumerate(_shards()):
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
                case_ids=DIAGNOSTIC_CASE_IDS,
                permutation="balanced_canary",
            )
            canary_outputs = []
            canary_case_order = [case["case_id"] for case in canary_input["cases"]]
            canary_shards = [
                canary_case_order[index : index + CASES_PER_SHARD]
                for index in range(0, len(canary_case_order), CASES_PER_SHARD)
            ]
            for index, case_ids in enumerate(canary_shards):
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
                    base_instructions=neutral_alignment_base_instructions(),
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
        score = _score_subset(
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
            "schema_version": V26_DIAGNOSTIC_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v26_staged_diagnostic_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v26_staged_diagnostic_passed_full_calibration_authorized"
                if passed
                else "v26_staged_diagnostic_quality_gate_not_passed"
            ),
            "babysitter_status": (
                "v26_staged_diagnostic_passed_full_calibration_authorized"
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
    parser = argparse.ArgumentParser(description="Run v26 staged judge diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v23-root", default=str(DEFAULT_V23_ROOT))
    parser.add_argument("--design-root", default=str(DEFAULT_V26_PRESEMANTIC_DESIGN_ROOT))
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v26_staged_diagnostic(
            output_dir=Path(args.output_dir),
            v23_root=Path(args.v23_root),
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
