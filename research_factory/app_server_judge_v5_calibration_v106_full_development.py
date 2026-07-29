from __future__ import annotations

"""Fresh full development calibration for the reconciled v105 judge protocol."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import (
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
    validate_pointwise_support_output,
)
from .app_server_judge_v5_calibration import (
    CALIBRATION_GATES,
    CALIBRATION_TRUTH_VERSION,
    calibration_case_shards,
    make_v5_calibration_pool,
    pointwise_input_subset,
    score_v5_calibration,
    validate_v5_calibration_truth,
)
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
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
    _validate_scoreable_alignment_output,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record_matches,
    _verify_record,
    _write_stable_created,
)
from .app_server_judge_v5_calibration_v101_reference_v8_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V101_ROOT,
)
from .app_server_judge_v5_calibration_v102_fresh_gpt55_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V102_ROOT,
)
from .app_server_judge_v5_calibration_v103_unsupported_inference_protocol import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V103_ROOT,
)
from .app_server_judge_v5_calibration_v104_side_free_adjudication import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V104_ROOT,
)
from .app_server_judge_v5_calibration_v105_reconciliation_receipt import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V105_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _validate_usage,
)
from .util import now_iso


V106_SPEC_VERSION = "pif_app_server_judge_v5_4_v106_full_spec_v1"
V106_SCORE_VERSION = "pif_app_server_judge_v5_4_v106_full_score_v1"
V106_FAILURE_VERSION = "pif_app_server_judge_v5_4_v106_failure_v1"
V106_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v106_terminal_v1"
V106_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V106_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V106_PHASE_ID = "judge_v5_4_v106_full_development_calibration"

PRIMARY_MODEL = "gpt-5.5"
ADJUDICATOR_MODEL = "gpt-5.6-sol"
EFFORT = "high"
CASES_PER_SHARD = 6
POINTWISE_TURNS = tuple(f"pointwise_support_shard_{index:02d}" for index in range(11))
BASE_TURNS = tuple(f"neutral_alignment_base_shard_{index:02d}" for index in range(11))
CANARY_TURNS = tuple(f"neutral_alignment_canary_shard_{index:02d}" for index in range(2))
ADJUDICATION_TURN = "disagreement_adjudication"
TURN_NAMES = POINTWISE_TURNS + BASE_TURNS + CANARY_TURNS + (ADJUDICATION_TURN,)
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V105_ROOT.parent / "judge-calibration-v5_4-v106-full-development"
).resolve()


class JudgeV5CalibrationV106Error(RuntimeError):
    """The v106 full calibration cannot preserve its frozen contract."""


def _sum_usage(values: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    total = {field: 0 for field in USAGE_FIELDS}
    for value in values:
        usage = value.get("usage")
        if value.get("usage_status") != "complete" or not isinstance(usage, Mapping):
            raise JudgeV5CalibrationV106Error("predecessor usage is incomplete")
        for field in USAGE_FIELDS:
            item = usage.get(field)
            if isinstance(item, bool) or not isinstance(item, int) or item < 0:
                raise JudgeV5CalibrationV106Error("predecessor usage is malformed")
            total[field] += item
    return total


def _validate_predecessors() -> dict[str, Any]:
    paths = {
        "v101_terminal": DEFAULT_V101_ROOT / "terminal.json",
        "v101_truth": DEFAULT_V101_ROOT / "calibration-truth-v8.private.json",
        "v101_receipt": DEFAULT_V101_ROOT / "reference-receipt.json",
        "v102_terminal": DEFAULT_V102_ROOT / "terminal.json",
        "v103_terminal": DEFAULT_V103_ROOT / "terminal.json",
        "v104_terminal": DEFAULT_V104_ROOT / "terminal.json",
        "v105_terminal": DEFAULT_V105_ROOT / "terminal.json",
        "v105_receipt": DEFAULT_V105_ROOT / "protocol-authorization-receipt.json",
        "v105_audit": DEFAULT_V105_ROOT / "reconciliation-audit.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t101, t105, receipt = (
        values["v101_terminal"],
        values["v105_terminal"],
        values["v105_receipt"],
    )
    if (
        t101.get("state") != "completed"
        or t101.get("reference_frozen") is not True
        or t101.get("production_mutated") is not False
        or not _record_matches(t101.get("truth"), paths["v101_truth"])
        or not _record_matches(t101.get("reference_receipt"), paths["v101_receipt"])
        or t105.get("state") != "completed"
        or t105.get("terminal_reason")
        != "v105_reconciliation_frozen_full_calibration_authorized"
        or t105.get("fresh_full_development_calibration_authorized") is not True
        or t105.get("selection_authorized") is not False
        or t105.get("holdout_authorized") is not False
        or t105.get("production_mutated") is not False
        or t105.get("usage", {}).get("total_tokens") != 0
        or not _record_matches(t105.get("protocol_receipt"), paths["v105_receipt"])
        or not _record_matches(t105.get("reconciliation_audit"), paths["v105_audit"])
        or receipt.get("state") != "frozen"
        or receipt.get("judge_model") != PRIMARY_MODEL
        or receipt.get("adjudicator_model") != ADJUDICATOR_MODEL
        or receipt.get("reference_version")
        != "fixture_reference_v8_v101_stance_inference_reconciled"
        or receipt.get("reference_change_applied") is not False
        or receipt.get("fresh_full_development_calibration_authorized") is not True
        or receipt.get("selection_authorized") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("production_mutated") is not False
    ):
        raise JudgeV5CalibrationV106Error("v101/v105 authorization contract drifted")
    development = [values["v102_terminal"], values["v103_terminal"], values["v104_terminal"]]
    for terminal in development:
        if (
            terminal.get("usage_status") != "complete"
            or terminal.get("accounting_complete") is not True
            or terminal.get("semantic_retry_count") != 0
            or terminal.get("production_mutated") is not False
        ):
            raise JudgeV5CalibrationV106Error("diagnostic usage contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "development_usage": _sum_usage(development),
    }


def _calibration_inputs(reference: Mapping[str, Any]) -> dict[str, Any]:
    pool, mapping, _legacy = make_v5_calibration_pool()
    expected = deepcopy(dict(reference))
    expected["schema_version"] = CALIBRATION_TRUTH_VERSION
    expected["fixture_reference_schema_version"] = reference.get("schema_version")
    validate_v5_calibration_truth(pool=pool, mapping=mapping, expected=expected)
    pointwise = build_pointwise_support_input(pool)
    if (
        len(pointwise.get("units") or []) != 182
        or len(expected.get("canary_case_ids") or []) != 12
        or any(
            expected["cases"][case_id]["structured_fields"][witness_id]
            != ("incorrect" if fields else "correct")
            for case_id in expected["cases"]
            for witness_id, fields in expected["cases"][case_id]["field_issues"].items()
        )
    ):
        raise JudgeV5CalibrationV106Error("reference v8 calibration projection drifted")
    return {"pool": pool, "mapping": mapping, "expected": expected, "pointwise": pointwise}


def pointwise_base_instructions_v106() -> str:
    return pointwise_support_base_instructions() + (
        " The checklist label unsupported_inference is not a stored boolean event field. Include it in "
        "field_issue_fields only when claim_text adds at least one unsupported material assertion; omit it "
        "when every material claim is source-supported. Do not invert that rule because of the label name."
    )


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path, policy_path = root / "capacity-policy-audit.json", root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V106_CAPACITY_AUDIT_VERSION,
        "phase_id": V106_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_created(audit_path, audit, "v106 capacity audit")
    policy = {
        "schema_version": V106_CAPACITY_POLICY_VERSION,
        "phase_id": V106_PHASE_ID,
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
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": math.ceil(
            bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_created(policy_path, policy, "v106 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v106(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors()
    data = _calibration_inputs(predecessor["values"]["v101_truth"])
    case_shards = calibration_case_shards(data["pool"])
    files = {
        "pool": root / "shared-witness-pool.private.json",
        "mapping": root / "witness-mapping.private.json",
        "truth": root / "calibration-truth.private.json",
        "pointwise_full": root / "pointwise-input-full.private.json",
    }
    _write_immutable(files["pool"], data["pool"])
    _write_immutable(files["mapping"], data["mapping"])
    _write_immutable(files["truth"], data["expected"])
    _write_immutable(files["pointwise_full"], data["pointwise"])
    pointwise_shards = []
    for turn_name, case_ids in zip(POINTWISE_TURNS, case_shards, strict=True):
        value = pointwise_input_subset(data["pointwise"], case_ids)
        prompt, schema = build_pointwise_support_prompt(value), pointwise_support_output_schema(value)
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
        )
        pointwise_shards.append(
            {"turn_name": turn_name, "case_ids": case_ids, "input": value, "prompt": prompt, "schema": schema, "paths": paths}
        )
    capacity = _build_capacity_policy(root, predecessor["records"])
    runtime_dir = Path(__file__).resolve().parent
    runtime_files = [
        Path(__file__),
        runtime_dir / "app_server_judge_v5_calibration_v105_reconciliation_receipt.py",
        runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration.py",
        runtime_dir / "app_server_judge_v5.py",
        runtime_dir / "app_server_judge_v5_diagnostic.py",
        runtime_dir / "app_server_capacity_reserve.py",
        runtime_dir / "codex_app_server.py",
    ]
    spec = {
        "schema_version": V106_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "protocol_version": PROTOCOL_VERSION,
        "primary_model": PRIMARY_MODEL,
        "adjudicator_model": ADJUDICATOR_MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "case_count": 66,
        "witness_count": 182,
        "cases_per_shard": CASES_PER_SHARD,
        "pointwise_shard_count": 11,
        "base_alignment_shard_count": 11,
        "canary_shard_count": 2,
        "maximum_adjudication_call_count": 1,
        "minimum_turn_count": 24,
        "maximum_turn_count": 25,
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "all_semantic_turns_fresh": True,
        "prior_model_outputs_reused": False,
        "reference_truth_exposed_to_model": False,
        "support_is_side_free": True,
        "alignment_is_origin_neutral": True,
        "unsupported_inference_semantics_corrected": True,
        "observable_disagreement_policy": "one_side_free_sol_adjudication_call_only",
        "calibration_gates": CALIBRATION_GATES,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "predecessor_development_usage": predecessor["development_usage"],
        "runtime_files": [_record(path) for path in runtime_files],
        "frozen_instructions": {
            "pointwise_base_sha256": _sha256_text(pointwise_base_instructions_v106()),
            "alignment_base_sha256": _sha256_text(neutral_alignment_base_instructions()),
        },
        "frozen_inputs": {
            **{key: _record(path) for key, path in files.items()},
            "pointwise_shards": [
                {
                    "turn_name": shard["turn_name"],
                    "case_ids": shard["case_ids"],
                    "input": _record(shard["paths"]["input"]),
                    "prompt": _record(shard["paths"]["prompt"]),
                    "schema": _record(shard["paths"]["schema"]),
                }
                for shard in pointwise_shards
            ],
        },
        "privacy": "private_source_event_prompts_outputs_truth_and_mapping_sanitized_terminal_only",
    }
    spec_path = root / "full-calibration-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v106 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV106Error("immutable v106 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "pool": data["pool"],
        "mapping": data["mapping"],
        "expected": data["expected"],
        "pointwise_input": data["pointwise"],
        "case_shards": case_shards,
        "pointwise_shards": pointwise_shards,
        "capacity_policy": capacity["policy"],
        "predecessor_development_usage": predecessor["development_usage"],
    }


def _sha256_text(value: str) -> str:
    import hashlib

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _write_failure(
    *, root: Path, turn_name: Optional[str], error_class: str
) -> dict[str, Any]:
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v106 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V106_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "attempts": attempts,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
        "known_usage_lower_bound": usage,
        "unknown_usage_turn_count": unknown,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V106_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "calibration_passed": False,
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


def _cumulative_usage(
    predecessor: Mapping[str, int], current: Mapping[str, int]
) -> dict[str, int]:
    return {field: int(predecessor[field]) + int(current[field]) for field in USAGE_FIELDS}


async def run_v106(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v106 terminal")
    frozen = freeze_v106(output_dir=root, timeout_seconds=timeout_seconds)
    factory = client_factory or _client_factory
    policy_path = frozen["capacity_policy"]
    sidecars: list[dict[str, Any]] = []
    adoptions: dict[str, bool] = {}
    current_turn: Optional[str] = None
    try:
        async with factory(policy_path) as client:
            pointwise_outputs = []
            for shard in frozen["pointwise_shards"]:
                current_turn = shard["turn_name"]
                output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=shard["paths"],
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                    base_instructions=pointwise_base_instructions_v106(),
                    model=PRIMARY_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["units"]),
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard: validate_pointwise_support_output(
                        value, item["input"]
                    ),
                )
                pointwise_outputs.append(output)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
            pointwise_output = _merge_outputs(pointwise_outputs, "units")
            if validate_pointwise_support_output(pointwise_output, frozen["pointwise_input"]):
                raise JudgeV5CalibrationV106Error("aggregate pointwise output is invalid")
            pointwise_path = root / "pointwise-output-full.private.json"
            _write_immutable(pointwise_path, pointwise_output)
            support_receipts = freeze_support_receipts(
                pointwise_output, frozen["pointwise_input"]
            )
            support_path = root / "support-receipts.private.json"
            _write_immutable(support_path, support_receipts)

            base_input = build_neutral_alignment_input(frozen["pool"], support_receipts)
            base_outputs = []
            for turn_name, case_ids in zip(BASE_TURNS, frozen["case_shards"], strict=True):
                shard_input = build_neutral_alignment_input(
                    frozen["pool"], support_receipts, case_ids=case_ids
                )
                prompt = build_neutral_alignment_prompt(shard_input)
                schema = neutral_alignment_output_schema(shard_input)
                current_turn = turn_name
                paths = _freeze_turn_request(
                    root=root,
                    turn_name=current_turn,
                    input_value=shard_input,
                    prompt=prompt,
                    schema=schema,
                )
                output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=neutral_alignment_base_instructions(),
                    model=PRIMARY_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard_input["cases"]),
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard_input: _validate_scoreable_alignment_output(
                        value, item
                    ),
                )
                base_outputs.append(output)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
            base_output = _merge_outputs(base_outputs, "cases")
            if _validate_scoreable_alignment_output(base_output, base_input):
                raise JudgeV5CalibrationV106Error("aggregate base alignment is invalid")
            base_path = root / "base-alignment-full.private.json"
            _write_immutable(base_path, base_output)

            canary_input = build_neutral_alignment_input(
                frozen["pool"],
                support_receipts,
                case_ids=frozen["expected"]["canary_case_ids"],
                permutation="balanced_canary",
            )
            canary_order = [row["case_id"] for row in canary_input["cases"]]
            canary_case_shards = [
                canary_order[index : index + CASES_PER_SHARD]
                for index in range(0, len(canary_order), CASES_PER_SHARD)
            ]
            if len(canary_case_shards) != 2 or any(len(row) != 6 for row in canary_case_shards):
                raise JudgeV5CalibrationV106Error("canary shard layout drifted")
            canary_outputs = []
            for turn_name, case_ids in zip(CANARY_TURNS, canary_case_shards, strict=True):
                shard_input = build_neutral_alignment_input(
                    frozen["pool"],
                    support_receipts,
                    case_ids=case_ids,
                    permutation="balanced_canary",
                )
                prompt = build_neutral_alignment_prompt(shard_input)
                schema = neutral_alignment_output_schema(shard_input)
                current_turn = turn_name
                paths = _freeze_turn_request(
                    root=root,
                    turn_name=current_turn,
                    input_value=shard_input,
                    prompt=prompt,
                    schema=schema,
                )
                output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=neutral_alignment_base_instructions(),
                    model=PRIMARY_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=6,
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard_input: _validate_scoreable_alignment_output(
                        value, item
                    ),
                )
                canary_outputs.append(output)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
            canary_output = _merge_outputs(canary_outputs, "cases")
            canary_path = root / "canary-alignment-full.private.json"
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
                current_turn = ADJUDICATION_TURN
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
                adjudication_output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=neutral_alignment_base_instructions(),
                    model=ADJUDICATOR_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(adjudication_input["cases"]),
                    policy_path=policy_path,
                    output_validator=lambda value: _validate_scoreable_alignment_output(
                        value, adjudication_input
                    ),
                )
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted

        reconciled = _reconcile_scoreable_alignment(
            base_output=base_output,
            base_input=base_input,
            adjudication_output=adjudication_output,
            adjudication_input=adjudication_input,
        )
        reconciled_path = root / "reconciled-alignment.private.json"
        _write_immutable(reconciled_path, reconciled)
        score = score_v5_calibration(
            pointwise_output=pointwise_output,
            reconciled_alignment=reconciled,
            expected=frozen["expected"],
            observable_disagreements=disagreements,
        )
        score["schema_version"] = V106_SCORE_VERSION
        score["raw_observable_disagreement_case_count"] = disagreements[
            "disagreement_case_count"
        ]
        score["side_free_adjudication_applied"] = adjudication_output is not None
        score_path = root / "calibration-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        cumulative = _cumulative_usage(
            frozen["predecessor_development_usage"], accounting["usage"]
        )
        terminal = {
            "schema_version": V106_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v106_full_calibration_passed_selection_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v106_full_calibration_passed_selection_authorized"
                if passed
                else "v106_full_calibration_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "calibration_passed": passed,
            "selection_authorized": passed,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "support_receipts": _record(support_path),
            "observable_disagreements": _record(disagreements_path),
            "reconciled_alignment": _record(reconciled_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": adoptions,
            "predecessor_development_usage": frozen["predecessor_development_usage"],
            "cumulative_protocol_development_usage": cumulative,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "checks": score["checks"],
            "gates": score["gates"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root=root, turn_name=exc.turn_name, error_class=exc.error_class)
    except Exception as exc:
        return _write_failure(root=root, turn_name=current_turn, error_class=type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v106 full development calibration")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v106(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "calibration_passed": terminal.get("calibration_passed", False),
                "selection_authorized": terminal.get("selection_authorized", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
