from __future__ import annotations

"""Fresh layered diagnostic on 18 retained v9 fixture cases."""

import argparse
import asyncio
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v108_layered_diagnostic as v108
from .app_server_judge_v5 import (
    adjudication_alignment_input,
    build_disagreement_adjudication_prompt,
    build_neutral_alignment_input,
    build_pointwise_support_input,
    freeze_support_receipts,
    neutral_alignment_output_schema,
)
from .app_server_judge_v5_calibration import (
    CALIBRATION_GATES,
    make_v5_calibration_pool,
    pointwise_input_subset,
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
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v118_capped_alignment_repair import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V118_ROOT,
    _iter_records,
)
from .app_server_judge_v5_calibration_v119_full_reference_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V119_ROOT,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V120_SELECTION_VERSION = "pif_app_server_judge_v5_4_v120_retained_selection_v1"
V120_TRUTH_VERSION = "pif_app_server_judge_v5_4_v120_retained_truth_v1"
V120_SPEC_VERSION = "pif_app_server_judge_v5_4_v120_retained_diagnostic_spec_v1"
V120_SCORE_VERSION = "pif_app_server_judge_v5_4_v120_retained_diagnostic_score_v1"
V120_FAILURE_VERSION = "pif_app_server_judge_v5_4_v120_failure_v1"
V120_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v120_terminal_v1"
V120_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V120_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V120_PHASE_ID = "judge_v5_4_v120_retained_case_diagnostic"

POINTWISE_MODEL = "gpt-5.6-luna"
ALIGNMENT_MODEL = "gpt-5.6-terra"
ADJUDICATOR_MODEL = "gpt-5.4"
EFFORT = "high"
CASES_PER_SHARD = 6
POINTWISE_TURNS = tuple(f"retained_pointwise_shard_{index:02d}" for index in range(3))
BASE_TURNS = tuple(f"retained_alignment_shard_{index:02d}" for index in range(3))
CANARY_TURNS = tuple(f"retained_canary_shard_{index:02d}" for index in range(2))
ADJUDICATION_TURN = "retained_disagreement_adjudication"
TURN_NAMES = POINTWISE_TURNS + BASE_TURNS + CANARY_TURNS + (ADJUDICATION_TURN,)
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V119_ROOT.parent / "judge-calibration-v5_4-v120-retained-case-diagnostic"
).resolve()


class JudgeV5CalibrationV120Error(RuntimeError):
    """The v120 retained-case diagnostic cannot preserve its frozen contract."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v119() -> dict[str, Any]:
    root = DEFAULT_V119_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "receipt": root / "reference-receipt.json",
        "truth": root / "calibration-truth-v9-full.private.json",
        "audit": root / "full-reference-projection-audit.json",
    }
    values = {name: _load_json(path, f"v119 {name}") for name, path in paths.items()}
    terminal, receipt, truth, audit = (
        values["terminal"],
        values["receipt"],
        values["truth"],
        values["audit"],
    )
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v119_full_reference_v9_frozen_full_calibration_authorized"
        or terminal.get("reference_frozen") is not True
        or terminal.get("fresh_full_calibration_authorized") is not True
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage", {}).get("total_tokens") != 0
        or receipt.get("state") != "frozen"
        or receipt.get("case_count") != 66
        or receipt.get("witness_count") != 182
        or receipt.get("canary_case_count") != 12
        or receipt.get("audited_subset_case_count") != 18
        or receipt.get("retained_v8_case_count") != 48
        or receipt.get("reference_frozen") is not True
        or receipt.get("fresh_full_calibration_authorized") is not True
        or receipt.get("selection_authorized") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("production_mutated") is not False
        or len(truth.get("cases") or {}) != 66
        or len(truth.get("canary_case_ids") or []) != 12
        or audit.get("changed_case_count") != 18
    ):
        raise JudgeV5CalibrationV120Error("v119 full reference drifted")
    if not all(_verify_record(record) for record in _iter_records(receipt)):
        raise JudgeV5CalibrationV120Error("v119 predecessor record drifted")
    for record in (terminal.get("truth"), terminal.get("projection_audit"), terminal.get("reference_receipt")):
        if not isinstance(record, Mapping) or not _verify_record(record):
            raise JudgeV5CalibrationV120Error("v119 terminal record drifted")
    pool, mapping, _ = make_v5_calibration_pool()
    validate_v5_calibration_truth(pool=pool, mapping=mapping, expected=truth)
    audited = _load_json(
        DEFAULT_V118_ROOT / "fixture-reference-v9-frozen.private.json",
        "v118 audited reference",
    )
    audited_record = receipt.get("predecessor", {}).get("v118_reference")
    if not isinstance(audited_record, Mapping) or not _verify_record(audited_record):
        raise JudgeV5CalibrationV120Error("v118 audited subset record drifted")
    if set(audited["cases"]) != set(
        case_id for case_id, case in truth["cases"].items() if case_id in audited["cases"]
    ):
        raise JudgeV5CalibrationV120Error("v118 audited subset coverage drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {
            **{name: _record(path) for name, path in paths.items()},
            "audited_subset": _record(DEFAULT_V118_ROOT / "fixture-reference-v9-frozen.private.json"),
        },
        "pool": pool,
        "mapping": mapping,
        "audited_case_ids": set(audited["cases"]),
    }


def _balanced_ids(
    candidates: Sequence[str], truth: Mapping[str, Any], count: int, salt: str
) -> list[str]:
    buckets: dict[str, list[str]] = {}
    for case_id in candidates:
        buckets.setdefault(str(truth["cases"][case_id]["shape"]), []).append(case_id)
    for shape, values in buckets.items():
        values.sort(key=lambda case_id: sha256_text(f"{salt}|{shape}|{case_id}"))
    selected = []
    shapes = sorted(buckets)
    while len(selected) < count:
        progressed = False
        for shape in shapes:
            if buckets[shape] and len(selected) < count:
                selected.append(buckets[shape].pop(0))
                progressed = True
        if not progressed:
            break
    if len(selected) != count:
        raise JudgeV5CalibrationV120Error("v120 balanced selection coverage drifted")
    return selected


def build_v120_selection(v119: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    truth = v119["values"]["truth"]
    retained = sorted(set(truth["cases"]) - v119["audited_case_ids"])
    if len(retained) != 48:
        raise JudgeV5CalibrationV120Error("v120 retained cohort drifted")
    selected = _balanced_ids(retained, truth, 18, "v120-selected")
    canary = _balanced_ids(selected, truth, 12, "v120-canary")
    selected_shapes = Counter(truth["cases"][case_id]["shape"] for case_id in selected)
    canary_shapes = Counter(truth["cases"][case_id]["shape"] for case_id in canary)
    expected = deepcopy(truth)
    expected["schema_version"] = V120_TRUTH_VERSION
    expected["cases"] = {case_id: deepcopy(truth["cases"][case_id]) for case_id in selected}
    expected["canary_case_ids"] = list(canary)
    expected["diagnostic_source"] = "v119_retained_v8_cases_excluding_all_18_audited_cases"
    selection = {
        "schema_version": V120_SELECTION_VERSION,
        "created_at": now_iso(),
        "retained_candidate_count": 48,
        "selected_case_count": 18,
        "canary_case_count": 12,
        "selected": list(selected),
        "canary": list(canary),
        "selected_shape_counts": dict(sorted(selected_shapes.items())),
        "canary_shape_counts": dict(sorted(canary_shapes.items())),
        "audited_v118_case_overlap_count": len(set(selected) & v119["audited_case_ids"]),
        "selection_uses_source_text": False,
        "selection_uses_only_reference_labels_and_provenance": True,
        "model_outputs_used_for_selection": False,
        "privacy": "opaque_case_ids_and_aggregate_label_counts_only",
    }
    return selection, expected


def _case_shards(case_ids: Sequence[str]) -> list[list[str]]:
    values = list(case_ids)
    shards = [values[index : index + CASES_PER_SHARD] for index in range(0, len(values), CASES_PER_SHARD)]
    if len(shards) != 3 or any(len(shard) != 6 for shard in shards):
        raise JudgeV5CalibrationV120Error("v120 shard coverage drifted")
    return shards


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V120_CAPACITY_AUDIT_VERSION,
        "phase_id": V120_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V120_CAPACITY_POLICY_VERSION,
        "phase_id": V120_PHASE_ID,
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
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v120(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v120 terminal")}
    v119 = _validate_v119()
    selection, expected = build_v120_selection(v119)
    pointwise_full = build_pointwise_support_input(v119["pool"])
    selected_pointwise = pointwise_input_subset(pointwise_full, selection["selected"])
    witness_count = len(selected_pointwise["units"])
    if witness_count <= 0 or witness_count >= 182:
        raise JudgeV5CalibrationV120Error("v120 selected witness coverage drifted")
    shards = _case_shards(selection["selected"])
    selection_path = root / "retained-diagnostic-selection.json"
    truth_path = root / "retained-diagnostic-truth.private.json"
    pool_path = root / "shared-witness-pool.private.json"
    pointwise_path = root / "pointwise-input-full.private.json"
    _write_stable_time(selection_path, selection, "created_at")
    _write_immutable(truth_path, expected)
    _write_immutable(pool_path, v119["pool"])
    _write_immutable(pointwise_path, selected_pointwise)
    pointwise_shards = []
    for turn_name, case_ids in zip(POINTWISE_TURNS, shards, strict=True):
        value = pointwise_input_subset(selected_pointwise, case_ids)
        prompt = v108.build_pointwise_checklist_prompt(value)
        schema = v108.pointwise_checklist_schema(value)
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=value,
            prompt=prompt,
            schema=schema,
        )
        pointwise_shards.append(
            {
                "turn_name": turn_name,
                "case_ids": case_ids,
                "input": value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    capacity = _build_capacity_policy(root, v119["records"])
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V120_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "pointwise_model": POINTWISE_MODEL,
        "alignment_model": ALIGNMENT_MODEL,
        "adjudicator_model": ADJUDICATOR_MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_luna_pointwise_terra_alignment_and_gpt54_observable_disagreement_adjudication",
        "retained_candidate_count": 48,
        "case_count": 18,
        "witness_count": witness_count,
        "canary_case_count": 12,
        "cases_per_shard": 6,
        "minimum_turn_count": 8,
        "maximum_turn_count": 9,
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "all_semantic_turns_fresh": True,
        "prior_model_outputs_reused": False,
        "reference_truth_exposed_to_model": False,
        "explicit_pointwise_15_field_checklist": True,
        "supported_equivalent_assignment_priority": True,
        "merge_split_atom_normalization": True,
        "majority_voting_used": False,
        "observable_disagreement_policy": "one_capped_side_free_gpt54_adjudication_call_only",
        "promotion_rule": "all_existing_frozen_support_field_relation_equivalence_abstention_and_order_gates",
        "calibration_gates": CALIBRATION_GATES,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": v119["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v119_full_reference_freeze.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v108_layered_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_instruction_hashes": {
            "pointwise": sha256_text(v108.pointwise_checklist_instructions()),
            "alignment": sha256_text(v108.alignment_instructions_v108()),
        },
        "frozen_inputs": {
            "selection": _record(selection_path),
            "truth": _record(truth_path),
            "pool": _record(pool_path),
            "pointwise": _record(pointwise_path),
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
        "privacy": "private_source_event_outputs_no_source_text_in_reports",
    }
    spec_path = root / "retained-diagnostic-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "selection": selection,
        "expected": expected,
        "pool": v119["pool"],
        "pointwise_full": selected_pointwise,
        "shards": shards,
        "pointwise_shards": pointwise_shards,
    }


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v120 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V120_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
        "known_usage_lower_bound": usage,
        "unknown_usage_turn_count": unknown,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V120_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "diagnostic_passed": False,
        "fresh_full_calibration_authorized": False,
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


async def run_v120(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v120 terminal")
    frozen = freeze_v120(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    sidecars = []
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            pointwise_outputs = []
            for shard in frozen["pointwise_shards"]:
                current_turn = shard["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=shard["paths"],
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                    base_instructions=v108.pointwise_checklist_instructions(),
                    model=POINTWISE_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["units"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=shard["input"]: v108.validate_pointwise_checklist_output(candidate, item),
                )
                pointwise_outputs.append(output)
                sidecars.append(sidecar)
            checklist = _merge_outputs(pointwise_outputs, "units")
            errors = v108.validate_pointwise_checklist_output(checklist, frozen["pointwise_full"])
            if errors:
                raise JudgeV5CalibrationV120Error("aggregate pointwise checklist invalid")
            checklist_path = root / "pointwise-checklist-full.private.json"
            _write_immutable(checklist_path, checklist)
            pointwise = v108.project_pointwise_checklist(checklist)
            pointwise_path = root / "pointwise-output-full.private.json"
            _write_immutable(pointwise_path, pointwise)
            support = freeze_support_receipts(pointwise, frozen["pointwise_full"])
            support_path = root / "support-receipts.private.json"
            _write_immutable(support_path, support)

            base_input = build_neutral_alignment_input(
                frozen["pool"], support, case_ids=frozen["selection"]["selected"]
            )
            base_outputs = []
            for turn_name, case_ids in zip(BASE_TURNS, frozen["shards"], strict=True):
                shard_input = build_neutral_alignment_input(
                    frozen["pool"], support, case_ids=case_ids
                )
                prompt = v108.build_alignment_prompt_v108(shard_input)
                schema = neutral_alignment_output_schema(shard_input)
                current_turn = turn_name
                paths = _freeze_turn_request(
                    root=root,
                    turn_name=current_turn,
                    input_value=shard_input,
                    prompt=prompt,
                    schema=schema,
                )
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=v108.alignment_instructions_v108(),
                    model=ALIGNMENT_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=6,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=shard_input: _validate_scoreable_alignment_output(candidate, item),
                )
                base_outputs.append(output)
                sidecars.append(sidecar)
            base_output = _merge_outputs(base_outputs, "cases")
            if _validate_scoreable_alignment_output(base_output, base_input):
                raise JudgeV5CalibrationV120Error("aggregate base alignment invalid")
            base_path = root / "base-alignment-full.private.json"
            _write_immutable(base_path, base_output)

            canary_input = build_neutral_alignment_input(
                frozen["pool"],
                support,
                case_ids=frozen["selection"]["canary"],
                permutation="balanced_canary",
            )
            canary_order = [str(row["case_id"]) for row in canary_input["cases"]]
            canary_shards = [canary_order[:6], canary_order[6:]]
            canary_outputs = []
            for turn_name, case_ids in zip(CANARY_TURNS, canary_shards, strict=True):
                shard_input = build_neutral_alignment_input(
                    frozen["pool"],
                    support,
                    case_ids=case_ids,
                    permutation="balanced_canary",
                )
                prompt = v108.build_alignment_prompt_v108(shard_input)
                schema = neutral_alignment_output_schema(shard_input)
                current_turn = turn_name
                paths = _freeze_turn_request(
                    root=root,
                    turn_name=current_turn,
                    input_value=shard_input,
                    prompt=prompt,
                    schema=schema,
                )
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=v108.alignment_instructions_v108(),
                    model=ALIGNMENT_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=6,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=shard_input: _validate_scoreable_alignment_output(candidate, item),
                )
                canary_outputs.append(output)
                sidecars.append(sidecar)
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
            adjudication_output = adjudication_input = None
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
                adjudication_output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=v108.alignment_instructions_v108(),
                    model=ADJUDICATOR_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(adjudication_input["cases"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate: _validate_scoreable_alignment_output(candidate, adjudication_input),
                )
                sidecars.append(sidecar)
        reconciled = _reconcile_scoreable_alignment(
            base_output=base_output,
            base_input=base_input,
            adjudication_output=adjudication_output,
            adjudication_input=adjudication_input,
        )
        reconciled_path = root / "reconciled-alignment.private.json"
        _write_immutable(reconciled_path, reconciled)
        score = v108.score_v108(
            pointwise_output=pointwise,
            reconciled_alignment=reconciled,
            expected=frozen["expected"],
            observable_disagreements=disagreements,
        )
        score["schema_version"] = V120_SCORE_VERSION
        score_path = root / "retained-diagnostic-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V120_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v120_retained_diagnostic_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v120_retained_case_diagnostic_passed"
                if passed
                else "v120_retained_case_diagnostic_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "diagnostic_passed": passed,
            "fresh_full_calibration_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "support_receipts": _record(support_path),
            "observable_disagreements": _record(disagreements_path),
            "reconciled_alignment": _record(reconciled_path),
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "checks": score["checks"],
            "attempts": _attempt_records(root),
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v120 retained-case diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v120(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "diagnostic_passed": terminal.get("diagnostic_passed", False),
                "fresh_full_calibration_authorized": terminal.get(
                    "fresh_full_calibration_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
