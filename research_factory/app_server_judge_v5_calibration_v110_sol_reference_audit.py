from __future__ import annotations

"""Independent side-free Sol reference audit for v109 disagreements."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v108_layered_diagnostic as v108
from .app_server_judge_v5 import (
    build_neutral_alignment_input,
    freeze_support_receipts,
    neutral_alignment_output_schema,
)
from .app_server_judge_v5_calibration import pointwise_input_subset
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_TOKENS_PER_TURN,
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _merge_outputs,
    _validate_scoreable_alignment_output,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _record_matches,
    _verify_record,
)
from .app_server_judge_v5_calibration_v109_layered_recovery import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V109_ROOT,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso


V110_SPEC_VERSION = "pif_app_server_judge_v5_4_v110_sol_reference_audit_spec_v1"
V110_SCORE_VERSION = "pif_app_server_judge_v5_4_v110_sol_reference_audit_score_v1"
V110_TRUTH_VERSION = "pif_app_server_judge_v5_4_calibration_truth_v9"
V110_FAILURE_VERSION = "pif_app_server_judge_v5_4_v110_failure_v1"
V110_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v110_terminal_v1"
V110_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V110_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V110_PHASE_ID = "judge_v5_4_v110_sol_reference_audit"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
POINTWISE_TURNS = tuple(f"sol_pointwise_owner_shard_{index:02d}" for index in range(3))
ALIGNMENT_TURNS = tuple(f"sol_alignment_owner_shard_{index:02d}" for index in range(2))
TURN_NAMES = POINTWISE_TURNS + ALIGNMENT_TURNS
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V109_ROOT.parent / "judge-calibration-v5_4-v110-sol-reference-audit"
).resolve()


class JudgeV5CalibrationV110Error(RuntimeError):
    """The v110 independent reference audit cannot preserve its frozen contract."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        prior = _load_json(path, f"existing {path.name}")
        value[key] = prior.get(key)
    _write_immutable(path, value)


def _validate_predecessor() -> dict[str, Any]:
    execution = DEFAULT_V109_ROOT / "semantic-execution"
    paths = {
        "v109_spec": DEFAULT_V109_ROOT / "layered-recovery-spec.json",
        "v109_terminal": DEFAULT_V109_ROOT / "terminal.json",
        "core_terminal": execution / "terminal.json",
        "score": execution / "diagnostic-score.json",
        "selection": execution / "diagnostic-selection.json",
        "truth": execution / "diagnostic-truth.private.json",
        "pointwise_input": execution / "pointwise-checklist-full.private.json",
        "pointwise_output": execution / "pointwise-output-full.private.json",
        "support": execution / "support-receipts.private.json",
        "alignment": execution / "reconciled-alignment.private.json",
        "disagreements": execution / "observable-disagreements.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal, core, score, spec = (
        values["v109_terminal"],
        values["core_terminal"],
        values["score"],
        values["v109_spec"],
    )
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("diagnostic_passed") is not False
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or core.get("state") != "inactive"
        or core.get("turn_count") != 8
        or core.get("semantic_retry_count") != 0
        or core.get("accounting_complete") is not True
        or score.get("passed") is not False
        or score.get("metrics", {}).get("case_count") != 18
        or score.get("metrics", {}).get("witness_count") != 53
        or values["disagreements"].get("disagreement_case_count") != 0
        or spec.get("retry_count_per_turn") != 0
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV110Error("v109 predecessor contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def _pointwise_tuple(row: Mapping[str, Any]) -> tuple[str, str, tuple[str, ...]]:
    return (
        str(row["proposition_verdict"]),
        str(row["structured_field_verdict"]),
        tuple(sorted(str(field) for field in row["field_issue_fields"])),
    )


def _truth_pointwise_tuple(
    truth: Mapping[str, Any], case_id: str, witness_id: str
) -> tuple[str, str, tuple[str, ...]]:
    case = truth["cases"][case_id]
    return (
        str(case["proposition"][witness_id]),
        str(case["structured_fields"][witness_id]),
        tuple(sorted(str(field) for field in case["field_issues"][witness_id])),
    )


def _project_alignment(row: Mapping[str, Any]) -> dict[str, Any]:
    pairs = [
        {
            "witness_ids": sorted(str(value) for value in pair["witness_ids"]),
            "relation": str(pair["relation"]),
            "mismatch_fields": sorted(str(value) for value in pair.get("mismatch_fields") or []),
        }
        for pair in row.get("alignment_pairs") or []
    ]
    pairs.sort(key=lambda item: tuple(item["witness_ids"]))
    groups = [sorted(str(value) for value in group) for group in row.get("equivalence_groups") or []]
    groups.sort()
    return {
        "pairs": pairs,
        "equivalence_groups": groups,
        "unpaired_witness_ids": sorted(str(value) for value in row.get("unpaired_witness_ids") or []),
    }


def _truth_alignment(case: Mapping[str, Any]) -> dict[str, Any]:
    return _project_alignment(
        {
            "alignment_pairs": case["pairs"],
            "equivalence_groups": case["equivalence_groups"],
            "unpaired_witness_ids": case["unpaired_witness_ids"],
        }
    )


def _select_alignment_audit_ids(predecessor: Mapping[str, Any]) -> dict[str, list[str]]:
    truth = predecessor["values"]["truth"]["cases"]
    observed = {
        str(row["case_id"]): row
        for row in predecessor["values"]["alignment"]["cases"]
    }
    candidates = sorted(
        case_id
        for case_id, case in truth.items()
        if _project_alignment(observed[case_id]) != _truth_alignment(case)
    )
    if len(candidates) != 4:
        raise JudgeV5CalibrationV110Error("v110 alignment candidate count drifted")
    exact_by_shape: dict[str, list[str]] = {}
    for case_id, case in truth.items():
        if case_id in candidates or _project_alignment(observed[case_id]) != _truth_alignment(case):
            continue
        exact_by_shape.setdefault(case["shape"], []).append(case_id)
    for values in exact_by_shape.values():
        values.sort()
    controls = []
    shapes = sorted(exact_by_shape)
    while len(controls) < 8:
        progressed = False
        for shape in shapes:
            if exact_by_shape[shape] and len(controls) < 8:
                controls.append(exact_by_shape[shape].pop(0))
                progressed = True
        if not progressed:
            break
    if len(controls) != 8 or len(set(candidates + controls)) != 12:
        raise JudgeV5CalibrationV110Error("v110 alignment controls drifted")
    return {"candidates": candidates, "controls": controls, "audit": candidates + controls}


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit_path, policy_path = root / "capacity-policy-audit.json", root / "capacity-policy.json"
    audit = {
        "schema_version": V110_CAPACITY_AUDIT_VERSION,
        "phase_id": V110_PHASE_ID,
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
        "schema_version": V110_CAPACITY_POLICY_VERSION,
        "phase_id": V110_PHASE_ID,
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
        "projected_phase_quota_points": math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v110(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessor()
    selection = predecessor["values"]["selection"]
    case_ids = list(selection["selected"])
    pointwise_full = pointwise_input_subset(
        v108._validate_predecessors()["values"]["v106_pointwise_input"], case_ids
    )
    shards = v108._case_shards(case_ids)
    alignment_selection = _select_alignment_audit_ids(predecessor)
    selection_path = root / "owner-audit-selection.json"
    _write_immutable(selection_path, alignment_selection)
    pointwise_shards = []
    for turn_name, shard_ids in zip(POINTWISE_TURNS, shards, strict=True):
        value = pointwise_input_subset(pointwise_full, shard_ids)
        prompt = v108.build_pointwise_checklist_prompt(value)
        schema = v108.pointwise_checklist_schema(value)
        paths = _freeze_turn_request(root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema)
        pointwise_shards.append({"turn_name": turn_name, "input": value, "prompt": prompt, "schema": schema, "paths": paths})
    capacity = _build_capacity_policy(root, predecessor["records"])
    runtime_dir = Path(__file__).resolve().parent
    runtime_files = [
        Path(__file__),
        runtime_dir / "app_server_judge_v5_calibration_v109_layered_recovery.py",
        runtime_dir / "app_server_judge_v5_calibration_v108_layered_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration.py",
        runtime_dir / "app_server_judge_v5.py",
        runtime_dir / "app_server_capacity_reserve.py",
        runtime_dir / "codex_app_server.py",
    ]
    spec = {
        "schema_version": V110_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "pointwise_case_count": 18,
        "pointwise_witness_count": 53,
        "alignment_candidate_case_count": 4,
        "alignment_control_case_count": 8,
        "minimum_turn_count": 5,
        "maximum_turn_count": 5,
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "owner_is_side_free": True,
        "gpt55_outputs_exposed_to_owner": False,
        "fixture_truth_exposed_to_owner": False,
        "pointwise_exact_control_gate_min": 0.90,
        "alignment_exact_control_gate_min": 0.875,
        "reference_patch_rule": "gpt55_and_sol_exact_agreement_against_reference_after_control_gates",
        "reference_patch_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [_record(path) for path in runtime_files],
        "frozen_inputs": {
            "selection": _record(selection_path),
            "pointwise_shards": [
                {
                    "turn_name": shard["turn_name"],
                    "input": _record(shard["paths"]["input"]),
                    "prompt": _record(shard["paths"]["prompt"]),
                    "schema": _record(shard["paths"]["schema"]),
                }
                for shard in pointwise_shards
            ],
        },
        "privacy": "private_source_event_prompts_outputs_truth_sanitized_terminal_only",
    }
    spec_path = root / "sol-reference-audit-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v110 spec")
        spec["created_at"] = prior.get("created_at")
        if spec != prior:
            raise JudgeV5CalibrationV110Error("immutable v110 spec drifted")
    else:
        _write_immutable(spec_path, spec)
    if not all(_verify_record(record) for record in spec["runtime_files"]):
        raise JudgeV5CalibrationV110Error("v110 runtime record drifted")
    return {
        "root": root,
        "spec": spec,
        "predecessor": predecessor,
        "pointwise_full": pointwise_full,
        "pointwise_shards": pointwise_shards,
        "alignment_selection": alignment_selection,
        "capacity_policy": capacity["policy"],
    }


def score_owner_audit(
    *,
    predecessor: Mapping[str, Any],
    sol_pointwise: Mapping[str, Any],
    sol_alignment: Mapping[str, Any],
    alignment_selection: Mapping[str, Sequence[str]],
) -> dict[str, Any]:
    truth = predecessor["values"]["truth"]
    gpt_pointwise = {
        str(row["witness_id"]): row
        for row in predecessor["values"]["pointwise_output"]["units"]
    }
    sol_pointwise_rows = {str(row["witness_id"]): row for row in sol_pointwise["units"]}
    witness_to_case = {
        witness_id: case_id
        for case_id, case in truth["cases"].items()
        for witness_id in case["proposition"]
    }
    controls = []
    candidates = []
    for witness_id, gpt_row in gpt_pointwise.items():
        case_id = witness_to_case[witness_id]
        reference = _truth_pointwise_tuple(truth, case_id, witness_id)
        gpt = _pointwise_tuple(gpt_row)
        sol = _pointwise_tuple(sol_pointwise_rows[witness_id])
        row = {
            "case_id": case_id,
            "witness_id": witness_id,
            "sol_matches_reference": sol == reference,
            "sol_matches_gpt55": sol == gpt,
            "gpt55_matches_reference": gpt == reference,
        }
        (controls if gpt == reference else candidates).append(row)
    pointwise_control_exact = sum(row["sol_matches_reference"] for row in controls)
    pointwise_control_rate = round(pointwise_control_exact / len(controls), 6) if controls else 0.0

    gpt_alignment = {
        str(row["case_id"]): row for row in predecessor["values"]["alignment"]["cases"]
    }
    sol_alignment_rows = {str(row["case_id"]): row for row in sol_alignment["cases"]}
    alignment_controls = []
    alignment_candidates = []
    for case_id in alignment_selection["audit"]:
        reference = _truth_alignment(truth["cases"][case_id])
        gpt = _project_alignment(gpt_alignment[case_id])
        sol = _project_alignment(sol_alignment_rows[case_id])
        row = {
            "case_id": case_id,
            "sol_matches_reference": sol == reference,
            "sol_matches_gpt55": sol == gpt,
            "gpt55_matches_reference": gpt == reference,
        }
        (alignment_controls if case_id in alignment_selection["controls"] else alignment_candidates).append(row)
    alignment_control_exact = sum(row["sol_matches_reference"] for row in alignment_controls)
    alignment_control_rate = round(alignment_control_exact / len(alignment_controls), 6)
    pointwise_gate = pointwise_control_rate >= 0.90
    alignment_gate = alignment_control_rate >= 0.875
    patch_authorized = pointwise_gate and alignment_gate
    pointwise_consensus = [
        row for row in candidates if row["sol_matches_gpt55"] and not row["gpt55_matches_reference"]
    ]
    alignment_consensus = [
        row for row in alignment_candidates if row["sol_matches_gpt55"] and not row["gpt55_matches_reference"]
    ]
    return {
        "schema_version": V110_SCORE_VERSION,
        "pointwise_control_count": len(controls),
        "pointwise_control_exact_count": pointwise_control_exact,
        "pointwise_control_exact_rate": pointwise_control_rate,
        "pointwise_candidate_count": len(candidates),
        "pointwise_consensus_patch_count": len(pointwise_consensus),
        "alignment_control_count": len(alignment_controls),
        "alignment_control_exact_count": alignment_control_exact,
        "alignment_control_exact_rate": alignment_control_rate,
        "alignment_candidate_count": len(alignment_candidates),
        "alignment_consensus_patch_count": len(alignment_consensus),
        "checks": {
            "pointwise_control_exact_rate": pointwise_gate,
            "alignment_control_exact_rate": alignment_gate,
        },
        "reference_patch_authorized": patch_authorized,
        "pointwise_consensus": pointwise_consensus,
        "alignment_consensus": alignment_consensus,
    }


def apply_authorized_patch(
    *,
    predecessor: Mapping[str, Any],
    sol_pointwise: Mapping[str, Any],
    sol_alignment: Mapping[str, Any],
    owner_score: Mapping[str, Any],
) -> dict[str, Any]:
    truth = deepcopy(predecessor["values"]["truth"])
    if not owner_score["reference_patch_authorized"]:
        truth["schema_version"] = V110_TRUTH_VERSION
        truth["reference_patch_authorized"] = False
        truth["reference_patch_count"] = 0
        return truth
    gpt_pointwise = {
        str(row["witness_id"]): row
        for row in predecessor["values"]["pointwise_output"]["units"]
    }
    sol_pointwise_rows = {str(row["witness_id"]): row for row in sol_pointwise["units"]}
    witness_to_case = {
        witness_id: case_id
        for case_id, case in truth["cases"].items()
        for witness_id in case["proposition"]
    }
    pointwise_patches = 0
    for witness_id, gpt_row in gpt_pointwise.items():
        case_id = witness_to_case[witness_id]
        reference = _truth_pointwise_tuple(truth, case_id, witness_id)
        gpt = _pointwise_tuple(gpt_row)
        sol = _pointwise_tuple(sol_pointwise_rows[witness_id])
        if gpt == sol and gpt != reference:
            case = truth["cases"][case_id]
            case["proposition"][witness_id] = gpt[0]
            case["structured_fields"][witness_id] = gpt[1]
            case["field_issues"][witness_id] = list(gpt[2])
            pointwise_patches += 1
    gpt_alignment = {
        str(row["case_id"]): row for row in predecessor["values"]["alignment"]["cases"]
    }
    sol_alignment_rows = {str(row["case_id"]): row for row in sol_alignment["cases"]}
    alignment_patches = 0
    for case_id, sol_row in sol_alignment_rows.items():
        reference = _truth_alignment(truth["cases"][case_id])
        gpt = _project_alignment(gpt_alignment[case_id])
        sol = _project_alignment(sol_row)
        if gpt == sol and gpt != reference:
            truth["cases"][case_id]["pairs"] = deepcopy(gpt["pairs"])
            truth["cases"][case_id]["equivalence_groups"] = deepcopy(gpt["equivalence_groups"])
            truth["cases"][case_id]["unpaired_witness_ids"] = deepcopy(gpt["unpaired_witness_ids"])
            alignment_patches += 1
    truth["schema_version"] = V110_TRUTH_VERSION
    truth["reference_version"] = "fixture_reference_v9_sol_owner_consensus"
    truth["reference_patch_authorized"] = True
    truth["pointwise_patch_count"] = pointwise_patches
    truth["alignment_patch_count"] = alignment_patches
    truth["reference_patch_count"] = pointwise_patches + alignment_patches
    return truth


def _write_failure(*, root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v110 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V110_FAILURE_VERSION,
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
        "schema_version": V110_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_patch_authorized": False,
        "reference_frozen": False,
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


async def run_v110(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v110 terminal")
    frozen = freeze_v110(output_dir=root, timeout_seconds=timeout_seconds)
    policy_path = frozen["capacity_policy"]
    sidecars = []
    current_turn = None
    try:
        async with (client_factory or _client_factory)(policy_path) as client:
            explicit_outputs = []
            for shard in frozen["pointwise_shards"]:
                current_turn = shard["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=shard["paths"],
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                    base_instructions=v108.pointwise_checklist_instructions(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["units"]),
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard: v108.validate_pointwise_checklist_output(value, item["input"]),
                )
                explicit_outputs.append(output)
                sidecars.append(sidecar)
            explicit = _merge_outputs(explicit_outputs, "units")
            errors = v108.validate_pointwise_checklist_output(explicit, frozen["pointwise_full"])
            if errors:
                raise JudgeV5CalibrationV110Error("aggregate Sol pointwise output invalid")
            explicit_path = root / "sol-pointwise-checklist-full.private.json"
            _write_immutable(explicit_path, explicit)
            pointwise_output = v108.project_pointwise_checklist(explicit)
            pointwise_path = root / "sol-pointwise-output-full.private.json"
            _write_immutable(pointwise_path, pointwise_output)
            support = freeze_support_receipts(pointwise_output, frozen["pointwise_full"])
            support_path = root / "sol-support-receipts.private.json"
            _write_immutable(support_path, support)

            audit_ids = list(frozen["alignment_selection"]["audit"])
            alignment_shards = [audit_ids[:6], audit_ids[6:]]
            alignment_outputs = []
            pool = v108._validate_predecessors()["values"]["v106_pool"]
            for turn_name, case_ids in zip(ALIGNMENT_TURNS, alignment_shards, strict=True):
                alignment_input = build_neutral_alignment_input(pool, support, case_ids=case_ids)
                prompt = v108.build_alignment_prompt_v108(alignment_input)
                schema = neutral_alignment_output_schema(alignment_input)
                current_turn = turn_name
                paths = _freeze_turn_request(root=root, turn_name=current_turn, input_value=alignment_input, prompt=prompt, schema=schema)
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=v108.alignment_instructions_v108(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=6,
                    policy_path=policy_path,
                    output_validator=lambda value, item=alignment_input: _validate_scoreable_alignment_output(value, item),
                )
                alignment_outputs.append(output)
                sidecars.append(sidecar)
        sol_alignment = _merge_outputs(alignment_outputs, "cases")
        alignment_path = root / "sol-alignment-output.private.json"
        _write_immutable(alignment_path, sol_alignment)
        owner_score = score_owner_audit(
            predecessor=frozen["predecessor"],
            sol_pointwise=pointwise_output,
            sol_alignment=sol_alignment,
            alignment_selection=frozen["alignment_selection"],
        )
        owner_score_path = root / "owner-audit-score.json"
        _write_immutable(owner_score_path, owner_score)
        truth_candidate = apply_authorized_patch(
            predecessor=frozen["predecessor"],
            sol_pointwise=pointwise_output,
            sol_alignment=sol_alignment,
            owner_score=owner_score,
        )
        candidate_path = root / "calibration-truth-v9-candidate.private.json"
        _write_immutable(candidate_path, truth_candidate)
        reference_frozen = bool(owner_score["reference_patch_authorized"])
        truth_path = root / "calibration-truth-v9.private.json"
        if reference_frozen:
            _write_immutable(truth_path, truth_candidate)
        gpt_score = v108.score_v108(
            pointwise_output=frozen["predecessor"]["values"]["pointwise_output"],
            reconciled_alignment=frozen["predecessor"]["values"]["alignment"],
            expected=truth_candidate,
            observable_disagreements=frozen["predecessor"]["values"]["disagreements"],
        )
        gpt_score_path = root / "gpt55-score-against-v9-candidate.json"
        _write_immutable(gpt_score_path, gpt_score)
        full_authorized = reference_frozen and bool(gpt_score["passed"])
        accounting = _aggregate_usage(sidecars)
        terminal = {
            "schema_version": V110_TERMINAL_VERSION,
            "state": "completed" if full_authorized else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": "v110_reference_v9_frozen_fresh_full_calibration_authorized" if full_authorized else "inactive_incomplete_recovery_required",
            "development_terminal_reason": "v110_sol_owner_control_and_reconciliation_passed" if full_authorized else "v110_sol_owner_or_reconciled_quality_gate_not_passed",
            "overall_evaluation_complete": False,
            "reference_patch_authorized": reference_frozen,
            "reference_frozen": reference_frozen,
            "fresh_full_calibration_authorized": full_authorized,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "owner_score": _record(owner_score_path),
            "truth_candidate": _record(candidate_path),
            "truth": _record(truth_path) if reference_frozen else None,
            "reconciled_gpt55_score": _record(gpt_score_path),
            "owner_checks": owner_score["checks"],
            "owner_metrics": {
                key: value for key, value in owner_score.items()
                if key.endswith("count") or key.endswith("rate")
            },
            "failed_quality_gates": gpt_score["failed_checks"],
            "metrics": gpt_score["metrics"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root=root, turn_name=exc.turn_name, error_class=exc.error_class)
    except Exception as exc:
        return _write_failure(root=root, turn_name=current_turn, error_class=type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v110 independent Sol reference audit")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v110(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({
        "state": terminal["state"],
        "terminal_reason": terminal["terminal_reason"],
        "reference_frozen": terminal.get("reference_frozen", False),
        "fresh_full_calibration_authorized": terminal.get("fresh_full_calibration_authorized", False),
        "usage_status": terminal.get("usage_status"),
    }, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
