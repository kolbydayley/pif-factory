from __future__ import annotations

"""Capacity-safe fresh full calibration after the presemantic v167 nonlaunch."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from . import app_server_judge_v5_calibration_v149_fresh_corrected_field_diagnostic as v149
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v167_fresh_full_replacement as v167
from .app_server_judge_v5 import normalize_neutral_alignment_output, validate_neutral_alignment_output
from .app_server_judge_v5_calibration import pointwise_input_subset
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
from .app_server_judge_v5_calibration_v26_diagnostic import (
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    output_schema as field_output_schema,
    validate_output as validate_field_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import _record, _verify_record
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V168_SPEC_VERSION = "pif_app_server_judge_v5_4_v168_spec_v1"
V168_SCORE_VERSION = "pif_app_server_judge_v5_4_v168_score_v1"
V168_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v168_development_protocol_v1"
V168_FAILURE_VERSION = "pif_app_server_judge_v5_4_v168_failure_v1"
V168_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v168_terminal_v1"
V168_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V168_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V168_PHASE_ID = "judge_v5_4_v168_capacity_recovery"

SUPPORT_MODEL = v167.SUPPORT_MODEL
FIELD_MODEL = v167.FIELD_MODEL
ALIGNMENT_MODEL = v167.ALIGNMENT_MODEL
VERIFIER_MODEL = v167.VERIFIER_MODEL
EFFORT = v167.EFFORT
SUPPORT_PRIMARY_TURNS = tuple(f"recovery_support_shard_{index:02d}" for index in range(7))
SUPPORT_CANARY_TURN = "recovery_support_canary"
FIELD_PRIMARY_TURNS = tuple(f"recovery_field_singleton_{index:02d}" for index in range(28))
FIELD_REPEAT_TURN = "recovery_field_repeat_batch"
ALIGNMENT_PRIMARY_TURNS = tuple(f"recovery_alignment_shard_{index:02d}" for index in range(11))
ALIGNMENT_CANARY_TURN = "recovery_alignment_canary"
VERIFIER_PRIMARY_TURNS = tuple(f"recovery_verifier_shard_{index:02d}" for index in range(12))
VERIFIER_CANARY_TURNS = tuple(f"recovery_verifier_canary_{index:02d}" for index in range(3))
TURN_NAMES = (
    SUPPORT_PRIMARY_TURNS
    + (SUPPORT_CANARY_TURN,)
    + FIELD_PRIMARY_TURNS
    + (FIELD_REPEAT_TURN,)
    + ALIGNMENT_PRIMARY_TURNS
    + (ALIGNMENT_CANARY_TURN,)
    + VERIFIER_PRIMARY_TURNS
    + VERIFIER_CANARY_TURNS
)
MAXIMUM_TOTAL_TOKENS_PER_TURN = 39_500
TIMEOUT_SECONDS = v167.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v167.DEFAULT_OUTPUT_ROOT.parent / "judge-calibration-v5_4-v168-capacity-recovery"
).resolve()


class JudgeV5CalibrationV168Error(RuntimeError):
    """The immutable v168 capacity-recovery contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v167_nonlaunch() -> dict[str, Any]:
    root = v167.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "fresh-full-replacement-spec.json",
        "policy": root / "capacity-policy.json",
        "audit": root / "capacity-policy-audit.json",
        "pool": root / "shared-witness-pool.private.json",
        "pointwise": root / "pointwise-input-full.private.json",
        "truth": root / "full-calibration-truth.private.json",
        "selection": root / "full-calibration-selection-audit.json",
    }
    values = {name: _load_json(path, f"v167 {name}") for name, path in paths.items()}
    spec, policy = values["spec"], values["policy"]
    if (
        spec.get("state") != "frozen_before_model_calls"
        or spec.get("maximum_turn_count") != 67
        or spec.get("retry_count_per_turn") != 0
        or policy.get("phase_total_token_bound") != 2680000
        or policy.get("projected_phase_quota_points") != 46
        or policy.get("minimum_remaining_reserve_percent") != 20
        or (root / "terminal.json").exists()
        or list(root.rglob("capacity.json"))
        or list(root.rglob("sidecar.json"))
        or list(root.rglob("output.private.json"))
    ):
        raise JudgeV5CalibrationV168Error("v167 is not an immutable presemantic nonlaunch")
    for record in spec["runtime_files"]:
        _verify_record(record)
    for record in spec["frozen_inputs"]["static_turns"]:
        for key in ("input", "prompt", "schema"):
            _verify_record(record[key])
    source = v167._validate_v166()
    data = v167.build_v167_inputs(source)
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "v166": source,
        "v167_data": data,
        "cumulative_usage": source["cumulative_usage"],
    }


def build_v168_inputs(source: Mapping[str, Any]) -> dict[str, Any]:
    base = source["v167_data"]
    case_ids = sorted(
        (str(row["case_id"]) for row in base["pool"]["cases"]),
        key=lambda value: sha256_text(f"v168|support-case|{value}"),
    )
    support_shards = v167._balanced_shards(case_ids, len(SUPPORT_PRIMARY_TURNS))
    support_turns = []
    for turn_name, shard in zip(SUPPORT_PRIMARY_TURNS, support_shards, strict=True):
        subset = pointwise_input_subset(base["pointwise"], shard)
        support_turns.append(
            {"turn_name": turn_name, "turn_role": "support_primary", "value": v155._support_value(subset["units"])}
        )
    base_canary = next(row for row in base["static_turns"] if row["turn_role"] == "support_canary")
    support_turns.append(
        {"turn_name": SUPPORT_CANARY_TURN, "turn_role": "support_canary", "value": deepcopy(base_canary["value"])}
    )
    fields = [deepcopy(row) for row in base["static_turns"] if row["turn_role"].startswith("field_")]
    primary_fields = [row for row in fields if row["turn_role"] == "field_singleton"]
    repeat_fields = [row for row in fields if row["turn_role"] == "field_repeat_batch"]
    if len(primary_fields) != 28 or len(repeat_fields) != 1:
        raise JudgeV5CalibrationV168Error("v167 field turn coverage drifted")
    for turn_name, row in zip(FIELD_PRIMARY_TURNS, primary_fields, strict=True):
        row["turn_name"] = turn_name
    repeat_fields[0]["turn_name"] = FIELD_REPEAT_TURN
    selection = {
        **deepcopy(base["selection"]),
        "schema_version": V168_SPEC_VERSION,
        "created_at": now_iso(),
        "support_primary_turn_count": len(SUPPORT_PRIMARY_TURNS),
        "capacity_recovery_from_v167": True,
        "v167_semantic_turn_count": 0,
        "v167_usage_tokens": 0,
    }
    return {
        **{key: base[key] for key in ("pool", "reference", "pointwise", "truth", "alignment_case_shards")},
        "selection": selection,
        "support_case_shards": support_shards,
        "static_turns": support_turns + primary_fields + repeat_fields,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    audit = {
        "schema_version": V168_CAPACITY_AUDIT_VERSION,
        "phase_id": V168_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "v167_presemantic_nonlaunch": {
            "observed_primary_used_percent": 35,
            "primary_remaining_percent": 65,
            "v167_projected_quota_points": 46,
            "usable_points_above_reserve": 45,
            "v167_cleared_for_semantic_turn": False,
            "thread_started": False,
            "turn_started": False,
            "semantic_usage_tokens": 0,
        },
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "measured_predecessor_maximum_total_tokens": 38416,
            "projected_terminal_remaining_percent_at_35_used": 22,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V168_CAPACITY_POLICY_VERSION,
        "phase_id": V168_PHASE_ID,
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
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v168(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v168 terminal")}
    source = _validate_v167_nonlaunch()
    data = build_v168_inputs(source)
    paths = {
        "pool": root / "shared-witness-pool.private.json",
        "pointwise": root / "pointwise-input-full.private.json",
        "truth": root / "full-calibration-truth.private.json",
        "selection": root / "full-calibration-selection-audit.json",
    }
    _write_immutable(paths["pool"], data["pool"])
    _write_immutable(paths["pointwise"], data["pointwise"])
    _write_immutable(paths["truth"], data["truth"])
    _write_stable_time(paths["selection"], data["selection"], "created_at")
    turns = []
    for row in data["static_turns"]:
        is_support = row["turn_role"].startswith("support_")
        prompt = v143.support_prompt_v143(row["value"]) if is_support else v149.field_prompt_v149(row["value"])
        schema = v143.support_output_schema(row["value"]) if is_support else field_output_schema(row["value"])
        request_paths = _freeze_turn_request(
            root=root, turn_name=row["turn_name"], input_value=row["value"], prompt=prompt, schema=schema
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": request_paths})
    predecessor = {
        **{f"v167_{name}": record for name, record in source["records"].items()},
        "v167_semantic_attempt_started": False,
        "v167_usage_tokens": 0,
        "v166_cumulative_usage": source["cumulative_usage"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V168_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "support_model": SUPPORT_MODEL,
        "field_model": FIELD_MODEL,
        "alignment_model": ALIGNMENT_MODEL,
        "equivalent_pair_verifier_model": VERIFIER_MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "capacity_recovery_seven_support_shards_singleton_fields_alignment_all_equivalent_verifier_strict_canary",
        "case_count": 66,
        "witness_count": 182,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": len(TURN_NAMES),
        "maximum_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "all_semantic_turns_fresh": True,
        "prior_model_outputs_reused": False,
        "v167_semantic_attempt_replayed": False,
        "reference_truth_exposed_to_model": False,
        "all_primary_equivalent_pairs_verified": True,
        "strict_canary_no_repair_call": True,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [_record(Path(__file__)), _record(Path(v167.__file__)), *source["values"]["spec"]["runtime_files"]],
        "frozen_instructions": {
            "support_sha256": sha256_text(v143.support_base_instructions_v143()),
            "field_sha256": sha256_text(v146.base_instructions_v146()),
            "alignment_sha256": sha256_text(v130.alignment_instructions_v130()),
        },
        "frozen_inputs": {
            **{name: _record(path) for name, path in paths.items()},
            "field_protocol": source["v166"]["field_protocol_record"],
            "alignment_protocol": source["v166"]["records"]["protocol"],
            "reference": source["v166"]["records"]["reference"],
            "truth_source": source["v166"]["records"]["truth"],
            "static_turns": [
                {
                    "turn_name": turn["turn_name"],
                    "role": turn["turn_role"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_truth_and_mapping_sanitized_terminal_only",
    }
    spec_path = root / "fresh-full-capacity-recovery-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "data": data,
        "source": source,
    }


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v168 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    predecessor = _validate_v167_nonlaunch()["cumulative_usage"]
    cumulative = _sum_usage(predecessor, usage)
    failure = {
        "schema_version": V168_FAILURE_VERSION,
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
        "predecessor_cumulative_usage": predecessor,
        "cumulative_known_usage_lower_bound": cumulative,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V168_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "development_judge_frozen": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "predecessor_cumulative_usage": predecessor,
        "cumulative_known_usage_lower_bound": cumulative,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v168(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v168 terminal")
    frozen = freeze_v168(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    sidecars = []
    try:
        support_primary, support_canary, field_primary, field_repeats = [], [], [], []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                is_support = turn["turn_role"].startswith("support_")
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v143.support_base_instructions_v143() if is_support else v146.base_instructions_v146(),
                    model=SUPPORT_MODEL if is_support else FIELD_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=turn["value"].get("unit_count", turn["value"].get("task_count")),
                    policy_path=frozen["capacity_policy"],
                    output_validator=(lambda candidate, item=turn["value"]: v143.validate_support_output(candidate, item)) if is_support else (lambda candidate, item=turn["value"]: validate_field_output(candidate, item)),
                )
                sidecars.append(sidecar)
                if turn["turn_role"] == "support_primary": support_primary.append(output)
                elif turn["turn_role"] == "support_canary": support_canary.append(output)
                elif turn["turn_role"] == "field_singleton": field_primary.append(output)
                else: field_repeats.append(output)
            support = v155._merge_outputs(support_primary, "units")
            support_repeat = v155._merge_outputs(support_canary, "units")
            fields = v155._merge_outputs(field_primary, "decisions")
            repeats = v155._merge_outputs(field_repeats, "decisions")
            if v143.validate_support_output(support, v155._support_value(frozen["data"]["pointwise"]["units"])):
                raise JudgeV5CalibrationV168Error("aggregate support output is invalid")
            receipts = v155._support_receipts(support)
            _write_immutable(root / "support-output-full.private.json", support)
            _write_immutable(root / "support-canary-output.private.json", support_repeat)
            _write_immutable(root / "support-receipts.private.json", receipts)
            _write_immutable(root / "field-output.private.json", fields)
            _write_immutable(root / "field-repeat-output.private.json", repeats)

            alignment_outputs = []
            for turn_name, case_ids in zip(ALIGNMENT_PRIMARY_TURNS, frozen["data"]["alignment_case_shards"], strict=True):
                value = v155._support_positive_alignment_input(
                    frozen["data"]["pool"], receipts, case_ids=case_ids, permutation="base"
                )
                current_turn = turn_name
                output, sidecar = await v167._run_alignment_turn(
                    client=client, root=root, turn_name=turn_name, value=value,
                    model=ALIGNMENT_MODEL, timeout_seconds=timeout_seconds,
                    policy_path=frozen["capacity_policy"],
                )
                alignment_outputs.append(output)
                sidecars.append(sidecar)
            base_output = v155._merge_outputs(alignment_outputs, "cases")
            all_case_ids = [str(row["case_id"]) for row in frozen["data"]["pool"]["cases"]]
            base_input = v155._support_positive_alignment_input(
                frozen["data"]["pool"], receipts, case_ids=all_case_ids, permutation="base"
            )
            if validate_neutral_alignment_output(base_output, base_input):
                raise JudgeV5CalibrationV168Error("aggregate base alignment is invalid")
            base_normalized = normalize_neutral_alignment_output(base_output, base_input)
            _write_immutable(root / "alignment-base-output.private.json", base_output)

            canary_ids = frozen["data"]["truth"]["alignment_canary_case_ids"]
            canary_input = v155._support_positive_alignment_input(
                frozen["data"]["pool"], receipts, case_ids=canary_ids, permutation="balanced_canary"
            )
            current_turn = ALIGNMENT_CANARY_TURN
            canary_output, sidecar = await v167._run_alignment_turn(
                client=client, root=root, turn_name=ALIGNMENT_CANARY_TURN, value=canary_input,
                model=ALIGNMENT_MODEL, timeout_seconds=timeout_seconds,
                policy_path=frozen["capacity_policy"],
            )
            sidecars.append(sidecar)
            canary_normalized = normalize_neutral_alignment_output(canary_output, canary_input)
            _write_immutable(root / "alignment-canary-output.private.json", canary_output)

            base_pairs = v167._pair_rows(normalized=base_normalized, alignment_input=base_input, prefix="v168-base")
            canary_pairs = v167._pair_rows(normalized=canary_normalized, alignment_input=canary_input, prefix="v168-canary")
            base_shards = v167._balanced_shards([row["pair_case_id"] for row in base_pairs], len(VERIFIER_PRIMARY_TURNS))
            canary_shards = v167._balanced_shards([row["pair_case_id"] for row in canary_pairs], len(VERIFIER_CANARY_TURNS))
            base_by_id = {row["pair_case_id"]: row for row in base_pairs}
            canary_by_id = {row["pair_case_id"]: row for row in canary_pairs}
            _write_immutable(
                root / "equivalent-pair-verifier-selection-audit.json",
                {
                    "schema_version": V168_SPEC_VERSION,
                    "base_primary_equivalent_pair_count": len(base_pairs),
                    "canary_primary_equivalent_pair_count": len(canary_pairs),
                    "all_primary_equivalent_pairs_selected": True,
                    "base_turn_count": len(base_shards),
                    "canary_turn_count": len(canary_shards),
                    "truth_labels_used_for_selection": False,
                },
            )
            verifier_outputs, verifier_inputs = [], []
            for turn_name, shard_ids in zip(VERIFIER_PRIMARY_TURNS, base_shards, strict=True):
                rows = [base_by_id[key] for key in shard_ids]
                value = v167._verifier_turn_value(template=base_input, rows=rows, permutation="base")
                current_turn = turn_name
                output, sidecar = await v167._run_alignment_turn(
                    client=client, root=root, turn_name=turn_name, value=value,
                    model=VERIFIER_MODEL, timeout_seconds=timeout_seconds,
                    policy_path=frozen["capacity_policy"],
                )
                verifier_outputs.append(output)
                verifier_inputs.append(value)
                sidecars.append(sidecar)
            verifier_normalized = v167._merge_normalized(verifier_outputs, verifier_inputs)
            final_base = v167._apply_verifier(
                normalized=base_normalized, pair_rows=base_pairs, verifier=verifier_normalized
            )
            _write_immutable(root / "alignment-final-base.private.json", final_base)

            verifier_canary_outputs, verifier_canary_inputs = [], []
            for turn_name, shard_ids in zip(VERIFIER_CANARY_TURNS, canary_shards, strict=True):
                rows = [canary_by_id[key] for key in shard_ids]
                value = v167._verifier_turn_value(template=canary_input, rows=rows, permutation="balanced_canary")
                current_turn = turn_name
                output, sidecar = await v167._run_alignment_turn(
                    client=client, root=root, turn_name=turn_name, value=value,
                    model=VERIFIER_MODEL, timeout_seconds=timeout_seconds,
                    policy_path=frozen["capacity_policy"],
                )
                verifier_canary_outputs.append(output)
                verifier_canary_inputs.append(value)
                sidecars.append(sidecar)
            verifier_canary_normalized = v167._merge_normalized(verifier_canary_outputs, verifier_canary_inputs)
            final_canary = v167._apply_verifier(
                normalized=canary_normalized, pair_rows=canary_pairs, verifier=verifier_canary_normalized
            )
            _write_immutable(root / "alignment-final-canary.private.json", final_canary)

        base_cases = {str(row["case_id"]): v130._project_alignment(row) for row in final_base["cases"]}
        canary_cases = {str(row["case_id"]): v130._project_alignment(row) for row in final_canary["cases"]}
        canary_exact = sum(base_cases[key] == canary_cases[key] for key in canary_cases)
        score = v155.score_v155(
            support=support,
            support_canary=support_repeat,
            fields=fields,
            field_repeats=repeats,
            alignment=final_base,
            truth=frozen["data"]["truth"],
            raw_alignment_disagreement_count=len(canary_cases) - canary_exact,
            final_canary_exact_count=canary_exact,
        )
        score["schema_version"] = V168_SCORE_VERSION
        score["equivalent_pair_verifier_base_count"] = len(base_pairs)
        score["equivalent_pair_verifier_canary_count"] = len(canary_pairs)
        score_path = root / "fresh-full-capacity-recovery-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        protocol_path = root / "development-judge-protocol-v168.json"
        if passed:
            _write_immutable(
                protocol_path,
                {
                    "schema_version": V168_PROTOCOL_VERSION,
                    "frozen_at": now_iso(),
                    "support_model": SUPPORT_MODEL,
                    "field_model": FIELD_MODEL,
                    "alignment_model": ALIGNMENT_MODEL,
                    "equivalent_pair_verifier_model": VERIFIER_MODEL,
                    "reasoning_effort": EFFORT,
                    "support_instructions_sha256": sha256_text(v143.support_base_instructions_v143()),
                    "field_instructions_sha256": sha256_text(v146.base_instructions_v146()),
                    "alignment_instructions_sha256": sha256_text(v130.alignment_instructions_v130()),
                    "field_protocol": frozen["spec"]["frozen_inputs"]["field_protocol"],
                    "alignment_protocol": frozen["spec"]["frozen_inputs"]["alignment_protocol"],
                    "reference": frozen["spec"]["frozen_inputs"]["reference"],
                    "truth": frozen["spec"]["frozen_inputs"]["truth_source"],
                    "all_primary_equivalent_pairs_verified": True,
                    "strict_canary_no_repair_call": True,
                    "retry_count_per_turn": 0,
                    "quality_gates_unchanged": True,
                    "selection_authorized": True,
                    "holdout_authorized": False,
                    "production_mutation_allowed": False,
                },
            )
        accounting = _aggregate_usage(sidecars)
        predecessor = frozen["source"]["cumulative_usage"]
        cumulative = _sum_usage(predecessor, accounting["usage"])
        terminal = {
            "schema_version": V168_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": "v168_full_development_calibration_passed_selection_authorized" if passed else "inactive_incomplete_recovery_required",
            "development_terminal_reason": "v168_full_development_calibration_passed" if passed else "v168_full_development_calibration_quality_gate_not_passed",
            "overall_evaluation_complete": False,
            "development_judge_frozen": passed,
            "selection_authorized": passed,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "equivalent_pair_verifier_base_count": len(base_pairs),
            "equivalent_pair_verifier_canary_count": len(canary_pairs),
            "score": _record(score_path),
            "protocol": _record(protocol_path) if passed else None,
            "reference": frozen["spec"]["frozen_inputs"]["reference"],
            "truth": frozen["spec"]["frozen_inputs"]["truth_source"],
            "predecessor_cumulative_usage": predecessor,
            "cumulative_calibration_usage": cumulative,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v168 capacity-safe full calibration")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v168(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({
        "state": terminal["state"],
        "terminal_reason": terminal["terminal_reason"],
        "development_judge_frozen": terminal.get("development_judge_frozen", False),
        "selection_authorized": terminal.get("selection_authorized", False),
        "usage_status": terminal.get("usage_status"),
    }, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
