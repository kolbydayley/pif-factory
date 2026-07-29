from __future__ import annotations

"""Continue the v168 fresh calibration after its measured capacity-bound stop."""

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
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from . import app_server_judge_v5_calibration_v167_fresh_full_replacement as v167
from . import app_server_judge_v5_calibration_v168_capacity_recovery as v168
from .app_server_judge_v5 import (
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
    validate_neutral_alignment_output,
)
from .app_server_judge_v5_calibration import calibration_case_shards
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V169_SPEC_VERSION = "pif_app_server_judge_v5_4_v169_spec_v1"
V169_SCORE_VERSION = "pif_app_server_judge_v5_4_v169_score_v1"
V169_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v169_development_protocol_v1"
V169_FAILURE_VERSION = "pif_app_server_judge_v5_4_v169_failure_v1"
V169_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v169_terminal_v1"
V169_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V169_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V169_PHASE_ID = "judge_v5_4_v169_capacity_bound_continuation"

ALIGNMENT_PRIMARY_INDEXES = tuple(range(3, 11))
ALIGNMENT_PRIMARY_TURNS = tuple(
    f"continuation_alignment_shard_{index:02d}" for index in ALIGNMENT_PRIMARY_INDEXES
)
ALIGNMENT_CANARY_TURN = "continuation_alignment_canary"
VERIFIER_PRIMARY_TURNS = tuple(
    f"continuation_verifier_shard_{index:02d}" for index in range(12)
)
VERIFIER_CANARY_TURNS = tuple(
    f"continuation_verifier_canary_{index:02d}" for index in range(3)
)
TURN_NAMES = (
    ALIGNMENT_PRIMARY_TURNS
    + (ALIGNMENT_CANARY_TURN,)
    + VERIFIER_PRIMARY_TURNS
    + VERIFIER_CANARY_TURNS
)
MAXIMUM_TOTAL_TOKENS_PER_TURN = 42_000
TIMEOUT_SECONDS = v168.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v168.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v169-capacity-bound-continuation"
).resolve()


class JudgeV5CalibrationV169Error(RuntimeError):
    """The immutable v169 continuation contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v168_failure() -> dict[str, Any]:
    root = v168.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "spec": root / "fresh-full-capacity-recovery-spec.json",
        "policy": root / "capacity-policy.json",
        "audit": root / "capacity-policy-audit.json",
        "pool": root / "shared-witness-pool.private.json",
        "pointwise": root / "pointwise-input-full.private.json",
        "truth": root / "full-calibration-truth.private.json",
        "selection": root / "full-calibration-selection-audit.json",
        "support": root / "support-output-full.private.json",
        "support_canary": root / "support-canary-output.private.json",
        "support_receipts": root / "support-receipts.private.json",
        "fields": root / "field-output.private.json",
        "field_repeats": root / "field-repeat-output.private.json",
    }
    values = {name: _load_json(path, f"v168 {name}") for name, path in paths.items()}
    terminal, failure, spec, policy = (
        values["terminal"],
        values["failure"],
        values["spec"],
        values["policy"],
    )
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("development_judge_frozen") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("error_class") != "ReserveCapacityError"
        or failure.get("failed_turn_name") != "recovery_alignment_shard_02"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("usage_status") != "complete"
        or failure.get("unknown_usage_turn_count") != 0
        or spec.get("maximum_turn_count") != 64
        or spec.get("retry_count_per_turn") != 0
        or policy.get("maximum_total_tokens_per_turn") != 39500
    ):
        raise JudgeV5CalibrationV169Error("v168 terminal contract drifted")

    completed_names = set(
        v168.SUPPORT_PRIMARY_TURNS
        + (v168.SUPPORT_CANARY_TURN,)
        + v168.FIELD_PRIMARY_TURNS
        + (v168.FIELD_REPEAT_TURN,)
        + v168.ALIGNMENT_PRIMARY_TURNS[:3]
    )
    attempts = failure.get("attempts")
    if (
        not isinstance(attempts, list)
        or len(attempts) != 40
        or {str(row.get("turn_name")) for row in attempts} != completed_names
    ):
        raise JudgeV5CalibrationV169Error("v168 completed-prefix coverage drifted")
    usage = {field: 0 for field in USAGE_FIELDS}
    for attempt in attempts:
        for key in ("capacity", "sidecar", "output"):
            record = attempt.get(key)
            if not isinstance(record, Mapping):
                raise JudgeV5CalibrationV169Error(f"v168 {key} record is missing")
            _verify_record(record)
        measured = _validate_usage(
            _load_json(Path(attempt["sidecar"]["path"]), "v168 measured sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    if usage != failure.get("usage") or usage.get("total_tokens") != 945212:
        raise JudgeV5CalibrationV169Error("v168 measured usage drifted")
    failed_sidecar = _load_json(
        root / "turns/recovery-alignment-shard-02/sidecar.json",
        "v168 failed-bound sidecar",
    )
    if (
        failed_sidecar.get("status") != "completed"
        or failed_sidecar.get("usage_status") != "measured"
        or failed_sidecar.get("usage_complete") is not True
        or failed_sidecar.get("usage", {}).get("total_tokens") != 39678
        or int(failed_sidecar["usage"]["total_tokens"])
        <= int(policy["maximum_total_tokens_per_turn"])
    ):
        raise JudgeV5CalibrationV169Error("v168 capacity-bound evidence drifted")
    cumulative = failure.get("cumulative_known_usage_lower_bound")
    if not isinstance(cumulative, Mapping) or cumulative.get("total_tokens") != 4161686:
        raise JudgeV5CalibrationV169Error("v168 cumulative accounting drifted")

    for record in spec["runtime_files"]:
        _verify_record(record)
    for record in spec["frozen_inputs"]["static_turns"]:
        for key in ("input", "prompt", "schema"):
            _verify_record(record[key])
    for path in paths.values():
        if not path.is_file():
            raise JudgeV5CalibrationV169Error("v168 predecessor artifact disappeared")
    for index, name in enumerate(v168.ALIGNMENT_PRIMARY_TURNS[:3]):
        turn_root = root / "turns" / name.replace("_", "-")
        for filename in (
            "input.private.json",
            "prompt.private.md",
            "schema.json",
            "capacity.json",
            "sidecar.json",
            "output.private.json",
        ):
            if not (turn_root / filename).is_file():
                raise JudgeV5CalibrationV169Error("v168 alignment prefix is incomplete")
        projection_path = turn_root / "structural-projection-audit.json"
        if index < 2 and not projection_path.is_file():
            raise JudgeV5CalibrationV169Error("v168 completed alignment projection is missing")
        if index == 2 and projection_path.exists():
            raise JudgeV5CalibrationV169Error("v168 failed-bound turn advanced after its stop")
    later_names = set(v168.ALIGNMENT_PRIMARY_TURNS[3:]) | {
        v168.ALIGNMENT_CANARY_TURN,
        *v168.VERIFIER_PRIMARY_TURNS,
        *v168.VERIFIER_CANARY_TURNS,
    }
    if any(
        (root / "turns" / name.replace("_", "-") / "sidecar.json").exists()
        for name in later_names
    ):
        raise JudgeV5CalibrationV169Error("v168 contains a turn after its failed prefix")

    alignment_prefix = []
    for name in v168.ALIGNMENT_PRIMARY_TURNS[:3]:
        turn_root = root / "turns" / name.replace("_", "-")
        value = _load_json(turn_root / "input.private.json", f"v168 {name} input")
        output = _load_json(turn_root / "output.private.json", f"v168 {name} output")
        errors = v157.validate_structurally_projectable_output(output, value)
        if errors:
            raise JudgeV5CalibrationV169Error("v168 retained alignment output is invalid")
        projected, audit = v157.project_exact_spans_and_relation(output, value)
        projection_path = turn_root / "structural-projection-audit.json"
        if projection_path.exists() and audit != _load_json(
            projection_path, f"v168 {name} projection audit"
        ):
            raise JudgeV5CalibrationV169Error("v168 retained projection audit drifted")
        alignment_prefix.append({"turn_name": name, "input": value, "output": projected})

    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "alignment_prefix": alignment_prefix,
        "usage": usage,
        "cumulative_usage": {field: int(cumulative[field]) for field in USAGE_FIELDS},
    }


def _alignment_value(
    *, pool: Mapping[str, Any], receipts: Mapping[str, Any], case_ids: Sequence[str], permutation: str
) -> dict[str, Any]:
    return v155._support_positive_alignment_input(
        pool, receipts, case_ids=list(case_ids), permutation=permutation
    )


def _build_capacity_policy(root: Path, source: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V169_CAPACITY_AUDIT_VERSION,
        "phase_id": V169_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v168_failure": source["records"]["failure"],
        "v168_terminal": source["records"]["terminal"],
        "measured_basis": {
            "v168_completed_turn_count": 40,
            "v168_measured_total_tokens": source["usage"]["total_tokens"],
            "v168_measured_maximum_total_tokens": 39678,
            "v168_frozen_maximum_total_tokens": 39500,
            "v168_overrun_tokens": 178,
            "remaining_declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
        "continuation_rule": {
            "reuse_every_completed_valid_v168_prefix_output": True,
            "selection_based_on_output_content": False,
            "replay_completed_v168_turns": False,
            "new_attempt_has_separate_accounting": True,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V169_CAPACITY_POLICY_VERSION,
        "phase_id": V169_PHASE_ID,
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
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v169(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v169 terminal")}
    source = _validate_v168_failure()
    values = source["values"]
    pool = values["pool"]
    pointwise = values["pointwise"]
    truth = values["truth"]
    receipts = values["support_receipts"]
    case_shards = calibration_case_shards(pool)
    if len(case_shards) != 11:
        raise JudgeV5CalibrationV169Error("alignment shard topology drifted")

    paths = {
        "pool": root / "shared-witness-pool.private.json",
        "pointwise": root / "pointwise-input-full.private.json",
        "truth": root / "full-calibration-truth.private.json",
        "support": root / "support-output-full.private.json",
        "support_canary": root / "support-canary-output.private.json",
        "support_receipts": root / "support-receipts.private.json",
        "fields": root / "field-output.private.json",
        "field_repeats": root / "field-repeat-output.private.json",
    }
    for name, path in paths.items():
        _write_immutable(path, values[name])
    selection_path = root / "continuation-selection-audit.json"
    _write_stable_time(
        selection_path,
        {
            "schema_version": V169_SPEC_VERSION,
            "created_at": now_iso(),
            "v168_completed_prefix_turn_count": 40,
            "v168_completed_prefix_reused_in_full": True,
            "v168_completed_prefix_selected_by_lifecycle_not_content": True,
            "v168_completed_turns_replayed": False,
            "v168_failed_turn_had_completed_measured_output": True,
            "v168_failed_turn_output_reused": True,
            "remaining_alignment_primary_indexes": list(ALIGNMENT_PRIMARY_INDEXES),
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )

    turns = []
    for turn_name, index in zip(ALIGNMENT_PRIMARY_TURNS, ALIGNMENT_PRIMARY_INDEXES, strict=True):
        value = _alignment_value(
            pool=pool, receipts=receipts, case_ids=case_shards[index], permutation="base"
        )
        prompt = v130.alignment_prompt_v130(value)
        schema = neutral_alignment_output_schema(value)
        request_paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "alignment_primary",
                "shard_index": index,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": request_paths,
            }
        )
    canary_ids = truth["alignment_canary_case_ids"]
    canary_value = _alignment_value(
        pool=pool, receipts=receipts, case_ids=canary_ids, permutation="balanced_canary"
    )
    canary_prompt = v130.alignment_prompt_v130(canary_value)
    canary_schema = neutral_alignment_output_schema(canary_value)
    canary_paths = _freeze_turn_request(
        root=root,
        turn_name=ALIGNMENT_CANARY_TURN,
        input_value=canary_value,
        prompt=canary_prompt,
        schema=canary_schema,
    )
    canary_turn = {
        "turn_name": ALIGNMENT_CANARY_TURN,
        "turn_role": "alignment_canary",
        "value": canary_value,
        "prompt": canary_prompt,
        "schema": canary_schema,
        "paths": canary_paths,
    }

    capacity = _build_capacity_policy(root, source)
    runtime_files = [_record(Path(__file__)), *values["spec"]["runtime_files"]]
    spec = {
        "schema_version": V169_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V169_PHASE_ID,
        "alignment_model": v168.ALIGNMENT_MODEL,
        "equivalent_pair_verifier_model": v168.VERIFIER_MODEL,
        "reasoning_effort": v168.EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "complete_every_remaining_alignment_and_verifier_turn_after_full_valid_v168_prefix_reuse",
        "case_count": 66,
        "witness_count": 182,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": len(TURN_NAMES),
        "maximum_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "v168_completed_prefix_turn_count": 40,
        "v168_completed_prefix_outputs_reused": True,
        "v168_completed_turns_replayed": False,
        "all_remaining_semantic_turns_fresh": True,
        "prior_output_reuse_is_complete_prefix_and_content_blind": True,
        "reference_truth_exposed_to_model": False,
        "all_primary_equivalent_pairs_verified": True,
        "strict_canary_no_repair_call": True,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": source["records"],
        "runtime_files": runtime_files,
        "frozen_instructions": {
            "support_sha256": sha256_text(v143.support_base_instructions_v143()),
            "field_sha256": sha256_text(v146.base_instructions_v146()),
            "alignment_sha256": sha256_text(v130.alignment_instructions_v130()),
        },
        "frozen_inputs": {
            **{name: _record(path) for name, path in paths.items()},
            "selection": _record(selection_path),
            "v168_alignment_prefix": [
                {
                    "turn_name": row["turn_name"],
                    "input": _record(
                        source["root"]
                        / "turns"
                        / row["turn_name"].replace("_", "-")
                        / "input.private.json"
                    ),
                    "output": _record(
                        source["root"]
                        / "turns"
                        / row["turn_name"].replace("_", "-")
                        / "output.private.json"
                    ),
                }
                for row in source["alignment_prefix"]
            ],
            "remaining_alignment_turns": [
                {
                    "turn_name": row["turn_name"],
                    "input": _record(row["paths"]["input"]),
                    "prompt": _record(row["paths"]["prompt"]),
                    "schema": _record(row["paths"]["schema"]),
                }
                for row in [*turns, canary_turn]
            ],
            "field_protocol": values["spec"]["frozen_inputs"]["field_protocol"],
            "alignment_protocol": values["spec"]["frozen_inputs"]["alignment_protocol"],
            "reference": values["spec"]["frozen_inputs"]["reference"],
            "truth_source": values["spec"]["frozen_inputs"]["truth_source"],
        },
        "privacy": "private_source_event_output_truth_and_mapping_sanitized_terminal_only",
    }
    spec_path = root / "capacity-bound-continuation-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "canary_turn": canary_turn,
        "data": {
            "pool": pool,
            "pointwise": pointwise,
            "truth": truth,
            "receipts": receipts,
            "case_shards": case_shards,
        },
        "source": source,
    }


def _write_failure(root: Path, source: Mapping[str, Any], turn_name: Optional[str], error_class: str) -> dict[str, Any]:
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
            measured = _validate_usage(
                _load_json(Path(record["path"]), "v169 measured sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    predecessor = source["cumulative_usage"]
    cumulative = _sum_usage(predecessor, usage)
    failure = {
        "schema_version": V169_FAILURE_VERSION,
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
        "schema_version": V169_TERMINAL_VERSION,
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


async def run_v169(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v169 terminal")
    frozen = freeze_v169(output_dir=root, timeout_seconds=timeout_seconds)
    source = frozen["source"]
    current_turn: Optional[str] = None
    sidecars = []
    try:
        base_inputs = [row["input"] for row in source["alignment_prefix"]]
        base_outputs = [row["output"] for row in source["alignment_prefix"]]
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar = await v167._run_alignment_turn(
                    client=client,
                    root=root,
                    turn_name=current_turn,
                    value=turn["value"],
                    model=v168.ALIGNMENT_MODEL,
                    timeout_seconds=timeout_seconds,
                    policy_path=frozen["capacity_policy"],
                )
                base_inputs.append(turn["value"])
                base_outputs.append(output)
                sidecars.append(sidecar)

            base_output = v155._merge_outputs(base_outputs, "cases")
            all_case_ids = [str(row["case_id"]) for row in frozen["data"]["pool"]["cases"]]
            base_input = _alignment_value(
                pool=frozen["data"]["pool"],
                receipts=frozen["data"]["receipts"],
                case_ids=all_case_ids,
                permutation="base",
            )
            if validate_neutral_alignment_output(base_output, base_input):
                raise JudgeV5CalibrationV169Error("aggregate base alignment is invalid")
            base_normalized = normalize_neutral_alignment_output(base_output, base_input)
            _write_immutable(root / "alignment-base-output.private.json", base_output)

            current_turn = ALIGNMENT_CANARY_TURN
            canary_output, sidecar = await v167._run_alignment_turn(
                client=client,
                root=root,
                turn_name=current_turn,
                value=frozen["canary_turn"]["value"],
                model=v168.ALIGNMENT_MODEL,
                timeout_seconds=timeout_seconds,
                policy_path=frozen["capacity_policy"],
            )
            sidecars.append(sidecar)
            canary_normalized = normalize_neutral_alignment_output(
                canary_output, frozen["canary_turn"]["value"]
            )
            _write_immutable(root / "alignment-canary-output.private.json", canary_output)

            base_pairs = v167._pair_rows(
                normalized=base_normalized, alignment_input=base_input, prefix="v169-base"
            )
            canary_pairs = v167._pair_rows(
                normalized=canary_normalized,
                alignment_input=frozen["canary_turn"]["value"],
                prefix="v169-canary",
            )
            base_shards = v167._balanced_shards(
                [row["pair_case_id"] for row in base_pairs], len(VERIFIER_PRIMARY_TURNS)
            )
            canary_shards = v167._balanced_shards(
                [row["pair_case_id"] for row in canary_pairs], len(VERIFIER_CANARY_TURNS)
            )
            base_by_id = {row["pair_case_id"]: row for row in base_pairs}
            canary_by_id = {row["pair_case_id"]: row for row in canary_pairs}
            _write_immutable(
                root / "equivalent-pair-verifier-selection-audit.json",
                {
                    "schema_version": V169_SPEC_VERSION,
                    "base_primary_equivalent_pair_count": len(base_pairs),
                    "canary_primary_equivalent_pair_count": len(canary_pairs),
                    "all_primary_equivalent_pairs_selected": True,
                    "base_turn_count": len(base_shards),
                    "canary_turn_count": len(canary_shards),
                    "truth_labels_used_for_selection": False,
                },
            )

            verifier_outputs, verifier_inputs = [], []
            for turn_name, shard_ids in zip(
                VERIFIER_PRIMARY_TURNS, base_shards, strict=True
            ):
                rows = [base_by_id[key] for key in shard_ids]
                value = v167._verifier_turn_value(
                    template=base_input, rows=rows, permutation="base"
                )
                current_turn = turn_name
                output, sidecar = await v167._run_alignment_turn(
                    client=client,
                    root=root,
                    turn_name=turn_name,
                    value=value,
                    model=v168.VERIFIER_MODEL,
                    timeout_seconds=timeout_seconds,
                    policy_path=frozen["capacity_policy"],
                )
                verifier_outputs.append(output)
                verifier_inputs.append(value)
                sidecars.append(sidecar)
            verifier_normalized = v167._merge_normalized(
                verifier_outputs, verifier_inputs
            )
            final_base = v167._apply_verifier(
                normalized=base_normalized,
                pair_rows=base_pairs,
                verifier=verifier_normalized,
            )
            _write_immutable(root / "alignment-final-base.private.json", final_base)

            verifier_canary_outputs, verifier_canary_inputs = [], []
            for turn_name, shard_ids in zip(
                VERIFIER_CANARY_TURNS, canary_shards, strict=True
            ):
                rows = [canary_by_id[key] for key in shard_ids]
                value = v167._verifier_turn_value(
                    template=frozen["canary_turn"]["value"],
                    rows=rows,
                    permutation="balanced_canary",
                )
                current_turn = turn_name
                output, sidecar = await v167._run_alignment_turn(
                    client=client,
                    root=root,
                    turn_name=turn_name,
                    value=value,
                    model=v168.VERIFIER_MODEL,
                    timeout_seconds=timeout_seconds,
                    policy_path=frozen["capacity_policy"],
                )
                verifier_canary_outputs.append(output)
                verifier_canary_inputs.append(value)
                sidecars.append(sidecar)
            verifier_canary_normalized = v167._merge_normalized(
                verifier_canary_outputs, verifier_canary_inputs
            )
            final_canary = v167._apply_verifier(
                normalized=canary_normalized,
                pair_rows=canary_pairs,
                verifier=verifier_canary_normalized,
            )
            _write_immutable(root / "alignment-final-canary.private.json", final_canary)

        base_cases = {
            str(row["case_id"]): v130._project_alignment(row)
            for row in final_base["cases"]
        }
        canary_cases = {
            str(row["case_id"]): v130._project_alignment(row)
            for row in final_canary["cases"]
        }
        canary_exact = sum(base_cases[key] == canary_cases[key] for key in canary_cases)
        score = v155.score_v155(
            support=source["values"]["support"],
            support_canary=source["values"]["support_canary"],
            fields=source["values"]["fields"],
            field_repeats=source["values"]["field_repeats"],
            alignment=final_base,
            truth=frozen["data"]["truth"],
            raw_alignment_disagreement_count=len(canary_cases) - canary_exact,
            final_canary_exact_count=canary_exact,
        )
        score["schema_version"] = V169_SCORE_VERSION
        score["equivalent_pair_verifier_base_count"] = len(base_pairs)
        score["equivalent_pair_verifier_canary_count"] = len(canary_pairs)
        score["v168_completed_prefix_turn_count"] = 40
        score_path = root / "fresh-full-capacity-continuation-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        protocol_path = root / "development-judge-protocol-v169.json"
        if passed:
            _write_immutable(
                protocol_path,
                {
                    "schema_version": V169_PROTOCOL_VERSION,
                    "frozen_at": now_iso(),
                    "support_model": v168.SUPPORT_MODEL,
                    "field_model": v168.FIELD_MODEL,
                    "alignment_model": v168.ALIGNMENT_MODEL,
                    "equivalent_pair_verifier_model": v168.VERIFIER_MODEL,
                    "reasoning_effort": v168.EFFORT,
                    "support_instructions_sha256": sha256_text(
                        v143.support_base_instructions_v143()
                    ),
                    "field_instructions_sha256": sha256_text(
                        v146.base_instructions_v146()
                    ),
                    "alignment_instructions_sha256": sha256_text(
                        v130.alignment_instructions_v130()
                    ),
                    "field_protocol": frozen["spec"]["frozen_inputs"]["field_protocol"],
                    "alignment_protocol": frozen["spec"]["frozen_inputs"]["alignment_protocol"],
                    "reference": frozen["spec"]["frozen_inputs"]["reference"],
                    "truth": frozen["spec"]["frozen_inputs"]["truth_source"],
                    "v168_completed_prefix_reused": True,
                    "v168_completed_turns_replayed": False,
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
        predecessor = source["cumulative_usage"]
        cumulative = _sum_usage(predecessor, accounting["usage"])
        terminal = {
            "schema_version": V169_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v169_full_development_calibration_passed_selection_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v169_full_development_calibration_passed"
                if passed
                else "v169_full_development_calibration_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "development_judge_frozen": passed,
            "selection_authorized": passed,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "v168_completed_prefix_turn_count": 40,
            "v168_completed_prefix_usage": source["usage"],
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
        return _write_failure(root, source, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, source, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run the v169 capacity-bound continuation")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v169(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "development_judge_frozen": terminal.get("development_judge_frozen", False),
                "selection_authorized": terminal.get("selection_authorized", False),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
