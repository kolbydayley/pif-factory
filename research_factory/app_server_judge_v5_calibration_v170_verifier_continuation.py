from __future__ import annotations

"""Finish the full calibration from retained v168/v169 alignment evidence."""

import argparse
import asyncio
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from . import app_server_judge_v5_calibration_v167_fresh_full_replacement as v167
from . import app_server_judge_v5_calibration_v168_capacity_recovery as v168
from . import app_server_judge_v5_calibration_v169_capacity_bound_continuation as v169
from .app_server_judge_v5 import normalize_neutral_alignment_output
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V170_SPEC_VERSION = "pif_app_server_judge_v5_4_v170_spec_v1"
V170_SCORE_VERSION = "pif_app_server_judge_v5_4_v170_score_v1"
V170_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v170_development_protocol_v1"
V170_FAILURE_VERSION = "pif_app_server_judge_v5_4_v170_failure_v1"
V170_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v170_terminal_v1"
V170_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V170_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V170_PHASE_ID = "judge_v5_4_v170_verifier_continuation"

VERIFIER_PRIMARY_TURNS = tuple(f"final_verifier_shard_{index:02d}" for index in range(12))
VERIFIER_CANARY_TURNS = tuple(f"final_verifier_canary_{index:02d}" for index in range(3))
TURN_NAMES = VERIFIER_PRIMARY_TURNS + VERIFIER_CANARY_TURNS
MAXIMUM_TOTAL_TOKENS_PER_TURN = 40_000
TIMEOUT_SECONDS = v169.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v169.DEFAULT_OUTPUT_ROOT.parent / "judge-calibration-v5_4-v170-verifier-continuation"
).resolve()


class JudgeV5CalibrationV170Error(RuntimeError):
    """The immutable v170 verifier continuation cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _project_retained_turn(turn_root: Path, label: str) -> tuple[dict[str, Any], dict[str, Any]]:
    value = _load_json(turn_root / "input.private.json", f"{label} input")
    output = _load_json(turn_root / "output.private.json", f"{label} output")
    errors = v157.validate_structurally_projectable_output(output, value)
    if errors:
        raise JudgeV5CalibrationV170Error(f"{label} retained output is invalid")
    projected, audit = v157.project_exact_spans_and_relation(output, value)
    audit_path = turn_root / "structural-projection-audit.json"
    if audit_path.exists() and audit != _load_json(audit_path, f"{label} projection audit"):
        raise JudgeV5CalibrationV170Error(f"{label} projection audit drifted")
    return value, projected


def _validate_v169_failure() -> dict[str, Any]:
    root = v169.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "spec": root / "capacity-bound-continuation-spec.json",
        "policy": root / "capacity-policy.json",
        "audit": root / "capacity-policy-audit.json",
        "pool": root / "shared-witness-pool.private.json",
        "pointwise": root / "pointwise-input-full.private.json",
        "truth": root / "full-calibration-truth.private.json",
        "support": root / "support-output-full.private.json",
        "support_canary": root / "support-canary-output.private.json",
        "support_receipts": root / "support-receipts.private.json",
        "fields": root / "field-output.private.json",
        "field_repeats": root / "field-repeat-output.private.json",
        "selection": root / "continuation-selection-audit.json",
    }
    values = {name: _load_json(path, f"v169 {name}") for name, path in paths.items()}
    terminal, failure, spec, policy = (
        values["terminal"], values["failure"], values["spec"], values["policy"]
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
        or failure.get("failed_turn_name") != v169.ALIGNMENT_CANARY_TURN
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("usage_status") != "complete"
        or failure.get("unknown_usage_turn_count") != 0
        or spec.get("maximum_turn_count") != 24
        or spec.get("retry_count_per_turn") != 0
        or policy.get("maximum_total_tokens_per_turn") != 42000
    ):
        raise JudgeV5CalibrationV170Error("v169 terminal contract drifted")

    completed_names = set(v169.ALIGNMENT_PRIMARY_TURNS + (v169.ALIGNMENT_CANARY_TURN,))
    attempts = failure.get("attempts")
    if (
        not isinstance(attempts, list)
        or len(attempts) != 9
        or {str(row.get("turn_name")) for row in attempts} != completed_names
    ):
        raise JudgeV5CalibrationV170Error("v169 completed-prefix coverage drifted")
    usage = {field: 0 for field in USAGE_FIELDS}
    for attempt in attempts:
        for key in ("capacity", "sidecar", "output"):
            record = attempt.get(key)
            if not isinstance(record, Mapping):
                raise JudgeV5CalibrationV170Error(f"v169 {key} record is missing")
            _verify_record(record)
        measured = _validate_usage(
            _load_json(Path(attempt["sidecar"]["path"]), "v169 measured sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    if usage != failure.get("usage") or usage.get("total_tokens") != 352312:
        raise JudgeV5CalibrationV170Error("v169 measured usage drifted")
    canary_sidecar = _load_json(
        root / "turns" / v169.ALIGNMENT_CANARY_TURN.replace("_", "-") / "sidecar.json",
        "v169 canary sidecar",
    )
    if (
        canary_sidecar.get("status") != "completed"
        or canary_sidecar.get("usage_status") != "measured"
        or canary_sidecar.get("usage_complete") is not True
        or canary_sidecar.get("usage", {}).get("total_tokens") != 50994
        or int(canary_sidecar["usage"]["total_tokens"])
        <= int(policy["maximum_total_tokens_per_turn"])
    ):
        raise JudgeV5CalibrationV170Error("v169 canary capacity evidence drifted")
    cumulative = failure.get("cumulative_known_usage_lower_bound")
    if not isinstance(cumulative, Mapping) or cumulative.get("total_tokens") != 4513998:
        raise JudgeV5CalibrationV170Error("v169 cumulative accounting drifted")
    for record in spec["runtime_files"]:
        _verify_record(record)
    for record in spec["frozen_inputs"]["remaining_alignment_turns"]:
        for key in ("input", "prompt", "schema"):
            _verify_record(record[key])
    for path in paths.values():
        if not path.is_file():
            raise JudgeV5CalibrationV170Error("v169 predecessor artifact disappeared")
    if any(
        (root / "turns" / name.replace("_", "-") / "sidecar.json").exists()
        for name in (*v169.VERIFIER_PRIMARY_TURNS, *v169.VERIFIER_CANARY_TURNS)
    ):
        raise JudgeV5CalibrationV170Error("v169 contains a verifier turn after its stop")
    if (root / "equivalent-pair-verifier-selection-audit.json").exists():
        raise JudgeV5CalibrationV170Error("v169 advanced to verifier selection after its stop")

    v168_source = v169._validate_v168_failure()
    base_inputs = [row["input"] for row in v168_source["alignment_prefix"]]
    base_outputs = [row["output"] for row in v168_source["alignment_prefix"]]
    for name in v169.ALIGNMENT_PRIMARY_TURNS:
        value, output = _project_retained_turn(
            root / "turns" / name.replace("_", "-"), f"v169 {name}"
        )
        base_inputs.append(value)
        base_outputs.append(output)
    canary_root = root / "turns" / v169.ALIGNMENT_CANARY_TURN.replace("_", "-")
    if (canary_root / "structural-projection-audit.json").exists():
        raise JudgeV5CalibrationV170Error("v169 failed canary advanced after its bound stop")
    canary_input, canary_output = _project_retained_turn(
        canary_root, "v169 alignment canary"
    )

    pool = values["pool"]
    receipts = values["support_receipts"]
    all_case_ids = [str(row["case_id"]) for row in pool["cases"]]
    base_input = v155._support_positive_alignment_input(
        pool, receipts, case_ids=all_case_ids, permutation="base"
    )
    base_output = v155._merge_outputs(base_outputs, "cases")
    base_normalized = normalize_neutral_alignment_output(base_output, base_input)
    canary_normalized = normalize_neutral_alignment_output(canary_output, canary_input)
    base_pairs = v167._pair_rows(
        normalized=base_normalized, alignment_input=base_input, prefix="v170-base"
    )
    canary_pairs = v167._pair_rows(
        normalized=canary_normalized, alignment_input=canary_input, prefix="v170-canary"
    )
    if len(base_pairs) != 56 or len(canary_pairs) != 10:
        raise JudgeV5CalibrationV170Error("retained equivalent-pair topology drifted")

    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "usage": usage,
        "cumulative_usage": {field: int(cumulative[field]) for field in USAGE_FIELDS},
        "v168_source": v168_source,
        "base_input": base_input,
        "base_output": base_output,
        "base_normalized": base_normalized,
        "canary_input": canary_input,
        "canary_output": canary_output,
        "canary_normalized": canary_normalized,
        "base_pairs": base_pairs,
        "canary_pairs": canary_pairs,
    }


def _build_capacity_policy(root: Path, source: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V170_CAPACITY_AUDIT_VERSION,
        "phase_id": V170_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v169_failure": source["records"]["failure"],
        "v169_terminal": source["records"]["terminal"],
        "measured_basis": {
            "v169_completed_turn_count": 9,
            "v169_measured_total_tokens": source["usage"]["total_tokens"],
            "v169_failed_canary_total_tokens": 50994,
            "v169_failed_canary_is_not_in_remaining_workload": True,
            "prior_comparable_sol_verifier_turn_count": 6,
            "prior_comparable_sol_verifier_maximum_total_tokens": 33290,
            "remaining_declared_turn_count": len(TURN_NAMES),
            "maximum_cases_per_remaining_turn": 5,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
        "continuation_rule": {
            "reuse_every_completed_valid_v168_v169_alignment_output": True,
            "selection_based_on_output_content": False,
            "replay_completed_predecessor_turns": False,
            "run_every_equivalent_pair_verifier_turn": True,
            "new_attempt_has_separate_accounting": True,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V170_CAPACITY_POLICY_VERSION,
        "phase_id": V170_PHASE_ID,
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


def freeze_v170(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v170 terminal")}
    source = _validate_v169_failure()
    values = source["values"]
    paths = {
        "pool": root / "shared-witness-pool.private.json",
        "pointwise": root / "pointwise-input-full.private.json",
        "truth": root / "full-calibration-truth.private.json",
        "support": root / "support-output-full.private.json",
        "support_canary": root / "support-canary-output.private.json",
        "support_receipts": root / "support-receipts.private.json",
        "fields": root / "field-output.private.json",
        "field_repeats": root / "field-repeat-output.private.json",
        "base_alignment": root / "alignment-base-retained.private.json",
        "canary_alignment": root / "alignment-canary-retained.private.json",
    }
    for name, path in paths.items():
        if name == "base_alignment":
            value = source["base_output"]
        elif name == "canary_alignment":
            value = source["canary_output"]
        else:
            value = values[name]
        _write_immutable(path, value)

    base_shards = v167._balanced_shards(
        [row["pair_case_id"] for row in source["base_pairs"]], len(VERIFIER_PRIMARY_TURNS)
    )
    canary_shards = v167._balanced_shards(
        [row["pair_case_id"] for row in source["canary_pairs"]], len(VERIFIER_CANARY_TURNS)
    )
    base_by_id = {row["pair_case_id"]: row for row in source["base_pairs"]}
    canary_by_id = {row["pair_case_id"]: row for row in source["canary_pairs"]}
    turns = []
    for turn_name, shard_ids in zip(VERIFIER_PRIMARY_TURNS, base_shards, strict=True):
        value = v167._verifier_turn_value(
            template=source["base_input"],
            rows=[base_by_id[key] for key in shard_ids],
            permutation="base",
        )
        prompt = v130.alignment_prompt_v130(value)
        schema = v169.neutral_alignment_output_schema(value)
        request_paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "verifier_primary",
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": request_paths,
            }
        )
    for turn_name, shard_ids in zip(VERIFIER_CANARY_TURNS, canary_shards, strict=True):
        value = v167._verifier_turn_value(
            template=source["canary_input"],
            rows=[canary_by_id[key] for key in shard_ids],
            permutation="balanced_canary",
        )
        prompt = v130.alignment_prompt_v130(value)
        schema = v169.neutral_alignment_output_schema(value)
        request_paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "verifier_canary",
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": request_paths,
            }
        )
    selection_path = root / "equivalent-pair-verifier-selection-audit.json"
    _write_stable_time(
        selection_path,
        {
            "schema_version": V170_SPEC_VERSION,
            "created_at": now_iso(),
            "base_primary_equivalent_pair_count": len(source["base_pairs"]),
            "canary_primary_equivalent_pair_count": len(source["canary_pairs"]),
            "all_primary_equivalent_pairs_selected": True,
            "base_turn_count": len(base_shards),
            "canary_turn_count": len(canary_shards),
            "base_cases_per_turn": [len(shard) for shard in base_shards],
            "canary_cases_per_turn": [len(shard) for shard in canary_shards],
            "truth_labels_used_for_selection": False,
            "selection_authorized": False,
            "holdout_authorized": False,
        },
        "created_at",
    )
    capacity = _build_capacity_policy(root, source)
    spec = {
        "schema_version": V170_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V170_PHASE_ID,
        "equivalent_pair_verifier_model": v168.VERIFIER_MODEL,
        "reasoning_effort": v168.EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "complete_all_equivalent_pair_verifier_turns_from_full_retained_alignment_prefix",
        "case_count": 66,
        "witness_count": 182,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": len(TURN_NAMES),
        "maximum_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "v168_completed_prefix_turn_count": 40,
        "v169_completed_prefix_turn_count": 9,
        "predecessor_outputs_reused": True,
        "predecessor_turns_replayed": False,
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
        "runtime_files": [_record(Path(__file__)), *values["spec"]["runtime_files"]],
        "frozen_instructions": {
            "support_sha256": sha256_text(v143.support_base_instructions_v143()),
            "field_sha256": sha256_text(v146.base_instructions_v146()),
            "alignment_sha256": sha256_text(v130.alignment_instructions_v130()),
        },
        "frozen_inputs": {
            **{name: _record(path) for name, path in paths.items()},
            "selection": _record(selection_path),
            "field_protocol": values["spec"]["frozen_inputs"]["field_protocol"],
            "alignment_protocol": values["spec"]["frozen_inputs"]["alignment_protocol"],
            "reference": values["spec"]["frozen_inputs"]["reference"],
            "truth_source": values["spec"]["frozen_inputs"]["truth_source"],
            "turns": [
                {
                    "turn_name": row["turn_name"],
                    "role": row["turn_role"],
                    "input": _record(row["paths"]["input"]),
                    "prompt": _record(row["paths"]["prompt"]),
                    "schema": _record(row["paths"]["schema"]),
                }
                for row in turns
            ],
        },
        "privacy": "private_source_event_output_truth_and_mapping_sanitized_terminal_only",
    }
    spec_path = root / "verifier-continuation-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "data": {
            "truth": values["truth"],
            "base_input": source["base_input"],
            "canary_input": source["canary_input"],
            "base_normalized": source["base_normalized"],
            "canary_normalized": source["canary_normalized"],
            "base_pairs": source["base_pairs"],
            "canary_pairs": source["canary_pairs"],
        },
        "source": source,
    }


def _write_failure(
    root: Path, source: Mapping[str, Any], turn_name: Optional[str], error_class: str
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
            measured = _validate_usage(
                _load_json(Path(record["path"]), "v170 measured sidecar")
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
        "schema_version": V170_FAILURE_VERSION,
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
        "schema_version": V170_TERMINAL_VERSION,
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


async def run_v170(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v170 terminal")
    frozen = freeze_v170(output_dir=root, timeout_seconds=timeout_seconds)
    source = frozen["source"]
    current_turn: Optional[str] = None
    sidecars = []
    try:
        primary_outputs, primary_inputs = [], []
        canary_outputs, canary_inputs = [], []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar = await v167._run_alignment_turn(
                    client=client,
                    root=root,
                    turn_name=current_turn,
                    value=turn["value"],
                    model=v168.VERIFIER_MODEL,
                    timeout_seconds=timeout_seconds,
                    policy_path=frozen["capacity_policy"],
                )
                if turn["turn_role"] == "verifier_primary":
                    primary_outputs.append(output)
                    primary_inputs.append(turn["value"])
                else:
                    canary_outputs.append(output)
                    canary_inputs.append(turn["value"])
                sidecars.append(sidecar)

        primary_normalized = v167._merge_normalized(primary_outputs, primary_inputs)
        canary_normalized = v167._merge_normalized(canary_outputs, canary_inputs)
        final_base = v167._apply_verifier(
            normalized=frozen["data"]["base_normalized"],
            pair_rows=frozen["data"]["base_pairs"],
            verifier=primary_normalized,
        )
        final_canary = v167._apply_verifier(
            normalized=frozen["data"]["canary_normalized"],
            pair_rows=frozen["data"]["canary_pairs"],
            verifier=canary_normalized,
        )
        _write_immutable(root / "alignment-final-base.private.json", final_base)
        _write_immutable(root / "alignment-final-canary.private.json", final_canary)

        base_cases = {
            str(row["case_id"]): v130._project_alignment(row) for row in final_base["cases"]
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
        score["schema_version"] = V170_SCORE_VERSION
        score["equivalent_pair_verifier_base_count"] = len(frozen["data"]["base_pairs"])
        score["equivalent_pair_verifier_canary_count"] = len(
            frozen["data"]["canary_pairs"]
        )
        score["retained_predecessor_turn_count"] = 49
        score_path = root / "fresh-full-verifier-continuation-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        protocol_path = root / "development-judge-protocol-v170.json"
        if passed:
            _write_immutable(
                protocol_path,
                {
                    "schema_version": V170_PROTOCOL_VERSION,
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
                    "v168_v169_complete_prefix_reused": True,
                    "predecessor_turns_replayed": False,
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
            "schema_version": V170_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v170_full_development_calibration_passed_selection_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v170_full_development_calibration_passed"
                if passed
                else "v170_full_development_calibration_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "development_judge_frozen": passed,
            "selection_authorized": passed,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "v168_completed_prefix_turn_count": 40,
            "v169_completed_prefix_turn_count": 9,
            "v168_completed_prefix_usage": source["v168_source"]["usage"],
            "v169_completed_prefix_usage": source["usage"],
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "equivalent_pair_verifier_base_count": len(frozen["data"]["base_pairs"]),
            "equivalent_pair_verifier_canary_count": len(
                frozen["data"]["canary_pairs"]
            ),
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
    parser = argparse.ArgumentParser(description="Run v170 verifier continuation")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v170(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
