from __future__ import annotations

"""Adopt v179's completed output and continue phase one under a measured bound."""

import argparse
import asyncio
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from . import app_server_judge_v5_selection_v177_alignment_scale_diagnostic as v177
from . import app_server_judge_v5_selection_v178_alignment_scale_postprocess_recovery as v178
from . import app_server_judge_v5_selection_v179_primary_alignment_phase1 as v179
from .app_server_judge_v5 import normalize_neutral_alignment_output
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
    _validate_completed_turn,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V180_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v180_spec_v1"
V180_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v180_terminal_v1"
V180_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v180_failure_v1"
V180_ADOPTION_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v180_adoption_v1"
V180_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V180_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V180_PHASE_ID = "judge_v5_4_selection_v180_primary_alignment_phase1_bound_recovery1"

MODEL = v177.MODEL
EFFORT = v177.EFFORT
RECOVERY_START_INDEX = 1
FRESH_CASE_COUNT = 4
TURN_NAMES = tuple(
    f"selection_alignment_primary_phase1_recovery1_{index:02d}"
    for index in range(FRESH_CASE_COUNT)
)
MAXIMUM_TOTAL_TOKENS_PER_TURN = 160_000
MAXIMUM_PROMPT_BYTES = v177.MAXIMUM_PROMPT_BYTES
MAXIMUM_SCHEMA_BYTES = v177.MAXIMUM_SCHEMA_BYTES
TIMEOUT_SECONDS = v177.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v179.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v180-primary-alignment-phase1-bound-recovery1"
).resolve()


class JudgeV5SelectionV180Error(RuntimeError):
    """The immutable v180 policy-bound recovery cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _expected_partition_artifact(
    partition: Mapping[str, Any], predecessor: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "schema_version": v179.V179_PARTITION_VERSION,
        "adopted_case_id": partition["adopted_case"]["case_id"],
        "adopted_v177_base_output": predecessor["v177"]["records"]["base_projected"],
        "phase_case_ids": [
            [row["case_id"] for row in phase] for phase in partition["phases"]
        ],
        "phase_prompt_bytes": [
            [row["prompt_bytes"] for row in phase] for phase in partition["phases"]
        ],
        "phase_schema_bytes": [
            [row["schema_bytes"] for row in phase] for phase in partition["phases"]
        ],
        "all_nonempty_case_count": 28,
        "adopted_case_count": 1,
        "fresh_primary_case_count": 27,
        "phase_count": 3,
        "cases_per_phase": v179.PHASE_CASE_COUNT,
        "partition_uses_prompt_bytes_and_opaque_case_id_only": True,
        "semantic_labels_used_for_partition": False,
    }


def _validate_v179_policy_failure() -> dict[str, Any]:
    root = v179.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "spec": root / "primary-alignment-phase1-spec.json",
        "partition": root / "primary-alignment-partition.private.json",
        "policy": root / "capacity-policy.json",
    }
    values = {name: _load_json(path, f"v179 {name}") for name, path in paths.items()}
    terminal, failure, spec = values["terminal"], values["failure"], values["spec"]
    usage = {
        "cached_input_tokens": 93056,
        "input_tokens": 105923,
        "output_tokens": 4354,
        "reasoning_output_tokens": 232,
        "total_tokens": 110277,
    }
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != usage
        or terminal.get("primary_phase2_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != v179.TURN_NAMES[0]
        or failure.get("error_class") != "ReserveCapacityError"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("accounting_complete") is not True
        or failure.get("usage_status") != "complete"
        or failure.get("usage") != usage
        or failure.get("unknown_usage_turn_count") != 0
        or spec.get("state") != "frozen_before_model_calls"
        or spec.get("turn_plan") != list(v179.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5SelectionV180Error("v179 immutable policy failure contract drifted")
    if terminal.get("failure") != _record(paths["failure"]):
        raise JudgeV5SelectionV180Error("v179 failure binding drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV180Error("v179 runtime record drifted")

    predecessor = v179._validate_v178_success()
    partition = v179.build_primary_partition(predecessor)
    persisted_partition = dict(values["partition"])
    persisted_partition.pop("created_at", None)
    if persisted_partition != _expected_partition_artifact(partition, predecessor):
        raise JudgeV5SelectionV180Error("v179 primary partition drifted")
    if (
        spec.get("frozen_inputs", {}).get("primary_partition") != _record(paths["partition"])
        or spec.get("capacity_policy") != _record(paths["policy"])
        or spec.get("predecessor") != predecessor["records"]
    ):
        raise JudgeV5SelectionV180Error("v179 frozen input binding drifted")

    frozen_turns = spec.get("frozen_inputs", {}).get("turns") or []
    if len(frozen_turns) != len(v179.TURN_NAMES):
        raise JudgeV5SelectionV180Error("v179 frozen turn coverage drifted")
    row = partition["phases"][0][0]
    frozen_turn = frozen_turns[0]
    turn_root = root / "turns" / v179.TURN_NAMES[0].replace("_", "-")
    turn_paths = {
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }
    if (
        frozen_turn.get("case_id") != row["case_id"]
        or frozen_turn.get("input") != _record(turn_paths["input"])
        or frozen_turn.get("prompt") != _record(turn_paths["prompt"])
        or frozen_turn.get("schema") != _record(turn_paths["schema"])
        or _load_json(turn_paths["input"], "v179 completed input") != row["value"]
        or turn_paths["prompt"].read_text() != row["prompt"]
        or _load_json(turn_paths["schema"], "v179 completed schema") != row["schema"]
    ):
        raise JudgeV5SelectionV180Error("v179 completed request drifted")
    output, sidecar = _validate_completed_turn(
        paths=turn_paths,
        prompt=row["prompt"],
        schema=row["schema"],
        base_instructions=v130.alignment_instructions_v130(),
        model=MODEL,
        effort=EFFORT,
        policy_path=paths["policy"],
        output_validator=lambda candidate: v157.validate_structurally_projectable_output(
            candidate, row["value"]
        ),
    )
    if _validate_usage(sidecar) != usage or usage["total_tokens"] <= v179.MAXIMUM_TOTAL_TOKENS_PER_TURN:
        raise JudgeV5SelectionV180Error("v179 measured bound failure drifted")
    projected, projection_audit = v157.project_exact_spans_and_relation(output, row["value"])
    normalized = normalize_neutral_alignment_output(projected, row["value"])
    if (
        projection_audit.get("dropped_nonexact_span_count") != 0
        or projection_audit.get("semantic_checklist_decisions_changed") is not False
        or projection_audit.get("witness_assignments_changed") is not False
        or len(normalized.get("cases") or []) != 1
    ):
        raise JudgeV5SelectionV180Error("v179 completed output is not safely adoptable")
    expected_cumulative = _sum_usage(predecessor["cumulative_usage"], usage)
    if terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative:
        raise JudgeV5SelectionV180Error("v179 cumulative usage drifted")
    for later in frozen_turns[1:]:
        later_root = root / "turns" / str(later["turn_name"]).replace("_", "-")
        if any((later_root / name).exists() for name in ("capacity.json", "sidecar.json", "output.private.json")):
            raise JudgeV5SelectionV180Error("v179 executed a turn after its failed bound")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "predecessor": predecessor,
        "partition": partition,
        "row": row,
        "turn_paths": turn_paths,
        "output": output,
        "sidecar": sidecar,
        "projected": projected,
        "projection_audit": projection_audit,
        "normalized": normalized,
        "usage": usage,
        "cumulative_usage": expected_cumulative,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V180_CAPACITY_AUDIT_VERSION,
        "phase_id": V180_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v179_terminal": predecessor["records"]["terminal"],
        "v179_failure": predecessor["records"]["failure"],
        "measured_basis": {
            "v179_completed_turn_total_tokens": predecessor["usage"]["total_tokens"],
            "v179_failed_frozen_bound": v179.MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "declared_turn_count": len(TURN_NAMES),
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V180_CAPACITY_POLICY_VERSION,
        "phase_id": V180_PHASE_ID,
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


def freeze_v180(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v180 terminal")}
    predecessor = _validate_v179_policy_failure()
    rows = predecessor["partition"]["phases"][0][
        RECOVERY_START_INDEX : RECOVERY_START_INDEX + FRESH_CASE_COUNT
    ]
    if len(rows) != FRESH_CASE_COUNT:
        raise JudgeV5SelectionV180Error("v180 recovery tranche coverage drifted")

    adopted_projected_path = root / "adopted-v179-alignment-projected.private.json"
    adopted_audit_path = root / "adopted-v179-structural-projection-audit.json"
    adopted_normalized_path = root / "adopted-v179-alignment-normalized.private.json"
    _write_immutable(adopted_projected_path, predecessor["projected"])
    _write_immutable(adopted_audit_path, predecessor["projection_audit"])
    _write_immutable(adopted_normalized_path, predecessor["normalized"])
    adoption_path = root / "v179-output-adoption-receipt.json"
    adoption = {
        "schema_version": V180_ADOPTION_VERSION,
        "created_at": now_iso(),
        "v179_terminal": predecessor["records"]["terminal"],
        "v179_failure": predecessor["records"]["failure"],
        "v179_sidecar": _record(predecessor["turn_paths"]["sidecar"]),
        "v179_output": _record(predecessor["turn_paths"]["output"]),
        "v179_usage": predecessor["usage"],
        "v179_output_structurally_valid": True,
        "v179_output_replayed": False,
        "adopted_semantic_turn_count": 1,
        "new_semantic_turn_count_for_adoption": 0,
        "projected_output": _record(adopted_projected_path),
        "projection_audit": _record(adopted_audit_path),
        "normalized_output": _record(adopted_normalized_path),
        "production_mutated": False,
    }
    _write_stable_time(adoption_path, adoption, "created_at")

    turns = []
    for turn_name, row in zip(TURN_NAMES, rows, strict=True):
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=row["value"],
            prompt=row["prompt"],
            schema=row["schema"],
        )
        turns.append({**row, "turn_name": turn_name, "paths": paths})
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V180_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V180_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "adopt_valid_v179_output_then_four_case_measured_bound_recovery",
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "v179_completed_output_replayed": False,
        "v179_completed_output_adopted": True,
        "v179_usage_counted_in_predecessor": True,
        "fresh_case_count": FRESH_CASE_COUNT,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "lossless_compact_serialization": True,
        "semantic_fields_pruned": False,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "support_receipts_frozen": True,
        "primary_alignment_complete": False,
        "balanced_canary_authorized": False,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v179.__file__)),
            _record(Path(v178.__file__)),
            _record(Path(v177.__file__)),
            _record(Path(v157.__file__)),
            _record(Path(v130.__file__)),
            *predecessor["values"]["spec"]["runtime_files"],
        ],
        "frozen_instructions": {
            "alignment_base_sha256": sha256_text(v130.alignment_instructions_v130()),
        },
        "frozen_inputs": {
            "pool": predecessor["predecessor"]["pool_record"],
            "support_receipts": predecessor["predecessor"]["receipts_record"],
            "primary_partition": predecessor["records"]["partition"],
            "v179_output_adoption": _record(adoption_path),
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "case_id": turn["case_id"],
                    "witness_count": turn["witness_count"],
                    "prompt_bytes": turn["prompt_bytes"],
                    "schema_bytes": turn["schema_bytes"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_prompt_output_mapping_sanitized_counts_hashes_only",
    }
    spec_path = root / "primary-alignment-phase1-bound-recovery1-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "predecessor": predecessor,
        "adoption_path": adoption_path,
        "adopted_normalized": predecessor["normalized"],
    }


def _write_failure(
    root: Path, predecessor: Mapping[str, Any], turn_name: Optional[str], error_class: str
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v180 sidecar"))
        except Exception:
            unknown += 1
            continue
        usage = _sum_usage(usage, measured)
    complete = unknown == 0
    cumulative = _sum_usage(predecessor["cumulative_usage"], usage)
    failure = {
        "schema_version": V180_FAILURE_VERSION,
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
        "v179_usage_preserved": predecessor["usage"],
        "predecessor_cumulative_usage": predecessor["cumulative_usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V180_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "v179_completed_output_adopted": True,
        "v179_completed_output_replayed": False,
        "support_receipts_frozen": True,
        "primary_alignment_complete": False,
        "primary_phase1_recovery2_authorized": False,
        "balanced_canary_authorized": False,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v180(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v180 terminal")
    frozen = freeze_v180(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    fresh_normalized_cases, sidecars = [], []
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v130.alignment_instructions_v130(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=turn["witness_count"],
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: (
                        v157.validate_structurally_projectable_output(candidate, item)
                    ),
                )
                projected, projection_audit = v157.project_exact_spans_and_relation(
                    output, turn["value"]
                )
                turn_root = root / "turns" / current_turn.replace("_", "-")
                _write_immutable(turn_root / "alignment-projected.private.json", projected)
                _write_immutable(turn_root / "structural-projection-audit.json", projection_audit)
                fresh_normalized_cases.extend(
                    normalize_neutral_alignment_output(projected, turn["value"])["cases"]
                )
                sidecars.append(sidecar)
        normalized = {
            "schema_version": "pif_app_server_judge_v5_4_selection_primary_alignment_phase1_recovery1_v1",
            "cases": sorted(
                [*frozen["adopted_normalized"]["cases"], *fresh_normalized_cases],
                key=lambda row: str(row["case_id"]),
            ),
            "adopted_v179_case_count": 1,
            "fresh_v180_case_count": FRESH_CASE_COUNT,
            "mismatch_fields_projected_from_checklists": True,
            "origin_neutral": True,
        }
        normalized_path = root / "primary-alignment-phase1-recovery1-normalized.private.json"
        _write_immutable(normalized_path, normalized)
        accounting = _aggregate_usage(sidecars)
        cumulative = _sum_usage(
            frozen["predecessor"]["cumulative_usage"], accounting["usage"]
        )
        pair_count = sum(len(case["alignment_pairs"]) for case in normalized["cases"])
        equivalent_count = sum(
            pair["relation"] == "equivalent"
            for case in normalized["cases"]
            for pair in case["alignment_pairs"]
        )
        terminal = {
            "schema_version": V180_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": "v180_phase1_bound_recovery1_completed_recovery2_authorized",
            "overall_evaluation_complete": False,
            "v179_completed_output_adopted": True,
            "v179_completed_output_replayed": False,
            "v179_output_adoption": _record(frozen["adoption_path"]),
            "support_receipts_frozen": True,
            "primary_alignment_complete": False,
            "primary_phase1_recovery1_frozen": True,
            "primary_phase1_recovery2_authorized": True,
            "balanced_canary_authorized": False,
            "selection_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "adopted_case_count": 1,
            "fresh_case_count": FRESH_CASE_COUNT,
            "partial_phase1_case_count": len(normalized["cases"]),
            "alignment_pair_count": pair_count,
            "equivalent_pair_count": equivalent_count,
            "normalized_output": _record(normalized_path),
            "predecessor_cumulative_usage": frozen["predecessor"]["cumulative_usage"],
            "cumulative_evaluation_usage": cumulative,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(
            root, frozen["predecessor"], exc.turn_name, exc.error_class
        )
    except Exception as exc:
        return _write_failure(
            root, frozen["predecessor"], current_turn, type(exc).__name__
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v180 phase-one bound recovery")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v180(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "primary_phase1_recovery2_authorized": terminal.get(
                    "primary_phase1_recovery2_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
