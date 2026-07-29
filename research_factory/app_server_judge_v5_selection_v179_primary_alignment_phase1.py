from __future__ import annotations

"""Run the first capacity-bounded primary alignment tranche for selection."""

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
from .app_server_judge_v5 import neutral_alignment_output_schema, normalize_neutral_alignment_output
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
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _validate_usage,
)
from .util import now_iso, sha256_text


V179_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v179_spec_v1"
V179_PARTITION_VERSION = "pif_app_server_judge_v5_4_selection_primary_partition_v1"
V179_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v179_terminal_v1"
V179_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v179_failure_v1"
V179_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V179_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V179_PHASE_ID = "judge_v5_4_selection_v179_primary_alignment_phase1"

MODEL = v177.MODEL
EFFORT = v177.EFFORT
PHASE_CASE_COUNT = 9
PHASE_NUMBER = 1
TURN_NAMES = tuple(
    f"selection_alignment_primary_phase1_{index:02d}" for index in range(PHASE_CASE_COUNT)
)
MAXIMUM_TOTAL_TOKENS_PER_TURN = v177.MAXIMUM_TOTAL_TOKENS_PER_TURN
MAXIMUM_PROMPT_BYTES = v177.MAXIMUM_PROMPT_BYTES
MAXIMUM_SCHEMA_BYTES = v177.MAXIMUM_SCHEMA_BYTES
TIMEOUT_SECONDS = v177.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v178.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v179-primary-alignment-phase1"
).resolve()


class JudgeV5SelectionV179Error(RuntimeError):
    """The immutable v179 primary-alignment tranche cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v178_success() -> dict[str, Any]:
    root = v178.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "spec": root / "selection-alignment-scale-postprocess-recovery-spec.json",
        "score": root / "selection-alignment-scale-postprocess-score.json",
        "receipt": root / "alignment-scale-diagnostic-receipt.json",
    }
    values = {name: _load_json(path, f"v178 {name}") for name, path in paths.items()}
    terminal, spec, score, receipt = (
        values["terminal"], values["spec"], values["score"], values["receipt"]
    )
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v178_alignment_scale_postprocess_passed_full_alignment_authorized"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != zero_usage
        or terminal.get("semantic_turn_count") != 0
        or terminal.get("predecessor_v177_usage", {}).get("total_tokens") != 173535
        or terminal.get("cumulative_evaluation_usage", {}).get("total_tokens") != 6842317
        or terminal.get("full_alignment_authorized") is not True
        or terminal.get("selection_winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or spec.get("semantic_turn_count") != 0
        or spec.get("v177_turns_replayed") != 0
        or spec.get("v177_completed_outputs_reused") != 2
        or spec.get("full_alignment_authorized") is not True
        or score.get("passed") is not True
        or score.get("alignment_pair_count") != 19
        or score.get("abstained_pair_count") != 0
        or receipt.get("diagnostic_passed") is not True
        or receipt.get("base_canary_projection_exact") is not True
        or receipt.get("maximum_observed_total_tokens_per_turn") != 88821
        or receipt.get("full_alignment_authorized") is not True
    ):
        raise JudgeV5SelectionV179Error("v178 authorization contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV179Error("v178 runtime record drifted")
    if (
        terminal.get("score") != _record(paths["score"])
        or terminal.get("receipt") != _record(paths["receipt"])
        or spec.get("score") != _record(paths["score"])
        or spec.get("receipt") != _record(paths["receipt"])
        or receipt.get("score") != _record(paths["score"])
    ):
        raise JudgeV5SelectionV179Error("v178 record binding drifted")
    predecessor = v178._validate_v177_failure()
    rebuilt = v178.score_v178(
        predecessor["normalized_base"], predecessor["normalized_canary"]
    )
    if score != rebuilt or terminal["cumulative_evaluation_usage"] != predecessor["cumulative_usage"]:
        raise JudgeV5SelectionV179Error("v178 score or cumulative usage drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "v177": predecessor,
        "pool": predecessor["v176"]["pool"],
        "pool_record": predecessor["v176"]["pool_record"],
        "receipts": predecessor["v176"]["receipts"],
        "receipts_record": predecessor["v176"]["records"]["support_receipts"],
        "cumulative_usage": predecessor["cumulative_usage"],
    }


def build_primary_partition(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    candidates = []
    for case in predecessor["pool"]["cases"]:
        case_id = str(case["case_id"])
        if not any(
            row["case_id"] == case_id and row["proposition_verdict"] == "supported"
            for row in predecessor["receipts"]["units"]
        ):
            continue
        value = v177.build_supported_alignment_input(
            predecessor["pool"],
            predecessor["receipts"],
            case_id=case_id,
            permutation="base",
        )
        prompt, compact = v177.compact_alignment_prompt(value)
        schema = neutral_alignment_output_schema(value)
        prompt_bytes, schema_bytes = v177._request_bytes(prompt, schema)
        if prompt_bytes > MAXIMUM_PROMPT_BYTES or schema_bytes > MAXIMUM_SCHEMA_BYTES:
            raise JudgeV5SelectionV179Error("primary alignment request exceeds frozen byte cap")
        candidates.append(
            {
                "case_id": case_id,
                "value": value,
                "prompt": prompt,
                "compact": compact,
                "schema": schema,
                "prompt_bytes": prompt_bytes,
                "schema_bytes": schema_bytes,
                "witness_count": len(value["cases"][0]["witnesses"]),
            }
        )
    if len(candidates) != 28:
        raise JudgeV5SelectionV179Error("primary alignment case coverage drifted")

    adopted_case_id = str(predecessor["v177"]["normalized_base"]["cases"][0]["case_id"])
    adopted = next((row for row in candidates if row["case_id"] == adopted_case_id), None)
    if adopted is None:
        raise JudgeV5SelectionV179Error("v177 adopted primary case disappeared")
    v177_spec = predecessor["v177"]["values"]["spec"]
    v177_base = v177_spec["frozen_inputs"]["turns"][0]
    if (
        adopted["value"] != predecessor["v177"]["values"]["base_input"]
        or adopted["prompt"] != Path(v177_base["prompt"]["path"]).read_text()
        or adopted["schema"] != _load_json(Path(v177_base["schema"]["path"]), "v177 base schema")
    ):
        raise JudgeV5SelectionV179Error("v177 adopted request is not exactly reusable")

    remaining = sorted(
        (row for row in candidates if row["case_id"] != adopted_case_id),
        key=lambda row: (-row["prompt_bytes"], row["case_id"]),
    )
    phases = [remaining[index : index + PHASE_CASE_COUNT] for index in range(0, len(remaining), PHASE_CASE_COUNT)]
    if len(phases) != 3 or any(len(phase) != PHASE_CASE_COUNT for phase in phases):
        raise JudgeV5SelectionV179Error("primary alignment phase partition drifted")
    return {
        "schema_version": V179_PARTITION_VERSION,
        "adopted_case": adopted,
        "phases": phases,
        "all_candidates": candidates,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V179_CAPACITY_AUDIT_VERSION,
        "phase_id": V179_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v178_terminal": predecessor["records"]["terminal"],
        "v178_receipt": predecessor["records"]["receipt"],
        "measured_basis": {
            "v177_base_total_tokens": 84714,
            "v177_canary_total_tokens": 88821,
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
        "schema_version": V179_CAPACITY_POLICY_VERSION,
        "phase_id": V179_PHASE_ID,
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


def freeze_v179(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v179 terminal")}
    predecessor = _validate_v178_success()
    partition = build_primary_partition(predecessor)
    partition_path = root / "primary-alignment-partition.private.json"
    _write_stable_time(
        partition_path,
        {
            "schema_version": V179_PARTITION_VERSION,
            "created_at": now_iso(),
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
            "cases_per_phase": PHASE_CASE_COUNT,
            "partition_uses_prompt_bytes_and_opaque_case_id_only": True,
            "semantic_labels_used_for_partition": False,
        },
        "created_at",
    )
    turns = []
    for turn_name, row in zip(TURN_NAMES, partition["phases"][0], strict=True):
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
        "schema_version": V179_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V179_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "adopt_exact_v177_base_then_three_nine_case_primary_phases_largest_first",
        "phase_number": PHASE_NUMBER,
        "phase_case_count": PHASE_CASE_COUNT,
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "v177_primary_turn_replayed": False,
        "v177_primary_output_adopted": True,
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
            "pool": predecessor["pool_record"],
            "support_receipts": predecessor["receipts_record"],
            "primary_partition": _record(partition_path),
            "adopted_v177_base": predecessor["v177"]["records"]["base_projected"],
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
    spec_path = root / "primary-alignment-phase1-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "predecessor": predecessor,
        "partition": partition,
        "partition_path": partition_path,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v179 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    cumulative = _sum_usage(predecessor["cumulative_usage"], usage)
    failure = {
        "schema_version": V179_FAILURE_VERSION,
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
        "predecessor_cumulative_usage": predecessor["cumulative_usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V179_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "support_receipts_frozen": True,
        "primary_alignment_complete": False,
        "primary_phase2_authorized": False,
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


async def run_v179(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v179 terminal")
    frozen = freeze_v179(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    normalized_cases, sidecars = [], []
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
                normalized_cases.extend(
                    normalize_neutral_alignment_output(projected, turn["value"])["cases"]
                )
                sidecars.append(sidecar)
        normalized = {
            "schema_version": "pif_app_server_judge_v5_4_selection_primary_alignment_phase1_v1",
            "cases": sorted(normalized_cases, key=lambda row: str(row["case_id"])),
            "mismatch_fields_projected_from_checklists": True,
            "origin_neutral": True,
        }
        normalized_path = root / "primary-alignment-phase1-normalized.private.json"
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
            "schema_version": V179_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": "v179_primary_alignment_phase1_completed_phase2_authorized",
            "overall_evaluation_complete": False,
            "support_receipts_frozen": True,
            "primary_alignment_complete": False,
            "primary_phase1_frozen": True,
            "primary_phase2_authorized": True,
            "balanced_canary_authorized": False,
            "selection_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "phase_case_count": len(normalized["cases"]),
            "alignment_pair_count": pair_count,
            "equivalent_pair_count": equivalent_count,
            "normalized_output": _record(normalized_path),
            "primary_partition": _record(frozen["partition_path"]),
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
    parser = argparse.ArgumentParser(description="Run v179 primary alignment phase one")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v179(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "primary_phase2_authorized": terminal.get("primary_phase2_authorized", False),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
