from __future__ import annotations

"""Judge two source-critical cases for the cap-safe composite candidate space."""

import argparse
import asyncio
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_selection_v181_structural_partition_recovery as v181
from . import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from .app_server_judge_v5 import normalize_neutral_alignment_output
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
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V187_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v187_spec_v1"
V187_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v187_terminal_v1"
V187_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v187_failure_v1"
V187_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V187_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V187_PHASE_ID = "judge_v5_4_selection_v187_composite_stratified_diagnostic"

SELECTED_CASE_IDS = (
    "jcase_2d658557a9bfb3db84af1695",
    "jcase_fbf53634384ba42de9ec5589",
)
SELECTED_SOURCE_IDS = ("odd-lots", "latent-space")
TURN_NAMES = (
    "selection_alignment_composite_odd_lots_00",
    "selection_alignment_composite_latent_space_00",
)
MODEL = v181.MODEL
EFFORT = v181.EFFORT
MAXIMUM_TOTAL_TOKENS_PER_TURN = v181.MAXIMUM_TOTAL_TOKENS_PER_TURN
TIMEOUT_SECONDS = v181.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v186.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v187-composite-stratified-diagnostic"
).resolve()


class JudgeV5SelectionV187Error(RuntimeError):
    """The v187 composite diagnostic cannot be frozen or preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v186_checkpoint() -> dict[str, Any]:
    root = v186.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "spec": root / "convergence-checkpoint-spec.json",
        "score": root / "selection-convergence-score.json",
        "bound": root / "optimistic-remaining-case-bound.json",
        "audit": root / "v185-operator-stop-audit.json",
    }
    values = {name: _load_json(path, f"v186 {name}") for name, path in paths.items()}
    terminal, spec, score, bound, audit = (
        values["terminal"],
        values["spec"],
        values["score"],
        values["bound"],
        values["audit"],
    )
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason")
        != "development_semantic_noninferiority_not_recoverable_by_remaining_alignment_cases"
        or terminal.get("terminal_classification") != "inactive_incomplete_recovery_required"
        or terminal.get("judge_protocol_passed") is not True
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("viable_systems") != []
        or terminal.get("quality_leader") != "batch_3_same_thread"
        or terminal.get("more_development_cases_can_change_viability") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("attempt_usage", {}).get("total_tokens") != 405354
        or terminal.get("cumulative_usage_status") != "unknown"
        or terminal.get("cumulative_unknown_usage_turn_count") != 1
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 120000
        or terminal.get("cumulative_known_usage_lower_bound", {}).get("total_tokens") != 8087855
        or spec.get("state") != "zero_token_postprocess_completed"
        or spec.get("semantic_model_calls_started") != 0
        or spec.get("new_judge_prompt_or_rubric_created") is not False
        or spec.get("production_mutation_allowed") is not False
        or score.get("viable_systems") != []
        or score.get("completed_quality_leader") != "batch_3_same_thread"
        or score.get("all_arms_pass_production_amortized_token_gate") is not True
        or bound.get("remaining_case_count") != 13
        or bound.get("more_development_alignment_can_change_selection_viability") is not False
        or audit.get("completed_turn_count") != 5
        or audit.get("never_started_turn_count") != 4
    ):
        raise JudgeV5SelectionV187Error("v186 checkpoint contract drifted")
    for key in ("spec", "score", "optimistic_remaining_bound", "operator_stop_audit"):
        record = terminal.get(key)
        expected = {
            "spec": paths["spec"],
            "score": paths["score"],
            "optimistic_remaining_bound": paths["bound"],
            "operator_stop_audit": paths["audit"],
        }[key]
        if record != _record(expected):
            raise JudgeV5SelectionV187Error("v186 terminal binding drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV187Error("v186 runtime record drifted")
    stop = v186._validate_v185_operator_stop()
    if stop["cumulative_known_lower_bound"] != terminal["cumulative_known_usage_lower_bound"]:
        raise JudgeV5SelectionV187Error("v186 cumulative accounting drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "stop": stop,
        "cumulative_known_lower_bound": stop["cumulative_known_lower_bound"],
        "cumulative_unknown_usage_turn_count": 1,
        "cumulative_unknown_usage_upper_bound": 120000,
    }


def _selected_rows(predecessor: Mapping[str, Any]) -> list[dict[str, Any]]:
    partition = predecessor["stop"]["predecessor"]["partition"]
    rows = [partition["adopted_case"], *[row for phase in partition["phases"] for row in phase]]
    by_id = {str(row["case_id"]): row for row in rows}
    if set(SELECTED_CASE_IDS) - set(by_id):
        raise JudgeV5SelectionV187Error("selected composite diagnostic case disappeared")
    selected = [by_id[case_id] for case_id in SELECTED_CASE_IDS]
    completed_ids = {
        str(row["case_id"])
        for row in v186._completed_alignment_sets(predecessor["stop"])["all_completed"]
    }
    if completed_ids.intersection(SELECTED_CASE_IDS):
        raise JudgeV5SelectionV187Error("v187 selected a completed alignment case")
    sources = v186._selection_sources()
    provenance = {
        str(row["case_id"]): row["case_provenance"]
        for row in sources["mapping"]["cases"]
    }
    if tuple(provenance[case_id]["source_id"] for case_id in SELECTED_CASE_IDS) != SELECTED_SOURCE_IDS:
        raise JudgeV5SelectionV187Error("v187 source stratification drifted")
    remaining_by_source: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        case_id = str(row["case_id"])
        if case_id in completed_ids:
            continue
        source_id = str(provenance[case_id]["source_id"])
        remaining_by_source.setdefault(source_id, []).append(row)
    for source_id, selected_row in zip(SELECTED_SOURCE_IDS, selected, strict=True):
        largest = max(
            remaining_by_source[source_id],
            key=lambda row: (int(row["prompt_bytes"]), str(row["case_id"])),
        )
        if largest["case_id"] != selected_row["case_id"]:
            raise JudgeV5SelectionV187Error("v187 did not select largest critical-source case")
    return selected


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V187_CAPACITY_AUDIT_VERSION,
        "phase_id": V187_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v186_terminal": predecessor["records"]["terminal"],
        "measured_basis": {
            "predecessor_known_total_tokens_lower_bound": predecessor[
                "cumulative_known_lower_bound"
            ]["total_tokens"],
            "predecessor_unknown_usage_turn_count": 1,
            "predecessor_unknown_usage_upper_bound": 120000,
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V187_CAPACITY_POLICY_VERSION,
        "phase_id": V187_PHASE_ID,
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


def freeze_v187(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v187 terminal")}
    predecessor = _validate_v186_checkpoint()
    rows = _selected_rows(predecessor)
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
        "schema_version": V187_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V187_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "two_case_source_critical_composite_viability_diagnostic",
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "selected_case_ids": list(SELECTED_CASE_IDS),
        "selected_source_ids": list(SELECTED_SOURCE_IDS),
        "selection_rule": "largest_prompt_unjudged_case_in_each_source_where_composite_needs_positive_delta",
        "existing_extraction_outputs_only": True,
        "extraction_model_calls_allowed": False,
        "new_judge_prompt_or_rubric_created": False,
        "alignment_prompt_and_schema_reused_exactly": True,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "semantic_fields_pruned": False,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "postprocess_stop_rule": (
            "recompute_all_cost_and_cap_safe_composites_then_stop_if_candidate_favoring_"
            "remaining_case_bound_cannot_clear_every_frozen_semantic_gate"
        ),
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v186.__file__)),
            _record(Path(v181.__file__)),
            _record(Path(v130.__file__)),
            *predecessor["values"]["spec"]["runtime_files"],
        ],
        "frozen_instructions": {
            "alignment_base_sha256": sha256_text(v130.alignment_instructions_v130()),
        },
        "frozen_inputs": {
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "case_id": turn["case_id"],
                    "source_id": source_id,
                    "witness_count": turn["witness_count"],
                    "prompt_bytes": turn["prompt_bytes"],
                    "schema_bytes": turn["schema_bytes"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn, source_id in zip(turns, SELECTED_SOURCE_IDS, strict=True)
            ],
        },
        "privacy": "private_source_event_prompt_output_mapping_sanitized_counts_hashes_only",
    }
    spec_path = root / "composite-stratified-diagnostic-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "predecessor": predecessor,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v187 sidecar"))
        except Exception:
            unknown += 1
            continue
        usage = _sum_usage(usage, measured)
    complete = unknown == 0
    cumulative_lower = _sum_usage(predecessor["cumulative_known_lower_bound"], usage)
    failure = {
        "schema_version": V187_FAILURE_VERSION,
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
        "cumulative_known_usage_lower_bound": cumulative_lower,
        "cumulative_unknown_usage_turn_count": 1 + unknown,
        "cumulative_conservative_unknown_usage_upper_bound": 120000
        + unknown * MAXIMUM_TOTAL_TOKENS_PER_TURN,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V187_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": cumulative_lower,
        "cumulative_unknown_usage_turn_count": 1 + unknown,
        "cumulative_conservative_unknown_usage_upper_bound": failure[
            "cumulative_conservative_unknown_usage_upper_bound"
        ],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v187(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v187 terminal")
    frozen = freeze_v187(output_dir=root, timeout_seconds=timeout_seconds)
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
                        v181.validate_structurally_completable_output(candidate, item)
                    ),
                )
                projected, audit = v181.project_structural_unpaired_and_exact_spans(
                    output, turn["value"]
                )
                turn_root = root / "turns" / current_turn.replace("_", "-")
                _write_immutable(turn_root / "alignment-projected.private.json", projected)
                _write_immutable(turn_root / "structural-projection-audit.json", audit)
                normalized_cases.extend(
                    normalize_neutral_alignment_output(projected, turn["value"])["cases"]
                )
                sidecars.append(sidecar)
        normalized = {
            "schema_version": "pif_app_server_judge_v5_4_selection_v187_normalized_v1",
            "cases": sorted(normalized_cases, key=lambda row: str(row["case_id"])),
            "case_count": len(normalized_cases),
            "origin_neutral": True,
        }
        normalized_path = root / "composite-stratified-alignment.private.json"
        _write_immutable(normalized_path, normalized)
        accounting = _aggregate_usage(sidecars)
        cumulative_lower = _sum_usage(
            frozen["predecessor"]["cumulative_known_lower_bound"], accounting["usage"]
        )
        terminal = {
            "schema_version": V187_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": "v187_composite_stratified_alignment_completed_postprocess_authorized",
            "overall_evaluation_complete": False,
            "selected_case_ids": list(SELECTED_CASE_IDS),
            "selected_source_ids": list(SELECTED_SOURCE_IDS),
            "alignment_output": _record(normalized_path),
            "postprocess_authorized": True,
            "additional_alignment_authorized": False,
            "selection_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "cumulative_usage_status": "unknown",
            "cumulative_known_usage_lower_bound": cumulative_lower,
            "cumulative_unknown_usage_turn_count": 1,
            "cumulative_conservative_unknown_usage_upper_bound": 120000,
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
    parser = argparse.ArgumentParser(description="Run v187 composite stratified diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v187(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "postprocess_authorized": terminal.get("postprocess_authorized", False),
                "usage_status": terminal.get("usage_status"),
                "holdout_authorized": terminal.get("holdout_authorized", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
