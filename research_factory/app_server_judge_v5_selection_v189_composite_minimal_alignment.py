from __future__ import annotations

"""Judge the one v188-authorized source-critical composite case."""

import argparse
import asyncio
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_selection_v181_structural_partition_recovery as v181
from . import app_server_judge_v5_selection_v187_composite_stratified_diagnostic as v187
from . import app_server_judge_v5_selection_v188_composite_postprocess as v188
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


V189_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v189_spec_v1"
V189_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v189_terminal_v1"
V189_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v189_failure_v1"
V189_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V189_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V189_PHASE_ID = "judge_v5_4_selection_v189_composite_minimal_alignment"

TURN_NAME = "selection_alignment_composite_minimal_odd_lots_00"
MODEL = v181.MODEL
EFFORT = v181.EFFORT
MAXIMUM_TOTAL_TOKENS_PER_TURN = v181.MAXIMUM_TOTAL_TOKENS_PER_TURN
TIMEOUT_SECONDS = v181.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v188.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v189-composite-minimal-alignment"
).resolve()


class JudgeV5SelectionV189Error(RuntimeError):
    """The v189 minimal alignment attempt cannot be frozen safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v188_authorization() -> dict[str, Any]:
    root = v188.DEFAULT_OUTPUT_ROOT
    terminal_path = root / "terminal.json"
    score_path = root / "composite-selection-score.json"
    bound_path = root / "composite-optimistic-bound.json"
    spec_path = root / "composite-postprocess-spec.json"
    terminal = _load_json(terminal_path, "v188 terminal")
    score = _load_json(score_path, "v188 score")
    bound = _load_json(bound_path, "v188 bound")
    spec = _load_json(spec_path, "v188 spec")
    next_case = terminal.get("authorized_next_case")
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "composite_viability_unresolved_minimal_additional_alignment_required"
        or terminal.get("terminal_classification") != "active_development_recovery_required"
        or terminal.get("overall_evaluation_complete") is not False
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("more_development_cases_can_change_viability") is not True
        or terminal.get("additional_alignment_authorized") is not True
        or not isinstance(next_case, dict)
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("attempt_usage", {}).get("total_tokens") != 0
        or terminal.get("cumulative_usage_status") != "unknown"
        or terminal.get("cumulative_unknown_usage_turn_count") != 1
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 120000
        or terminal.get("score") != _record(score_path)
        or terminal.get("optimistic_remaining_bound") != _record(bound_path)
        or terminal.get("spec") != _record(spec_path)
        or score.get("observed_viable_systems") != []
        or score.get("more_development_cases_can_change_viability") is not True
        or score.get("next_case") != next_case
        or bound.get("any_system_can_become_semantically_viable") is not True
        or spec.get("state") != "zero_token_postprocess_completed"
        or spec.get("semantic_model_calls_started") != 0
        or spec.get("extraction_model_calls_started") != 0
        or spec.get("new_judge_prompt_or_rubric_created") is not False
    ):
        raise JudgeV5SelectionV189Error("v188 authorization contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV189Error("v188 runtime binding drifted")
    success = v188._validate_v187_success()
    if terminal.get("cumulative_known_usage_lower_bound") != success["terminal"].get(
        "cumulative_known_usage_lower_bound"
    ):
        raise JudgeV5SelectionV189Error("v188 cumulative accounting drifted")
    return {
        "root": root,
        "terminal": terminal,
        "score": score,
        "bound": bound,
        "spec": spec,
        "next_case": next_case,
        "records": {
            "terminal": _record(terminal_path),
            "score": _record(score_path),
            "bound": _record(bound_path),
            "spec": _record(spec_path),
        },
        "v187": success,
    }


def _selected_row(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    partition = predecessor["v187"]["predecessor"]["stop"]["predecessor"]["partition"]
    rows = [partition["adopted_case"], *[row for phase in partition["phases"] for row in phase]]
    by_id = {str(row["case_id"]): row for row in rows}
    next_case = predecessor["next_case"]
    row = by_id.get(str(next_case["case_id"]))
    if row is None:
        raise JudgeV5SelectionV189Error("v188-authorized case disappeared")
    if (
        row.get("prompt_bytes") != next_case.get("prompt_bytes")
        or row.get("schema_bytes") != next_case.get("schema_bytes")
        or row.get("witness_count") != next_case.get("witness_count")
        or next_case.get("critical_source_id") != "odd-lots"
    ):
        raise JudgeV5SelectionV189Error("v188-authorized case metadata drifted")
    completed = {
        str(case["case_id"])
        for case in v186_completed_cases(predecessor)
    }
    if str(row["case_id"]) in completed:
        raise JudgeV5SelectionV189Error("v189 attempted to replay a completed case")
    return row


def v186_completed_cases(predecessor: Mapping[str, Any]) -> list[dict[str, Any]]:
    previous = v188.v186._completed_alignment_sets(
        predecessor["v187"]["predecessor"]["stop"]
    )["all_completed"]
    return [*previous, *predecessor["v187"]["normalized"]["cases"]]


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V189_CAPACITY_AUDIT_VERSION,
        "phase_id": V189_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v188_terminal": predecessor["records"]["terminal"],
        "measured_basis": {
            "predecessor_known_total_tokens_lower_bound": predecessor["terminal"][
                "cumulative_known_usage_lower_bound"
            ]["total_tokens"],
            "predecessor_unknown_usage_turn_count": 1,
            "predecessor_unknown_usage_upper_bound": 120000,
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V189_CAPACITY_POLICY_VERSION,
        "phase_id": V189_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [TURN_NAME],
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


def freeze_v189(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v189 terminal")}
    predecessor = _validate_v188_authorization()
    row = _selected_row(predecessor)
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=row["value"],
        prompt=row["prompt"],
        schema=row["schema"],
    )
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V189_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V189_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_case_v188_authorized_minimal_composite_viability_diagnostic",
        "turn_plan": [TURN_NAME],
        "retry_count_per_turn": 0,
        "selected_case_id": row["case_id"],
        "selected_source_id": predecessor["next_case"]["critical_source_id"],
        "selection_rule": predecessor["next_case"]["selection_rule"],
        "existing_extraction_outputs_only": True,
        "extraction_model_calls_allowed": False,
        "new_judge_prompt_or_rubric_created": False,
        "alignment_prompt_and_schema_reused_exactly": True,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "semantic_fields_pruned": False,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "additional_alignment_after_this_turn_authorized": False,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v188.__file__)),
            _record(Path(v187.__file__)),
            _record(Path(v181.__file__)),
            _record(Path(v130.__file__)),
            *predecessor["spec"]["runtime_files"],
        ],
        "frozen_instructions": {
            "alignment_base_sha256": sha256_text(v130.alignment_instructions_v130()),
        },
        "frozen_input": {
            "case_id": row["case_id"],
            "source_id": predecessor["next_case"]["critical_source_id"],
            "witness_count": row["witness_count"],
            "prompt_bytes": row["prompt_bytes"],
            "schema_bytes": row["schema_bytes"],
            "input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
        },
        "privacy": "private_source_event_prompt_output_mapping_sanitized_counts_hashes_only",
    }
    spec_path = root / "composite-minimal-alignment-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "row": row,
        "paths": paths,
        "predecessor": predecessor,
    }


def _write_failure(
    root: Path, predecessor: Mapping[str, Any], error_class: str
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v189 sidecar"))
        except Exception:
            unknown += 1
            continue
        usage = _sum_usage(usage, measured)
    complete = unknown == 0
    cumulative_lower = _sum_usage(
        predecessor["terminal"]["cumulative_known_usage_lower_bound"], usage
    )
    failure = {
        "schema_version": V189_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
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
        "schema_version": V189_TERMINAL_VERSION,
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


async def run_v189(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v189 terminal")
    frozen = freeze_v189(output_dir=root, timeout_seconds=timeout_seconds)
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _ = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["row"]["prompt"],
                schema=frozen["row"]["schema"],
                base_instructions=v130.alignment_instructions_v130(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=frozen["row"]["witness_count"],
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: v181.validate_structurally_completable_output(
                    candidate, frozen["row"]["value"]
                ),
            )
        projected, audit = v181.project_structural_unpaired_and_exact_spans(
            output, frozen["row"]["value"]
        )
        turn_root = root / "turns" / TURN_NAME.replace("_", "-")
        _write_immutable(turn_root / "alignment-projected.private.json", projected)
        _write_immutable(turn_root / "structural-projection-audit.json", audit)
        normalized = normalize_neutral_alignment_output(
            projected, frozen["row"]["value"]
        )
        normalized_path = root / "composite-minimal-alignment.private.json"
        _write_immutable(normalized_path, normalized)
        accounting = _aggregate_usage([sidecar])
        cumulative_lower = _sum_usage(
            frozen["predecessor"]["terminal"]["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        terminal = {
            "schema_version": V189_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": "v189_minimal_composite_alignment_completed_postprocess_authorized",
            "overall_evaluation_complete": False,
            "selected_case_id": frozen["row"]["case_id"],
            "selected_source_id": frozen["predecessor"]["next_case"]["critical_source_id"],
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
        return _write_failure(root, frozen["predecessor"], exc.error_class)
    except Exception as exc:
        return _write_failure(root, frozen["predecessor"], type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v189 minimal composite alignment")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v189(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
