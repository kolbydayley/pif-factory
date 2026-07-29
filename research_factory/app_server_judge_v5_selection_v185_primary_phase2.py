from __future__ import annotations

"""Run the nine never-started cases in primary alignment phase two."""

import argparse
import asyncio
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_selection_v179_primary_alignment_phase1 as v179
from . import app_server_judge_v5_selection_v181_structural_partition_recovery as v181
from . import app_server_judge_v5_selection_v183_missing_group_repair as v183
from . import app_server_judge_v5_selection_v184_schema_subset_recovery as v184
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


V185_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v185_spec_v1"
V185_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v185_terminal_v1"
V185_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v185_failure_v1"
V185_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V185_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V185_PHASE_ID = "judge_v5_4_selection_v185_primary_alignment_phase2"

MODEL = v184.MODEL
EFFORT = v184.EFFORT
PHASE_INDEX = 1
FRESH_CASE_COUNT = 9
TURN_NAMES = tuple(
    f"selection_alignment_primary_phase2_{index:02d}"
    for index in range(FRESH_CASE_COUNT)
)
MAXIMUM_TOTAL_TOKENS_PER_TURN = v181.MAXIMUM_TOTAL_TOKENS_PER_TURN
TIMEOUT_SECONDS = v181.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v184.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v185-primary-alignment-phase2"
).resolve()


class JudgeV5SelectionV185Error(RuntimeError):
    """The immutable v185 primary phase-two attempt cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v184_success() -> dict[str, Any]:
    root = v184.DEFAULT_OUTPUT_ROOT
    turn_root = root / "turns" / v184.TURN_NAME.replace("_", "-")
    paths = {
        "terminal": root / "terminal.json",
        "spec": root / "schema-subset-recovery-spec.json",
        "policy": root / "capacity-policy.json",
        "normalized": root / "primary-alignment-phase1-complete.private.json",
        "repaired": root / "repaired-primary-alignment-case.private.json",
        "repair_audit": root / "missing-group-repair-audit.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "output": turn_root / "output.private.json",
    }
    values = {
        name: _load_json(path, f"v184 {name}")
        for name, path in paths.items()
        if name != "prompt"
    }
    terminal, spec = values["terminal"], values["spec"]
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v184_schema_subset_recovery_completed_phase2_authorized"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("cumulative_usage_status") != "unknown"
        or terminal.get("cumulative_unknown_usage_turn_count") != 1
        or terminal.get("v183_unknown_usage_turn_count") != 1
        or terminal.get("primary_phase1_complete") is not True
        or terminal.get("primary_phase2_authorized") is not True
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or spec.get("state") != "frozen_before_model_calls"
        or spec.get("turn_plan") != list(v184.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("v183_attempt_replayed_in_place") is not False
        or spec.get("schema_change_only") is not True
        or spec.get("production_mutation_allowed") is not False
        or terminal.get("normalized_output") != _record(paths["normalized"])
        or terminal.get("repaired_case") != _record(paths["repaired"])
        or terminal.get("repair_audit") != _record(paths["repair_audit"])
    ):
        raise JudgeV5SelectionV185Error("v184 success contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV185Error("v184 runtime record drifted")

    predecessor = v184._validate_v183_schema_failure()
    schema = v184.repair_schema_v184(predecessor["value"])
    prompt = predecessor["prompt"]
    frozen_turn = spec.get("frozen_inputs", {}).get("turn") or {}
    if (
        values["input"] != predecessor["value"]
        or paths["prompt"].read_text() != prompt
        or values["schema"] != schema
        or frozen_turn.get("input") != _record(paths["input"])
        or frozen_turn.get("prompt") != _record(paths["prompt"])
        or frozen_turn.get("schema") != _record(paths["schema"])
    ):
        raise JudgeV5SelectionV185Error("v184 frozen request drifted")
    output, sidecar = _validate_completed_turn(
        paths={
            "input": paths["input"],
            "prompt": paths["prompt"],
            "schema": paths["schema"],
            "capacity": paths["capacity"],
            "sidecar": paths["sidecar"],
            "output": paths["output"],
        },
        prompt=prompt,
        schema=schema,
        base_instructions=v183.missing_group_repair_instructions(),
        model=MODEL,
        effort=EFFORT,
        policy_path=paths["policy"],
        output_validator=lambda candidate: v183.validate_repair_output(
            candidate, predecessor["value"]
        ),
    )
    repaired, repair_audit = v183.apply_group_repair(predecessor["predecessor"], output)
    if repaired != values["repaired"] or repair_audit != values["repair_audit"]:
        raise JudgeV5SelectionV185Error("v184 repair projection drifted")
    normalized_case = normalize_neutral_alignment_output(
        repaired, predecessor["predecessor"]["failed"]["row"]["value"]
    )["cases"]
    rebuilt_normalized = {
        "schema_version": "pif_app_server_judge_v5_4_selection_primary_alignment_phase1_complete_v184_v1",
        "cases": sorted(
            [
                *predecessor["predecessor"]["normalized_prefix"]["cases"],
                *normalized_case,
            ],
            key=lambda row: str(row["case_id"]),
        ),
        "case_count": 9,
        "missing_group_repair_count": 2,
        "origin_neutral": True,
    }
    if rebuilt_normalized != values["normalized"]:
        raise JudgeV5SelectionV185Error("v184 normalized output drifted")
    accounting = _aggregate_usage([sidecar])
    cumulative_lower = _sum_usage(
        predecessor["cumulative_known_lower_bound"], accounting["usage"]
    )
    if (
        terminal.get("usage") != accounting["usage"]
        or terminal.get("cumulative_known_usage_lower_bound") != cumulative_lower
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound")
        != predecessor["predecessor_unknown_usage_upper_bound"]
    ):
        raise JudgeV5SelectionV185Error("v184 accounting drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "predecessor": predecessor,
        "partition": predecessor["predecessor"]["partition"],
        "normalized": rebuilt_normalized,
        "usage": accounting["usage"],
        "cumulative_known_lower_bound": cumulative_lower,
        "cumulative_unknown_usage_turn_count": 1,
        "cumulative_unknown_usage_upper_bound": predecessor[
            "predecessor_unknown_usage_upper_bound"
        ],
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V185_CAPACITY_AUDIT_VERSION,
        "phase_id": V185_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v184_terminal": predecessor["records"]["terminal"],
        "measured_basis": {
            "v184_total_tokens": predecessor["usage"]["total_tokens"],
            "predecessor_unknown_usage_turn_count": predecessor[
                "cumulative_unknown_usage_turn_count"
            ],
            "predecessor_unknown_usage_upper_bound": predecessor[
                "cumulative_unknown_usage_upper_bound"
            ],
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
        "schema_version": V185_CAPACITY_POLICY_VERSION,
        "phase_id": V185_PHASE_ID,
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


def freeze_v185(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v185 terminal")}
    predecessor = _validate_v184_success()
    rows = predecessor["partition"]["phases"][PHASE_INDEX]
    if len(rows) != FRESH_CASE_COUNT:
        raise JudgeV5SelectionV185Error("v185 phase-two coverage drifted")
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
        "schema_version": V185_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V185_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "nine_case_primary_phase2_with_structural_unpaired_projection",
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "v184_turns_replayed": False,
        "adopted_phase1_case_count": 9,
        "fresh_phase2_case_count": FRESH_CASE_COUNT,
        "predecessor_cumulative_usage_status": "unknown",
        "predecessor_unknown_usage_turn_count": predecessor[
            "cumulative_unknown_usage_turn_count"
        ],
        "predecessor_unknown_usage_upper_bound": predecessor[
            "cumulative_unknown_usage_upper_bound"
        ],
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "structural_projection_changes_semantic_groups_pairs_or_checklists": False,
        "semantic_fields_pruned": False,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "support_receipts_frozen": True,
        "primary_phase1_complete": True,
        "primary_phase2_complete": False,
        "primary_phase3_authorized": False,
        "balanced_canary_authorized": False,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v184.__file__)),
            _record(Path(v181.__file__)),
            _record(Path(v179.__file__)),
            _record(Path(v130.__file__)),
            *predecessor["values"]["spec"]["runtime_files"],
        ],
        "frozen_instructions": {
            "alignment_base_sha256": sha256_text(v130.alignment_instructions_v130()),
        },
        "frozen_inputs": {
            "primary_partition": _record(
                v179.DEFAULT_OUTPUT_ROOT / "primary-alignment-partition.private.json"
            ),
            "adopted_phase1": predecessor["records"]["normalized"],
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
    spec_path = root / "primary-alignment-phase2-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "predecessor": predecessor,
        "adopted_phase1": predecessor["normalized"],
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v185 sidecar"))
        except Exception:
            unknown += 1
            continue
        usage = _sum_usage(usage, measured)
    complete = unknown == 0
    cumulative_lower = _sum_usage(predecessor["cumulative_known_lower_bound"], usage)
    failure = {
        "schema_version": V185_FAILURE_VERSION,
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
        "predecessor_cumulative_usage_status": "unknown",
        "predecessor_unknown_usage_turn_count": predecessor[
            "cumulative_unknown_usage_turn_count"
        ],
        "cumulative_unknown_usage_turn_count": predecessor[
            "cumulative_unknown_usage_turn_count"
        ]
        + unknown,
        "cumulative_known_usage_lower_bound": cumulative_lower,
        "cumulative_conservative_unknown_usage_upper_bound": predecessor[
            "cumulative_unknown_usage_upper_bound"
        ]
        + unknown * MAXIMUM_TOTAL_TOKENS_PER_TURN,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V185_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "support_receipts_frozen": True,
        "primary_phase1_complete": True,
        "primary_phase2_complete": False,
        "primary_phase3_authorized": False,
        "balanced_canary_authorized": False,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_usage_status": "unknown",
        "cumulative_unknown_usage_turn_count": failure[
            "cumulative_unknown_usage_turn_count"
        ],
        "cumulative_known_usage_lower_bound": cumulative_lower,
        "cumulative_conservative_unknown_usage_upper_bound": failure[
            "cumulative_conservative_unknown_usage_upper_bound"
        ],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v185(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v185 terminal")
    frozen = freeze_v185(output_dir=root, timeout_seconds=timeout_seconds)
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
            "schema_version": "pif_app_server_judge_v5_4_selection_primary_alignment_phase2_v185_v1",
            "cases": sorted(normalized_cases, key=lambda row: str(row["case_id"])),
            "case_count": FRESH_CASE_COUNT,
            "mismatch_fields_projected_from_checklists": True,
            "origin_neutral": True,
        }
        normalized_path = root / "primary-alignment-phase2-complete.private.json"
        _write_immutable(normalized_path, normalized)
        accounting = _aggregate_usage(sidecars)
        cumulative_lower = _sum_usage(
            frozen["predecessor"]["cumulative_known_lower_bound"],
            accounting["usage"],
        )
        terminal = {
            "schema_version": V185_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": "v185_primary_alignment_phase2_completed_phase3_authorized",
            "overall_evaluation_complete": False,
            "support_receipts_frozen": True,
            "primary_alignment_complete": False,
            "primary_phase1_complete": True,
            "primary_phase2_complete": True,
            "primary_phase3_authorized": True,
            "balanced_canary_authorized": False,
            "selection_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "v184_turns_replayed": False,
            "adopted_phase1_case_count": 9,
            "fresh_phase2_case_count": FRESH_CASE_COUNT,
            "phase2_normalized_output": _record(normalized_path),
            "adopted_phase1_output": frozen["predecessor"]["records"]["normalized"],
            "cumulative_usage_status": "unknown",
            "cumulative_unknown_usage_turn_count": frozen["predecessor"][
                "cumulative_unknown_usage_turn_count"
            ],
            "cumulative_known_usage_lower_bound": cumulative_lower,
            "cumulative_conservative_unknown_usage_upper_bound": frozen[
                "predecessor"
            ]["cumulative_unknown_usage_upper_bound"],
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
    parser = argparse.ArgumentParser(description="Run v185 primary alignment phase two")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v185(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "primary_phase3_authorized": terminal.get(
                    "primary_phase3_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
                "cumulative_usage_status": terminal.get("cumulative_usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
