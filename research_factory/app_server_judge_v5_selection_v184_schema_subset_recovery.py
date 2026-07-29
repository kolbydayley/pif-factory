from __future__ import annotations

"""Recover v183 by removing only unsupported Structured Outputs keywords."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v183_missing_group_repair as v183
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
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .app_server_llm_judge import validate_app_server_output_schema_subset
from .util import now_iso, sha256_text


V184_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v184_spec_v1"
V184_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v184_terminal_v1"
V184_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v184_failure_v1"
V184_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V184_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V184_PHASE_ID = "judge_v5_4_selection_v184_schema_subset_recovery"

MODEL = v183.MODEL
EFFORT = v183.EFFORT
TURN_NAME = "selection_alignment_missing_group_repair_schema_recovery"
TURN_NAMES = (TURN_NAME,)
MAXIMUM_TOTAL_TOKENS_PER_TURN = v183.MAXIMUM_TOTAL_TOKENS_PER_TURN
TIMEOUT_SECONDS = v183.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v183.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v184-missing-group-schema-subset-recovery"
).resolve()


class JudgeV5SelectionV184Error(RuntimeError):
    """The immutable v184 schema-subset recovery cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _remove_unsupported_schema_keywords(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _remove_unsupported_schema_keywords(child)
            for key, child in value.items()
            if key != "uniqueItems"
        }
    if isinstance(value, list):
        return [_remove_unsupported_schema_keywords(child) for child in value]
    return deepcopy(value)


def repair_schema_v184(value: Mapping[str, Any]) -> dict[str, Any]:
    original = v183.repair_schema(value)
    corrected = _remove_unsupported_schema_keywords(original)
    errors = validate_app_server_output_schema_subset(corrected)
    if errors:
        raise JudgeV5SelectionV184Error("v184 schema exceeds Structured Outputs subset")
    return corrected


def _validate_v183_schema_failure() -> dict[str, Any]:
    root = v183.DEFAULT_OUTPUT_ROOT
    turn_root = root / "turns" / v183.TURN_NAME.replace("_", "-")
    paths = {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "spec": root / "missing-group-repair-spec.json",
        "policy": root / "capacity-policy.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "output": turn_root / "output.private.json",
    }
    values = {
        name: _load_json(path, f"v183 {name}")
        for name, path in paths.items()
        if name not in {"output", "prompt"}
    }
    terminal, failure, spec, sidecar = (
        values["terminal"],
        values["failure"],
        values["spec"],
        values["sidecar"],
    )
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not False
        or terminal.get("usage_status") != "unknown"
        or terminal.get("usage") is not None
        or terminal.get("primary_phase2_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != v183.TURN_NAME
        or failure.get("error_class") != "ReserveCapacityError"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("accounting_complete") is not False
        or failure.get("usage_status") != "unknown"
        or failure.get("usage") is not None
        or failure.get("unknown_usage_turn_count") != 1
        or len(failure.get("attempts") or []) != 1
        or spec.get("state") != "frozen_before_model_calls"
        or spec.get("turn_plan") != list(v183.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or sidecar.get("state") != "failed"
        or sidecar.get("status") != "failed"
        or sidecar.get("error_class") != "turn_failed"
        or sidecar.get("usage_status") != "unknown"
        or sidecar.get("usage_complete") is not False
        or sidecar.get("usage") is not None
        or sidecar.get("wall_elapsed_seconds") != 3.885
        or sidecar.get("turn_error", {}).get("codex_error_info") != "other"
        or sidecar.get("turn_error", {}).get("message_bytes") != 360
        or sidecar.get("turn_error", {}).get("message_sha256")
        != "dc339b9da4c362863d68bf2352b8a9e0cf1794db961e9826e1503dcf9aee15a2"
        or paths["output"].exists()
    ):
        raise JudgeV5SelectionV184Error("v183 immutable schema failure drifted")
    if terminal.get("failure") != _record(paths["failure"]):
        raise JudgeV5SelectionV184Error("v183 failure binding drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV184Error("v183 runtime record drifted")
    predecessor = v183._validate_v182_failure()
    value = v183.build_repair_input(predecessor)
    prompt = v183.repair_prompt(value)
    original_schema = v183.repair_schema(value)
    if (
        values["input"] != value
        or paths["prompt"].read_text() != prompt
        or values["schema"] != original_schema
        or spec.get("frozen_inputs", {}).get("turn", {}).get("input") != _record(paths["input"])
        or spec.get("frozen_inputs", {}).get("turn", {}).get("prompt") != _record(paths["prompt"])
        or spec.get("frozen_inputs", {}).get("turn", {}).get("schema") != _record(paths["schema"])
    ):
        raise JudgeV5SelectionV184Error("v183 frozen request drifted")
    unsupported = validate_app_server_output_schema_subset(original_schema)
    if unsupported != [
        "$.properties.assignments.items.properties.source_evidence_spans.uniqueItems",
        "$.properties.assignments.items.properties.witness_evidence_ids.uniqueItems",
    ]:
        raise JudgeV5SelectionV184Error("v183 unsupported schema diagnosis drifted")
    if terminal.get("cumulative_known_usage_lower_bound") != predecessor["cumulative_usage"]:
        raise JudgeV5SelectionV184Error("v183 cumulative lower bound drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {
            name: _record(path) for name, path in paths.items() if path.exists()
        },
        "predecessor": predecessor,
        "value": value,
        "prompt": prompt,
        "original_schema": original_schema,
        "unsupported_schema_paths": unsupported,
        "cumulative_known_lower_bound": predecessor["cumulative_usage"],
        "predecessor_unknown_usage_turn_count": 1,
        "predecessor_unknown_usage_upper_bound": MAXIMUM_TOTAL_TOKENS_PER_TURN,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V184_CAPACITY_AUDIT_VERSION,
        "phase_id": V184_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v183_terminal": predecessor["records"]["terminal"],
        "v183_failure": predecessor["records"]["failure"],
        "measured_basis": {
            "v183_unknown_usage_turn_count": 1,
            "v183_unknown_usage_upper_bound": bound,
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": bound,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V184_CAPACITY_POLICY_VERSION,
        "phase_id": V184_PHASE_ID,
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
        "maximum_total_tokens_per_turn": bound,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v184(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v184 terminal")}
    predecessor = _validate_v183_schema_failure()
    schema = repair_schema_v184(predecessor["value"])
    prompt_bytes, schema_bytes = v183.v177._request_bytes(predecessor["prompt"], schema)
    if prompt_bytes > v183.MAXIMUM_PROMPT_BYTES or schema_bytes > v183.MAXIMUM_SCHEMA_BYTES:
        raise JudgeV5SelectionV184Error("v184 corrected request exceeds frozen byte cap")
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=predecessor["value"],
        prompt=predecessor["prompt"],
        schema=schema,
    )
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V184_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V184_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "identical_v183_repair_with_unsupported_schema_keywords_removed_only",
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "v183_attempt_replayed_in_place": False,
        "v183_usage_status": "unknown",
        "v183_unknown_usage_turn_count": 1,
        "v183_unknown_usage_upper_bound": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "prompt_identical_to_v183": True,
        "input_identical_to_v183": True,
        "base_instructions_identical_to_v183": True,
        "semantic_validator_identical_to_v183": True,
        "schema_change_only": True,
        "removed_schema_keywords": predecessor["unsupported_schema_paths"],
        "prompt_bytes": prompt_bytes,
        "schema_bytes": schema_bytes,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "semantic_fields_pruned": False,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "primary_alignment_complete": False,
        "primary_phase2_authorized": False,
        "balanced_canary_authorized": False,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v183.__file__)),
            *predecessor["values"]["spec"]["runtime_files"],
        ],
        "frozen_instructions": {
            "repair_sha256": sha256_text(v183.missing_group_repair_instructions()),
        },
        "frozen_inputs": {
            "v183_input": predecessor["records"]["input"],
            "v183_prompt": predecessor["records"]["prompt"],
            "v183_schema": predecessor["records"]["schema"],
            "turn": {
                "turn_name": TURN_NAME,
                "input": _record(paths["input"]),
                "prompt": _record(paths["prompt"]),
                "schema": _record(paths["schema"]),
            },
        },
        "privacy": "private_source_event_prompt_output_mapping_sanitized_counts_hashes_only",
    }
    spec_path = root / "schema-subset-recovery-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "predecessor": predecessor,
        "value": predecessor["value"],
        "prompt": predecessor["prompt"],
        "schema": schema,
        "paths": paths,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v184 sidecar"))
        except Exception:
            unknown += 1
            continue
        usage = _sum_usage(usage, measured)
    complete = unknown == 0
    cumulative_lower = _sum_usage(predecessor["cumulative_known_lower_bound"], usage)
    failure = {
        "schema_version": V184_FAILURE_VERSION,
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
        "predecessor_unknown_usage_turn_count": 1,
        "cumulative_unknown_usage_turn_count": 1 + unknown,
        "cumulative_known_usage_lower_bound": cumulative_lower,
        "cumulative_conservative_unknown_usage_upper_bound": (
            predecessor["predecessor_unknown_usage_upper_bound"]
            + unknown * MAXIMUM_TOTAL_TOKENS_PER_TURN
        ),
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V184_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "primary_alignment_complete": False,
        "primary_phase2_authorized": False,
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
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v184(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v184 terminal")
    frozen = freeze_v184(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            current_turn = TURN_NAME
            output, sidecar, _ = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=v183.missing_group_repair_instructions(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=len(frozen["value"]["omitted_witness_ids"]),
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: v183.validate_repair_output(
                    candidate, frozen["value"]
                ),
            )
        accounting = _aggregate_usage([sidecar])
        cumulative_lower = _sum_usage(
            frozen["predecessor"]["cumulative_known_lower_bound"],
            accounting["usage"],
        )
        if any(row["placement"] == "abstain" for row in output["assignments"]):
            terminal = {
                "schema_version": V184_TERMINAL_VERSION,
                "state": "inactive",
                "terminal_at": now_iso(),
                "terminal_reason": "v184_missing_group_repair_abstained_recovery_required",
                "overall_evaluation_complete": False,
                "primary_alignment_complete": False,
                "primary_phase2_authorized": False,
                "holdout_authorized": False,
                "production_mutated": False,
                "cumulative_usage_status": "unknown",
                "cumulative_unknown_usage_turn_count": 1,
                "cumulative_known_usage_lower_bound": cumulative_lower,
                **accounting,
            }
            _write_immutable(terminal_path, terminal)
            return terminal
        repaired, repair_audit = v183.apply_group_repair(
            frozen["predecessor"]["predecessor"], output
        )
        repaired_path = root / "repaired-primary-alignment-case.private.json"
        repair_audit_path = root / "missing-group-repair-audit.json"
        _write_immutable(repaired_path, repaired)
        _write_immutable(repair_audit_path, repair_audit)
        normalized_case = normalize_neutral_alignment_output(
            repaired,
            frozen["predecessor"]["predecessor"]["failed"]["row"]["value"],
        )["cases"]
        normalized = {
            "schema_version": "pif_app_server_judge_v5_4_selection_primary_alignment_phase1_complete_v184_v1",
            "cases": sorted(
                [
                    *frozen["predecessor"]["predecessor"]["normalized_prefix"]["cases"],
                    *normalized_case,
                ],
                key=lambda row: str(row["case_id"]),
            ),
            "case_count": 9,
            "missing_group_repair_count": 2,
            "origin_neutral": True,
        }
        normalized_path = root / "primary-alignment-phase1-complete.private.json"
        _write_immutable(normalized_path, normalized)
        terminal = {
            "schema_version": V184_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": "v184_schema_subset_recovery_completed_phase2_authorized",
            "overall_evaluation_complete": False,
            "v183_attempt_replayed_in_place": False,
            "v183_unknown_usage_turn_count": 1,
            "repair_turn_count": 1,
            "repaired_witness_count": 2,
            "primary_alignment_complete": False,
            "primary_phase1_complete": True,
            "primary_phase2_authorized": True,
            "balanced_canary_authorized": False,
            "selection_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "normalized_output": _record(normalized_path),
            "repaired_case": _record(repaired_path),
            "repair_audit": _record(repair_audit_path),
            "cumulative_usage_status": "unknown",
            "cumulative_unknown_usage_turn_count": 1,
            "cumulative_known_usage_lower_bound": cumulative_lower,
            "cumulative_conservative_unknown_usage_upper_bound": (
                frozen["predecessor"]["predecessor_unknown_usage_upper_bound"]
            ),
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
    parser = argparse.ArgumentParser(description="Run v184 schema-subset recovery")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v184(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "primary_phase2_authorized": terminal.get("primary_phase2_authorized", False),
                "usage_status": terminal.get("usage_status"),
                "cumulative_usage_status": terminal.get("cumulative_usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
