from __future__ import annotations

"""Recover omitted alignment bookkeeping without changing LLM semantics."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from . import app_server_judge_v5_selection_v177_alignment_scale_diagnostic as v177
from . import app_server_judge_v5_selection_v179_primary_alignment_phase1 as v179
from . import app_server_judge_v5_selection_v180_phase1_bound_recovery as v180
from .app_server_judge_v5 import (
    normalize_neutral_alignment_output,
    validate_neutral_alignment_output,
)
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


V181_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v181_spec_v1"
V181_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v181_terminal_v1"
V181_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v181_failure_v1"
V181_ADOPTION_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v181_adoption_v1"
V181_PROJECTION_VERSION = "pif_app_server_judge_v5_4_selection_structural_unpaired_projection_v1"
V181_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V181_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V181_PHASE_ID = "judge_v5_4_selection_v181_structural_partition_recovery"

MODEL = v177.MODEL
EFFORT = v177.EFFORT
SOURCE_START_INDEX = 3
FRESH_CASE_COUNT = 2
TURN_NAMES = tuple(
    f"selection_alignment_primary_phase1_structural_recovery_{index:02d}"
    for index in range(FRESH_CASE_COUNT)
)
MAXIMUM_TOTAL_TOKENS_PER_TURN = v180.MAXIMUM_TOTAL_TOKENS_PER_TURN
TIMEOUT_SECONDS = v180.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v180.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v181-primary-alignment-structural-partition-recovery"
).resolve()


class JudgeV5SelectionV181Error(RuntimeError):
    """The immutable v181 structural recovery cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def project_structural_unpaired_and_exact_spans(
    output: Mapping[str, Any], alignment_input: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Complete only the pair-or-unpaired ID partition, then apply v157 projection."""

    original_errors = validate_neutral_alignment_output(output, alignment_input)
    if any(not error.endswith("_alignment_partition_mismatch") for error in original_errors):
        raise JudgeV5SelectionV181Error("alignment output has a non-structural defect")
    projected_input = deepcopy(output)
    expected = {
        str(case["case_id"]): case for case in alignment_input.get("cases") or []
    }
    added_rows = []
    for case_index, row in enumerate(projected_input.get("cases") or []):
        case_id = str(row.get("case_id"))
        if case_id not in expected:
            raise JudgeV5SelectionV181Error("alignment case coverage drifted")
        all_ids = {
            str(item["witness_id"]) for item in expected[case_id].get("witnesses") or []
        }
        used: set[str] = set()
        for pair in row.get("alignment_pairs") or []:
            first, second = pair.get("witness_id_1"), pair.get("witness_id_2")
            if (
                first not in all_ids
                or second not in all_ids
                or first == second
                or first in used
                or second in used
            ):
                raise JudgeV5SelectionV181Error("alignment pair identity partition drifted")
            used.update((first, second))
        unpaired = row.get("unpaired_witness_ids")
        if (
            not isinstance(unpaired, list)
            or len(unpaired) != len(set(unpaired))
            or any(item not in all_ids or item in used for item in unpaired)
        ):
            raise JudgeV5SelectionV181Error("alignment unpaired identity set drifted")
        missing = sorted(all_ids - used - set(unpaired))
        row["unpaired_witness_ids"] = [*unpaired, *missing]
        added_rows.append(
            {
                "case_index": case_index,
                "added_unpaired_witness_count": len(missing),
            }
        )
    projected, exact_audit = v157.project_exact_spans_and_relation(
        projected_input, alignment_input
    )
    audit = {
        **exact_audit,
        "schema_version": V181_PROJECTION_VERSION,
        "original_validation_errors": original_errors,
        "structural_unpaired_addition_count": sum(
            row["added_unpaired_witness_count"] for row in added_rows
        ),
        "structural_unpaired_additions": added_rows,
        "semantic_equivalence_groups_changed": False,
        "semantic_alignment_pairs_changed": False,
        "semantic_checklist_decisions_changed": False,
        "rationales_changed": False,
        "deterministic_operations": [
            "complete_pair_or_unpaired_identity_partition_by_exact_set_difference",
            *exact_audit["deterministic_operations"],
        ],
    }
    return projected, audit


def validate_structurally_completable_output(
    output: Any, alignment_input: Mapping[str, Any]
) -> list[str]:
    try:
        project_structural_unpaired_and_exact_spans(output, alignment_input)
    except Exception:
        errors = validate_neutral_alignment_output(output, alignment_input)
        return errors or ["structural_unpaired_projection_failed"]
    return []


def _validate_v180_failure() -> dict[str, Any]:
    root = v180.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "spec": root / "primary-alignment-phase1-bound-recovery1-spec.json",
        "policy": root / "capacity-policy.json",
        "adoption": root / "v179-output-adoption-receipt.json",
    }
    values = {name: _load_json(path, f"v180 {name}") for name, path in paths.items()}
    terminal, failure, spec = values["terminal"], values["failure"], values["spec"]
    usage = {
        "cached_input_tokens": 3840,
        "input_tokens": 107394,
        "output_tokens": 43630,
        "reasoning_output_tokens": 20554,
        "total_tokens": 151024,
    }
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != usage
        or terminal.get("primary_phase1_recovery2_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != v180.TURN_NAMES[1]
        or failure.get("error_class") != "JudgeV5CalibrationV26DiagnosticError"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("accounting_complete") is not True
        or failure.get("usage") != usage
        or failure.get("unknown_usage_turn_count") != 0
        or len(failure.get("attempts") or []) != 2
        or spec.get("state") != "frozen_before_model_calls"
        or spec.get("turn_plan") != list(v180.TURN_NAMES)
        or spec.get("v179_completed_output_adopted") is not True
        or spec.get("v179_completed_output_replayed") is not False
        or spec.get("retry_count_per_turn") != 0
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5SelectionV181Error("v180 immutable failure contract drifted")
    if terminal.get("failure") != _record(paths["failure"]):
        raise JudgeV5SelectionV181Error("v180 failure binding drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV181Error("v180 runtime record drifted")
    predecessor = v180._validate_v179_policy_failure()
    partition = predecessor["partition"]
    rows = partition["phases"][0][1:5]
    frozen_turns = spec.get("frozen_inputs", {}).get("turns") or []
    if len(rows) != 4 or len(frozen_turns) != 4:
        raise JudgeV5SelectionV181Error("v180 turn coverage drifted")

    completed = []
    measured = {field: 0 for field in USAGE_FIELDS}
    for index in range(2):
        row = rows[index]
        frozen_turn = frozen_turns[index]
        turn_name = v180.TURN_NAMES[index]
        turn_root = root / "turns" / turn_name.replace("_", "-")
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
            or _load_json(turn_paths["input"], "v180 completed input") != row["value"]
            or turn_paths["prompt"].read_text() != row["prompt"]
            or _load_json(turn_paths["schema"], "v180 completed schema") != row["schema"]
        ):
            raise JudgeV5SelectionV181Error("v180 completed request drifted")
        output, sidecar = _validate_completed_turn(
            paths=turn_paths,
            prompt=row["prompt"],
            schema=row["schema"],
            base_instructions=v130.alignment_instructions_v130(),
            model=MODEL,
            effort=EFFORT,
            policy_path=paths["policy"],
            output_validator=lambda candidate: [],
        )
        turn_usage = _validate_usage(sidecar)
        measured = _sum_usage(measured, turn_usage)
        projected, audit = project_structural_unpaired_and_exact_spans(
            output, row["value"]
        )
        normalized = normalize_neutral_alignment_output(projected, row["value"])
        completed.append(
            {
                "row": row,
                "turn_name": turn_name,
                "paths": turn_paths,
                "output": output,
                "sidecar": sidecar,
                "projected": projected,
                "audit": audit,
                "normalized": normalized,
            }
        )
    if (
        measured != usage
        or completed[0]["audit"]["structural_unpaired_addition_count"] != 0
        or completed[1]["audit"]["structural_unpaired_addition_count"] != 50
        or completed[0]["projected"]
        != _load_json(
            completed[0]["paths"]["output"].parent / "alignment-projected.private.json",
            "v180 projected output",
        )
    ):
        raise JudgeV5SelectionV181Error("v180 structural recovery evidence drifted")
    expected_cumulative = _sum_usage(predecessor["cumulative_usage"], usage)
    if terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative:
        raise JudgeV5SelectionV181Error("v180 cumulative usage drifted")
    for index in range(2, 4):
        later_root = root / "turns" / v180.TURN_NAMES[index].replace("_", "-")
        if any((later_root / name).exists() for name in ("capacity.json", "sidecar.json", "output.private.json")):
            raise JudgeV5SelectionV181Error("v180 executed a turn after structural failure")

    normalized_cases = [*predecessor["normalized"]["cases"]]
    for item in completed:
        normalized_cases.extend(item["normalized"]["cases"])
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "predecessor": predecessor,
        "partition": partition,
        "rows": rows,
        "completed": completed,
        "normalized": {
            "schema_version": "pif_app_server_judge_v5_4_selection_primary_alignment_v181_adopted_v1",
            "cases": sorted(normalized_cases, key=lambda row: str(row["case_id"])),
            "adopted_v179_case_count": 1,
            "adopted_v180_case_count": 2,
            "mismatch_fields_projected_from_checklists": True,
            "origin_neutral": True,
        },
        "usage": usage,
        "cumulative_usage": expected_cumulative,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V181_CAPACITY_AUDIT_VERSION,
        "phase_id": V181_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v180_terminal": predecessor["records"]["terminal"],
        "v180_failure": predecessor["records"]["failure"],
        "measured_basis": {
            "v180_maximum_completed_turn_total_tokens": max(
                item["sidecar"]["usage"]["total_tokens"]
                for item in predecessor["completed"]
            ),
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
        "schema_version": V181_CAPACITY_POLICY_VERSION,
        "phase_id": V181_PHASE_ID,
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


def freeze_v181(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v181 terminal")}
    predecessor = _validate_v180_failure()
    rows = predecessor["partition"]["phases"][0][
        SOURCE_START_INDEX : SOURCE_START_INDEX + FRESH_CASE_COUNT
    ]
    if len(rows) != FRESH_CASE_COUNT:
        raise JudgeV5SelectionV181Error("v181 fresh tranche coverage drifted")

    adopted_normalized_path = root / "adopted-alignment-normalized.private.json"
    _write_immutable(adopted_normalized_path, predecessor["normalized"])
    completed_records = []
    for index, item in enumerate(predecessor["completed"]):
        projected_path = root / f"adopted-v180-{index:02d}-projected.private.json"
        audit_path = root / f"adopted-v180-{index:02d}-projection-audit.json"
        _write_immutable(projected_path, item["projected"])
        _write_immutable(audit_path, item["audit"])
        completed_records.append(
            {
                "source_sidecar": _record(item["paths"]["sidecar"]),
                "source_output": _record(item["paths"]["output"]),
                "projected_output": _record(projected_path),
                "projection_audit": _record(audit_path),
                "structural_unpaired_addition_count": item["audit"][
                    "structural_unpaired_addition_count"
                ],
            }
        )
    adoption_path = root / "v180-output-adoption-receipt.json"
    adoption = {
        "schema_version": V181_ADOPTION_VERSION,
        "created_at": now_iso(),
        "v180_terminal": predecessor["records"]["terminal"],
        "v180_failure": predecessor["records"]["failure"],
        "v180_completed_turn_count": 2,
        "v180_completed_turns_replayed": False,
        "new_semantic_turn_count_for_adoption": 0,
        "structural_projection_only": True,
        "semantic_equivalence_groups_changed": False,
        "semantic_alignment_pairs_changed": False,
        "semantic_checklists_changed": False,
        "rationales_changed": False,
        "structural_unpaired_addition_count": 50,
        "completed_turns": completed_records,
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
        "schema_version": V181_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V181_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "structural_unpaired_projection_then_two_never_started_primary_cases",
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "v180_completed_turns_replayed": False,
        "v180_completed_turns_adopted": 2,
        "fresh_case_count": FRESH_CASE_COUNT,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "structural_projection_changes_semantic_groups_pairs_or_checklists": False,
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
            _record(Path(v180.__file__)),
            _record(Path(v179.__file__)),
            _record(Path(v157.__file__)),
            _record(Path(v130.__file__)),
            *predecessor["values"]["spec"]["runtime_files"],
        ],
        "frozen_instructions": {
            "alignment_base_sha256": sha256_text(v130.alignment_instructions_v130()),
        },
        "frozen_inputs": {
            "pool": predecessor["predecessor"]["predecessor"]["pool_record"],
            "support_receipts": predecessor["predecessor"]["predecessor"]["receipts_record"],
            "primary_partition": predecessor["predecessor"]["records"]["partition"],
            "v180_output_adoption": _record(adoption_path),
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
    spec_path = root / "primary-alignment-structural-partition-recovery-spec.json"
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v181 sidecar"))
        except Exception:
            unknown += 1
            continue
        usage = _sum_usage(usage, measured)
    complete = unknown == 0
    cumulative = _sum_usage(predecessor["cumulative_usage"], usage)
    failure = {
        "schema_version": V181_FAILURE_VERSION,
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
        "schema_version": V181_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "v180_completed_turns_adopted": 2,
        "v180_completed_turns_replayed": False,
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


async def run_v181(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v181 terminal")
    frozen = freeze_v181(output_dir=root, timeout_seconds=timeout_seconds)
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
                        validate_structurally_completable_output(candidate, item)
                    ),
                )
                projected, projection_audit = project_structural_unpaired_and_exact_spans(
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
            "schema_version": "pif_app_server_judge_v5_4_selection_primary_alignment_phase1_structural_recovery_v1",
            "cases": sorted(
                [*frozen["adopted_normalized"]["cases"], *fresh_normalized_cases],
                key=lambda row: str(row["case_id"]),
            ),
            "adopted_case_count": 3,
            "fresh_v181_case_count": FRESH_CASE_COUNT,
            "mismatch_fields_projected_from_checklists": True,
            "origin_neutral": True,
        }
        normalized_path = root / "primary-alignment-phase1-structural-recovery-normalized.private.json"
        _write_immutable(normalized_path, normalized)
        accounting = _aggregate_usage(sidecars)
        cumulative = _sum_usage(
            frozen["predecessor"]["cumulative_usage"], accounting["usage"]
        )
        terminal = {
            "schema_version": V181_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": "v181_structural_partition_recovery_completed_recovery2_authorized",
            "overall_evaluation_complete": False,
            "v180_completed_turns_adopted": 2,
            "v180_completed_turns_replayed": False,
            "v180_output_adoption": _record(frozen["adoption_path"]),
            "support_receipts_frozen": True,
            "primary_alignment_complete": False,
            "primary_phase1_recovery2_authorized": True,
            "balanced_canary_authorized": False,
            "selection_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "adopted_case_count": 3,
            "fresh_case_count": FRESH_CASE_COUNT,
            "partial_phase1_case_count": len(normalized["cases"]),
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
    parser = argparse.ArgumentParser(description="Run v181 structural partition recovery")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v181(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
