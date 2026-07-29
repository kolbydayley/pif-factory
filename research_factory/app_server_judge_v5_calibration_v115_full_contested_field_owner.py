from __future__ import annotations

"""Full Luna owner pass over every contested pointwise field decision."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_TOKENS_PER_TURN,
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    _field_contracts,
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v94_systematic_field_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V94_ROOT,
)
from .app_server_judge_v5_calibration_v97_residual_field_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V97_ROOT,
)
from .app_server_judge_v5_calibration_v99_refined_luna_diagnostic import (
    _general_requested_field_value,
)
from .app_server_judge_v5_calibration_v112_luna_minimal_root_diagnostic import (
    _checklist_map,
    _validate_sources,
    base_instructions_v112,
)
from .app_server_judge_v5_calibration_v114_final_field_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V114_ROOT,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .util import now_iso, sha256_text


V115_INPUT_VERSION = "pif_app_server_judge_v5_4_v115_contested_field_input_v1"
V115_TRUTH_VERSION = "pif_app_server_judge_v5_4_v115_contested_field_truth_v1"
V115_SELECTION_VERSION = "pif_app_server_judge_v5_4_v115_selection_v1"
V115_SPEC_VERSION = "pif_app_server_judge_v5_4_v115_spec_v1"
V115_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v115_output_v1"
V115_SCORE_VERSION = "pif_app_server_judge_v5_4_v115_score_v1"
V115_REFERENCE_VERSION = "pif_app_server_judge_v5_4_calibration_truth_v9_pointwise_candidate"
V115_FAILURE_VERSION = "pif_app_server_judge_v5_4_v115_failure_v1"
V115_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v115_terminal_v1"
V115_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V115_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V115_PHASE_ID = "judge_v5_4_v115_full_contested_field_owner"

MODEL = "gpt-5.6-luna"
EFFORT = "high"
PRIMARY_TASKS_PER_TURN = 16
CANARY_TASKS_PER_TURN = 12
PRIMARY_TURNS = tuple(f"contested_field_owner_shard_{index:02d}" for index in range(10))
CANARY_TURNS = ("contested_field_owner_canary_00",)
TURN_NAMES = PRIMARY_TURNS + CANARY_TURNS
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V114_ROOT.parent / "judge-calibration-v5_4-v115-full-contested-field-owner"
).resolve()


class JudgeV5CalibrationV115Error(RuntimeError):
    """The v115 full contested-field owner contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v114(v114_root: Path = DEFAULT_V114_ROOT) -> dict[str, Any]:
    root = v114_root.expanduser().resolve()
    paths = {
        "spec": root / "final-owner-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "final-owner-score.json",
        "resolution": root / "resolution.json",
        "input": root / "final-owner-input.private.json",
        "truth": root / "final-owner-truth.private.json",
        "output": root / "final-owner-output.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v114 {name}") for name, path in paths.items()}
    terminal, score, resolution, spec = (
        values["terminal"],
        values["score"],
        values["resolution"],
        values["spec"],
    )
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v114_final_field_owner_passed_contested_owner_authorized"
        or terminal.get("reference_patch_authorized") is not True
        or terminal.get("contested_field_reference_owner_authorized") is not True
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 22901
        or score.get("passed") is not True
        or score.get("failed_checks") != []
        or score.get("metrics", {}).get("matched_control_exact_count") != 4
        or score.get("fixture_truth_dispute_status") != "incorrect"
        or score.get("permutation_dispute_status") != "correct"
        or resolution.get("state") != "authorized"
        or resolution.get("contested_field_reference_owner_authorized") is not True
        or resolution.get("fresh_full_calibration_authorized") is not False
        or resolution.get("selection_authorized") is not False
        or resolution.get("holdout_authorized") is not False
        or resolution.get("production_mutated") is not False
        or resolution.get("majority_voting_used") is not False
        or spec.get("model") != "gpt-5.6-terra"
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5CalibrationV115Error("v114 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV115Error("v114 runtime record drifted")
    turn_root = root / "turns" / "final-side-free-field-owner"
    turn_paths = {
        name: turn_root / filename
        for name, filename in {
            "capacity": "capacity.json",
            "input": "input.private.json",
            "prompt": "prompt.private.md",
            "schema": "schema.json",
            "sidecar": "sidecar.json",
            "output": "output.private.json",
        }.items()
    }
    if any(not path.is_file() for path in turn_paths.values()):
        raise JudgeV5CalibrationV115Error("v114 turn coverage is incomplete")
    measured = _validate_usage(_load_json(turn_paths["sidecar"], "v114 sidecar"))
    if measured != terminal.get("usage"):
        raise JudgeV5CalibrationV115Error("v114 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "turn_records": {name: _record(path) for name, path in turn_paths.items()},
        "usage": measured,
    }


def _task_id(case_id: str, witness_id: str, field: str) -> str:
    return "field_" + sha256_text(f"v115|{case_id}|{witness_id}|{field}")[:24]


def _canary_id(owner_task_id: str) -> str:
    return "perm_" + sha256_text(f"v115|canary|{owner_task_id}")[:24]


def _select_canaries(truth_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    selected = []
    used_witnesses = set()
    for role, count in (("consensus_reference_dispute", 6), ("model_disagreement", 6)):
        candidates = sorted(
            [row for row in truth_rows if row["role"] == role],
            key=lambda row: sha256_text(
                f"v115-canary|{role}|{row['field']}|{row['case_id']}|{row['witness_id']}"
            ),
        )
        role_selected = []
        used_fields = set()
        for row in candidates:
            if row["witness_id"] in used_witnesses or row["field"] in used_fields:
                continue
            role_selected.append(row)
            used_witnesses.add(row["witness_id"])
            used_fields.add(row["field"])
            if len(role_selected) == count:
                break
        if len(role_selected) != count:
            raise JudgeV5CalibrationV115Error("v115 canary diversity pool is too small")
        selected.extend(role_selected)
    return selected


def build_v115_inputs(
    sources: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    values = sources["values"]
    truth = values["v109_truth"]
    gpt = _checklist_map(values["v109_checklist"])
    sol = _checklist_map(values["v110_checklist"])
    units = {str(row["witness_id"]): row for row in sources["units"]}
    witness_to_case = {
        witness_id: case_id
        for case_id, case in truth["cases"].items()
        for witness_id in case["proposition"]
    }
    contracts = _field_contracts()
    contracts.update(deepcopy(values["v94_rubric"]["field_contracts"]))
    contracts.update(deepcopy(values["v97_rubric"]["field_contracts"]))
    tasks = []
    truth_rows = []
    for witness_id, unit in units.items():
        case_id = witness_to_case[witness_id]
        expected_issues = set(truth["cases"][case_id]["field_issues"][witness_id])
        event = compact_empty_event_fields(deepcopy(unit["structured_event"]))
        for field, gpt_status in gpt[witness_id].items():
            current_status = "incorrect" if field in expected_issues else "correct"
            sol_status = sol[witness_id][field]
            if current_status == gpt_status == sol_status:
                continue
            role = (
                "consensus_reference_dispute"
                if gpt_status == sol_status
                else "model_disagreement"
            )
            task_id = _task_id(case_id, witness_id, field)
            tasks.append(
                {
                    "task_id": task_id,
                    "field": field,
                    "field_contract": deepcopy(contracts[field]),
                    "requested_field_value": _general_requested_field_value(field, event),
                    "source_excerpt": unit["source_excerpt"],
                    "structured_event": event,
                }
            )
            truth_rows.append(
                {
                    "task_id": task_id,
                    "case_id": case_id,
                    "witness_id": witness_id,
                    "field": field,
                    "role": role,
                    "current_status": current_status,
                    "gpt55_status": gpt_status,
                    "sol_status": sol_status,
                    "proposition_status": truth["cases"][case_id]["proposition"][witness_id],
                }
            )
    tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    role_counts = {
        role: sum(row["role"] == role for row in truth_rows)
        for role in ("consensus_reference_dispute", "model_disagreement")
    }
    if len(tasks) != 145 or role_counts != {
        "consensus_reference_dispute": 93,
        "model_disagreement": 52,
    }:
        raise JudgeV5CalibrationV115Error("v115 contested task coverage drifted")
    task_by_id = {row["task_id"]: row for row in tasks}
    canary_owners = _select_canaries(truth_rows)
    canary_tasks = []
    canary_map = []
    for row in reversed(canary_owners):
        task = deepcopy(task_by_id[row["task_id"]])
        canary_task_id = _canary_id(row["task_id"])
        task["task_id"] = canary_task_id
        canary_tasks.append(task)
        canary_map.append(
            {"owner_task_id": row["task_id"], "canary_task_id": canary_task_id}
        )
    value = {
        "schema_version": V115_INPUT_VERSION,
        "task_count": 145,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V115_TRUTH_VERSION,
        "task_count": 145,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V115_INPUT_VERSION,
        "task_count": 12,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V115_SELECTION_VERSION,
        "created_at": now_iso(),
        "source_field_decision_count": 795,
        "unanimous_retained_count": 650,
        "contested_task_count": 145,
        "contested_role_counts": role_counts,
        "permutation_canary_count": 12,
        "permutation_canary_role_counts": {
            "consensus_reference_dispute": 6,
            "model_disagreement": 6,
        },
        "selection_uses_source_text": False,
        "selection_uses_only_prior_llm_decisions_reference_status_field_enums_and_opaque_ids": True,
        "semantic_pruning_performed": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "empty_event_fields_omitted_only": True,
        "privacy": "private_source_event_output_aggregate_reports_only",
    }
    return value, truth_value, canary, selection


def _shards(value: Mapping[str, Any], size: int) -> list[dict[str, Any]]:
    tasks = value.get("tasks") or []
    result = []
    for index in range(0, len(tasks), size):
        result.append(
            {
                **{key: deepcopy(child) for key, child in value.items() if key != "tasks"},
                "task_count": len(tasks[index : index + size]),
                "tasks": deepcopy(tasks[index : index + size]),
                "shard_ordinal": index // size,
                "shard_count": math.ceil(len(tasks) / size),
            }
        )
    return result


def base_instructions_v115() -> str:
    return base_instructions_v112() + (
        " For unsupported_inference, mark incorrect exactly when claim_text adds a material assertion "
        "not entailed by the source, and correct when every material proposition is source-supported."
    )


def build_prompt_v115(value: Mapping[str, Any]) -> str:
    return (
        "Return one independent final field decision for every opaque task_id. The first evidence span "
        "must directly support the requested field decision and every span must be an exact substring of "
        "that task's source_excerpt. Do not compare tasks or emit whole-event verdicts.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def merge_outputs(outputs: Sequence[Mapping[str, Any]], expected_count: int) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(decisions) != expected_count or len({str(row["task_id"]) for row in decisions}) != expected_count:
        raise JudgeV5CalibrationV115Error("v115 output coverage drifted")
    return {"schema_version": V115_OUTPUT_VERSION, "decisions": decisions}


def score_v115(
    primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {str(row["task_id"]): row for row in truth["tasks"]}
    observed = {str(row["task_id"]): row for row in primary["decisions"]}
    repeated = {str(row["task_id"]): row for row in canary["decisions"]}
    if set(expected) != set(observed) or len(repeated) != 12:
        raise JudgeV5CalibrationV115Error("v115 score coverage drifted")
    canary_exact = sum(
        observed[row["owner_task_id"]]["field_status"]
        == repeated[row["canary_task_id"]]["field_status"]
        for row in truth["canary_map"]
    )
    all_rows = list(observed.values()) + list(repeated.values())
    abstentions = sum(row["field_status"] == "abstain" for row in all_rows)
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in all_rows)
    unsupported_conflicts = 0
    for row in expected.values():
        if row["field"] != "unsupported_inference":
            continue
        wanted = {
            "supported": "correct",
            "unsupported": "incorrect",
            "abstain": "abstain",
        }[row["proposition_status"]]
        unsupported_conflicts += int(observed[row["task_id"]]["field_status"] != wanted)
    checks = {
        "permutation_canary_exact_rate": canary_exact == 12,
        "abstention_count": abstentions == 0,
        "evidence_complete_rate": evidence_complete == 157,
        "unsupported_inference_consistency": unsupported_conflicts == 0,
    }
    trigger_count = (12 - canary_exact) + abstentions + unsupported_conflicts
    return {
        "schema_version": V115_SCORE_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": 145,
            "permutation_canary_count": 12,
            "permutation_canary_exact_count": canary_exact,
            "abstention_count": abstentions,
            "evidence_complete_count": evidence_complete,
            "unsupported_inference_conflict_count": unsupported_conflicts,
            "observable_repair_trigger_count": trigger_count,
        },
        "pointwise_reference_patch_authorized": all(checks.values()),
        "capped_repair_authorized": not all(checks.values()) and 0 < trigger_count <= 12,
        "capped_repair_limit": 12,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "majority_voting_used": False,
    }


def build_reference_candidate(
    *, current_truth: Mapping[str, Any], truth: Mapping[str, Any], primary: Mapping[str, Any]
) -> dict[str, Any]:
    candidate = deepcopy(current_truth)
    observed = {str(row["task_id"]): row for row in primary["decisions"]}
    changed = 0
    for row in truth["tasks"]:
        case = candidate["cases"][row["case_id"]]
        fields = set(case["field_issues"][row["witness_id"]])
        before = row["field"] in fields
        after = observed[row["task_id"]]["field_status"] == "incorrect"
        if after:
            fields.add(row["field"])
        else:
            fields.discard(row["field"])
        case["field_issues"][row["witness_id"]] = sorted(fields)
        case["structured_fields"][row["witness_id"]] = (
            "incorrect" if fields else "correct"
        )
        changed += int(before != after)
    candidate["schema_version"] = V115_REFERENCE_VERSION
    candidate["reference_version"] = "fixture_reference_v9_pointwise_owner_candidate"
    candidate["pointwise_field_change_count"] = changed
    candidate["pointwise_reference_patch_authorized"] = True
    candidate["alignment_reference_frozen"] = False
    return candidate


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V115_CAPACITY_AUDIT_VERSION,
        "phase_id": V115_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V115_CAPACITY_POLICY_VERSION,
        "phase_id": V115_PHASE_ID,
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
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": math.ceil(
            bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v115(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v115 terminal")}
    v114 = _validate_v114()
    sources = _validate_sources()
    value, truth, canary, selection = build_v115_inputs(sources)
    input_path = root / "contested-field-input.private.json"
    truth_path = root / "contested-field-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_time(selection_path, selection, "created_at")
    primary = _shards(value, PRIMARY_TASKS_PER_TURN)
    canary_shards = _shards(canary, CANARY_TASKS_PER_TURN)
    if len(primary) != 10 or len(canary_shards) != 1:
        raise JudgeV5CalibrationV115Error("v115 shard count drifted")
    turns = []
    for turn_name, turn_value in zip(TURN_NAMES, primary + canary_shards, strict=True):
        prompt = build_prompt_v115(turn_value)
        schema = output_schema(turn_value)
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=turn_value,
            prompt=prompt,
            schema=schema,
        )
        turns.append(
            {
                "turn_name": turn_name,
                "value": turn_value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    predecessor = {
        **sources["records"],
        **sources["prior_luna_records"],
        **{f"v114_{name}": record for name, record in v114["records"].items()},
        "v114_turn": v114["turn_records"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    runtime_files = [
        Path(__file__),
        runtime_dir / "app_server_judge_v5_calibration_v114_final_field_owner.py",
        runtime_dir / "app_server_judge_v5_calibration_v112_luna_minimal_root_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v99_refined_luna_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v97_residual_field_audit.py",
        runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py",
        runtime_dir / "app_server_capacity_reserve.py",
        runtime_dir / "codex_app_server.py",
    ]
    spec = {
        "schema_version": V115_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "full_side_free_owner_for_all_nonunanimous_pointwise_field_decisions_with_small_permutation_canary",
        "task_count": 145,
        "canary_task_count": 12,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "selection_uses_source_text": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_canaries_exact_all_evidence_zero_abstentions_zero_unsupported_consistency_conflicts",
        "capped_repair_limit": 12,
        "pointwise_reference_patch_authorized": False,
        "alignment_reference_frozen": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [_record(path) for path in runtime_files],
        "frozen_inputs": {
            "input": _record(input_path),
            "truth": _record(truth_path),
            "canary": _record(canary_path),
            "selection": _record(selection_path),
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "contested-field-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "current_truth": sources["values"]["v109_truth"],
    }


def _write_failure(*, root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v115 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V115_FAILURE_VERSION,
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
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V115_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "pointwise_reference_patch_authorized": False,
        "capped_repair_authorized": False,
        "alignment_reference_frozen": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v115(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v115 terminal")
    frozen = freeze_v115(output_dir=root, timeout_seconds=timeout_seconds)
    sidecars = []
    current_turn = None
    try:
        outputs = []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=base_instructions_v115(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(turn["value"]["tasks"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda value, item=turn["value"]: validate_output(value, item),
                )
                outputs.append(output)
                sidecars.append(sidecar)
        primary = merge_outputs(outputs[:10], 145)
        canary = merge_outputs(outputs[10:], 12)
        primary_path = root / "contested-field-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(primary_path, primary)
        _write_immutable(canary_path, canary)
        score = score_v115(primary, canary, frozen["truth"])
        score_path = root / "contested-field-owner-score.json"
        _write_immutable(score_path, score)
        candidate_path = root / "pointwise-reference-v9-candidate.private.json"
        if score["passed"]:
            candidate = build_reference_candidate(
                current_truth=frozen["current_truth"],
                truth=frozen["truth"],
                primary=primary,
            )
            _write_immutable(candidate_path, candidate)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V115_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v115_contested_field_owner_passed_pointwise_patch_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v115_contested_field_owner_passed"
                if passed
                else "v115_contested_field_owner_repair_or_recovery_required"
            ),
            "overall_evaluation_complete": False,
            "pointwise_reference_patch_authorized": passed,
            "capped_repair_authorized": score["capped_repair_authorized"],
            "alignment_reference_frozen": False,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "primary_output": _record(primary_path),
            "canary_output": _record(canary_path),
            "reference_candidate": _record(candidate_path) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root=root, turn_name=exc.turn_name, error_class=exc.error_class)
    except Exception as exc:
        return _write_failure(root=root, turn_name=current_turn, error_class=type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v115 full contested-field owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v115(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "pointwise_reference_patch_authorized": terminal.get(
                    "pointwise_reference_patch_authorized", False
                ),
                "capped_repair_authorized": terminal.get("capped_repair_authorized", False),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
