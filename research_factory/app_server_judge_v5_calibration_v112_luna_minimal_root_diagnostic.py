from __future__ import annotations

"""Fresh Luna diagnostic for independent minimal-root field adjudication."""

import argparse
import asyncio
import json
import math
from collections import Counter
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
from .app_server_judge_v5_calibration_v91_fresh_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V91_ROOT,
)
from .app_server_judge_v5_calibration_v94_systematic_field_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V94_ROOT,
)
from .app_server_judge_v5_calibration_v96_fresh_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V96_ROOT,
)
from .app_server_judge_v5_calibration_v97_residual_field_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V97_ROOT,
)
from .app_server_judge_v5_calibration_v99_refined_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V99_ROOT,
    _general_requested_field_value,
)
from . import app_server_judge_v5_calibration_v110_sol_reference_audit as v110
from .app_server_judge_v5_calibration_v111_sol_reference_recovery import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V111_ROOT,
    _validate_v110,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .util import now_iso, sha256_text


V112_INPUT_VERSION = "pif_app_server_judge_v5_4_v112_minimal_root_input_v1"
V112_TRUTH_VERSION = "pif_app_server_judge_v5_4_v112_minimal_root_truth_v1"
V112_SELECTION_VERSION = "pif_app_server_judge_v5_4_v112_selection_v1"
V112_SPEC_VERSION = "pif_app_server_judge_v5_4_v112_spec_v1"
V112_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v112_output_v1"
V112_SCORE_VERSION = "pif_app_server_judge_v5_4_v112_score_v1"
V112_FAILURE_VERSION = "pif_app_server_judge_v5_4_v112_failure_v1"
V112_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v112_terminal_v1"
V112_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V112_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V112_PHASE_ID = "judge_v5_4_v112_luna_minimal_root_diagnostic"

MODEL = "gpt-5.6-luna"
EFFORT = "high"
PRIMARY_TURNS = tuple(f"luna_minimal_root_shard_{index:02d}" for index in range(3))
CANARY_TURN = "luna_minimal_root_permutation_canary"
TURN_NAMES = PRIMARY_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V111_ROOT.parent / "judge-calibration-v5_4-v112-luna-minimal-root-diagnostic"
).resolve()

CONTROL_SLOTS = (
    ("attribution", "correct"),
    ("certainty", "correct"),
    ("event_boundary", "correct"),
    ("target", "correct"),
    ("actor", "incorrect"),
    ("evidence", "incorrect"),
    ("speaker", "incorrect"),
    ("unsupported_inference", "incorrect"),
)
CONSENSUS_DISPUTE_FIELDS = (
    "attribution",
    "speaker",
    "evidence",
    "target",
    "actor",
    "metric",
)
MODEL_DISAGREEMENT_FIELDS = (
    "certainty",
    "event_boundary",
    "attribution",
    "temporal_horizon",
)


class JudgeV5CalibrationV112Error(RuntimeError):
    """The v112 diagnostic cannot preserve its frozen side-free contract."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _load_prior_luna_pairs() -> tuple[set[tuple[str, str, str]], dict[str, Any]]:
    paths = {
        "v91_truth": DEFAULT_V91_ROOT / "fresh-luna-truth.private.json",
        "v96_truth": DEFAULT_V96_ROOT / "fresh-luna-v6-truth.private.json",
        "v99_truth": DEFAULT_V99_ROOT / "refined-luna-truth.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    pairs = {
        (str(row["case_id"]), str(row["witness_id"]), str(row["field"]))
        for value in values.values()
        for row in value.get("tasks") or []
        if row.get("case_id") and row.get("witness_id") and row.get("field")
    }
    return pairs, {name: _record(path) for name, path in paths.items()}


def _validate_sources() -> dict[str, Any]:
    v110_evidence = _validate_v110()
    paths = {
        "v111_terminal": DEFAULT_V111_ROOT / "terminal.json",
        "v111_score": DEFAULT_V111_ROOT / "owner-audit-score.json",
        "v111_taxonomy": DEFAULT_V111_ROOT / "sanitized-owner-disagreement-taxonomy.json",
        "v109_pointwise_input_00": v110.DEFAULT_V109_ROOT / "semantic-execution" / "turns" / "pointwise-checklist-shard-00" / "input.private.json",
        "v109_pointwise_input_01": v110.DEFAULT_V109_ROOT / "semantic-execution" / "turns" / "pointwise-checklist-shard-01" / "input.private.json",
        "v109_pointwise_input_02": v110.DEFAULT_V109_ROOT / "semantic-execution" / "turns" / "pointwise-checklist-shard-02" / "input.private.json",
        "v109_checklist": v110.DEFAULT_V109_ROOT / "semantic-execution" / "pointwise-checklist-full.private.json",
        "v109_truth": v110.DEFAULT_V109_ROOT / "semantic-execution" / "diagnostic-truth.private.json",
        "v110_checklist": v110.DEFAULT_OUTPUT_ROOT / "sol-pointwise-checklist-full.private.json",
        "v94_rubric": DEFAULT_V94_ROOT / "field-rubric.json",
        "v97_rubric": DEFAULT_V97_ROOT / "field-rubric.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v111_terminal"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("reference_frozen") is not False
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage", {}).get("total_tokens") != 0
        or terminal.get("predecessor_v110_usage", {}).get("total_tokens") != 199677
        or terminal.get("next_experiment_required")
        != "independent_minimal_root_field_reference_owner_diagnostic"
    ):
        raise JudgeV5CalibrationV112Error("v111 predecessor contract drifted")
    prior_pairs, prior_records = _load_prior_luna_pairs()
    units = []
    for name in (
        "v109_pointwise_input_00",
        "v109_pointwise_input_01",
        "v109_pointwise_input_02",
    ):
        units.extend(values[name].get("units") or [])
    if len(units) != 53 or len({str(row["witness_id"]) for row in units}) != 53:
        raise JudgeV5CalibrationV112Error("v109 pointwise input coverage drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "v110_evidence": v110_evidence,
        "prior_luna_pairs": prior_pairs,
        "prior_luna_records": prior_records,
        "units": units,
    }


def _task_id(role: str, case_id: str, witness_id: str, field: str) -> str:
    return "owner_" + sha256_text(f"v112|{role}|{case_id}|{witness_id}|{field}")[:24]


def _canary_id(owner_task_id: str) -> str:
    return "perm_" + sha256_text(f"v112|canary|{owner_task_id}")[:24]


def _checklist_map(value: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    return {
        str(row["witness_id"]): {
            str(item["field"]): str(item["decision"])
            for item in row["field_checklist"]
        }
        for row in value["units"]
    }


def _select_one(
    candidates: Sequence[dict[str, Any]], *, used_witnesses: set[str], salt: str
) -> dict[str, Any]:
    available = [row for row in candidates if row["witness_id"] not in used_witnesses]
    if not available:
        raise JudgeV5CalibrationV112Error(f"v112 selection slot empty: {salt}")
    selected = min(
        available,
        key=lambda row: sha256_text(
            f"v112-select|{salt}|{row['case_id']}|{row['witness_id']}|{row['field']}"
        ),
    )
    used_witnesses.add(selected["witness_id"])
    return selected


def build_v112_inputs(
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
    candidates = []
    for witness_id, unit in units.items():
        case_id = witness_to_case[witness_id]
        expected_issues = set(truth["cases"][case_id]["field_issues"][witness_id])
        for field, gpt_decision in gpt[witness_id].items():
            expected = "incorrect" if field in expected_issues else "correct"
            sol_decision = sol[witness_id][field]
            identity = (case_id, witness_id, field)
            if identity in sources["prior_luna_pairs"]:
                continue
            event = compact_empty_event_fields(deepcopy(unit["structured_event"]))
            requested = _general_requested_field_value(field, event)
            if requested["presence"] != "present":
                continue
            role = (
                "settled_control"
                if gpt_decision == sol_decision == expected
                else (
                    "consensus_reference_dispute"
                    if gpt_decision == sol_decision != expected
                    else "model_disagreement"
                )
            )
            candidates.append(
                {
                    "case_id": case_id,
                    "witness_id": witness_id,
                    "field": field,
                    "current_status": expected,
                    "gpt55_status": gpt_decision,
                    "sol_status": sol_decision,
                    "role": role,
                    "field_contract": deepcopy(contracts[field]),
                    "requested_field_value": requested,
                    "source_excerpt": unit["source_excerpt"],
                    "structured_event": event,
                }
            )
    used_witnesses: set[str] = set()
    selected = []
    for field, status in CONTROL_SLOTS:
        pool = [
            row
            for row in candidates
            if row["role"] == "settled_control"
            and row["field"] == field
            and row["current_status"] == status
        ]
        selected.append(_select_one(pool, used_witnesses=used_witnesses, salt=f"control-{field}-{status}"))
    for field in CONSENSUS_DISPUTE_FIELDS:
        pool = [
            row
            for row in candidates
            if row["role"] == "consensus_reference_dispute"
            and row["field"] == field
            and row["current_status"] == "correct"
            and row["gpt55_status"] == "incorrect"
        ]
        selected.append(_select_one(pool, used_witnesses=used_witnesses, salt=f"consensus-{field}"))
    for field in MODEL_DISAGREEMENT_FIELDS:
        pool = [
            row
            for row in candidates
            if row["role"] == "model_disagreement" and row["field"] == field
        ]
        selected.append(_select_one(pool, used_witnesses=used_witnesses, salt=f"disagreement-{field}"))
    if len(selected) != 18 or len(used_witnesses) != 18:
        raise JudgeV5CalibrationV112Error("v112 selected coverage drifted")
    selected.sort(key=lambda row: _task_id(row["role"], row["case_id"], row["witness_id"], row["field"]))
    tasks = []
    truth_rows = []
    for row in selected:
        task_id = _task_id(row["role"], row["case_id"], row["witness_id"], row["field"])
        tasks.append(
            {
                "task_id": task_id,
                "field": row["field"],
                "field_contract": row["field_contract"],
                "requested_field_value": row["requested_field_value"],
                "source_excerpt": row["source_excerpt"],
                "structured_event": row["structured_event"],
            }
        )
        truth_rows.append(
            {
                "task_id": task_id,
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "field": row["field"],
                "role": row["role"],
                "current_status": row["current_status"],
                "gpt55_status": row["gpt55_status"],
                "sol_status": row["sol_status"],
            }
        )
    by_role = {
        role: [row for row in truth_rows if row["role"] == role]
        for role in ("settled_control", "consensus_reference_dispute", "model_disagreement")
    }
    control_correct = [row for row in by_role["settled_control"] if row["current_status"] == "correct"]
    control_incorrect = [row for row in by_role["settled_control"] if row["current_status"] == "incorrect"]
    canary_owners = control_correct[:2] + control_incorrect[:2]
    canary_owners += by_role["consensus_reference_dispute"][:1]
    canary_owners += by_role["model_disagreement"][:1]
    task_by_id = {row["task_id"]: row for row in tasks}
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
        "schema_version": V112_INPUT_VERSION,
        "task_count": 18,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V112_TRUTH_VERSION,
        "task_count": 18,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V112_INPUT_VERSION,
        "task_count": 6,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V112_SELECTION_VERSION,
        "created_at": now_iso(),
        "task_count": 18,
        "distinct_witness_count": 18,
        "settled_control_count": 8,
        "settled_control_status_counts": {"correct": 4, "incorrect": 4},
        "consensus_reference_dispute_count": 6,
        "model_disagreement_count": 4,
        "permutation_canary_count": 6,
        "prior_luna_witness_field_pairs_excluded": len(sources["prior_luna_pairs"]),
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


def primary_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value.get("tasks") or []
    if len(tasks) != 18:
        raise JudgeV5CalibrationV112Error("v112 primary task count drifted")
    return [
        {
            **{key: deepcopy(child) for key, child in value.items() if key != "tasks"},
            "task_count": 6,
            "tasks": deepcopy(tasks[index : index + 6]),
            "shard_ordinal": index // 6,
            "shard_count": 3,
        }
        for index in range(0, 18, 6)
    ]


def base_instructions_v112() -> str:
    return (
        "You are the final neutral reference owner for a blinded source-to-field audit. Judge only "
        "the requested existing field under its supplied field_contract. Mentally repair every other "
        "event field first. Mark incorrect only if the requested field remains independently wrong after "
        "all other fields are corrected; a downstream consequence of another field is correct here, not "
        "a second root error. requested_field_value explicitly states the requested value and presence. "
        "Never substitute claim wording, actor, speaker, attribution, or another event field. Empty or "
        "omitted fields are not errors. Exact evidence text is insufficient unless it licenses the exact "
        "truth-conditional field value. Cite exact source substrings. Abstain only when the source genuinely "
        "cannot determine the requested field. Do not compare tasks, vote, use confidence, regex, keywords, "
        "overlap, embeddings, prior labels, model identity, or system identity."
    )


def build_prompt_v112(value: Mapping[str, Any]) -> str:
    return (
        "Return one independent field decision for every opaque task_id. The first evidence span must "
        "directly support the requested field decision and every span must be an exact substring of that "
        "task's source_excerpt. Do not emit whole-event verdicts.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def merge_outputs(outputs: Sequence[Mapping[str, Any]], expected_count: int) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(decisions) != expected_count or len({str(row["task_id"]) for row in decisions}) != expected_count:
        raise JudgeV5CalibrationV112Error("v112 output coverage drifted")
    return {"schema_version": V112_OUTPUT_VERSION, "decisions": decisions}


def score_v112(
    primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {str(row["task_id"]): row for row in truth["tasks"]}
    observed = {str(row["task_id"]): row for row in primary["decisions"]}
    repeated = {str(row["task_id"]): row for row in canary["decisions"]}
    if set(expected) != set(observed) or len(repeated) != 6:
        raise JudgeV5CalibrationV112Error("v112 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "settled_control"]
    consensus = [row for row in expected.values() if row["role"] == "consensus_reference_dispute"]
    disagreements = [row for row in expected.values() if row["role"] == "model_disagreement"]
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["current_status"]
        for row in controls
    )
    correct_controls = [row for row in controls if row["current_status"] == "correct"]
    incorrect_controls = [row for row in controls if row["current_status"] == "incorrect"]
    correct_exact = sum(observed[row["task_id"]]["field_status"] == "correct" for row in correct_controls)
    incorrect_exact = sum(observed[row["task_id"]]["field_status"] == "incorrect" for row in incorrect_controls)
    canary_exact = sum(
        repeated[row["canary_task_id"]]["field_status"]
        == observed[row["owner_task_id"]]["field_status"]
        for row in truth["canary_map"]
    )
    all_rows = list(observed.values()) + list(repeated.values())
    abstentions = sum(row["field_status"] == "abstain" for row in all_rows)
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in all_rows)
    consensus_matches = sum(
        observed[row["task_id"]]["field_status"] == row["gpt55_status"]
        for row in consensus
    )
    disagreement_resolution = Counter(
        observed[row["task_id"]]["field_status"] for row in disagreements
    )
    checks = {
        "settled_control_exact_rate": control_exact == 8,
        "correct_control_specificity": correct_exact == 4,
        "incorrect_control_sensitivity": incorrect_exact == 4,
        "permutation_canary_exact_rate": canary_exact == 6,
        "abstention_count": abstentions == 0,
        "evidence_complete_rate": evidence_complete == 24,
    }
    return {
        "schema_version": V112_SCORE_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": 18,
            "settled_control_count": 8,
            "settled_control_exact_count": control_exact,
            "correct_control_count": 4,
            "correct_control_exact_count": correct_exact,
            "incorrect_control_count": 4,
            "incorrect_control_exact_count": incorrect_exact,
            "consensus_reference_dispute_count": 6,
            "consensus_reference_dispute_owner_match_count": consensus_matches,
            "model_disagreement_count": 4,
            "model_disagreement_resolution_counts": dict(sorted(disagreement_resolution.items())),
            "permutation_canary_count": 6,
            "permutation_canary_exact_count": canary_exact,
            "abstention_count": abstentions,
            "evidence_complete_count": evidence_complete,
        },
        "contested_field_reference_owner_authorized": all(checks.values()),
        "majority_voting_used": False,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V112_CAPACITY_AUDIT_VERSION,
        "phase_id": V112_PHASE_ID,
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
        "schema_version": V112_CAPACITY_POLICY_VERSION,
        "phase_id": V112_PHASE_ID,
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


def freeze_v112(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v112 terminal")}
    sources = _validate_sources()
    value, truth, canary, selection = build_v112_inputs(sources)
    input_path = root / "minimal-root-input.private.json"
    truth_path = root / "minimal-root-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_time(selection_path, selection, "created_at")
    turn_values = primary_shards(value) + [canary]
    turns = []
    for turn_name, turn_value in zip(TURN_NAMES, turn_values, strict=True):
        prompt = build_prompt_v112(turn_value)
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
    predecessor_records = {
        **sources["records"],
        **sources["prior_luna_records"],
        "v110_terminal": sources["v110_evidence"]["records"]["terminal"],
        "v110_failure": sources["v110_evidence"]["records"]["failure"],
    }
    capacity = _build_capacity_policy(root, predecessor_records)
    runtime_dir = Path(__file__).resolve().parent
    runtime_files = [
        Path(__file__),
        runtime_dir / "app_server_judge_v5_calibration_v111_sol_reference_recovery.py",
        runtime_dir / "app_server_judge_v5_calibration_v110_sol_reference_audit.py",
        runtime_dir / "app_server_judge_v5_calibration_v99_refined_luna_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v97_residual_field_audit.py",
        runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py",
        runtime_dir / "app_server_capacity_reserve.py",
        runtime_dir / "codex_app_server.py",
    ]
    spec = {
        "schema_version": V112_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "side_free_minimal_root_field_owner_with_settled_controls_disputes_and_small_permutation_canary",
        "task_count": 18,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "selection_uses_source_text": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_8_controls_all_6_canaries_all_evidence_zero_abstentions",
        "contested_field_reference_owner_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor_records,
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
    spec_path = root / "minimal-root-diagnostic-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v112 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V112_FAILURE_VERSION,
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
        "schema_version": V112_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "diagnostic_passed": False,
        "contested_field_reference_owner_authorized": False,
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


async def run_v112(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v112 terminal")
    frozen = freeze_v112(output_dir=root, timeout_seconds=timeout_seconds)
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
                    base_instructions=base_instructions_v112(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(turn["value"]["tasks"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda value, item=turn["value"]: validate_output(value, item),
                )
                outputs.append(output)
                sidecars.append(sidecar)
        primary = merge_outputs(outputs[:3], 18)
        canary = merge_outputs(outputs[3:], 6)
        primary_path = root / "minimal-root-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(primary_path, primary)
        _write_immutable(canary_path, canary)
        score = score_v112(primary, canary, frozen["truth"])
        score_path = root / "minimal-root-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V112_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v112_minimal_root_field_diagnostic_passed_contested_reference_owner_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v112_minimal_root_field_diagnostic_passed"
                if passed
                else "v112_minimal_root_field_diagnostic_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "diagnostic_passed": passed,
            "contested_field_reference_owner_authorized": passed,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "primary_output": _record(primary_path),
            "canary_output": _record(canary_path),
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
    parser = argparse.ArgumentParser(description="Run v112 Luna minimal-root diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v112(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "diagnostic_passed": terminal.get("diagnostic_passed", False),
                "contested_field_reference_owner_authorized": terminal.get(
                    "contested_field_reference_owner_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
