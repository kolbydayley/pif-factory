from __future__ import annotations

"""Fresh GPT-5.5 diagnostic against the corrected reference v8."""

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
from .app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    project_exact_spans,
)
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    V23_ROOT,
    _field_contracts,
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v79_reference_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V79_ROOT,
)
from .app_server_judge_v5_calibration_v83_fresh_alignment_diagnostic import score_v83
from .app_server_judge_v5_calibration_v84_metric_target_reference_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V84_ROOT,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record_matches,
    _verify_record,
    _write_stable_created,
)
from .app_server_judge_v5_calibration_v87_observable_repair import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V87_ROOT,
)
from .app_server_judge_v5_calibration_v89_control_truth_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V89_ROOT,
)
from .app_server_judge_v5_calibration_v92_speaker_repair import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V92_ROOT,
)
from .app_server_judge_v5_calibration_v94_systematic_field_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V94_ROOT,
)
from .app_server_judge_v5_calibration_v97_residual_field_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V97_ROOT,
)
from .app_server_judge_v5_calibration_v99_refined_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V99_ROOT,
    _general_requested_field_value,
)
from .app_server_judge_v5_calibration_v100_stance_inference_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V100_ROOT,
)
from .app_server_judge_v5_calibration_v101_reference_v8_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V101_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _validate_usage,
)
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .util import now_iso, sha256_text


V102_INPUT_VERSION = "pif_app_server_judge_v5_4_v102_fresh_gpt55_input_v1"
V102_TRUTH_VERSION = "pif_app_server_judge_v5_4_v102_fresh_gpt55_truth_v1"
V102_RUBRIC_VERSION = "pif_app_server_judge_v5_4_v102_field_rubric_v1"
V102_SELECTION_VERSION = "pif_app_server_judge_v5_4_v102_selection_v1"
V102_SPEC_VERSION = "pif_app_server_judge_v5_4_v102_fresh_gpt55_spec_v1"
V102_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v102_fresh_gpt55_output_v1"
V102_SCORE_VERSION = "pif_app_server_judge_v5_4_v102_fresh_gpt55_score_v1"
V102_AUDIT_VERSION = "pif_app_server_judge_v5_4_v102_projection_audit_v1"
V102_FAILURE_VERSION = "pif_app_server_judge_v5_4_v102_failure_v1"
V102_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v102_terminal_v1"
V102_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V102_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V102_PHASE_ID = "judge_v5_4_v102_fresh_gpt55_diagnostic"

MODEL = "gpt-5.5"
EFFORT = "high"
V99_REGRESSION_FIELDS = (
    "causal_mechanism",
    "reported_actor",
    "stance",
    "unsupported_inference",
)
SPARSE_REGRESSION_FIELDS = ("metric",)
TARGETED_FIELDS = V99_REGRESSION_FIELDS + SPARSE_REGRESSION_FIELDS
FRESH_FIELD_POLARITY_SLOTS = (
    ("negation", "incorrect"),
    ("actor", "incorrect"),
    ("attribution", "incorrect"),
    ("event_boundary", "correct"),
    ("event_type", "incorrect"),
    ("temporal_horizon", "incorrect"),
    ("certainty", "correct"),
    ("evidence", "correct"),
    ("speaker", "incorrect"),
    ("target", "correct"),
)
PRIMARY_TURNS = tuple(f"fresh_gpt55_shard_{index:02d}" for index in range(5))
CANARY_TURN = "fresh_gpt55_permutation_canary"
TURN_NAMES = PRIMARY_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V101_ROOT.parent / "judge-calibration-v5_4-v102-fresh-gpt55-diagnostic"
).resolve()


class JudgeV5CalibrationV102Error(RuntimeError):
    """The v102 diagnostic cannot preserve its frozen evidence contract."""


def _prior_gpt55_sources() -> dict[str, tuple[Path, Path]]:
    return {
        "v79": (
            DEFAULT_V79_ROOT / "reference-owner-spec.json",
            DEFAULT_V79_ROOT / "reference-owner-input.private.json",
        ),
        "v84": (
            DEFAULT_V84_ROOT / "metric-target-spec.json",
            DEFAULT_V84_ROOT / "metric-target-input.private.json",
        ),
        "v87": (
            DEFAULT_V87_ROOT / "observable-repair-spec.json",
            DEFAULT_V87_ROOT / "observable-repair-input.private.json",
        ),
        "v89": (
            DEFAULT_V89_ROOT / "control-truth-spec.json",
            DEFAULT_V89_ROOT / "control-truth-input.private.json",
        ),
        "v92": (
            DEFAULT_V92_ROOT / "speaker-repair-spec.json",
            DEFAULT_V92_ROOT / "speaker-repair-input.private.json",
        ),
        "v94": (
            DEFAULT_V94_ROOT / "systematic-field-spec.json",
            DEFAULT_V94_ROOT / "systematic-field-input.private.json",
        ),
        "v97": (
            DEFAULT_V97_ROOT / "residual-field-spec.json",
            DEFAULT_V97_ROOT / "residual-field-input.private.json",
        ),
        "v100": (
            DEFAULT_V100_ROOT / "stance-inference-spec.json",
            DEFAULT_V100_ROOT / "stance-inference-input.private.json",
        ),
    }


def _validate_predecessors() -> dict[str, Any]:
    paths = {
        "v101_terminal": DEFAULT_V101_ROOT / "terminal.json",
        "v101_receipt": DEFAULT_V101_ROOT / "reference-receipt.json",
        "v101_truth": DEFAULT_V101_ROOT / "calibration-truth-v8.private.json",
        "v101_audit": DEFAULT_V101_ROOT / "reference-patch-audit.json",
        "v99_terminal": DEFAULT_V99_ROOT / "terminal.json",
        "v99_spec": DEFAULT_V99_ROOT / "refined-luna-spec.json",
        "v99_input": DEFAULT_V99_ROOT / "refined-luna-input.private.json",
        "v99_truth": DEFAULT_V99_ROOT / "refined-luna-truth.private.json",
        "v99_output": DEFAULT_V99_ROOT / "refined-luna-output.private.json",
        "v99_canary": DEFAULT_V99_ROOT / "permutation-canary-output.private.json",
        "v99_score": DEFAULT_V99_ROOT / "refined-luna-score.json",
        "v100_terminal": DEFAULT_V100_ROOT / "terminal.json",
        "v100_spec": DEFAULT_V100_ROOT / "stance-inference-spec.json",
        "v100_rubric": DEFAULT_V100_ROOT / "field-rubric.json",
        "v94_spec": DEFAULT_V94_ROOT / "systematic-field-spec.json",
        "v94_rubric": DEFAULT_V94_ROOT / "field-rubric.json",
        "v97_spec": DEFAULT_V97_ROOT / "residual-field-spec.json",
        "v97_rubric": DEFAULT_V97_ROOT / "field-rubric.json",
        "v23_pointwise": V23_ROOT / "pointwise-input-full.private.json",
    }
    for name, (spec_path, input_path) in _prior_gpt55_sources().items():
        paths[f"{name}_gpt55_spec"] = spec_path
        paths[f"{name}_gpt55_input"] = input_path
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t101 = values["v101_terminal"]
    t99 = values["v99_terminal"]
    t100 = values["v100_terminal"]
    if (
        t101.get("state") != "completed"
        or t101.get("reference_frozen") is not True
        or t101.get("fresh_diagnostic_authorized") is not True
        or t101.get("full_calibration_authorized") is not False
        or t101.get("selection_authorized") is not False
        or t101.get("holdout_authorized") is not False
        or t101.get("production_mutated") is not False
        or t101.get("usage", {}).get("total_tokens") != 0
        or not _record_matches(t101.get("truth"), paths["v101_truth"])
        or not _record_matches(t101.get("reference_receipt"), paths["v101_receipt"])
        or not _record_matches(t101.get("patch_audit"), paths["v101_audit"])
        or t99.get("state") != "inactive"
        or t99.get("development_terminal_reason")
        != "v99_refined_luna_diagnostic_quality_gate_not_passed"
        or t99.get("usage_status") != "complete"
        or t99.get("accounting_complete") is not True
        or t99.get("production_mutated") is not False
        or not _record_matches(t99.get("score"), paths["v99_score"])
        or not _record_matches(t99.get("output"), paths["v99_output"])
        or not _record_matches(t99.get("canary_output"), paths["v99_canary"])
        or values["v99_score"].get("passed") is not False
        or values["v99_score"].get("metrics", {}).get("task_count") != 15
        or t100.get("state") != "completed"
        or t100.get("reference_patch_authorized") is not True
        or t100.get("usage_status") != "complete"
        or t100.get("production_mutated") is not False
        or not _record_matches(
            values["v100_spec"].get("frozen_inputs", {}).get("rubric"),
            paths["v100_rubric"],
        )
        or not _record_matches(
            values["v94_spec"].get("frozen_inputs", {}).get("rubric"),
            paths["v94_rubric"],
        )
        or not _record_matches(
            values["v97_spec"].get("frozen_inputs", {}).get("rubric"),
            paths["v97_rubric"],
        )
        or len(values["v23_pointwise"].get("units") or []) != 182
    ):
        raise JudgeV5CalibrationV102Error("v99-v101 diagnostic contract drifted")
    for name in _prior_gpt55_sources():
        spec = values[f"{name}_gpt55_spec"]
        input_path = paths[f"{name}_gpt55_input"]
        if (
            spec.get("state") != "frozen_before_model_calls"
            or spec.get("model") != MODEL
            or not _record_matches(
                spec.get("frozen_inputs", {}).get("input"), input_path
            )
            or not all(_verify_record(row) for row in spec.get("runtime_files") or [])
        ):
            raise JudgeV5CalibrationV102Error(
                f"{name} prior GPT-5.5 exposure contract drifted"
            )
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def _payload_hash(task: Mapping[str, Any]) -> str:
    payload = {
        "source_excerpt": task["source_excerpt"],
        "structured_event": compact_empty_event_fields(
            deepcopy(task["structured_event"])
        ),
    }
    return sha256_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    )


def _task_id(role: str, case_id: str, witness_id: str, field: str) -> str:
    return "fresh_" + sha256_text(
        f"v102|{role}|{case_id}|{witness_id}|{field}"
    )[:24]


def _canary_task_id(primary_task_id: str) -> str:
    return "perm_" + sha256_text(f"v102|canary|{primary_task_id}")[:24]


def _combined_rubric(
    *, v94_rubric: Mapping[str, Any], v97_rubric: Mapping[str, Any], v100_rubric: Mapping[str, Any]
) -> dict[str, Any]:
    contracts = deepcopy(_field_contracts())
    contracts.update(deepcopy(v94_rubric["field_contracts"]))
    contracts.update(deepcopy(v97_rubric["field_contracts"]))
    contracts.update(deepcopy(v100_rubric["field_contracts"]))
    if len(contracts) != 15:
        raise JudgeV5CalibrationV102Error("v102 field-contract coverage drifted")
    return {
        "schema_version": V102_RUBRIC_VERSION,
        "field_contracts": contracts,
        "requested_field_presence_is_explicit": True,
        "whole_event_error_propagation": False,
        "decision_statuses": ["correct", "incorrect", "abstain"],
        "prior_labels_available_to_model": False,
        "semantic_regex_or_keyword_rules_used": False,
        "majority_voting_used": False,
    }


def build_v102_inputs(
    *,
    pointwise: Mapping[str, Any],
    reference: Mapping[str, Any],
    v99_input: Mapping[str, Any],
    v99_truth: Mapping[str, Any],
    v99_output: Mapping[str, Any],
    v99_score: Mapping[str, Any],
    prior_gpt55_inputs: Sequence[Mapping[str, Any]],
    rubric: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    contracts = rubric["field_contracts"]
    pointwise_by_identity = {
        (row["case_id"], row["witness_id"]): row
        for row in pointwise.get("units") or []
    }
    v99_tasks = {row["task_id"]: row for row in v99_input["tasks"]}
    v99_truth_by_id = {row["task_id"]: row for row in v99_truth["tasks"]}
    v99_observed = {row["task_id"]: row for row in v99_output["decisions"]}
    regression_ids = set(v99_score["observable_repair_task_ids"])
    regression_ids.update(
        task_id
        for task_id, row in v99_truth_by_id.items()
        if v99_observed[task_id]["field_status"] != row["expected_status"]
    )
    if (
        len(regression_ids) != 4
        or Counter(v99_truth_by_id[task_id]["field"] for task_id in regression_ids)
        != Counter(V99_REGRESSION_FIELDS)
    ):
        raise JudgeV5CalibrationV102Error("v102 regression coverage drifted")

    rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
    used_identities: set[tuple[str, str]] = set()
    used_cases: set[str] = set()
    used_payloads: set[str] = set()
    for source_task_id in sorted(regression_ids):
        truth_row = v99_truth_by_id[source_task_id]
        case_id, witness_id, field = (
            truth_row["case_id"],
            truth_row["witness_id"],
            truth_row["field"],
        )
        identity = (case_id, witness_id)
        source_task = v99_tasks[source_task_id]
        event = compact_empty_event_fields(deepcopy(source_task["structured_event"]))
        issues = set(reference["cases"][case_id]["field_issues"][witness_id])
        expected = "incorrect" if field in issues else "correct"
        task_id = _task_id("regression", case_id, witness_id, field)
        task = {
            "task_id": task_id,
            "field": field,
            "field_contract": deepcopy(contracts[field]),
            "requested_field_value": _general_requested_field_value(field, event),
            "source_excerpt": source_task["source_excerpt"],
            "structured_event": event,
        }
        rows.append(
            (
                task,
                {
                    "task_id": task_id,
                    "case_id": case_id,
                    "witness_id": witness_id,
                    "field": field,
                    "expected_status": expected,
                    "cohort_role": "v99_regression",
                },
            )
        )
        used_identities.add(identity)
        used_cases.add(case_id)
        used_payloads.add(_payload_hash(task))
    if Counter(row[1]["expected_status"] for row in rows) != Counter({"correct": 4}):
        raise JudgeV5CalibrationV102Error("v102 corrected regression truth drifted")

    prior_payloads = {
        _payload_hash(task)
        for value in prior_gpt55_inputs
        for task in value.get("tasks") or []
    }
    units = pointwise.get("units") or []
    for sparse_field in SPARSE_REGRESSION_FIELDS:
        sparse_candidates = []
        for unit in units:
            identity = (unit["case_id"], unit["witness_id"])
            if identity in used_identities or unit["case_id"] in used_cases:
                continue
            issues = set(
                reference["cases"][unit["case_id"]]["field_issues"][unit["witness_id"]]
            )
            if sparse_field not in issues:
                continue
            event = compact_empty_event_fields(deepcopy(unit["structured_event"]))
            payload_hash = _payload_hash(
                {"source_excerpt": unit["source_excerpt"], "structured_event": event}
            )
            if payload_hash in prior_payloads and payload_hash not in used_payloads:
                sparse_candidates.append((unit, event, payload_hash))
        sparse_candidates.sort(
            key=lambda row: sha256_text(
                f"v102-sparse|{sparse_field}|{row[0]['case_id']}|{row[0]['witness_id']}"
            )
        )
        if not sparse_candidates:
            raise JudgeV5CalibrationV102Error(
                f"v102 sparse regression is absent: {sparse_field}"
            )
        unit, event, payload_hash = sparse_candidates[0]
        case_id, witness_id = unit["case_id"], unit["witness_id"]
        task_id = _task_id("sparse_regression", case_id, witness_id, sparse_field)
        task = {
            "task_id": task_id,
            "field": sparse_field,
            "field_contract": deepcopy(contracts[sparse_field]),
            "requested_field_value": _general_requested_field_value(sparse_field, event),
            "source_excerpt": unit["source_excerpt"],
            "structured_event": event,
        }
        rows.append(
            (
                task,
                {
                    "task_id": task_id,
                    "case_id": case_id,
                    "witness_id": witness_id,
                    "field": sparse_field,
                    "expected_status": "incorrect",
                    "cohort_role": "sparse_prior_exposed_regression",
                },
            )
        )
        used_identities.add((case_id, witness_id))
        used_cases.add(case_id)
        used_payloads.add(payload_hash)

    for field, status in FRESH_FIELD_POLARITY_SLOTS:
        candidates = []
        for unit in units:
            identity = (unit["case_id"], unit["witness_id"])
            if identity in used_identities or unit["case_id"] in used_cases:
                continue
            event = compact_empty_event_fields(deepcopy(unit["structured_event"]))
            candidate = {
                "source_excerpt": unit["source_excerpt"],
                "structured_event": event,
            }
            payload_hash = _payload_hash(candidate)
            if payload_hash in prior_payloads or payload_hash in used_payloads:
                continue
            issues = set(
                reference["cases"][unit["case_id"]]["field_issues"][unit["witness_id"]]
            )
            expected = "incorrect" if field in issues else "correct"
            if expected == status:
                candidates.append((unit, event, payload_hash))
        candidates.sort(
            key=lambda row: sha256_text(
                f"v102-select|{field}|{status}|{row[0]['case_id']}|{row[0]['witness_id']}"
            )
        )
        if not candidates:
            raise JudgeV5CalibrationV102Error(
                f"v102 fresh field-polarity slot is empty: {field}/{status}"
            )
        unit, event, payload_hash = candidates[0]
        case_id, witness_id = unit["case_id"], unit["witness_id"]
        identity = (case_id, witness_id)
        task_id = _task_id("fresh", case_id, witness_id, field)
        task = {
            "task_id": task_id,
            "field": field,
            "field_contract": deepcopy(contracts[field]),
            "requested_field_value": _general_requested_field_value(field, event),
            "source_excerpt": unit["source_excerpt"],
            "structured_event": event,
        }
        rows.append(
            (
                task,
                {
                    "task_id": task_id,
                    "case_id": case_id,
                    "witness_id": witness_id,
                    "field": field,
                    "expected_status": status,
                    "cohort_role": "fresh_to_gpt55",
                },
            )
        )
        used_identities.add(identity)
        used_cases.add(case_id)
        used_payloads.add(payload_hash)

    if len(rows) != 15 or len(used_identities) != 15 or len(used_cases) != 15:
        raise JudgeV5CalibrationV102Error("v102 diagnostic diversity drifted")
    tasks = sorted((task for task, _ in rows), key=lambda row: row["task_id"])
    truth_rows = sorted((truth for _, truth in rows), key=lambda row: row["task_id"])
    field_counts = Counter(row["field"] for row in truth_rows)
    polarity_counts = Counter(row["expected_status"] for row in truth_rows)
    if field_counts != Counter(rubric["field_contracts"].keys()) or polarity_counts != Counter(
        {"incorrect": 7, "correct": 8}
    ):
        raise JudgeV5CalibrationV102Error(
            f"v102 field or polarity balance drifted: {field_counts}/{polarity_counts}"
        )
    task_by_id = {row["task_id"]: row for row in tasks}
    canary_owner_ids = sorted(
        row["task_id"]
        for row in truth_rows
        if row["cohort_role"] == "v99_regression"
    )
    canary_tasks, canary_map = [], []
    for primary_task_id in reversed(canary_owner_ids):
        task = deepcopy(task_by_id[primary_task_id])
        canary_task_id = _canary_task_id(primary_task_id)
        task["task_id"] = canary_task_id
        canary_tasks.append(task)
        canary_map.append(
            {
                "canary_task_id": canary_task_id,
                "primary_task_id": primary_task_id,
            }
        )
    value = {
        "schema_version": V102_INPUT_VERSION,
        "task_count": 15,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth = {
        "schema_version": V102_TRUTH_VERSION,
        "reference_version": reference["reference_version"],
        "task_count": 15,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V102_INPUT_VERSION,
        "task_count": 4,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V102_SELECTION_VERSION,
        "created_at": now_iso(),
        "source_witness_count": len(units),
        "targeted_regression_count": 5,
        "targeted_regression_fields": list(TARGETED_FIELDS),
        "v99_regression_count": 4,
        "sparse_prior_exposed_regression_count": 1,
        "fresh_to_gpt55_count": 10,
        "selected_distinct_witness_count": 15,
        "selected_distinct_case_count": 15,
        "expected_correct_count": 8,
        "expected_incorrect_count": 7,
        "checklist_field_coverage_count": 15,
        "permutation_canary_count": 4,
        "canary_fields": list(V99_REGRESSION_FIELDS),
        "prior_gpt55_input_artifact_count": len(prior_gpt55_inputs),
        "prior_gpt55_exact_payload_count": len(prior_payloads),
        "selection_uses_source_semantics": False,
        "selection_rule": (
            "four_v99_regressions_plus_one_disclosed_sparse_metric_regression_plus_"
            "id_hash_ranked_field_polarity_slots_"
            "excluding_exact_prior_gpt55_source_event_payloads"
        ),
        "exact_payload_hash_scope": "prior_exposure_and_duplicate_identity_only",
        "requested_field_presence_explicit": True,
        "empty_event_fields_omitted_only": True,
        "semantic_pruning_performed": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return value, truth, canary, selection


def primary_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value.get("tasks") or []
    if len(tasks) != 15:
        raise JudgeV5CalibrationV102Error("v102 task count drifted")
    return [
        {
            **{key: deepcopy(child) for key, child in value.items() if key != "tasks"},
            "task_count": 3,
            "tasks": deepcopy(tasks[index : index + 3]),
            "shard_ordinal": index // 3,
            "shard_count": 5,
        }
        for index in range(0, 15, 3)
    ]


def merge_outputs(
    outputs: Sequence[Mapping[str, Any]], expected_count: int
) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if (
        len(decisions) != expected_count
        or len({row["task_id"] for row in decisions}) != expected_count
    ):
        raise JudgeV5CalibrationV102Error("v102 output coverage drifted")
    return {"schema_version": V102_OUTPUT_VERSION, "decisions": decisions}


def base_instructions_v102() -> str:
    return (
        "You are a neutral blinded structured-field judge. Judge only the requested existing field under "
        "its supplied field_contract. requested_field_value explicitly states whether that exact field is "
        "present or absent; never substitute actor, speaker, reported_actor, or another field. A functional "
        "recommendation or capability does not by itself express supportive stance. A merged claim is not "
        "unsupported when every material conjunct and link are stated, but exact copied text is insufficient "
        "when it does not support every material event claim. A temporal number is not automatically a metric. "
        "For merged claims, speaker must validly voice all material propositions. Apply the certainty contract's "
        "neutral-medium convention. The first evidence span must exactly express the aligned proposition. "
        "Abstain only when the source genuinely cannot determine the requested field. Do not use regex, "
        "keywords, overlap, embeddings, prior labels, system identity, confidence, or voting."
    )


def build_prompt_v102(value: Mapping[str, Any]) -> str:
    return (
        "Return one field decision for every opaque task_id. Do not compare tasks or emit whole-event "
        "verdicts. Every source_evidence_span must be an exact substring of that task's source_excerpt.\n\n"
        + json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    )


def score_v102(
    primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    score = score_v83(primary, canary, truth)
    score["schema_version"] = V102_SCORE_VERSION
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in primary["decisions"]}
    score["field_results"] = [
        {
            "task_id": task_id,
            "field": expected[task_id]["field"],
            "cohort_role": expected[task_id]["cohort_role"],
            "expected_status": expected[task_id]["expected_status"],
            "observed_status": observed[task_id]["field_status"],
            "exact": observed[task_id]["field_status"]
            == expected[task_id]["expected_status"],
        }
        for task_id in sorted(expected)
    ]
    return score


def _build_capacity_policy(
    root: Path, predecessor: Mapping[str, Any]
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V102_CAPACITY_AUDIT_VERSION,
        "phase_id": V102_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_created(audit_path, audit, "v102 capacity audit")
    policy = {
        "schema_version": V102_CAPACITY_POLICY_VERSION,
        "phase_id": V102_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v102 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v102(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors()
    values = predecessor["values"]
    rubric = _combined_rubric(
        v94_rubric=values["v94_rubric"],
        v97_rubric=values["v97_rubric"],
        v100_rubric=values["v100_rubric"],
    )
    prior_inputs = [
        values[f"{name}_gpt55_input"] for name in _prior_gpt55_sources()
    ]
    value, truth, canary, selection = build_v102_inputs(
        pointwise=values["v23_pointwise"],
        reference=values["v101_truth"],
        v99_input=values["v99_input"],
        v99_truth=values["v99_truth"],
        v99_output=values["v99_output"],
        v99_score=values["v99_score"],
        prior_gpt55_inputs=prior_inputs,
        rubric=rubric,
    )
    files = {
        "rubric": root / "field-rubric.json",
        "input": root / "fresh-gpt55-v8-input.private.json",
        "truth": root / "fresh-gpt55-v8-truth.private.json",
        "canary": root / "permutation-canary-input.private.json",
        "selection_audit": root / "selection-audit.json",
    }
    _write_immutable(files["rubric"], rubric)
    _write_immutable(files["input"], value)
    _write_immutable(files["truth"], truth)
    _write_immutable(files["canary"], canary)
    _write_stable_created(files["selection_audit"], selection, "v102 selection audit")
    turn_values = primary_shards(value) + [canary]
    turns = []
    for turn_name, turn_value in zip(TURN_NAMES, turn_values, strict=True):
        prompt = build_prompt_v102(turn_value)
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
    capacity = _build_capacity_policy(root, predecessor["records"])
    runtime_dir = Path(__file__).resolve().parent
    runtime_files = [
        Path(__file__),
        runtime_dir / "app_server_judge_v5_calibration_v101_reference_v8_freeze.py",
        runtime_dir / "app_server_judge_v5_calibration_v100_stance_inference_audit.py",
        runtime_dir / "app_server_judge_v5_calibration_v99_refined_luna_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v97_residual_field_audit.py",
        runtime_dir / "app_server_judge_v5_calibration_v94_systematic_field_audit.py",
        runtime_dir / "app_server_judge_v5_calibration_v83_fresh_alignment_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v75_exact_span_remaining_shard.py",
        runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py",
        runtime_dir / "app_server_capacity_reserve.py",
        runtime_dir / "codex_app_server.py",
    ]
    spec = {
        "schema_version": V102_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": (
            "fresh_gpt55_field_complete_diagnostic_against_reference_v8_with_"
            "v99_regressions_and_exact_prior_payload_exclusion"
        ),
        "task_count": 15,
        "distinct_witness_count": 15,
        "distinct_case_count": 15,
        "checklist_field_coverage_count": 15,
        "permutation_canary_count": 4,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": (
            "15_of_15_sensitivity_specificity_exact_evidence_all_canaries_zero_abstentions"
        ),
        "fresh_full_development_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [_record(path) for path in runtime_files],
        "frozen_inputs": {
            **{key: _record(path) for key, path in files.items()},
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
    spec_path = root / "fresh-gpt55-v8-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v102 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV102Error("immutable v102 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "truth": truth,
        "turns": turns,
        "capacity_policy": capacity["policy"],
    }


def _real_attempts(root: Path) -> list[dict[str, Any]]:
    return [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]


def _write_failure(
    root: Path, turn_name: Optional[str], error_class: str
) -> dict[str, Any]:
    attempts = _real_attempts(root)
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(
                _load_json(Path(record["path"]), "v102 sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V102_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "attempts": attempts,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V102_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "fresh_full_development_calibration_authorized": False,
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


async def run_v102(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v102 terminal")
    frozen = freeze_v102(output_dir=root, timeout_seconds=timeout_seconds)
    factory = client_factory or _client_factory
    primary_outputs: list[dict[str, Any]] = []
    canary_outputs: list[dict[str, Any]] = []
    sidecars: list[dict[str, Any]] = []
    operations: list[dict[str, Any]] = []
    adoptions: dict[str, bool] = {}
    current_turn: Optional[str] = None
    try:
        async with factory(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=base_instructions_v102(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(turn["value"]["tasks"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, value=turn["value"]: validate_output(
                        project_exact_spans(candidate, value)[0], value
                    ),
                )
                projected, turn_operations = project_exact_spans(output, turn["value"])
                if validate_output(projected, turn["value"]):
                    raise JudgeV5CalibrationV102Error(
                        "projected v102 output is invalid"
                    )
                if current_turn == CANARY_TURN:
                    canary_outputs.append(projected)
                else:
                    primary_outputs.append(projected)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
                operations.extend(
                    [{**row, "turn_name": current_turn} for row in turn_operations]
                )
        primary = merge_outputs(primary_outputs, 15)
        canary = merge_outputs(canary_outputs, 4)
        primary_path = root / "fresh-gpt55-v8-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(primary_path, primary)
        _write_immutable(canary_path, canary)
        audit = {
            "schema_version": V102_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "projection_scope": "nonexact_source_span_removal_only",
            "semantic_status_changed": False,
            "privacy": "opaque_task_ids_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v102(primary, canary, frozen["truth"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "fresh-gpt55-v8-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V102_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v102_fresh_gpt55_diagnostic_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v102_fresh_gpt55_diagnostic_passed_full_calibration_authorized"
                if passed
                else "v102_fresh_gpt55_diagnostic_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "fresh_full_development_calibration_authorized": passed,
            "bounded_observable_repair_authorized": score[
                "bounded_observable_repair_authorized"
            ],
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(primary_path),
            "canary_output": _record(canary_path),
            "projection_audit": _record(audit_path),
            "attempts": _real_attempts(root),
            "completed_checkpoint_adoptions": adoptions,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v102 fresh GPT-5.5 diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v102(
            output_dir=Path(args.output_dir),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "fresh_full_development_calibration_authorized": terminal.get(
                    "fresh_full_development_calibration_authorized", False
                ),
                "bounded_observable_repair_authorized": terminal.get(
                    "bounded_observable_repair_authorized", False
                ),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
