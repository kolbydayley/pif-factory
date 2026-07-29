from __future__ import annotations

"""Correct the unsupported-inference label semantics and test them prospectively."""

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
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record_matches,
    _verify_record,
    _write_stable_created,
)
from .app_server_judge_v5_calibration_v101_reference_v8_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V101_ROOT,
)
from .app_server_judge_v5_calibration_v102_fresh_gpt55_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V102_ROOT,
    _payload_hash,
    _prior_gpt55_sources,
    _validate_predecessors as _validate_v102_predecessors,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _validate_usage,
)
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .util import now_iso, sha256_text


V103_INPUT_VERSION = "pif_app_server_judge_v5_4_v103_unsupported_input_v1"
V103_TRUTH_VERSION = "pif_app_server_judge_v5_4_v103_unsupported_truth_v1"
V103_RUBRIC_VERSION = "pif_app_server_judge_v5_4_v103_unsupported_rubric_v1"
V103_SELECTION_VERSION = "pif_app_server_judge_v5_4_v103_selection_v1"
V103_SPEC_VERSION = "pif_app_server_judge_v5_4_v103_unsupported_spec_v1"
V103_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v103_unsupported_output_v1"
V103_SCORE_VERSION = "pif_app_server_judge_v5_4_v103_unsupported_score_v1"
V103_AUDIT_VERSION = "pif_app_server_judge_v5_4_v103_projection_audit_v1"
V103_FAILURE_VERSION = "pif_app_server_judge_v5_4_v103_failure_v1"
V103_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v103_terminal_v1"
V103_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V103_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V103_PHASE_ID = "judge_v5_4_v103_unsupported_inference_protocol"

MODEL = "gpt-5.5"
EFFORT = "high"
PRIMARY_TURNS = ("unsupported_protocol_shard_00", "unsupported_protocol_shard_01")
CANARY_TURN = "unsupported_protocol_permutation_canary"
TURN_NAMES = PRIMARY_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V102_ROOT.parent
    / "judge-calibration-v5_4-v103-unsupported-inference-protocol"
).resolve()


class JudgeV5CalibrationV103Error(RuntimeError):
    """The v103 protocol diagnostic cannot preserve its frozen contract."""


def field_rubric_v103() -> dict[str, Any]:
    return {
        "schema_version": V103_RUBRIC_VERSION,
        "field_contracts": {
            "unsupported_inference": {
                "field": "unsupported_inference",
                "definition": (
                    "A derived correctness check for whether the structured event adds a material "
                    "assertion, mechanism, scope, certainty, or conclusion not supported by the source."
                ),
                "decision_rule": (
                    "This is not a stored boolean event field. Return correct when every material part "
                    "of the event claim is supported by the source, including a merged claim whose "
                    "conjuncts and causal link are explicitly stated. Return incorrect when one or more "
                    "material event assertions are unsupported. Never invert the output because the "
                    "checklist label contains the words unsupported inference."
                ),
                "output_semantics": {
                    "correct": "the event contains no unsupported material inference",
                    "incorrect": "the event contains at least one unsupported material inference",
                    "abstain": "the source genuinely cannot determine support",
                },
            }
        },
        "requested_value_kind": "derived_support_check_not_stored_event_field",
        "prior_labels_available_to_model": False,
        "semantic_regex_or_keyword_rules_used": False,
        "majority_voting_used": False,
    }


def _validate_predecessors() -> dict[str, Any]:
    base = _validate_v102_predecessors()
    paths = {
        "v101_truth": DEFAULT_V101_ROOT / "calibration-truth-v8.private.json",
        "v102_terminal": DEFAULT_V102_ROOT / "terminal.json",
        "v102_spec": DEFAULT_V102_ROOT / "fresh-gpt55-v8-spec.json",
        "v102_input": DEFAULT_V102_ROOT / "fresh-gpt55-v8-input.private.json",
        "v102_truth": DEFAULT_V102_ROOT / "fresh-gpt55-v8-truth.private.json",
        "v102_output": DEFAULT_V102_ROOT / "fresh-gpt55-v8-output.private.json",
        "v102_canary": DEFAULT_V102_ROOT / "permutation-canary-output.private.json",
        "v102_score": DEFAULT_V102_ROOT / "fresh-gpt55-v8-score.json",
        "v23_pointwise": V23_ROOT / "pointwise-input-full.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal, score, spec = (
        values["v102_terminal"],
        values["v102_score"],
        values["v102_spec"],
    )
    mismatches = [row for row in score.get("field_results") or [] if not row.get("exact")]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("development_terminal_reason")
        != "v102_fresh_gpt55_diagnostic_quality_gate_not_passed"
        or terminal.get("fresh_full_development_calibration_authorized") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or len(terminal.get("attempts") or []) != 6
        or not all(
            _verify_record(record)
            for attempt in terminal.get("attempts") or []
            for record in (
                attempt.get("capacity"),
                attempt.get("sidecar"),
                attempt.get("output"),
            )
        )
        or not _record_matches(terminal.get("score"), paths["v102_score"])
        or not _record_matches(terminal.get("output"), paths["v102_output"])
        or not _record_matches(terminal.get("canary_output"), paths["v102_canary"])
        or spec.get("model") != MODEL
        or not _record_matches(spec.get("frozen_inputs", {}).get("input"), paths["v102_input"])
        or not _record_matches(spec.get("frozen_inputs", {}).get("truth"), paths["v102_truth"])
        or score.get("passed") is not False
        or score.get("metrics", {}).get("exact_count") != 14
        or score.get("metrics", {}).get("incorrect_sensitivity") != 1.0
        or score.get("metrics", {}).get("permutation_canary_exact_rate") != 1.0
        or score.get("metrics", {}).get("evidence_complete_rate") != 1.0
        or score.get("metrics", {}).get("observable_repair_trigger_count") != 0
        or len(mismatches) != 1
        or mismatches[0].get("field") != "unsupported_inference"
        or mismatches[0].get("expected_status") != "correct"
        or mismatches[0].get("observed_status") != "incorrect"
        or len(values["v23_pointwise"].get("units") or []) != 182
    ):
        raise JudgeV5CalibrationV103Error("v102 protocol-failure contract drifted")
    records = {**base["records"], **{name: _record(path) for name, path in paths.items()}}
    return {"paths": paths, "values": {**base["values"], **values}, "records": records}


def _requested_value(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "kind": "derived_support_check_not_stored_event_field",
        "output_semantics": {
            "correct": "no unsupported material inference",
            "incorrect": "at least one unsupported material inference",
        },
        "claim_text": event.get("claim_text"),
        "submitted_evidence": event.get("evidence"),
    }


def _task_id(role: str, case_id: str, witness_id: str) -> str:
    return "fresh_" + sha256_text(f"v103|{role}|{case_id}|{witness_id}")[:24]


def _canary_id(owner_id: str) -> str:
    return "perm_" + sha256_text(f"v103|canary|{owner_id}")[:24]


def build_v103_inputs(
    *,
    pointwise: Mapping[str, Any],
    reference: Mapping[str, Any],
    v102_input: Mapping[str, Any],
    v102_truth: Mapping[str, Any],
    prior_gpt55_inputs: Sequence[Mapping[str, Any]],
    rubric: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    contract = rubric["field_contracts"]["unsupported_inference"]
    v102_tasks = {row["task_id"]: row for row in v102_input["tasks"]}
    regression_truth = next(
        row
        for row in v102_truth["tasks"]
        if row["field"] == "unsupported_inference"
    )
    regression_source = v102_tasks[regression_truth["task_id"]]
    regression_event = compact_empty_event_fields(
        deepcopy(regression_source["structured_event"])
    )
    regression_id = _task_id(
        "regression", regression_truth["case_id"], regression_truth["witness_id"]
    )
    rows = [
        (
            {
                "task_id": regression_id,
                "field": "unsupported_inference",
                "field_contract": deepcopy(contract),
                "requested_field_value": _requested_value(regression_event),
                "source_excerpt": regression_source["source_excerpt"],
                "structured_event": regression_event,
            },
            {
                "task_id": regression_id,
                "case_id": regression_truth["case_id"],
                "witness_id": regression_truth["witness_id"],
                "field": "unsupported_inference",
                "expected_status": "correct",
                "cohort_role": "v102_protocol_regression",
            },
        )
    ]
    prior_payloads = {
        _payload_hash(task)
        for value in prior_gpt55_inputs
        for task in value.get("tasks") or []
    }
    used_cases = {regression_truth["case_id"]}
    candidates: dict[str, list[tuple[Mapping[str, Any], dict[str, Any], str]]] = {
        "correct": [],
        "incorrect": [],
    }
    for unit in pointwise.get("units") or []:
        if unit["case_id"] in used_cases:
            continue
        event = compact_empty_event_fields(deepcopy(unit["structured_event"]))
        payload_hash = _payload_hash(
            {"source_excerpt": unit["source_excerpt"], "structured_event": event}
        )
        if payload_hash in prior_payloads:
            continue
        issues = set(
            reference["cases"][unit["case_id"]]["field_issues"][unit["witness_id"]]
        )
        status = "incorrect" if "unsupported_inference" in issues else "correct"
        candidates[status].append((unit, event, payload_hash))
    for status in candidates:
        candidates[status].sort(
            key=lambda row: sha256_text(
                f"v103-select|{status}|{row[0]['case_id']}|{row[0]['witness_id']}"
            )
        )
    if len(candidates["incorrect"]) != 3 or len(candidates["correct"]) < 2:
        raise JudgeV5CalibrationV103Error("v103 fresh polarity availability drifted")
    selected = candidates["incorrect"] + candidates["correct"][:2]
    used_payloads = {_payload_hash(rows[0][0])}
    for unit, event, payload_hash in selected:
        if unit["case_id"] in used_cases or payload_hash in used_payloads:
            raise JudgeV5CalibrationV103Error("v103 fresh diversity drifted")
        task_id = _task_id("fresh", unit["case_id"], unit["witness_id"])
        issues = set(
            reference["cases"][unit["case_id"]]["field_issues"][unit["witness_id"]]
        )
        status = "incorrect" if "unsupported_inference" in issues else "correct"
        rows.append(
            (
                {
                    "task_id": task_id,
                    "field": "unsupported_inference",
                    "field_contract": deepcopy(contract),
                    "requested_field_value": _requested_value(event),
                    "source_excerpt": unit["source_excerpt"],
                    "structured_event": event,
                },
                {
                    "task_id": task_id,
                    "case_id": unit["case_id"],
                    "witness_id": unit["witness_id"],
                    "field": "unsupported_inference",
                    "expected_status": status,
                    "cohort_role": "fresh_to_gpt55",
                },
            )
        )
        used_cases.add(unit["case_id"])
        used_payloads.add(payload_hash)
    tasks = sorted((task for task, _ in rows), key=lambda row: row["task_id"])
    truth_rows = sorted((truth for _, truth in rows), key=lambda row: row["task_id"])
    if (
        len(tasks) != 6
        or len(used_cases) != 6
        or Counter(row["expected_status"] for row in truth_rows)
        != Counter({"correct": 3, "incorrect": 3})
    ):
        raise JudgeV5CalibrationV103Error("v103 task balance drifted")
    task_by_id = {row["task_id"]: row for row in tasks}
    fresh_incorrect = sorted(
        row["task_id"]
        for row in truth_rows
        if row["cohort_role"] == "fresh_to_gpt55"
        and row["expected_status"] == "incorrect"
    )
    canary_owner_ids = [regression_id, fresh_incorrect[0]]
    canary_tasks, canary_map = [], []
    for owner_id in reversed(canary_owner_ids):
        task = deepcopy(task_by_id[owner_id])
        canary_task_id = _canary_id(owner_id)
        task["task_id"] = canary_task_id
        canary_tasks.append(task)
        canary_map.append(
            {"canary_task_id": canary_task_id, "primary_task_id": owner_id}
        )
    value = {
        "schema_version": V103_INPUT_VERSION,
        "task_count": 6,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth = {
        "schema_version": V103_TRUTH_VERSION,
        "reference_version": reference["reference_version"],
        "task_count": 6,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V103_INPUT_VERSION,
        "task_count": 2,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V103_SELECTION_VERSION,
        "created_at": now_iso(),
        "protocol_regression_count": 1,
        "fresh_to_gpt55_count": 5,
        "fresh_incorrect_count": 3,
        "fresh_correct_count": 2,
        "distinct_case_count": 6,
        "permutation_canary_count": 2,
        "selection_uses_source_semantics": False,
        "selection_rule": (
            "v102_unsupported_protocol_regression_plus_all_three_exact_payload_unseen_"
            "incorrect_cases_plus_two_id_hash_ranked_unseen_correct_cases"
        ),
        "exact_payload_hash_scope": "prior_exposure_and_duplicate_identity_only",
        "semantic_pruning_performed": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "privacy": "aggregate_counts_only",
    }
    return value, truth, canary, selection


def _shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value["tasks"]
    if len(tasks) != 6:
        raise JudgeV5CalibrationV103Error("v103 task count drifted")
    return [
        {
            **{key: deepcopy(child) for key, child in value.items() if key != "tasks"},
            "task_count": 3,
            "tasks": deepcopy(tasks[index : index + 3]),
            "shard_ordinal": index // 3,
            "shard_count": 2,
        }
        for index in range(0, 6, 3)
    ]


def _merge(outputs: Sequence[Mapping[str, Any]], expected_count: int) -> dict[str, Any]:
    rows = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(rows) != expected_count or len({row["task_id"] for row in rows}) != expected_count:
        raise JudgeV5CalibrationV103Error("v103 output coverage drifted")
    return {"schema_version": V103_OUTPUT_VERSION, "decisions": rows}


def _instructions() -> str:
    return (
        "You are a neutral blinded support-check judge. unsupported_inference is a derived correctness "
        "check, not a stored true/false event field. Return correct when the event contains no unsupported "
        "material inference. Return incorrect when it contains at least one unsupported material inference. "
        "A merged claim is correct when every material conjunct and causal link are explicitly supported. "
        "Exact evidence text is necessary but not sufficient. Judge each task independently, provide an "
        "exact source span, and abstain only when source support genuinely cannot be determined. Do not use "
        "regex, keywords, embeddings, prior labels, system identity, confidence, or voting."
    )


def _prompt(value: Mapping[str, Any]) -> str:
    return (
        "Return one field decision for every opaque task_id. Apply the supplied output_semantics literally. "
        "Every source_evidence_span must be an exact substring of source_excerpt.\n\n"
        + json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    )


def score_v103(
    primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in primary["decisions"]}
    repeated = {row["task_id"]: row for row in canary["decisions"]}
    if set(expected) != set(observed) or len(repeated) != 2:
        raise JudgeV5CalibrationV103Error("v103 score coverage drifted")
    exact = sum(observed[key]["field_status"] == row["expected_status"] for key, row in expected.items())
    incorrect = [row for row in expected.values() if row["expected_status"] == "incorrect"]
    correct = [row for row in expected.values() if row["expected_status"] == "correct"]
    incorrect_exact = sum(observed[row["task_id"]]["field_status"] == "incorrect" for row in incorrect)
    correct_exact = sum(observed[row["task_id"]]["field_status"] == "correct" for row in correct)
    canary_exact = sum(
        repeated[row["canary_task_id"]]["field_status"]
        == observed[row["primary_task_id"]]["field_status"]
        for row in truth["canary_map"]
    )
    decisions = list(observed.values()) + list(repeated.values())
    evidence = sum(bool(row["source_evidence_spans"]) for row in decisions)
    abstentions = sum(row["field_status"] == "abstain" for row in decisions)
    checks = {
        "exact": exact == 6,
        "incorrect_sensitivity": incorrect_exact == 3,
        "correct_specificity": correct_exact == 3,
        "canary_exact": canary_exact == 2,
        "evidence_complete": evidence == 8,
        "zero_abstentions": abstentions == 0,
    }
    passed = all(checks.values())
    return {
        "schema_version": V103_SCORE_VERSION,
        "passed": passed,
        "metrics": {
            "task_count": 6,
            "exact_count": exact,
            "incorrect_sensitivity": round(incorrect_exact / 3, 6),
            "correct_specificity": round(correct_exact / 3, 6),
            "permutation_canary_exact_count": canary_exact,
            "evidence_complete_count": evidence,
            "abstention_count": abstentions,
        },
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "fresh_full_development_calibration_authorized": passed,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _capacity(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V103_CAPACITY_AUDIT_VERSION,
        "phase_id": V103_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_created(audit_path, audit, "v103 capacity audit")
    policy = {
        "schema_version": V103_CAPACITY_POLICY_VERSION,
        "phase_id": V103_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v103 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v103(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors()
    values = predecessor["values"]
    rubric = field_rubric_v103()
    prior_inputs = [
        values[f"{name}_gpt55_input"] for name in _prior_gpt55_sources()
    ] + [values["v102_input"]]
    value, truth, canary, selection = build_v103_inputs(
        pointwise=values["v23_pointwise"],
        reference=values["v101_truth"],
        v102_input=values["v102_input"],
        v102_truth=values["v102_truth"],
        prior_gpt55_inputs=prior_inputs,
        rubric=rubric,
    )
    files = {
        "rubric": root / "field-rubric.json",
        "input": root / "unsupported-protocol-input.private.json",
        "truth": root / "unsupported-protocol-truth.private.json",
        "canary": root / "permutation-canary-input.private.json",
        "selection_audit": root / "selection-audit.json",
    }
    _write_immutable(files["rubric"], rubric)
    _write_immutable(files["input"], value)
    _write_immutable(files["truth"], truth)
    _write_immutable(files["canary"], canary)
    _write_stable_created(files["selection_audit"], selection, "v103 selection audit")
    turn_values = _shards(value) + [canary]
    turns = []
    for turn_name, turn_value in zip(TURN_NAMES, turn_values, strict=True):
        prompt, schema = _prompt(turn_value), output_schema(turn_value)
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
    capacity = _capacity(root, predecessor["records"])
    runtime_dir = Path(__file__).resolve().parent
    runtime_files = [
        Path(__file__),
        runtime_dir / "app_server_judge_v5_calibration_v102_fresh_gpt55_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v101_reference_v8_freeze.py",
        runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v75_exact_span_remaining_shard.py",
        runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py",
        runtime_dir / "app_server_capacity_reserve.py",
        runtime_dir / "codex_app_server.py",
    ]
    spec = {
        "schema_version": V103_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": (
            "unsupported_inference_derived_check_semantics_with_one_v102_regression_"
            "and_five_exact_payload_unseen_cases"
        ),
        "task_count": 6,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": (
            "6_of_6_primary_2_of_2_canary_all_evidence_zero_abstentions_authorizes_"
            "one_fresh_full_development_calibration_only"
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
    spec_path = root / "unsupported-protocol-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v103 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV103Error("immutable v103 spec drifted")
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


def _failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = _real_attempts(root)
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v103 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V103_FAILURE_VERSION,
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
        "schema_version": V103_TERMINAL_VERSION,
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


async def run_v103(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v103 terminal")
    frozen = freeze_v103(output_dir=root, timeout_seconds=timeout_seconds)
    primary_outputs: list[dict[str, Any]] = []
    canary_outputs: list[dict[str, Any]] = []
    sidecars: list[dict[str, Any]] = []
    operations: list[dict[str, Any]] = []
    current_turn: Optional[str] = None
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=_instructions(),
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
                    raise JudgeV5CalibrationV103Error("projected v103 output is invalid")
                (canary_outputs if current_turn == CANARY_TURN else primary_outputs).append(projected)
                sidecars.append(sidecar)
                operations.extend(
                    [{**row, "turn_name": current_turn} for row in turn_operations]
                )
        primary, canary = _merge(primary_outputs, 6), _merge(canary_outputs, 2)
        primary_path = root / "unsupported-protocol-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(primary_path, primary)
        _write_immutable(canary_path, canary)
        audit = {
            "schema_version": V103_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "projection_scope": "nonexact_source_span_removal_only",
            "semantic_status_changed": False,
            "privacy": "opaque_task_ids_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v103(primary, canary, frozen["truth"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "unsupported-protocol-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V103_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v103_unsupported_protocol_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v103_unsupported_protocol_passed_full_calibration_authorized"
                if passed
                else "v103_unsupported_protocol_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "fresh_full_development_calibration_authorized": passed,
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
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v103 unsupported-inference protocol diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v103(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "fresh_full_development_calibration_authorized": terminal.get(
                    "fresh_full_development_calibration_authorized", False
                ),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
