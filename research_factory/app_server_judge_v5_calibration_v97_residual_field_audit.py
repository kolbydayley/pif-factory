from __future__ import annotations

"""Independent audit of the four v96 hidden field disagreements."""

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
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record_matches,
    _verify_record,
    _write_stable_created,
)
from .app_server_judge_v5_calibration_v95_reference_v6_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V95_ROOT,
)
from .app_server_judge_v5_calibration_v96_fresh_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V96_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _validate_usage,
)
from .util import now_iso, sha256_text


V97_INPUT_VERSION = "pif_app_server_judge_v5_4_v97_residual_field_input_v1"
V97_TRUTH_VERSION = "pif_app_server_judge_v5_4_v97_residual_field_truth_v1"
V97_RUBRIC_VERSION = "pif_app_server_judge_v5_4_v97_residual_field_rubric_v1"
V97_SELECTION_VERSION = "pif_app_server_judge_v5_4_v97_selection_v1"
V97_SPEC_VERSION = "pif_app_server_judge_v5_4_v97_residual_field_spec_v1"
V97_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v97_residual_field_output_v1"
V97_SCORE_VERSION = "pif_app_server_judge_v5_4_v97_residual_field_score_v1"
V97_AUDIT_VERSION = "pif_app_server_judge_v5_4_v97_projection_audit_v1"
V97_FAILURE_VERSION = "pif_app_server_judge_v5_4_v97_failure_v1"
V97_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v97_terminal_v1"
V97_PATCH_VERSION = "pif_app_server_judge_v5_4_v97_reference_patch_v1"
V97_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V97_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V97_PHASE_ID = "judge_v5_4_v97_residual_field_audit"

MODEL = "gpt-5.5"
EFFORT = "high"
DISPUTE_FIELDS = ("evidence", "metric", "reported_actor", "speaker")
PRIMARY_TASK_COUNT = 12
PRIMARY_TURNS = tuple(f"residual_field_shard_{index:02d}" for index in range(4))
CANARY_TURN = "residual_field_permutation_canary"
TURN_NAMES = PRIMARY_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V96_ROOT.parent / "judge-calibration-v5_4-v97-residual-field-audit"
).resolve()


class JudgeV5CalibrationV97Error(RuntimeError):
    """The v97 residual field audit cannot preserve its frozen contract."""


def field_rubric_v97() -> dict[str, Any]:
    return {
        "schema_version": V97_RUBRIC_VERSION,
        "field_contracts": {
            "speaker": {
                "field": "speaker",
                "definition": "The person or role voicing the event's proposition in the source.",
                "decision_rule": (
                    "Correct only when the existing speaker voices the aligned proposition. For a merged "
                    "event, one speaker assignment must validly cover the material merged propositions; a "
                    "speaker who voices only one conjunct is incorrect. Do not infer a speaker for an "
                    "unattributed narrative assertion from a speaker in an adjacent sentence."
                ),
            },
            "reported_actor": {
                "field": "reported_actor",
                "definition": (
                    "The actor whose claim is being reported by a distinct speaker; absent means the event "
                    "asserts no reported actor."
                ),
                "decision_rule": (
                    "Judge only the existing reported_actor value or its explicit absence. Do not borrow the "
                    "event's actor or speaker. Absence is correct for a direct or unattributed proposition "
                    "with no distinct reported source, and incorrect when the source clearly reports a "
                    "distinct actor's claim."
                ),
            },
            "metric": {
                "field": "metric",
                "definition": (
                    "A quantitative value, unit, comparator, direction, or raw measurement that is material "
                    "to the event's proposition."
                ),
                "decision_rule": (
                    "A missing metric is correct when the proposition asserts no material quantitative "
                    "measurement. A date, horizon, or duration used only for temporal scope is not a metric. "
                    "A present metric must match the source value, unit, comparator, and scope."
                ),
            },
            "evidence": {
                "field": "evidence",
                "definition": "The exact source span that materially supports the event's proposition.",
                "decision_rule": (
                    "Exact substring status is necessary but not sufficient. Correct evidence must entail or "
                    "materially support every material claim in the event. A span copied exactly from the "
                    "source is incorrect when it supports a different, narrower, adjacent, or contradictory "
                    "proposition."
                ),
            },
        },
        "absent_requested_field_rule": (
            "When requested_field_value.presence is absent, judge the existing value as explicitly empty or "
            "not present; never substitute another event field."
        ),
        "decision_statuses": ["correct", "incorrect", "abstain"],
        "prior_labels_available_to_model": False,
        "semantic_regex_or_keyword_rules_used": False,
        "majority_voting_used": False,
    }


def _validate_predecessors(*, v96_root: Path, v95_root: Path) -> dict[str, Any]:
    paths = {
        "v96_terminal": v96_root / "terminal.json",
        "v96_spec": v96_root / "fresh-luna-v6-spec.json",
        "v96_score": v96_root / "fresh-luna-v6-score.json",
        "v96_input": v96_root / "fresh-luna-v6-input.private.json",
        "v96_truth": v96_root / "fresh-luna-v6-truth.private.json",
        "v96_output": v96_root / "fresh-luna-v6-output.private.json",
        "v96_canary": v96_root / "permutation-canary-output.private.json",
        "v95_terminal": v95_root / "terminal.json",
        "v95_receipt": v95_root / "reference-receipt.json",
        "v95_truth": v95_root / "calibration-truth-v6.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t96 = values["v96_terminal"]
    s96 = values["v96_score"]
    sp96 = values["v96_spec"]
    t95 = values["v95_terminal"]
    if (
        t96.get("state") != "inactive"
        or t96.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or t96.get("development_terminal_reason")
        != "v96_fresh_luna_diagnostic_quality_gate_not_passed"
        or t96.get("fresh_full_development_calibration_authorized") is not False
        or t96.get("bounded_observable_repair_authorized") is not False
        or t96.get("usage_status") != "complete"
        or t96.get("accounting_complete") is not True
        or t96.get("semantic_retry_count") != 0
        or t96.get("production_mutated") is not False
        or len(t96.get("attempts") or []) != 6
        or not all(
            _verify_record(record)
            for attempt in t96.get("attempts") or []
            for record in (attempt.get("capacity"), attempt.get("sidecar"), attempt.get("output"))
        )
        or not _record_matches(t96.get("score"), paths["v96_score"])
        or not _record_matches(t96.get("output"), paths["v96_output"])
        or not _record_matches(t96.get("canary_output"), paths["v96_canary"])
        or s96.get("passed") is not False
        or s96.get("metrics", {}).get("exact_count") != 11
        or s96.get("metrics", {}).get("permutation_canary_exact_count") != 4
        or s96.get("metrics", {}).get("evidence_complete_count") != 19
        or s96.get("metrics", {}).get("primary_abstention_count") != 0
        or not _record_matches(sp96.get("frozen_inputs", {}).get("input"), paths["v96_input"])
        or not _record_matches(sp96.get("frozen_inputs", {}).get("truth"), paths["v96_truth"])
        or not all(_verify_record(row) for row in sp96.get("runtime_files") or [])
        or t95.get("state") != "completed"
        or t95.get("reference_frozen") is not True
        or t95.get("fresh_diagnostic_authorized") is not True
        or t95.get("production_mutated") is not False
        or not _record_matches(t95.get("truth"), paths["v95_truth"])
        or not _record_matches(t95.get("reference_receipt"), paths["v95_receipt"])
    ):
        raise JudgeV5CalibrationV97Error("v95/v96 residual-audit contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def _requested_field_value(field: str, event: Mapping[str, Any]) -> dict[str, Any]:
    keys = {
        "speaker": ("speaker_name", "speaker_role", "speaker_affiliation"),
        "reported_actor": (
            "reported_actor_name",
            "reported_actor_type",
            "reported_actor_affiliation",
        ),
        "metric": (
            "metric_value",
            "metric_unit",
            "metric_comparator",
            "metric_direction",
            "metric_raw_text",
        ),
        "evidence": ("evidence",),
    }[field]
    values = {key: event.get(key) for key in keys if event.get(key) not in (None, "", [], {})}
    if field == "metric" and values == {"metric_direction": "not_applicable"}:
        values = {}
    return {"presence": "present" if values else "absent", "values": values}


def _task_id(role: str, field: str, identity: str) -> str:
    return "audit_" + sha256_text(f"v97|{role}|{field}|{identity}")[:24]


def _canary_task_id(owner_task_id: str) -> str:
    return "perm_" + sha256_text(f"v97|canary|{owner_task_id}")[:24]


def _control_rows(rubric: Mapping[str, Any]) -> list[tuple[dict[str, Any], str]]:
    raw = [
        ("speaker", "Maya says the assistant saves two hours each week.", {"claim_text": "The assistant saves two hours each week.", "speaker_name": "Maya", "evidence": "Maya says the assistant saves two hours each week."}, "correct"),
        ("speaker", "The model projects rain next year. Researchers discuss uncertainty after five years.", {"claim_text": "The model projects rain next year.", "speaker_name": "Researchers", "evidence": "The model projects rain next year."}, "incorrect"),
        ("reported_actor", "The host says the bar association found the tool reliable.", {"claim_text": "The host reports that the bar association found the tool reliable.", "speaker_name": "Host", "reported_actor_name": "Bar association", "evidence": "The host says the bar association found the tool reliable."}, "correct"),
        ("reported_actor", "The detector blocks known phishing domains.", {"claim_text": "The detector blocks known phishing domains.", "speaker_name": "Analyst", "reported_actor_name": "Vendor", "evidence": "The detector blocks known phishing domains."}, "incorrect"),
        ("metric", "Caching cut median latency by 18 percent.", {"claim_text": "Caching cut median latency by 18 percent.", "metric_value": "18", "metric_unit": "percent", "metric_raw_text": "18 percent", "evidence": "Caching cut median latency by 18 percent."}, "correct"),
        ("metric", "Caching cut median latency by 18 percent.", {"claim_text": "Caching cut median latency by 30 percent.", "metric_value": "30", "metric_unit": "percent", "metric_raw_text": "30 percent", "evidence": "Caching cut median latency by 18 percent."}, "incorrect"),
        ("evidence", "The filter blocks known malware signatures. It may miss novel attacks.", {"claim_text": "The filter blocks known malware signatures.", "evidence": "The filter blocks known malware signatures."}, "correct"),
        ("evidence", "The filter blocks known malware signatures. It may miss novel attacks.", {"claim_text": "The filter blocks every malicious file.", "evidence": "The filter blocks known malware signatures."}, "incorrect"),
    ]
    rows = []
    for ordinal, (field, source, event, status) in enumerate(raw):
        task = {
            "task_id": _task_id("control", field, str(ordinal)),
            "field": field,
            "field_contract": deepcopy(rubric["field_contracts"][field]),
            "requested_field_value": _requested_field_value(field, event),
            "source_excerpt": source,
            "structured_event": deepcopy(event),
        }
        rows.append((task, status))
    return rows


def build_v97_inputs(
    *,
    v96_input: Mapping[str, Any],
    v96_truth: Mapping[str, Any],
    v96_output: Mapping[str, Any],
    rubric: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    tasks96 = {row["task_id"]: row for row in v96_input["tasks"]}
    observed = {row["task_id"]: row for row in v96_output["decisions"]}
    disputes = []
    for row in v96_truth["tasks"]:
        if observed[row["task_id"]]["field_status"] == row["expected_status"]:
            continue
        if row["field"] not in DISPUTE_FIELDS:
            raise JudgeV5CalibrationV97Error("v97 unexpected residual field")
        source_task = tasks96[row["task_id"]]
        task_id = _task_id("dispute", row["field"], row["task_id"])
        event = deepcopy(source_task["structured_event"])
        task = {
            "task_id": task_id,
            "field": row["field"],
            "field_contract": deepcopy(rubric["field_contracts"][row["field"]]),
            "requested_field_value": _requested_field_value(row["field"], event),
            "source_excerpt": source_task["source_excerpt"],
            "structured_event": event,
        }
        disputes.append(
            (
                task,
                {
                    "task_id": task_id,
                    "role": "reference_dispute",
                    "case_id": row["case_id"],
                    "witness_id": row["witness_id"],
                    "field": row["field"],
                    "prior_status": row["expected_status"],
                },
            )
        )
    if len(disputes) != 4 or Counter(task["field"] for task, _ in disputes) != Counter(DISPUTE_FIELDS):
        raise JudgeV5CalibrationV97Error("v97 dispute coverage drifted")
    tasks = [task for task, _ in disputes]
    truth_rows = [truth for _, truth in disputes]
    for task, status in _control_rows(rubric):
        tasks.append(task)
        truth_rows.append(
            {
                "task_id": task["task_id"],
                "role": "settled_control",
                "case_id": None,
                "witness_id": None,
                "field": task["field"],
                "prior_status": None,
                "control_expected_status": status,
            }
        )
    tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    task_by_id = {row["task_id"]: row for row in tasks}
    owner_ids = [row["task_id"] for row in truth_rows if row["role"] == "reference_dispute"]
    canary_tasks = []
    canary_map = []
    for owner_task_id in reversed(sorted(owner_ids)):
        task = deepcopy(task_by_id[owner_task_id])
        canary_task_id = _canary_task_id(owner_task_id)
        task["task_id"] = canary_task_id
        canary_tasks.append(task)
        canary_map.append({"canary_task_id": canary_task_id, "owner_task_id": owner_task_id})
    value = {
        "schema_version": V97_INPUT_VERSION,
        "task_count": PRIMARY_TASK_COUNT,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth = {
        "schema_version": V97_TRUTH_VERSION,
        "task_count": PRIMARY_TASK_COUNT,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V97_INPUT_VERSION,
        "task_count": 4,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V97_SELECTION_VERSION,
        "created_at": now_iso(),
        "reference_dispute_count": 4,
        "dispute_field_counts": {field: 1 for field in DISPUTE_FIELDS},
        "settled_control_count": 8,
        "settled_control_status_counts": {"correct": 4, "incorrect": 4},
        "permutation_canary_count": 4,
        "requested_empty_field_presence_explicit": True,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return value, truth, canary, selection


def primary_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value.get("tasks") or []
    if len(tasks) != PRIMARY_TASK_COUNT:
        raise JudgeV5CalibrationV97Error("v97 shard source drifted")
    return [
        {
            **{key: deepcopy(child) for key, child in value.items() if key != "tasks"},
            "task_count": 3,
            "tasks": deepcopy(tasks[index : index + 3]),
            "shard_ordinal": index // 3,
            "shard_count": 4,
        }
        for index in range(0, PRIMARY_TASK_COUNT, 3)
    ]


def merge_outputs(outputs: Sequence[Mapping[str, Any]], expected_count: int) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(decisions) != expected_count or len({row["task_id"] for row in decisions}) != expected_count:
        raise JudgeV5CalibrationV97Error("v97 output coverage drifted")
    return {"schema_version": V97_OUTPUT_VERSION, "decisions": decisions}


def base_instructions_v97() -> str:
    return (
        "You are the neutral reference owner for a blinded source-to-field audit. Judge only the "
        "requested existing field under its supplied field_contract. requested_field_value explicitly "
        "states whether that field is present or absent; never substitute actor, speaker, or another "
        "event field for an absent value. Exact evidence text is not enough unless it materially supports "
        "the event's claim. A temporal number is not automatically a metric. For merged claims, speaker "
        "must validly cover the merged propositions. The first evidence span must exactly express the "
        "aligned proposition. Abstain only when the source genuinely cannot determine the requested field. "
        "Do not use regex, keywords, overlap, embeddings, prior labels, system identity, confidence, or voting."
    )


def build_prompt_v97(value: Mapping[str, Any]) -> str:
    return (
        "Return one field decision for every opaque task_id. Do not compare tasks or emit whole-event "
        "verdicts. Every source_evidence_span must be an exact substring of that task's source_excerpt.\n\n"
        + json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    )


def score_v97(
    owner: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in owner["decisions"]}
    repeated = {row["task_id"]: row for row in canary["decisions"]}
    if set(expected) != set(observed) or len(repeated) != 4:
        raise JudgeV5CalibrationV97Error("v97 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "settled_control"]
    disputes = [row for row in expected.values() if row["role"] == "reference_dispute"]
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    canary_exact = sum(
        repeated[row["canary_task_id"]]["field_status"]
        == observed[row["owner_task_id"]]["field_status"]
        for row in truth["canary_map"]
    )
    abstentions = sum(row["field_status"] == "abstain" for row in observed.values())
    abstentions += sum(row["field_status"] == "abstain" for row in repeated.values())
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    evidence_complete += sum(bool(row["source_evidence_spans"]) for row in repeated.values())
    checks = {
        "settled_control_exact_rate": control_exact == 8,
        "permutation_canary_exact_rate": canary_exact == 4,
        "abstention_count": abstentions == 0,
        "evidence_complete_rate": evidence_complete == 16,
    }
    passed = all(checks.values())
    proposal = [
        {
            "case_id": row["case_id"],
            "witness_id": row["witness_id"],
            "field": row["field"],
            "prior_status": row["prior_status"],
            "owner_status": observed[row["task_id"]]["field_status"],
            "reference_change": observed[row["task_id"]]["field_status"] != row["prior_status"],
        }
        for row in sorted(disputes, key=lambda child: child["task_id"])
    ]
    return {
        "schema_version": V97_SCORE_VERSION,
        "passed": passed,
        "metrics": {
            "task_count": 12,
            "reference_dispute_count": 4,
            "settled_control_count": 8,
            "settled_control_exact_count": control_exact,
            "settled_control_exact_rate": round(control_exact / 8, 6),
            "permutation_canary_count": 4,
            "permutation_canary_exact_count": canary_exact,
            "permutation_canary_exact_rate": round(canary_exact / 4, 6),
            "abstention_count": abstentions,
            "evidence_complete_count": evidence_complete,
            "evidence_complete_rate": round(evidence_complete / 16, 6),
            "reference_change_count": sum(row["reference_change"] for row in proposal),
            "reference_change_field_counts": dict(
                sorted(Counter(row["field"] for row in proposal if row["reference_change"]).items())
            ),
        },
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "reference_patch_proposal": proposal if passed else [],
        "reference_patch_authorized": passed,
        "reference_freeze_authorized": False,
        "fresh_diagnostic_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "gates_frozen_before_semantic_calls": True,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V97_CAPACITY_AUDIT_VERSION,
        "phase_id": V97_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_created(audit_path, audit, "v97 capacity audit")
    policy = {
        "schema_version": V97_CAPACITY_POLICY_VERSION,
        "phase_id": V97_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v97 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v97(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v96_root: Path = DEFAULT_V96_ROOT,
    v95_root: Path = DEFAULT_V95_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors(v96_root=v96_root.resolve(), v95_root=v95_root.resolve())
    values = predecessor["values"]
    rubric = field_rubric_v97()
    rubric_path = root / "field-rubric.json"
    _write_immutable(rubric_path, rubric)
    value, truth, canary, selection = build_v97_inputs(
        v96_input=values["v96_input"],
        v96_truth=values["v96_truth"],
        v96_output=values["v96_output"],
        rubric=rubric,
    )
    input_path = root / "residual-field-input.private.json"
    truth_path = root / "residual-field-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_created(selection_path, selection, "v97 selection audit")
    turn_values = primary_shards(value) + [canary]
    turns = []
    for turn_name, turn_value in zip(TURN_NAMES, turn_values, strict=True):
        prompt = build_prompt_v97(turn_value)
        schema = output_schema(turn_value)
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=turn_value, prompt=prompt, schema=schema
        )
        turns.append(
            {"turn_name": turn_name, "value": turn_value, "prompt": prompt, "schema": schema, "paths": paths}
        )
    capacity = _build_capacity_policy(root, predecessor["records"])
    runtime_dir = Path(__file__).resolve().parent
    runtime_files = [
        Path(__file__),
        runtime_dir / "app_server_judge_v5_calibration_v96_fresh_luna_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v95_reference_v6_freeze.py",
        runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v75_exact_span_remaining_shard.py",
        runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py",
        runtime_dir / "app_server_capacity_reserve.py",
        runtime_dir / "codex_app_server.py",
    ]
    spec = {
        "schema_version": V97_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "independent_residual_reference_owner_with_explicit_empty_fields_and_entailing_evidence",
        "task_count": PRIMARY_TASK_COUNT,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_eight_controls_all_four_canaries_all_evidence_zero_abstentions",
        "reference_patch_authorized": False,
        "reference_freeze_authorized": False,
        "fresh_diagnostic_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [_record(path) for path in runtime_files],
        "frozen_inputs": {
            "rubric": _record(rubric_path),
            "input": _record(input_path),
            "truth": _record(truth_path),
            "canary": _record(canary_path),
            "selection_audit": _record(selection_path),
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
    spec_path = root / "residual-field-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v97 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV97Error("immutable v97 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {"root": root, "spec": spec, "truth": truth, "turns": turns, "capacity_policy": capacity["policy"]}


def _real_attempts(root: Path) -> list[dict[str, Any]]:
    return [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = _real_attempts(root)
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v97 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V97_FAILURE_VERSION,
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
        "schema_version": V97_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_patch_authorized": False,
        "reference_freeze_authorized": False,
        "fresh_diagnostic_authorized": False,
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


async def run_v97(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v97 terminal")
    frozen = freeze_v97(output_dir=root, timeout_seconds=timeout_seconds)
    factory = client_factory or _client_factory
    primary_outputs = []
    canary_outputs = []
    sidecars = []
    operations = []
    adoptions = {}
    current_turn = None
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
                    base_instructions=base_instructions_v97(),
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
                    raise JudgeV5CalibrationV97Error("projected v97 output is invalid")
                if current_turn == CANARY_TURN:
                    canary_outputs.append(projected)
                else:
                    primary_outputs.append(projected)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
                operations.extend([{**row, "turn_name": current_turn} for row in turn_operations])
        owner = merge_outputs(primary_outputs, PRIMARY_TASK_COUNT)
        canary = merge_outputs(canary_outputs, 4)
        owner_path = root / "residual-field-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(owner_path, owner)
        _write_immutable(canary_path, canary)
        audit = {
            "schema_version": V97_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "projection_scope": "nonexact_source_span_removal_only",
            "semantic_status_changed": False,
            "privacy": "opaque_task_ids_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v97(owner, canary, frozen["truth"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "residual-field-score.json"
        _write_immutable(score_path, score)
        proposal_path = root / "reference-patch-proposal.json"
        if score["passed"]:
            _write_immutable(
                proposal_path,
                {
                    "schema_version": V97_PATCH_VERSION,
                    "created_at": now_iso(),
                    "changes": score["reference_patch_proposal"],
                    "reference_freeze_authorized": False,
                    "fresh_diagnostic_authorized": False,
                },
            )
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V97_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v97_residual_field_audit_passed_reference_patch_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v97_residual_field_audit_passed_reference_patch_authorized"
                if passed
                else "v97_residual_field_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reference_patch_authorized": passed,
            "reference_freeze_authorized": False,
            "fresh_diagnostic_authorized": False,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(owner_path),
            "canary_output": _record(canary_path),
            "patch_proposal": _record(proposal_path) if passed else None,
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
    parser = argparse.ArgumentParser(description="Run v97 residual field audit")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v97(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_patch_authorized": terminal.get("reference_patch_authorized", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
