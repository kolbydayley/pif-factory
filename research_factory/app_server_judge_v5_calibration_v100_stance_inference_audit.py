from __future__ import annotations

"""Independent audit of v99 stance and unsupported-inference truth."""

import argparse
import asyncio
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
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
from .app_server_judge_v5_calibration_v75_exact_span_remaining_shard import project_exact_spans
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import output_schema, validate_output
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record_matches,
    _verify_record,
    _write_stable_created,
)
from .app_server_judge_v5_calibration_v98_reference_v7_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V98_ROOT,
)
from .app_server_judge_v5_calibration_v99_refined_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V99_ROOT,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _record, _validate_usage
from .util import now_iso, sha256_text


V100_INPUT_VERSION = "pif_app_server_judge_v5_4_v100_stance_inference_input_v1"
V100_TRUTH_VERSION = "pif_app_server_judge_v5_4_v100_stance_inference_truth_v1"
V100_RUBRIC_VERSION = "pif_app_server_judge_v5_4_v100_stance_inference_rubric_v1"
V100_SPEC_VERSION = "pif_app_server_judge_v5_4_v100_stance_inference_spec_v1"
V100_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v100_stance_inference_output_v1"
V100_SCORE_VERSION = "pif_app_server_judge_v5_4_v100_stance_inference_score_v1"
V100_FAILURE_VERSION = "pif_app_server_judge_v5_4_v100_failure_v1"
V100_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v100_terminal_v1"
V100_PATCH_VERSION = "pif_app_server_judge_v5_4_v100_reference_patch_v1"
V100_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V100_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V100_PHASE_ID = "judge_v5_4_v100_stance_inference_audit"
MODEL = "gpt-5.5"
EFFORT = "high"
PRIMARY_TURNS = ("stance_inference_shard_00", "stance_inference_shard_01")
CANARY_TURN = "stance_inference_permutation_canary"
TURN_NAMES = PRIMARY_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V99_ROOT.parent / "judge-calibration-v5_4-v100-stance-inference-audit"
).resolve()


class JudgeV5CalibrationV100Error(RuntimeError):
    """The v100 truth audit cannot preserve its frozen contract."""


def field_rubric_v100() -> dict[str, Any]:
    return {
        "schema_version": V100_RUBRIC_VERSION,
        "field_contracts": {
            "stance": {
                "field": "stance",
                "definition": "The evaluative position expressed by the source toward the event target.",
                "decision_rule": (
                    "Judge the source's evaluative position, not a verb's surface polarity. A system that "
                    "recommends routes is describing a capability and does not by itself express a speaker's "
                    "supportive stance toward those routes. Mark neutral correct when no evaluative approval, "
                    "opposition, warning, promotion, or uncertainty is expressed."
                ),
            },
            "unsupported_inference": {
                "field": "unsupported_inference",
                "definition": (
                    "Whether the event adds a material assertion, mechanism, scope, certainty, or conclusion "
                    "that the supplied source does not support."
                ),
                "decision_rule": (
                    "The existing field is correct when every material part of the event's claim is supported "
                    "by its exact source context, including a merged claim whose conjuncts and causal link are "
                    "explicitly stated. It is incorrect when the event adds a material claim absent from the "
                    "source. Do not treat merging two supported adjacent propositions as unsupported merely "
                    "because event_boundary may differ."
                ),
            },
        },
        "prior_labels_available_to_model": False,
        "semantic_regex_or_keyword_rules_used": False,
        "majority_voting_used": False,
    }


def _validate_predecessors(*, v99_root: Path, v98_root: Path) -> dict[str, Any]:
    paths = {
        "v99_terminal": v99_root / "terminal.json",
        "v99_spec": v99_root / "refined-luna-spec.json",
        "v99_score": v99_root / "refined-luna-score.json",
        "v99_input": v99_root / "refined-luna-input.private.json",
        "v99_truth": v99_root / "refined-luna-truth.private.json",
        "v99_output": v99_root / "refined-luna-output.private.json",
        "v99_canary": v99_root / "permutation-canary-output.private.json",
        "v98_terminal": v98_root / "terminal.json",
        "v98_receipt": v98_root / "reference-receipt.json",
        "v98_truth": v98_root / "calibration-truth-v7.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t99, s99, sp99 = values["v99_terminal"], values["v99_score"], values["v99_spec"]
    t98 = values["v98_terminal"]
    if (
        t99.get("state") != "inactive"
        or t99.get("development_terminal_reason")
        != "v99_refined_luna_diagnostic_quality_gate_not_passed"
        or t99.get("fresh_full_development_calibration_authorized") is not False
        or t99.get("bounded_observable_repair_authorized") is not True
        or t99.get("usage_status") != "complete"
        or t99.get("accounting_complete") is not True
        or t99.get("production_mutated") is not False
        or len(t99.get("attempts") or []) != 6
        or not all(
            _verify_record(record)
            for attempt in t99.get("attempts") or []
            for record in (attempt.get("capacity"), attempt.get("sidecar"), attempt.get("output"))
        )
        or not _record_matches(t99.get("score"), paths["v99_score"])
        or not _record_matches(t99.get("output"), paths["v99_output"])
        or not _record_matches(t99.get("canary_output"), paths["v99_canary"])
        or s99.get("metrics", {}).get("exact_count") != 14
        or s99.get("metrics", {}).get("observable_repair_trigger_count") != 3
        or not _record_matches(sp99.get("frozen_inputs", {}).get("input"), paths["v99_input"])
        or not _record_matches(sp99.get("frozen_inputs", {}).get("truth"), paths["v99_truth"])
        or not all(_verify_record(row) for row in sp99.get("runtime_files") or [])
        or t98.get("state") != "completed"
        or t98.get("reference_frozen") is not True
        or t98.get("production_mutated") is not False
        or not _record_matches(t98.get("truth"), paths["v98_truth"])
        or not _record_matches(t98.get("reference_receipt"), paths["v98_receipt"])
    ):
        raise JudgeV5CalibrationV100Error("v98/v99 truth-audit contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def _task_id(role: str, field: str, identity: str) -> str:
    return "audit_" + sha256_text(f"v100|{role}|{field}|{identity}")[:24]


def _canary_id(owner_id: str) -> str:
    return "perm_" + sha256_text(f"v100|canary|{owner_id}")[:24]


def _controls(rubric: Mapping[str, Any]) -> list[tuple[dict[str, Any], str]]:
    raw = [
        ("stance", "The routing agent recommends lower-fuel routes.", {"claim_text": "The routing agent recommends lower-fuel routes.", "stance": "neutral", "evidence": "The routing agent recommends lower-fuel routes."}, "correct"),
        ("stance", "The producer opposes using the model for final mastering.", {"claim_text": "The producer opposes using the model for final mastering.", "stance": "supportive", "evidence": "The producer opposes using the model for final mastering."}, "incorrect"),
        ("unsupported_inference", "Caching cut latency by 18 percent. Engineers attribute the gain to avoiding repeated reads.", {"claim_text": "Caching cut latency by 18 percent by avoiding repeated reads.", "evidence": "Caching cut latency by 18 percent. Engineers attribute the gain to avoiding repeated reads."}, "correct"),
        ("unsupported_inference", "The filter blocks known malware signatures.", {"claim_text": "The filter blocks every malicious file.", "evidence": "The filter blocks known malware signatures."}, "incorrect"),
    ]
    result = []
    for ordinal, (field, source, event, status) in enumerate(raw):
        result.append(
            (
                {
                    "task_id": _task_id("control", field, str(ordinal)),
                    "field": field,
                    "field_contract": deepcopy(rubric["field_contracts"][field]),
                    "source_excerpt": source,
                    "structured_event": event,
                },
                status,
            )
        )
    return result


def build_v100_inputs(
    *,
    v99_input: Mapping[str, Any],
    v99_truth: Mapping[str, Any],
    v99_output: Mapping[str, Any],
    v99_canary: Mapping[str, Any],
    rubric: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    tasks = {row["task_id"]: row for row in v99_input["tasks"]}
    truth = {row["task_id"]: row for row in v99_truth["tasks"]}
    observed = {row["task_id"]: row for row in v99_output["decisions"]}
    repeated = {row["task_id"]: row for row in v99_canary["decisions"]}
    canary_map = {row["primary_task_id"]: row["canary_task_id"] for row in v99_truth["canary_map"]}
    stance_ids = [task_id for task_id, row in truth.items() if row["field"] == "stance" and observed[task_id]["field_status"] != row["expected_status"]]
    unsupported_ids = [
        task_id
        for task_id, row in truth.items()
        if row["field"] == "unsupported_inference"
        and task_id in canary_map
        and observed[task_id]["field_status"] != repeated[canary_map[task_id]]["field_status"]
    ]
    source_ids = stance_ids + unsupported_ids
    if len(stance_ids) != 1 or len(unsupported_ids) != 1:
        raise JudgeV5CalibrationV100Error("v100 dispute selection drifted")
    model_tasks = []
    truth_rows = []
    for source_id in source_ids:
        row = truth[source_id]
        source_task = tasks[source_id]
        task_id = _task_id("dispute", row["field"], source_id)
        model_tasks.append(
            {
                "task_id": task_id,
                "field": row["field"],
                "field_contract": deepcopy(rubric["field_contracts"][row["field"]]),
                "source_excerpt": source_task["source_excerpt"],
                "structured_event": deepcopy(source_task["structured_event"]),
            }
        )
        truth_rows.append(
            {
                "task_id": task_id,
                "role": "reference_dispute",
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "field": row["field"],
                "prior_status": row["expected_status"],
            }
        )
    for task, expected in _controls(rubric):
        model_tasks.append(task)
        truth_rows.append(
            {
                "task_id": task["task_id"],
                "role": "settled_control",
                "case_id": None,
                "witness_id": None,
                "field": task["field"],
                "prior_status": None,
                "control_expected_status": expected,
            }
        )
    model_tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    by_id = {row["task_id"]: row for row in model_tasks}
    owner_ids = [row["task_id"] for row in truth_rows if row["role"] == "reference_dispute"]
    canary_tasks, canary_rows = [], []
    for owner_id in reversed(sorted(owner_ids)):
        task = deepcopy(by_id[owner_id])
        canary_id = _canary_id(owner_id)
        task["task_id"] = canary_id
        canary_tasks.append(task)
        canary_rows.append({"canary_task_id": canary_id, "owner_task_id": owner_id})
    value = {
        "schema_version": V100_INPUT_VERSION,
        "task_count": 6,
        "tasks": model_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V100_TRUTH_VERSION,
        "task_count": 6,
        "tasks": truth_rows,
        "canary_map": canary_rows,
    }
    canary = {
        "schema_version": V100_INPUT_VERSION,
        "task_count": 2,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    return value, truth_value, canary


def _shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value["tasks"]
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


def _merge(outputs: Sequence[Mapping[str, Any]], count: int) -> dict[str, Any]:
    rows = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(rows) != count or len({row["task_id"] for row in rows}) != count:
        raise JudgeV5CalibrationV100Error("v100 output coverage drifted")
    return {"schema_version": V100_OUTPUT_VERSION, "decisions": rows}


def _instructions() -> str:
    return (
        "You are a neutral reference owner. Judge only the requested field under its supplied contract. "
        "For stance, distinguish an agent's functional recommendation from a speaker's evaluative stance. "
        "For unsupported_inference, a merged claim is supported when every conjunct and the stated link are "
        "explicit in the source. The first evidence span must be an exact source substring. Abstain only when "
        "the source cannot determine the field. Do not use regex, keywords, embeddings, prior labels, system "
        "identity, confidence, or voting."
    )


def _prompt(value: Mapping[str, Any]) -> str:
    return (
        "Return one field decision per opaque task_id. Do not compare tasks. Every source_evidence_span must "
        "be an exact substring of source_excerpt.\n\n"
        + json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    )


def score_v100(owner: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in owner["decisions"]}
    repeated = {row["task_id"]: row for row in canary["decisions"]}
    controls = [row for row in expected.values() if row["role"] == "settled_control"]
    disputes = [row for row in expected.values() if row["role"] == "reference_dispute"]
    control_exact = sum(observed[row["task_id"]]["field_status"] == row["control_expected_status"] for row in controls)
    canary_exact = sum(repeated[row["canary_task_id"]]["field_status"] == observed[row["owner_task_id"]]["field_status"] for row in truth["canary_map"])
    abstentions = sum(row["field_status"] == "abstain" for row in list(observed.values()) + list(repeated.values()))
    evidence = sum(bool(row["source_evidence_spans"]) for row in list(observed.values()) + list(repeated.values()))
    checks = {
        "control_exact": control_exact == 4,
        "canary_exact": canary_exact == 2,
        "zero_abstentions": abstentions == 0,
        "evidence_complete": evidence == 8,
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
        for row in disputes
    ]
    return {
        "schema_version": V100_SCORE_VERSION,
        "passed": passed,
        "metrics": {
            "task_count": 6,
            "reference_dispute_count": 2,
            "settled_control_exact_count": control_exact,
            "permutation_canary_exact_count": canary_exact,
            "abstention_count": abstentions,
            "evidence_complete_count": evidence,
            "reference_change_count": sum(row["reference_change"] for row in proposal),
            "reference_change_field_counts": dict(sorted(Counter(row["field"] for row in proposal if row["reference_change"]).items())),
        },
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "reference_patch_proposal": proposal if passed else [],
        "reference_patch_authorized": passed,
        "reference_freeze_authorized": False,
        "fresh_diagnostic_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _capacity(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path, policy_path = root / "capacity-policy-audit.json", root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V100_CAPACITY_AUDIT_VERSION,
        "phase_id": V100_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {"declared_turn_count": 3, "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN, "phase_total_token_bound": bound},
    }
    _write_stable_created(audit_path, audit, "v100 capacity audit")
    policy = {
        "schema_version": V100_CAPACITY_POLICY_VERSION,
        "phase_id": V100_PHASE_ID,
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
        "projected_phase_quota_points": math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_created(policy_path, policy, "v100 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v100(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v99_root: Path = DEFAULT_V99_ROOT,
    v98_root: Path = DEFAULT_V98_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors(v99_root=v99_root.resolve(), v98_root=v98_root.resolve())
    values = predecessor["values"]
    rubric = field_rubric_v100()
    input_value, truth, canary = build_v100_inputs(
        v99_input=values["v99_input"],
        v99_truth=values["v99_truth"],
        v99_output=values["v99_output"],
        v99_canary=values["v99_canary"],
        rubric=rubric,
    )
    files = {
        "rubric": root / "field-rubric.json",
        "input": root / "stance-inference-input.private.json",
        "truth": root / "stance-inference-truth.private.json",
        "canary": root / "permutation-canary-input.private.json",
    }
    for key, value in (("rubric", rubric), ("input", input_value), ("truth", truth), ("canary", canary)):
        _write_immutable(files[key], value)
    turn_values = _shards(input_value) + [canary]
    turns = []
    for turn_name, value in zip(TURN_NAMES, turn_values, strict=True):
        prompt, schema = _prompt(value), output_schema(value)
        paths = _freeze_turn_request(root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema)
        turns.append({"turn_name": turn_name, "value": value, "prompt": prompt, "schema": schema, "paths": paths})
    capacity = _capacity(root, predecessor["records"])
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V100_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "independent_stance_and_unsupported_inference_reference_owner",
        "task_count": 6,
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "reference_patch_authorized": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [_record(path) for path in [Path(__file__), runtime_dir / "app_server_judge_v5_calibration_v99_refined_luna_diagnostic.py", runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py", runtime_dir / "app_server_judge_v5_calibration_v75_exact_span_remaining_shard.py", runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py", runtime_dir / "app_server_capacity_reserve.py", runtime_dir / "codex_app_server.py"]],
        "frozen_inputs": {
            **{key: _record(path) for key, path in files.items()},
            "turns": [{"turn_name": turn["turn_name"], "input": _record(turn["paths"]["input"]), "prompt": _record(turn["paths"]["prompt"]), "schema": _record(turn["paths"]["schema"])} for turn in turns],
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "stance-inference-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v100 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV100Error("immutable v100 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {"root": root, "spec": spec, "truth": truth, "turns": turns, "capacity_policy": capacity["policy"]}


def _real_attempts(root: Path) -> list[dict[str, Any]]:
    return [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]


def _failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts, usage, unknown = _real_attempts(root), {field: 0 for field in USAGE_FIELDS}, 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v100 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {"schema_version": V100_FAILURE_VERSION, "terminal_at": now_iso(), "classification": "infrastructure_or_judge_attempt_failed", "failed_turn_name": turn_name, "error_class": error_class, "retry_allowed_in_this_version": False, "attempts": attempts, "accounting_complete": complete, "usage_status": "complete" if complete else "unknown", "usage": usage if complete else None}
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {"schema_version": V100_TERMINAL_VERSION, "state": "failed", "terminal_reason": "infrastructure_or_judge_attempt_failed", "overall_evaluation_complete": False, "failure": _record(failure_path), "reference_patch_authorized": False, "selection_authorized": False, "holdout_authorized": False, "production_mutated": False, "semantic_retry_count": 0, "accounting_complete": complete, "usage_status": failure["usage_status"], "usage": failure["usage"]}
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v100(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v100 terminal")
    frozen = freeze_v100(output_dir=root, timeout_seconds=timeout_seconds)
    primary_outputs, canary_outputs, sidecars, current = [], [], [], None
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current = turn["turn_name"]
                output, sidecar, _adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=_instructions(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(turn["value"]["tasks"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, value=turn["value"]: validate_output(project_exact_spans(candidate, value)[0], value),
                )
                projected, _operations = project_exact_spans(output, turn["value"])
                (canary_outputs if current == CANARY_TURN else primary_outputs).append(projected)
                sidecars.append(sidecar)
        owner, canary = _merge(primary_outputs, 6), _merge(canary_outputs, 2)
        owner_path, canary_path = root / "stance-inference-output.private.json", root / "permutation-canary-output.private.json"
        _write_immutable(owner_path, owner)
        _write_immutable(canary_path, canary)
        score = score_v100(owner, canary, frozen["truth"])
        score_path = root / "stance-inference-score.json"
        _write_immutable(score_path, score)
        proposal_path = root / "reference-patch-proposal.json"
        if score["passed"]:
            _write_immutable(proposal_path, {"schema_version": V100_PATCH_VERSION, "created_at": now_iso(), "changes": score["reference_patch_proposal"], "reference_freeze_authorized": False})
        accounting = _aggregate_usage(sidecars)
        passed = score["passed"]
        terminal = {"schema_version": V100_TERMINAL_VERSION, "state": "completed" if passed else "inactive", "terminal_at": now_iso(), "terminal_reason": "v100_stance_inference_audit_passed_reference_patch_authorized" if passed else "inactive_incomplete_recovery_required", "development_terminal_reason": "v100_stance_inference_audit_passed_reference_patch_authorized" if passed else "v100_stance_inference_quality_gate_not_passed", "overall_evaluation_complete": False, "reference_patch_authorized": passed, "reference_freeze_authorized": False, "fresh_diagnostic_authorized": False, "selection_authorized": False, "holdout_authorized": False, "production_mutated": False, "semantic_attempt_started": True, "semantic_retry_count": 0, "score": _record(score_path), "output": _record(owner_path), "canary_output": _record(canary_path), "patch_proposal": _record(proposal_path) if passed else None, "attempts": _real_attempts(root), **accounting}
        _write_immutable(root / "terminal.json", terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _failure(root, current, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v100 stance/inference audit")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v100(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({"state": terminal["state"], "terminal_reason": terminal["terminal_reason"], "reference_patch_authorized": terminal.get("reference_patch_authorized", False), "usage_status": terminal["usage_status"]}, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
