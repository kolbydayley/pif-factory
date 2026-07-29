from __future__ import annotations

"""One capped Terra repair pass for the nine observable v115 failures."""

import argparse
import asyncio
import json
import math
from collections import Counter, defaultdict
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
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v114_final_field_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V114_ROOT,
)
from .app_server_judge_v5_calibration_v115_full_contested_field_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V115_ROOT,
    build_reference_candidate,
    base_instructions_v115,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V116_INPUT_VERSION = "pif_app_server_judge_v5_4_v116_capped_repair_input_v1"
V116_TRUTH_VERSION = "pif_app_server_judge_v5_4_v116_capped_repair_truth_v1"
V116_SELECTION_VERSION = "pif_app_server_judge_v5_4_v116_selection_v1"
V116_SPEC_VERSION = "pif_app_server_judge_v5_4_v116_spec_v1"
V116_SCORE_VERSION = "pif_app_server_judge_v5_4_v116_score_v1"
V116_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v116_reconciled_output_v1"
V116_REFERENCE_VERSION = "pif_app_server_judge_v5_4_calibration_truth_v9_pointwise_candidate_repaired"
V116_FAILURE_VERSION = "pif_app_server_judge_v5_4_v116_failure_v1"
V116_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v116_terminal_v1"
V116_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V116_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V116_PHASE_ID = "judge_v5_4_v116_capped_contested_repair"

MODEL = "gpt-5.6-terra"
EFFORT = "high"
TURN_NAME = "capped_contested_field_repair"
REPAIR_LIMIT = 12
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V115_ROOT.parent / "judge-calibration-v5_4-v116-capped-contested-repair"
).resolve()


class JudgeV5CalibrationV116Error(RuntimeError):
    """The v116 capped repair contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v115(v115_root: Path = DEFAULT_V115_ROOT) -> dict[str, Any]:
    root = v115_root.expanduser().resolve()
    paths = {
        "spec": root / "contested-field-owner-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "contested-field-owner-score.json",
        "input": root / "contested-field-input.private.json",
        "truth": root / "contested-field-truth.private.json",
        "output": root / "contested-field-output.private.json",
        "canary_input": root / "permutation-canary-input.private.json",
        "canary_output": root / "permutation-canary-output.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v115 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v115_contested_field_owner_repair_or_recovery_required"
        or terminal.get("pointwise_reference_patch_authorized") is not False
        or terminal.get("capped_repair_authorized") is not True
        or terminal.get("alignment_reference_frozen") is not False
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 299702
        or score.get("passed") is not False
        or score.get("capped_repair_authorized") is not True
        or score.get("capped_repair_limit") != REPAIR_LIMIT
        or score.get("metrics", {}).get("observable_repair_trigger_count") != 9
        or score.get("metrics", {}).get("permutation_canary_exact_count") != 9
        or score.get("metrics", {}).get("abstention_count") != 5
        or score.get("metrics", {}).get("unsupported_inference_conflict_count") != 1
        or spec.get("model") != "gpt-5.6-luna"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("capped_repair_limit") != REPAIR_LIMIT
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV116Error("v115 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV116Error("v115 runtime record drifted")
    current_reference_record = (spec.get("predecessor") or {}).get("v109_truth")
    if not isinstance(current_reference_record, Mapping) or not _verify_record(
        current_reference_record
    ):
        raise JudgeV5CalibrationV116Error("v115 full current-reference record drifted")
    current_reference_path = Path(str(current_reference_record["path"])).resolve()
    values["current_reference"] = _load_json(
        current_reference_path, "v115 full current reference"
    )
    paths["current_reference"] = current_reference_path
    usage = {field: 0 for field in USAGE_FIELDS}
    turn_records = {}
    for turn_name in spec["turn_plan"]:
        turn_root = root / "turns" / turn_name.replace("_", "-")
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
            raise JudgeV5CalibrationV116Error("v115 turn coverage is incomplete")
        measured = _validate_usage(_load_json(turn_paths["sidecar"], "v115 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        turn_records[turn_name] = {name: _record(path) for name, path in turn_paths.items()}
    if usage != terminal.get("usage"):
        raise JudgeV5CalibrationV116Error("v115 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "turn_records": turn_records,
        "usage": usage,
    }


def _validate_v114_controls() -> dict[str, Any]:
    root = DEFAULT_V114_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "input": root / "final-owner-input.private.json",
        "truth": root / "final-owner-truth.private.json",
        "output": root / "final-owner-output.private.json",
    }
    values = {name: _load_json(path, f"v114 {name}") for name, path in paths.items()}
    if (
        values["terminal"].get("state") != "completed"
        or values["terminal"].get("contested_field_reference_owner_authorized") is not True
    ):
        raise JudgeV5CalibrationV116Error("v114 control source drifted")
    return {
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def _repair_task_id(source_task_id: str) -> str:
    return "repair_" + sha256_text(f"v116|repair|{source_task_id}")[:24]


def _control_task_id(source_task_id: str) -> str:
    return "control_" + sha256_text(f"v116|control|{source_task_id}")[:24]


def _trigger_map(v115: Mapping[str, Any]) -> dict[str, set[str]]:
    truth = v115["values"]["truth"]
    primary = {
        str(row["task_id"]): row for row in v115["values"]["output"]["decisions"]
    }
    canary = {
        str(row["task_id"]): row
        for row in v115["values"]["canary_output"]["decisions"]
    }
    expected = {str(row["task_id"]): row for row in truth["tasks"]}
    triggers: dict[str, set[str]] = defaultdict(set)
    for task_id, row in primary.items():
        if row["field_status"] == "abstain":
            triggers[task_id].add("primary_abstention")
    for mapping in truth["canary_map"]:
        owner_id, canary_id = mapping["owner_task_id"], mapping["canary_task_id"]
        if canary[canary_id]["field_status"] == "abstain":
            triggers[owner_id].add("canary_abstention")
        if primary[owner_id]["field_status"] != canary[canary_id]["field_status"]:
            triggers[owner_id].add("canary_disagreement")
    for task_id, row in expected.items():
        if row["field"] != "unsupported_inference":
            continue
        wanted = {
            "supported": "correct",
            "unsupported": "incorrect",
            "abstain": "abstain",
        }[row["proposition_status"]]
        if primary[task_id]["field_status"] != wanted:
            triggers[task_id].add("unsupported_consistency")
    if len(triggers) != 9 or any(not reasons for reasons in triggers.values()):
        raise JudgeV5CalibrationV116Error("v115 unique repair trigger coverage drifted")
    return dict(triggers)


def build_v116_inputs(
    v115: Mapping[str, Any], v114: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    source = {
        str(row["task_id"]): row for row in v115["values"]["input"]["tasks"]
    }
    truth = {
        str(row["task_id"]): row for row in v115["values"]["truth"]["tasks"]
    }
    triggers = _trigger_map(v115)
    tasks = []
    truth_rows = []
    for source_id in sorted(triggers):
        target_id = _repair_task_id(source_id)
        task = deepcopy(source[source_id])
        task["task_id"] = target_id
        tasks.append(task)
        row = truth[source_id]
        truth_rows.append(
            {
                "task_id": target_id,
                "source_task_id": source_id,
                "role": "observable_repair",
                "field": row["field"],
                "proposition_status": row["proposition_status"],
                "trigger_reasons": sorted(triggers[source_id]),
            }
        )
    control_source = {
        str(row["task_id"]): row for row in v114["values"]["input"]["tasks"]
    }
    control_truth = {
        str(row["task_id"]): row for row in v114["values"]["truth"]["tasks"]
    }
    controls = [row for row in control_truth.values() if row["role"] == "matched_control"]
    if len(controls) != 4:
        raise JudgeV5CalibrationV116Error("v114 matched control coverage drifted")
    for row in controls:
        target_id = _control_task_id(row["task_id"])
        task = deepcopy(control_source[row["task_id"]])
        task["task_id"] = target_id
        tasks.append(task)
        truth_rows.append(
            {
                "task_id": target_id,
                "source_task_id": row["task_id"],
                "role": "matched_control",
                "field": row["field"],
                "control_expected_status": row["control_expected_status"],
            }
        )
    tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    if len(tasks) != 13 or len({row["task_id"] for row in tasks}) != 13:
        raise JudgeV5CalibrationV116Error("v116 task coverage drifted")
    value = {
        "schema_version": V116_INPUT_VERSION,
        "task_count": 13,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V116_TRUTH_VERSION,
        "task_count": 13,
        "tasks": truth_rows,
    }
    selection = {
        "schema_version": V116_SELECTION_VERSION,
        "created_at": now_iso(),
        "repair_task_count": 9,
        "repair_limit": REPAIR_LIMIT,
        "matched_control_count": 4,
        "trigger_reason_counts": dict(
            sorted(Counter(reason for reasons in triggers.values() for reason in reasons).items())
        ),
        "trigger_field_counts": dict(
            sorted(Counter(truth[task_id]["field"] for task_id in triggers).items())
        ),
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "capped_repair_turn_count": 1,
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return value, truth_value, selection


def build_prompt_v116(value: Mapping[str, Any]) -> str:
    return (
        "Return one final patch decision for every opaque task_id. Judge only the requested field "
        "against its exact source unit after mentally repairing every other field. Every source_evidence_span "
        "must be an exact substring of that task's source_excerpt. Do not compare tasks.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def score_v116(output: Mapping[str, Any], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {str(row["task_id"]): row for row in truth["tasks"]}
    observed = {str(row["task_id"]): row for row in output["decisions"]}
    if set(expected) != set(observed):
        raise JudgeV5CalibrationV116Error("v116 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "matched_control"]
    repairs = [row for row in expected.values() if row["role"] == "observable_repair"]
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    repair_abstentions = sum(
        observed[row["task_id"]]["field_status"] == "abstain" for row in repairs
    )
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    unsupported_conflicts = 0
    for row in repairs:
        if row["field"] != "unsupported_inference":
            continue
        wanted = {
            "supported": "correct",
            "unsupported": "incorrect",
            "abstain": "abstain",
        }[row["proposition_status"]]
        unsupported_conflicts += int(observed[row["task_id"]]["field_status"] != wanted)
    checks = {
        "matched_control_exact_rate": control_exact == 4,
        "repair_abstention_count": repair_abstentions == 0,
        "evidence_complete_rate": evidence_complete == 13,
        "unsupported_inference_consistency": unsupported_conflicts == 0,
    }
    return {
        "schema_version": V116_SCORE_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": 13,
            "repair_task_count": 9,
            "matched_control_count": 4,
            "matched_control_exact_count": control_exact,
            "repair_abstention_count": repair_abstentions,
            "evidence_complete_count": evidence_complete,
            "unsupported_inference_conflict_count": unsupported_conflicts,
        },
        "pointwise_reference_patch_authorized": all(checks.values()),
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "majority_voting_used": False,
    }


def reconcile_v115(
    *, v115: Mapping[str, Any], repair_truth: Mapping[str, Any], repair_output: Mapping[str, Any]
) -> dict[str, Any]:
    final = deepcopy(v115["values"]["output"])
    final_rows = {str(row["task_id"]): row for row in final["decisions"]}
    repair_rows = {str(row["task_id"]): row for row in repair_output["decisions"]}
    repaired = 0
    for row in repair_truth["tasks"]:
        if row["role"] != "observable_repair":
            continue
        final_rows[row["source_task_id"]]["field_status"] = repair_rows[row["task_id"]][
            "field_status"
        ]
        final_rows[row["source_task_id"]]["source_evidence_spans"] = deepcopy(
            repair_rows[row["task_id"]]["source_evidence_spans"]
        )
        final_rows[row["source_task_id"]]["rationale"] = "Capped side-free repair owner decision."
        repaired += 1
    if repaired != 9:
        raise JudgeV5CalibrationV116Error("v116 repair application coverage drifted")
    final["schema_version"] = V116_OUTPUT_VERSION
    final["repaired_task_count"] = repaired
    final["repair_basis"] = "v116_capped_side_free_terra_owner"
    return final


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V116_CAPACITY_AUDIT_VERSION,
        "phase_id": V116_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V116_CAPACITY_POLICY_VERSION,
        "phase_id": V116_PHASE_ID,
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
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        "projected_phase_quota_points": math.ceil(
            MAX_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v116(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v116 terminal")}
    v115 = _validate_v115()
    v114 = _validate_v114_controls()
    value, truth, selection = build_v116_inputs(v115, v114)
    input_path = root / "capped-repair-input.private.json"
    truth_path = root / "capped-repair-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    prompt = build_prompt_v116(value)
    schema = output_schema(value)
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=value, prompt=prompt, schema=schema
    )
    predecessor = {
        **{f"v115_{name}": record for name, record in v115["records"].items()},
        "v115_turns": v115["turn_records"],
        **{f"v114_{name}": record for name, record in v114["records"].items()},
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V116_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_capped_side_free_repair_for_only_nine_observable_v115_failures",
        "repair_task_count": 9,
        "repair_limit": REPAIR_LIMIT,
        "matched_control_count": 4,
        "turn_plan": [TURN_NAME],
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "four_exact_controls_all_repairs_nonabstain_all_evidence_zero_unsupported_conflicts",
        "pointwise_reference_patch_authorized": False,
        "alignment_reference_frozen": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v115_full_contested_field_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v114_final_field_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "truth": _record(truth_path),
            "selection": _record(selection_path),
            "turn_input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "capped-repair-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "value": value,
        "truth": truth,
        "prompt": prompt,
        "schema": schema,
        "paths": paths,
        "v115": v115,
    }


def _write_failure(root: Path, error_class: str) -> dict[str, Any]:
    attempts = [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v116 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V116_FAILURE_VERSION,
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
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V116_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "pointwise_reference_patch_authorized": False,
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


async def run_v116(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v116 terminal")
    frozen = freeze_v116(output_dir=root, timeout_seconds=timeout_seconds)
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _ = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=base_instructions_v115(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=13,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: validate_output(candidate, frozen["value"]),
            )
        output_path = root / "capped-repair-output.private.json"
        _write_immutable(output_path, output)
        score = score_v116(output, frozen["truth"])
        score_path = root / "capped-repair-score.json"
        _write_immutable(score_path, score)
        final_output_path = root / "reconciled-contested-field-output.private.json"
        candidate_path = root / "pointwise-reference-v9-candidate.private.json"
        if score["passed"]:
            final_output = reconcile_v115(
                v115=frozen["v115"],
                repair_truth=frozen["truth"],
                repair_output=output,
            )
            _write_immutable(final_output_path, final_output)
            candidate = build_reference_candidate(
                current_truth=frozen["v115"]["values"]["current_reference"],
                truth=frozen["v115"]["values"]["truth"],
                primary=final_output,
            )
            candidate["schema_version"] = V116_REFERENCE_VERSION
            candidate["reference_version"] = "fixture_reference_v9_pointwise_owner_repaired_candidate"
            candidate["repair_task_count"] = 9
            _write_immutable(candidate_path, candidate)
        accounting = _aggregate_usage([sidecar])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V116_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v116_capped_repair_passed_pointwise_patch_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v116_capped_repair_passed"
                if passed
                else "v116_capped_repair_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "pointwise_reference_patch_authorized": passed,
            "alignment_reference_frozen": False,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "repair_output": _record(output_path),
            "reconciled_output": _record(final_output_path) if passed else None,
            "reference_candidate": _record(candidate_path) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v115_usage": frozen["v115"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.error_class)
    except Exception as exc:
        return _write_failure(root, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v116 capped contested-field repair")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v116(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "pointwise_reference_patch_authorized": terminal.get(
                    "pointwise_reference_patch_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
