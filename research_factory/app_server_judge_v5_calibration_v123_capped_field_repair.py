from __future__ import annotations

"""One capped independent repair for the five observable v121 field-owner triggers."""

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
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v115_full_contested_field_owner import (
    base_instructions_v115,
)
from .app_server_judge_v5_calibration_v120_retained_case_diagnostic import (
    _validate_v119,
)
from .app_server_judge_v5_calibration_v121_retained_field_owner import (
    V121_REFERENCE_VERSION,
    build_reference_candidate,
    score_v121,
)
from .app_server_judge_v5_calibration_v122_field_owner_recovery import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V122_ROOT,
    _validate_v121,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V123_INPUT_VERSION = "pif_app_server_judge_v5_4_v123_capped_field_repair_input_v1"
V123_TRUTH_VERSION = "pif_app_server_judge_v5_4_v123_capped_field_repair_truth_v1"
V123_SELECTION_VERSION = "pif_app_server_judge_v5_4_v123_selection_v1"
V123_SPEC_VERSION = "pif_app_server_judge_v5_4_v123_spec_v1"
V123_SCORE_VERSION = "pif_app_server_judge_v5_4_v123_score_v1"
V123_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v123_reconciled_output_v1"
V123_REFERENCE_VERSION = (
    "pif_app_server_judge_v5_4_calibration_truth_v10_retained_field_candidate_repaired"
)
V123_FAILURE_VERSION = "pif_app_server_judge_v5_4_v123_failure_v1"
V123_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v123_terminal_v1"
V123_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V123_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V123_PHASE_ID = "judge_v5_4_v123_capped_retained_field_repair"

MODEL = "gpt-5.4"
EFFORT = "high"
TURN_NAME = "capped_retained_field_repair"
REPAIR_COUNT = 5
CONTROL_COUNT = 4
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V122_ROOT.parent / "judge-calibration-v5_4-v123-capped-field-repair"
).resolve()


class JudgeV5CalibrationV123Error(RuntimeError):
    """The v123 capped repair contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v122() -> dict[str, Any]:
    root = DEFAULT_V122_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "score": root / "retained-field-owner-score.json",
        "primary": root / "retained-field-output.private.json",
        "canary": root / "permutation-canary-output.private.json",
        "audit": root / "v121-recovery-audit.json",
        "taxonomy": root / "sanitized-recovery-taxonomy.json",
    }
    values = {name: _load_json(path, f"v122 {name}") for name, path in paths.items()}
    terminal, score = values["terminal"], values["score"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v122_recovered_v121_quality_result_capped_repair_required"
        or terminal.get("capped_repair_authorized") is not True
        or terminal.get("retained_field_reference_patch_authorized") is not False
        or terminal.get("fresh_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_attempt_started") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != {field: 0 for field in USAGE_FIELDS}
        or score.get("passed") is not False
        or score.get("capped_repair_authorized") is not True
        or score.get("metrics", {}).get("observable_repair_trigger_count") != REPAIR_COUNT
        or len(score.get("observable_repair_triggers") or []) != REPAIR_COUNT
    ):
        raise JudgeV5CalibrationV123Error("v122 recovery contract drifted")
    terminal_records = {
        "score": "score",
        "primary": "recovered_primary",
        "canary": "recovered_canary",
        "audit": "recovery_audit",
        "taxonomy": "sanitized_taxonomy",
    }
    for name, terminal_key in terminal_records.items():
        record = terminal.get(terminal_key)
        if (
            not isinstance(record, Mapping)
            or dict(record) != _record(paths[name])
            or not _verify_record(record)
        ):
            raise JudgeV5CalibrationV123Error(f"v122 {name} record drifted")

    v121 = _validate_v121()
    input_record = v121["values"]["spec"]["frozen_inputs"]["input"]
    if not isinstance(input_record, Mapping) or not _verify_record(input_record):
        raise JudgeV5CalibrationV123Error("v121 input record drifted")
    v121_input_path = Path(str(input_record["path"])).resolve()
    v121_input = _load_json(v121_input_path, "v121 retained field input")
    recomputed = score_v121(values["primary"], values["canary"], v121["values"]["truth"])
    if recomputed != score or terminal.get("predecessor_v121_usage") != v121["usage"]:
        raise JudgeV5CalibrationV123Error("v122 recovered score or usage drifted")
    if values["audit"].get("new_semantic_turn_count") != 0:
        raise JudgeV5CalibrationV123Error("v122 recovery unexpectedly used a semantic turn")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "v121": v121,
        "v121_input": v121_input,
        "v121_input_record": dict(input_record),
    }


def _repair_task_id(source_task_id: str) -> str:
    return "repair_" + sha256_text(f"v123|repair|{source_task_id}")[:24]


def _control_task_id(source_task_id: str) -> str:
    return "control_" + sha256_text(f"v123|control|{source_task_id}")[:24]


def _required_gate_status(
    *, source_task_id: str, reasons: set[str], truth_row: Mapping[str, Any],
    primary: Mapping[str, Mapping[str, Any]], canary: Mapping[str, Mapping[str, Any]],
    canary_map: Mapping[str, str],
) -> str:
    required = set()
    if "canary_disagreement" in reasons:
        required.add(str(canary[canary_map[source_task_id]]["field_status"]))
    if "matched_control_mismatch" in reasons:
        required.add(str(truth_row["control_expected_status"]))
    if "unsupported_consistency" in reasons:
        required.add(
            {
                "supported": "correct",
                "unsupported": "incorrect",
                "abstain": "abstain",
            }[str(truth_row["proposition_status"])]
        )
    if not required or len(required) != 1:
        raise JudgeV5CalibrationV123Error("v123 repair gate target is ambiguous")
    required_status = next(iter(required))
    if primary[source_task_id]["field_status"] == required_status:
        raise JudgeV5CalibrationV123Error("v123 trigger no longer requires repair")
    return required_status


def build_v123_inputs(
    v122: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    input_tasks = {
        str(row["task_id"]): row for row in v122["v121_input"]["tasks"]
    }
    truth_rows = {
        str(row["task_id"]): row for row in v122["v121"]["values"]["truth"]["tasks"]
    }
    primary = {
        str(row["task_id"]): row for row in v122["values"]["primary"]["decisions"]
    }
    canary = {
        str(row["task_id"]): row for row in v122["values"]["canary"]["decisions"]
    }
    canary_map = {
        str(row["owner_task_id"]): str(row["canary_task_id"])
        for row in v122["v121"]["values"]["truth"]["canary_map"]
    }
    triggers = {
        str(row["task_id"]): set(row["reasons"])
        for row in v122["values"]["score"]["observable_repair_triggers"]
    }
    if len(triggers) != REPAIR_COUNT or not all(reasons for reasons in triggers.values()):
        raise JudgeV5CalibrationV123Error("v123 repair trigger coverage drifted")

    tasks: list[dict[str, Any]] = []
    repair_truth: list[dict[str, Any]] = []
    for source_id in sorted(triggers):
        source = deepcopy(input_tasks[source_id])
        task_id = _repair_task_id(source_id)
        source["task_id"] = task_id
        tasks.append(source)
        row = truth_rows[source_id]
        repair_truth.append(
            {
                "task_id": task_id,
                "source_task_id": source_id,
                "role": "observable_repair",
                "field": row["field"],
                "trigger_reasons": sorted(triggers[source_id]),
                "required_gate_status": _required_gate_status(
                    source_task_id=source_id,
                    reasons=triggers[source_id],
                    truth_row=row,
                    primary=primary,
                    canary=canary,
                    canary_map=canary_map,
                ),
                "proposition_status": row["proposition_status"],
            }
        )

    eligible_controls = [
        row
        for row in truth_rows.values()
        if row["role"] == "matched_control"
        and row["task_id"] not in triggers
        and primary[row["task_id"]]["field_status"] == row["control_expected_status"]
        and primary[row["task_id"]]["field_status"] != "abstain"
    ]
    eligible_controls.sort(
        key=lambda row: sha256_text(f"v123|control-rank|{row['field']}|{row['task_id']}")
    )
    controls: list[Mapping[str, Any]] = []
    used_fields = set()
    for row in eligible_controls:
        if row["field"] in used_fields:
            continue
        controls.append(row)
        used_fields.add(row["field"])
        if len(controls) == CONTROL_COUNT:
            break
    if len(controls) != CONTROL_COUNT:
        raise JudgeV5CalibrationV123Error("v123 matched control coverage drifted")

    control_truth = []
    for row in controls:
        source_id = str(row["task_id"])
        source = deepcopy(input_tasks[source_id])
        task_id = _control_task_id(source_id)
        source["task_id"] = task_id
        tasks.append(source)
        control_truth.append(
            {
                "task_id": task_id,
                "source_task_id": source_id,
                "role": "matched_control",
                "field": row["field"],
                "control_expected_status": row["control_expected_status"],
            }
        )

    tasks.sort(key=lambda row: sha256_text(f"v123|model-order|{row['task_id']}"))
    all_truth = sorted(repair_truth + control_truth, key=lambda row: row["task_id"])
    if len(tasks) != REPAIR_COUNT + CONTROL_COUNT or len({row["task_id"] for row in tasks}) != 9:
        raise JudgeV5CalibrationV123Error("v123 task coverage drifted")
    value = {
        "schema_version": V123_INPUT_VERSION,
        "task_count": 9,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth = {
        "schema_version": V123_TRUTH_VERSION,
        "task_count": 9,
        "repair_task_count": REPAIR_COUNT,
        "matched_control_count": CONTROL_COUNT,
        "tasks": all_truth,
    }
    selection = {
        "schema_version": V123_SELECTION_VERSION,
        "created_at": now_iso(),
        "repair_task_count": REPAIR_COUNT,
        "repair_limit": 12,
        "matched_control_count": CONTROL_COUNT,
        "trigger_reason_counts": dict(
            sorted(Counter(reason for reasons in triggers.values() for reason in reasons).items())
        ),
        "trigger_field_counts": dict(
            sorted(Counter(truth_rows[task_id]["field"] for task_id in triggers).items())
        ),
        "control_field_count": len({row["field"] for row in controls}),
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "trigger_reasons_in_model_input": False,
        "majority_voting_used": False,
        "capped_repair_turn_count": 1,
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return value, truth, selection


def build_prompt_v123(value: Mapping[str, Any]) -> str:
    return (
        "Return one independent final field decision for every opaque task_id. Judge only the requested "
        "field against its exact source unit after mentally repairing every other field. The first "
        "source_evidence_span must directly support the decision and every span must be an exact substring "
        "of that task's source_excerpt. Do not compare tasks or emit whole-event verdicts.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def reconcile_v122(
    *, primary: Mapping[str, Any], truth: Mapping[str, Any], repair_output: Mapping[str, Any]
) -> dict[str, Any]:
    final = deepcopy(primary)
    final_rows = {str(row["task_id"]): row for row in final["decisions"]}
    repairs = {str(row["task_id"]): row for row in repair_output["decisions"]}
    repaired = 0
    for row in truth["tasks"]:
        if row["role"] != "observable_repair":
            continue
        source_id = str(row["source_task_id"])
        patch = repairs[str(row["task_id"])]
        final_rows[source_id]["field_status"] = patch["field_status"]
        final_rows[source_id]["source_evidence_spans"] = deepcopy(
            patch["source_evidence_spans"]
        )
        final_rows[source_id]["rationale"] = "Capped side-free independent repair decision."
        repaired += 1
    if repaired != REPAIR_COUNT:
        raise JudgeV5CalibrationV123Error("v123 repair application coverage drifted")
    final["schema_version"] = V123_OUTPUT_VERSION
    final["repaired_task_count"] = repaired
    final["repair_basis"] = "v123_capped_side_free_gpt54_owner"
    return final


def score_v123(
    output: Mapping[str, Any], truth: Mapping[str, Any], full_score: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {str(row["task_id"]): row for row in truth["tasks"]}
    observed = {str(row["task_id"]): row for row in output["decisions"]}
    if set(expected) != set(observed):
        raise JudgeV5CalibrationV123Error("v123 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "matched_control"]
    repairs = [row for row in expected.values() if row["role"] == "observable_repair"]
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    repair_gate_exact = sum(
        observed[row["task_id"]]["field_status"] == row["required_gate_status"]
        for row in repairs
    )
    repair_abstentions = sum(
        observed[row["task_id"]]["field_status"] == "abstain" for row in repairs
    )
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    unsupported_conflicts = sum(
        observed[row["task_id"]]["field_status"] != row["required_gate_status"]
        for row in repairs
        if row["field"] == "unsupported_inference"
    )
    checks = {
        "matched_control_exact_rate": control_exact == CONTROL_COUNT,
        "repair_gate_exact_rate": repair_gate_exact == REPAIR_COUNT,
        "repair_abstention_count": repair_abstentions == 0,
        "evidence_complete_rate": evidence_complete == REPAIR_COUNT + CONTROL_COUNT,
        "unsupported_inference_consistency": unsupported_conflicts == 0,
        "full_v121_gate_recomputed": full_score.get("passed") is True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V123_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": REPAIR_COUNT + CONTROL_COUNT,
            "repair_task_count": REPAIR_COUNT,
            "matched_control_count": CONTROL_COUNT,
            "matched_control_exact_count": control_exact,
            "repair_gate_exact_count": repair_gate_exact,
            "repair_abstention_count": repair_abstentions,
            "evidence_complete_count": evidence_complete,
            "unsupported_inference_conflict_count": unsupported_conflicts,
            "full_v121_matched_control_exact_count": full_score.get("metrics", {}).get(
                "matched_control_exact_count"
            ),
            "full_v121_permutation_canary_exact_count": full_score.get("metrics", {}).get(
                "permutation_canary_exact_count"
            ),
            "full_v121_unsupported_inference_conflict_count": full_score.get(
                "metrics", {}
            ).get("unsupported_inference_conflict_count"),
        },
        "retained_field_reference_patch_authorized": passed,
        "proposition_reference_frozen": False,
        "alignment_reference_frozen": False,
        "fresh_diagnostic_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "majority_voting_used": False,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V123_CAPACITY_AUDIT_VERSION,
        "phase_id": V123_PHASE_ID,
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
        "schema_version": V123_CAPACITY_POLICY_VERSION,
        "phase_id": V123_PHASE_ID,
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


def freeze_v123(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v123 terminal")}
    v122 = _validate_v122()
    v119 = _validate_v119()
    value, truth, selection = build_v123_inputs(v122)
    input_path = root / "capped-repair-input.private.json"
    truth_path = root / "capped-repair-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    prompt = build_prompt_v123(value)
    schema = output_schema(value)
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=value, prompt=prompt, schema=schema
    )
    predecessor = {
        **{f"v122_{name}": record for name, record in v122["records"].items()},
        **{f"v121_{name}": record for name, record in v122["v121"]["records"].items()},
        "v121_input": v122["v121_input_record"],
        "v121_attempts": v122["v121"]["attempts"],
        **{f"v119_{name}": record for name, record in v119["records"].items()},
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V123_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_capped_side_free_gpt54_repair_for_five_observable_v121_triggers",
        "repair_task_count": REPAIR_COUNT,
        "repair_limit": 12,
        "matched_control_count": CONTROL_COUNT,
        "turn_plan": [TURN_NAME],
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "trigger_reasons_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "four_exact_controls_five_exact_repairs_complete_evidence_and_full_v121_gate_recomputed",
        "retained_field_reference_patch_authorized": False,
        "proposition_reference_frozen": False,
        "alignment_reference_frozen": False,
        "fresh_diagnostic_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v122_field_owner_recovery.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v121_retained_field_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v120_retained_case_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v115_full_contested_field_owner.py"),
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
        "v122": v122,
        "v119": v119,
    }


def _write_failure(root: Path, error_class: str) -> dict[str, Any]:
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v123 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V123_FAILURE_VERSION,
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
        "schema_version": V123_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "retained_field_reference_patch_authorized": False,
        "proposition_reference_frozen": False,
        "alignment_reference_frozen": False,
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


async def run_v123(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v123 terminal")
    frozen = freeze_v123(output_dir=root, timeout_seconds=timeout_seconds)
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
                batch_size=REPAIR_COUNT + CONTROL_COUNT,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: validate_output(candidate, frozen["value"]),
            )
        output_path = root / "capped-repair-output.private.json"
        _write_immutable(output_path, output)
        reconciled = reconcile_v122(
            primary=frozen["v122"]["values"]["primary"],
            truth=frozen["truth"],
            repair_output=output,
        )
        full_score = score_v121(
            reconciled,
            frozen["v122"]["values"]["canary"],
            frozen["v122"]["v121"]["values"]["truth"],
        )
        score = score_v123(output, frozen["truth"], full_score)
        score_path = root / "capped-repair-score.json"
        full_score_path = root / "recomputed-full-field-owner-score.json"
        reconciled_path = root / "reconciled-retained-field-output.private.json"
        _write_immutable(score_path, score)
        _write_immutable(full_score_path, full_score)
        _write_immutable(reconciled_path, reconciled)
        candidate_path = root / "calibration-truth-v10-retained-field-candidate.private.json"
        if score["passed"]:
            candidate = build_reference_candidate(
                frozen["v119"]["values"]["truth"],
                frozen["v122"]["v121"]["values"]["truth"],
                reconciled,
            )
            if candidate.get("schema_version") != V121_REFERENCE_VERSION:
                raise JudgeV5CalibrationV123Error("v121 reference projection drifted")
            candidate["schema_version"] = V123_REFERENCE_VERSION
            candidate["reference_version"] = (
                "fixture_reference_v10_retained_field_owner_repaired_candidate"
            )
            candidate["retained_field_capped_repair_count"] = REPAIR_COUNT
            _write_immutable(candidate_path, candidate)
        accounting = _aggregate_usage([sidecar])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V123_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v123_capped_repair_passed_retained_field_patch_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v123_retained_field_reference_patch_authorized"
                if passed
                else "v123_capped_repair_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "retained_field_reference_patch_authorized": passed,
            "proposition_reference_frozen": False,
            "alignment_reference_frozen": False,
            "fresh_diagnostic_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "full_field_owner_score": _record(full_score_path),
            "repair_output": _record(output_path),
            "reconciled_output": _record(reconciled_path),
            "reference_candidate": _record(candidate_path) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v121_usage": frozen["v122"]["v121"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.error_class)
    except Exception as exc:
        return _write_failure(root, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v123 capped retained-field repair")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v123(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "retained_field_reference_patch_authorized": terminal.get(
                    "retained_field_reference_patch_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
