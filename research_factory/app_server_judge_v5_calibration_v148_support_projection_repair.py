from __future__ import annotations

"""Pointwise-support projection repair for the two unstable v146 UI decisions."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from . import app_server_judge_v5_calibration_v147_v146_postprocess_recovery as v147
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
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V148_TRUTH_VERSION = "pif_app_server_judge_v5_4_v148_support_projection_truth_v1"
V148_SELECTION_VERSION = "pif_app_server_judge_v5_4_v148_selection_v1"
V148_SPEC_VERSION = "pif_app_server_judge_v5_4_v148_spec_v1"
V148_SCORE_VERSION = "pif_app_server_judge_v5_4_v148_score_v1"
V148_REFERENCE_VERSION = (
    "pif_app_server_judge_v5_4_calibration_truth_v13_support_projection_frozen"
)
V148_FAILURE_VERSION = "pif_app_server_judge_v5_4_v148_failure_v1"
V148_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v148_terminal_v1"
V148_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V148_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V148_PHASE_ID = "judge_v5_4_v148_support_projection_repair"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
TURN_NAMES = ("support_projection_primary", "support_projection_order_canary")
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    v147.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v148-support-projection-repair"
).resolve()


class JudgeV5CalibrationV148Error(RuntimeError):
    """The v148 pointwise-support projection contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v147() -> dict[str, Any]:
    root = v147.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "score": root / "recovered-v146-score.json",
        "plan": root / "unsupported-inference-support-projection-plan.json",
    }
    values = {name: _load_json(path, f"v147 {name}") for name, path in paths.items()}
    terminal, score, plan = values["terminal"], values["score"], values["plan"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v146_singleton_repeat_quality_gate_not_passed_postprocessing_recovered"
        or terminal.get("support_projection_repair_authorized") is not True
        or terminal.get("semantic_attempt_started") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage", {}).get("total_tokens") != 0
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or score.get("passed") is not False
        or score.get("failed_checks") != ["unstable_owner_repeat_exact_rate"]
        or plan.get("failed_repeat_field_counts") != {"unsupported_inference": 2}
        or plan.get("missing_fresh_support_unit_count") != 1
        or plan.get("fresh_turn_count") != 2
        or plan.get("retry_count_per_turn") != 0
        or plan.get("selection_authorized") is not False
        or plan.get("holdout_authorized") is not False
        or plan.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV148Error("v147 recovery contract drifted")
    for name, key in {"score": "recovered_score", "plan": "support_projection_plan"}.items():
        record = terminal.get(key)
        if record != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV148Error(f"v147 {name} record drifted")
    source = v147._validate_v146_failure()
    if (
        terminal.get("predecessor_v146_terminal") != source["records"]["terminal"]
        or terminal.get("predecessor_v146_failure") != source["records"]["failure"]
        or terminal.get("predecessor_v146_usage") != source["usage"]
    ):
        raise JudgeV5CalibrationV148Error("v147 predecessor binding drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "v146": source,
    }


def _v143_support_units(source: Mapping[str, Any]) -> tuple[dict[tuple[str, str], Any], list[dict[str, Any]]]:
    v143_source = source["v146"]["predecessor"]["v144"]["v143"]
    records = []
    units = {}
    for row in v143_source["values"]["spec"]["frozen_inputs"]["turns"]:
        if row["role"] not in {"support_primary", "support_order_canary"}:
            continue
        record = row["input"]
        if not _verify_record(record):
            raise JudgeV5CalibrationV148Error("v143 support input drifted")
        records.append(record)
        value = _load_json(Path(record["path"]), "v143 support input")
        for unit in value["units"]:
            units[(str(unit["case_id"]), str(unit["witness_id"]))] = unit
    if len(records) != 2 or len(units) != 12:
        raise JudgeV5CalibrationV148Error("v143 support source coverage drifted")
    return units, records


def build_v148_inputs(
    source: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    v146_source = source["v146"]
    plan = source["values"]["plan"]
    missing = [row for row in plan["units"] if not row["pointwise_support_receipt_available"]]
    inherited = [row for row in plan["units"] if row["pointwise_support_receipt_available"]]
    if len(missing) != 1 or len(inherited) != 1:
        raise JudgeV5CalibrationV148Error("v148 projection target coverage drifted")
    truth_map = {row["task_id"]: row for row in v146_source["truth"]["tasks"]}
    missing_truth = truth_map[missing[0]["owner_task_id"]]
    source_task = v146_source["predecessor"]["input_tasks"][
        missing_truth["source_v145_task_id"]
    ]
    target_unit = {
        "case_id": missing_truth["case_id"],
        "witness_id": missing_truth["witness_id"],
        "proposition": {"claim_text": source_task["structured_event"]["claim_text"]},
        "source_excerpt": source_task["source_excerpt"],
    }

    source_units, support_input_records = _v143_support_units(source)
    v143_source = v146_source["predecessor"]["v144"]["v143"]
    primary = {
        (row["case_id"], row["witness_id"]): row
        for row in v143_source["values"]["support"]["units"]
    }
    canary = {
        (row["case_id"], row["witness_id"]): row
        for row in v143_source["values"]["support_canary"]["units"]
    }
    truth_support = v143_source["values"]["truth"]["support"]
    controls = []
    for status in ("supported", "unsupported"):
        candidates = [
            row
            for row in truth_support
            if row["expected_status"] == status
            and primary[(row["case_id"], row["witness_id"])]["support_status"] == status
            and canary[(row["case_id"], row["witness_id"])]["support_status"] == status
        ]
        candidates.sort(
            key=lambda row: sha256_text(
                f"v148|control|{status}|{row['case_id']}|{row['witness_id']}"
            )
        )
        if not candidates:
            raise JudgeV5CalibrationV148Error("v148 support control coverage drifted")
        controls.append(candidates[0])
    units = [deepcopy(source_units[(row["case_id"], row["witness_id"])]) for row in controls]
    units.append(target_unit)
    units.sort(key=lambda row: sha256_text(f"v148|unit|{row['case_id']}|{row['witness_id']}"))
    primary_input = v143._support_input(units)
    canary_input = v143._support_input(list(reversed(units)))
    truth = {
        "schema_version": V148_TRUTH_VERSION,
        "unit_count": 3,
        "control_count": 2,
        "target_count": 1,
        "controls": [
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "expected_status": row["expected_status"],
            }
            for row in controls
        ],
        "target": {
            "case_id": target_unit["case_id"],
            "witness_id": target_unit["witness_id"],
            "owner_task_id": missing_truth["task_id"],
            "source_v143_task_id": missing_truth["source_v143_task_id"],
        },
        "inherited_projection": inherited[0],
    }
    selection = {
        "schema_version": V148_SELECTION_VERSION,
        "created_at": now_iso(),
        "unit_count": 3,
        "control_status_counts": {"supported": 1, "unsupported": 1},
        "target_count": 1,
        "primary_turn_count": 1,
        "order_canary_turn_count": 1,
        "maximum_units_per_turn": 3,
        "canary_marker_in_model_input": False,
        "canary_membership_identical": True,
        "canary_order_reversed": True,
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "semantic_pruning_performed": False,
        "projection_is_deterministic_from_llm_support_status": True,
        "projection_rule": {"supported": "correct", "unsupported": "incorrect"},
        "majority_voting_used": False,
        "privacy": "private_source_event_output_aggregate_reports_only",
    }
    return primary_input, canary_input, truth, selection, support_input_records


def score_v148(
    *, primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    observed = {(row["case_id"], row["witness_id"]): row for row in primary["units"]}
    repeated = {(row["case_id"], row["witness_id"]): row for row in canary["units"]}
    expected_keys = {
        (row["case_id"], row["witness_id"]) for row in truth["controls"]
    } | {(truth["target"]["case_id"], truth["target"]["witness_id"])}
    if set(observed) != expected_keys or set(repeated) != expected_keys:
        raise JudgeV5CalibrationV148Error("v148 output coverage drifted")
    control_exact = sum(
        observed[(row["case_id"], row["witness_id"])]["support_status"]
        == row["expected_status"]
        and repeated[(row["case_id"], row["witness_id"])]["support_status"]
        == row["expected_status"]
        for row in truth["controls"]
    )
    order_exact = sum(
        observed[key]["support_status"] == repeated[key]["support_status"]
        for key in expected_keys
    )
    target_key = (truth["target"]["case_id"], truth["target"]["witness_id"])
    target_status = observed[target_key]["support_status"]
    evidence_complete = sum(
        bool(row["source_evidence_spans"])
        for row in list(observed.values()) + list(repeated.values())
    )
    checks = {
        "settled_control_exact_rate": control_exact == 2,
        "order_canary_exact_rate": order_exact == 3,
        "target_abstention_count": target_status != "abstain",
        "evidence_complete_rate": evidence_complete == 6,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V148_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "unit_count": 3,
            "control_count": 2,
            "control_exact_count": control_exact,
            "order_canary_decision_count": 3,
            "order_canary_exact_count": order_exact,
            "target_abstention_count": int(target_status == "abstain"),
            "evidence_complete_count": evidence_complete,
        },
        "target_support_status": target_status,
        "reference_patch_authorized": passed,
        "fresh_corrected_field_diagnostic_authorized": passed,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _project_owner_output(
    *, source: Mapping[str, Any], truth: Mapping[str, Any], fresh_support: Mapping[str, Any]
) -> dict[str, Any]:
    result = v146._merge_primary(source["v146"]["outputs"])
    rows = {row["task_id"]: row for row in result["decisions"]}
    inherited = truth["inherited_projection"]
    inherited_key = (inherited["case_id"], inherited["witness_id"])
    v143_source = source["v146"]["predecessor"]["v144"]["v143"]
    inherited_receipts = {
        (row["case_id"], row["witness_id"]): row
        for row in v143_source["values"]["support"]["units"]
    }
    inherited_repeat = {
        (row["case_id"], row["witness_id"]): row
        for row in v143_source["values"]["support_canary"]["units"]
    }
    inherited_row = inherited_receipts[inherited_key]
    if (
        inherited_row["support_status"] == "abstain"
        or inherited_row["support_status"]
        != inherited_repeat[inherited_key]["support_status"]
    ):
        raise JudgeV5CalibrationV148Error("v148 inherited support receipt drifted")
    fresh_key = (truth["target"]["case_id"], truth["target"]["witness_id"])
    fresh_row = {
        (row["case_id"], row["witness_id"]): row for row in fresh_support["units"]
    }[fresh_key]
    projection = {"supported": "correct", "unsupported": "incorrect"}
    for task_id, receipt in (
        (inherited["owner_task_id"], inherited_row),
        (truth["target"]["owner_task_id"], fresh_row),
    ):
        if receipt["support_status"] not in projection:
            raise JudgeV5CalibrationV148Error("v148 cannot project abstaining support")
        rows[task_id]["field_status"] = projection[receipt["support_status"]]
        rows[task_id]["source_evidence_spans"] = deepcopy(receipt["source_evidence_spans"])
        rows[task_id]["rationale"] = "Deterministic projection from side-free LLM pointwise support receipt."
    result["schema_version"] = V148_REFERENCE_VERSION
    result["unsupported_inference_projection_count"] = 2
    return result


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V148_CAPACITY_AUDIT_VERSION,
        "phase_id": V148_PHASE_ID,
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
        "schema_version": V148_CAPACITY_POLICY_VERSION,
        "phase_id": V148_PHASE_ID,
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


def freeze_v148(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v148 terminal")}
    source = _validate_v147()
    primary, canary, truth, selection, support_input_records = build_v148_inputs(source)
    truth_path = root / "support-projection-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    turns = []
    for turn_name, role, value in (
        (TURN_NAMES[0], "support_primary", primary),
        (TURN_NAMES[1], "support_order_canary", canary),
    ):
        prompt, schema = v143.support_prompt_v143(value), v143.support_output_schema(value)
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=value,
            prompt=prompt,
            schema=schema,
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": role,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    predecessor_records = {
        **{f"v147_{name}": record for name, record in source["records"].items()},
        **{f"v146_{name}": record for name, record in source["v146"]["records"].items()},
        "v146_attempts": source["v146"]["attempts"],
        "v143_support_input_records": support_input_records,
        "v143_support_output": source["v146"]["predecessor"]["v144"]["v143"]["records"]["support"],
        "v143_support_canary": source["v146"]["predecessor"]["v144"]["v143"]["records"]["support_canary"],
        "v143_truth": source["v146"]["predecessor"]["v144"]["v143"]["records"]["truth"],
    }
    capacity = _build_capacity_policy(root, predecessor_records)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V148_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "side_free_pointwise_support_projection_for_two_unstable_unsupported_inference_fields",
        "unit_count": 3,
        "control_count": 2,
        "target_count": 1,
        "turn_plan": list(TURN_NAMES),
        "maximum_units_per_turn": 3,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "canary_marker_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "two_controls_exact_target_nonabstaining_all_three_order_exact_complete_exact_evidence",
        "reference_patch_authorized": False,
        "fresh_corrected_field_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor_records,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v147_v146_postprocess_recovery.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v146_singleton_reference_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v143_corrected_layered_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "truth": _record(truth_path),
            "selection": _record(selection_path),
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "role": turn["turn_role"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "support-projection-repair-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "source": source,
    }


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v148 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V148_FAILURE_VERSION,
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
        "schema_version": V148_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_patch_authorized": False,
        "fresh_corrected_field_diagnostic_authorized": False,
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


async def run_v148(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v148 terminal")
    frozen = freeze_v148(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        outputs = {}
        sidecars = []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v143.support_base_instructions_v143(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=3,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: v143.validate_support_output(
                        candidate, item
                    ),
                )
                outputs[current_turn] = output
                sidecars.append(sidecar)
        primary, canary = outputs[TURN_NAMES[0]], outputs[TURN_NAMES[1]]
        score = score_v148(primary=primary, canary=canary, truth=frozen["truth"])
        paths = {
            "primary": root / "support-projection-primary.private.json",
            "canary": root / "support-projection-canary.private.json",
            "score": root / "support-projection-score.json",
            "owner": root / "projected-singleton-owner-output.private.json",
            "truth": root / "patched-v143-truth-audit-only.private.json",
            "reference": root / "calibration-truth-v13-support-projection.private.json",
            "v144_score": root / "old-v144-rescore-audit-only.json",
        }
        _write_immutable(paths["primary"], primary)
        _write_immutable(paths["canary"], canary)
        _write_immutable(paths["score"], score)
        passed = bool(score["passed"])
        if passed:
            owner = _project_owner_output(
                source=frozen["source"], truth=frozen["truth"], fresh_support=primary
            )
            patched_truth, reference = v146._patch_truth_and_reference(
                current_truth=frozen["source"]["v146"]["predecessor"]["v144"]["v143"]["values"]["truth"],
                current_reference=frozen["source"]["v146"]["predecessor"]["v144"]["v143"]["v142"]["values"]["reference"],
                owner_truth=frozen["source"]["v146"]["truth"],
                owner_output=owner,
            )
            reference["schema_version"] = V148_REFERENCE_VERSION
            reference["reference_version"] = "fixture_reference_v13_support_projection_frozen"
            reference["v148_support_projection_count"] = 2
            reference["v148_reference_basis"] = (
                "singleton_field_owner_plus_side_free_llm_pointwise_support_projection"
            )
            v144_source = frozen["source"]["v146"]["predecessor"]["v144"]
            old_v144_score = v143.score_v143(
                support=v144_source["v143"]["values"]["support"],
                support_canary=v144_source["v143"]["values"]["support_canary"],
                fields=v144_source["values"]["fields"],
                field_canary=v144_source["values"]["field_canary"],
                truth=patched_truth,
            )
            _write_immutable(paths["owner"], owner)
            _write_immutable(paths["truth"], patched_truth)
            _write_immutable(paths["reference"], reference)
            _write_immutable(paths["v144_score"], old_v144_score)
        accounting = _aggregate_usage(sidecars)
        terminal = {
            "schema_version": V148_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v148_reference_v13_frozen_fresh_corrected_field_diagnostic_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v148_support_projection_repair_passed"
                if passed
                else "v148_support_projection_repair_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reference_patch_authorized": passed,
            "reference_frozen": passed,
            "fresh_corrected_field_diagnostic_authorized": passed,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(paths["score"]),
            "primary_output": _record(paths["primary"]),
            "canary_output": _record(paths["canary"]),
            "projected_owner_output": _record(paths["owner"]) if passed else None,
            "patched_truth_audit_only": _record(paths["truth"]) if passed else None,
            "old_v144_rescore_audit_only": _record(paths["v144_score"]) if passed else None,
            "reference": _record(paths["reference"]) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v146_usage": frozen["source"]["v146"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v148 pointwise-support projection repair")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v148(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal.get("reference_frozen", False),
                "fresh_corrected_field_diagnostic_authorized": terminal.get(
                    "fresh_corrected_field_diagnostic_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
