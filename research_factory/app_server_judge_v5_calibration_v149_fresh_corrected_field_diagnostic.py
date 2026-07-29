from __future__ import annotations

"""Fresh 18-case diagnostic for the v148 support-projected field protocol."""

import argparse
import asyncio
import itertools
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_calibration_v145_comprehensive_field_reference_owner as v145
from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from . import app_server_judge_v5_calibration_v148_support_projection_repair as v148
from .app_server_judge_v5 import CHECKLIST_FIELDS
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
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V149_INPUT_VERSION = "pif_app_server_judge_v5_4_v149_field_input_v1"
V149_TRUTH_VERSION = "pif_app_server_judge_v5_4_v149_truth_v1"
V149_SELECTION_VERSION = "pif_app_server_judge_v5_4_v149_selection_v1"
V149_SPEC_VERSION = "pif_app_server_judge_v5_4_v149_spec_v1"
V149_SCORE_VERSION = "pif_app_server_judge_v5_4_v149_score_v1"
V149_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v149_protocol_v1"
V149_FAILURE_VERSION = "pif_app_server_judge_v5_4_v149_failure_v1"
V149_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v149_terminal_v1"
V149_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V149_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V149_PHASE_ID = "judge_v5_4_v149_fresh_corrected_field_diagnostic"

SUPPORT_MODEL = "gpt-5.6-sol"
FIELD_MODEL = "gpt-5.5"
EFFORT = "high"
SUPPORT_TURNS = ("fresh_support_primary", "fresh_support_order_canary")
FIELD_TURNS = tuple(f"fresh_field_singleton_{index:02d}" for index in range(16))
REPEAT_TURNS = tuple(f"fresh_field_repeat_{index:02d}" for index in range(4))
TURN_NAMES = SUPPORT_TURNS + FIELD_TURNS + REPEAT_TURNS
REQUIRED_PAIRED_FIELDS = ("unsupported_inference",)
REPEAT_FIELDS = ("evidence", "metric", "reported_actor", "target")
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    v148.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v149-fresh-corrected-field-diagnostic"
).resolve()


class JudgeV5CalibrationV149Error(RuntimeError):
    """The v149 fresh diagnostic contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _f1(tp: int, fp: int, fn: int) -> float:
    denominator = 2 * tp + fp + fn
    return round(2 * tp / denominator, 6) if denominator else 1.0


def _validate_v148() -> dict[str, Any]:
    root = v148.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "support-projection-repair-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "support-projection-score.json",
        "primary": root / "support-projection-primary.private.json",
        "canary": root / "support-projection-canary.private.json",
        "owner": root / "projected-singleton-owner-output.private.json",
        "truth": root / "patched-v143-truth-audit-only.private.json",
        "reference": root / "calibration-truth-v13-support-projection.private.json",
        "v144_score": root / "old-v144-rescore-audit-only.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v148 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v148_reference_v13_frozen_fresh_corrected_field_diagnostic_authorized"
        or terminal.get("reference_frozen") is not True
        or terminal.get("fresh_corrected_field_diagnostic_authorized") is not True
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 42584
        or score.get("passed") is not True
        or score.get("metrics", {}).get("control_exact_count") != 2
        or score.get("metrics", {}).get("order_canary_exact_count") != 3
        or score.get("metrics", {}).get("evidence_complete_count") != 6
        or spec.get("turn_plan") != list(v148.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV149Error("v148 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV149Error("v148 runtime record drifted")
    for name, key in {
        "score": "score",
        "primary": "primary_output",
        "canary": "canary_output",
        "owner": "projected_owner_output",
        "truth": "patched_truth_audit_only",
        "reference": "reference",
        "v144_score": "old_v144_rescore_audit_only",
    }.items():
        record = terminal.get(key)
        if record != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV149Error(f"v148 {name} record drifted")
    attempts = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_name in spec["turn_plan"]:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        records = {}
        for name, filename in {
            "capacity": "capacity.json",
            "sidecar": "sidecar.json",
            "output": "output.private.json",
        }.items():
            path = turn_root / filename
            if not path.is_file():
                raise JudgeV5CalibrationV149Error("v148 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v148 sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV149Error("v148 usage aggregate drifted")
    source = v148._validate_v147()
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "v147": source,
    }


def _task_id(source_task_id: str) -> str:
    return "diagnostic_" + sha256_text(f"v149|field|{source_task_id}")[:24]


def _field_value(task: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": V149_INPUT_VERSION,
        "requested_field": task["field"],
        "task_count": 1,
        "tasks": [deepcopy(task)],
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "disagreement_reasons_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "singleton_context": True,
    }


def _select_field_tasks(source: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = source["values"]["truth"]["field_tasks"]
    by_field = {
        field: sorted(
            [row for row in rows if row["field"] == field],
            key=lambda row: sha256_text(f"v149|candidate|{row['task_id']}"),
        )
        for field in CHECKLIST_FIELDS
    }
    if any(len(values) != 2 for values in by_field.values()):
        raise JudgeV5CalibrationV149Error("v149 field status coverage drifted")
    best = None
    optional_double_fields = [
        field for field in CHECKLIST_FIELDS if field not in REQUIRED_PAIRED_FIELDS
    ]
    for extra_doubles in itertools.combinations(optional_double_fields, 2):
        doubled = set(REQUIRED_PAIRED_FIELDS) | set(extra_doubles)
        fixed_rows = [row for field in doubled for row in by_field[field]]
        variable_fields = []
        fixed_single_rows = []
        for field in CHECKLIST_FIELDS:
            if field in doubled:
                continue
            statuses = {row["expected_status"] for row in by_field[field]}
            if len(statuses) == 1:
                fixed_single_rows.append(by_field[field][0])
            else:
                variable_fields.append(field)
        for choices in itertools.product((0, 1), repeat=len(variable_fields)):
            candidate = fixed_rows + fixed_single_rows + [
                by_field[field][choice]
                for field, choice in zip(variable_fields, choices, strict=True)
            ]
            if sum(row["expected_status"] == "correct" for row in candidate) != 9:
                continue
            unique_cases = len({row["case_id"] for row in candidate})
            rank = (
                -unique_cases,
                sha256_text(
                    "v149|selection|"
                    + "|".join(sorted(row["task_id"] for row in candidate))
                ),
            )
            if best is None or rank < best[0]:
                best = (rank, candidate)
    if best is None:
        raise JudgeV5CalibrationV149Error("v149 balanced selection is unavailable")
    selected = sorted(best[1], key=lambda row: sha256_text(f"v149|order|{row['task_id']}"))
    if (
        len(selected) != 18
        or len({row["task_id"] for row in selected}) != 18
        or {row["field"] for row in selected} != set(CHECKLIST_FIELDS)
        or sum(row["expected_status"] == "correct" for row in selected) != 9
        or len({row["case_id"] for row in selected}) < 12
    ):
        raise JudgeV5CalibrationV149Error("v149 selection invariant failed")
    return selected


def build_v149_inputs(
    source: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    selected = _select_field_tasks(source)
    v143_source = source["v147"]["v146"]["predecessor"]["v144"]["v143"]
    input_tasks = v143_source["input_tasks"]
    truth_rows = []
    field_rows = []
    support_targets = []
    for row in selected:
        task = deepcopy(input_tasks[row["task_id"]])
        task_id = _task_id(row["task_id"])
        task["task_id"] = task_id
        task["field_contract"] = {
            "field": row["field"],
            **deepcopy(v145.FIELD_RULES_V145[row["field"]]),
        }
        truth_row = {
            "task_id": task_id,
            "source_v143_task_id": row["task_id"],
            "case_id": row["case_id"],
            "witness_id": row["witness_id"],
            "field": row["field"],
            "expected_status": row["expected_status"],
        }
        truth_rows.append(truth_row)
        if row["field"] == "unsupported_inference":
            support_targets.append(
                {
                    "case_id": row["case_id"],
                    "witness_id": row["witness_id"],
                    "proposition": {"claim_text": task["structured_event"]["claim_text"]},
                    "source_excerpt": task["source_excerpt"],
                    "task_id": task_id,
                }
            )
        else:
            field_rows.append(
                {
                    "turn_role": "field_singleton",
                    "value": _field_value(task),
                    "task_id": task_id,
                    "field": row["field"],
                }
            )
    field_rows.sort(key=lambda row: sha256_text(f"v149|field-turn|{row['task_id']}"))
    if len(field_rows) != 16 or len(support_targets) != 2:
        raise JudgeV5CalibrationV149Error("v149 layered task split drifted")

    source_units, support_input_records = v148._v143_support_units(source["v147"])
    support_truth = v143_source["values"]["truth"]["support"]
    support_primary = {
        (row["case_id"], row["witness_id"]): row
        for row in v143_source["values"]["support"]["units"]
    }
    support_canary = {
        (row["case_id"], row["witness_id"]): row
        for row in v143_source["values"]["support_canary"]["units"]
    }
    controls = []
    for status in ("supported", "unsupported"):
        candidates = [
            row
            for row in support_truth
            if row["expected_status"] == status
            and support_primary[(row["case_id"], row["witness_id"])]["support_status"] == status
            and support_canary[(row["case_id"], row["witness_id"])]["support_status"] == status
        ]
        candidates.sort(key=lambda row: sha256_text(f"v149|support-control|{status}|{row['case_id']}|{row['witness_id']}"))
        if not candidates:
            raise JudgeV5CalibrationV149Error("v149 support controls drifted")
        controls.append(candidates[0])
    units = [deepcopy(source_units[(row["case_id"], row["witness_id"])]) for row in controls]
    units.extend({key: value for key, value in row.items() if key != "task_id"} for row in support_targets)
    units.sort(key=lambda row: sha256_text(f"v149|support-unit|{row['case_id']}|{row['witness_id']}"))
    support_value = v143._support_input(units)
    support_repeat = v143._support_input(list(reversed(units)))

    repeat_rows = []
    for field in REPEAT_FIELDS:
        candidates = [row for row in field_rows if row["field"] == field]
        candidates.sort(key=lambda row: sha256_text(f"v149|repeat|{row['task_id']}"))
        if not candidates:
            raise JudgeV5CalibrationV149Error("v149 repeat field coverage drifted")
        row = candidates[0]
        repeat_rows.append(
            {
                "turn_role": "field_repeat",
                "value": deepcopy(row["value"]),
                "task_id": row["task_id"],
                "field": field,
            }
        )
    rows = [
        {"turn_role": "support_primary", "value": support_value},
        {"turn_role": "support_order_canary", "value": support_repeat},
        *field_rows,
        *repeat_rows,
    ]
    for turn_name, row in zip(TURN_NAMES, rows, strict=True):
        row["turn_name"] = turn_name
    truth = {
        "schema_version": V149_TRUTH_VERSION,
        "task_count": 18,
        "status_counts": {"correct": 9, "incorrect": 9},
        "field_count": 15,
        "distinct_case_count": len({row["case_id"] for row in truth_rows}),
        "tasks": truth_rows,
        "support_controls": [
            {"case_id": row["case_id"], "witness_id": row["witness_id"], "expected_status": row["expected_status"]}
            for row in controls
        ],
        "support_target_task_ids": [row["task_id"] for row in support_targets],
        "repeat_task_ids": [row["task_id"] for row in repeat_rows],
    }
    selection = {
        "schema_version": V149_SELECTION_VERSION,
        "created_at": now_iso(),
        "task_count": 18,
        "status_counts": {"correct": 9, "incorrect": 9},
        "field_count": 15,
        "required_paired_fields": list(REQUIRED_PAIRED_FIELDS),
        "doubled_fields": sorted(
            field
            for field in CHECKLIST_FIELDS
            if sum(row["field"] == field for row in truth_rows) == 2
        ),
        "repeat_fields": list(REPEAT_FIELDS),
        "distinct_case_count": truth["distinct_case_count"],
        "support_unit_count": 4,
        "support_control_count": 2,
        "support_target_count": 2,
        "field_singleton_turn_count": 16,
        "field_repeat_turn_count": 4,
        "maximum_field_tasks_per_turn": 1,
        "maximum_support_units_per_turn": 4,
        "selection_uses_source_text": False,
        "selection_uses_only_frozen_reference_labels_provenance_and_opaque_ids": True,
        "canary_marker_in_model_input": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "semantic_pruning_performed": False,
        "unsupported_inference_is_projected_from_llm_support": True,
        "majority_voting_used": False,
        "privacy": "private_source_event_output_aggregate_reports_only",
    }
    return rows, truth, selection, support_input_records


def field_prompt_v149(value: Mapping[str, Any]) -> str:
    return (
        "Return the one independent field decision for the opaque task_id. The first evidence span "
        "must directly support the requested field decision and every span must be an exact "
        "substring of source_excerpt. Do not emit a whole-event verdict.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def score_v149(
    *,
    support: Mapping[str, Any],
    support_canary: Mapping[str, Any],
    field_outputs: Mapping[str, Mapping[str, Any]],
    repeat_outputs: Mapping[str, Mapping[str, Any]],
    truth: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {
        output["decisions"][0]["task_id"]: output["decisions"][0]
        for output in field_outputs.values()
    }
    repeated = {
        output["decisions"][0]["task_id"]: output["decisions"][0]
        for output in repeat_outputs.values()
    }
    support_rows = {(row["case_id"], row["witness_id"]): row for row in support["units"]}
    support_repeat = {(row["case_id"], row["witness_id"]): row for row in support_canary["units"]}
    projection = {"supported": "correct", "unsupported": "incorrect", "abstain": "abstain"}
    for task_id in truth["support_target_task_ids"]:
        row = expected[task_id]
        receipt = support_rows[(row["case_id"], row["witness_id"])]
        observed[task_id] = {
            "task_id": task_id,
            "field_status": projection[receipt["support_status"]],
            "source_evidence_spans": receipt["source_evidence_spans"],
            "rationale": receipt["rationale"],
        }
    if set(observed) != set(expected) or set(repeated) != set(truth["repeat_task_ids"]):
        raise JudgeV5CalibrationV149Error("v149 score coverage drifted")
    tp = fp = fn = correct = abstentions = 0
    for task_id, expected_row in expected.items():
        actual = observed[task_id]["field_status"]
        wanted = expected_row["expected_status"]
        correct += int(actual == wanted)
        abstentions += int(actual == "abstain")
        tp += int(wanted == "incorrect" and actual == "incorrect")
        fp += int(wanted == "correct" and actual == "incorrect")
        fn += int(wanted == "incorrect" and actual != "incorrect")
    control_exact = sum(
        support_rows[(row["case_id"], row["witness_id"])]["support_status"] == row["expected_status"]
        and support_repeat[(row["case_id"], row["witness_id"])]["support_status"] == row["expected_status"]
        for row in truth["support_controls"]
    )
    support_order_exact = sum(
        support_rows[key]["support_status"] == support_repeat[key]["support_status"]
        for key in support_rows
    )
    repeat_exact = sum(
        observed[task_id]["field_status"] == repeated[task_id]["field_status"]
        for task_id in truth["repeat_task_ids"]
    )
    evidence_complete = sum(
        bool(row["source_evidence_spans"])
        for row in list(observed.values())
        + list(repeated.values())
        + list(support_rows.values())
        + list(support_repeat.values())
    )
    ui_expected = [expected[task_id] for task_id in truth["support_target_task_ids"]]
    ui_supported = sum(
        support_rows[(row["case_id"], row["witness_id"])]["support_status"] == "supported"
        for row in ui_expected
        if row["expected_status"] == "correct"
    )
    ui_unsupported = sum(
        support_rows[(row["case_id"], row["witness_id"])]["support_status"] == "unsupported"
        for row in ui_expected
        if row["expected_status"] == "incorrect"
    )
    metrics = {
        "task_count": 18,
        "field_decision_accuracy": _ratio(correct, 18),
        "structured_field_accuracy": _ratio(correct, 18),
        "pointwise_field_issue_f1": _f1(tp, fp, fn),
        "field_abstention_count": abstentions,
        "support_sensitivity": _ratio(ui_supported, sum(row["expected_status"] == "correct" for row in ui_expected)),
        "support_specificity": _ratio(ui_unsupported, sum(row["expected_status"] == "incorrect" for row in ui_expected)),
        "support_control_exact_count": control_exact,
        "support_control_count": 2,
        "support_order_canary_exact_count": support_order_exact,
        "support_order_canary_decision_count": 4,
        "field_repeat_exact_count": repeat_exact,
        "field_repeat_decision_count": 4,
        "evidence_complete_count": evidence_complete,
        "evidence_decision_count": 30,
    }
    checks = {
        "field_decision_accuracy": metrics["field_decision_accuracy"] >= 0.95,
        "structured_field_accuracy": metrics["structured_field_accuracy"] >= 0.95,
        "pointwise_field_issue_f1": metrics["pointwise_field_issue_f1"] >= 0.95,
        "field_abstention_count": abstentions == 0,
        "support_sensitivity": metrics["support_sensitivity"] >= 0.95,
        "support_specificity": metrics["support_specificity"] >= 0.95,
        "support_control_exact_rate": control_exact == 2,
        "support_order_canary_exact_rate": support_order_exact == 4,
        "field_repeat_exact_rate": repeat_exact == 4,
        "evidence_complete_rate": evidence_complete == 30,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V149_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": metrics,
        "alignment_diagnostic_authorized": passed,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V149_CAPACITY_AUDIT_VERSION,
        "phase_id": V149_PHASE_ID,
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
        "schema_version": V149_CAPACITY_POLICY_VERSION,
        "phase_id": V149_PHASE_ID,
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
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v149(*, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v149 terminal")}
    source = _validate_v148()
    rows, truth, selection, support_input_records = build_v149_inputs(source)
    truth_path = root / "fresh-corrected-field-truth.private.json"
    selection_path = root / "selection-audit.json"
    rubric_path = root / "field-rubric-v149.json"
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    _write_immutable(rubric_path, v145.field_rubric_v145())
    turns = []
    for row in rows:
        value = row["value"]
        is_support = row["turn_role"].startswith("support_")
        prompt = v143.support_prompt_v143(value) if is_support else field_prompt_v149(value)
        schema = v143.support_output_schema(value) if is_support else output_schema(value)
        paths = _freeze_turn_request(root=root, turn_name=row["turn_name"], input_value=value, prompt=prompt, schema=schema)
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor_records = {
        **{f"v148_{name}": record for name, record in source["records"].items()},
        "v148_attempts": source["attempts"],
        "v143_support_input_records": support_input_records,
        "v143_field_inputs": [
            row["input"]
            for row in source["v147"]["v146"]["predecessor"]["v144"]["v143"]["values"]["spec"]["frozen_inputs"]["turns"]
            if row["role"].startswith("field_")
        ],
    }
    capacity = _build_capacity_policy(root, predecessor_records)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V149_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "support_model": SUPPORT_MODEL,
        "field_model": FIELD_MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_balanced_18_case_support_projected_singleton_field_diagnostic",
        "turn_plan": list(TURN_NAMES),
        "task_count": 18,
        "support_turn_count": 2,
        "field_singleton_turn_count": 16,
        "field_repeat_turn_count": 4,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "canary_marker_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_frozen_support_field_accuracy_f1_abstention_repeat_evidence_gates",
        "alignment_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor_records,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v148_support_projection_repair.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v145_comprehensive_field_reference_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v143_corrected_layered_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "truth": _record(truth_path),
            "selection": _record(selection_path),
            "rubric": _record(rubric_path),
            "reference": source["records"]["reference"],
            "turns": [
                {"turn_name": turn["turn_name"], "role": turn["turn_role"], "input": _record(turn["paths"]["input"]), "prompt": _record(turn["paths"]["prompt"]), "schema": _record(turn["paths"]["schema"])}
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "fresh-corrected-field-diagnostic-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {"root": root, "spec": spec, "spec_path": spec_path, "capacity_policy": capacity["policy"], "turns": turns, "truth": truth, "source": source}


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v149 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {"schema_version": V149_FAILURE_VERSION, "terminal_at": now_iso(), "classification": "infrastructure_or_judge_attempt_failed", "failed_turn_name": turn_name, "error_class": error_class, "retry_allowed_in_this_version": False, "accounting_complete": complete, "usage_status": "complete" if complete else "unknown", "usage": usage if complete else None, "known_usage_lower_bound": usage, "unknown_usage_turn_count": unknown, "attempts": attempts}
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {"schema_version": V149_TERMINAL_VERSION, "state": "failed", "terminal_reason": "infrastructure_or_judge_attempt_failed", "overall_evaluation_complete": False, "failure": _record(failure_path), "alignment_diagnostic_authorized": False, "fresh_full_calibration_authorized": False, "selection_authorized": False, "holdout_authorized": False, "production_mutated": False, "semantic_retry_count": 0, "accounting_complete": complete, "usage_status": failure["usage_status"], "usage": failure["usage"]}
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v149(*, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS, client_factory: Optional[Callable[[Path], Any]] = None) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v149 terminal")
    frozen = freeze_v149(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        outputs = {}
        sidecars = []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                is_support = turn["turn_role"].startswith("support_")
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=(v143.support_base_instructions_v143() if is_support else v146.base_instructions_v146()),
                    model=SUPPORT_MODEL if is_support else FIELD_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=turn["value"].get("unit_count", 1),
                    policy_path=frozen["capacity_policy"],
                    output_validator=(lambda candidate, item=turn["value"]: v143.validate_support_output(candidate, item)) if is_support else (lambda candidate, item=turn["value"]: validate_output(candidate, item)),
                )
                outputs[current_turn] = output
                sidecars.append(sidecar)
        field_outputs = {name: outputs[name] for name in FIELD_TURNS}
        repeat_outputs = {name: outputs[name] for name in REPEAT_TURNS}
        score = score_v149(support=outputs[SUPPORT_TURNS[0]], support_canary=outputs[SUPPORT_TURNS[1]], field_outputs=field_outputs, repeat_outputs=repeat_outputs, truth=frozen["truth"])
        paths = {"support": root / "fresh-support-primary.private.json", "support_canary": root / "fresh-support-canary.private.json", "fields": root / "fresh-field-singletons.private.json", "repeats": root / "fresh-field-repeats.private.json", "score": root / "fresh-corrected-field-score.json", "protocol": root / "field-judge-protocol-v149.json"}
        _write_immutable(paths["support"], outputs[SUPPORT_TURNS[0]])
        _write_immutable(paths["support_canary"], outputs[SUPPORT_TURNS[1]])
        _write_immutable(paths["fields"], {"turns": field_outputs})
        _write_immutable(paths["repeats"], {"turns": repeat_outputs})
        _write_immutable(paths["score"], score)
        passed = bool(score["passed"])
        if passed:
            protocol = {"schema_version": V149_PROTOCOL_VERSION, "frozen_at": now_iso(), "support_model": SUPPORT_MODEL, "field_model": FIELD_MODEL, "reasoning_effort": EFFORT, "support_instructions_sha256": sha256_text(v143.support_base_instructions_v143()), "field_instructions_sha256": sha256_text(v146.base_instructions_v146()), "field_rubric": _record(root / "field-rubric-v149.json"), "reference": frozen["source"]["records"]["reference"], "unsupported_inference_projection_rule": {"supported": "correct", "unsupported": "incorrect"}, "quality_gates_unchanged": True, "alignment_pending": True, "selection_authorized": False, "holdout_authorized": False, "production_mutation_allowed": False}
            _write_immutable(paths["protocol"], protocol)
        accounting = _aggregate_usage(sidecars)
        terminal = {"schema_version": V149_TERMINAL_VERSION, "state": "completed" if passed else "inactive", "terminal_at": now_iso(), "terminal_reason": "v149_field_protocol_frozen_alignment_diagnostic_authorized" if passed else "inactive_incomplete_recovery_required", "development_terminal_reason": "v149_fresh_corrected_field_diagnostic_passed" if passed else "v149_fresh_corrected_field_diagnostic_quality_gate_not_passed", "overall_evaluation_complete": False, "field_protocol_frozen": passed, "alignment_diagnostic_authorized": passed, "fresh_full_calibration_authorized": False, "selection_authorized": False, "holdout_authorized": False, "production_mutated": False, "semantic_attempt_started": True, "semantic_retry_count": 0, "score": _record(paths["score"]), "support_output": _record(paths["support"]), "support_canary": _record(paths["support_canary"]), "field_outputs": _record(paths["fields"]), "field_repeats": _record(paths["repeats"]), "protocol": _record(paths["protocol"]) if passed else None, "reference": frozen["source"]["records"]["reference"], "failed_quality_gates": score["failed_checks"], "metrics": score["metrics"], "predecessor_v148_usage": frozen["source"]["usage"], **accounting}
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v149 fresh corrected field diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v149(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({"state": terminal["state"], "terminal_reason": terminal["terminal_reason"], "field_protocol_frozen": terminal.get("field_protocol_frozen", False), "alignment_diagnostic_authorized": terminal.get("alignment_diagnostic_authorized", False), "usage_status": terminal.get("usage_status")}, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
