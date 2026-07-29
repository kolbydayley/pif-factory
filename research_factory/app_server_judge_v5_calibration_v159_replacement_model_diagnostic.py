from __future__ import annotations

"""Blinded replacement-model diagnostic authorized by the v158 quality terminal."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v145_comprehensive_field_reference_owner as v145
from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from . import app_server_judge_v5_calibration_v149_fresh_corrected_field_diagnostic as v149
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from . import app_server_judge_v5_calibration_v158_postprocess_quality_terminal as v158
from .app_server_judge_v5 import (
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
)
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    output_schema as field_output_schema,
    validate_output as validate_field_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v99_refined_luna_diagnostic import (
    _general_requested_field_value,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .util import now_iso, sha256_text


V159_SPEC_VERSION = "pif_app_server_judge_v5_4_v159_spec_v1"
V159_SCORE_VERSION = "pif_app_server_judge_v5_4_v159_score_v1"
V159_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v159_replacement_diagnostic_protocol_v1"
V159_FAILURE_VERSION = "pif_app_server_judge_v5_4_v159_failure_v1"
V159_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v159_terminal_v1"
V159_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V159_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V159_PHASE_ID = "judge_v5_4_v159_replacement_model_diagnostic"

FIELD_MODEL = "gpt-5.6-sol"
ALIGNMENT_MODEL = "gpt-5.5"
EFFORT = "high"
FIELD_TURNS = tuple(f"replacement_field_singleton_{index:02d}" for index in range(12))
ALIGNMENT_PRIMARY_TURNS = (
    "replacement_alignment_primary_00",
    "replacement_alignment_primary_01",
)
ALIGNMENT_CANARY_TURNS = (
    "replacement_alignment_canary_00",
    "replacement_alignment_canary_01",
)
TURN_NAMES = FIELD_TURNS + ALIGNMENT_PRIMARY_TURNS + ALIGNMENT_CANARY_TURNS
MAXIMUM_TOTAL_TOKENS_PER_TURN = 45000
TIMEOUT_SECONDS = v155.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v158.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v159-replacement-model-diagnostic"
).resolve()


class JudgeV5CalibrationV159Error(RuntimeError):
    """The immutable v159 diagnostic contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v158() -> dict[str, Any]:
    root = v158.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "audit": root / "postprocess-audit.json",
        "score": root / "full-calibration-score.json",
        "taxonomy": root / "residual-error-taxonomy.json",
        "diagnostic": root / "replacement-diagnostic-contract.json",
    }
    values = {name: _load_json(path, f"v158 {name}") for name, path in paths.items()}
    terminal, score, taxonomy, diagnostic = (
        values["terminal"],
        values["score"],
        values["taxonomy"],
        values["diagnostic"],
    )
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v158_full_development_calibration_quality_gate_not_passed"
        or terminal.get("development_judge_frozen") is not False
        or terminal.get("bounded_replacement_model_diagnostic_authorized") is not True
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_attempt_started") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 0
        or terminal.get("predecessor_cumulative_usage", {}).get("total_tokens") != 1506890
        or score.get("passed") is not False
        or taxonomy.get("field_error_count") != 6
        or taxonomy.get("alignment_error_case_count") != 6
        or diagnostic.get("maximum_turn_count") != 16
        or diagnostic.get("maximum_total_token_bound") != 720000
        or diagnostic.get("truth_labels_exposed_to_model") is not False
        or diagnostic.get("selection_authorized") is not False
        or diagnostic.get("holdout_authorized") is not False
        or terminal.get("audit") != _record(paths["audit"])
        or terminal.get("score") != _record(paths["score"])
        or terminal.get("taxonomy") != _record(paths["taxonomy"])
        or terminal.get("diagnostic_contract") != _record(paths["diagnostic"])
    ):
        raise JudgeV5CalibrationV159Error("v158 diagnostic authorization drifted")
    source = v158._validate_v157_failure()
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "source": source,
        "cumulative_usage": terminal["predecessor_cumulative_usage"],
    }


def _field_value(
    *, task_id: str, truth_row: Mapping[str, Any], pool: Mapping[str, Any]
) -> dict[str, Any]:
    units = v155._witness_units(pool)
    unit = units[str(truth_row["witness_id"])]
    event = compact_empty_event_fields(deepcopy(unit["structured_event"]))
    field = str(truth_row["field"])
    task = {
        "task_id": task_id,
        "field": field,
        "field_contract": {"field": field, **deepcopy(v145.FIELD_RULES_V145[field])},
        "requested_field_value": _general_requested_field_value(field, event),
        "source_excerpt": unit["source_excerpt"],
        "structured_event": event,
    }
    return v149._field_value(task)


def _visible_support_projection(
    expected: Mapping[str, Any], visible_witness_ids: set[str]
) -> dict[str, Any]:
    """Project frozen truth onto the witnesses visible after the support pass."""
    pairs = [
        deepcopy(row)
        for row in expected["pairs"]
        if set(row["witness_ids"]) <= visible_witness_ids
    ]
    groups = []
    for group in expected["equivalence_groups"]:
        retained = sorted(set(group) & visible_witness_ids)
        if retained and retained not in groups:
            groups.append(retained)
    for witness_id in sorted(visible_witness_ids):
        if not any(witness_id in group for group in groups):
            groups.append([witness_id])
    groups.sort()
    paired = {witness_id for pair in pairs for witness_id in pair["witness_ids"]}
    return {
        "pairs": sorted(pairs, key=lambda row: tuple(sorted(row["witness_ids"]))),
        "equivalence_groups": groups,
        "unpaired_witness_ids": sorted(visible_witness_ids - paired),
    }


def build_v159_inputs(source: Mapping[str, Any]) -> dict[str, Any]:
    diagnostic = source["values"]["diagnostic"]
    v155_source = source["source"]["source"]["v155"]
    truth = v155_source["values"]["truth"]
    pool = v155_source["values"]["pool"]
    receipts = v155_source["values"]["support_receipts"]
    field_truth = {str(row["task_id"]): row for row in truth["field_tasks"]}
    selected_field_ids = [
        *diagnostic["field_error_task_ids"],
        *diagnostic["field_control_task_ids"],
    ]
    selected_field_ids.sort(key=lambda value: sha256_text(f"v159|field-order|{value}"))
    field_turns = []
    for turn_name, task_id in zip(FIELD_TURNS, selected_field_ids, strict=True):
        field_turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "field_singleton",
                "task_id": task_id,
                "value": _field_value(
                    task_id=task_id, truth_row=field_truth[task_id], pool=pool
                ),
            }
        )

    selected_case_ids = [
        *diagnostic["alignment_error_case_ids"],
        *diagnostic["alignment_control_case_ids"],
    ]
    selected_case_ids.sort(
        key=lambda value: sha256_text(f"v159|alignment-order|{value}")
    )
    shards = (selected_case_ids[:6], selected_case_ids[6:])
    alignment_turns = []
    for turn_name, case_ids in zip(ALIGNMENT_PRIMARY_TURNS, shards, strict=True):
        alignment_turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "alignment_primary",
                "case_ids": list(case_ids),
                "value": v155._support_positive_alignment_input(
                    pool, receipts, case_ids=case_ids, permutation="base"
                ),
            }
        )
    for turn_name, case_ids in zip(ALIGNMENT_CANARY_TURNS, shards, strict=True):
        alignment_turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "alignment_canary",
                "case_ids": list(case_ids),
                "value": v155._support_positive_alignment_input(
                    pool,
                    receipts,
                    case_ids=case_ids,
                    permutation="balanced_canary",
                ),
            }
        )
    full_expected = {
        str(row["case_id"]): row["expected"] for row in truth["alignment_cases"]
    }
    alignment_diagnostic_expected = {}
    for turn in alignment_turns:
        if turn["turn_role"] != "alignment_primary":
            continue
        for case in turn["value"]["cases"]:
            case_id = str(case["case_id"])
            visible = {str(row["witness_id"]) for row in case["witnesses"]}
            alignment_diagnostic_expected[case_id] = _visible_support_projection(
                full_expected[case_id], visible
            )
    if (
        len(field_turns) != 12
        or len(alignment_turns) != 4
        or len(set(selected_field_ids)) != 12
        or len(set(selected_case_ids)) != 12
        or set(alignment_diagnostic_expected) != set(selected_case_ids)
    ):
        raise JudgeV5CalibrationV159Error("v159 diagnostic coverage drifted")
    return {
        "field_turns": field_turns,
        "alignment_turns": alignment_turns,
        "selected_field_ids": selected_field_ids,
        "selected_case_ids": selected_case_ids,
        "alignment_diagnostic_expected": alignment_diagnostic_expected,
        "truth": truth,
        "pool": pool,
        "receipts": receipts,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    audit = {
        "schema_version": V159_CAPACITY_AUDIT_VERSION,
        "phase_id": V159_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V159_CAPACITY_POLICY_VERSION,
        "phase_id": V159_PHASE_ID,
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
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": math.ceil(
            bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v159(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v159 terminal")}
    source = _validate_v158()
    data = build_v159_inputs(source)
    turns = []
    for row in data["field_turns"] + data["alignment_turns"]:
        is_field = row["turn_role"] == "field_singleton"
        prompt = (
            v149.field_prompt_v149(row["value"])
            if is_field
            else v130.alignment_prompt_v130(row["value"])
        )
        schema = (
            field_output_schema(row["value"])
            if is_field
            else neutral_alignment_output_schema(row["value"])
        )
        paths = _freeze_turn_request(
            root=root,
            turn_name=row["turn_name"],
            input_value=row["value"],
            prompt=prompt,
            schema=schema,
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor = {
        **{f"v158_{name}": record for name, record in source["records"].items()},
        "cumulative_usage": source["cumulative_usage"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V159_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "field_model": FIELD_MODEL,
        "alignment_model": ALIGNMENT_MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "blinded_replacement_models_six_residuals_plus_six_controls_per_layer",
        "field_turn_count": 12,
        "alignment_primary_turn_count": 2,
        "alignment_canary_turn_count": 2,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": len(TURN_NAMES),
        "maximum_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "truth_labels_exposed_to_model": False,
        "side_labels_exposed_to_model": False,
        "system_identity_exposed_to_model": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v158_postprocess_quality_terminal.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v157_exact_span_canary_recovery.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v155_fresh_full_development.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v149_fresh_corrected_field_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v146_singleton_reference_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v145_comprehensive_field_reference_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v130_retained_alignment_owner.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v99_refined_luna_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
            _record(runtime_dir / "util.py"),
        ],
        "frozen_instructions": {
            "field_sha256": sha256_text(v146.base_instructions_v146()),
            "alignment_sha256": sha256_text(v130.alignment_instructions_v130()),
        },
        "frozen_inputs": {
            "truth": source["source"]["source"]["v155"]["records"]["truth"],
            "diagnostic_contract": source["records"]["diagnostic"],
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
        "privacy": "private_source_event_output_truth_and_mapping_sanitized_terminal_only",
    }
    spec_path = root / "replacement-model-diagnostic-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "data": data,
        "source": source,
    }


def score_v159(
    *,
    field_output: Mapping[str, Any],
    alignment_primary: Mapping[str, Any],
    alignment_canary: Mapping[str, Any],
    data: Mapping[str, Any],
    source: Mapping[str, Any],
) -> dict[str, Any]:
    diagnostic = source["values"]["diagnostic"]
    field_truth = {
        str(row["task_id"]): row for row in data["truth"]["field_tasks"]
    }
    field_observed = {
        str(row["task_id"]): row for row in field_output["decisions"]
    }
    selected_field_ids = set(data["selected_field_ids"])
    if set(field_observed) != selected_field_ids:
        raise JudgeV5CalibrationV159Error("v159 field score coverage drifted")
    field_exact = {
        task_id: field_observed[task_id]["field_status"]
        == field_truth[task_id]["expected_status"]
        for task_id in selected_field_ids
    }
    field_abstentions = sum(
        field_observed[task_id]["field_status"] == "abstain"
        for task_id in selected_field_ids
    )

    expected = {
        str(row["case_id"]): row["expected"]
        for row in data["truth"]["alignment_cases"]
    }
    diagnostic_expected = data["alignment_diagnostic_expected"]
    primary_input = deepcopy(data["alignment_turns"][0]["value"])
    primary_input["cases"] = [
        deepcopy(case)
        for row in data["alignment_turns"]
        if row["turn_role"] == "alignment_primary"
        for case in row["value"]["cases"]
    ]
    canary_input = deepcopy(
        next(
            row["value"]
            for row in data["alignment_turns"]
            if row["turn_role"] == "alignment_canary"
        )
    )
    canary_input["cases"] = [
        deepcopy(case)
        for row in data["alignment_turns"]
        if row["turn_role"] == "alignment_canary"
        for case in row["value"]["cases"]
    ]
    normalized_primary = normalize_neutral_alignment_output(
        alignment_primary, primary_input
    )
    normalized_canary = normalize_neutral_alignment_output(
        alignment_canary, canary_input
    )
    primary = {
        str(row["case_id"]): v130._project_alignment(row)
        for row in normalized_primary["cases"]
    }
    canary = {
        str(row["case_id"]): v130._project_alignment(row)
        for row in normalized_canary["cases"]
    }
    selected_case_ids = set(data["selected_case_ids"])
    if set(primary) != selected_case_ids or set(canary) != selected_case_ids:
        raise JudgeV5CalibrationV159Error("v159 alignment score coverage drifted")
    current_reconciled = source["source"]["values"]["reconciled"]
    replacement = {
        str(row["case_id"]): v130._project_alignment(row)
        for row in current_reconciled["cases"]
    }
    replacement.update(primary)
    replacement_metrics = v158._projected_alignment_metrics(replacement, expected)
    primary_exact = {
        case_id: primary[case_id] == diagnostic_expected[case_id]
        for case_id in selected_case_ids
    }
    canary_exact = {
        case_id: primary[case_id] == canary[case_id]
        for case_id in selected_case_ids
    }
    alignment_abstentions = sum(
        v130._case_has_abstention(row) for row in normalized_primary["cases"]
    ) + sum(v130._case_has_abstention(row) for row in normalized_canary["cases"])

    field_error_ids = set(diagnostic["field_error_task_ids"])
    field_control_ids = set(diagnostic["field_control_task_ids"])
    alignment_error_ids = set(diagnostic["alignment_error_case_ids"])
    alignment_control_ids = set(diagnostic["alignment_control_case_ids"])
    metrics = {
        "field_overall_exact_count": sum(field_exact.values()),
        "field_decision_count": len(field_exact),
        "field_residual_exact_count": sum(field_exact[key] for key in field_error_ids),
        "field_residual_count": len(field_error_ids),
        "field_control_exact_count": sum(field_exact[key] for key in field_control_ids),
        "field_control_count": len(field_control_ids),
        "field_abstention_count": field_abstentions,
        "alignment_residual_exact_case_count": sum(
            primary_exact[key] for key in alignment_error_ids
        ),
        "alignment_residual_case_count": len(alignment_error_ids),
        "alignment_control_exact_case_count": sum(
            primary_exact[key] for key in alignment_control_ids
        ),
        "alignment_control_case_count": len(alignment_control_ids),
        "alignment_permutation_exact_case_count": sum(canary_exact.values()),
        "alignment_permutation_case_count": len(canary_exact),
        "alignment_abstention_case_count": alignment_abstentions,
        **replacement_metrics,
    }
    checks = {
        "field_overall_exact_count": metrics["field_overall_exact_count"] >= 11,
        "field_residual_exact_count": metrics["field_residual_exact_count"] >= 5,
        "field_control_exact_count": metrics["field_control_exact_count"] == 6,
        "field_abstention_count": metrics["field_abstention_count"] == 0,
        "alignment_residual_exact_case_count": metrics[
            "alignment_residual_exact_case_count"
        ]
        >= 5,
        "alignment_control_exact_case_count": metrics[
            "alignment_control_exact_case_count"
        ]
        == 6,
        "alignment_permutation_exact_case_count": metrics[
            "alignment_permutation_exact_case_count"
        ]
        == 12,
        "alignment_abstention_case_count": metrics[
            "alignment_abstention_case_count"
        ]
        == 0,
        "replacement_alignment_frozen_gates": all(
            metrics[key] >= 0.95
            for key in (
                "alignment_f1",
                "relation_accuracy",
                "equivalent_sensitivity",
                "equivalent_specificity",
                "mismatch_field_f1",
                "equivalence_partition_exact_case_rate",
                "unpaired_exact_case_rate",
            )
        ),
    }
    passed = all(checks.values())
    return {
        "schema_version": V159_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": metrics,
        "fresh_full_replacement_calibration_authorized": passed,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


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
            measured = _validate_usage(
                _load_json(Path(record["path"]), "v159 measured sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    predecessor = _validate_v158()["cumulative_usage"]
    cumulative = _sum_usage(predecessor, usage)
    failure = {
        "schema_version": V159_FAILURE_VERSION,
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
        "predecessor_cumulative_usage": predecessor,
        "cumulative_known_usage_lower_bound": cumulative,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V159_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "replacement_diagnostic_passed": False,
        "fresh_full_replacement_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "predecessor_cumulative_usage": predecessor,
        "cumulative_known_usage_lower_bound": cumulative,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v159(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v159 terminal")
    frozen = freeze_v159(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    sidecars = []
    field_outputs = []
    primary_outputs = []
    canary_outputs = []
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                is_field = turn["turn_role"] == "field_singleton"
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v146.base_instructions_v146()
                    if is_field
                    else v130.alignment_instructions_v130(),
                    model=FIELD_MODEL if is_field else ALIGNMENT_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=1 if is_field else len(turn["value"]["cases"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=(
                        (lambda candidate, item=turn["value"]: validate_field_output(candidate, item))
                        if is_field
                        else (
                            lambda candidate, item=turn["value"]: v157.validate_structurally_projectable_output(candidate, item)
                        )
                    ),
                )
                sidecars.append(sidecar)
                if is_field:
                    field_outputs.append(output)
                    continue
                projected, audit = v157.project_exact_spans_and_relation(
                    output, turn["value"]
                )
                turn_root = root / "turns" / current_turn.replace("_", "-")
                _write_immutable(turn_root / "structurally-projected.private.json", projected)
                _write_immutable(turn_root / "structural-projection-audit.json", audit)
                if turn["turn_role"] == "alignment_primary":
                    primary_outputs.append(projected)
                else:
                    canary_outputs.append(projected)

        field_output = v155._merge_outputs(field_outputs, "decisions")
        primary_output = v155._merge_outputs(primary_outputs, "cases")
        canary_output = v155._merge_outputs(canary_outputs, "cases")
        _write_immutable(root / "field-output.private.json", field_output)
        _write_immutable(root / "alignment-primary-projected.private.json", primary_output)
        _write_immutable(root / "alignment-canary-projected.private.json", canary_output)
        score = score_v159(
            field_output=field_output,
            alignment_primary=primary_output,
            alignment_canary=canary_output,
            data=frozen["data"],
            source=frozen["source"],
        )
        score_path = root / "replacement-model-diagnostic-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        protocol_path = root / "replacement-model-diagnostic-protocol-v159.json"
        if passed:
            protocol = {
                "schema_version": V159_PROTOCOL_VERSION,
                "frozen_at": now_iso(),
                "field_model": FIELD_MODEL,
                "alignment_model": ALIGNMENT_MODEL,
                "reasoning_effort": EFFORT,
                "field_context_mode": "singleton",
                "field_instructions_sha256": sha256_text(v146.base_instructions_v146()),
                "alignment_instructions_sha256": sha256_text(v130.alignment_instructions_v130()),
                "alignment_structural_projection_operations": [
                    "retain_only_exact_source_substrings",
                    "derive_relation_from_frozen_llm_checklist_precedence",
                ],
                "truth_labels_exposed_to_model": False,
                "quality_gates_unchanged": True,
                "fresh_full_replacement_calibration_authorized": True,
                "selection_authorized": False,
                "holdout_authorized": False,
                "production_mutation_allowed": False,
            }
            _write_immutable(protocol_path, protocol)
        accounting = _aggregate_usage(sidecars)
        predecessor_usage = frozen["source"]["cumulative_usage"]
        cumulative_usage = _sum_usage(predecessor_usage, accounting["usage"])
        terminal = {
            "schema_version": V159_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": "v159_replacement_model_diagnostic_passed_full_replacement_authorized"
            if passed
            else "inactive_incomplete_recovery_required",
            "development_terminal_reason": "v159_replacement_model_diagnostic_passed"
            if passed
            else "v159_replacement_model_diagnostic_quality_gate_not_passed",
            "overall_evaluation_complete": False,
            "replacement_diagnostic_passed": passed,
            "fresh_full_replacement_calibration_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "score": _record(score_path),
            "protocol": _record(protocol_path) if passed else None,
            "predecessor_cumulative_usage": predecessor_usage,
            "cumulative_calibration_usage": cumulative_usage,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v159 replacement-model diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v159(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "replacement_diagnostic_passed": terminal.get(
                    "replacement_diagnostic_passed", False
                ),
                "fresh_full_replacement_calibration_authorized": terminal.get(
                    "fresh_full_replacement_calibration_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
