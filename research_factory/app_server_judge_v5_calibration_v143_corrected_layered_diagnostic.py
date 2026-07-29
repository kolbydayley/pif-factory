from __future__ import annotations

"""Fresh balanced support-first diagnostic for the corrected v142 field protocol."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v108_layered_diagnostic as v108
from . import app_server_judge_v5_calibration_v142_corrected_field_reference_owner as v142
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
    _field_contracts,
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v99_refined_luna_diagnostic import (
    _general_requested_field_value,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _validate_usage,
)
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .app_server_llm_judge import validate_app_server_output_schema_subset
from .util import now_iso, sha256_text


V143_SUPPORT_INPUT_VERSION = "pif_app_server_judge_v5_4_v143_support_input_v1"
V143_SUPPORT_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v143_support_output_v1"
V143_FIELD_INPUT_VERSION = "pif_app_server_judge_v5_4_v143_field_input_v1"
V143_TRUTH_VERSION = "pif_app_server_judge_v5_4_v143_truth_v1"
V143_SELECTION_VERSION = "pif_app_server_judge_v5_4_v143_selection_v1"
V143_SPEC_VERSION = "pif_app_server_judge_v5_4_v143_spec_v1"
V143_SCORE_VERSION = "pif_app_server_judge_v5_4_v143_score_v1"
V143_FAILURE_VERSION = "pif_app_server_judge_v5_4_v143_failure_v1"
V143_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v143_terminal_v1"
V143_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V143_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V143_PHASE_ID = "judge_v5_4_v143_corrected_layered_diagnostic"

SUPPORT_MODEL = "gpt-5.6-sol"
FIELD_MODEL = "gpt-5.5"
EFFORT = "high"
SUPPORT_COUNT = 12
FIELD_TASK_COUNT = 30
FIELD_TASKS_PER_TURN = 5
SUPPORT_TURNS = ("corrected_support_primary", "corrected_support_order_canary")
FIELD_PRIMARY_TURNS = tuple(f"corrected_field_primary_{index:02d}" for index in range(6))
FIELD_CANARY_TURNS = tuple(f"corrected_field_order_canary_{index:02d}" for index in range(6))
TURN_NAMES = SUPPORT_TURNS + FIELD_PRIMARY_TURNS + FIELD_CANARY_TURNS
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    v142.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v143-corrected-layered-diagnostic"
).resolve()


class JudgeV5CalibrationV143Error(RuntimeError):
    """The v143 corrected layered diagnostic contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v142() -> dict[str, Any]:
    root = v142.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "corrected-field-reference-owner-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "corrected-field-owner-score.json",
        "primary": root / "corrected-field-primary.private.json",
        "canary": root / "corrected-field-order-canary.private.json",
        "truth": root / "corrected-field-owner-truth.private.json",
        "selection": root / "selection-audit.json",
        "rubric": root / "field-rubric-v142.json",
        "reference": root / "calibration-truth-v12-corrected-field-owner.private.json",
        "v140_truth": root / "patched-v140-truth-audit-only.private.json",
        "v140_score": root / "old-v140-rescore-audit-only.json",
    }
    values = {name: _load_json(path, f"v142 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v142_reference_v12_frozen_corrected_protocol_diagnostic_authorized"
        or terminal.get("development_terminal_reason")
        != "v142_corrected_field_owner_passed"
        or terminal.get("reference_frozen") is not True
        or terminal.get("corrected_protocol_diagnostic_authorized") is not True
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 222453
        or score.get("passed") is not True
        or score.get("failed_checks") != []
        or score.get("metrics", {}).get("control_exact_count") != 5
        or score.get("metrics", {}).get("order_canary_exact_count") != 14
        or score.get("metrics", {}).get("owner_abstention_count") != 0
        or score.get("metrics", {}).get("owner_current_reference_agreement_count") != 7
        or spec.get("model") != "gpt-5.5"
        or spec.get("turn_plan") != list(v142.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV143Error("v142 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV143Error("v142 runtime record drifted")
    for name, key in {
        "score": "score",
        "primary": "primary_output",
        "canary": "canary_output",
        "reference": "reference",
        "v140_truth": "patched_truth_audit_only",
        "v140_score": "old_v140_rescore_audit_only",
    }.items():
        record = terminal.get(key)
        if record != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV143Error(f"v142 {name} record drifted")
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
                raise JudgeV5CalibrationV143Error("v142 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v142 sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV143Error("v142 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "v141": v142._validate_v141(),
    }


def _validate_source() -> dict[str, Any]:
    source = v108._validate_predecessors()
    pointwise = source["values"]["v106_pointwise_input"]
    units = pointwise.get("units")
    if not isinstance(units, list) or len(units) != 182:
        raise JudgeV5CalibrationV143Error("v106 pointwise source coverage drifted")
    if source["records"]["v106_pointwise_input"] != _record(
        source["paths"]["v106_pointwise_input"]
    ):
        raise JudgeV5CalibrationV143Error("v106 pointwise source record drifted")
    return source


def _field_task_id(case_id: str, witness_id: str, field: str) -> str:
    return "field_" + sha256_text(f"v143|{case_id}|{witness_id}|{field}")[:24]


def _support_input(units: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": V143_SUPPORT_INPUT_VERSION,
        "unit_count": len(units),
        "units": [
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "proposition": deepcopy(row["proposition"]),
                "source_excerpt": row["source_excerpt"],
            }
            for row in units
        ],
        "side_labels_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }


def _field_input(tasks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": V143_FIELD_INPUT_VERSION,
        "task_count": len(tasks),
        "tasks": [deepcopy(row) for row in tasks],
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "disagreement_reasons_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }


def build_v143_inputs(
    predecessor: Mapping[str, Any], source: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    reference = predecessor["values"]["reference"]
    v140_chain = predecessor["v141"]["v140"]
    v139_source = v140_chain["v139"]
    v133_source = (
        v139_source["v138"]["v137"]["v136"]["v134"]["v133"]
    )
    excluded = set(v133_source["values"]["truth"]["cases"])
    excluded.update(v139_source["values"]["selection"]["selected"])
    candidates = sorted(set(reference["cases"]) - excluded)
    if len(excluded) != 30 or len(candidates) != 36:
        raise JudgeV5CalibrationV143Error("v143 fresh case partition drifted")
    source_units = {
        str(row["witness_id"]): row
        for row in source["values"]["v106_pointwise_input"]["units"]
        if row["case_id"] in candidates
    }
    identities = [
        (case_id, witness_id)
        for case_id in candidates
        for witness_id in reference["cases"][case_id]["proposition"]
    ]
    if set(witness_id for _, witness_id in identities) != set(source_units):
        raise JudgeV5CalibrationV143Error("v143 source witness coverage drifted")

    support_selected = []
    support_used_cases = set()
    for status, count in (("unsupported", 6), ("supported", 6)):
        pool = [
            (case_id, witness_id)
            for case_id, witness_id in identities
            if reference["cases"][case_id]["proposition"][witness_id] == status
        ]
        pool.sort(
            key=lambda row: sha256_text(
                f"v143|support|{status}|{row[0]}|{row[1]}"
            )
        )
        chosen = []
        for row in pool:
            if row[0] in support_used_cases:
                continue
            chosen.append(row)
            support_used_cases.add(row[0])
            if len(chosen) == count:
                break
        if len(chosen) != count:
            raise JudgeV5CalibrationV143Error("v143 balanced support selection drifted")
        support_selected.extend(chosen)
    support_selected.sort(key=lambda row: sha256_text(f"v143|support-order|{row[0]}|{row[1]}"))
    support_units = [source_units[witness_id] for _, witness_id in support_selected]
    support_primary = _support_input(support_units)
    support_canary = _support_input(list(reversed(support_units)))

    field_tasks = []
    field_truth = []
    contracts = _field_contracts()
    contracts.update(
        {
            field: {"field": field, **deepcopy(contract)}
            for field, contract in v142.FIELD_RULES.items()
        }
    )
    for field in CHECKLIST_FIELDS:
        for expected_status in ("incorrect", "correct"):
            pool = [
                (case_id, witness_id)
                for case_id, witness_id in identities
                if (
                    field in reference["cases"][case_id]["field_issues"][witness_id]
                )
                == (expected_status == "incorrect")
            ]
            pool.sort(
                key=lambda row: sha256_text(
                    f"v143|field|{field}|{expected_status}|{row[0]}|{row[1]}"
                )
            )
            if not pool:
                raise JudgeV5CalibrationV143Error("v143 field polarity slot is empty")
            case_id, witness_id = pool[0]
            unit = source_units[witness_id]
            event = compact_empty_event_fields(deepcopy(unit["structured_event"]))
            task_id = _field_task_id(case_id, witness_id, field)
            field_tasks.append(
                    {
                        "task_id": task_id,
                        "field": field,
                        "field_contract": deepcopy(contracts[field]),
                    "requested_field_value": _general_requested_field_value(field, event),
                    "source_excerpt": unit["source_excerpt"],
                    "structured_event": event,
                }
            )
            field_truth.append(
                {
                    "task_id": task_id,
                    "case_id": case_id,
                    "witness_id": witness_id,
                    "field": field,
                    "expected_status": expected_status,
                }
            )
    field_tasks.sort(key=lambda row: sha256_text(f"v143|task-order|{row['task_id']}"))
    if len(field_tasks) != FIELD_TASK_COUNT or len({row["task_id"] for row in field_tasks}) != 30:
        raise JudgeV5CalibrationV143Error("v143 field task coverage drifted")
    shards = [
        _field_input(field_tasks[index : index + FIELD_TASKS_PER_TURN])
        for index in range(0, FIELD_TASK_COUNT, FIELD_TASKS_PER_TURN)
    ]
    if len(shards) != 6 or any(shard["task_count"] != 5 for shard in shards):
        raise JudgeV5CalibrationV143Error("v143 field shard coverage drifted")

    truth = {
        "schema_version": V143_TRUTH_VERSION,
        "support": [
            {
                "case_id": case_id,
                "witness_id": witness_id,
                "expected_status": reference["cases"][case_id]["proposition"][witness_id],
            }
            for case_id, witness_id in support_selected
        ],
        "field_tasks": sorted(field_truth, key=lambda row: row["task_id"]),
    }
    selection = {
        "schema_version": V143_SELECTION_VERSION,
        "created_at": now_iso(),
        "excluded_recent_case_count": len(excluded),
        "candidate_case_count": len(candidates),
        "support_unit_count": SUPPORT_COUNT,
        "support_status_counts": {"supported": 6, "unsupported": 6},
        "support_distinct_case_count": len({row[0] for row in support_selected}),
        "field_task_count": FIELD_TASK_COUNT,
        "field_status_counts": {"correct": 15, "incorrect": 15},
        "field_enum_count": len(CHECKLIST_FIELDS),
        "field_tasks_per_turn": FIELD_TASKS_PER_TURN,
        "primary_field_turn_count": 6,
        "order_canary_field_turn_count": 6,
        "canary_marker_in_model_input": False,
        "canary_membership_identical": True,
        "canary_order_reversed": True,
        "selection_uses_source_text": False,
        "selection_uses_only_frozen_reference_labels_provenance_and_opaque_ids": True,
        "model_outputs_used_for_selection": False,
        "semantic_pruning_performed": False,
        "privacy": "opaque_ids_and_aggregate_counts_only",
    }
    return support_primary, support_canary, shards, truth, selection


def support_output_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    units = value.get("units") or []
    case_ids = sorted({str(row["case_id"]) for row in units})
    witness_ids = [str(row["witness_id"]) for row in units]
    row = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "case_id",
            "witness_id",
            "support_status",
            "source_evidence_spans",
            "rationale",
        ],
        "properties": {
            "case_id": {"type": "string", "enum": case_ids},
            "witness_id": {"type": "string", "enum": witness_ids},
            "support_status": {
                "type": "string",
                "enum": ["supported", "unsupported", "abstain"],
            },
            "source_evidence_spans": {
                "type": "array",
                "minItems": 1,
                "maxItems": 4,
                "items": {"type": "string", "minLength": 1, "maxLength": 1000},
            },
            "rationale": {"type": "string", "minLength": 1, "maxLength": 600},
        },
    }
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["units"],
        "properties": {
            "units": {
                "type": "array",
                "minItems": len(units),
                "maxItems": len(units),
                "items": row,
            }
        },
    }
    errors = validate_app_server_output_schema_subset(schema)
    if errors:
        raise JudgeV5CalibrationV143Error("v143 support schema is unsupported")
    return schema


def validate_support_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping) or set(output) != {"units"} or not isinstance(output["units"], list):
        return ["invalid_support_output_root"]
    expected = {
        (str(row["case_id"]), str(row["witness_id"])): row for row in value["units"]
    }
    required = {
        "case_id",
        "witness_id",
        "support_status",
        "source_evidence_spans",
        "rationale",
    }
    errors, seen = [], set()
    for index, row in enumerate(output["units"]):
        if not isinstance(row, Mapping) or set(row) != required:
            errors.append(f"support_{index}_invalid_shape")
            continue
        key = (str(row["case_id"]), str(row["witness_id"]))
        if key not in expected or key in seen:
            errors.append(f"support_{index}_invalid_or_duplicate_id")
            continue
        seen.add(key)
        spans = row["source_evidence_spans"]
        source = expected[key]["source_excerpt"]
        if (
            not isinstance(spans, list)
            or not 1 <= len(spans) <= 4
            or len(spans) != len(set(spans))
            or any(not isinstance(span, str) or not span or len(span) > 1000 or span not in source for span in spans)
        ):
            errors.append(f"support_{index}_evidence_not_exact")
        if row["support_status"] not in {"supported", "unsupported", "abstain"}:
            errors.append(f"support_{index}_invalid_status")
        rationale = row["rationale"]
        if not isinstance(rationale, str) or not rationale or len(rationale) > 600:
            errors.append(f"support_{index}_invalid_rationale")
    if seen != set(expected):
        errors.append("support_coverage_mismatch")
    return errors


def support_base_instructions_v143() -> str:
    return (
        "You are a side-free pointwise proposition-support evaluator. For each opaque witness, "
        "judge only whether every material claim in proposition.claim_text is entailed by that "
        "witness's source_excerpt. Harmless paraphrase and resolved coreference pass. Exact copied "
        "wording alone does not license an unsupported inference. Do not judge actor, speaker, "
        "reported actor, event type, evidence-field quality, or any other structured field in this "
        "pass. Cite one or more exact source substrings for every decision. Abstain only when the "
        "source genuinely cannot determine support. Do not compare witnesses, vote, use confidence, "
        "regex, keywords, overlap, embeddings, prior labels, model identity, or system identity."
    )


def support_prompt_v143(value: Mapping[str, Any]) -> str:
    return (
        "Return one independent support decision for every opaque witness_id. Preserve case_id and "
        "witness_id exactly. Every evidence span must be an exact substring of that unit's "
        "source_excerpt.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def _merge_field_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(decisions) != FIELD_TASK_COUNT or len({row["task_id"] for row in decisions}) != FIELD_TASK_COUNT:
        raise JudgeV5CalibrationV143Error("v143 field output coverage drifted")
    return {"schema_version": V143_FIELD_INPUT_VERSION, "decisions": decisions}


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _f1(tp: int, fp: int, fn: int) -> float:
    denominator = 2 * tp + fp + fn
    return round(2 * tp / denominator, 6) if denominator else 1.0


def score_v143(
    *,
    support: Mapping[str, Any],
    support_canary: Mapping[str, Any],
    fields: Mapping[str, Any],
    field_canary: Mapping[str, Any],
    truth: Mapping[str, Any],
) -> dict[str, Any]:
    expected_support = {str(row["witness_id"]): row for row in truth["support"]}
    observed_support = {str(row["witness_id"]): row for row in support["units"]}
    repeated_support = {str(row["witness_id"]): row for row in support_canary["units"]}
    expected_fields = {str(row["task_id"]): row for row in truth["field_tasks"]}
    observed_fields = {str(row["task_id"]): row for row in fields["decisions"]}
    repeated_fields = {str(row["task_id"]): row for row in field_canary["decisions"]}
    if (
        set(observed_support) != set(expected_support)
        or set(repeated_support) != set(expected_support)
        or set(observed_fields) != set(expected_fields)
        or set(repeated_fields) != set(expected_fields)
    ):
        raise JudgeV5CalibrationV143Error("v143 score coverage drifted")
    tp = fn = tn = fp = support_abstentions = 0
    for witness_id, expected in expected_support.items():
        status = observed_support[witness_id]["support_status"]
        support_abstentions += int(status == "abstain")
        if expected["expected_status"] == "supported" and status == "supported":
            tp += 1
        elif expected["expected_status"] == "supported":
            fn += 1
        elif status == "unsupported":
            tn += 1
        else:
            fp += 1
    support_canary_exact = sum(
        observed_support[witness_id]["support_status"]
        == repeated_support[witness_id]["support_status"]
        for witness_id in expected_support
    )
    field_correct = field_tp = field_fp = field_fn = field_abstentions = 0
    for task_id, expected in expected_fields.items():
        status = observed_fields[task_id]["field_status"]
        field_abstentions += int(status == "abstain")
        field_correct += int(status == expected["expected_status"])
        wanted = expected["expected_status"] == "incorrect"
        got = status == "incorrect"
        if wanted and got:
            field_tp += 1
        elif wanted:
            field_fn += 1
        elif got:
            field_fp += 1
    field_canary_exact = sum(
        observed_fields[task_id]["field_status"]
        == repeated_fields[task_id]["field_status"]
        for task_id in expected_fields
    )
    evidence_complete = sum(
        bool(row["source_evidence_spans"])
        for row in list(observed_support.values()) + list(repeated_support.values())
    ) + sum(
        bool(row["source_evidence_spans"])
        for row in list(observed_fields.values()) + list(repeated_fields.values())
    )
    metrics = {
        "support_witness_count": len(expected_support),
        "support_sensitivity": _ratio(tp, tp + fn),
        "support_specificity": _ratio(tn, tn + fp),
        "support_abstention_count": support_abstentions,
        "support_canary_decision_count": len(expected_support),
        "support_canary_exact_count": support_canary_exact,
        "field_decision_count": len(expected_fields),
        "field_decision_accuracy": _ratio(field_correct, len(expected_fields)),
        "structured_field_accuracy": _ratio(field_correct, len(expected_fields)),
        "pointwise_field_issue_f1": _f1(field_tp, field_fp, field_fn),
        "field_abstention_count": field_abstentions,
        "field_canary_decision_count": len(expected_fields),
        "field_canary_exact_count": field_canary_exact,
        "evidence_complete_count": evidence_complete,
    }
    checks = {
        "support_sensitivity": metrics["support_sensitivity"] >= 0.95,
        "support_specificity": metrics["support_specificity"] >= 0.95,
        "support_abstention_count": support_abstentions == 0,
        "support_order_canary_exact_rate": support_canary_exact == len(expected_support),
        "field_decision_accuracy": metrics["field_decision_accuracy"] >= 0.95,
        "structured_field_accuracy": metrics["structured_field_accuracy"] >= 0.95,
        "pointwise_field_issue_f1": metrics["pointwise_field_issue_f1"] >= 0.95,
        "field_abstention_count": field_abstentions == 0,
        "field_order_canary_exact_rate": field_canary_exact == len(expected_fields),
        "evidence_complete_rate": evidence_complete == 2 * (len(expected_support) + len(expected_fields)),
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V143_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": metrics,
        "corrected_alignment_diagnostic_authorized": passed,
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
        "schema_version": V143_CAPACITY_AUDIT_VERSION,
        "phase_id": V143_PHASE_ID,
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
        "schema_version": V143_CAPACITY_POLICY_VERSION,
        "phase_id": V143_PHASE_ID,
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


def freeze_v143(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v143 terminal")}
    predecessor = _validate_v142()
    source = _validate_source()
    support_primary, support_canary, field_shards, truth, selection = build_v143_inputs(
        predecessor, source
    )
    truth_path = root / "corrected-layered-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    turns = []
    for turn_name, role, value in zip(
        SUPPORT_TURNS,
        ("support_primary", "support_order_canary"),
        (support_primary, support_canary),
        strict=True,
    ):
        prompt, schema = support_prompt_v143(value), support_output_schema(value)
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
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
    for turn_name, value in zip(FIELD_PRIMARY_TURNS, field_shards, strict=True):
        prompt, schema = v142.build_prompt_v142(value), output_schema(value)
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "field_primary",
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    for turn_name, original in zip(FIELD_CANARY_TURNS, field_shards, strict=True):
        value = _field_input(list(reversed(original["tasks"])))
        prompt, schema = v142.build_prompt_v142(value), output_schema(value)
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "field_order_canary",
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    turns.sort(key=lambda row: TURN_NAMES.index(row["turn_name"]))
    predecessor_records = {
        **{f"v142_{name}": record for name, record in predecessor["records"].items()},
        "v142_attempts": predecessor["attempts"],
        "v106_pointwise_input": source["records"]["v106_pointwise_input"],
        "v133_truth": predecessor["v141"]["v140"]["v139"]["v138"]["v137"]["v136"]["v134"]["v133"]["records"]["truth"],
        "v139_selection": predecessor["v141"]["v140"]["v139"]["records"]["selection"],
    }
    capacity = _build_capacity_policy(root, predecessor_records)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V143_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "support_model": SUPPORT_MODEL,
        "field_model": FIELD_MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "balanced_support_only_then_corrected_field_microtasks_with_unmarked_reversal",
        "support_unit_count": SUPPORT_COUNT,
        "field_task_count": FIELD_TASK_COUNT,
        "maximum_tasks_per_turn": SUPPORT_COUNT,
        "turn_plan": list(TURN_NAMES),
        "canary_marker_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_frozen_support_field_abstention_evidence_and_order_gates",
        "corrected_alignment_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor_records,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v142_corrected_field_reference_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v108_layered_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "truth": _record(truth_path),
            "selection": _record(selection_path),
            "reference": predecessor["records"]["reference"],
            "field_rubric": predecessor["records"]["rubric"],
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
    spec_path = root / "corrected-layered-diagnostic-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v142": predecessor,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v143 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V143_FAILURE_VERSION,
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
        "schema_version": V143_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "corrected_alignment_diagnostic_authorized": False,
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


async def run_v143(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v143 terminal")
    frozen = freeze_v143(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        support = support_canary = None
        field_outputs, field_canary_outputs, sidecars = [], [], []
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
                    base_instructions=(
                        support_base_instructions_v143()
                        if is_support
                        else v142.base_instructions_v142()
                    ),
                    model=SUPPORT_MODEL if is_support else FIELD_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=(
                        turn["value"]["unit_count"]
                        if is_support
                        else turn["value"]["task_count"]
                    ),
                    policy_path=frozen["capacity_policy"],
                    output_validator=(
                        (lambda candidate, item=turn["value"]: validate_support_output(candidate, item))
                        if is_support
                        else (lambda candidate, item=turn["value"]: validate_output(candidate, item))
                    ),
                )
                if turn["turn_role"] == "support_primary":
                    support = output
                elif turn["turn_role"] == "support_order_canary":
                    support_canary = output
                elif turn["turn_role"] == "field_primary":
                    field_outputs.append(output)
                else:
                    field_canary_outputs.append(output)
                sidecars.append(sidecar)
        if support is None or support_canary is None:
            raise JudgeV5CalibrationV143Error("v143 support output coverage drifted")
        fields = _merge_field_outputs(field_outputs)
        field_canary = _merge_field_outputs(field_canary_outputs)
        score = score_v143(
            support=support,
            support_canary=support_canary,
            fields=fields,
            field_canary=field_canary,
            truth=frozen["truth"],
        )
        passed = bool(score["passed"])
        paths = {
            "support": root / "support-output.private.json",
            "support_canary": root / "support-order-canary.private.json",
            "fields": root / "field-output.private.json",
            "field_canary": root / "field-order-canary.private.json",
            "score": root / "corrected-layered-score.json",
        }
        _write_immutable(paths["support"], support)
        _write_immutable(paths["support_canary"], support_canary)
        _write_immutable(paths["fields"], fields)
        _write_immutable(paths["field_canary"], field_canary)
        _write_immutable(paths["score"], score)
        accounting = _aggregate_usage(sidecars)
        terminal = {
            "schema_version": V143_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v143_corrected_layered_diagnostic_passed_alignment_diagnostic_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v143_corrected_support_field_gates_passed"
                if passed
                else "v143_corrected_support_field_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reference_frozen": True,
            "corrected_alignment_diagnostic_authorized": passed,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(paths["score"]),
            "support_output": _record(paths["support"]),
            "support_canary": _record(paths["support_canary"]),
            "field_output": _record(paths["fields"]),
            "field_canary": _record(paths["field_canary"]),
            "reference": frozen["v142"]["records"]["reference"],
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v142_usage": frozen["v142"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v143 corrected layered diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v143(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "corrected_alignment_diagnostic_authorized": terminal.get(
                    "corrected_alignment_diagnostic_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
