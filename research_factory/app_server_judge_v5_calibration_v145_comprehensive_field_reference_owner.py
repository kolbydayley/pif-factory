from __future__ import annotations

"""Independent Sol owner for the observable v144 field/reference disputes."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v142_corrected_field_reference_owner as v142
from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_calibration_v144_same_field_diagnostic as v144
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
from .app_server_judge_v5_calibration_v126_singleton_field_owner import (
    _validate_v125,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _validate_usage,
)
from .app_server_judge_v5_fixture import DEFAULT_FIXTURE_AUDIT_PATH
from .util import now_iso, sha256_text


V145_INPUT_VERSION = "pif_app_server_judge_v5_4_v145_field_owner_input_v1"
V145_TRUTH_VERSION = "pif_app_server_judge_v5_4_v145_field_owner_truth_v1"
V145_RUBRIC_VERSION = "pif_app_server_judge_v5_4_v145_comprehensive_rubric_v1"
V145_SELECTION_VERSION = "pif_app_server_judge_v5_4_v145_selection_v1"
V145_SPEC_VERSION = "pif_app_server_judge_v5_4_v145_spec_v1"
V145_SCORE_VERSION = "pif_app_server_judge_v5_4_v145_score_v1"
V145_REFERENCE_VERSION = (
    "pif_app_server_judge_v5_4_calibration_truth_v13_comprehensive_field_owner_frozen"
)
V145_FAILURE_VERSION = "pif_app_server_judge_v5_4_v145_failure_v1"
V145_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v145_terminal_v1"
V145_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V145_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V145_PHASE_ID = "judge_v5_4_v145_comprehensive_field_reference_owner"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
FIELDS = (
    "actor",
    "attribution",
    "certainty",
    "event_type",
    "evidence",
    "metric",
    "negation",
    "reported_actor",
    "target",
    "temporal_horizon",
    "unsupported_inference",
)
PRIMARY_TURNS = tuple(f"sol_owner_{field}_primary" for field in FIELDS)
CANARY_TURNS = tuple(f"sol_owner_{field}_order_canary" for field in FIELDS)
TURN_NAMES = PRIMARY_TURNS + CANARY_TURNS
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    v144.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v145-comprehensive-field-reference-owner"
).resolve()


class JudgeV5CalibrationV145Error(RuntimeError):
    """The v145 comprehensive field-owner contract cannot be preserved."""


FIELD_RULES_V145 = {
    "actor": {
        "definition": "The entity that performs the action, has the capability, or holds the stance in the event.",
        "decision_rule": "Judge the actor after repairing claim wording and attribution. A speaker, reported actor, target, or merely mentioned entity is not automatically the actor.",
    },
    "attribution": {
        "definition": "The reporting or source relationship that attaches the proposition to its origin.",
        "decision_rule": "Judge the attribution relation independently from actor, direct speaker, and reported actor. A name mismatch in one of those fields is not by itself an attribution error.",
    },
    "causal_mechanism": {
        "definition": "The asserted mechanism by which one condition produces or changes another.",
        "decision_rule": "Require an asserted causal link and its mechanism; temporal sequence, correlation, or co-occurrence alone is not causation.",
    },
    "certainty": {
        "definition": "The proposition's expressed epistemic strength, such as definite, probable, possible, or uncertain.",
        "decision_rule": "Judge explicit or clearly entailed modality after repairing other content. Do not infer high certainty from a bare assertion or lower certainty merely because another field is unsupported.",
    },
    "event_boundary": deepcopy(v142.FIELD_RULES["event_boundary"]),
    "event_type": {
        "definition": "The semantic category of the repaired event, such as forecast, capability claim, adoption report, or stance position.",
        "decision_rule": "Classify the event after hypothetically correcting other fields. Wrong claim details do not change event type unless the source describes a materially different kind of event.",
    },
    "evidence": deepcopy(v142.FIELD_RULES["evidence"]),
    "metric": deepcopy(v142.FIELD_RULES["metric"]),
    "negation": {
        "definition": "Whether the requested proposition is affirmed or negated.",
        "decision_rule": "Judge proposition polarity only. Negative sentiment, absence, limitation, or negation of an adjacent proposition does not negate the requested proposition.",
    },
    "reported_actor": deepcopy(v142.FIELD_RULES["reported_actor"]),
    "speaker": {
        "definition": "The direct speaker or source voice that utters the proposition in the excerpt.",
        "decision_rule": "Keep direct speaker independent from actor and reported actor. A quoted or reported third party is not the direct speaker unless the excerpt makes that role explicit.",
    },
    "stance": deepcopy(v142.FIELD_RULES["stance"]),
    "target": {
        "definition": "The entity, concept, action, or outcome toward which the event is directed.",
        "decision_rule": "Judge the target after repairing actor and claim wording. Do not substitute an instrument, actor, metric scope, or adjacent topic for the event's target.",
    },
    "temporal_horizon": {
        "definition": "When the proposition applies, such as past, present, near future, or long-term future.",
        "decision_rule": "Judge the proposition's applicability time independently from certainty, duration, and any adjacent proposition's time reference.",
    },
    "unsupported_inference": {
        "definition": "A material assertion in claim_text that the source does not entail.",
        "decision_rule": "Incorrect exactly when claim_text adds at least one material unsupported assertion; correct when every material claim is entailed. Do not import errors from other structured fields.",
    },
}


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v144() -> dict[str, Any]:
    root = v144.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "same-field-diagnostic-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "same-field-layered-score.json",
        "fields": root / "same-field-output.private.json",
        "field_canary": root / "same-field-order-canary.private.json",
        "selection": root / "selection-audit.json",
        "taxonomy": root / "v143-field-error-taxonomy.json",
    }
    values = {name: _load_json(path, f"v144 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v144_same_field_quality_gate_not_passed"
        or terminal.get("corrected_alignment_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 661257
        or score.get("passed") is not False
        or score.get("failed_checks")
        != [
            "field_decision_accuracy",
            "field_order_canary_exact_rate",
            "pointwise_field_issue_f1",
            "structured_field_accuracy",
        ]
        or score.get("metrics", {}).get("support_sensitivity") != 1.0
        or score.get("metrics", {}).get("support_specificity") != 1.0
        or score.get("metrics", {}).get("field_decision_accuracy") != 0.566667
        or score.get("metrics", {}).get("pointwise_field_issue_f1") != 0.551724
        or score.get("metrics", {}).get("field_canary_exact_count") != 26
        or spec.get("model") != "gpt-5.5"
        or spec.get("turn_plan") != list(v144.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV145Error("v144 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV145Error("v144 runtime record drifted")
    for name, key in {
        "score": "score",
        "fields": "field_output",
        "field_canary": "field_canary",
    }.items():
        record = terminal.get(key)
        if record != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV145Error(f"v144 {name} record drifted")
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
                raise JudgeV5CalibrationV145Error("v144 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v144 sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV145Error("v144 usage aggregate drifted")
    predecessor_v143 = v144._validate_v143()
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "v143": predecessor_v143,
        "input_tasks": predecessor_v143["input_tasks"],
    }


def field_rubric_v145() -> dict[str, Any]:
    return {
        "schema_version": V145_RUBRIC_VERSION,
        "source_fixture_audit": _record(DEFAULT_FIXTURE_AUDIT_PATH),
        "field_independence_rule": "Judge only the requested field after hypothetically correcting every other field.",
        "requested_presence_rule": "requested_field_value identifies the value and presence under review; structural sentinels that mean absence are not semantic values.",
        "minimal_root_rule": "Mark only independently wrong truth-conditional root fields, not downstream consequences of another field's error.",
        "fields": deepcopy(FIELD_RULES_V145),
        "rubric_changes_quality_gates": False,
        "rubric_changes_source_evidence_contract": False,
        "semantic_pruning_performed": False,
    }


def _task_id(role: str, source_task_id: str) -> str:
    return role + "_" + sha256_text(f"v145|{role}|{source_task_id}")[:24]


def _field_input(field: str, tasks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": V145_INPUT_VERSION,
        "requested_field": field,
        "task_count": len(tasks),
        "tasks": [deepcopy(row) for row in tasks],
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "disagreement_reasons_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }


def build_v145_inputs(
    predecessor: Mapping[str, Any], controls_source: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    truth_rows = {
        row["task_id"]: row
        for row in predecessor["v143"]["values"]["truth"]["field_tasks"]
    }
    primary = {
        row["task_id"]: row for row in predecessor["values"]["fields"]["decisions"]
    }
    canary = {
        row["task_id"]: row
        for row in predecessor["values"]["field_canary"]["decisions"]
    }
    triggers = [
        row
        for task_id, row in truth_rows.items()
        if primary[task_id]["field_status"] != row["expected_status"]
        or primary[task_id]["field_status"] != canary[task_id]["field_status"]
    ]
    counts = {field: sum(row["field"] == field for row in triggers) for field in FIELDS}
    if len(triggers) != 14 or counts != {
        "actor": 1,
        "attribution": 1,
        "certainty": 2,
        "event_type": 1,
        "evidence": 1,
        "metric": 1,
        "negation": 2,
        "reported_actor": 1,
        "target": 1,
        "temporal_horizon": 1,
        "unsupported_inference": 2,
    }:
        raise JudgeV5CalibrationV145Error("v145 observable trigger coverage drifted")

    v121 = controls_source["v124"]["v122"]["v121"]
    control_truth = v121["values"]["truth"]["tasks"]
    control_input = {
        row["task_id"]: row for row in controls_source["v124"]["v122"]["v121_input"]["tasks"]
    }
    control_primary = {
        row["task_id"]: row
        for row in controls_source["v124"]["v122"]["values"]["primary"]["decisions"]
    }
    v143_primary = {
        row["task_id"]: row
        for row in predecessor["v143"]["values"]["fields"]["decisions"]
    }
    v143_canary = {
        row["task_id"]: row
        for row in predecessor["v143"]["values"]["field_canary"]["decisions"]
    }
    trigger_ids = {row["task_id"] for row in triggers}
    by_field: dict[str, list[dict[str, Any]]] = {field: [] for field in FIELDS}
    owner_truth = []
    for field in FIELDS:
        candidates = [
            {
                "source_task_id": row["task_id"],
                "expected_status": row["control_expected_status"],
                "task": control_input[row["task_id"]],
                "source_lineage": "v121_validated_matched_control",
            }
            for row in control_truth
            if row["role"] == "matched_control"
            and row["field"] == field
            and control_primary[row["task_id"]]["field_status"]
            == row["control_expected_status"]
            and control_primary[row["task_id"]]["field_status"] != "abstain"
        ]
        if not candidates and field == "event_type":
            candidates = [
                {
                    "source_task_id": task_id,
                    "expected_status": row["expected_status"],
                    "task": predecessor["input_tasks"][task_id],
                    "source_lineage": "v143_v144_unanimous_order_stable_control",
                }
                for task_id, row in truth_rows.items()
                if task_id not in trigger_ids
                and row["field"] == field
                and row["expected_status"] != "abstain"
                and v143_primary[task_id]["field_status"] == row["expected_status"]
                and v143_canary[task_id]["field_status"] == row["expected_status"]
                and primary[task_id]["field_status"] == row["expected_status"]
                and canary[task_id]["field_status"] == row["expected_status"]
            ]
        candidates.sort(
            key=lambda row: sha256_text(
                f"v145|control|{row['source_lineage']}|{row['source_task_id']}"
            )
        )
        if not candidates:
            raise JudgeV5CalibrationV145Error("v145 control coverage drifted")
        source = candidates[0]
        task = deepcopy(source["task"])
        task_id = _task_id("control", source["source_task_id"])
        task["task_id"] = task_id
        task["field_contract"] = {"field": field, **deepcopy(FIELD_RULES_V145[field])}
        by_field[field].append(task)
        owner_truth.append(
            {
                "task_id": task_id,
                "role": "control",
                "field": field,
                "source_task_id": source["source_task_id"],
                "control_source_lineage": source["source_lineage"],
                "control_expected_status": source["expected_status"],
            }
        )
    for trigger in sorted(
        triggers,
        key=lambda row: sha256_text(
            f"v145|owner|{row['field']}|{row['case_id']}|{row['witness_id']}"
        ),
    ):
        source_task_id = trigger["task_id"]
        task = deepcopy(predecessor["input_tasks"][source_task_id])
        task_id = _task_id("owner", source_task_id)
        task["task_id"] = task_id
        task["field_contract"] = {
            "field": trigger["field"],
            **deepcopy(FIELD_RULES_V145[trigger["field"]]),
        }
        by_field[trigger["field"]].append(task)
        owner_truth.append(
            {
                "task_id": task_id,
                "role": "owner",
                "field": trigger["field"],
                "source_v143_task_id": source_task_id,
                "case_id": trigger["case_id"],
                "witness_id": trigger["witness_id"],
                "current_status": trigger["expected_status"],
                "v144_primary_status": primary[source_task_id]["field_status"],
                "v144_canary_status": canary[source_task_id]["field_status"],
            }
        )
    rows = []
    for field, primary_turn, canary_turn in zip(
        FIELDS, PRIMARY_TURNS, CANARY_TURNS, strict=True
    ):
        tasks = sorted(by_field[field], key=lambda row: str(row["task_id"]))
        rows.append(
            {
                "turn_name": primary_turn,
                "turn_role": "field_primary",
                "field": field,
                "value": _field_input(field, tasks),
            }
        )
        rows.append(
            {
                "turn_name": canary_turn,
                "turn_role": "field_order_canary",
                "field": field,
                "value": _field_input(field, list(reversed(tasks))),
            }
        )
    rows.sort(key=lambda row: TURN_NAMES.index(row["turn_name"]))
    truth = {
        "schema_version": V145_TRUTH_VERSION,
        "task_count": 25,
        "control_count": 11,
        "owner_count": 14,
        "tasks": sorted(owner_truth, key=lambda row: str(row["task_id"])),
    }
    selection = {
        "schema_version": V145_SELECTION_VERSION,
        "created_at": now_iso(),
        "observable_trigger_count": 14,
        "trigger_field_counts": counts,
        "control_count": 11,
        "field_turn_count": 11,
        "order_canary_turn_count": 11,
        "maximum_tasks_per_turn": 3,
        "canary_marker_in_model_input": False,
        "canary_membership_identical": True,
        "canary_order_reversed": True,
        "selection_uses_source_text": False,
        "selection_uses_only_observable_v144_disagreement_control_status_and_opaque_ids": True,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
        "semantic_pruning_performed": False,
        "privacy": "private_source_event_output_aggregate_reports_only",
    }
    return rows, truth, selection


def score_v145(
    *, primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {str(row["task_id"]): row for row in truth["tasks"]}
    observed = {str(row["task_id"]): row for row in primary["decisions"]}
    repeated = {str(row["task_id"]): row for row in canary["decisions"]}
    if set(observed) != set(expected) or set(repeated) != set(expected):
        raise JudgeV5CalibrationV145Error("v145 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "control"]
    owners = [row for row in expected.values() if row["role"] == "owner"]
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    owner_abstentions = sum(
        observed[row["task_id"]]["field_status"] == "abstain" for row in owners
    )
    order_exact = sum(
        observed[task_id]["field_status"] == repeated[task_id]["field_status"]
        for task_id in expected
    )
    evidence_complete = sum(
        bool(row["source_evidence_spans"])
        for row in list(observed.values()) + list(repeated.values())
    )
    current_agreement = sum(
        observed[row["task_id"]]["field_status"] == row["current_status"]
        for row in owners
    )
    checks = {
        "control_exact_rate": control_exact == len(controls),
        "owner_abstention_count": owner_abstentions == 0,
        "order_canary_exact_rate": order_exact == len(expected),
        "evidence_complete_rate": evidence_complete == 2 * len(expected),
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V145_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": len(expected),
            "control_count": len(controls),
            "control_exact_count": control_exact,
            "owner_count": len(owners),
            "owner_abstention_count": owner_abstentions,
            "owner_current_reference_agreement_count": current_agreement,
            "order_canary_decision_count": len(expected),
            "order_canary_exact_count": order_exact,
            "evidence_complete_count": evidence_complete,
        },
        "reference_patch_authorized": passed,
        "corrected_field_diagnostic_authorized": passed,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _patch_truth_and_reference(
    *,
    current_truth: Mapping[str, Any],
    current_reference: Mapping[str, Any],
    owner_truth: Mapping[str, Any],
    primary: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    truth = deepcopy(current_truth)
    reference = deepcopy(current_reference)
    observed = {str(row["task_id"]): row for row in primary["decisions"]}
    task_map = {str(row["task_id"]): row for row in truth["field_tasks"]}
    if not isinstance(reference.get("cases"), dict):
        raise JudgeV5CalibrationV145Error("v145 reference cases are unavailable")
    changed = 0
    for row in owner_truth["tasks"]:
        if row["role"] != "owner":
            continue
        status = observed[row["task_id"]]["field_status"]
        if status == "abstain":
            raise JudgeV5CalibrationV145Error("v145 cannot patch an abstaining owner")
        target = task_map[row["source_v143_task_id"]]
        before = target["expected_status"]
        target["expected_status"] = status
        case = reference["cases"][row["case_id"]]
        issues = set(case["field_issues"][row["witness_id"]])
        if status == "incorrect":
            issues.add(row["field"])
        else:
            issues.discard(row["field"])
        case["field_issues"][row["witness_id"]] = [
            field for field in CHECKLIST_FIELDS if field in issues
        ]
        case["structured_fields"][row["witness_id"]] = (
            "incorrect" if issues else "correct"
        )
        changed += int(before != status)
    truth["schema_version"] = V145_TRUTH_VERSION
    truth["v145_owner_task_count"] = 14
    truth["v145_reference_change_count"] = changed
    reference["schema_version"] = V145_REFERENCE_VERSION
    reference["reference_version"] = "fixture_reference_v13_comprehensive_field_owner_frozen"
    reference["v145_owner_task_count"] = 14
    reference["v145_reference_change_count"] = changed
    reference["v145_owner_basis"] = (
        "fresh_sol_same_field_owner_with_controls_and_unmarked_reversed_permutation"
    )
    return truth, reference


def base_instructions_v145() -> str:
    return (
        "You are the final neutral reference owner for a blinded source-to-field audit. Judge only "
        "the requested field under its supplied field_contract. Mentally repair every other event "
        "field first. Mark incorrect only if the requested field remains independently wrong after "
        "all other fields are corrected; do not mark downstream consequences as extra root errors. "
        "requested_field_value identifies the value and presence under review, and structural "
        "sentinels that mean absence are not semantic values. Cite exact source substrings. Abstain "
        "only when the source genuinely cannot determine the requested field. Do not compare tasks, "
        "vote, use confidence, regex, keywords, overlap, embeddings, prior labels, model identity, "
        "or system identity."
    )


def build_prompt_v145(value: Mapping[str, Any]) -> str:
    return (
        "Return one independent final decision for every opaque task_id. All tasks request the same "
        "field. The first evidence span must directly support the requested field decision and every "
        "span must be an exact substring of that task's source_excerpt. Do not emit whole-event "
        "verdicts.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def _merge_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(decisions) != 25 or len({row["task_id"] for row in decisions}) != 25:
        raise JudgeV5CalibrationV145Error("v145 output coverage drifted")
    return {"schema_version": V145_INPUT_VERSION, "decisions": decisions}


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V145_CAPACITY_AUDIT_VERSION,
        "phase_id": V145_PHASE_ID,
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
        "schema_version": V145_CAPACITY_POLICY_VERSION,
        "phase_id": V145_PHASE_ID,
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


def freeze_v145(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v145 terminal")}
    predecessor = _validate_v144()
    controls_source = _validate_v125()
    rows, truth, selection = build_v145_inputs(predecessor, controls_source)
    truth_path = root / "comprehensive-field-owner-truth.private.json"
    selection_path = root / "selection-audit.json"
    rubric_path = root / "field-rubric-v145.json"
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    _write_immutable(rubric_path, field_rubric_v145())
    turns = []
    for row in rows:
        value = row["value"]
        prompt, schema = build_prompt_v145(value), output_schema(value)
        paths = _freeze_turn_request(
            root=root, turn_name=row["turn_name"], input_value=value, prompt=prompt, schema=schema
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor_records = {
        **{f"v144_{name}": record for name, record in predecessor["records"].items()},
        "v144_attempts": predecessor["attempts"],
        **{
            f"v121_{name}": record
            for name, record in controls_source["v124"]["v122"]["v121"]["records"].items()
        },
        "v121_input": controls_source["v124"]["v122"]["v121_input_record"],
    }
    capacity = _build_capacity_policy(root, predecessor_records)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V145_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "comprehensive_sol_same_field_reference_owner_with_controls_and_reversal",
        "control_count": 11,
        "owner_count": 14,
        "maximum_tasks_per_turn": 3,
        "turn_plan": list(TURN_NAMES),
        "canary_marker_in_model_input": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_controls_all_order_checks_no_abstentions_complete_evidence",
        "old_v144_rescore_is_audit_only": True,
        "reference_patch_authorized": False,
        "corrected_field_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor_records,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v144_same_field_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v143_corrected_layered_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v142_corrected_field_reference_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "truth": _record(truth_path),
            "selection": _record(selection_path),
            "rubric": _record(rubric_path),
            "current_reference": predecessor["v143"]["v142"]["records"]["reference"],
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "role": turn["turn_role"],
                    "field": turn["field"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "comprehensive-field-reference-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v144": predecessor,
        "current_reference": predecessor["v143"]["v142"]["values"]["reference"],
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v145 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V145_FAILURE_VERSION,
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
        "schema_version": V145_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_patch_authorized": False,
        "corrected_field_diagnostic_authorized": False,
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


async def run_v145(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v145 terminal")
    frozen = freeze_v145(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        primary_outputs, canary_outputs, sidecars = [], [], []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=base_instructions_v145(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=turn["value"]["task_count"],
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_output(
                        candidate, item
                    ),
                )
                if turn["turn_role"] == "field_primary":
                    primary_outputs.append(output)
                else:
                    canary_outputs.append(output)
                sidecars.append(sidecar)
        primary = _merge_outputs(primary_outputs)
        canary = _merge_outputs(canary_outputs)
        score = score_v145(primary=primary, canary=canary, truth=frozen["truth"])
        patched_truth, patched_reference = _patch_truth_and_reference(
            current_truth=frozen["v144"]["v143"]["values"]["truth"],
            current_reference=frozen["current_reference"],
            owner_truth=frozen["truth"],
            primary=primary,
        )
        old_v144_rescore = v143.score_v143(
            support=frozen["v144"]["v143"]["values"]["support"],
            support_canary=frozen["v144"]["v143"]["values"]["support_canary"],
            fields=frozen["v144"]["values"]["fields"],
            field_canary=frozen["v144"]["values"]["field_canary"],
            truth=patched_truth,
        )
        passed = bool(score["passed"])
        paths = {
            "primary": root / "comprehensive-field-owner-primary.private.json",
            "canary": root / "comprehensive-field-owner-canary.private.json",
            "score": root / "comprehensive-field-owner-score.json",
            "truth": root / "patched-v143-truth-audit-only.private.json",
            "v144_score": root / "old-v144-rescore-audit-only.json",
            "reference": root / "calibration-truth-v13-comprehensive-field-owner.private.json",
        }
        _write_immutable(paths["primary"], primary)
        _write_immutable(paths["canary"], canary)
        _write_immutable(paths["score"], score)
        _write_immutable(paths["truth"], patched_truth)
        _write_immutable(paths["v144_score"], old_v144_rescore)
        if passed:
            _write_immutable(paths["reference"], patched_reference)
        accounting = _aggregate_usage(sidecars)
        terminal = {
            "schema_version": V145_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v145_reference_v13_frozen_corrected_field_diagnostic_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v145_comprehensive_field_owner_passed"
                if passed
                else "v145_comprehensive_field_owner_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reference_patch_authorized": passed,
            "reference_frozen": passed,
            "corrected_field_diagnostic_authorized": passed,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "old_v144_rescore_is_audit_only": True,
            "score": _record(paths["score"]),
            "primary_output": _record(paths["primary"]),
            "canary_output": _record(paths["canary"]),
            "patched_truth_audit_only": _record(paths["truth"]),
            "old_v144_rescore_audit_only": _record(paths["v144_score"]),
            "reference": _record(paths["reference"]) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "old_v144_audit_metrics": old_v144_rescore["metrics"],
            "predecessor_v144_usage": frozen["v144"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v145 comprehensive field reference owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v145(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal.get("reference_frozen", False),
                "corrected_field_diagnostic_authorized": terminal.get(
                    "corrected_field_diagnostic_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
