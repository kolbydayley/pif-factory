from __future__ import annotations

"""Corrected-rubric field owner for the nine stable v140 reference disputes."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v140_layered_field_diagnostic as v140
from . import app_server_judge_v5_calibration_v141_luna_field_reference_owner as v141
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
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _validate_usage,
)
from .app_server_judge_v5_fixture import DEFAULT_FIXTURE_AUDIT_PATH
from .util import now_iso, sha256_text


V142_INPUT_VERSION = "pif_app_server_judge_v5_4_v142_corrected_field_input_v1"
V142_TRUTH_VERSION = "pif_app_server_judge_v5_4_v142_corrected_field_truth_v1"
V142_RUBRIC_VERSION = "pif_app_server_judge_v5_4_v142_field_rubric_v1"
V142_SELECTION_VERSION = "pif_app_server_judge_v5_4_v142_selection_v1"
V142_SPEC_VERSION = "pif_app_server_judge_v5_4_v142_spec_v1"
V142_SCORE_VERSION = "pif_app_server_judge_v5_4_v142_score_v1"
V142_REFERENCE_VERSION = (
    "pif_app_server_judge_v5_4_calibration_truth_v12_corrected_field_owner_frozen"
)
V142_FAILURE_VERSION = "pif_app_server_judge_v5_4_v142_failure_v1"
V142_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v142_terminal_v1"
V142_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V142_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V142_PHASE_ID = "judge_v5_4_v142_corrected_field_reference_owner"

MODEL = "gpt-5.5"
EFFORT = "high"
FIELDS = v141.FIELDS
PRIMARY_TURNS = tuple(f"gpt55_{field}_primary" for field in FIELDS)
CANARY_TURNS = tuple(f"gpt55_{field}_order_canary" for field in FIELDS)
TURN_NAMES = PRIMARY_TURNS + CANARY_TURNS
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    v141.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v142-corrected-field-reference-owner"
).resolve()


class JudgeV5CalibrationV142Error(RuntimeError):
    """The v142 corrected field-reference contract cannot be preserved."""


FIELD_RULES = {
    "event_boundary": {
        "definition": (
            "The number and semantic scope of source propositions assigned to this event."
        ),
        "decision_rule": (
            "Judge merge or split scope only, after hypothetically correcting every other field. "
            "Wrong content inside one proposition is not by itself an event-boundary error."
        ),
    },
    "evidence": {
        "definition": (
            "An exact source window that materially supports the event after other fields are "
            "hypothetically corrected."
        ),
        "decision_rule": (
            "The window need not be minimal. Adjacent context is allowed when the window still "
            "contains exact material support and does not change or contradict event scope."
        ),
    },
    "metric": {
        "definition": (
            "A material quantitative value, unit, comparator, direction, and measurement scope."
        ),
        "decision_rule": (
            "Judge only asserted quantitative content. The schema sentinel not_applicable asserts "
            "absence and is not itself a metric value."
        ),
    },
    "reported_actor": {
        "definition": (
            "A third party to whom the direct speaker attributes the proposition."
        ),
        "decision_rule": (
            "Keep direct speaker and reported actor independent. Naming the direct speaker in "
            "speaker_name or claim wording does not create a reported actor."
        ),
    },
    "stance": {
        "definition": (
            "An expressed evaluative position toward the target, such as supportive, skeptical, "
            "warning, opposed, or neutral."
        ),
        "decision_rule": (
            "A bare factual assertion or report is neutral unless the source expresses evaluative "
            "polarity toward the target."
        ),
    },
}


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v141() -> dict[str, Any]:
    root = v141.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "luna-field-reference-owner-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "luna-field-owner-score.json",
        "outputs": root / "luna-field-owner-outputs.private.json",
        "truth": root / "luna-field-owner-truth.private.json",
        "selection": root / "selection-audit.json",
        "patched_truth": root / "patched-v140-truth.private.json",
        "v140_score": root / "repaired-v140-score.json",
    }
    values = {name: _load_json(path, f"v141 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v141_luna_field_reference_owner_quality_gate_not_passed"
        or terminal.get("reference_frozen") is not False
        or terminal.get("reference_patch_authorized") is not False
        or terminal.get("expanded_layered_field_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 281058
        or score.get("passed") is not False
        or score.get("failed_checks")
        != ["singleton_control_exact_rate", "singleton_owner_abstention_count"]
        or score.get("metrics", {}).get("control_exact_count") != 4
        or score.get("metrics", {}).get("control_count") != 5
        or score.get("metrics", {}).get("owner_abstention_count") != 2
        or score.get("metrics", {}).get("owner_count") != 9
        or spec.get("model") != "gpt-5.6-luna"
        or spec.get("turn_plan") != list(v141.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV142Error("v141 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV142Error("v141 runtime record drifted")
    for name, key in {
        "score": "owner_score",
        "outputs": "outputs",
        "patched_truth": "patched_truth",
        "v140_score": "repaired_v140_score",
    }.items():
        record = terminal.get(key)
        if record != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV142Error(f"v141 {name} record drifted")
    frozen_turns = {row["turn_name"]: row for row in spec["frozen_inputs"]["turns"]}
    attempts = {}
    input_tasks = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_name in spec["turn_plan"]:
        frozen = frozen_turns[turn_name]
        value = _load_json(Path(frozen["input"]["path"]), "v141 frozen input")
        if value.get("task_count") != 1 or len(value.get("tasks") or []) != 1:
            raise JudgeV5CalibrationV142Error("v141 singleton input drifted")
        task = value["tasks"][0]
        input_tasks[str(task["task_id"])] = task
        turn_root = root / "turns" / turn_name.replace("_", "-")
        records = {}
        for name, filename in {
            "capacity": "capacity.json",
            "sidecar": "sidecar.json",
            "output": "output.private.json",
        }.items():
            path = turn_root / filename
            if not path.is_file():
                raise JudgeV5CalibrationV142Error("v141 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v141 sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if len(input_tasks) != 14 or usage != terminal["usage"]:
        raise JudgeV5CalibrationV142Error("v141 task or usage aggregate drifted")
    predecessor_v140 = v141._validate_v140()
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "input_tasks": input_tasks,
        "v140": predecessor_v140,
        "current_reference": predecessor_v140["v139"]["v138"]["values"]["reference"],
    }


def field_rubric_v142() -> dict[str, Any]:
    return {
        "schema_version": V142_RUBRIC_VERSION,
        "source_fixture_audit": _record(DEFAULT_FIXTURE_AUDIT_PATH),
        "field_independence_rule": (
            "Judge only the requested field after hypothetically correcting every other field."
        ),
        "requested_presence_rule": (
            "requested_field_value identifies the value and presence under review; structural "
            "sentinels that mean absence are not semantic values."
        ),
        "fields": deepcopy(FIELD_RULES),
        "rubric_changes_quality_gates": False,
        "rubric_changes_source_evidence_contract": False,
        "semantic_pruning_performed": False,
    }


def _task_id(role: str, source_task_id: str) -> str:
    return role + "_" + sha256_text(f"v142|{role}|{source_task_id}")[:24]


def _field_input(field: str, tasks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": V142_INPUT_VERSION,
        "task_count": len(tasks),
        "requested_field": field,
        "tasks": [deepcopy(task) for task in tasks],
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "disagreement_reasons_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }


def build_v142_inputs(
    predecessor: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    old_truth = {row["task_id"]: row for row in predecessor["values"]["truth"]["tasks"]}
    by_field: dict[str, list[dict[str, Any]]] = {field: [] for field in FIELDS}
    truth_rows = []
    for old_task_id, old_row in old_truth.items():
        task = deepcopy(predecessor["input_tasks"][old_task_id])
        role = str(old_row["role"])
        task_id = _task_id(role, old_task_id)
        task["task_id"] = task_id
        task["field_contract"] = {
            "field": old_row["field"],
            **deepcopy(FIELD_RULES[old_row["field"]]),
        }
        by_field[old_row["field"]].append(task)
        truth_row = {
            "task_id": task_id,
            "source_v141_task_id": old_task_id,
            "role": role,
            "field": old_row["field"],
        }
        for key in (
            "control_expected_status",
            "source_v140_task_id",
            "case_id",
            "witness_id",
            "current_status",
            "v140_status",
        ):
            if key in old_row:
                truth_row[key] = old_row[key]
        truth_rows.append(truth_row)
    signature = {field: len(by_field[field]) for field in FIELDS}
    if signature != {
        "event_boundary": 2,
        "evidence": 2,
        "metric": 4,
        "reported_actor": 3,
        "stance": 3,
    }:
        raise JudgeV5CalibrationV142Error("v142 same-field coverage drifted")
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
        "schema_version": V142_TRUTH_VERSION,
        "task_count": 14,
        "control_count": 5,
        "owner_count": 9,
        "tasks": sorted(truth_rows, key=lambda row: str(row["task_id"])),
    }
    selection = {
        "schema_version": V142_SELECTION_VERSION,
        "created_at": now_iso(),
        "source_dispute_count": 9,
        "control_count": 5,
        "field_task_counts": signature,
        "primary_turn_count": 5,
        "order_canary_turn_count": 5,
        "maximum_tasks_per_turn": 4,
        "canary_marker_in_model_input": False,
        "canary_membership_identical": True,
        "canary_order_reversed": True,
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "semantic_pruning_performed": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
        "privacy": "private_source_event_output_aggregate_reports_only",
    }
    return rows, truth, selection


def score_v142(
    *, primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {str(row["task_id"]): row for row in truth["tasks"]}
    observed = {str(row["task_id"]): row for row in primary["decisions"]}
    repeated = {str(row["task_id"]): row for row in canary["decisions"]}
    if set(observed) != set(expected) or set(repeated) != set(expected):
        raise JudgeV5CalibrationV142Error("v142 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "control"]
    owners = [row for row in expected.values() if row["role"] == "owner"]
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    owner_abstentions = sum(
        observed[row["task_id"]]["field_status"] == "abstain" for row in owners
    )
    canary_exact = sum(
        observed[task_id]["field_status"] == repeated[task_id]["field_status"]
        for task_id in expected
    )
    evidence_complete = sum(
        bool(row["source_evidence_spans"])
        for row in list(observed.values()) + list(repeated.values())
    )
    owner_current_agreement = sum(
        observed[row["task_id"]]["field_status"] == row["current_status"]
        for row in owners
    )
    checks = {
        "control_exact_rate": control_exact == len(controls),
        "owner_abstention_count": owner_abstentions == 0,
        "order_canary_exact_rate": canary_exact == len(expected),
        "evidence_complete_rate": evidence_complete == 2 * len(expected),
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V142_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": len(expected),
            "control_count": len(controls),
            "control_exact_count": control_exact,
            "owner_count": len(owners),
            "owner_abstention_count": owner_abstentions,
            "owner_current_reference_agreement_count": owner_current_agreement,
            "order_canary_decision_count": len(expected),
            "order_canary_exact_count": canary_exact,
            "evidence_complete_count": evidence_complete,
        },
        "reference_patch_authorized": passed,
        "corrected_protocol_diagnostic_authorized": passed,
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
    truth_tasks = {str(row["task_id"]): row for row in truth["field_tasks"]}
    changed = 0
    for row in owner_truth["tasks"]:
        if row["role"] != "owner":
            continue
        status = observed[row["task_id"]]["field_status"]
        if status == "abstain":
            raise JudgeV5CalibrationV142Error("v142 cannot patch an abstaining owner")
        source_task_id = row["source_v140_task_id"]
        target = truth_tasks[source_task_id]
        before = target["expected_status"]
        target["expected_status"] = status
        for container in (truth, reference):
            case = container["cases"][row["case_id"]]
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
    truth["schema_version"] = V142_TRUTH_VERSION
    truth["v142_owner_task_count"] = 9
    truth["v142_reference_change_count"] = changed
    reference["schema_version"] = V142_REFERENCE_VERSION
    reference["reference_version"] = "fixture_reference_v12_corrected_field_owner_frozen"
    reference["v142_owner_task_count"] = 9
    reference["v142_reference_change_count"] = changed
    reference["v142_owner_basis"] = (
        "fresh_gpt55_same_field_owner_with_unmarked_reversed_permutation"
    )
    return truth, reference


def base_instructions_v142() -> str:
    return (
        "You are the final neutral reference owner for a blinded source-to-field audit. Judge only "
        "the requested field under its supplied field_contract. Mentally repair every other event "
        "field first. Mark incorrect only if the requested field remains independently wrong after "
        "all other fields are corrected. requested_field_value identifies the value and presence "
        "under review; structural sentinels that mean absence are not semantic values. Never "
        "substitute claim wording, actor, speaker, attribution, or another event field. Exact source "
        "text is necessary but must materially license the requested field under the supplied rule. "
        "Cite exact source substrings. Abstain only when the source genuinely cannot determine the "
        "requested field. Do not compare tasks, vote, use confidence, regex, keywords, overlap, "
        "embeddings, prior labels, model identity, or system identity."
    )


def build_prompt_v142(value: Mapping[str, Any]) -> str:
    return (
        "Return one independent final decision for every opaque task_id. All tasks request the same "
        "field, but each must be judged independently. The first evidence span must directly support "
        "the requested field decision and every span must be an exact substring of that task's "
        "source_excerpt. Do not emit whole-event verdicts.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def _merge_outputs(outputs: Sequence[Mapping[str, Any]], expected_count: int) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(decisions) != expected_count or len({row["task_id"] for row in decisions}) != expected_count:
        raise JudgeV5CalibrationV142Error("v142 output merge coverage drifted")
    return {"schema_version": V142_INPUT_VERSION, "decisions": decisions}


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V142_CAPACITY_AUDIT_VERSION,
        "phase_id": V142_PHASE_ID,
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
        "schema_version": V142_CAPACITY_POLICY_VERSION,
        "phase_id": V142_PHASE_ID,
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


def freeze_v142(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v142 terminal")}
    predecessor = _validate_v141()
    rows, truth, selection = build_v142_inputs(predecessor)
    truth_path = root / "corrected-field-owner-truth.private.json"
    selection_path = root / "selection-audit.json"
    rubric_path = root / "field-rubric-v142.json"
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    _write_immutable(rubric_path, field_rubric_v142())
    turns = []
    for row in rows:
        value = row["value"]
        prompt, schema = build_prompt_v142(value), output_schema(value)
        paths = _freeze_turn_request(
            root=root,
            turn_name=row["turn_name"],
            input_value=value,
            prompt=prompt,
            schema=schema,
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor_records = {
        **{f"v141_{name}": record for name, record in predecessor["records"].items()},
        "v141_attempts": predecessor["attempts"],
        "v138_reference": predecessor["v140"]["v139"]["v138"]["records"]["reference"],
    }
    capacity = _build_capacity_policy(root, predecessor_records)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V142_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "corrected_same_field_gpt55_owner_with_unmarked_reversed_permutation",
        "control_count": 5,
        "owner_count": 9,
        "primary_turn_count": 5,
        "order_canary_turn_count": 5,
        "maximum_tasks_per_turn": 4,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "canary_marker_in_model_input": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_controls_all_order_checks_no_abstentions_complete_evidence",
        "old_v140_rescore_is_audit_only": True,
        "reference_patch_authorized": False,
        "corrected_protocol_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor_records,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v141_luna_field_reference_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v140_layered_field_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "truth": _record(truth_path),
            "selection": _record(selection_path),
            "rubric": _record(rubric_path),
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
    spec_path = root / "corrected-field-reference-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v141": predecessor,
        "current_reference": predecessor["current_reference"],
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v142 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V142_FAILURE_VERSION,
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
        "schema_version": V142_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_patch_authorized": False,
        "corrected_protocol_diagnostic_authorized": False,
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


async def run_v142(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v142 terminal")
    frozen = freeze_v142(output_dir=root, timeout_seconds=timeout_seconds)
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
                    base_instructions=base_instructions_v142(),
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
        primary = _merge_outputs(primary_outputs, 14)
        canary = _merge_outputs(canary_outputs, 14)
        score = score_v142(primary=primary, canary=canary, truth=frozen["truth"])
        patched_truth, patched_reference = _patch_truth_and_reference(
            current_truth=frozen["v141"]["v140"]["values"]["truth"],
            current_reference=frozen["current_reference"],
            owner_truth=frozen["truth"],
            primary=primary,
        )
        old_v140_rescore = v140.score_v140(
            support=frozen["v141"]["v140"]["values"]["support"],
            support_canary=frozen["v141"]["v140"]["values"]["support_canary"],
            fields=frozen["v141"]["v140"]["values"]["fields"],
            field_canary=frozen["v141"]["v140"]["values"]["field_canary"],
            truth=patched_truth,
        )
        passed = bool(score["passed"])
        paths = {
            "primary": root / "corrected-field-primary.private.json",
            "canary": root / "corrected-field-order-canary.private.json",
            "score": root / "corrected-field-owner-score.json",
            "truth": root / "patched-v140-truth-audit-only.private.json",
            "v140_score": root / "old-v140-rescore-audit-only.json",
            "reference": root / "calibration-truth-v12-corrected-field-owner.private.json",
        }
        _write_immutable(paths["primary"], primary)
        _write_immutable(paths["canary"], canary)
        _write_immutable(paths["score"], score)
        _write_immutable(paths["truth"], patched_truth)
        _write_immutable(paths["v140_score"], old_v140_rescore)
        if passed:
            _write_immutable(paths["reference"], patched_reference)
        accounting = _aggregate_usage(sidecars)
        terminal = {
            "schema_version": V142_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v142_reference_v12_frozen_corrected_protocol_diagnostic_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v142_corrected_field_owner_passed"
                if passed
                else "v142_corrected_field_owner_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reference_patch_authorized": passed,
            "reference_frozen": passed,
            "corrected_protocol_diagnostic_authorized": passed,
            "expanded_layered_field_diagnostic_authorized": False,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "old_v140_rescore_is_audit_only": True,
            "score": _record(paths["score"]),
            "primary_output": _record(paths["primary"]),
            "canary_output": _record(paths["canary"]),
            "patched_truth_audit_only": _record(paths["truth"]),
            "old_v140_rescore_audit_only": _record(paths["v140_score"]),
            "reference": _record(paths["reference"]) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "old_v140_audit_metrics": old_v140_rescore["metrics"],
            "predecessor_v141_usage": frozen["v141"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v142 corrected field reference owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v142(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal.get("reference_frozen", False),
                "corrected_protocol_diagnostic_authorized": terminal.get(
                    "corrected_protocol_diagnostic_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
