from __future__ import annotations

"""Fresh singleton owner for the observable v145 field-reference disputes."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_calibration_v145_comprehensive_field_reference_owner as v145
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
from .util import now_iso, sha256_text


V146_INPUT_VERSION = "pif_app_server_judge_v5_4_v146_singleton_reference_input_v1"
V146_TRUTH_VERSION = "pif_app_server_judge_v5_4_v146_singleton_reference_truth_v1"
V146_SELECTION_VERSION = "pif_app_server_judge_v5_4_v146_selection_v1"
V146_SPEC_VERSION = "pif_app_server_judge_v5_4_v146_spec_v1"
V146_SCORE_VERSION = "pif_app_server_judge_v5_4_v146_score_v1"
V146_REFERENCE_VERSION = (
    "pif_app_server_judge_v5_4_calibration_truth_v13_singleton_reference_owner_frozen"
)
V146_FAILURE_VERSION = "pif_app_server_judge_v5_4_v146_failure_v1"
V146_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v146_terminal_v1"
V146_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V146_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V146_PHASE_ID = "judge_v5_4_v146_singleton_reference_owner"

MODEL = "gpt-5.5"
EFFORT = "high"
CONTROL_TURNS = tuple(f"singleton_control_{index:02d}" for index in range(5))
OWNER_TURNS = tuple(f"singleton_owner_{index:02d}" for index in range(14))
REPEAT_TURNS = tuple(f"singleton_repeat_{index:02d}" for index in range(5))
TURN_NAMES = CONTROL_TURNS + OWNER_TURNS + REPEAT_TURNS
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    v145.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v146-singleton-reference-owner"
).resolve()


class JudgeV5CalibrationV146Error(RuntimeError):
    """The v146 singleton-reference contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v145() -> dict[str, Any]:
    root = v145.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "comprehensive-field-reference-owner-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "comprehensive-field-owner-score.json",
        "truth": root / "comprehensive-field-owner-truth.private.json",
        "primary": root / "comprehensive-field-owner-primary.private.json",
        "canary": root / "comprehensive-field-owner-canary.private.json",
        "selection": root / "selection-audit.json",
        "rubric": root / "field-rubric-v145.json",
        "patched_truth": root / "patched-v143-truth-audit-only.private.json",
        "old_v144_score": root / "old-v144-rescore-audit-only.json",
    }
    values = {name: _load_json(path, f"v145 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v145_comprehensive_field_owner_quality_gate_not_passed"
        or terminal.get("reference_frozen") is not False
        or terminal.get("corrected_field_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 476652
        or score.get("passed") is not False
        or score.get("failed_checks")
        != ["control_exact_rate", "order_canary_exact_rate"]
        or score.get("metrics", {}).get("control_exact_count") != 9
        or score.get("metrics", {}).get("order_canary_exact_count") != 19
        or score.get("metrics", {}).get("owner_count") != 14
        or score.get("metrics", {}).get("owner_abstention_count") != 0
        or spec.get("model") != "gpt-5.6-sol"
        or spec.get("turn_plan") != list(v145.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV146Error("v145 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV146Error("v145 runtime record drifted")
    attempts: dict[str, dict[str, Any]] = {}
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
                raise JudgeV5CalibrationV146Error("v145 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v145 sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV146Error("v145 usage aggregate drifted")
    for name, terminal_key in {
        "score": "score",
        "primary": "primary_output",
        "canary": "canary_output",
        "patched_truth": "patched_truth_audit_only",
        "old_v144_score": "old_v144_rescore_audit_only",
    }.items():
        record = terminal.get(terminal_key)
        if record != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV146Error(f"v145 {name} record drifted")
    input_tasks: dict[str, dict[str, Any]] = {}
    input_records = []
    for row in spec["frozen_inputs"]["turns"]:
        record = row["input"]
        if not _verify_record(record):
            raise JudgeV5CalibrationV146Error("v145 frozen input drifted")
        input_records.append(record)
        value = _load_json(Path(record["path"]), "v145 frozen input")
        for task in value["tasks"]:
            input_tasks[str(task["task_id"])] = task
    predecessor_v144 = v145._validate_v144()
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "input_tasks": input_tasks,
        "input_records": input_records,
        "v144": predecessor_v144,
    }


def _task_id(role: str, source_task_id: str) -> str:
    return role + "_" + sha256_text(f"v146|{role}|{source_task_id}")[:24]


def _one_task_value(task: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": V146_INPUT_VERSION,
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


def _frozen_task_map(spec: Mapping[str, Any], label: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    tasks: dict[str, Any] = {}
    records = []
    for row in spec["frozen_inputs"]["turns"]:
        record = row["input"]
        if not _verify_record(record):
            raise JudgeV5CalibrationV146Error(f"{label} input record drifted")
        records.append(record)
        value = _load_json(Path(record["path"]), f"{label} frozen input")
        for task in value["tasks"]:
            tasks[str(task["task_id"])] = task
    return tasks, records


def build_v146_inputs(
    predecessor: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    v145_truth = {row["task_id"]: row for row in predecessor["values"]["truth"]["tasks"]}
    v145_primary = {
        row["task_id"]: row for row in predecessor["values"]["primary"]["decisions"]
    }
    v145_canary = {
        row["task_id"]: row for row in predecessor["values"]["canary"]["decisions"]
    }
    owner_sources = [row for row in v145_truth.values() if row["role"] == "owner"]
    unstable_source_ids = {
        row["task_id"]
        for row in owner_sources
        if v145_primary[row["task_id"]]["field_status"]
        != v145_canary[row["task_id"]]["field_status"]
    }
    if len(owner_sources) != 14 or len(unstable_source_ids) != 5:
        raise JudgeV5CalibrationV146Error("v146 observable owner coverage drifted")

    v142 = predecessor["v144"]["v143"]["v142"]
    v142_tasks, v142_input_records = _frozen_task_map(v142["values"]["spec"], "v142")
    v142_primary = {
        row["task_id"]: row for row in v142["values"]["primary"]["decisions"]
    }
    v142_canary = {
        row["task_id"]: row for row in v142["values"]["canary"]["decisions"]
    }
    control_sources = [
        row
        for row in v142["values"]["truth"]["tasks"]
        if row["role"] == "control"
        and v142_primary[row["task_id"]]["field_status"]
        == row["control_expected_status"]
        and v142_canary[row["task_id"]]["field_status"]
        == row["control_expected_status"]
        and row["control_expected_status"] != "abstain"
    ]
    if len(control_sources) != 5:
        raise JudgeV5CalibrationV146Error("v146 settled control coverage drifted")

    rows: list[dict[str, Any]] = []
    truth_rows: list[dict[str, Any]] = []
    for source in sorted(
        control_sources, key=lambda row: sha256_text(f"v146|control-order|{row['task_id']}")
    ):
        task = deepcopy(v142_tasks[source["task_id"]])
        task_id = _task_id("control", source["task_id"])
        task["task_id"] = task_id
        task["field_contract"] = {
            "field": source["field"],
            **deepcopy(v145.FIELD_RULES_V145[source["field"]]),
        }
        rows.append({"turn_role": "singleton_control", "value": _one_task_value(task)})
        truth_rows.append(
            {
                "task_id": task_id,
                "role": "control",
                "field": source["field"],
                "source_v142_task_id": source["task_id"],
                "control_expected_status": source["control_expected_status"],
            }
        )
    owner_rows = []
    for source in owner_sources:
        task = deepcopy(predecessor["input_tasks"][source["task_id"]])
        task_id = _task_id("owner", source["task_id"])
        task["task_id"] = task_id
        task["field_contract"] = {
            "field": source["field"],
            **deepcopy(v145.FIELD_RULES_V145[source["field"]]),
        }
        owner_rows.append(
            {
                "turn_role": "singleton_owner",
                "value": _one_task_value(task),
                "source_v145_task_id": source["task_id"],
                "truth": {
                    "task_id": task_id,
                    "role": "owner",
                    "field": source["field"],
                    "source_v145_task_id": source["task_id"],
                    "source_v143_task_id": source["source_v143_task_id"],
                    "case_id": source["case_id"],
                    "witness_id": source["witness_id"],
                    "current_status": source["current_status"],
                },
            }
        )
    owner_rows.sort(
        key=lambda row: sha256_text(f"v146|owner-order|{row['source_v145_task_id']}")
    )
    rows.extend(owner_rows)
    truth_rows.extend(deepcopy(row["truth"]) for row in owner_rows)

    repeat_map = []
    repeat_rows = [
        row for row in owner_rows if row["source_v145_task_id"] in unstable_source_ids
    ]
    repeat_rows.sort(
        key=lambda row: sha256_text(f"v146|repeat-order|{row['source_v145_task_id']}")
    )
    for row in repeat_rows:
        rows.append(
            {
                "turn_role": "singleton_repeat",
                "value": deepcopy(row["value"]),
                "source_v145_task_id": row["source_v145_task_id"],
            }
        )
        repeat_map.append(
            {
                "owner_task_id": row["truth"]["task_id"],
                "source_v145_task_id": row["source_v145_task_id"],
            }
        )
    for turn_name, row in zip(TURN_NAMES, rows, strict=True):
        row["turn_name"] = turn_name
    truth = {
        "schema_version": V146_TRUTH_VERSION,
        "task_count": 19,
        "control_count": 5,
        "owner_count": 14,
        "repeat_count": 5,
        "tasks": truth_rows,
        "repeat_map": repeat_map,
    }
    selection = {
        "schema_version": V146_SELECTION_VERSION,
        "created_at": now_iso(),
        "control_count": 5,
        "owner_count": 14,
        "repeat_count": 5,
        "turn_count": 24,
        "maximum_tasks_per_turn": 1,
        "repeat_membership_is_exactly_v145_unstable_owners": True,
        "repeat_input_is_byte_identical_before_freeze": all(
            row["value"] == next(
                owner["value"]
                for owner in owner_rows
                if owner["source_v145_task_id"] == row["source_v145_task_id"]
            )
            for row in repeat_rows
        ),
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
        "semantic_pruning_performed": False,
        "privacy": "private_source_event_output_aggregate_reports_only",
    }
    return rows, truth, selection, v142_input_records


def base_instructions_v146() -> str:
    return (
        "You are the neutral final reference owner for one blinded source-to-field decision. Judge "
        "only the requested field under its supplied field_contract. Mentally correct every other "
        "event field first, then mark incorrect only when this field remains independently wrong. "
        "requested_field_value identifies the value and presence under review; structural absence "
        "sentinels are not semantic values. Cite exact source substrings. Abstain only when the "
        "source genuinely cannot determine the requested field. Do not use confidence, compare "
        "tasks, vote, infer prior labels, or use regex, keywords, overlap, embeddings, model "
        "identity, or system identity."
    )


def build_prompt_v146(value: Mapping[str, Any]) -> str:
    return (
        "Return the one independent field decision for the opaque task_id. The first evidence span "
        "must directly support the requested field decision and every span must be an exact "
        "substring of source_excerpt. Do not emit a whole-event verdict.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def score_v146(
    *, outputs: Mapping[str, Mapping[str, Any]], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    primary = {}
    repeats = {}
    for turn_name, output in outputs.items():
        row = output["decisions"][0]
        target = repeats if turn_name in REPEAT_TURNS else primary
        if row["task_id"] in target:
            raise JudgeV5CalibrationV146Error("v146 duplicate output task")
        target[row["task_id"]] = row
    if set(primary) != set(expected):
        raise JudgeV5CalibrationV146Error("v146 primary coverage drifted")
    repeat_ids = {row["owner_task_id"] for row in truth["repeat_map"]}
    if set(repeats) != repeat_ids:
        raise JudgeV5CalibrationV146Error("v146 repeat coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "control"]
    owners = [row for row in expected.values() if row["role"] == "owner"]
    control_exact = sum(
        primary[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    owner_abstentions = sum(
        primary[row["task_id"]]["field_status"] == "abstain" for row in owners
    )
    repeat_exact = sum(
        primary[task_id]["field_status"] == repeats[task_id]["field_status"]
        for task_id in repeat_ids
    )
    evidence_complete = sum(
        bool(row["source_evidence_spans"])
        for row in list(primary.values()) + list(repeats.values())
    )
    checks = {
        "settled_control_exact_rate": control_exact == 5,
        "singleton_owner_abstention_count": owner_abstentions == 0,
        "unstable_owner_repeat_exact_rate": repeat_exact == 5,
        "evidence_complete_rate": evidence_complete == 24,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V146_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": 19,
            "control_count": 5,
            "control_exact_count": control_exact,
            "owner_count": 14,
            "owner_abstention_count": owner_abstentions,
            "repeat_count": 5,
            "repeat_exact_count": repeat_exact,
            "evidence_complete_count": evidence_complete,
        },
        "reference_patch_authorized": passed,
        "fresh_corrected_field_diagnostic_authorized": passed,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
    }


def _merge_primary(outputs: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    decisions = [
        deepcopy(output["decisions"][0])
        for turn_name, output in outputs.items()
        if turn_name in OWNER_TURNS
    ]
    if len(decisions) != 14 or len({row["task_id"] for row in decisions}) != 14:
        raise JudgeV5CalibrationV146Error("v146 owner merge drifted")
    return {"schema_version": V146_INPUT_VERSION, "decisions": decisions}


def _patch_truth_and_reference(
    *,
    current_truth: Mapping[str, Any],
    current_reference: Mapping[str, Any],
    owner_truth: Mapping[str, Any],
    owner_output: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    truth = deepcopy(current_truth)
    reference = deepcopy(current_reference)
    observed = {str(row["task_id"]): row for row in owner_output["decisions"]}
    task_map = {str(row["task_id"]): row for row in truth["field_tasks"]}
    if not isinstance(reference.get("cases"), dict):
        raise JudgeV5CalibrationV146Error("v146 reference cases are unavailable")
    changed = 0
    for row in owner_truth["tasks"]:
        if row["role"] != "owner":
            continue
        status = observed[row["task_id"]]["field_status"]
        if status == "abstain":
            raise JudgeV5CalibrationV146Error("v146 cannot patch an abstaining owner")
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
    truth["schema_version"] = V146_TRUTH_VERSION
    truth["v146_owner_task_count"] = 14
    truth["v146_reference_change_count"] = changed
    reference["schema_version"] = V146_REFERENCE_VERSION
    reference["reference_version"] = "fixture_reference_v13_singleton_reference_owner_frozen"
    reference["v146_owner_task_count"] = 14
    reference["v146_reference_change_count"] = changed
    reference["v146_owner_basis"] = (
        "fresh_gpt55_singleton_owner_with_settled_controls_and_targeted_repeat_canary"
    )
    return truth, reference


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V146_CAPACITY_AUDIT_VERSION,
        "phase_id": V146_PHASE_ID,
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
        "schema_version": V146_CAPACITY_POLICY_VERSION,
        "phase_id": V146_PHASE_ID,
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


def freeze_v146(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v146 terminal")}
    predecessor = _validate_v145()
    rows, truth, selection, v142_input_records = build_v146_inputs(predecessor)
    truth_path = root / "singleton-reference-truth.private.json"
    selection_path = root / "selection-audit.json"
    rubric_path = root / "field-rubric-v146.json"
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    _write_immutable(rubric_path, v145.field_rubric_v145())
    turns = []
    for row in rows:
        value = row["value"]
        prompt, schema = build_prompt_v146(value), output_schema(value)
        paths = _freeze_turn_request(
            root=root,
            turn_name=row["turn_name"],
            input_value=value,
            prompt=prompt,
            schema=schema,
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor_records = {
        **{f"v145_{name}": record for name, record in predecessor["records"].items()},
        "v145_attempts": predecessor["attempts"],
        "v145_frozen_inputs": predecessor["input_records"],
        **{
            f"v142_{name}": record
            for name, record in predecessor["v144"]["v143"]["v142"]["records"].items()
        },
        "v142_attempts": predecessor["v144"]["v143"]["v142"]["attempts"],
        "v142_frozen_inputs": v142_input_records,
    }
    capacity = _build_capacity_policy(root, predecessor_records)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V146_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_gpt55_singleton_owner_for_all_v145_disputes_with_five_settled_controls_and_five_targeted_repeats",
        "control_count": 5,
        "owner_count": 14,
        "repeat_count": 5,
        "maximum_tasks_per_turn": 1,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "single_decisive_owner": True,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "five_settled_controls_fourteen_nonabstaining_owners_five_exact_repeats_complete_evidence",
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
    spec_path = root / "singleton-reference-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "predecessor": predecessor,
        "current_reference": predecessor["v144"]["v143"]["v142"]["values"]["reference"],
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v146 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V146_FAILURE_VERSION,
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
        "schema_version": V146_TERMINAL_VERSION,
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


async def run_v146(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v146 terminal")
    frozen = freeze_v146(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        outputs: dict[str, Mapping[str, Any]] = {}
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
                    base_instructions=base_instructions_v146(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=1,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_output(
                        candidate, item
                    ),
                )
                outputs[current_turn] = output
                sidecars.append(sidecar)
        score = score_v146(outputs=outputs, truth=frozen["truth"])
        owner_output = _merge_primary(outputs)
        patched_truth, patched_reference = _patch_truth_and_reference(
            current_truth=frozen["predecessor"]["v144"]["v143"]["values"]["truth"],
            current_reference=frozen["current_reference"],
            owner_truth=frozen["truth"],
            owner_output=owner_output,
        )
        old_v144_rescore = v143.score_v143(
            support=frozen["predecessor"]["v144"]["v143"]["values"]["support"],
            support_canary=frozen["predecessor"]["v144"]["v143"]["values"]["support_canary"],
            fields=frozen["predecessor"]["values"]["primary"],
            field_canary=frozen["predecessor"]["values"]["canary"],
            truth=patched_truth,
        )
        paths = {
            "outputs": root / "singleton-reference-outputs.private.json",
            "owner": root / "singleton-owner-output.private.json",
            "score": root / "singleton-reference-score.json",
            "truth": root / "patched-v143-truth-audit-only.private.json",
            "old_v144_score": root / "old-v144-rescore-audit-only.json",
            "reference": root / "calibration-truth-v13-singleton-reference-owner.private.json",
        }
        _write_immutable(paths["outputs"], {"turns": outputs})
        _write_immutable(paths["owner"], owner_output)
        _write_immutable(paths["score"], score)
        _write_immutable(paths["truth"], patched_truth)
        _write_immutable(paths["old_v144_score"], old_v144_rescore)
        passed = bool(score["passed"])
        if passed:
            _write_immutable(paths["reference"], patched_reference)
        accounting = _aggregate_usage(sidecars)
        terminal = {
            "schema_version": V146_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v146_reference_v13_frozen_fresh_corrected_field_diagnostic_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v146_singleton_reference_owner_passed"
                if passed
                else "v146_singleton_reference_owner_quality_gate_not_passed"
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
            "outputs": _record(paths["outputs"]),
            "owner_output": _record(paths["owner"]),
            "patched_truth_audit_only": _record(paths["truth"]),
            "old_v144_rescore_audit_only": _record(paths["old_v144_score"]),
            "reference": _record(paths["reference"]) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "old_v144_audit_metrics": old_v144_rescore["metrics"],
            "predecessor_v145_usage": frozen["predecessor"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v146 singleton reference owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v146(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
