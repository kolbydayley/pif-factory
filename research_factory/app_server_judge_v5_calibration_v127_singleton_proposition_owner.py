from __future__ import annotations

"""Singleton Sol proposition-support owners for the three retained-cohort disputes."""

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
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5 import validate_app_server_output_schema_subset
from .app_server_judge_v5_calibration_v121_retained_field_owner import (
    _validate_v106,
    _validate_v120,
)
from .app_server_judge_v5_calibration_v126_singleton_field_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V126_ROOT,
    _validate_v125,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V127_INPUT_VERSION = "pif_app_server_judge_v5_4_v127_singleton_proposition_owner_input_v1"
V127_TRUTH_VERSION = "pif_app_server_judge_v5_4_v127_singleton_proposition_owner_truth_v1"
V127_SELECTION_VERSION = "pif_app_server_judge_v5_4_v127_selection_v1"
V127_SPEC_VERSION = "pif_app_server_judge_v5_4_v127_spec_v1"
V127_SCORE_VERSION = "pif_app_server_judge_v5_4_v127_score_v1"
V127_REFERENCE_VERSION = (
    "pif_app_server_judge_v5_4_calibration_truth_v10_retained_proposition_owner_frozen"
)
V127_FAILURE_VERSION = "pif_app_server_judge_v5_4_v127_failure_v1"
V127_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v127_terminal_v1"
V127_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V127_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V127_PHASE_ID = "judge_v5_4_v127_singleton_proposition_owner"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
CONTROL_TURNS = tuple(f"singleton_proposition_control_{index:02d}" for index in range(3))
OWNER_TURNS = tuple(f"singleton_proposition_owner_{index:02d}" for index in range(3))
TURN_NAMES = CONTROL_TURNS + OWNER_TURNS
STATUSES = ("supported", "unsupported", "abstain")
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V126_ROOT.parent / "judge-calibration-v5_4-v127-singleton-proposition-owner"
).resolve()


class JudgeV5CalibrationV127Error(RuntimeError):
    """The v127 singleton proposition-owner contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v126() -> dict[str, Any]:
    root = DEFAULT_V126_ROOT
    paths = {
        "spec": root / "singleton-owner-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "singleton-owner-score.json",
        "outputs": root / "singleton-owner-outputs.private.json",
        "final_owner": root / "final-owner-output.private.json",
        "reconciled": root / "reconciled-retained-field-output.private.json",
        "reference": root / "calibration-truth-v10-retained-field-owner.private.json",
        "truth": root / "singleton-owner-truth.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v126 {name}") for name, path in paths.items()}
    terminal, score, spec, reference = (
        values["terminal"], values["score"], values["spec"], values["reference"]
    )
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v126_singleton_owner_passed_retained_field_patch_authorized"
        or terminal.get("retained_field_reference_patch_authorized") is not True
        or terminal.get("proposition_reference_frozen") is not False
        or terminal.get("alignment_reference_frozen") is not False
        or terminal.get("fresh_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 127256
        or score.get("passed") is not True
        or score.get("metrics", {}).get("singleton_control_exact_count") != 3
        or score.get("metrics", {}).get("singleton_owner_abstention_count") != 0
        or spec.get("model") != MODEL
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
        or reference.get("retained_field_reference_patch_authorized") is not True
        or reference.get("proposition_reference_frozen") is not False
        or reference.get("alignment_reference_frozen") is not False
        or len(reference.get("cases") or {}) != 66
    ):
        raise JudgeV5CalibrationV127Error("v126 predecessor contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5CalibrationV127Error("v126 runtime record drifted")
    terminal_records = {
        "score": "score",
        "outputs": "singleton_outputs",
        "final_owner": "final_owner_output",
        "reconciled": "reconciled_output",
        "reference": "reference_candidate",
    }
    for name, key in terminal_records.items():
        record = terminal.get(key)
        if not isinstance(record, Mapping) or dict(record) != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV127Error(f"v126 {name} record drifted")
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
                raise JudgeV5CalibrationV127Error("v126 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(_load_json(Path(records["sidecar"]["path"]), "v126 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV127Error("v126 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "v125": _validate_v125(),
    }


def _status_map(pointwise: Mapping[str, Any]) -> dict[str, str]:
    return {str(row["witness_id"]): str(row["proposition_verdict"]) for row in pointwise["units"]}


def _task_id(role: str, witness_id: str) -> str:
    return role + "_" + sha256_text(f"v127|{role}|{witness_id}")[:24]


def _one_task_value(task: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": V127_INPUT_VERSION,
        "task_count": 1,
        "tasks": [deepcopy(task)],
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "singleton_context": True,
    }


def build_v127_inputs(
    v126: Mapping[str, Any], v120: Mapping[str, Any], v106: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    reference = v126["values"]["reference"]
    cohort_cases = set(v120["values"]["truth"]["cases"])
    units = {
        str(row["witness_id"]): row for row in v120["values"]["pointwise_input"]["units"]
    }
    luna = _status_map(v120["values"]["pointwise"])
    gpt55 = _status_map(v106["values"]["pointwise"])
    rows = []
    for case_id in sorted(cohort_cases):
        for witness_id, current in reference["cases"][case_id]["proposition"].items():
            row = {
                "case_id": case_id,
                "witness_id": witness_id,
                "current_status": current,
                "luna_status": luna[witness_id],
                "gpt55_status": gpt55[witness_id],
            }
            row["role"] = (
                "unanimous_control"
                if len({current, row["luna_status"], row["gpt55_status"]}) == 1
                else (
                    "consensus_reference_dispute"
                    if row["luna_status"] == row["gpt55_status"] != current
                    else "model_disagreement"
                )
            )
            rows.append(row)
    disputes = [row for row in rows if row["role"] != "unanimous_control"]
    controls_pool = [row for row in rows if row["role"] == "unanimous_control"]
    if len(disputes) != 3 or Counter(row["role"] for row in disputes) != {
        "consensus_reference_dispute": 1,
        "model_disagreement": 2,
    }:
        raise JudgeV5CalibrationV127Error("v127 proposition dispute coverage drifted")
    controls = []
    for status, count in (("unsupported", 1), ("supported", 2)):
        candidates = sorted(
            [row for row in controls_pool if row["current_status"] == status],
            key=lambda row: sha256_text(f"v127|control|{status}|{row['witness_id']}"),
        )
        used_cases = {row["case_id"] for row in controls}
        for row in candidates:
            if row["case_id"] in used_cases:
                continue
            controls.append(row)
            used_cases.add(row["case_id"])
            if sum(item["current_status"] == status for item in controls) == count:
                break
    if len(controls) != 3 or Counter(row["current_status"] for row in controls) != {
        "supported": 2,
        "unsupported": 1,
    }:
        raise JudgeV5CalibrationV127Error("v127 proposition control coverage drifted")

    def packet(row: Mapping[str, Any], role: str) -> dict[str, Any]:
        unit = units[row["witness_id"]]
        claim_text = unit["structured_event"].get("claim_text")
        if not isinstance(claim_text, str) or not claim_text:
            raise JudgeV5CalibrationV127Error("v127 proposition text is missing")
        task_id = _task_id(role, row["witness_id"])
        return {
            "turn_role": role,
            "value": _one_task_value(
                {
                    "task_id": task_id,
                    "source_excerpt": unit["source_excerpt"],
                    "proposition_text": claim_text,
                }
            ),
            "truth": {
                "task_id": task_id,
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "role": role,
                "reference_role": row["role"],
                "control_expected_status": row["current_status"] if role == "singleton_control" else None,
            },
        }

    control_rows = [packet(row, "singleton_control") for row in controls]
    owner_rows = [packet(row, "singleton_owner") for row in disputes]
    control_rows.sort(key=lambda row: sha256_text(f"v127|control-order|{row['truth']['task_id']}"))
    owner_rows.sort(key=lambda row: sha256_text(f"v127|owner-order|{row['truth']['task_id']}"))
    turn_rows = control_rows + owner_rows
    for turn_name, row in zip(TURN_NAMES, turn_rows, strict=True):
        row["turn_name"] = turn_name
    truth = {
        "schema_version": V127_TRUTH_VERSION,
        "task_count": 6,
        "singleton_control_count": 3,
        "singleton_owner_count": 3,
        "tasks": [deepcopy(row["truth"]) for row in turn_rows],
    }
    selection = {
        "schema_version": V127_SELECTION_VERSION,
        "created_at": now_iso(),
        "cohort_case_count": len(cohort_cases),
        "cohort_witness_count": len(rows),
        "unanimous_proposition_count": len(controls_pool),
        "nonunanimous_proposition_count": len(disputes),
        "dispute_role_counts": dict(sorted(Counter(row["role"] for row in disputes).items())),
        "control_status_counts": dict(sorted(Counter(row["current_status"] for row in controls).items())),
        "maximum_tasks_per_turn": 1,
        "proposition_support_separate_from_structured_field_correctness": True,
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "singleton_owner_is_decisive": True,
        "majority_voting_used": False,
        "privacy": "aggregate_counts_only",
    }
    return turn_rows, truth, selection


def proposition_output_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    task_ids = [row["task_id"] for row in value["tasks"]]
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["decisions"],
        "properties": {
            "decisions": {
                "type": "array",
                "minItems": len(task_ids),
                "maxItems": len(task_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["task_id", "proposition_status", "source_evidence_spans", "rationale"],
                    "properties": {
                        "task_id": {"type": "string", "enum": task_ids},
                        "proposition_status": {"type": "string", "enum": list(STATUSES)},
                        "source_evidence_spans": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 2,
                            "items": {"type": "string", "minLength": 1, "maxLength": 1000},
                        },
                        "rationale": {"type": "string", "minLength": 1, "maxLength": 300},
                    },
                },
            }
        },
    }
    errors = validate_app_server_output_schema_subset(schema)
    if errors:
        raise JudgeV5CalibrationV127Error("v127 output schema is unsupported")
    return schema


def validate_proposition_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping) or set(output) != {"decisions"}:
        return ["invalid_root"]
    tasks = {row["task_id"]: row for row in value["tasks"]}
    seen = set()
    errors = []
    for index, row in enumerate(output.get("decisions") or []):
        prefix = f"decision_{index}"
        if not isinstance(row, Mapping) or set(row) != {
            "task_id", "proposition_status", "source_evidence_spans", "rationale"
        }:
            errors.append(prefix + "_shape")
            continue
        task_id = row.get("task_id")
        if task_id not in tasks or task_id in seen:
            errors.append(prefix + "_identity")
            continue
        seen.add(task_id)
        spans = row.get("source_evidence_spans")
        source = tasks[task_id]["source_excerpt"]
        if row.get("proposition_status") not in STATUSES:
            errors.append(prefix + "_status")
        if (
            not isinstance(spans, list)
            or not 1 <= len(spans) <= 2
            or len(spans) != len(set(spans))
            or any(not isinstance(span, str) or not span or span not in source for span in spans)
        ):
            errors.append(prefix + "_evidence")
        if not isinstance(row.get("rationale"), str) or not row["rationale"]:
            errors.append(prefix + "_rationale")
    if seen != set(tasks):
        errors.append("coverage")
    return errors


def proposition_instructions() -> str:
    return (
        "You are a side-free source-to-proposition support owner. Decide whether every material claim in "
        "proposition_text is entailed by source_excerpt. supported permits faithful paraphrase and clear "
        "coreference; unsupported means at least one material assertion is added, contradicted, or not "
        "entailed. Exact wording alone is not sufficient when the proposition changes meaning. abstain only "
        "when the supplied source cannot determine support. Cite exact source substrings. Do not judge actor, "
        "event type, attribution, stance, or other structured fields except insofar as they are explicit material "
        "claims inside proposition_text. Do not vote, compare tasks, use confidence, regex, keywords, overlap, "
        "embeddings, or infer system identity."
    )


def proposition_prompt(value: Mapping[str, Any]) -> str:
    return (
        "Return one independent proposition-support decision for the opaque task_id. The first evidence span "
        "must directly ground the decision and every span must be an exact substring of source_excerpt.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def score_v127(outputs: Mapping[str, Mapping[str, Any]], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for output in outputs.values() for row in output["decisions"]}
    if set(expected) != set(observed):
        raise JudgeV5CalibrationV127Error("v127 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "singleton_control"]
    owners = [row for row in expected.values() if row["role"] == "singleton_owner"]
    control_exact = sum(
        observed[row["task_id"]]["proposition_status"] == row["control_expected_status"]
        for row in controls
    )
    owner_abstentions = sum(
        observed[row["task_id"]]["proposition_status"] == "abstain" for row in owners
    )
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    checks = {
        "singleton_control_exact_rate": control_exact == 3,
        "singleton_owner_abstention_count": owner_abstentions == 0,
        "evidence_complete_rate": evidence_complete == 6,
        "proposition_support_separate_from_structured_fields": True,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V127_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": 6,
            "singleton_control_count": 3,
            "singleton_control_exact_count": control_exact,
            "singleton_owner_count": 3,
            "singleton_owner_abstention_count": owner_abstentions,
            "evidence_complete_count": evidence_complete,
        },
        "retained_proposition_reference_patch_authorized": passed,
        "proposition_reference_frozen": passed,
        "alignment_reference_frozen": False,
        "fresh_diagnostic_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "majority_voting_used": False,
    }


def build_reference_candidate_v127(
    *, current_reference: Mapping[str, Any], truth: Mapping[str, Any], outputs: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    candidate = deepcopy(current_reference)
    observed = {row["task_id"]: row for output in outputs.values() for row in output["decisions"]}
    changed = unsupported_projection_changes = 0
    for row in truth["tasks"]:
        if row["role"] != "singleton_owner":
            continue
        decision = observed[row["task_id"]]["proposition_status"]
        case = candidate["cases"][row["case_id"]]
        witness_id = row["witness_id"]
        before = case["proposition"][witness_id]
        case["proposition"][witness_id] = decision
        changed += int(before != decision)
        issues = set(case["field_issues"][witness_id])
        before_unsupported = "unsupported_inference" in issues
        if decision == "unsupported":
            issues.add("unsupported_inference")
        else:
            issues.discard("unsupported_inference")
        case["field_issues"][witness_id] = sorted(issues)
        case["structured_fields"][witness_id] = "incorrect" if issues else "correct"
        unsupported_projection_changes += int(before_unsupported != ("unsupported_inference" in issues))
    if changed > 3 or any(
        status == "abstain"
        for case in candidate["cases"].values()
        for status in case["proposition"].values()
    ):
        raise JudgeV5CalibrationV127Error("v127 proposition candidate drifted")
    candidate["schema_version"] = V127_REFERENCE_VERSION
    candidate["reference_version"] = "fixture_reference_v10_retained_proposition_owner_frozen"
    candidate["retained_proposition_owner_change_count"] = changed
    candidate["unsupported_inference_projection_change_count"] = unsupported_projection_changes
    candidate["retained_field_reference_patch_authorized"] = True
    candidate["retained_proposition_reference_patch_authorized"] = True
    candidate["proposition_reference_frozen"] = True
    candidate["alignment_reference_frozen"] = False
    candidate["fresh_diagnostic_authorized"] = False
    return candidate


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V127_CAPACITY_AUDIT_VERSION,
        "phase_id": V127_PHASE_ID,
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
        "schema_version": V127_CAPACITY_POLICY_VERSION,
        "phase_id": V127_PHASE_ID,
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


def freeze_v127(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v127 terminal")}
    v126, v120, v106 = _validate_v126(), _validate_v120(), _validate_v106()
    rows, truth, selection = build_v127_inputs(v126, v120, v106)
    truth_path = root / "singleton-proposition-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    turns = []
    for row in rows:
        prompt = proposition_prompt(row["value"])
        schema = proposition_output_schema(row["value"])
        paths = _freeze_turn_request(
            root=root, turn_name=row["turn_name"], input_value=row["value"], prompt=prompt, schema=schema
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor = {
        **{f"v126_{name}": record for name, record in v126["records"].items()},
        "v126_attempts": v126["attempts"],
        **{f"v120_{name}": record for name, record in v120["records"].items()},
        "v120_attempts": v120["attempt_records"],
        **{f"v106_{name}": record for name, record in v106["records"].items()},
        "v106_attempts": v106["attempt_records"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V127_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "isolated_singleton_proposition_support_owner_for_three_nonunanimous_retained_witnesses_with_three_controls",
        "singleton_control_count": 3,
        "singleton_owner_count": 3,
        "maximum_tasks_per_turn": 1,
        "turn_plan": list(TURN_NAMES),
        "proposition_support_separate_from_structured_field_correctness": True,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "three_exact_controls_three_decisive_owners_and_complete_exact_evidence",
        "retained_proposition_reference_patch_authorized": False,
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
            _record(runtime_dir / "app_server_judge_v5_calibration_v126_singleton_field_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v121_retained_field_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v108_layered_diagnostic.py"),
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
        "privacy": "private_source_proposition_output_no_source_text_in_reports",
    }
    spec_path = root / "singleton-proposition-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v126": v126,
    }


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
            measured = _validate_usage(_load_json(Path(record["path"]), "v127 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V127_FAILURE_VERSION,
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
        "schema_version": V127_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "retained_proposition_reference_patch_authorized": False,
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


async def run_v127(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v127 terminal")
    frozen = freeze_v127(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        outputs, sidecars = {}, []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=proposition_instructions(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=1,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_proposition_output(candidate, item),
                )
                outputs[current_turn] = output
                sidecars.append(sidecar)
        outputs_path = root / "singleton-proposition-outputs.private.json"
        _write_immutable(outputs_path, {"turns": outputs})
        score = score_v127(outputs, frozen["truth"])
        score_path = root / "singleton-proposition-score.json"
        _write_immutable(score_path, score)
        candidate_path = root / "calibration-truth-v10-retained-proposition-owner.private.json"
        if score["passed"]:
            candidate = build_reference_candidate_v127(
                current_reference=frozen["v126"]["values"]["reference"],
                truth=frozen["truth"],
                outputs=outputs,
            )
            _write_immutable(candidate_path, candidate)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V127_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v127_singleton_proposition_owner_passed_patch_authorized"
                if passed else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v127_retained_proposition_reference_patch_authorized"
                if passed else "v127_singleton_proposition_owner_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "retained_proposition_reference_patch_authorized": passed,
            "proposition_reference_frozen": passed,
            "alignment_reference_frozen": False,
            "fresh_diagnostic_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "outputs": _record(outputs_path),
            "reference_candidate": _record(candidate_path) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v126_usage": frozen["v126"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v127 singleton proposition owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v127(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({"state": terminal["state"], "terminal_reason": terminal["terminal_reason"], "retained_proposition_reference_patch_authorized": terminal.get("retained_proposition_reference_patch_authorized", False), "usage_status": terminal.get("usage_status")}, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
