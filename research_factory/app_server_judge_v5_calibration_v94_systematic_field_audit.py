from __future__ import annotations

"""Systematic target/certainty reference audit after the v91-v93 diagnostic."""

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
from .app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    project_exact_spans,
)
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    V23_ROOT,
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record_matches,
    _verify_record,
    _write_stable_created,
)
from .app_server_judge_v5_calibration_v90_reference_v5_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V90_ROOT,
)
from .app_server_judge_v5_calibration_v93_repair_scoring_receipt import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V93_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _validate_usage,
)
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .util import now_iso, sha256_text


V94_INPUT_VERSION = "pif_app_server_judge_v5_4_v94_systematic_field_input_v1"
V94_TRUTH_VERSION = "pif_app_server_judge_v5_4_v94_systematic_field_truth_v1"
V94_RUBRIC_VERSION = "pif_app_server_judge_v5_4_v94_field_rubric_v1"
V94_SELECTION_VERSION = "pif_app_server_judge_v5_4_v94_selection_v1"
V94_SPEC_VERSION = "pif_app_server_judge_v5_4_v94_systematic_field_spec_v1"
V94_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v94_systematic_field_output_v1"
V94_SCORE_VERSION = "pif_app_server_judge_v5_4_v94_systematic_field_score_v1"
V94_AUDIT_VERSION = "pif_app_server_judge_v5_4_v94_projection_audit_v1"
V94_FAILURE_VERSION = "pif_app_server_judge_v5_4_v94_failure_v1"
V94_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v94_terminal_v1"
V94_PATCH_VERSION = "pif_app_server_judge_v5_4_v94_reference_patch_v1"
V94_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V94_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V94_PHASE_ID = "judge_v5_4_v94_systematic_field_audit"

MODEL = "gpt-5.5"
EFFORT = "high"
CHALLENGE_COUNTS = {"certainty": 18, "target": 7}
CONTROL_COUNT = 10
PRIMARY_TASK_COUNT = sum(CHALLENGE_COUNTS.values()) + CONTROL_COUNT
PRIMARY_TURNS = tuple(f"systematic_field_shard_{index:02d}" for index in range(9))
CANARY_TURN = "systematic_field_permutation_canary"
TURN_NAMES = PRIMARY_TURNS + (CANARY_TURN,)
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V90_ROOT.parent / "judge-calibration-v5_4-v94-systematic-field-audit"
).resolve()

FIXTURE_AUDIT_PATH = (
    Path(__file__).resolve().parent / "evaluation/judge_fixture_truth_audit_v3.json"
)
CODEBOOK_PATH = (
    Path(__file__).resolve().parent.parent
    / "label_packs/ai_discourse_v3_1/codebook.md"
)
SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent
    / "label_packs/ai_discourse_v3_1/schema.json"
)


class JudgeV5CalibrationV94Error(RuntimeError):
    """The v94 systematic field audit cannot preserve its frozen contract."""


def field_rubric_v94() -> dict[str, Any]:
    return {
        "schema_version": V94_RUBRIC_VERSION,
        "audit_scope": ["certainty", "target"],
        "field_contracts": {
            "target": {
                "field": "target",
                "definition": (
                    "The object, system, behavior, population, or concept to which "
                    "the event's proposition applies."
                ),
                "decision_rule": (
                    "Correct when the existing target preserves the source referent and "
                    "material scope and is coherent with the event's claim. Incorrect when "
                    "it changes the referent or scope, or names a target that does not fit "
                    "the proposition the event actually claims. Harmless paraphrase and "
                    "coreference are correct. A merge/split difference alone is not a target "
                    "error unless the target itself omits or changes a material target."
                ),
            },
            "certainty": {
                "field": "certainty",
                "definition": (
                    "The modal strength, confidence, or epistemic commitment expressed by "
                    "the source for the event's proposition."
                ),
                "decision_rule": (
                    "The enum is low, medium, high, or hedged. Medium is a valid neutral "
                    "mapping for an unhedged assertion with no explicit exceptional confidence "
                    "or uncertainty; never upgrade medium to high merely because a sentence is "
                    "declarative. Medium and high may both be compatible with an ordinary "
                    "unqualified assertion when the source does not license a finer distinction. "
                    "Explicit may/might/could/estimate/uncertainty conflicts with high. Explicit "
                    "guarantee/proof/certainty conflicts with low or hedged. When the event adds "
                    "an absolute or strong commitment absent from the source, high is incorrect "
                    "even if the event's own unsupported claim uses assertive wording."
                ),
            },
        },
        "decision_statuses": ["correct", "incorrect", "abstain"],
        "abstention_rule": (
            "Use abstain only when the source and event genuinely cannot determine whether the "
            "existing requested field is compatible after applying the field convention."
        ),
        "whole_event_error_propagation": False,
        "prior_labels_available_to_model": False,
        "semantic_regex_or_keyword_rules_used": False,
        "majority_voting_used": False,
    }


def _validate_predecessors(
    *, v90_root: Path, v93_root: Path, v23_root: Path
) -> dict[str, Any]:
    paths = {
        "v90_terminal": v90_root / "terminal.json",
        "v90_receipt": v90_root / "reference-receipt.json",
        "v90_truth": v90_root / "calibration-truth-v5.private.json",
        "v93_terminal": v93_root / "terminal.json",
        "v93_score": v93_root / "repair-scoring.json",
        "v93_audit": v93_root / "repair-scoring-audit.json",
        "v23_pointwise": v23_root / "pointwise-input-full.private.json",
        "fixture_audit": FIXTURE_AUDIT_PATH,
        "codebook": CODEBOOK_PATH,
        "schema": SCHEMA_PATH,
    }
    values = {name: _load_json(path, name) for name, path in paths.items() if path.suffix == ".json"}
    v90_terminal = values["v90_terminal"]
    v90_receipt = values["v90_receipt"]
    v93_terminal = values["v93_terminal"]
    if (
        v90_terminal.get("state") != "completed"
        or v90_terminal.get("reference_frozen") is not True
        or v90_terminal.get("fresh_diagnostic_authorized") is not True
        or v90_terminal.get("full_calibration_authorized") is not False
        or v90_terminal.get("selection_authorized") is not False
        or v90_terminal.get("holdout_authorized") is not False
        or v90_terminal.get("production_mutated") is not False
        or not _record_matches(v90_terminal.get("truth"), paths["v90_truth"])
        or not _record_matches(v90_terminal.get("reference_receipt"), paths["v90_receipt"])
        or v90_receipt.get("case_count") != 66
        or v90_receipt.get("witness_count") != 182
        or v90_receipt.get("reference_frozen") is not True
        or v93_terminal.get("state") != "completed"
        or v93_terminal.get("terminal_reason")
        != "v93_repair_trigger_cleared_residual_reference_audit_authorized"
        or v93_terminal.get("residual_reference_audit_authorized") is not True
        or v93_terminal.get("fresh_full_development_calibration_authorized") is not False
        or v93_terminal.get("selection_authorized") is not False
        or v93_terminal.get("holdout_authorized") is not False
        or v93_terminal.get("production_mutated") is not False
        or v93_terminal.get("semantic_attempt_started") is not False
        or v93_terminal.get("usage_status") != "complete"
        or v93_terminal.get("usage", {}).get("total_tokens") != 0
        or not _record_matches(v93_terminal.get("score"), paths["v93_score"])
        or not _record_matches(v93_terminal.get("audit"), paths["v93_audit"])
        or not all(_verify_record(row) for row in (v93_terminal.get("predecessor") or {}).values())
        or len(values["v23_pointwise"].get("units") or []) != 182
        or values["v90_truth"].get("case_count") != 66
        or values["v90_truth"].get("witness_count") != 182
        or values["fixture_audit"].get("schema_version")
        != "pif_judge_fixture_truth_audit_v3"
    ):
        raise JudgeV5CalibrationV94Error("v90/v93/reference source contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def _task_id(role: str, field: str, identity: str) -> str:
    return "audit_" + sha256_text(f"v94|{role}|{field}|{identity}")[:24]


def _canary_task_id(owner_task_id: str) -> str:
    return "perm_" + sha256_text(f"v94|canary|{owner_task_id}")[:24]


def _payload_group(task: Mapping[str, Any]) -> str:
    value = {key: task[key] for key in ("field", "field_contract", "source_excerpt", "structured_event")}
    return sha256_text(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True))


def _control_rows(rubric: Mapping[str, Any]) -> list[tuple[dict[str, Any], str]]:
    contracts = rubric["field_contracts"]
    rows = [
        (
            {
                "field": "target",
                "source_excerpt": "The inspection system detects surface cracks on the assembly line.",
                "structured_event": {
                    "claim_text": "The inspection system detects surface cracks.",
                    "target_name": "surface cracks",
                    "evidence": "The inspection system detects surface cracks on the assembly line.",
                },
            },
            "correct",
        ),
        (
            {
                "field": "target",
                "source_excerpt": "The assistant ranks applicants for recruiter review.",
                "structured_event": {
                    "claim_text": "Applicants are ranked for a recruiter's review.",
                    "target_name": "applicant ranking for recruiter review",
                    "evidence": "The assistant ranks applicants for recruiter review.",
                },
            },
            "correct",
        ),
        (
            {
                "field": "target",
                "source_excerpt": "The inspection system detects surface cracks on the assembly line.",
                "structured_event": {
                    "claim_text": "The inspection system detects surface cracks.",
                    "target_name": "billing decisions",
                    "evidence": "The inspection system detects surface cracks on the assembly line.",
                },
            },
            "incorrect",
        ),
        (
            {
                "field": "target",
                "source_excerpt": "The filter blocks files with known malware signatures.",
                "structured_event": {
                    "claim_text": "The filter blocks every malicious file.",
                    "target_name": "every malicious file",
                    "evidence": "The filter blocks files with known malware signatures.",
                },
            },
            "incorrect",
        ),
        (
            {
                "field": "certainty",
                "source_excerpt": "The cache reduced median latency by 18 percent.",
                "structured_event": {
                    "claim_text": "The cache reduced median latency by 18 percent.",
                    "certainty": "medium",
                    "evidence": "The cache reduced median latency by 18 percent.",
                },
            },
            "correct",
        ),
        (
            {
                "field": "certainty",
                "source_excerpt": "The team says the migration may finish in June.",
                "structured_event": {
                    "claim_text": "The migration may finish in June.",
                    "certainty": "hedged",
                    "evidence": "The team says the migration may finish in June.",
                },
            },
            "correct",
        ),
        (
            {
                "field": "certainty",
                "source_excerpt": "The vendor guarantees the service will remain available.",
                "structured_event": {
                    "claim_text": "The vendor guarantees service availability.",
                    "certainty": "high",
                    "evidence": "The vendor guarantees the service will remain available.",
                },
            },
            "correct",
        ),
        (
            {
                "field": "certainty",
                "source_excerpt": "The team says the migration may finish in June.",
                "structured_event": {
                    "claim_text": "The migration will finish in June.",
                    "certainty": "high",
                    "evidence": "The team says the migration may finish in June.",
                },
            },
            "incorrect",
        ),
        (
            {
                "field": "certainty",
                "source_excerpt": "The vendor guarantees the service will remain available.",
                "structured_event": {
                    "claim_text": "The service might remain available.",
                    "certainty": "hedged",
                    "evidence": "The vendor guarantees the service will remain available.",
                },
            },
            "incorrect",
        ),
        (
            {
                "field": "certainty",
                "source_excerpt": "The audit proves the control always rejects invalid requests.",
                "structured_event": {
                    "claim_text": "The control rejects invalid requests.",
                    "certainty": "low",
                    "evidence": "The audit proves the control always rejects invalid requests.",
                },
            },
            "incorrect",
        ),
    ]
    rendered = []
    for ordinal, (row, status) in enumerate(rows):
        field = row["field"]
        task = {
            "task_id": _task_id("control", field, str(ordinal)),
            "field": field,
            "field_contract": deepcopy(contracts[field]),
            "source_excerpt": row["source_excerpt"],
            "structured_event": deepcopy(row["structured_event"]),
        }
        rendered.append((task, status))
    return rendered


def build_v94_inputs(
    *, pointwise: Mapping[str, Any], reference: Mapping[str, Any], rubric: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    contracts = rubric["field_contracts"]
    challenges = []
    for unit in pointwise.get("units") or []:
        case_id = unit["case_id"]
        witness_id = unit["witness_id"]
        issues = set(reference["cases"][case_id]["field_issues"].get(witness_id, []))
        for field in sorted(set(CHALLENGE_COUNTS) & issues):
            task = {
                "task_id": _task_id("challenge", field, f"{case_id}|{witness_id}"),
                "field": field,
                "field_contract": deepcopy(contracts[field]),
                "source_excerpt": unit["source_excerpt"],
                "structured_event": compact_empty_event_fields(deepcopy(unit["structured_event"])),
            }
            challenges.append(
                (
                    task,
                    {
                        "task_id": task["task_id"],
                        "role": "reference_challenge",
                        "case_id": case_id,
                        "witness_id": witness_id,
                        "field": field,
                        "prior_status": "incorrect",
                        "payload_group": _payload_group(task),
                    },
                )
            )
    counts = Counter(task["field"] for task, _ in challenges)
    if dict(counts) != CHALLENGE_COUNTS:
        raise JudgeV5CalibrationV94Error("v94 challenge coverage drifted")

    tasks = [task for task, _ in challenges]
    truth_rows = [truth for _, truth in challenges]
    for task, expected_status in _control_rows(rubric):
        tasks.append(task)
        truth_rows.append(
            {
                "task_id": task["task_id"],
                "role": "settled_control",
                "case_id": None,
                "witness_id": None,
                "field": task["field"],
                "prior_status": None,
                "control_expected_status": expected_status,
                "payload_group": _payload_group(task),
            }
        )
    if len(tasks) != PRIMARY_TASK_COUNT or len({row["task_id"] for row in tasks}) != PRIMARY_TASK_COUNT:
        raise JudgeV5CalibrationV94Error("v94 primary task coverage drifted")
    tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    task_by_id = {row["task_id"]: row for row in tasks}

    by_field_role: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in truth_rows:
        by_field_role[(row["field"], row["role"])].append(row["task_id"])
    canary_owner_ids = []
    for field in sorted(CHALLENGE_COUNTS):
        canary_owner_ids.extend(
            sorted(
                by_field_role[(field, "reference_challenge")],
                key=lambda task_id: sha256_text(f"v94|challenge-canary|{task_id}"),
            )[:2]
        )
        canary_owner_ids.extend(
            sorted(
                by_field_role[(field, "settled_control")],
                key=lambda task_id: sha256_text(f"v94|control-canary|{task_id}"),
            )[:1]
        )
    if len(canary_owner_ids) != 6 or len(set(canary_owner_ids)) != 6:
        raise JudgeV5CalibrationV94Error("v94 canary coverage drifted")
    canary_tasks = []
    canary_map = []
    for owner_task_id in reversed(canary_owner_ids):
        task = deepcopy(task_by_id[owner_task_id])
        canary_task_id = _canary_task_id(owner_task_id)
        task["task_id"] = canary_task_id
        canary_tasks.append(task)
        canary_map.append(
            {"canary_task_id": canary_task_id, "owner_task_id": owner_task_id}
        )

    value = {
        "schema_version": V94_INPUT_VERSION,
        "task_count": PRIMARY_TASK_COUNT,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth = {
        "schema_version": V94_TRUTH_VERSION,
        "reference_version": reference["reference_version"],
        "task_count": PRIMARY_TASK_COUNT,
        "tasks": truth_rows,
        "canary_map": canary_map,
    }
    canary = {
        "schema_version": V94_INPUT_VERSION,
        "task_count": 6,
        "tasks": canary_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "permutation_canary": True,
    }
    selection = {
        "schema_version": V94_SELECTION_VERSION,
        "created_at": now_iso(),
        "source_witness_count": len(pointwise.get("units") or []),
        "challenge_count": sum(CHALLENGE_COUNTS.values()),
        "challenge_field_counts": dict(sorted(CHALLENGE_COUNTS.items())),
        "settled_control_count": CONTROL_COUNT,
        "settled_control_status_counts": dict(
            sorted(
                Counter(
                    row["control_expected_status"]
                    for row in truth_rows
                    if row["role"] == "settled_control"
                ).items()
            )
        ),
        "permutation_canary_count": 6,
        "selection_uses_source_text": False,
        "selection_rule": "all_reference_v5_target_and_certainty_issues_plus_frozen_minimal_pair_controls",
        "empty_event_fields_omitted_only": True,
        "semantic_pruning_performed": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return value, truth, canary, selection


def primary_shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    tasks = value.get("tasks") or []
    if len(tasks) != PRIMARY_TASK_COUNT:
        raise JudgeV5CalibrationV94Error("v94 shard source coverage drifted")
    shards = [
        {
            **{key: deepcopy(child) for key, child in value.items() if key != "tasks"},
            "task_count": len(tasks[index : index + 4]),
            "tasks": deepcopy(tasks[index : index + 4]),
            "shard_ordinal": index // 4,
            "shard_count": len(PRIMARY_TURNS),
        }
        for index in range(0, len(tasks), 4)
    ]
    if len(shards) != len(PRIMARY_TURNS):
        raise JudgeV5CalibrationV94Error("v94 shard count drifted")
    return shards


def merge_outputs(outputs: Sequence[Mapping[str, Any]], expected_count: int) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(decisions) != expected_count or len({row["task_id"] for row in decisions}) != expected_count:
        raise JudgeV5CalibrationV94Error("v94 output coverage drifted")
    return {"schema_version": V94_OUTPUT_VERSION, "decisions": decisions}


def base_instructions_v94() -> str:
    return (
        "You are the neutral reference owner for a blinded source-to-structured-field audit. "
        "For each task, align the event's claim and evidence to the source, then judge only the "
        "existing requested field under the supplied field_contract. Do not propagate unrelated "
        "whole-event errors into the requested field, but require the requested field to be valid "
        "for both the source and the proposition the event actually claims. For certainty, medium "
        "is a valid neutral value for an ordinary unhedged assertion; never require high merely "
        "because grammar is declarative. Medium and high may both be compatible when the source "
        "does not license a finer distinction. Explicit hedging conflicts with high, and an event's "
        "added absolute commitment is incorrect when absent from the source. For target, preserve "
        "referent and material scope and accept harmless paraphrase or coreference. The first "
        "evidence span must exactly express the aligned proposition. Abstain only when the source "
        "genuinely cannot determine compatibility after applying the contract. Do not use regex, "
        "keywords, overlap, embeddings, prior labels, system identity, confidence, or voting."
    )


def build_prompt_v94(value: Mapping[str, Any]) -> str:
    return (
        "Return one field decision for every opaque task_id. Do not compare tasks or emit whole-event "
        "verdicts. Every source_evidence_span must be an exact substring of that task's source_excerpt.\n\n"
        + json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    )


def score_v94(
    owner: Mapping[str, Any],
    canary: Mapping[str, Any],
    truth: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for row in owner["decisions"]}
    repeated = {row["task_id"]: row for row in canary["decisions"]}
    if set(expected) != set(observed) or len(repeated) != 6:
        raise JudgeV5CalibrationV94Error("v94 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "settled_control"]
    challenges = [row for row in expected.values() if row["role"] == "reference_challenge"]
    if len(controls) != CONTROL_COUNT or len(challenges) != sum(CHALLENGE_COUNTS.values()):
        raise JudgeV5CalibrationV94Error("v94 truth role coverage drifted")
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    canary_exact = sum(
        repeated[row["canary_task_id"]]["field_status"]
        == observed[row["owner_task_id"]]["field_status"]
        for row in truth["canary_map"]
    )
    owner_abstentions = sum(row["field_status"] == "abstain" for row in observed.values())
    canary_abstentions = sum(row["field_status"] == "abstain" for row in repeated.values())
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    evidence_complete += sum(bool(row["source_evidence_spans"]) for row in repeated.values())
    grouped: dict[str, set[str]] = defaultdict(set)
    for row in challenges:
        grouped[row["payload_group"]].add(observed[row["task_id"]]["field_status"])
    exact_payload_consistent = all(len(statuses) == 1 for statuses in grouped.values())
    checks = {
        "settled_control_exact_rate": control_exact == CONTROL_COUNT,
        "permutation_canary_exact_rate": canary_exact == 6,
        "owner_abstention_count": owner_abstentions == 0,
        "canary_abstention_count": canary_abstentions == 0,
        "evidence_complete_rate": evidence_complete == PRIMARY_TASK_COUNT + 6,
        "exact_payload_consistency": exact_payload_consistent,
    }
    passed = all(checks.values())
    proposal = [
        {
            "case_id": row["case_id"],
            "witness_id": row["witness_id"],
            "field": row["field"],
            "prior_status": row["prior_status"],
            "owner_status": observed[row["task_id"]]["field_status"],
            "reference_change": observed[row["task_id"]]["field_status"] != row["prior_status"],
        }
        for row in sorted(challenges, key=lambda child: child["task_id"])
    ]
    change_counts = Counter(
        row["field"] for row in proposal if row["reference_change"]
    )
    return {
        "schema_version": V94_SCORE_VERSION,
        "passed": passed,
        "metrics": {
            "task_count": PRIMARY_TASK_COUNT,
            "reference_challenge_count": len(challenges),
            "reference_challenge_field_counts": dict(sorted(CHALLENGE_COUNTS.items())),
            "settled_control_count": CONTROL_COUNT,
            "settled_control_exact_count": control_exact,
            "settled_control_exact_rate": round(control_exact / CONTROL_COUNT, 6),
            "permutation_canary_count": 6,
            "permutation_canary_exact_count": canary_exact,
            "permutation_canary_exact_rate": round(canary_exact / 6, 6),
            "owner_abstention_count": owner_abstentions,
            "canary_abstention_count": canary_abstentions,
            "evidence_complete_count": evidence_complete,
            "evidence_complete_rate": round(evidence_complete / (PRIMARY_TASK_COUNT + 6), 6),
            "exact_payload_group_count": len(grouped),
            "inconsistent_exact_payload_group_count": sum(len(statuses) != 1 for statuses in grouped.values()),
            "reference_change_count": sum(row["reference_change"] for row in proposal),
            "reference_change_field_counts": dict(sorted(change_counts.items())),
        },
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "reference_patch_proposal": proposal if passed else [],
        "reference_patch_authorized": passed,
        "reference_freeze_authorized": False,
        "fresh_diagnostic_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "gates_frozen_before_semantic_calls": True,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V94_CAPACITY_AUDIT_VERSION,
        "phase_id": V94_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_created(audit_path, audit, "v94 capacity audit")
    policy = {
        "schema_version": V94_CAPACITY_POLICY_VERSION,
        "phase_id": V94_PHASE_ID,
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
    _write_stable_created(policy_path, policy, "v94 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v94(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v90_root: Path = DEFAULT_V90_ROOT,
    v93_root: Path = DEFAULT_V93_ROOT,
    v23_root: Path = V23_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors(
        v90_root=v90_root.resolve(), v93_root=v93_root.resolve(), v23_root=v23_root.resolve()
    )
    rubric = field_rubric_v94()
    rubric_path = root / "field-rubric.json"
    _write_immutable(rubric_path, rubric)
    value, truth, canary, selection = build_v94_inputs(
        pointwise=predecessor["values"]["v23_pointwise"],
        reference=predecessor["values"]["v90_truth"],
        rubric=rubric,
    )
    input_path = root / "systematic-field-input.private.json"
    truth_path = root / "systematic-field-truth.private.json"
    canary_path = root / "permutation-canary-input.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(canary_path, canary)
    _write_stable_created(selection_path, selection, "v94 selection audit")
    turn_values = primary_shards(value) + [canary]
    turns = []
    for turn_name, turn_value in zip(TURN_NAMES, turn_values, strict=True):
        prompt = build_prompt_v94(turn_value)
        schema = output_schema(turn_value)
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=turn_value, prompt=prompt, schema=schema
        )
        turns.append(
            {
                "turn_name": turn_name,
                "value": turn_value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    capacity = _build_capacity_policy(root, predecessor["records"])
    runtime_dir = Path(__file__).resolve().parent
    runtime_files = [
        Path(__file__),
        runtime_dir / "app_server_judge_v5_calibration_v93_repair_scoring_receipt.py",
        runtime_dir / "app_server_judge_v5_calibration_v90_reference_v5_freeze.py",
        runtime_dir / "app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v75_exact_span_remaining_shard.py",
        runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py",
        runtime_dir / "app_server_judge_v5_fixture.py",
        runtime_dir / "app_server_capacity_reserve.py",
        runtime_dir / "codex_app_server.py",
    ]
    spec = {
        "schema_version": V94_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "complete_target_certainty_issue_audit_with_frozen_field_semantics_controls_and_canaries",
        "task_count": PRIMARY_TASK_COUNT,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_controls_all_canaries_all_evidence_zero_abstentions_exact_payload_consistency",
        "reference_patch_authorized": False,
        "reference_freeze_authorized": False,
        "fresh_diagnostic_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [_record(path) for path in runtime_files],
        "frozen_inputs": {
            "rubric": _record(rubric_path),
            "input": _record(input_path),
            "truth": _record(truth_path),
            "canary": _record(canary_path),
            "selection_audit": _record(selection_path),
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "systematic-field-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v94 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV94Error("immutable v94 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "truth": truth,
        "turns": turns,
        "capacity_policy": capacity["policy"],
    }


def _real_attempts(root: Path) -> list[dict[str, Any]]:
    return [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = _real_attempts(root)
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v94 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V94_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "attempts": attempts,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V94_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_patch_authorized": False,
        "reference_freeze_authorized": False,
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


async def run_v94(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v94 terminal")
    frozen = freeze_v94(output_dir=root, timeout_seconds=timeout_seconds)
    factory = client_factory or _client_factory
    primary_outputs = []
    canary_outputs = []
    sidecars = []
    operations = []
    adoptions = {}
    current_turn = None
    try:
        async with factory(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=base_instructions_v94(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(turn["value"]["tasks"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, value=turn["value"]: validate_output(
                        project_exact_spans(candidate, value)[0], value
                    ),
                )
                projected, turn_operations = project_exact_spans(output, turn["value"])
                if validate_output(projected, turn["value"]):
                    raise JudgeV5CalibrationV94Error("projected v94 output is invalid")
                if current_turn == CANARY_TURN:
                    canary_outputs.append(projected)
                else:
                    primary_outputs.append(projected)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
                operations.extend(
                    [{**row, "turn_name": current_turn} for row in turn_operations]
                )
        owner = merge_outputs(primary_outputs, PRIMARY_TASK_COUNT)
        canary = merge_outputs(canary_outputs, 6)
        owner_path = root / "systematic-field-output.private.json"
        canary_path = root / "permutation-canary-output.private.json"
        _write_immutable(owner_path, owner)
        _write_immutable(canary_path, canary)
        audit = {
            "schema_version": V94_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "projection_scope": "nonexact_source_span_removal_only",
            "semantic_status_changed": False,
            "privacy": "opaque_task_ids_counts_and_span_hashes_only",
        }
        audit_path = root / "projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v94(owner, canary, frozen["truth"])
        score["projection_audit"] = _record(audit_path)
        score_path = root / "systematic-field-score.json"
        _write_immutable(score_path, score)
        proposal_path = root / "reference-patch-proposal.json"
        if score["passed"]:
            _write_immutable(
                proposal_path,
                {
                    "schema_version": V94_PATCH_VERSION,
                    "created_at": now_iso(),
                    "changes": score["reference_patch_proposal"],
                    "reference_freeze_authorized": False,
                    "fresh_diagnostic_authorized": False,
                },
            )
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V94_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v94_systematic_field_audit_passed_reference_patch_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v94_systematic_field_audit_passed_reference_patch_authorized"
                if passed
                else "v94_systematic_field_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reference_patch_authorized": passed,
            "reference_freeze_authorized": False,
            "fresh_diagnostic_authorized": False,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(owner_path),
            "canary_output": _record(canary_path),
            "patch_proposal": _record(proposal_path) if passed else None,
            "projection_audit": _record(audit_path),
            "attempts": _real_attempts(root),
            "completed_checkpoint_adoptions": adoptions,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v94 systematic target/certainty audit")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v94(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_patch_authorized": terminal.get("reference_patch_authorized", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
