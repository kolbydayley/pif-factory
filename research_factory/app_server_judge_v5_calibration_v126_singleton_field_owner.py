from __future__ import annotations

"""Isolated single-task Sol owners for the three v125 permutation disagreements."""

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
from .app_server_judge_v5_calibration_v123_capped_field_repair import build_prompt_v123
from .app_server_judge_v5_calibration_v125_final_field_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V125_ROOT,
    _validate_v119,
    _validate_v124,
    build_reference_candidate_v125,
    reconcile_v125,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V126_INPUT_VERSION = "pif_app_server_judge_v5_4_v126_singleton_field_owner_input_v1"
V126_TRUTH_VERSION = "pif_app_server_judge_v5_4_v126_singleton_field_owner_truth_v1"
V126_SELECTION_VERSION = "pif_app_server_judge_v5_4_v126_selection_v1"
V126_SPEC_VERSION = "pif_app_server_judge_v5_4_v126_spec_v1"
V126_SCORE_VERSION = "pif_app_server_judge_v5_4_v126_score_v1"
V126_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v126_final_owner_output_v1"
V126_REFERENCE_VERSION = (
    "pif_app_server_judge_v5_4_calibration_truth_v10_retained_field_singleton_owner_frozen"
)
V126_FAILURE_VERSION = "pif_app_server_judge_v5_4_v126_failure_v1"
V126_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v126_terminal_v1"
V126_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V126_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V126_PHASE_ID = "judge_v5_4_v126_singleton_retained_field_owner"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
CONTROL_TURNS = tuple(f"singleton_field_control_{index:02d}" for index in range(3))
OWNER_TURNS = tuple(f"singleton_field_owner_{index:02d}" for index in range(3))
TURN_NAMES = CONTROL_TURNS + OWNER_TURNS
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V125_ROOT.parent / "judge-calibration-v5_4-v126-singleton-field-owner"
).resolve()


class JudgeV5CalibrationV126Error(RuntimeError):
    """The v126 singleton-owner contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v125() -> dict[str, Any]:
    root = DEFAULT_V125_ROOT
    paths = {
        "spec": root / "final-owner-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "final-owner-score.json",
        "input": root / "final-owner-input.private.json",
        "truth": root / "final-owner-truth.private.json",
        "canary_input": root / "permutation-canary-input.private.json",
        "primary": root / "final-owner-output.private.json",
        "canary": root / "permutation-canary-output.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v125 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v125_final_field_owner_quality_gate_not_passed"
        or terminal.get("retained_field_reference_patch_authorized") is not False
        or terminal.get("fresh_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 51220
        or score.get("passed") is not False
        or score.get("failed_checks") != ["permutation_canary_exact_rate"]
        or score.get("metrics", {}).get("matched_control_exact_count") != 6
        or score.get("metrics", {}).get("permutation_canary_exact_count") != 3
        or score.get("metrics", {}).get("evidence_complete_count") != 18
        or spec.get("model") != MODEL
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV126Error("v125 predecessor contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5CalibrationV126Error("v125 runtime record drifted")
    attempts = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_name in spec["turn_plan"]:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        turn_paths = {
            name: turn_root / filename
            for name, filename in {
                "capacity": "capacity.json",
                "sidecar": "sidecar.json",
                "output": "output.private.json",
            }.items()
        }
        if any(not path.is_file() for path in turn_paths.values()):
            raise JudgeV5CalibrationV126Error("v125 turn coverage is incomplete")
        measured = _validate_usage(_load_json(turn_paths["sidecar"], "v125 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = {name: _record(path) for name, path in turn_paths.items()}
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV126Error("v125 usage aggregate drifted")
    primary = {row["task_id"]: row for row in values["primary"]["decisions"]}
    canary = {row["task_id"]: row for row in values["canary"]["decisions"]}
    truth = {row["task_id"]: row for row in values["truth"]["tasks"]}
    unstable = []
    stable = []
    for mapping in values["truth"]["canary_map"]:
        owner_id, canary_id = mapping["owner_task_id"], mapping["canary_task_id"]
        target = unstable if primary[owner_id]["field_status"] != canary[canary_id]["field_status"] else stable
        target.append(owner_id)
    if (
        len(unstable) != 3
        or len(stable) != 3
        or Counter(truth[task_id]["field"] for task_id in unstable)
        != {"attribution": 2, "unsupported_inference": 1}
    ):
        raise JudgeV5CalibrationV126Error("v125 observable disagreement coverage drifted")
    v124 = _validate_v124()
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "unstable_owner_ids": sorted(unstable),
        "stable_owner_ids": sorted(stable),
        "v124": v124,
    }


def _singleton_task_id(role: str, source_task_id: str) -> str:
    return role + "_" + sha256_text(f"v126|{role}|{source_task_id}")[:24]


def _one_task_value(task: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": V126_INPUT_VERSION,
        "task_count": 1,
        "tasks": [deepcopy(task)],
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "singleton_context": True,
    }


def build_v126_inputs(
    v125: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    v125_input = {row["task_id"]: row for row in v125["values"]["input"]["tasks"]}
    v125_truth = {row["task_id"]: row for row in v125["values"]["truth"]["tasks"]}
    owner_rows = []
    for owner_id in v125["unstable_owner_ids"]:
        source = deepcopy(v125_input[owner_id])
        task_id = _singleton_task_id("owner", v125_truth[owner_id]["source_task_id"])
        source["task_id"] = task_id
        owner_rows.append(
            {
                "turn_role": "singleton_owner",
                "value": _one_task_value(source),
                "truth": {
                    "task_id": task_id,
                    "v125_owner_task_id": owner_id,
                    "source_task_id": v125_truth[owner_id]["source_task_id"],
                    "role": "singleton_owner",
                    "field": v125_truth[owner_id]["field"],
                },
            }
        )
    owner_rows.sort(key=lambda row: sha256_text(f"v126|owner-order|{row['truth']['task_id']}"))

    v121_truth = {
        row["task_id"]: row for row in v125["v124"]["v122"]["v121"]["values"]["truth"]["tasks"]
    }
    v121_input = {
        row["task_id"]: row for row in v125["v124"]["v122"]["v121_input"]["tasks"]
    }
    v121_primary = {
        row["task_id"]: row for row in v125["v124"]["v122"]["values"]["primary"]["decisions"]
    }
    control_rows = []
    needed = Counter({"attribution": 2, "unsupported_inference": 1})
    for field, count in sorted(needed.items()):
        candidates = [
            row
            for row in v121_truth.values()
            if row["role"] == "matched_control"
            and row["field"] == field
            and v121_primary[row["task_id"]]["field_status"] == row["control_expected_status"]
            and v121_primary[row["task_id"]]["field_status"] != "abstain"
        ]
        candidates.sort(
            key=lambda row: (
                row["control_expected_status"] != "incorrect",
                sha256_text(f"v126|control-rank|{field}|{row['task_id']}"),
            )
        )
        if len(candidates) < count:
            raise JudgeV5CalibrationV126Error("v126 singleton control coverage drifted")
        for row in candidates[:count]:
            source = deepcopy(v121_input[row["task_id"]])
            task_id = _singleton_task_id("control", row["task_id"])
            source["task_id"] = task_id
            control_rows.append(
                {
                    "turn_role": "singleton_control",
                    "value": _one_task_value(source),
                    "truth": {
                        "task_id": task_id,
                        "source_task_id": row["task_id"],
                        "role": "singleton_control",
                        "field": field,
                        "control_expected_status": row["control_expected_status"],
                    },
                }
            )
    control_rows.sort(key=lambda row: sha256_text(f"v126|control-order|{row['truth']['task_id']}"))
    rows = control_rows + owner_rows
    for turn_name, row in zip(TURN_NAMES, rows, strict=True):
        row["turn_name"] = turn_name
    truth = {
        "schema_version": V126_TRUTH_VERSION,
        "task_count": 6,
        "singleton_control_count": 3,
        "singleton_owner_count": 3,
        "tasks": [deepcopy(row["truth"]) for row in rows],
    }
    selection = {
        "schema_version": V126_SELECTION_VERSION,
        "created_at": now_iso(),
        "singleton_control_count": 3,
        "singleton_owner_count": 3,
        "owner_field_counts": dict(sorted(Counter(row["truth"]["field"] for row in owner_rows).items())),
        "control_field_counts": dict(sorted(Counter(row["truth"]["field"] for row in control_rows).items())),
        "maximum_tasks_per_turn": 1,
        "task_order_effect_removed_by_construction": True,
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "singleton_owner_is_decisive": True,
        "majority_voting_used": False,
        "semantic_pruning_performed": False,
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return rows, truth, selection


def score_v126(outputs: Mapping[str, Mapping[str, Any]], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {}
    for output in outputs.values():
        for row in output["decisions"]:
            if row["task_id"] in observed:
                raise JudgeV5CalibrationV126Error("v126 duplicate output task")
            observed[row["task_id"]] = row
    if set(expected) != set(observed):
        raise JudgeV5CalibrationV126Error("v126 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "singleton_control"]
    owners = [row for row in expected.values() if row["role"] == "singleton_owner"]
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    owner_abstentions = sum(observed[row["task_id"]]["field_status"] == "abstain" for row in owners)
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    checks = {
        "singleton_control_exact_rate": control_exact == 3,
        "singleton_owner_abstention_count": owner_abstentions == 0,
        "evidence_complete_rate": evidence_complete == 6,
        "task_order_effect_removed_by_construction": True,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V126_SCORE_VERSION,
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
            "inherited_v125_control_exact_count": 6,
            "inherited_v125_stable_canary_exact_count": 3,
        },
        "retained_field_reference_patch_authorized": passed,
        "proposition_reference_frozen": False,
        "alignment_reference_frozen": False,
        "fresh_diagnostic_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "singleton_owner_is_decisive": True,
        "majority_voting_used": False,
    }


def finalize_v125_owner(
    *, v125: Mapping[str, Any], truth: Mapping[str, Any], outputs: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    final = deepcopy(v125["values"]["primary"])
    final_rows = {row["task_id"]: row for row in final["decisions"]}
    observed = {row["task_id"]: row for output in outputs.values() for row in output["decisions"]}
    changed = 0
    for row in truth["tasks"]:
        if row["role"] != "singleton_owner":
            continue
        target = final_rows[row["v125_owner_task_id"]]
        source = observed[row["task_id"]]
        target["field_status"] = source["field_status"]
        target["source_evidence_spans"] = deepcopy(source["source_evidence_spans"])
        target["rationale"] = "Decisive isolated single-task Sol owner decision."
        changed += 1
    if changed != 3:
        raise JudgeV5CalibrationV126Error("v126 final-owner projection drifted")
    final["schema_version"] = V126_OUTPUT_VERSION
    final["singleton_owner_change_count"] = changed
    final["singleton_owner_basis"] = "v126_isolated_sol_owner"
    return final


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V126_CAPACITY_AUDIT_VERSION,
        "phase_id": V126_PHASE_ID,
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
        "schema_version": V126_CAPACITY_POLICY_VERSION,
        "phase_id": V126_PHASE_ID,
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


def freeze_v126(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v126 terminal")}
    v125 = _validate_v125()
    v119 = _validate_v119()
    rows, truth, selection = build_v126_inputs(v125)
    truth_path = root / "singleton-owner-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    turns = []
    for row in rows:
        value = row["value"]
        prompt, schema = build_prompt_v123(value), output_schema(value)
        paths = _freeze_turn_request(
            root=root, turn_name=row["turn_name"], input_value=value, prompt=prompt, schema=schema
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor = {
        **{f"v125_{name}": record for name, record in v125["records"].items()},
        "v125_attempts": v125["attempts"],
        **{f"v124_{name}": record for name, record in v125["v124"]["records"].items()},
        "v121_input": v125["v124"]["v122"]["v121_input_record"],
        **{f"v119_{name}": record for name, record in v119["records"].items()},
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V126_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "isolated_single_task_sol_owner_for_three_v125_order_disagreements_with_three_same_field_controls",
        "singleton_control_count": 3,
        "singleton_owner_count": 3,
        "maximum_tasks_per_turn": 1,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "disagreement_reasons_in_model_input": False,
        "singleton_owner_is_decisive": True,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "three_exact_singleton_controls_three_decisive_nonabstaining_singleton_owners_and_complete_exact_evidence",
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
            _record(runtime_dir / "app_server_judge_v5_calibration_v125_final_field_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v124_scoreable_recovery.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v123_capped_field_repair.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
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
    spec_path = root / "singleton-owner-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v125": v125,
        "v119": v119,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v126 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V126_FAILURE_VERSION,
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
        "schema_version": V126_TERMINAL_VERSION,
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


async def run_v126(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v126 terminal")
    frozen = freeze_v126(output_dir=root, timeout_seconds=timeout_seconds)
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
                    base_instructions=base_instructions_v115(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=1,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_output(candidate, item),
                )
                outputs[current_turn] = output
                sidecars.append(sidecar)
        merged_path = root / "singleton-owner-outputs.private.json"
        _write_immutable(merged_path, {"turns": outputs})
        score = score_v126(outputs, frozen["truth"])
        score_path = root / "singleton-owner-score.json"
        _write_immutable(score_path, score)
        final_owner_path = root / "final-owner-output.private.json"
        reconciled_path = root / "reconciled-retained-field-output.private.json"
        candidate_path = root / "calibration-truth-v10-retained-field-owner.private.json"
        if score["passed"]:
            final_owner = finalize_v125_owner(
                v125=frozen["v125"], truth=frozen["truth"], outputs=outputs
            )
            _write_immutable(final_owner_path, final_owner)
            reconciled = reconcile_v125(
                base_primary=frozen["v125"]["v124"]["v122"]["values"]["primary"],
                truth=frozen["v125"]["values"]["truth"],
                owner=final_owner,
            )
            _write_immutable(reconciled_path, reconciled)
            candidate = build_reference_candidate_v125(
                current_reference=frozen["v119"]["values"]["truth"],
                v121_truth=frozen["v125"]["v124"]["v122"]["v121"]["values"]["truth"],
                reconciled=reconciled,
                owner_truth=frozen["v125"]["values"]["truth"],
            )
            candidate["schema_version"] = V126_REFERENCE_VERSION
            candidate["reference_version"] = "fixture_reference_v10_retained_field_singleton_owner_frozen"
            candidate["retained_field_singleton_owner_count"] = 3
            _write_immutable(candidate_path, candidate)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V126_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v126_singleton_owner_passed_retained_field_patch_authorized"
                if passed else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v126_retained_field_reference_patch_authorized"
                if passed else "v126_singleton_field_owner_quality_gate_not_passed"
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
            "singleton_outputs": _record(merged_path),
            "final_owner_output": _record(final_owner_path) if passed else None,
            "reconciled_output": _record(reconciled_path) if passed else None,
            "reference_candidate": _record(candidate_path) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v125_usage": frozen["v125"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v126 singleton retained-field owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v126(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({"state": terminal["state"], "terminal_reason": terminal["terminal_reason"], "retained_field_reference_patch_authorized": terminal.get("retained_field_reference_patch_authorized", False), "usage_status": terminal.get("usage_status")}, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
