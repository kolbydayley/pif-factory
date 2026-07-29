from __future__ import annotations

"""Singleton Terra owner for the sole v128 proposition permutation disagreement."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
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
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import _record, _verify_record
from .app_server_judge_v5_calibration_v127_singleton_proposition_owner import (
    proposition_instructions,
    proposition_output_schema,
    proposition_prompt,
    validate_proposition_output,
)
from .app_server_judge_v5_calibration_v128_proposition_migration_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V128_ROOT,
    _validate_v127,
    build_reference_candidate_v128,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V129_INPUT_VERSION = "pif_app_server_judge_v5_4_v129_singleton_proposition_repair_input_v1"
V129_TRUTH_VERSION = "pif_app_server_judge_v5_4_v129_singleton_proposition_repair_truth_v1"
V129_SELECTION_VERSION = "pif_app_server_judge_v5_4_v129_selection_v1"
V129_SPEC_VERSION = "pif_app_server_judge_v5_4_v129_spec_v1"
V129_SCORE_VERSION = "pif_app_server_judge_v5_4_v129_score_v1"
V129_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v129_final_proposition_owner_output_v1"
V129_REFERENCE_VERSION = (
    "pif_app_server_judge_v5_4_calibration_truth_v10_retained_proposition_singleton_frozen"
)
V129_FAILURE_VERSION = "pif_app_server_judge_v5_4_v129_failure_v1"
V129_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v129_terminal_v1"
V129_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V129_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V129_PHASE_ID = "judge_v5_4_v129_singleton_proposition_repair"

MODEL = "gpt-5.6-terra"
EFFORT = "high"
CONTROL_TURNS = ("singleton_proposition_repair_control_00", "singleton_proposition_repair_control_01")
OWNER_TURN = "singleton_proposition_repair_owner"
TURN_NAMES = CONTROL_TURNS + (OWNER_TURN,)
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V128_ROOT.parent / "judge-calibration-v5_4-v129-singleton-proposition-repair"
).resolve()


class JudgeV5CalibrationV129Error(RuntimeError):
    """The v129 singleton proposition repair contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v128() -> dict[str, Any]:
    root = DEFAULT_V128_ROOT
    paths = {
        "spec": root / "proposition-migration-owner-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "proposition-migration-score.json",
        "input": root / "proposition-migration-input.private.json",
        "truth": root / "proposition-migration-truth.private.json",
        "canary_input": root / "permutation-canary-input.private.json",
        "primary": root / "proposition-migration-output.private.json",
        "canary": root / "permutation-canary-output.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v128 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v128_proposition_migration_owner_quality_gate_not_passed"
        or terminal.get("retained_proposition_reference_patch_authorized") is not False
        or terminal.get("proposition_reference_frozen") is not False
        or terminal.get("alignment_reference_frozen") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 45176
        or score.get("passed") is not False
        or score.get("failed_checks") != ["permutation_canary_exact_rate"]
        or score.get("metrics", {}).get("audited_control_exact_count") != 4
        or score.get("metrics", {}).get("permutation_canary_exact_count") != 5
        or score.get("metrics", {}).get("evidence_complete_count") != 16
        or spec.get("model") != "gpt-5.4"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV129Error("v128 predecessor contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5CalibrationV129Error("v128 runtime record drifted")
    for name, key in {
        "score": "score",
        "primary": "primary_output",
        "canary": "canary_output",
    }.items():
        record = terminal.get(key)
        if not isinstance(record, Mapping) or dict(record) != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV129Error(f"v128 {name} record drifted")
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
                raise JudgeV5CalibrationV129Error("v128 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(_load_json(Path(records["sidecar"]["path"]), "v128 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV129Error("v128 usage aggregate drifted")
    truth = {row["task_id"]: row for row in values["truth"]["tasks"]}
    primary = {row["task_id"]: row for row in values["primary"]["decisions"]}
    canary = {row["task_id"]: row for row in values["canary"]["decisions"]}
    triggers = []
    for mapping in values["truth"]["canary_map"]:
        if (
            primary[mapping["owner_task_id"]]["proposition_status"]
            != canary[mapping["canary_task_id"]]["proposition_status"]
        ):
            triggers.append(mapping["owner_task_id"])
    if len(triggers) != 1 or truth[triggers[0]]["migration_role"] != "legacy_unsupported_definition_migration":
        raise JudgeV5CalibrationV129Error("v128 sole repair trigger drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "trigger_id": triggers[0],
        "v127": _validate_v127(),
    }


def _new_task_id(role: str, source_task_id: str) -> str:
    return role + "_" + sha256_text(f"v129|{role}|{source_task_id}")[:24]


def _one_task_value(task: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": V129_INPUT_VERSION,
        "task_count": 1,
        "tasks": [deepcopy(task)],
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "singleton_context": True,
    }


def build_v129_inputs(
    v128: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    source = {row["task_id"]: row for row in v128["values"]["input"]["tasks"]}
    truth = {row["task_id"]: row for row in v128["values"]["truth"]["tasks"]}
    controls = [row for row in truth.values() if row["role"] == "audited_control"]
    selected_controls = []
    for status in ("supported", "unsupported"):
        selected_controls.append(
            min(
                (row for row in controls if row["control_expected_status"] == status),
                key=lambda row: sha256_text(f"v129|control|{status}|{row['task_id']}"),
            )
        )
    rows = []
    for row in selected_controls:
        task = deepcopy(source[row["task_id"]])
        task_id = _new_task_id("control", row["task_id"])
        task["task_id"] = task_id
        rows.append(
            {
                "turn_role": "singleton_control",
                "value": _one_task_value(task),
                "truth": {
                    "task_id": task_id,
                    "source_task_id": row["task_id"],
                    "role": "singleton_control",
                    "control_expected_status": row["control_expected_status"],
                },
            }
        )
    trigger = truth[v128["trigger_id"]]
    owner_task = deepcopy(source[v128["trigger_id"]])
    owner_id = _new_task_id("owner", v128["trigger_id"])
    owner_task["task_id"] = owner_id
    rows.append(
        {
            "turn_role": "singleton_owner",
            "value": _one_task_value(owner_task),
            "truth": {
                "task_id": owner_id,
                "source_task_id": v128["trigger_id"],
                "case_id": trigger["case_id"],
                "witness_id": trigger["witness_id"],
                "role": "singleton_owner",
                "migration_role": trigger["migration_role"],
            },
        }
    )
    for turn_name, row in zip(TURN_NAMES, rows, strict=True):
        row["turn_name"] = turn_name
    truth_value = {
        "schema_version": V129_TRUTH_VERSION,
        "task_count": 3,
        "singleton_control_count": 2,
        "singleton_owner_count": 1,
        "tasks": [deepcopy(row["truth"]) for row in rows],
    }
    selection = {
        "schema_version": V129_SELECTION_VERSION,
        "created_at": now_iso(),
        "observable_repair_trigger_count": 1,
        "trigger_role": "legacy_unsupported_definition_migration",
        "singleton_control_count": 2,
        "control_status_counts": {"supported": 1, "unsupported": 1},
        "maximum_tasks_per_turn": 1,
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "trigger_reason_in_model_input": False,
        "singleton_owner_is_decisive": True,
        "majority_voting_used": False,
        "privacy": "aggregate_counts_only",
    }
    return rows, truth_value, selection


def score_v129(outputs: Mapping[str, Mapping[str, Any]], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    observed = {row["task_id"]: row for output in outputs.values() for row in output["decisions"]}
    if set(expected) != set(observed):
        raise JudgeV5CalibrationV129Error("v129 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "singleton_control"]
    owner = next(row for row in expected.values() if row["role"] == "singleton_owner")
    control_exact = sum(
        observed[row["task_id"]]["proposition_status"] == row["control_expected_status"]
        for row in controls
    )
    owner_abstention = observed[owner["task_id"]]["proposition_status"] == "abstain"
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    checks = {
        "singleton_control_exact_rate": control_exact == 2,
        "singleton_owner_abstention_count": not owner_abstention,
        "evidence_complete_rate": evidence_complete == 3,
        "task_order_effect_removed_by_construction": True,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V129_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": 3,
            "singleton_control_count": 2,
            "singleton_control_exact_count": control_exact,
            "singleton_owner_count": 1,
            "singleton_owner_abstention_count": int(owner_abstention),
            "evidence_complete_count": evidence_complete,
            "inherited_v128_audited_control_exact_count": 4,
            "inherited_v128_stable_canary_exact_count": 5,
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


def finalize_v128_primary(
    *, v128: Mapping[str, Any], truth: Mapping[str, Any], outputs: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    final = deepcopy(v128["values"]["primary"])
    final_rows = {row["task_id"]: row for row in final["decisions"]}
    observed = {row["task_id"]: row for output in outputs.values() for row in output["decisions"]}
    owner = next(row for row in truth["tasks"] if row["role"] == "singleton_owner")
    source = observed[owner["task_id"]]
    target = final_rows[owner["source_task_id"]]
    target["proposition_status"] = source["proposition_status"]
    target["source_evidence_spans"] = deepcopy(source["source_evidence_spans"])
    target["rationale"] = "Decisive singleton Terra proposition owner decision."
    final["schema_version"] = V129_OUTPUT_VERSION
    final["singleton_repair_count"] = 1
    return final


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path, policy_path = root / "capacity-policy-audit.json", root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V129_CAPACITY_AUDIT_VERSION,
        "phase_id": V129_PHASE_ID,
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
        "schema_version": V129_CAPACITY_POLICY_VERSION,
        "phase_id": V129_PHASE_ID,
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


def freeze_v129(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v129 terminal")}
    v128 = _validate_v128()
    rows, truth, selection = build_v129_inputs(v128)
    truth_path = root / "singleton-proposition-repair-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    turns = []
    for row in rows:
        prompt, schema = proposition_prompt(row["value"]), proposition_output_schema(row["value"])
        paths = _freeze_turn_request(
            root=root, turn_name=row["turn_name"], input_value=row["value"], prompt=prompt, schema=schema
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor = {
        **{f"v128_{name}": record for name, record in v128["records"].items()},
        "v128_attempts": v128["attempts"],
        **{f"v127_{name}": record for name, record in v128["v127"]["records"].items()},
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V129_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "singleton_terra_owner_for_one_v128_permutation_disagreement_with_two_audited_controls",
        "singleton_control_count": 2,
        "singleton_owner_count": 1,
        "maximum_tasks_per_turn": 1,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "trigger_reason_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "two_exact_controls_one_decisive_owner_and_complete_exact_evidence",
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
            _record(runtime_dir / "app_server_judge_v5_calibration_v128_proposition_migration_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v127_singleton_proposition_owner.py"),
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
    spec_path = root / "singleton-proposition-repair-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v128": v128,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v129 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V129_FAILURE_VERSION,
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
        "schema_version": V129_TERMINAL_VERSION,
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


async def run_v129(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v129 terminal")
    frozen = freeze_v129(output_dir=root, timeout_seconds=timeout_seconds)
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
        outputs_path = root / "singleton-proposition-repair-outputs.private.json"
        _write_immutable(outputs_path, {"turns": outputs})
        score = score_v129(outputs, frozen["truth"])
        score_path = root / "singleton-proposition-repair-score.json"
        _write_immutable(score_path, score)
        final_path = root / "final-proposition-owner-output.private.json"
        candidate_path = root / "calibration-truth-v10-retained-proposition-singleton.private.json"
        if score["passed"]:
            final = finalize_v128_primary(v128=frozen["v128"], truth=frozen["truth"], outputs=outputs)
            _write_immutable(final_path, final)
            candidate = build_reference_candidate_v128(
                current_reference=frozen["v128"]["v127"]["v126"]["values"]["reference"],
                truth=frozen["v128"]["values"]["truth"],
                primary=final,
            )
            candidate["schema_version"] = V129_REFERENCE_VERSION
            candidate["reference_version"] = "fixture_reference_v10_retained_proposition_singleton_frozen"
            candidate["retained_proposition_singleton_owner_count"] = 1
            _write_immutable(candidate_path, candidate)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V129_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v129_singleton_proposition_repair_passed_patch_authorized"
                if passed else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v129_retained_proposition_reference_patch_authorized"
                if passed else "v129_singleton_proposition_repair_quality_gate_not_passed"
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
            "final_owner_output": _record(final_path) if passed else None,
            "reference_candidate": _record(candidate_path) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v128_usage": frozen["v128"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v129 singleton proposition repair")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v129(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({"state": terminal["state"], "terminal_reason": terminal["terminal_reason"], "retained_proposition_reference_patch_authorized": terminal.get("retained_proposition_reference_patch_authorized", False), "usage_status": terminal.get("usage_status")}, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
