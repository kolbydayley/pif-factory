from __future__ import annotations

"""Fresh corrected field diagnostic against the frozen v162 reference."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v162_layer_corrected_field_adjudication as v162
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
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V163_SPEC_VERSION = "pif_app_server_judge_v5_4_v163_spec_v1"
V163_SCORE_VERSION = "pif_app_server_judge_v5_4_v163_score_v1"
V163_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v163_field_protocol_v1"
V163_FAILURE_VERSION = "pif_app_server_judge_v5_4_v163_failure_v1"
V163_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v163_terminal_v1"
V163_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V163_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V163_PHASE_ID = "judge_v5_4_v163_fresh_corrected_field_diagnostic"

FIELD_MODEL = "gpt-5.5"
SUPPORT_MODEL = "gpt-5.6-sol"
EFFORT = "high"
TURN_NAMES = tuple(f"corrected_field_singleton_{index:02d}" for index in range(11))
MAXIMUM_TOTAL_TOKENS_PER_TURN = 45000
TIMEOUT_SECONDS = v162.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v162.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v163-fresh-corrected-field-diagnostic"
).resolve()


class JudgeV5CalibrationV163Error(RuntimeError):
    """The immutable v163 corrected diagnostic contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v162() -> dict[str, Any]:
    root = v162.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "score": root / "layer-corrected-field-adjudication-score.json",
        "spec": root / "layer-corrected-field-adjudication-spec.json",
        "truth": root / "full-calibration-truth-v162.private.json",
        "reference": root / "fixture-reference-v14-v162.private.json",
        "rescore": root / "retrospective-field-rescore.json",
    }
    values = {name: _load_json(path, f"v162 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    sidecars = []
    for row in attempts:
        if not isinstance(row.get("sidecar"), Mapping):
            raise JudgeV5CalibrationV163Error("v162 measured sidecar coverage drifted")
        _verify_record(row["sidecar"])
        _validate_usage(_load_json(Path(row["sidecar"]["path"]), "v162 sidecar"))
        sidecars.append(row["sidecar"])
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v162_layer_corrected_field_reference_frozen_fresh_diagnostic_authorized"
        or terminal.get("reference_frozen") is not True
        or terminal.get("fresh_field_diagnostic_authorized") is not True
        or terminal.get("reference_change_count") != 5
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 129894
        or terminal.get("cumulative_calibration_usage", {}).get("total_tokens")
        != 2676438
        or score.get("passed") is not True
        or score.get("failed_checks") != []
        or values["reference"].get("reference_frozen") is not True
        or values["reference"].get("unsupported_inference_owned_by_support_projection")
        is not True
        or len(attempts) != 6
        or len(sidecars) != 6
        or spec.get("maximum_turn_count") != 6
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5CalibrationV163Error("v162 reference terminal drifted")
    for record in spec["runtime_files"]:
        _verify_record(record)
    predecessor = v162._validate_v161()
    data = v162.build_v162_inputs(predecessor)
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "sidecars": sidecars,
        "predecessor": predecessor,
        "data": data,
        "cumulative_usage": terminal["cumulative_calibration_usage"],
    }


def build_v163_inputs(source: Mapping[str, Any]) -> dict[str, Any]:
    all_data = source["predecessor"]["data"]
    truth = source["values"]["truth"]
    truth_rows = {str(row["task_id"]): row for row in truth["field_tasks"]}
    primary_values = {
        turn["task_id"]: turn["value"]
        for turn in all_data["turns"]
        if turn["turn_role"] == "reference_primary"
    }
    selected = list(all_data["primary_ids"])
    support_ids = [task_id for task_id in selected if truth_rows[task_id]["field"] == "unsupported_inference"]
    if len(support_ids) != 1:
        raise JudgeV5CalibrationV163Error("v163 support projection coverage drifted")
    support_id = support_ids[0]
    model_ids = [task_id for task_id in selected if task_id != support_id]
    model_ids.sort(key=lambda value: sha256_text(f"v163|order|{value}"))
    turns = [
        {
            "turn_name": turn_name,
            "turn_role": "field_singleton",
            "task_id": task_id,
            "value": deepcopy(primary_values[task_id]),
        }
        for turn_name, task_id in zip(TURN_NAMES, model_ids, strict=True)
    ]
    if len(turns) != 11 or len(set(model_ids)) != 11:
        raise JudgeV5CalibrationV163Error("v163 field coverage drifted")
    return {
        "turns": turns,
        "model_ids": model_ids,
        "support_id": support_id,
        "support_projection": source["data"]["support_projection"],
        "selected_ids": selected,
        "residual_ids": list(all_data["residual_ids"]),
        "control_ids": list(all_data["control_ids"]),
        "truth": truth,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    audit = {
        "schema_version": V163_CAPACITY_AUDIT_VERSION,
        "phase_id": V163_PHASE_ID,
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
        "schema_version": V163_CAPACITY_POLICY_VERSION,
        "phase_id": V163_PHASE_ID,
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


def freeze_v163(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v163 terminal")}
    source = _validate_v162()
    data = build_v163_inputs(source)
    turns = []
    for row in data["turns"]:
        prompt = v146.build_prompt_v146(row["value"])
        schema = field_output_schema(row["value"])
        paths = _freeze_turn_request(
            root=root,
            turn_name=row["turn_name"],
            input_value=row["value"],
            prompt=prompt,
            schema=schema,
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor = {
        **{f"v162_{name}": record for name, record in source["records"].items()},
        "v162_sidecars": source["sidecars"],
        "support_receipts": source["predecessor"]["support_record"],
        "cumulative_usage": source["cumulative_usage"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V163_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "field_model": FIELD_MODEL,
        "support_model": SUPPORT_MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_gpt55_eleven_singleton_fields_plus_frozen_sol_support_projection",
        "field_turn_count": 11,
        "support_projection_count": 1,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": len(TURN_NAMES),
        "maximum_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "truth_labels_exposed_to_model": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)), *source["values"]["spec"]["runtime_files"]
        ],
        "frozen_instructions": {
            "field_sha256": sha256_text(v146.base_instructions_v146())
        },
        "frozen_inputs": {
            "truth": source["records"]["truth"],
            "reference": source["records"]["reference"],
            "support_receipts": source["predecessor"]["support_record"],
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
    spec_path = root / "fresh-corrected-field-diagnostic-spec.json"
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


def score_v163(*, field_output: Mapping[str, Any], data: Mapping[str, Any]) -> dict[str, Any]:
    truth = {str(row["task_id"]): row for row in data["truth"]["field_tasks"]}
    observed = {str(row["task_id"]): row for row in field_output["decisions"]}
    observed[data["support_id"]] = {
        "task_id": data["support_id"],
        "field_status": data["support_projection"],
        "source_evidence_spans": ["frozen_support_receipt"],
        "rationale": "Owned by the frozen LLM support receipt.",
    }
    selected = set(data["selected_ids"])
    if set(observed) != selected:
        raise JudgeV5CalibrationV163Error("v163 field score coverage drifted")
    exact = {
        key: observed[key]["field_status"] == truth[key]["expected_status"]
        for key in selected
    }
    metrics = {
        "field_overall_exact_count": sum(exact.values()),
        "field_decision_count": len(exact),
        "field_residual_exact_count": sum(exact[key] for key in data["residual_ids"]),
        "field_residual_count": len(data["residual_ids"]),
        "field_control_exact_count": sum(exact[key] for key in data["control_ids"]),
        "field_control_count": len(data["control_ids"]),
        "field_abstention_count": sum(
            observed[key]["field_status"] == "abstain" for key in selected
        ),
        "support_projection_count": 1,
    }
    checks = {
        "field_overall_exact_count": metrics["field_overall_exact_count"] >= 11,
        "field_residual_exact_count": metrics["field_residual_exact_count"] >= 5,
        "field_control_exact_count": metrics["field_control_exact_count"] == 6,
        "field_abstention_count": metrics["field_abstention_count"] == 0,
        "support_projection_exact_rate": exact[data["support_id"]],
    }
    passed = all(checks.values())
    return {
        "schema_version": V163_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": metrics,
        "field_protocol_frozen": passed,
        "bounded_alignment_verifier_diagnostic_authorized": passed,
        "fresh_full_replacement_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
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
            measured = _validate_usage(
                _load_json(Path(record["path"]), "v163 measured sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    predecessor = _validate_v162()["cumulative_usage"]
    cumulative = _sum_usage(predecessor, usage)
    failure = {
        "schema_version": V163_FAILURE_VERSION,
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
        "schema_version": V163_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "field_protocol_frozen": False,
        "bounded_alignment_verifier_diagnostic_authorized": False,
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


async def run_v163(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v163 terminal")
    frozen = freeze_v163(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    sidecars = []
    outputs = []
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v146.base_instructions_v146(),
                    model=FIELD_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=1,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_field_output(
                        candidate, item
                    ),
                )
                outputs.append(output)
                sidecars.append(sidecar)
        field_output = v155._merge_outputs(outputs, "decisions")
        _write_immutable(root / "field-output.private.json", field_output)
        score = score_v163(field_output=field_output, data=frozen["data"])
        score_path = root / "fresh-corrected-field-diagnostic-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        protocol_path = root / "field-protocol-v163.json"
        if passed:
            _write_immutable(
                protocol_path,
                {
                    "schema_version": V163_PROTOCOL_VERSION,
                    "frozen_at": now_iso(),
                    "field_model": FIELD_MODEL,
                    "support_model": SUPPORT_MODEL,
                    "reasoning_effort": EFFORT,
                    "field_context_mode": "singleton",
                    "unsupported_inference_owner": "llm_pointwise_support_projection",
                    "field_instructions_sha256": sha256_text(
                        v146.base_instructions_v146()
                    ),
                    "reference": frozen["source"]["records"]["reference"],
                    "truth": frozen["source"]["records"]["truth"],
                    "quality_gates_unchanged": True,
                    "bounded_alignment_verifier_diagnostic_authorized": True,
                    "fresh_full_replacement_calibration_authorized": False,
                    "selection_authorized": False,
                    "holdout_authorized": False,
                    "production_mutation_allowed": False,
                },
            )
        accounting = _aggregate_usage(sidecars)
        predecessor = frozen["source"]["cumulative_usage"]
        cumulative = _sum_usage(predecessor, accounting["usage"])
        terminal = {
            "schema_version": V163_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": "v163_corrected_field_diagnostic_passed_alignment_verifier_authorized"
            if passed
            else "inactive_incomplete_recovery_required",
            "development_terminal_reason": "v163_corrected_field_diagnostic_passed"
            if passed
            else "v163_corrected_field_diagnostic_quality_gate_not_passed",
            "overall_evaluation_complete": False,
            "field_protocol_frozen": passed,
            "bounded_alignment_verifier_diagnostic_authorized": passed,
            "fresh_full_replacement_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "score": _record(score_path),
            "protocol": _record(protocol_path) if passed else None,
            "predecessor_cumulative_usage": predecessor,
            "cumulative_calibration_usage": cumulative,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v163 corrected field diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v163(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "field_protocol_frozen": terminal.get("field_protocol_frozen", False),
                "bounded_alignment_verifier_diagnostic_authorized": terminal.get(
                    "bounded_alignment_verifier_diagnostic_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
