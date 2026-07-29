from __future__ import annotations

"""No-replay v68 recovery for the measured v67 pointwise output."""

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
from .app_server_judge_v5_calibration_v67_layered_residual import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V67_ROOT,
    ROOT_EFFORT,
    ROOT_MODEL,
    ROOT_TURN,
    build_root_input,
    build_root_prompt,
    freeze_pointwise_receipts,
    project_root_contract,
    root_base_instructions,
    root_output_schema,
    score_v67,
    validate_pointwise_output,
    validate_root_output,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso, sha256_text


V68_SPEC_VERSION = "pif_app_server_judge_v5_4_v68_exact_span_recovery_spec_v1"
V68_POINTWISE_AUDIT_VERSION = (
    "pif_app_server_judge_v5_4_v68_pointwise_exact_span_projection_audit_v1"
)
V68_ROOT_AUDIT_VERSION = "pif_app_server_judge_v5_4_v68_root_projection_audit_v1"
V68_SCORE_VERSION = "pif_app_server_judge_v5_4_v68_score_v1"
V68_FAILURE_VERSION = "pif_app_server_judge_v5_4_v68_failure_v1"
V68_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v68_terminal_v1"
V68_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V68_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V68_PHASE_ID = "judge_v5_4_v68_exact_span_no_replay_recovery"
TURN_NAME = ROOT_TURN
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V67_ROOT.parent / "judge-calibration-v5_4-v68-exact-span-recovery"
).resolve()


class JudgeV5CalibrationV68Error(RuntimeError):
    """The no-replay v68 recovery cannot preserve its frozen contract."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _verify_record(record: Any) -> bool:
    return (
        isinstance(record, Mapping)
        and isinstance(record.get("path"), str)
        and _record_matches(record, Path(record["path"]))
    )


def _validate_v67(v67_root: Path) -> dict[str, Any]:
    paths = {
        "v67_terminal": v67_root / "terminal.json",
        "v67_failure": v67_root / "failure.json",
        "v67_spec": v67_root / "layered-diagnostic-spec.json",
        "v67_input": v67_root / "layered-input.private.json",
        "v67_truth": v67_root / "diagnostic-truth.private.json",
        "v67_roles": v67_root / "cohort-roles.json",
        "v67_capacity_policy": v67_root / "capacity-policy.json",
        "v67_capacity_audit": v67_root / "capacity-policy-audit.json",
        "v67_pointwise_input": v67_root / "turns/pointwise-field-facts/input.private.json",
        "v67_pointwise_prompt": v67_root / "turns/pointwise-field-facts/prompt.private.md",
        "v67_pointwise_schema": v67_root / "turns/pointwise-field-facts/schema.json",
        "v67_pointwise_capacity": v67_root / "turns/pointwise-field-facts/capacity.json",
        "v67_pointwise_sidecar": v67_root / "turns/pointwise-field-facts/sidecar.json",
        "v67_pointwise_output": v67_root / "turns/pointwise-field-facts/output.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items() if not name.endswith("prompt")}
    terminal = values["v67_terminal"]
    failure = values["v67_failure"]
    spec = values["v67_spec"]
    sidecar = values["v67_pointwise_sidecar"]
    output = values["v67_pointwise_output"]
    value = values["v67_input"]
    errors = validate_pointwise_output(output, value)
    expected_errors = ["unit_4_row_2_evidence"]
    attempts = terminal.get("attempts") or failure.get("attempts") or []
    root_dir = v67_root / "turns/neutral-root-verification"
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or not _record_matches(terminal.get("failure"), paths["v67_failure"])
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != "pointwise_field_facts"
        or failure.get("error_class") != "JudgeV5CalibrationV26DiagnosticError"
        or failure.get("retry_allowed_in_this_version") is not False
        or spec.get("state") != "frozen_before_model_calls"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
        or not all(_verify_record(record) for record in spec.get("runtime_files") or [])
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("model") != "gpt-5.6-luna"
        or sidecar.get("effort") != "low"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or _validate_usage(sidecar) != terminal.get("usage")
        or len(attempts) != 1
        or not _record_matches(attempts[0].get("capacity"), paths["v67_pointwise_capacity"])
        or not _record_matches(attempts[0].get("sidecar"), paths["v67_pointwise_sidecar"])
        or not _record_matches(attempts[0].get("output"), paths["v67_pointwise_output"])
        or errors != expected_errors
        or any(root_dir.glob("capacity.json"))
        or any(root_dir.glob("sidecar.json"))
        or any(root_dir.glob("output.private.json"))
    ):
        raise JudgeV5CalibrationV68Error("v67 measured predecessor is inadmissible")
    return {name: _record(path) for name, path in paths.items()}


def project_pointwise_exact_spans(
    output: Mapping[str, Any], value: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    projected = deepcopy(output)
    sources = {(row["case_id"], row["witness_id"]): row for row in value["units"]}
    operations = []
    for unit in projected.get("units") or []:
        key = (unit.get("case_id"), unit.get("witness_id"))
        source = sources.get(key)
        if source is None:
            continue
        for row in unit.get("field_facts") or []:
            spans = row.get("source_evidence_spans")
            if not isinstance(spans, list):
                continue
            retained = []
            for span in spans:
                if isinstance(span, str) and span and span in source["source_excerpt"]:
                    retained.append(span)
                else:
                    rendered = span if isinstance(span, str) else json.dumps(span, sort_keys=True)
                    operations.append(
                        {
                            "operation_type": "drop_nonexact_pointwise_evidence_span",
                            "case_id": key[0],
                            "witness_id": key[1],
                            "field": row.get("field"),
                            "span_sha256": sha256_text(rendered),
                            "span_size_bytes": len(rendered.encode("utf-8")),
                        }
                    )
            row["source_evidence_spans"] = retained
    if len(operations) != 1:
        raise JudgeV5CalibrationV68Error("v68 exact-span projection count drifted")
    errors = validate_pointwise_output(projected, value)
    if errors:
        raise JudgeV5CalibrationV68Error("v68 projected pointwise output is invalid")
    return projected, operations


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V68_CAPACITY_AUDIT_VERSION,
        "phase_id": V68_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v68 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV68Error("immutable v68 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V68_CAPACITY_POLICY_VERSION,
        "phase_id": V68_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [TURN_NAME],
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        "projected_phase_quota_points": math.ceil(
            MAX_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    if policy_path.exists():
        prior = _load_json(policy_path, "v68 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV68Error("immutable v68 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v68(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v67_root: Path = DEFAULT_V67_ROOT,
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v67(v67_root.resolve())
    value = _load_json(v67_root / "layered-input.private.json", "v67 input")
    truth = _load_json(v67_root / "diagnostic-truth.private.json", "v67 truth")
    roles = _load_json(v67_root / "cohort-roles.json", "v67 roles")
    raw_output = _load_json(
        v67_root / "turns/pointwise-field-facts/output.private.json", "v67 pointwise output"
    )
    projected_pointwise, pointwise_operations = project_pointwise_exact_spans(raw_output, value)
    pointwise_path = root / "projected-pointwise-output.private.json"
    _write_immutable(pointwise_path, projected_pointwise)
    pointwise_audit = {
        "schema_version": V68_POINTWISE_AUDIT_VERSION,
        "created_at": now_iso(),
        "operation_count": len(pointwise_operations),
        "operations": pointwise_operations,
        "semantic_content_changed": False,
        "projection_scope": "nonexact_source_span_removal_only",
    }
    pointwise_audit_path = root / "pointwise-exact-span-audit.json"
    _write_immutable(pointwise_audit_path, pointwise_audit)
    receipts = freeze_pointwise_receipts(projected_pointwise, value)
    receipt_path = root / "pointwise-field-receipts.private.json"
    _write_immutable(receipt_path, receipts)
    root_input = build_root_input(value, receipts)
    input_path = root / "layered-input.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    roles_path = root / "cohort-roles.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(roles_path, roles)
    prompt = build_root_prompt(root_input)
    schema = root_output_schema(root_input)
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=root_input, prompt=prompt, schema=schema
    )
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V68_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "adopt_v67_measured_pointwise_after_exact_span_projection_then_sol_root",
        "declared_new_turn_count": 1,
        "turn_plan": [{"turn_name": TURN_NAME, "model": ROOT_MODEL, "effort": ROOT_EFFORT}],
        "v67_pointwise_replayed": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "perfect_targeted_diagnostic_authorizes_fresh_all_36_only",
        "fresh_all_36_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v67_layered_residual.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "truth": _record(truth_path),
            "roles": _record(roles_path),
            "projected_pointwise": _record(pointwise_path),
            "pointwise_audit": _record(pointwise_audit_path),
            "pointwise_receipts": _record(receipt_path),
            "root_turn": {
                "input": _record(paths["input"]),
                "prompt": _record(paths["prompt"]),
                "schema": _record(paths["schema"]),
            },
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "exact-span-recovery-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v68 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV68Error("immutable v68 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
        "truth": truth,
        "roles": roles,
        "root_input": root_input,
        "prompt": prompt,
        "schema": schema,
        "paths": paths,
        "capacity_policy": capacity["policy"],
        "v67_usage": _load_json(v67_root / "terminal.json", "v67 terminal")["usage"],
    }


def _usage_sum(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = _attempt_records(root)
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    incomplete = False
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            if attempt.get("capacity") is not None or attempt.get("output") is not None:
                incomplete = True
            continue
        try:
            usage = _validate_usage(_load_json(Path(record["path"]), "v68 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = not incomplete and unknown == 0
    failure = {
        "schema_version": V68_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "new_usage": known if complete else None,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V68_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "fresh_all_36_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "new_usage": failure["new_usage"],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v68(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v67_root: Path = DEFAULT_V67_ROOT,
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v68 terminal")
    frozen = freeze_v68(
        output_dir=root, v67_root=v67_root, timeout_seconds=timeout_seconds
    )
    factory = client_factory or _client_factory
    current_turn = None
    try:
        async with factory(frozen["capacity_policy"]) as client:
            current_turn = TURN_NAME
            output, sidecar, adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=root_base_instructions(),
                model=ROOT_MODEL,
                effort=ROOT_EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=12,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda value: validate_root_output(
                    project_root_contract(value, frozen["root_input"])[0],
                    frozen["root_input"],
                ),
            )
        projected, operations = project_root_contract(output, frozen["root_input"])
        errors = validate_root_output(projected, frozen["root_input"])
        if errors:
            raise JudgeV5CalibrationV68Error("projected v68 root output is invalid")
        output_path = root / "layered-output.private.json"
        _write_immutable(output_path, projected)
        audit = {
            "schema_version": V68_ROOT_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "semantic_field_source": ROOT_MODEL,
            "new_semantic_decisions_from_deterministic_code": False,
            "privacy": "opaque_ids_enums_counts_and_span_hashes_only",
        }
        audit_path = root / "root-projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v67(projected, frozen["truth"], frozen["roles"])
        score["schema_version"] = V68_SCORE_VERSION
        score["root_projection_audit"] = _record(audit_path)
        score_path = root / "layered-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage([sidecar])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V68_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v68_layered_residual_passed_fresh_all_36_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v68_layered_residual_passed_fresh_all_36_authorized"
                if passed
                else "v68_layered_residual_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "layered_residual_diagnostic_passed": passed,
            "fresh_all_36_authorized": passed,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "v67_pointwise_replayed": False,
            "score": _record(score_path),
            "output": _record(output_path),
            "root_projection_audit": _record(audit_path),
            "pointwise_exact_span_audit": _record(root / "pointwise-exact-span-audit.json"),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": {TURN_NAME: adopted},
            "predecessor_v67_usage": frozen["v67_usage"],
            "cumulative_v67_v68_usage": _usage_sum(frozen["v67_usage"], accounting["usage"]),
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v68 exact-span no-replay recovery")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v67-root", default=str(DEFAULT_V67_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v68(
            output_dir=Path(args.output_dir),
            v67_root=Path(args.v67_root),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "layered_residual_diagnostic_passed": terminal.get(
                    "layered_residual_diagnostic_passed", False
                ),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
