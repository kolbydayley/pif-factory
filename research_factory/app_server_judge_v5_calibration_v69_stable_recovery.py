from __future__ import annotations

"""Stable no-replay recovery after the presemantic v68 refreeze failure."""

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
    project_root_contract,
    root_base_instructions,
    score_v67,
    validate_root_output,
)
from .app_server_judge_v5_calibration_v68_exact_span_recovery import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V68_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso


V68_PRESEMANTIC_FAILURE_VERSION = (
    "pif_app_server_judge_v5_4_v68_presemantic_failure_v1"
)
V68_PRESEMANTIC_TERMINAL_VERSION = (
    "pif_app_server_judge_v5_4_v68_presemantic_terminal_v1"
)
V69_SPEC_VERSION = "pif_app_server_judge_v5_4_v69_stable_recovery_spec_v1"
V69_AUDIT_VERSION = "pif_app_server_judge_v5_4_v69_root_projection_audit_v1"
V69_SCORE_VERSION = "pif_app_server_judge_v5_4_v69_score_v1"
V69_FAILURE_VERSION = "pif_app_server_judge_v5_4_v69_failure_v1"
V69_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v69_terminal_v1"
V69_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V69_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V69_PHASE_ID = "judge_v5_4_v69_stable_exact_span_recovery"
TURN_NAME = ROOT_TURN
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V68_ROOT.parent / "judge-calibration-v5_4-v69-stable-recovery"
).resolve()


class JudgeV5CalibrationV69Error(RuntimeError):
    """The v69 stable recovery cannot preserve its frozen contract."""


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


def _v68_paths(root: Path) -> dict[str, Path]:
    return {
        "v68_spec": root / "exact-span-recovery-spec.json",
        "v68_capacity_policy": root / "capacity-policy.json",
        "v68_capacity_audit": root / "capacity-policy-audit.json",
        "v68_input": root / "layered-input.private.json",
        "v68_truth": root / "diagnostic-truth.private.json",
        "v68_roles": root / "cohort-roles.json",
        "v68_projected_pointwise": root / "projected-pointwise-output.private.json",
        "v68_pointwise_audit": root / "pointwise-exact-span-audit.json",
        "v68_pointwise_receipts": root / "pointwise-field-receipts.private.json",
        "v68_root_input": root / "turns/neutral-root-verification/input.private.json",
        "v68_root_prompt": root / "turns/neutral-root-verification/prompt.private.md",
        "v68_root_schema": root / "turns/neutral-root-verification/schema.json",
    }


def terminalize_v68_presemantic_failure(
    v68_root: Path = DEFAULT_V68_ROOT,
) -> dict[str, Any]:
    root = v68_root.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        terminal = _load_json(terminal_path, "v68 terminal")
        if (
            terminal.get("state") != "failed"
            or terminal.get("terminal_reason")
            != "infrastructure_or_judge_attempt_failed"
            or terminal.get("semantic_attempt_started") is not False
            or terminal.get("usage_status") != "complete"
            or terminal.get("usage") != {field: 0 for field in USAGE_FIELDS}
        ):
            raise JudgeV5CalibrationV69Error("v68 terminal drifted")
        return terminal
    paths = _v68_paths(root)
    values = {
        name: _load_json(path, name)
        for name, path in paths.items()
        if name != "v68_root_prompt"
    }
    spec = values["v68_spec"]
    audit = values["v68_pointwise_audit"]
    turn_root = root / "turns/neutral-root-verification"
    if (
        spec.get("state") != "frozen_before_model_calls"
        or spec.get("declared_new_turn_count") != 1
        or spec.get("v67_pointwise_replayed") is not False
        or spec.get("retry_count_per_turn") != 0
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
        or not all(_verify_record(record) for record in spec.get("runtime_files") or [])
        or audit.get("operation_count") != 1
        or audit.get("semantic_content_changed") is not False
        or audit.get("projection_scope") != "nonexact_source_span_removal_only"
        or not _record_matches(spec.get("capacity_policy"), paths["v68_capacity_policy"])
        or not _record_matches(spec.get("capacity_audit"), paths["v68_capacity_audit"])
        or not _record_matches(spec["frozen_inputs"].get("input"), paths["v68_input"])
        or not _record_matches(spec["frozen_inputs"].get("truth"), paths["v68_truth"])
        or not _record_matches(spec["frozen_inputs"].get("roles"), paths["v68_roles"])
        or not _record_matches(
            spec["frozen_inputs"].get("projected_pointwise"),
            paths["v68_projected_pointwise"],
        )
        or not _record_matches(
            spec["frozen_inputs"].get("pointwise_audit"), paths["v68_pointwise_audit"]
        )
        or not _record_matches(
            spec["frozen_inputs"].get("pointwise_receipts"),
            paths["v68_pointwise_receipts"],
        )
        or not _record_matches(
            spec["frozen_inputs"]["root_turn"].get("input"), paths["v68_root_input"]
        )
        or not _record_matches(
            spec["frozen_inputs"]["root_turn"].get("prompt"), paths["v68_root_prompt"]
        )
        or not _record_matches(
            spec["frozen_inputs"]["root_turn"].get("schema"), paths["v68_root_schema"]
        )
        or (turn_root / "capacity.json").exists()
        or (turn_root / "sidecar.json").exists()
        or (turn_root / "output.private.json").exists()
    ):
        raise JudgeV5CalibrationV69Error("v68 presemantic state is inadmissible")
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    failure = {
        "schema_version": V68_PRESEMANTIC_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failure_stage": "presemantic_refreeze",
        "root_cause": "nondeterministic_created_at_in_pointwise_projection_audit",
        "semantic_attempt_started": False,
        "thread_started": False,
        "turn_started": False,
        "capacity_checkpoint_count": 0,
        "sidecar_count": 0,
        "retry_allowed_in_this_version": False,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": zero_usage,
    }
    failure_path = root / "presemantic-failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V68_PRESEMANTIC_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "fresh_all_36_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": zero_usage,
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def _validate_v68(v68_root: Path) -> dict[str, Any]:
    terminal = terminalize_v68_presemantic_failure(v68_root)
    paths = _v68_paths(v68_root.resolve())
    paths.update(
        {
            "v68_failure": v68_root / "presemantic-failure.json",
            "v68_terminal": v68_root / "terminal.json",
        }
    )
    failure = _load_json(paths["v68_failure"], "v68 failure")
    if (
        terminal.get("semantic_attempt_started") is not False
        or terminal.get("usage") != {field: 0 for field in USAGE_FIELDS}
        or not _record_matches(terminal.get("failure"), paths["v68_failure"])
        or failure.get("root_cause")
        != "nondeterministic_created_at_in_pointwise_projection_audit"
    ):
        raise JudgeV5CalibrationV69Error("v68 failure binding drifted")
    return {name: _record(path) for name, path in paths.items()}


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V69_CAPACITY_AUDIT_VERSION,
        "phase_id": V69_PHASE_ID,
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
        prior = _load_json(audit_path, "v69 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV69Error("immutable v69 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V69_CAPACITY_POLICY_VERSION,
        "phase_id": V69_PHASE_ID,
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
        prior = _load_json(policy_path, "v69 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV69Error("immutable v69 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v69(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v68_root: Path = DEFAULT_V68_ROOT,
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v68(v68_root.resolve())
    source_paths = _v68_paths(v68_root.resolve())
    value = _load_json(source_paths["v68_input"], "v68 input")
    truth = _load_json(source_paths["v68_truth"], "v68 truth")
    roles = _load_json(source_paths["v68_roles"], "v68 roles")
    root_input = _load_json(source_paths["v68_root_input"], "v68 root input")
    schema = _load_json(source_paths["v68_root_schema"], "v68 root schema")
    prompt = source_paths["v68_root_prompt"].read_text(encoding="utf-8")
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=root_input, prompt=prompt, schema=schema
    )
    input_path = root / "layered-input.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    roles_path = root / "cohort-roles.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(roles_path, roles)
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V69_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "reuse_exact_presemantic_v68_root_request_in_new_zero_retry_version",
        "declared_new_turn_count": 1,
        "turn_plan": [{"turn_name": TURN_NAME, "model": ROOT_MODEL, "effort": ROOT_EFFORT}],
        "v67_pointwise_replayed": False,
        "v68_semantic_turn_count": 0,
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
            _record(runtime_dir / "app_server_judge_v5_calibration_v68_exact_span_recovery.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "truth": _record(truth_path),
            "roles": _record(roles_path),
            "root_turn": {
                "input": _record(paths["input"]),
                "prompt": _record(paths["prompt"]),
                "schema": _record(paths["schema"]),
            },
        },
        "exact_request_reuse": {
            "v68_input": _record(source_paths["v68_root_input"]),
            "v68_prompt": _record(source_paths["v68_root_prompt"]),
            "v68_schema": _record(source_paths["v68_root_schema"]),
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "stable-recovery-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v69 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV69Error("immutable v69 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "truth": truth,
        "roles": roles,
        "root_input": root_input,
        "prompt": prompt,
        "schema": schema,
        "paths": paths,
        "capacity_policy": capacity["policy"],
        "v67_usage": _load_json(DEFAULT_V67_ROOT / "terminal.json", "v67 terminal")["usage"],
    }


def _usage_sum(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = _attempt_records(root)
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    partial = False
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            if attempt.get("capacity") is not None or attempt.get("output") is not None:
                partial = True
            continue
        try:
            usage = _validate_usage(_load_json(Path(record["path"]), "v69 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = not partial and unknown == 0
    failure = {
        "schema_version": V69_FAILURE_VERSION,
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
        "schema_version": V69_TERMINAL_VERSION,
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


async def run_v69(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v68_root: Path = DEFAULT_V68_ROOT,
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v69 terminal")
    frozen = freeze_v69(
        output_dir=root, v68_root=v68_root, timeout_seconds=timeout_seconds
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
            raise JudgeV5CalibrationV69Error("projected v69 root output is invalid")
        output_path = root / "layered-output.private.json"
        _write_immutable(output_path, projected)
        audit = {
            "schema_version": V69_AUDIT_VERSION,
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
        score["schema_version"] = V69_SCORE_VERSION
        score["root_projection_audit"] = _record(audit_path)
        score_path = root / "layered-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage([sidecar])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V69_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v69_layered_residual_passed_fresh_all_36_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v69_layered_residual_passed_fresh_all_36_authorized"
                if passed
                else "v69_layered_residual_quality_gate_not_passed"
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
            "v68_semantic_turn_count": 0,
            "score": _record(score_path),
            "output": _record(output_path),
            "root_projection_audit": _record(audit_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": {TURN_NAME: adopted},
            "predecessor_v67_usage": frozen["v67_usage"],
            "cumulative_v67_v69_usage": _usage_sum(frozen["v67_usage"], accounting["usage"]),
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v69 stable no-replay recovery")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v68-root", default=str(DEFAULT_V68_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v69(
            output_dir=Path(args.output_dir),
            v68_root=Path(args.v68_root),
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
