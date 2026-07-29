from __future__ import annotations

"""Stable no-replay recovery after the presemantic v73 refreeze failure."""

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
from .app_server_judge_v5_calibration_v73_direct_field_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V73_ROOT,
    EFFORT,
    MODEL,
    TASKS_PER_SHARD,
    TURN_NAMES,
    base_instructions,
    merge_outputs,
    score_v73,
    validate_output,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso


V73_PRESEMANTIC_FAILURE_VERSION = "pif_app_server_judge_v5_4_v73_presemantic_failure_v1"
V73_PRESEMANTIC_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v73_presemantic_terminal_v1"
V74_SPEC_VERSION = "pif_app_server_judge_v5_4_v74_stable_direct_field_spec_v1"
V74_SCORE_VERSION = "pif_app_server_judge_v5_4_v74_direct_field_score_v1"
V74_FAILURE_VERSION = "pif_app_server_judge_v5_4_v74_failure_v1"
V74_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v74_terminal_v1"
V74_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V74_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V74_PHASE_ID = "judge_v5_4_v74_stable_direct_field_audit"
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V73_ROOT.parent / "judge-calibration-v5_4-v74-stable-direct-field-audit"
).resolve()


class JudgeV5CalibrationV74Error(RuntimeError):
    """The v74 stable recovery cannot preserve its frozen request lineage."""


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


def _v73_paths(root: Path) -> dict[str, Path]:
    paths = {
        "v73_spec": root / "direct-field-spec.json",
        "v73_capacity_policy": root / "capacity-policy.json",
        "v73_capacity_audit": root / "capacity-policy-audit.json",
        "v73_input": root / "direct-field-input.private.json",
        "v73_truth": root / "selected-truth.private.json",
        "v73_taxonomy": root / "error-taxonomy.json",
    }
    for turn_name in TURN_NAMES:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        paths[f"{turn_name}_input"] = turn_root / "input.private.json"
        paths[f"{turn_name}_prompt"] = turn_root / "prompt.private.md"
        paths[f"{turn_name}_schema"] = turn_root / "schema.json"
    return paths


def terminalize_v73_presemantic_failure(
    v73_root: Path = DEFAULT_V73_ROOT,
) -> dict[str, Any]:
    root = v73_root.expanduser().resolve()
    terminal_path = root / "terminal.json"
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    if terminal_path.exists():
        terminal = _load_json(terminal_path, "v73 terminal")
        if (
            terminal.get("state") != "failed"
            or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
            or terminal.get("semantic_attempt_started") is not False
            or terminal.get("usage_status") != "complete"
            or terminal.get("usage") != zero_usage
            or terminal.get("production_mutated") is not False
        ):
            raise JudgeV5CalibrationV74Error("v73 terminal drifted")
        return terminal
    paths = _v73_paths(root)
    values = {
        name: _load_json(path, name)
        for name, path in paths.items()
        if not name.endswith("_prompt")
    }
    spec = values["v73_spec"]
    if (
        spec.get("state") != "frozen_before_model_calls"
        or spec.get("model") != MODEL
        or spec.get("reasoning_effort") != EFFORT
        or spec.get("turn_plan") != list(TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
        or not all(_verify_record(record) for record in spec.get("runtime_files") or [])
        or not _record_matches(spec.get("capacity_policy"), paths["v73_capacity_policy"])
        or not _record_matches(spec.get("capacity_audit"), paths["v73_capacity_audit"])
        or not _record_matches(spec["frozen_inputs"].get("input"), paths["v73_input"])
        or not _record_matches(
            spec["frozen_inputs"].get("selected_truth"), paths["v73_truth"]
        )
        or not _record_matches(
            spec["frozen_inputs"].get("error_taxonomy"), paths["v73_taxonomy"]
        )
    ):
        raise JudgeV5CalibrationV74Error("v73 presemantic state is inadmissible")
    turn_records = {row["turn_name"]: row for row in spec["frozen_inputs"]["turns"]}
    for turn_name in TURN_NAMES:
        record = turn_records.get(turn_name) or {}
        turn_root = root / "turns" / turn_name.replace("_", "-")
        if (
            not _record_matches(record.get("input"), paths[f"{turn_name}_input"])
            or not _record_matches(record.get("prompt"), paths[f"{turn_name}_prompt"])
            or not _record_matches(record.get("schema"), paths[f"{turn_name}_schema"])
            or (turn_root / "capacity.json").exists()
            or (turn_root / "sidecar.json").exists()
            or (turn_root / "output.private.json").exists()
        ):
            raise JudgeV5CalibrationV74Error("v73 semantic artifact or request drifted")
    failure = {
        "schema_version": V73_PRESEMANTIC_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failure_stage": "presemantic_refreeze",
        "root_cause": "nondeterministic_created_at_in_error_taxonomy",
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
        "schema_version": V73_PRESEMANTIC_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "proposal_verification_authorized": False,
        "reference_patch_authorized": False,
        "fresh_12_authorized": False,
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


def _validate_v73(v73_root: Path) -> dict[str, Any]:
    terminal = terminalize_v73_presemantic_failure(v73_root)
    paths = _v73_paths(v73_root.resolve())
    paths.update(
        {
            "v73_failure": v73_root / "presemantic-failure.json",
            "v73_terminal": v73_root / "terminal.json",
        }
    )
    failure = _load_json(paths["v73_failure"], "v73 failure")
    if (
        terminal.get("semantic_attempt_started") is not False
        or terminal.get("usage") != {field: 0 for field in USAGE_FIELDS}
        or not _record_matches(terminal.get("failure"), paths["v73_failure"])
        or failure.get("root_cause") != "nondeterministic_created_at_in_error_taxonomy"
    ):
        raise JudgeV5CalibrationV74Error("v73 failure binding drifted")
    return {name: _record(path) for name, path in paths.items()}


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    phase_bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V74_CAPACITY_AUDIT_VERSION,
        "phase_id": V74_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": phase_bound,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v74 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV74Error("immutable v74 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V74_CAPACITY_POLICY_VERSION,
        "phase_id": V74_PHASE_ID,
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
        "phase_total_token_bound": phase_bound,
        "projected_phase_quota_points": math.ceil(
            phase_bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    if policy_path.exists():
        prior = _load_json(policy_path, "v74 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV74Error("immutable v74 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v74(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v73_root: Path = DEFAULT_V73_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v73(v73_root.resolve())
    source_paths = _v73_paths(v73_root.resolve())
    value = _load_json(source_paths["v73_input"], "v73 input")
    truth = _load_json(source_paths["v73_truth"], "v73 truth")
    taxonomy = _load_json(source_paths["v73_taxonomy"], "v73 taxonomy")
    input_path = root / "direct-field-input.private.json"
    truth_path = root / "selected-truth.private.json"
    taxonomy_path = root / "error-taxonomy.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(taxonomy_path, taxonomy)
    turns = []
    turn_records = []
    exact_reuse = []
    for turn_name in TURN_NAMES:
        shard = _load_json(source_paths[f"{turn_name}_input"], f"v73 {turn_name} input")
        schema = _load_json(source_paths[f"{turn_name}_schema"], f"v73 {turn_name} schema")
        prompt = source_paths[f"{turn_name}_prompt"].read_text(encoding="utf-8")
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=shard,
            prompt=prompt,
            schema=schema,
        )
        turns.append(
            {"turn_name": turn_name, "value": shard, "prompt": prompt, "schema": schema, "paths": paths}
        )
        turn_records.append(
            {
                "turn_name": turn_name,
                "input": _record(paths["input"]),
                "prompt": _record(paths["prompt"]),
                "schema": _record(paths["schema"]),
            }
        )
        exact_reuse.append(
            {
                "turn_name": turn_name,
                "v73_input": _record(source_paths[f"{turn_name}_input"]),
                "v73_prompt": _record(source_paths[f"{turn_name}_prompt"]),
                "v73_schema": _record(source_paths[f"{turn_name}_schema"]),
            }
        )
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V74_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds_per_turn": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "byte_identical_v73_requests_in_new_stable_zero_retry_version",
        "task_count": 15,
        "tasks_per_shard": TASKS_PER_SHARD,
        "turn_plan": list(TURN_NAMES),
        "v73_semantic_turn_count": 0,
        "v73_turn_replayed": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "perfect_controls_no_abstention_exact_evidence_authorizes_neutral_proposal_verification_only",
        "reference_patch_authorized": False,
        "fresh_12_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v73_direct_field_audit.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "selected_truth": _record(truth_path),
            "error_taxonomy": _record(taxonomy_path),
            "turns": turn_records,
        },
        "exact_request_reuse": exact_reuse,
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "stable-direct-field-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v74 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV74Error("immutable v74 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
        "truth": truth,
        "turns": turns,
        "capacity_policy": capacity["policy"],
    }


def _failure_accounting(root: Path) -> dict[str, Any]:
    attempts = _attempt_records(root)
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            usage = _validate_usage(_load_json(Path(record["path"]), "v74 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = bool(attempts) and unknown == 0
    return {
        "attempts": attempts,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": known if complete else None,
        "unknown_usage_attempt_count": unknown,
    }


def _write_failure(root: Path, *, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    accounting = _failure_accounting(root)
    failure = {
        "schema_version": V74_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        **accounting,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V74_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "diagnostic_passed": False,
        "proposal_verification_authorized": False,
        "reference_patch_authorized": False,
        "fresh_12_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "v73_semantic_turn_count": 0,
        "accounting_complete": accounting["accounting_complete"],
        "usage_status": accounting["usage_status"],
        "usage": accounting["usage"],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v74(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v73_root: Path = DEFAULT_V73_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v74 terminal")
    frozen = freeze_v74(output_dir=root, v73_root=v73_root, timeout_seconds=timeout_seconds)
    factory = client_factory or _client_factory
    outputs = []
    sidecars = []
    adoptions = {}
    try:
        async with factory(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=turn["turn_name"],
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=base_instructions(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=TASKS_PER_SHARD,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, shard=turn["value"]: validate_output(candidate, shard),
                )
                outputs.append(output)
                sidecars.append(sidecar)
                adoptions[turn["turn_name"]] = adopted
        merged = merge_outputs(outputs)
        output_path = root / "direct-field-output.private.json"
        _write_immutable(output_path, merged)
        score = score_v73(merged, frozen["truth"])
        score["schema_version"] = V74_SCORE_VERSION
        score_path = root / "direct-field-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V74_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v74_direct_field_audit_passed_proposal_verification_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v74_direct_field_audit_passed_proposal_verification_authorized"
                if passed
                else "v74_direct_field_audit_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "diagnostic_passed": passed,
            "proposal_verification_authorized": passed,
            "reference_patch_authorized": False,
            "fresh_12_authorized": False,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "v73_semantic_turn_count": 0,
            "score": _record(score_path),
            "output": _record(output_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": adoptions,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, turn_name=exc.turn_name, error_class=exc.error_class)
    except Exception as exc:
        turn_name = frozen["turns"][len(outputs)]["turn_name"] if len(outputs) < 5 else None
        return _write_failure(root, turn_name=turn_name, error_class=type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v74 stable direct field audit")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v73-root", default=str(DEFAULT_V73_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v74(
            output_dir=Path(args.output_dir),
            v73_root=Path(args.v73_root),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "diagnostic_passed": terminal.get("diagnostic_passed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
