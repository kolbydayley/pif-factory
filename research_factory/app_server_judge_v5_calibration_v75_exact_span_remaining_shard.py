from __future__ import annotations

"""Adopt four measured v74 shards and run only its never-started fifth shard."""

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
from .app_server_judge_v5_calibration_v74_stable_direct_field_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V74_ROOT,
    EFFORT,
    MODEL,
    TASKS_PER_SHARD,
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
from .util import now_iso, sha256_text


V75_SPEC_VERSION = "pif_app_server_judge_v5_4_v75_exact_span_remaining_shard_spec_v1"
V75_AUDIT_VERSION = "pif_app_server_judge_v5_4_v75_exact_span_projection_audit_v1"
V75_SCORE_VERSION = "pif_app_server_judge_v5_4_v75_direct_field_score_v1"
V75_FAILURE_VERSION = "pif_app_server_judge_v5_4_v75_failure_v1"
V75_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v75_terminal_v1"
V75_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V75_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V75_PHASE_ID = "judge_v5_4_v75_exact_span_remaining_shard"
TURN_NAME = "direct_field_shard_04_recovery"
SOURCE_TURN_NAMES = tuple(f"direct_field_shard_{index:02d}" for index in range(5))
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V74_ROOT.parent / "judge-calibration-v5_4-v75-exact-span-remaining-shard"
).resolve()


class JudgeV5CalibrationV75Error(RuntimeError):
    """The v75 adoption/recovery contract cannot be preserved."""


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


def _turn_root(root: Path, turn_name: str) -> Path:
    return root / "turns" / turn_name.replace("_", "-")


def project_exact_spans(
    output: Mapping[str, Any], value: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    projected = deepcopy(output)
    tasks = {row["task_id"]: row for row in value["tasks"]}
    operations = []
    for decision in projected.get("decisions") or []:
        task = tasks.get(decision.get("task_id"))
        if task is None or not isinstance(decision.get("source_evidence_spans"), list):
            continue
        retained = []
        for span in decision["source_evidence_spans"]:
            if isinstance(span, str) and span and span in task["source_excerpt"]:
                retained.append(span)
            else:
                rendered = span if isinstance(span, str) else json.dumps(span, sort_keys=True)
                operations.append(
                    {
                        "operation_type": "drop_nonexact_source_span",
                        "task_id": decision.get("task_id"),
                        "field": task["field"],
                        "span_sha256": sha256_text(rendered),
                        "span_size_bytes": len(rendered.encode("utf-8")),
                    }
                )
        decision["source_evidence_spans"] = retained
    return projected, operations


def _validate_v74(v74_root: Path) -> dict[str, Any]:
    root = v74_root.resolve()
    paths = {
        "v74_terminal": root / "terminal.json",
        "v74_failure": root / "failure.json",
        "v74_spec": root / "stable-direct-field-spec.json",
        "v74_input": root / "direct-field-input.private.json",
        "v74_truth": root / "selected-truth.private.json",
        "v74_taxonomy": root / "error-taxonomy.json",
    }
    for turn_name in SOURCE_TURN_NAMES:
        turn_root = _turn_root(root, turn_name)
        paths[f"{turn_name}_input"] = turn_root / "input.private.json"
        paths[f"{turn_name}_prompt"] = turn_root / "prompt.private.md"
        paths[f"{turn_name}_schema"] = turn_root / "schema.json"
        if turn_name != SOURCE_TURN_NAMES[-1]:
            paths[f"{turn_name}_capacity"] = turn_root / "capacity.json"
            paths[f"{turn_name}_sidecar"] = turn_root / "sidecar.json"
            paths[f"{turn_name}_output"] = turn_root / "output.private.json"
    values = {
        name: _load_json(path, name)
        for name, path in paths.items()
        if not name.endswith("_prompt")
    }
    terminal = values["v74_terminal"]
    failure = values["v74_failure"]
    spec = values["v74_spec"]
    attempts = failure.get("attempts") or []
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("usage_status") != "unknown"
        or terminal.get("accounting_complete") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("holdout_authorized") is not False
        or not _record_matches(terminal.get("failure"), paths["v74_failure"])
        or failure.get("failed_turn_name") != "direct_field_shard_03"
        or failure.get("error_class") != "JudgeV5CalibrationV26DiagnosticError"
        or failure.get("unknown_usage_attempt_count") != 1
        or len(attempts) != 5
        or spec.get("v73_semantic_turn_count") != 0
        or spec.get("retry_count_per_turn") != 0
        or not all(_verify_record(record) for record in spec.get("runtime_files") or [])
    ):
        raise JudgeV5CalibrationV75Error("v74 predecessor is inadmissible")
    completed_sidecars = []
    projected_outputs = []
    projection_operations = []
    for index, turn_name in enumerate(SOURCE_TURN_NAMES[:4]):
        attempt = attempts[index]
        if (
            attempt.get("turn_name") != turn_name
            or attempt.get("state") != "completed"
            or attempt.get("status") != "completed"
            or attempt.get("usage_status") != "measured"
            or not _record_matches(attempt.get("capacity"), paths[f"{turn_name}_capacity"])
            or not _record_matches(attempt.get("sidecar"), paths[f"{turn_name}_sidecar"])
            or not _record_matches(attempt.get("output"), paths[f"{turn_name}_output"])
        ):
            raise JudgeV5CalibrationV75Error("v74 completed shard drifted")
        sidecar = values[f"{turn_name}_sidecar"]
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("usage_status") != "measured"
            or sidecar.get("usage_complete") is not True
            or sidecar.get("auth_type") != "chatgpt"
            or sidecar.get("plan_type") != "pro"
        ):
            raise JudgeV5CalibrationV75Error("v74 measured sidecar drifted")
        _validate_usage(sidecar)
        value = values[f"{turn_name}_input"]
        output = values[f"{turn_name}_output"]
        projected, operations = project_exact_spans(output, value)
        if validate_output(projected, value):
            raise JudgeV5CalibrationV75Error("v74 projected shard is invalid")
        completed_sidecars.append(sidecar)
        projected_outputs.append(projected)
        projection_operations.extend(operations)
    last_attempt = attempts[-1]
    last_root = _turn_root(root, SOURCE_TURN_NAMES[-1])
    if (
        last_attempt.get("turn_name") != SOURCE_TURN_NAMES[-1]
        or any(last_attempt.get(key) is not None for key in ("capacity", "sidecar", "output"))
        or (last_root / "capacity.json").exists()
        or (last_root / "sidecar.json").exists()
        or (last_root / "output.private.json").exists()
        or len(projection_operations) != 1
        or projection_operations[0].get("operation_type") != "drop_nonexact_source_span"
    ):
        raise JudgeV5CalibrationV75Error("v74 unstarted/projection contract drifted")
    corrected_usage = _aggregate_usage(completed_sidecars)
    return {
        "records": {name: _record(path) for name, path in paths.items()},
        "projected_outputs": projected_outputs,
        "projection_operations": projection_operations,
        "corrected_usage": corrected_usage,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V75_CAPACITY_AUDIT_VERSION,
        "phase_id": V75_PHASE_ID,
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
        prior = _load_json(audit_path, "v75 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV75Error("immutable v75 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V75_CAPACITY_POLICY_VERSION,
        "phase_id": V75_PHASE_ID,
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
        prior = _load_json(policy_path, "v75 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV75Error("immutable v75 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def _write_stable_created(path: Path, value: Mapping[str, Any], purpose: str) -> dict[str, Any]:
    candidate = deepcopy(value)
    if path.exists():
        prior = _load_json(path, purpose)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV75Error(f"immutable {purpose} drifted")
        return prior
    _write_immutable(path, candidate)
    return candidate


def freeze_v75(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v74_root: Path = DEFAULT_V74_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v74(v74_root.resolve())
    records = predecessor["records"]
    value = _load_json(v74_root / "direct-field-input.private.json", "v74 input")
    truth = _load_json(v74_root / "selected-truth.private.json", "v74 truth")
    taxonomy = _load_json(v74_root / "error-taxonomy.json", "v74 taxonomy")
    input_path = root / "direct-field-input.private.json"
    truth_path = root / "selected-truth.private.json"
    taxonomy_path = root / "error-taxonomy.json"
    adopted_path = root / "adopted-prefix-output.private.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(taxonomy_path, taxonomy)
    adopted_rows = [
        deepcopy(row)
        for output in predecessor["projected_outputs"]
        for row in output["decisions"]
    ]
    if len(adopted_rows) != 12 or len({row["task_id"] for row in adopted_rows}) != 12:
        raise JudgeV5CalibrationV75Error("v75 adopted prefix coverage drifted")
    _write_immutable(adopted_path, {"decisions": adopted_rows})
    projection_audit = {
        "schema_version": V75_AUDIT_VERSION,
        "created_at": now_iso(),
        "operation_count": len(predecessor["projection_operations"]),
        "operations": predecessor["projection_operations"],
        "projection_scope": "nonexact_source_span_removal_only",
        "semantic_status_changed": False,
        "retained_exact_span_count_for_projected_decision": 1,
        "v74_started_turn_count": 4,
        "v74_unstarted_turn_count": 1,
        "v74_terminal_unknown_usage_is_local_accounting_bug": True,
        "corrected_v74_usage": predecessor["corrected_usage"],
        "privacy": "opaque_task_ids_field_enums_counts_and_span_hashes_only",
    }
    projection_path = root / "v74-adoption-audit.json"
    projection_audit = _write_stable_created(
        projection_path, projection_audit, "v75 projection audit"
    )
    source_turn = _turn_root(v74_root.resolve(), SOURCE_TURN_NAMES[-1])
    shard = _load_json(source_turn / "input.private.json", "v74 unstarted input")
    schema = _load_json(source_turn / "schema.json", "v74 unstarted schema")
    prompt = (source_turn / "prompt.private.md").read_text(encoding="utf-8")
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=shard, prompt=prompt, schema=schema
    )
    capacity = _build_capacity_policy(root, records)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V75_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "adopt_four_measured_v74_shards_project_one_nonexact_span_run_only_unstarted_fifth",
        "declared_new_turn_count": 1,
        "turn_plan": [TURN_NAME],
        "v74_started_turn_count": 4,
        "v74_adopted_measured_turn_count": 4,
        "v74_unstarted_turn_count": 1,
        "v74_semantic_turn_replayed": False,
        "retry_count_per_turn": 0,
        "reference_patch_authorized": False,
        "fresh_12_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": records,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v74_stable_direct_field_audit.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v73_direct_field_audit.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "selected_truth": _record(truth_path),
            "error_taxonomy": _record(taxonomy_path),
            "adopted_prefix_output": _record(adopted_path),
            "v74_adoption_audit": _record(projection_path),
            "remaining_turn": {
                "input": _record(paths["input"]),
                "prompt": _record(paths["prompt"]),
                "schema": _record(paths["schema"]),
            },
        },
        "exact_request_reuse": {
            "v74_input": _record(source_turn / "input.private.json"),
            "v74_prompt": _record(source_turn / "prompt.private.md"),
            "v74_schema": _record(source_turn / "schema.json"),
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "exact-span-remaining-shard-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v75 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV75Error("immutable v75 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
        "truth": truth,
        "adopted_outputs": predecessor["projected_outputs"],
        "corrected_v74_usage": predecessor["corrected_usage"],
        "shard": shard,
        "prompt": prompt,
        "schema": schema,
        "paths": paths,
        "capacity_policy": capacity["policy"],
        "projection_path": projection_path,
    }


def _usage_sum(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _real_attempts(root: Path) -> list[dict[str, Any]]:
    return [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]


def _write_failure(root: Path, error_class: str) -> dict[str, Any]:
    attempts = _real_attempts(root)
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            usage = _validate_usage(_load_json(Path(record["path"]), "v75 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = unknown == 0
    failure = {
        "schema_version": V75_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
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
        "schema_version": V75_TERMINAL_VERSION,
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
        "v74_semantic_turn_replayed": False,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "new_usage": failure["new_usage"],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v75(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v74_root: Path = DEFAULT_V74_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v75 terminal")
    frozen = freeze_v75(output_dir=root, v74_root=v74_root, timeout_seconds=timeout_seconds)
    factory = client_factory or _client_factory
    try:
        async with factory(frozen["capacity_policy"]) as client:
            output, sidecar, adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=base_instructions(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=TASKS_PER_SHARD,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: validate_output(
                    project_exact_spans(candidate, frozen["shard"])[0], frozen["shard"]
                ),
            )
        projected, new_operations = project_exact_spans(output, frozen["shard"])
        if validate_output(projected, frozen["shard"]):
            raise JudgeV5CalibrationV75Error("projected v75 output is invalid")
        merged = merge_outputs([*frozen["adopted_outputs"], projected])
        output_path = root / "direct-field-output.private.json"
        _write_immutable(output_path, merged)
        audit = _load_json(frozen["projection_path"], "v75 projection audit")
        audit["new_turn_operation_count"] = len(new_operations)
        audit["new_turn_operations"] = new_operations
        final_audit_path = root / "final-projection-audit.json"
        _write_immutable(final_audit_path, audit)
        score = score_v73(merged, frozen["truth"])
        score["schema_version"] = V75_SCORE_VERSION
        score["projection_audit"] = _record(final_audit_path)
        score_path = root / "direct-field-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage([sidecar])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V75_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v75_direct_field_audit_passed_proposal_verification_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v75_direct_field_audit_passed_proposal_verification_authorized"
                if passed
                else "v75_direct_field_audit_quality_gate_not_passed"
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
            "v74_semantic_turn_replayed": False,
            "v74_corrected_usage": frozen["corrected_v74_usage"],
            "cumulative_v74_v75_usage": _usage_sum(
                frozen["corrected_v74_usage"]["usage"], accounting["usage"]
            ),
            "score": _record(score_path),
            "output": _record(output_path),
            "projection_audit": _record(final_audit_path),
            "attempts": _real_attempts(root),
            "completed_checkpoint_adoptions": {TURN_NAME: adopted},
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.error_class)
    except Exception as exc:
        return _write_failure(root, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v75 exact-span remaining shard")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v74-root", default=str(DEFAULT_V74_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v75(
            output_dir=Path(args.output_dir),
            v74_root=Path(args.v74_root),
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
