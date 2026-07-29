from __future__ import annotations

"""Presemantic recovery of the exact frozen v57 root-projection requests."""

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
from .app_server_judge_v5_calibration_v56_structured import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V56_ROOT,
    sanitize_nonexact_spans,
)
from .app_server_judge_v5_calibration_v57_repair import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V57_ROOT,
    TURN_NAMES,
    _validate_predecessor as _validate_v56_predecessor,
    _validate_sanitizable_output,
    score_v57,
    v57_base_instructions,
)
from .app_server_judge_v5_calibration_v55_structured import validate_v55_output
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso


V58_SPEC_VERSION = "pif_app_server_judge_v5_4_v58_presemantic_recovery_spec_v1"
V58_SCORE_VERSION = "pif_app_server_judge_v5_4_v58_root_projection_score_v1"
V58_AUDIT_VERSION = "pif_app_server_judge_v5_4_v58_exact_span_sanitization_audit_v1"
V58_FAILURE_VERSION = "pif_app_server_judge_v5_4_v58_root_projection_failure_v1"
V58_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v58_root_projection_terminal_v1"
V58_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V58_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V58_PHASE_ID = "judge_v5_4_v58_root_projection_presemantic_recovery"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V57_ROOT.parent / "judge-calibration-v5_4-v58-root-projection-recovery"
).resolve()


class JudgeV5CalibrationV58RecoveryError(RuntimeError):
    """The immutable v57 presemantic failure cannot be recovered safely."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _verify_record(record: Any) -> bool:
    if not isinstance(record, Mapping) or not isinstance(record.get("path"), str):
        return False
    return _record_matches(record, Path(record["path"]))


def _validate_v57_predecessor(v57_root: Path, v56_root: Path) -> dict[str, Any]:
    paths = {
        "v57_terminal": v57_root / "terminal.json",
        "v57_failure": v57_root / "presemantic-failure.json",
        "v57_spec": v57_root / "root-projection-spec.json",
        "v57_taxonomy": v57_root / "error-taxonomy.json",
        "v57_input": v57_root / "root-projection-input-full.private.json",
        "v57_truth": v57_root / "diagnostic-truth.private.json",
        "v57_capacity_policy": v57_root / "capacity-policy.json",
        "v57_capacity_audit": v57_root / "capacity-policy-audit.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v57_terminal"]
    failure = values["v57_failure"]
    spec = values["v57_spec"]
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("semantic_attempt_started") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != zero_usage
        or not _record_matches(terminal.get("failure"), paths["v57_failure"])
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failure_stage") != "presemantic_refreeze"
        or failure.get("root_cause") != "nondeterministic_created_at_in_frozen_taxonomy"
        or failure.get("semantic_attempt_started") is not False
        or failure.get("thread_started") is not False
        or failure.get("turn_started") is not False
        or failure.get("capacity_checkpoint_count") != 0
        or failure.get("sidecar_count") != 0
        or failure.get("usage") != zero_usage
        or spec.get("state") != "frozen_before_model_calls"
        or spec.get("turn_plan") != list(TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("full_calibration_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV58RecoveryError("v57 presemantic terminal is inadmissible")
    if any(v57_root.glob("turns/*/capacity.json")) or any(
        v57_root.glob("turns/*/sidecar.json")
    ) or any(v57_root.glob("turns/*/output.private.json")):
        raise JudgeV5CalibrationV58RecoveryError("v57 unexpectedly contains semantic artifacts")
    v56_records = _validate_v56_predecessor(v56_root.resolve())
    if spec.get("predecessor") != v56_records:
        raise JudgeV5CalibrationV58RecoveryError("v57 v56 predecessor binding drifted")
    if (
        not _record_matches(spec.get("capacity_policy"), paths["v57_capacity_policy"])
        or not _record_matches(spec.get("capacity_audit"), paths["v57_capacity_audit"])
        or not _record_matches(spec["frozen_inputs"].get("taxonomy"), paths["v57_taxonomy"])
        or not _record_matches(spec["frozen_inputs"].get("full"), paths["v57_input"])
        or not _record_matches(spec["frozen_inputs"].get("truth"), paths["v57_truth"])
        or not all(_verify_record(record) for record in spec.get("runtime_files") or [])
    ):
        raise JudgeV5CalibrationV58RecoveryError("v57 frozen artifact binding drifted")
    shards = spec["frozen_inputs"].get("shards") or []
    if len(shards) != len(TURN_NAMES):
        raise JudgeV5CalibrationV58RecoveryError("v57 frozen shard coverage drifted")
    for expected_name, shard in zip(TURN_NAMES, shards):
        if shard.get("turn_name") != expected_name or not all(
            _verify_record(shard.get(key)) for key in ("input", "prompt", "schema")
        ):
            raise JudgeV5CalibrationV58RecoveryError("v57 frozen shard binding drifted")
    return {
        **{name: _record(path) for name, path in paths.items()},
        **{f"v56_{name}": record for name, record in v56_records.items()},
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V58_CAPACITY_AUDIT_VERSION,
        "phase_id": V58_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "v57_semantic_tokens": 0,
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": len(TURN_NAMES) * MAX_TOKENS_PER_TURN,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v58 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV58RecoveryError("immutable v58 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V58_CAPACITY_POLICY_VERSION,
        "phase_id": V58_PHASE_ID,
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
        "phase_total_token_bound": len(TURN_NAMES) * MAX_TOKENS_PER_TURN,
        "projected_phase_quota_points": math.ceil(
            len(TURN_NAMES)
            * MAX_TOKENS_PER_TURN
            * QUOTA_POINTS_PER_MILLION_TOKENS
            / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    if policy_path.exists():
        prior = _load_json(policy_path, "v58 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV58RecoveryError("immutable v58 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v58_recovery(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v57_root: Path = DEFAULT_V57_ROOT,
    v56_root: Path = DEFAULT_V56_ROOT,
    model: str = "gpt-5.6-luna",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v57_predecessor(v57_root.resolve(), v56_root.resolve())
    taxonomy = _load_json(v57_root / "error-taxonomy.json", "v57 taxonomy")
    value = _load_json(v57_root / "root-projection-input-full.private.json", "v57 input")
    truth = _load_json(v57_root / "diagnostic-truth.private.json", "v57 truth")
    taxonomy_path = root / "error-taxonomy.json"
    input_path = root / "root-projection-input-full.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    _write_immutable(taxonomy_path, taxonomy)
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    frozen_shards = []
    for turn_name in TURN_NAMES:
        source = v57_root / "turns" / turn_name.replace("_", "-")
        shard_input = _load_json(source / "input.private.json", f"v57 {turn_name} input")
        schema = _load_json(source / "schema.json", f"v57 {turn_name} schema")
        prompt = (source / "prompt.private.md").read_text(encoding="utf-8")
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=shard_input,
            prompt=prompt,
            schema=schema,
        )
        frozen_shards.append(
            {"turn_name": turn_name, "input": shard_input, "prompt": prompt, "schema": schema, "paths": paths}
        )
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V58_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "exact_v57_request_recovery_after_zero_usage_presemantic_failure",
        "semantic_strategy_changed_from_v57": False,
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "promotion_rule": "pass_authorizes_one_fresh_integrated_12_case_development_diagnostic_only",
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v57_repair.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v56_structured.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v55_structured.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "taxonomy": _record(taxonomy_path),
            "full": _record(input_path),
            "truth": _record(truth_path),
            "shards": [
                {
                    "turn_name": shard["turn_name"],
                    "input": _record(shard["paths"]["input"]),
                    "prompt": _record(shard["paths"]["prompt"]),
                    "schema": _record(shard["paths"]["schema"]),
                }
                for shard in frozen_shards
            ],
        },
        "privacy": "private_inputs_prompts_outputs_no_source_text_in_reports",
    }
    spec_path = root / "root-projection-recovery-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v58 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV58RecoveryError("immutable v58 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "taxonomy": taxonomy,
        "truth": truth,
        "shards": frozen_shards,
        "capacity_policy": capacity["policy"],
    }


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
            usage = _validate_usage(_load_json(Path(record["path"]), "v58 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = not partial and unknown == 0
    failure = {
        "schema_version": V58_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": known if complete else None,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V58_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "integrated_diagnostic_authorized": False,
        "full_calibration_authorized": False,
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


async def run_v58_recovery(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v57_root: Path = DEFAULT_V57_ROOT,
    v56_root: Path = DEFAULT_V56_ROOT,
    model: str = "gpt-5.6-luna",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v58 terminal")
    frozen = freeze_v58_recovery(
        output_dir=root,
        v57_root=v57_root,
        v56_root=v56_root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
    )
    factory = client_factory or _client_factory
    outputs = []
    operations = []
    sidecars = []
    adopted = {}
    current_turn = None
    try:
        async with factory(frozen["capacity_policy"]) as client:
            for shard in frozen["shards"]:
                current_turn = shard["turn_name"]
                output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=shard["paths"],
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                    base_instructions=v57_base_instructions(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["units"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda value, item=shard: _validate_sanitizable_output(value, item["input"]),
                )
                sanitized, shard_operations = sanitize_nonexact_spans(output, shard["input"])
                if validate_v55_output(sanitized, shard["input"]):
                    raise JudgeV5CalibrationV58RecoveryError("sanitized v58 output remained invalid")
                sanitized_path = shard["paths"]["output"].with_name("sanitized-output.private.json")
                _write_immutable(sanitized_path, sanitized)
                outputs.extend(sanitized["units"])
                operations.extend(shard_operations)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
        merged = {"units": outputs}
        output_path = root / "root-projection-output-full.private.json"
        _write_immutable(output_path, merged)
        audit = {
            "schema_version": V58_AUDIT_VERSION,
            "created_at": now_iso(),
            "dropped_span_count": len(operations),
            "operations": operations,
            "semantic_decisions_changed": False,
            "rationales_changed": False,
            "privacy": "opaque_ids_field_enums_and_span_hashes_only",
        }
        audit_path = root / "exact-span-sanitization-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v57(merged, frozen["truth"], frozen["taxonomy"])
        score["schema_version"] = V58_SCORE_VERSION
        score["exact_span_sanitization"] = _record(audit_path)
        score_path = root / "root-projection-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V58_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v58_root_projection_passed_integrated_diagnostic_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v58_root_projection_passed_integrated_diagnostic_authorized"
                if passed
                else "v58_root_projection_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "root_projection_passed": passed,
            "integrated_diagnostic_authorized": passed,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(output_path),
            "sanitization_audit": _record(audit_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": adopted,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Recover exact v57 requests in v58")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v57-root", default=str(DEFAULT_V57_ROOT))
    parser.add_argument("--v56-root", default=str(DEFAULT_V56_ROOT))
    parser.add_argument("--model", default="gpt-5.6-luna")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v58_recovery(
            output_dir=Path(args.output_dir),
            v57_root=Path(args.v57_root),
            v56_root=Path(args.v56_root),
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "root_projection_passed": terminal.get("root_projection_passed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
