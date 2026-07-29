from __future__ import annotations

"""No-replay v55 continuation with audited exact-span sanitization."""

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
from .app_server_judge_v5_calibration_v55_structured import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V55_ROOT,
    V55_PHASE_ID,
    score_v55,
    v55_base_instructions,
    validate_v55_output,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso, sha256_text


V56_SPEC_VERSION = "pif_app_server_judge_v5_4_v56_structured_continuation_spec_v1"
V56_SCORE_VERSION = "pif_app_server_judge_v5_4_v56_structured_continuation_score_v1"
V56_AUDIT_VERSION = "pif_app_server_judge_v5_4_v56_exact_span_sanitization_audit_v1"
V56_FAILURE_VERSION = "pif_app_server_judge_v5_4_v56_structured_continuation_failure_v1"
V56_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v56_structured_continuation_terminal_v1"
V56_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V56_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V56_PHASE_ID = "judge_v5_4_v56_structured_checklist_continuation"
REMAINING_TURN_NAMES = (
    "structured_checklist_shard_01",
    "structured_checklist_shard_02",
    "structured_checklist_shard_03",
)
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V55_ROOT.parent / "judge-calibration-v5_4-v56-structured-checklist-continuation"
).resolve()


class JudgeV5CalibrationV56StructuredError(RuntimeError):
    """The v56 continuation cannot preserve v55 intent-to-treat evidence."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def sanitize_nonexact_spans(
    output: Mapping[str, Any], value: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    sanitized = deepcopy(output)
    sources = {
        (str(row["case_id"]), str(row["witness_id"])): str(row["source_excerpt"])
        for row in value.get("units") or []
    }
    operations = []
    for unit in sanitized.get("units") or []:
        key = (str(unit.get("case_id")), str(unit.get("witness_id")))
        source = sources.get(key)
        if source is None:
            continue
        for row in unit.get("checklist") or []:
            spans = row.get("source_evidence_spans")
            if not isinstance(spans, list):
                continue
            retained = []
            for span in spans:
                if isinstance(span, str) and span and span in source:
                    retained.append(span)
                else:
                    rendered = span if isinstance(span, str) else json.dumps(span, sort_keys=True)
                    operations.append(
                        {
                            "case_id": key[0],
                            "witness_id": key[1],
                            "field": str(row.get("field")),
                            "dropped_span_sha256": sha256_text(rendered),
                            "dropped_span_size_bytes": len(rendered.encode("utf-8")),
                            "semantic_decision_changed": False,
                        }
                    )
            row["source_evidence_spans"] = retained
    return sanitized, operations


def _validate_sanitizable_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping):
        return ["invalid_v56_output_root"]
    sanitized, _operations = sanitize_nonexact_spans(output, value)
    return validate_v55_output(sanitized, value)


def _validate_predecessor(v55_root: Path) -> dict[str, Any]:
    paths = {
        "v55_terminal": v55_root / "terminal.json",
        "v55_failure": v55_root / "failure.json",
        "v55_spec": v55_root / "structured-checklist-spec.json",
        "v55_truth": v55_root / "diagnostic-truth.private.json",
        "v55_full_input": v55_root / "structured-checklist-input-full.private.json",
        "v55_shard00_input": v55_root / "turns/structured-checklist-shard-00/input.private.json",
        "v55_shard00_output": v55_root / "turns/structured-checklist-shard-00/output.private.json",
        "v55_shard00_sidecar": v55_root / "turns/structured-checklist-shard-00/sidecar.json",
        "v55_shard00_capacity": v55_root / "turns/structured-checklist-shard-00/capacity.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v55_terminal"]
    failure = values["v55_failure"]
    attempts = failure.get("attempts") or []
    expected_turn_names = [
        "structured_checklist_shard_00",
        *REMAINING_TURN_NAMES,
    ]
    attempt_shape_valid = (
        len(attempts) == len(expected_turn_names)
        and [attempt.get("turn_name") for attempt in attempts] == expected_turn_names
        and attempts[0].get("state") == "completed"
        and attempts[0].get("status") == "completed"
        and attempts[0].get("usage_status") == "measured"
        and isinstance(attempts[0].get("output"), Mapping)
        and isinstance(attempts[0].get("sidecar"), Mapping)
        and isinstance(attempts[0].get("capacity"), Mapping)
        and all(
            set(attempt) == {"turn_name", "output", "sidecar", "capacity"}
            and attempt.get("output") is None
            and attempt.get("sidecar") is None
            and attempt.get("capacity") is None
            for attempt in attempts[1:]
        )
    )
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or not _record_matches(terminal.get("failure"), paths["v55_failure"])
        or failure.get("failed_turn_name") != "structured_checklist_shard_00"
        or failure.get("error_class") != "JudgeV5CalibrationV26DiagnosticError"
        or not attempt_shape_valid
    ):
        raise JudgeV5CalibrationV56StructuredError("v55 failure is not admissible")
    sanitized, operations = sanitize_nonexact_spans(
        values["v55_shard00_output"], values["v55_shard00_input"]
    )
    if len(operations) != 2 or validate_v55_output(sanitized, values["v55_shard00_input"]):
        raise JudgeV5CalibrationV56StructuredError("v55 failed output is not safely sanitizable")
    return {name: _record(path) for name, path in paths.items()}


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any], v55_root: Path) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    v55 = _load_json(v55_root / "terminal.json", "v55 terminal")
    audit = {
        "schema_version": V56_CAPACITY_AUDIT_VERSION,
        "phase_id": V56_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "v55_total_tokens": (v55.get("usage") or {}).get("total_tokens"),
            "remaining_declared_turn_count": len(REMAINING_TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": len(REMAINING_TURN_NAMES) * MAX_TOKENS_PER_TURN,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v56 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV56StructuredError("immutable v56 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V56_CAPACITY_POLICY_VERSION,
        "phase_id": V56_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(REMAINING_TURN_NAMES),
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": len(REMAINING_TURN_NAMES) * MAX_TOKENS_PER_TURN,
        "projected_phase_quota_points": math.ceil(
            len(REMAINING_TURN_NAMES)
            * MAX_TOKENS_PER_TURN
            * QUOTA_POINTS_PER_MILLION_TOKENS
            / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    if policy_path.exists():
        prior = _load_json(policy_path, "v56 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV56StructuredError("immutable v56 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v56_structured(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v55_root: Path = DEFAULT_V55_ROOT,
    model: str = "gpt-5.6-luna",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessor(v55_root.resolve())
    truth = _load_json(v55_root / "diagnostic-truth.private.json", "v55 truth")
    full_input = _load_json(
        v55_root / "structured-checklist-input-full.private.json", "v55 full input"
    )
    truth_path = root / "diagnostic-truth.private.json"
    input_path = root / "structured-checklist-input-full.private.json"
    _write_immutable(truth_path, truth)
    _write_immutable(input_path, full_input)
    adopted_input = _load_json(
        v55_root / "turns/structured-checklist-shard-00/input.private.json", "v55 shard00 input"
    )
    adopted_output = _load_json(
        v55_root / "turns/structured-checklist-shard-00/output.private.json", "v55 shard00 output"
    )
    sanitized_adopted, adopted_operations = sanitize_nonexact_spans(adopted_output, adopted_input)
    adopted_path = root / "adopted-shard-00-sanitized.private.json"
    _write_immutable(adopted_path, sanitized_adopted)
    frozen_shards = []
    for turn_name in REMAINING_TURN_NAMES:
        source = v55_root / "turns" / turn_name.replace("_", "-")
        shard_input = _load_json(source / "input.private.json", f"v55 {turn_name} input")
        schema = _load_json(source / "schema.json", f"v55 {turn_name} schema")
        prompt = (source / "prompt.private.md").read_text(encoding="utf-8")
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=shard_input,
            prompt=prompt,
            schema=schema,
        )
        frozen_shards.append(
            {
                "turn_name": turn_name,
                "input": shard_input,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    capacity = _build_capacity_policy(root, predecessor, v55_root)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V56_SPEC_VERSION,
        "state": "frozen_before_new_model_calls",
        "created_at": now_iso(),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "no_replay_continuation_with_exact_span_only_sanitization",
        "predecessor_phase_id": V55_PHASE_ID,
        "adopted_turn_names": ["structured_checklist_shard_00"],
        "new_turn_plan": list(REMAINING_TURN_NAMES),
        "retry_count_per_turn": 0,
        "semantic_decisions_changed_by_sanitizer": False,
        "adopted_nonexact_span_count": len(adopted_operations),
        "integration_diagnostic_authorized_before_pass": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v55_structured.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "full": _record(input_path),
            "truth": _record(truth_path),
            "adopted_shard_00": _record(adopted_path),
            "new_shards": [
                {
                    "turn_name": shard["turn_name"],
                    "input": _record(shard["paths"]["input"]),
                    "prompt": _record(shard["paths"]["prompt"]),
                    "schema": _record(shard["paths"]["schema"]),
                }
                for shard in frozen_shards
            ],
        },
        "privacy": "private_inputs_prompts_outputs_no_source_text_or_span_text_in_audit",
    }
    spec_path = root / "structured-checklist-continuation-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v56 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV56StructuredError("immutable v56 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "truth": truth,
        "full_input": full_input,
        "adopted_output": sanitized_adopted,
        "adopted_operations": adopted_operations,
        "shards": frozen_shards,
        "capacity_policy": capacity["policy"],
    }


def _sum_usage(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, int]:
    return {field: int(left.get(field) or 0) + int(right.get(field) or 0) for field in USAGE_FIELDS}


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
            usage = _validate_usage(_load_json(Path(record["path"]), "v56 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = not partial and unknown == 0
    failure = {
        "schema_version": V56_FAILURE_VERSION,
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
        "schema_version": V56_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "integration_diagnostic_authorized": False,
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


async def run_v56_structured(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v55_root: Path = DEFAULT_V55_ROOT,
    model: str = "gpt-5.6-luna",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v56 terminal")
    frozen = freeze_v56_structured(
        output_dir=root,
        v55_root=v55_root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
    )
    factory = client_factory or _client_factory
    outputs = list(frozen["adopted_output"]["units"])
    operations = list(frozen["adopted_operations"])
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
                    base_instructions=v55_base_instructions(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["units"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda value, item=shard: _validate_sanitizable_output(
                        value, item["input"]
                    ),
                )
                sanitized, shard_operations = sanitize_nonexact_spans(output, shard["input"])
                if validate_v55_output(sanitized, shard["input"]):
                    raise JudgeV5CalibrationV56StructuredError("sanitized output remained invalid")
                sanitized_path = shard["paths"]["output"].with_name("sanitized-output.private.json")
                _write_immutable(sanitized_path, sanitized)
                outputs.extend(sanitized["units"])
                operations.extend(shard_operations)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
        merged = {"units": outputs}
        output_path = root / "structured-checklist-output-full.private.json"
        _write_immutable(output_path, merged)
        audit = {
            "schema_version": V56_AUDIT_VERSION,
            "created_at": now_iso(),
            "dropped_span_count": len(operations),
            "operations": operations,
            "semantic_decisions_changed": False,
            "rationales_changed": False,
            "exact_spans_retained": True,
            "privacy": "opaque_ids_field_enums_and_span_hashes_only",
        }
        audit_path = root / "exact-span-sanitization-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v55(merged, frozen["truth"])
        score["schema_version"] = V56_SCORE_VERSION
        score["exact_span_sanitization"] = _record(audit_path)
        score_path = root / "structured-checklist-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        predecessor_usage = _load_json(v55_root / "terminal.json", "v55 terminal")["usage"]
        itt_usage = _sum_usage(predecessor_usage, accounting["usage"])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V56_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v56_structured_checklist_passed_integration_diagnostic_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v56_structured_checklist_passed_integration_diagnostic_authorized"
                if passed
                else "v56_structured_checklist_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "structured_checklist_passed": passed,
            "integration_diagnostic_authorized": passed,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "adopted_predecessor_turn_count": 1,
            "new_turn_count": accounting["turn_count"],
            "score": _record(score_path),
            "output": _record(output_path),
            "sanitization_audit": _record(audit_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": adopted,
            "predecessor_usage": predecessor_usage,
            "intent_to_treat_usage": itt_usage,
            "intent_to_treat_turn_count": 1 + accounting["turn_count"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Continue v55 without replay")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v55-root", default=str(DEFAULT_V55_ROOT))
    parser.add_argument("--model", default="gpt-5.6-luna")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v56_structured(
            output_dir=Path(args.output_dir),
            v55_root=Path(args.v55_root),
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
                "structured_checklist_passed": terminal.get("structured_checklist_passed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
