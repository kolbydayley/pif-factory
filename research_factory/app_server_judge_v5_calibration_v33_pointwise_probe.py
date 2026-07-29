from __future__ import annotations

"""Versioned v33 pointwise repair probe after the v31 pointwise ceiling."""

import argparse
import asyncio
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import (
    PROTOCOL_VERSION,
    build_pointwise_support_prompt,
    pointwise_support_base_instructions,
    pointwise_support_output_schema,
    validate_pointwise_support_output,
)
from .app_server_judge_v5_calibration import CALIBRATION_GATES, pointwise_input_subset
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    DEFAULT_PIPELINE_ROOT,
    MAX_TOKENS_PER_TURN,
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _merge_outputs,
    _ratio,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v28_diagnostic import (
    POINTWISE_CASES_PER_SHARD,
    V28_CASE_IDS,
    _chunked,
)
from .app_server_judge_v5_calibration_v32_continuation import (
    DEFAULT_V31_ROOT,
    _validate_v31_failure,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _canonical_json,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso


V33_POINTWISE_SPEC_VERSION = "pif_app_server_judge_v5_4_v33_pointwise_probe_spec_v1"
V33_POINTWISE_SCORE_VERSION = "pif_app_server_judge_v5_4_v33_pointwise_probe_score_v1"
V33_POINTWISE_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v33_pointwise_probe_terminal_v1"
V33_POINTWISE_FAILURE_VERSION = "pif_app_server_judge_v5_4_v33_pointwise_probe_failure_v1"
V33_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V33_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V33_PHASE_ID = "judge_v5_4_v33_pointwise_probe"

DEFAULT_OUTPUT_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v33-pointwise-probe"
).resolve()


class JudgeV5CalibrationV33PointwiseProbeError(RuntimeError):
    """The v33 pointwise probe cannot continue without violating its contract."""


def _turn_names() -> list[str]:
    return [
        f"pointwise_case_context_repair_shard_{index:02d}"
        for index in range(math.ceil(len(V28_CASE_IDS) / POINTWISE_CASES_PER_SHARD))
    ]


def _pointwise_probe_instructions_v33() -> str:
    return (
        pointwise_support_base_instructions()
        + " V33 pointwise probe: prior initial and repaired receipts are candidates, not "
        "truth. Resolve harmless role aliases, title variants, pronouns, and same-source "
        "coreference using the same-case context, but judge each witness independently. "
        "Do not copy a peer witness's unsupported assertion into this witness. "
        "Separate claim_text support from structured-field correctness. Specific field "
        "issues require a direct source contradiction or unlicensed populated field."
    )


def _build_capacity_policy(*, root: Path, v31_root: Path) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    turn_names = _turn_names()
    predecessor = _validate_v31_failure(v31_root)
    audit = {
        "schema_version": V33_CAPACITY_AUDIT_VERSION,
        "phase_id": V33_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v31_predecessor": predecessor,
        "measured_basis": {
            "v31_known_usage_total_tokens": int(
                (_load_json(v31_root / "terminal.json", "v31 terminal").get("usage") or {}).get(
                    "total_tokens"
                )
                or 0
            ),
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": len(turn_names) * MAX_TOKENS_PER_TURN,
        },
        "precommitted_experiment": "case_context_pointwise_repair_probe",
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v33 capacity audit")
        stable = deepcopy(audit)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV33PointwiseProbeError("immutable v33 capacity audit drifted")
        audit = prior
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V33_CAPACITY_POLICY_VERSION,
        "phase_id": V33_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": turn_names,
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": len(turn_names) * MAX_TOKENS_PER_TURN,
        "projected_phase_quota_points": math.ceil(
            len(turn_names) * MAX_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    if policy_path.exists():
        prior = _load_json(policy_path, "v33 capacity policy")
        stable = deepcopy(policy)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV33PointwiseProbeError("immutable v33 capacity policy drifted")
        policy = prior
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def _pointwise_output_subset(
    output: Mapping[str, Any], case_ids: Sequence[str]
) -> dict[str, Any]:
    wanted = set(case_ids)
    rows = [deepcopy(row) for row in output["units"] if row["case_id"] in wanted]
    if {row["case_id"] for row in rows} != wanted:
        raise JudgeV5CalibrationV33PointwiseProbeError("pointwise subset coverage drifted")
    return {"units": rows}


def _build_pointwise_probe_prompt(
    *,
    shard_input: Mapping[str, Any],
    prior_initial: Mapping[str, Any],
    prior_repaired: Mapping[str, Any],
) -> str:
    initial_by_id = {row["witness_id"]: row for row in prior_initial["units"]}
    repaired_by_id = {row["witness_id"]: row for row in prior_repaired["units"]}
    units = []
    for unit in shard_input["units"]:
        witness_id = unit["witness_id"]
        if witness_id not in initial_by_id or witness_id not in repaired_by_id:
            raise JudgeV5CalibrationV33PointwiseProbeError("prior pointwise receipt coverage drifted")
        units.append(
            {
                **deepcopy(unit),
                "prior_initial_receipt": deepcopy(initial_by_id[witness_id]),
                "prior_repaired_receipt": deepcopy(repaired_by_id[witness_id]),
            }
        )
    instructions = (
        "Return every case_id/witness_id exactly once. The two prior receipts are "
        "diagnostic candidates only. Re-evaluate from the source excerpt. Same-case peer "
        "units are visible only through shared case_id/source text and may be used for "
        "coreference resolution, never as truth. A supported proposition requires exact "
        "source entailment of claim_text. structured_field_verdict=incorrect requires the "
        "minimal direct issue fields; empty fields are not errors."
    )
    packet = {"units": units}
    return instructions + "\n\n# V33 pointwise probe units\n" + _canonical_json(packet) + "\n"


def freeze_v33_pointwise_probe(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v31_root: Path = DEFAULT_V31_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v31_failure(v31_root)
    pointwise_input = _load_json(v31_root / "pointwise-input-full.private.json", "v31 pointwise input")
    initial = _load_json(v31_root / "pointwise-output-initial.private.json", "v31 initial pointwise")
    repaired = _load_json(v31_root / "pointwise-output-full.private.json", "v31 repaired pointwise")
    truth = _load_json(v31_root / "diagnostic-truth.private.json", "v31 truth")
    for name, value in (
        ("pointwise-input-full.private.json", pointwise_input),
        ("pointwise-output-initial.private.json", initial),
        ("pointwise-output-predecessor.private.json", repaired),
        ("diagnostic-truth.private.json", truth),
    ):
        _write_immutable(root / name, value)
    shards = []
    for index, case_ids in enumerate(_chunked(V28_CASE_IDS, POINTWISE_CASES_PER_SHARD)):
        shard_input = pointwise_input_subset(pointwise_input, case_ids)
        prompt = _build_pointwise_probe_prompt(
            shard_input=shard_input,
            prior_initial=_pointwise_output_subset(initial, case_ids),
            prior_repaired=_pointwise_output_subset(repaired, case_ids),
        )
        schema = pointwise_support_output_schema(shard_input)
        turn_name = f"pointwise_case_context_repair_shard_{index:02d}"
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=shard_input,
            prompt=prompt,
            schema=schema,
        )
        shards.append(
            {
                "index": index,
                "case_ids": case_ids,
                "input": shard_input,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
                "turn_name": turn_name,
            }
        )
    capacity = _build_capacity_policy(root=root, v31_root=v31_root)
    spec = {
        "schema_version": V33_POINTWISE_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "protocol_version": PROTOCOL_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "case_context_pointwise_repair_probe",
        "case_count": len(V28_CASE_IDS),
        "witness_count": len(pointwise_input["units"]),
        "case_ids": list(V28_CASE_IDS),
        "turn_plan": _turn_names(),
        "retry_count_per_turn": 0,
        "predecessor_v31_replayed": False,
        "full_calibration_authorized_before_pointwise_probe_pass": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "v31_predecessor": predecessor,
        "frozen_inputs": {
            "pointwise_input": _record(root / "pointwise-input-full.private.json"),
            "initial_pointwise": _record(root / "pointwise-output-initial.private.json"),
            "predecessor_repaired_pointwise": _record(
                root / "pointwise-output-predecessor.private.json"
            ),
            "truth": _record(root / "diagnostic-truth.private.json"),
            "pointwise_shards": [
                {
                    "index": shard["index"],
                    "case_ids": shard["case_ids"],
                    "input": _record(shard["paths"]["input"]),
                    "prompt": _record(shard["paths"]["prompt"]),
                    "schema": _record(shard["paths"]["schema"]),
                }
                for shard in shards
            ],
        },
        "gates": {
            key: CALIBRATION_GATES[key]
            for key in (
                "support_sensitivity_min",
                "support_specificity_min",
                "structured_field_accuracy_min",
            )
        },
        "privacy": "private_inputs_prompts_outputs_no_sanitized_text_in_terminal",
    }
    spec_path = root / "pointwise-probe-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v33 pointwise probe spec")
        stable = deepcopy(spec)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV33PointwiseProbeError("immutable v33 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "truth": truth,
        "pointwise_input": pointwise_input,
        "pointwise_shards": shards,
        "capacity_policy": capacity["policy"],
    }


def _f1(tp: int, fp: int, fn: int) -> float:
    den = (2 * tp) + fp + fn
    return round((2 * tp) / den, 6) if den else 0.0


def _score_pointwise(output: Mapping[str, Any], truth: Mapping[str, Any]) -> dict[str, Any]:
    pointwise_rows = {str(row["witness_id"]): row for row in output["units"]}
    support_tp = support_fn = support_tn = support_fp = 0
    structured_correct = structured_total = 0
    field_tp = field_fp = field_fn = 0
    support_confusion: Counter[str] = Counter()
    for case_id, case_truth in truth["cases"].items():
        for witness_id, expected in case_truth["proposition"].items():
            observed = pointwise_rows[witness_id]["proposition_verdict"]
            support_confusion[f"{expected}->{observed}"] += 1
            if expected == "supported" and observed == "supported":
                support_tp += 1
            elif expected == "supported":
                support_fn += 1
            elif observed == "unsupported":
                support_tn += 1
            else:
                support_fp += 1
            structured_total += 1
            structured_correct += int(
                pointwise_rows[witness_id]["structured_field_verdict"]
                == case_truth["structured_fields"][witness_id]
            )
            wanted = set(case_truth["field_issues"][witness_id])
            got = set(pointwise_rows[witness_id]["field_issue_fields"])
            field_tp += len(wanted & got)
            field_fp += len(got - wanted)
            field_fn += len(wanted - got)
    metrics = {
        "case_count": len(truth["cases"]),
        "witness_count": structured_total,
        "support_sensitivity": _ratio(support_tp, support_tp + support_fn),
        "support_specificity": _ratio(support_tn, support_tn + support_fp),
        "structured_field_accuracy": _ratio(structured_correct, structured_total),
        "pointwise_field_issue_f1": _f1(field_tp, field_fp, field_fn),
    }
    checks = {
        "support_sensitivity": metrics["support_sensitivity"]
        >= CALIBRATION_GATES["support_sensitivity_min"],
        "support_specificity": metrics["support_specificity"]
        >= CALIBRATION_GATES["support_specificity_min"],
        "structured_field_accuracy": metrics["structured_field_accuracy"]
        >= CALIBRATION_GATES["structured_field_accuracy_min"],
    }
    return {
        "schema_version": V33_POINTWISE_SCORE_VERSION,
        "passed": all(checks.values()),
        "metrics": metrics,
        "checks": checks,
        "failed_checks": sorted(key for key, passed in checks.items() if not passed),
        "support_confusion": dict(sorted(support_confusion.items())),
        "gates": CALIBRATION_GATES,
    }


def _write_failure_terminal(
    *, root: Path, spec_path: Path, turn_name: Optional[str], error_class: str
) -> dict[str, Any]:
    attempts = _attempt_records(root)
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    partial = False
    for attempt in attempts:
        sidecar_record = attempt.get("sidecar")
        if not isinstance(sidecar_record, dict):
            if attempt.get("capacity") is not None or attempt.get("output") is not None:
                partial = True
            continue
        sidecar = _load_json(Path(sidecar_record["path"]), "v33 failed sidecar")
        try:
            usage = _validate_usage(sidecar)
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    accounting_complete = not partial and unknown == 0
    failure = {
        "schema_version": V33_POINTWISE_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": accounting_complete,
        "usage_status": "complete" if accounting_complete else "unknown",
        "usage": known if accounting_complete else None,
        "known_usage_lower_bound": known,
        "unknown_usage_turn_count": unknown,
        "partial_attempt_without_sidecar": partial,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V33_POINTWISE_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "spec_sha256": _sha256_file(spec_path),
        "failure": _record(failure_path),
        "pointwise_probe_passed": False,
        "full_diagnostic_authorized": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_allowed": False,
        "semantic_retry_count": 0,
        "accounting_complete": accounting_complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v33_pointwise_probe(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v31_root: Path = DEFAULT_V31_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v33 terminal")
    frozen = freeze_v33_pointwise_probe(
        output_dir=root,
        v31_root=v31_root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
    )
    policy_path = frozen["capacity_policy"]
    factory = client_factory or _client_factory
    sidecars: list[dict[str, Any]] = []
    adopted: dict[str, bool] = {}
    current_turn: Optional[str] = None
    try:
        async with factory(policy_path) as client:
            outputs = []
            for shard in frozen["pointwise_shards"]:
                current_turn = shard["turn_name"]
                output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=shard["paths"],
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                    base_instructions=_pointwise_probe_instructions_v33(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["units"]),
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard: validate_pointwise_support_output(
                        value, item["input"]
                    ),
                )
                outputs.append(output)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
        pointwise_output = _merge_outputs(outputs, "units")
        pointwise_output_path = root / "pointwise-output-full.private.json"
        _write_immutable(pointwise_output_path, pointwise_output)
        score = _score_pointwise(pointwise_output, frozen["truth"])
        score_path = root / "pointwise-probe-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V33_POINTWISE_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v33_pointwise_probe_passed_full_diagnostic_required"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v33_pointwise_probe_passed_full_diagnostic_required"
                if passed
                else "v33_pointwise_probe_quality_gate_not_passed"
            ),
            "spec_sha256": _sha256_file(frozen["spec_path"]),
            "pointwise_probe_passed": passed,
            "full_diagnostic_authorized": passed,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_allowed": False,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": adopted,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure_terminal(
            root=root,
            spec_path=frozen["spec_path"],
            turn_name=exc.turn_name,
            error_class=exc.error_class,
        )
    except Exception as exc:
        return _write_failure_terminal(
            root=root,
            spec_path=frozen["spec_path"],
            turn_name=current_turn,
            error_class=type(exc).__name__,
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v33 pointwise repair probe")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v31-root", default=str(DEFAULT_V31_ROOT))
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v33_pointwise_probe(
            output_dir=Path(args.output_dir),
            v31_root=Path(args.v31_root),
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal.get("state"),
                "terminal_reason": terminal.get("terminal_reason"),
                "pointwise_probe_passed": terminal.get("pointwise_probe_passed"),
                "full_diagnostic_authorized": terminal.get("full_diagnostic_authorized"),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
