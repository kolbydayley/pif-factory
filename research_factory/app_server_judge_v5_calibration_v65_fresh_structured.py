from __future__ import annotations

"""Fresh all-36-witness structured diagnostic against the v64 reference."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import CHECKLIST_FIELDS
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
)
from .app_server_judge_v5_calibration_v62_root_status import (
    build_v62_prompt,
    project_v62_contract,
    v62_base_instructions,
    v62_output_schema,
    validate_v62_output,
)
from .app_server_judge_v5_calibration_v64_reference_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V64_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso


V65_INPUT_VERSION = "pif_app_server_judge_v5_4_v65_fresh_structured_input_v1"
V65_TRUTH_VERSION = "pif_app_server_judge_v5_4_v65_fresh_structured_truth_v1"
V65_SPEC_VERSION = "pif_app_server_judge_v5_4_v65_fresh_structured_spec_v1"
V65_SCORE_VERSION = "pif_app_server_judge_v5_4_v65_fresh_structured_score_v1"
V65_AUDIT_VERSION = "pif_app_server_judge_v5_4_v65_contract_projection_audit_v1"
V65_FAILURE_VERSION = "pif_app_server_judge_v5_4_v65_fresh_structured_failure_v1"
V65_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v65_fresh_structured_terminal_v1"
V65_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V65_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V65_PHASE_ID = "judge_v5_4_v65_fresh_all_36_structured_diagnostic"
TURN_NAMES = tuple(f"fresh_root_status_shard_{index:02d}" for index in range(3))
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V64_ROOT.parent / "judge-calibration-v5_4-v65-fresh-all36-structured"
).resolve()


class JudgeV5CalibrationV65StructuredError(RuntimeError):
    """The fresh all-36 structured diagnostic cannot preserve its contract."""


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


def _validate_predecessors(v64_root: Path, v55_root: Path) -> dict[str, Any]:
    paths = {
        "v64_terminal": v64_root / "terminal.json",
        "v64_truth": v64_root / "calibration-truth.private.json",
        "v64_audit": v64_root / "reference-patch-audit.json",
        "v64_receipt": v64_root / "reference-receipt.json",
        "v55_terminal": v55_root / "terminal.json",
        "v55_spec": v55_root / "structured-checklist-spec.json",
        "v55_input": v55_root / "structured-checklist-input-full.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    v64 = values["v64_terminal"]
    receipt = values["v64_receipt"]
    v55 = values["v55_terminal"]
    spec = values["v55_spec"]
    if (
        v64.get("state") != "completed"
        or v64.get("terminal_reason") != "v64_reference_v2_frozen_fresh_diagnostic_required"
        or v64.get("reference_frozen") is not True
        or v64.get("fresh_diagnostic_authorized") is not True
        or v64.get("full_calibration_authorized") is not False
        or v64.get("production_mutated") is not False
        or v64.get("new_semantic_turn_count") != 0
        or not _record_matches(v64.get("truth"), paths["v64_truth"])
        or not _record_matches(v64.get("patch_audit"), paths["v64_audit"])
        or not _record_matches(v64.get("reference_receipt"), paths["v64_receipt"])
        or receipt.get("reference_frozen") is not True
        or receipt.get("case_count") != 66
        or receipt.get("witness_count") != 182
        or v55.get("state") != "failed"
        or v55.get("usage_status") != "complete"
        or v55.get("production_mutated") is not False
        or spec.get("case_count") != 12
        or spec.get("witness_count") != 36
        or not _record_matches(spec["frozen_inputs"].get("full"), paths["v55_input"])
        or not all(_verify_record(record) for record in spec.get("runtime_files") or [])
    ):
        raise JudgeV5CalibrationV65StructuredError("v64/v55 predecessors are inadmissible")
    return {name: _record(path) for name, path in paths.items()}


def build_v65_input(source: Mapping[str, Any]) -> dict[str, Any]:
    units = []
    for row in source.get("units") or []:
        units.append(
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "source_excerpt": row["source_excerpt"],
                "structured_event": deepcopy(row["structured_event"]),
                "frozen_proposition_verdict": row["frozen_proposition_verdict"],
            }
        )
    if len(units) != 36 or len({(row["case_id"], row["witness_id"]) for row in units}) != 36:
        raise JudgeV5CalibrationV65StructuredError("v65 source coverage drifted")
    return {
        "schema_version": V65_INPUT_VERSION,
        "units": units,
        "checklist_field_order": list(CHECKLIST_FIELDS),
        "prior_labels_present": False,
        "candidate_outputs_present": False,
        "system_identity_present": False,
    }


def build_v65_truth(full_truth: Mapping[str, Any], value: Mapping[str, Any]) -> dict[str, Any]:
    selected = {(row["case_id"], row["witness_id"]) for row in value["units"]}
    cases: dict[str, Any] = {}
    for case_id, witness_id in sorted(selected):
        source = full_truth["cases"].get(case_id)
        if source is None or witness_id not in source["field_issues"]:
            raise JudgeV5CalibrationV65StructuredError("v65 truth identity is absent")
        case = cases.setdefault(case_id, {"field_issues": {}, "structured_fields": {}})
        case["field_issues"][witness_id] = deepcopy(source["field_issues"][witness_id])
        case["structured_fields"][witness_id] = source["structured_fields"][witness_id]
    if len(cases) != 12 or sum(len(case["field_issues"]) for case in cases.values()) != 36:
        raise JudgeV5CalibrationV65StructuredError("v65 truth coverage drifted")
    return {
        "schema_version": V65_TRUTH_VERSION,
        "reference_version": full_truth["reference_version"],
        "case_count": 12,
        "witness_count": 36,
        "cases": cases,
    }


def _shards(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    units = value["units"]
    chunks = [units[index : index + 12] for index in range(0, len(units), 12)]
    if len(chunks) != 3 or any(len(chunk) != 12 for chunk in chunks):
        raise JudgeV5CalibrationV65StructuredError("v65 shard layout drifted")
    return [
        {**{key: deepcopy(child) for key, child in value.items() if key != "units"}, "units": deepcopy(chunk)}
        for chunk in chunks
    ]


def score_v65(output: Mapping[str, Any], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    rows = {(row["case_id"], row["witness_id"]): row for row in output.get("units") or []}
    if set(rows) != set(expected):
        raise JudgeV5CalibrationV65StructuredError("v65 score coverage drifted")
    tp = fp = fn = exact = verdict_correct = root_cells_correct = 0
    abstentions = unsupported_correct = boundary_correct = attribution_tn = attribution_total = 0
    for key, fields in expected.items():
        checklist = rows[key]["checklist"]
        observed = {row["field"] for row in checklist if row["independent_root_status"] == "root"}
        tp += len(fields & observed)
        fp += len(observed - fields)
        fn += len(fields - observed)
        exact += int(observed == fields)
        expected_verdict = "incorrect" if fields else "correct"
        verdict_correct += int(rows[key]["structured_field_verdict"] == expected_verdict)
        root_cells_correct += sum(
            (row["independent_root_status"] == "root") == (row["field"] in fields)
            for row in checklist
        )
        decisions = {row["field"]: row for row in checklist}
        unsupported_correct += int(
            (decisions["unsupported_inference"]["independent_root_status"] == "root")
            == ("unsupported_inference" in fields)
        )
        boundary_correct += int(
            decisions["event_boundary"]["independent_root_status"] == "not_different"
        )
        if "attribution" not in fields:
            attribution_total += 1
            attribution_tn += int(decisions["attribution"]["independent_root_status"] != "root")
        abstentions += sum(row["independent_root_status"] == "abstain" for row in checklist)
    den = 2 * tp + fp + fn
    total_cells = 36 * len(CHECKLIST_FIELDS)
    metrics = {
        "witness_count": 36,
        "structured_field_accuracy": round(verdict_correct / 36, 6),
        "root_field_f1": round((2 * tp) / den, 6) if den else 0.0,
        "root_checklist_cell_accuracy": round(root_cells_correct / total_cells, 6),
        "exact_case_rate": round(exact / 36, 6),
        "unsupported_inference_accuracy": round(unsupported_correct / 36, 6),
        "event_boundary_scope_accuracy": round(boundary_correct / 36, 6),
        "attribution_specificity": round(attribution_tn / attribution_total, 6),
        "abstention_count": abstentions,
    }
    checks = {
        "structured_field_accuracy": metrics["structured_field_accuracy"] >= 0.95,
        "root_field_f1": metrics["root_field_f1"] >= 0.95,
        "root_checklist_cell_accuracy": metrics["root_checklist_cell_accuracy"] >= 0.99,
        "exact_case_rate": metrics["exact_case_rate"] >= 0.95,
        "unsupported_inference_accuracy": metrics["unsupported_inference_accuracy"] == 1.0,
        "event_boundary_scope_accuracy": metrics["event_boundary_scope_accuracy"] == 1.0,
        "attribution_specificity": metrics["attribution_specificity"] >= 0.95,
        "abstention_count": metrics["abstention_count"] == 0,
    }
    return {
        "schema_version": V65_SCORE_VERSION,
        "passed": all(checks.values()),
        "metrics": metrics,
        "checks": checks,
        "failed_checks": sorted(key for key, passed in checks.items() if not passed),
        "gates_frozen_before_semantic_calls": True,
        "next_authorization_if_passed": "one_fresh_full_66_case_calibration_only",
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V65_CAPACITY_AUDIT_VERSION,
        "phase_id": V65_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": len(TURN_NAMES) * MAX_TOKENS_PER_TURN,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v65 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV65StructuredError("immutable v65 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V65_CAPACITY_POLICY_VERSION,
        "phase_id": V65_PHASE_ID,
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
        prior = _load_json(policy_path, "v65 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV65StructuredError("immutable v65 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v65(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v64_root: Path = DEFAULT_V64_ROOT,
    v55_root: Path = DEFAULT_V55_ROOT,
    model: str = "gpt-5.5",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors(v64_root.resolve(), v55_root.resolve())
    value = build_v65_input(
        _load_json(v55_root / "structured-checklist-input-full.private.json", "v55 input")
    )
    truth = build_v65_truth(
        _load_json(v64_root / "calibration-truth.private.json", "v64 truth"), value
    )
    input_path = root / "fresh-structured-input.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    frozen_shards = []
    for turn_name, shard_input in zip(TURN_NAMES, _shards(value)):
        prompt = build_v62_prompt(shard_input)
        schema = v62_output_schema(shard_input)
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
        "schema_version": V65_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_all_36_explicit_value_relation_and_root_status",
        "case_count": 12,
        "witness_count": 36,
        "units_per_turn": 12,
        "turn_plan": list(TURN_NAMES),
        "prior_labels_in_model_input": False,
        "candidate_outputs_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "pass_authorizes_one_fresh_full_66_case_calibration_only",
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v64_reference_freeze.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v62_root_status.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
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
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "fresh-structured-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v65 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV65StructuredError("immutable v65 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
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
            usage = _validate_usage(_load_json(Path(record["path"]), "v65 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = not partial and unknown == 0
    failure = {
        "schema_version": V65_FAILURE_VERSION,
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
        "schema_version": V65_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
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


async def run_v65(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v64_root: Path = DEFAULT_V64_ROOT,
    v55_root: Path = DEFAULT_V55_ROOT,
    model: str = "gpt-5.5",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v65 terminal")
    frozen = freeze_v65(
        output_dir=root,
        v64_root=v64_root,
        v55_root=v55_root,
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
                    base_instructions=v62_base_instructions(),
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=12,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda value, item=shard: validate_v62_output(
                        project_v62_contract(value, item["input"])[0], item["input"]
                    ),
                )
                projected, shard_operations = project_v62_contract(output, shard["input"])
                if validate_v62_output(projected, shard["input"]):
                    raise JudgeV5CalibrationV65StructuredError("projected v65 output remained invalid")
                projected_path = shard["paths"]["output"].with_name("projected-output.private.json")
                _write_immutable(projected_path, projected)
                outputs.extend(projected["units"])
                operations.extend(shard_operations)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
        merged = {"units": outputs}
        output_path = root / "fresh-structured-output.private.json"
        _write_immutable(output_path, merged)
        audit = {
            "schema_version": V65_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "root_field_projection_source": "llm_independent_root_status_only",
            "new_semantic_decisions_from_deterministic_code": False,
            "privacy": "opaque_ids_enums_counts_and_span_hashes_only",
        }
        audit_path = root / "contract-projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v65(merged, frozen["truth"])
        score["contract_projection_audit"] = _record(audit_path)
        score_path = root / "fresh-structured-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V65_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v65_fresh_structured_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v65_fresh_structured_passed_full_calibration_authorized"
                if passed
                else "v65_fresh_structured_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "fresh_structured_diagnostic_passed": passed,
            "full_calibration_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(output_path),
            "contract_projection_audit": _record(audit_path),
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
    parser = argparse.ArgumentParser(description="Run v65 fresh all-36 structured diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v64-root", default=str(DEFAULT_V64_ROOT))
    parser.add_argument("--v55-root", default=str(DEFAULT_V55_ROOT))
    parser.add_argument("--model", default="gpt-5.5")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v65(
            output_dir=Path(args.output_dir),
            v64_root=Path(args.v64_root),
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
                "fresh_structured_diagnostic_passed": terminal.get("fresh_structured_diagnostic_passed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
