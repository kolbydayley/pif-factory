from __future__ import annotations

"""Model-only gpt-5.4 specialist diagnostic over the exact v57 requests."""

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
from .app_server_judge_v5_calibration_v55_structured import validate_v55_output
from .app_server_judge_v5_calibration_v56_structured import sanitize_nonexact_spans
from .app_server_judge_v5_calibration_v57_repair import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V57_ROOT,
    TURN_NAMES,
    score_v57,
    v57_base_instructions,
)
from .app_server_judge_v5_calibration_v58_recovery import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V58_ROOT,
)
from .app_server_judge_v5_calibration_v59_projection import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V59_ROOT,
    _validate_v58,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _sha256_file,
    _validate_usage,
)
from .util import now_iso


V60_SPEC_VERSION = "pif_app_server_judge_v5_4_v60_gpt54_specialist_spec_v1"
V60_SCORE_VERSION = "pif_app_server_judge_v5_4_v60_gpt54_specialist_score_v1"
V60_AUDIT_VERSION = "pif_app_server_judge_v5_4_v60_contract_projection_audit_v1"
V60_FAILURE_VERSION = "pif_app_server_judge_v5_4_v60_gpt54_specialist_failure_v1"
V60_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v60_gpt54_specialist_terminal_v1"
V60_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V60_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V60_PHASE_ID = "judge_v5_4_v60_gpt54_structured_specialist_diagnostic"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V59_ROOT.parent / "judge-calibration-v5_4-v60-gpt54-specialist-diagnostic"
).resolve()


class JudgeV5CalibrationV60GPT54Error(RuntimeError):
    """The model-only gpt-5.4 diagnostic cannot preserve its frozen contract."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _validate_v59(v59_root: Path, v58_root: Path) -> dict[str, Any]:
    paths = {
        "v59_terminal": v59_root / "terminal.json",
        "v59_score": v59_root / "projected-root-score.json",
        "v59_audit": v59_root / "projection-audit.json",
        "v59_output": v59_root / "projected-root-output.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    terminal = values["v59_terminal"]
    score = values["v59_score"]
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v59_projected_root_quality_gate_not_passed"
        or terminal.get("projection_passed") is not False
        or terminal.get("alternate_structured_specialist_diagnostic_authorized") is not True
        or terminal.get("semantic_attempt_started") is not False
        or terminal.get("new_usage") != zero_usage
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("production_mutated") is not False
        or not _record_matches(terminal.get("score"), paths["v59_score"])
        or not _record_matches(terminal.get("projection_audit"), paths["v59_audit"])
        or not _record_matches(terminal.get("output"), paths["v59_output"])
        or score.get("passed") is not False
    ):
        raise JudgeV5CalibrationV60GPT54Error("v59 non-acceptance is inadmissible")
    v58_records = _validate_v58(v58_root.resolve())
    if terminal.get("predecessor") != v58_records:
        raise JudgeV5CalibrationV60GPT54Error("v59 v58 predecessor binding drifted")
    return {
        **{name: _record(path) for name, path in paths.items()},
        **{f"v58_{name}": record for name, record in v58_records.items()},
    }


def project_frozen_contract(
    output: Mapping[str, Any], value: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    projected, span_operations = sanitize_nonexact_spans(output, value)
    operations = [
        {**operation, "operation_type": "drop_nonexact_evidence_span"}
        for operation in span_operations
    ]
    expected = {
        (row["case_id"], row["witness_id"]): {
            "supported": "same",
            "unsupported": "different",
            "abstain": "abstain",
        }[row["frozen_proposition_verdict"]]
        for row in value.get("units") or []
    }
    for row in projected.get("units") or []:
        key = (row.get("case_id"), row.get("witness_id"))
        checklist = {
            item.get("field"): item
            for item in row.get("checklist") or []
            if isinstance(item, Mapping)
        }
        unsupported = checklist.get("unsupported_inference")
        if key in expected and isinstance(unsupported, Mapping):
            prior = unsupported.get("decision")
            if prior != expected[key]:
                unsupported["decision"] = expected[key]
                operations.append(
                    {
                        "operation_type": "project_frozen_support_receipt",
                        "case_id": key[0],
                        "witness_id": key[1],
                        "field": "unsupported_inference",
                        "prior_decision": prior,
                        "projected_decision": expected[key],
                        "semantic_source": "frozen_llm_proposition_receipt",
                    }
                )
        decisions = {
            item.get("decision")
            for item in row.get("checklist") or []
            if isinstance(item, Mapping)
        }
        row["structured_field_verdict"] = (
            "incorrect"
            if "different" in decisions
            else "abstain"
            if "abstain" in decisions
            else "correct"
        )
    return projected, operations


def _validate_projectable_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping):
        return ["invalid_v60_output_root"]
    projected, _operations = project_frozen_contract(output, value)
    return validate_v55_output(projected, value)


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V60_CAPACITY_AUDIT_VERSION,
        "phase_id": V60_PHASE_ID,
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
        prior = _load_json(audit_path, "v60 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV60GPT54Error("immutable v60 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V60_CAPACITY_POLICY_VERSION,
        "phase_id": V60_PHASE_ID,
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
        prior = _load_json(policy_path, "v60 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV60GPT54Error("immutable v60 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v60(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v59_root: Path = DEFAULT_V59_ROOT,
    v58_root: Path = DEFAULT_V58_ROOT,
    v57_root: Path = DEFAULT_V57_ROOT,
    model: str = "gpt-5.4",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v59(v59_root.resolve(), v58_root.resolve())
    taxonomy = _load_json(v57_root / "error-taxonomy.json", "v57 taxonomy")
    truth = _load_json(v57_root / "diagnostic-truth.private.json", "v57 truth")
    taxonomy_path = root / "error-taxonomy.json"
    truth_path = root / "diagnostic-truth.private.json"
    _write_immutable(taxonomy_path, taxonomy)
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
        "schema_version": V60_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "model_only_gpt54_structured_specialist_on_exact_v57_requests",
        "request_content_changed_from_v57": False,
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "deterministic_projection": "exact_span_cleanup_plus_frozen_llm_support_receipt_only",
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
            _record(runtime_dir / "app_server_judge_v5_calibration_v59_projection.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v58_recovery.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v57_repair.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v56_structured.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v55_structured.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "taxonomy": _record(taxonomy_path),
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
    spec_path = root / "gpt54-specialist-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v60 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV60GPT54Error("immutable v60 spec drifted")
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
            usage = _validate_usage(_load_json(Path(record["path"]), "v60 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = not partial and unknown == 0
    failure = {
        "schema_version": V60_FAILURE_VERSION,
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
        "schema_version": V60_TERMINAL_VERSION,
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


async def run_v60(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v59_root: Path = DEFAULT_V59_ROOT,
    v58_root: Path = DEFAULT_V58_ROOT,
    v57_root: Path = DEFAULT_V57_ROOT,
    model: str = "gpt-5.4",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v60 terminal")
    frozen = freeze_v60(
        output_dir=root,
        v59_root=v59_root,
        v58_root=v58_root,
        v57_root=v57_root,
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
                    output_validator=lambda value, item=shard: _validate_projectable_output(value, item["input"]),
                )
                projected, shard_operations = project_frozen_contract(output, shard["input"])
                if validate_v55_output(projected, shard["input"]):
                    raise JudgeV5CalibrationV60GPT54Error("projected v60 output remained invalid")
                projected_path = shard["paths"]["output"].with_name("projected-output.private.json")
                _write_immutable(projected_path, projected)
                outputs.extend(projected["units"])
                operations.extend(shard_operations)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
        merged = {"units": outputs}
        output_path = root / "gpt54-specialist-output-full.private.json"
        _write_immutable(output_path, merged)
        audit = {
            "schema_version": V60_AUDIT_VERSION,
            "created_at": now_iso(),
            "operation_count": len(operations),
            "operations": operations,
            "new_semantic_decisions_from_deterministic_code": False,
            "semantic_sources": ["gpt54_checklist", "frozen_llm_proposition_receipts"],
            "privacy": "opaque_ids_field_enums_and_span_hashes_only",
        }
        audit_path = root / "contract-projection-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v57(merged, frozen["truth"], frozen["taxonomy"])
        score["schema_version"] = V60_SCORE_VERSION
        score["contract_projection_audit"] = _record(audit_path)
        score_path = root / "gpt54-specialist-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V60_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v60_gpt54_specialist_passed_integrated_diagnostic_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v60_gpt54_specialist_passed_integrated_diagnostic_authorized"
                if passed
                else "v60_gpt54_specialist_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "specialist_diagnostic_passed": passed,
            "integrated_diagnostic_authorized": passed,
            "full_calibration_authorized": False,
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
    parser = argparse.ArgumentParser(description="Run v60 gpt-5.4 specialist diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v59-root", default=str(DEFAULT_V59_ROOT))
    parser.add_argument("--v58-root", default=str(DEFAULT_V58_ROOT))
    parser.add_argument("--v57-root", default=str(DEFAULT_V57_ROOT))
    parser.add_argument("--model", default="gpt-5.4")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v60(
            output_dir=Path(args.output_dir),
            v59_root=Path(args.v59_root),
            v58_root=Path(args.v58_root),
            v57_root=Path(args.v57_root),
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
                "specialist_diagnostic_passed": terminal.get("specialist_diagnostic_passed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
