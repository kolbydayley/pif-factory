from __future__ import annotations

"""One-turn side-free structured-field adjudication after the v51 failure."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import (
    CHECKLIST_FIELDS,
    STRUCTURED_FIELD_VERDICTS,
    validate_app_server_output_schema_subset,
)
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
from .app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_PROMPT_BYTES,
    MAX_SCHEMA_BYTES,
    MAX_TOKENS_PER_TURN,
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v50_reference_restore import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V50_ROOT,
)
from .app_server_judge_v5_calibration_v51_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V51_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    _canonical_json,
    _record,
    _sha256_file,
)
from .app_server_judge_v5_fixture import load_fixture_truth_audit
from .util import now_iso


V52_INPUT_VERSION = "pif_app_server_judge_v5_4_v52_structured_reference_input_v1"
V52_SPEC_VERSION = "pif_app_server_judge_v5_4_v52_structured_reference_spec_v1"
V52_TRUTH_VERSION = "pif_app_server_judge_v5_4_v52_adjudicated_reference_v1"
V52_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v52_reference_receipt_v1"
V52_FAILURE_VERSION = "pif_app_server_judge_v5_4_v52_reference_failure_v1"
V52_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v52_reference_terminal_v1"
V52_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V52_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V52_PHASE_ID = "judge_v5_4_v52_structured_reference_adjudication"
TURN_NAME = "structured_reference_adjudication"

DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V50_ROOT.parent / "judge-calibration-v5_4-v52-structured-reference-adjudication"
).resolve()


class JudgeV5CalibrationV52ReferenceError(RuntimeError):
    """The v52 reference adjudication cannot satisfy its frozen contract."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def build_disputed_structured_input(
    *,
    canonical_truth: Mapping[str, Any],
    pointwise_input: Mapping[str, Any],
    pointwise_output: Mapping[str, Any],
) -> dict[str, Any]:
    inputs = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in pointwise_input.get("units") or []
    }
    outputs = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in pointwise_output.get("units") or []
    }
    units = []
    for case_id, case in sorted((canonical_truth.get("cases") or {}).items()):
        for witness_id, expected_verdict in sorted(case["structured_fields"].items()):
            key = (str(case_id), str(witness_id))
            observed = outputs.get(key)
            if observed is None:
                continue
            expected_fields = sorted(case["field_issues"][witness_id])
            observed_fields = sorted(observed.get("field_issue_fields") or [])
            if (
                observed.get("structured_field_verdict") == expected_verdict
                and observed_fields == expected_fields
            ):
                continue
            source = inputs.get(key)
            if source is None:
                raise JudgeV5CalibrationV52ReferenceError("disputed input coverage drifted")
            units.append(
                {
                    "case_id": key[0],
                    "witness_id": key[1],
                    "source_excerpt": source["source_excerpt"],
                    "structured_event": deepcopy(source["structured_event"]),
                }
            )
    if len(units) != 16 or len({(row["case_id"], row["witness_id"]) for row in units}) != 16:
        raise JudgeV5CalibrationV52ReferenceError("v52 dispute set must contain 16 witnesses")
    return {
        "schema_version": V52_INPUT_VERSION,
        "units": units,
        "prior_labels_present": False,
        "expected_answers_present": False,
        "system_identity_present": False,
        "alignment_outputs_present": False,
        "selection_basis": "observable_v51_structured_decision_disagreement_only",
    }


def v52_base_instructions() -> str:
    return (
        "You are a side-free structured-event field reference adjudicator. Each unit contains "
        "only a source excerpt and one anonymous structured event. Proposition support and "
        "event alignment are outside this pass; judge every populated structured semantic "
        "field independently against the entire source excerpt. Empty or omitted fields are "
        "not errors. Resolve ordinary paraphrase and coreference, but do not invent dataset "
        "conventions. speaker is the entity or role that actually voices or authors the claim; "
        "actor is the entity performing the action or holding the stance. Merely appearing in "
        "the narrative does not license either role. attribution concerns the reporting or "
        "source relation itself and must not be added merely because actor or speaker is wrong. "
        "evidence concerns whether the populated evidence field is exact and supports the event; "
        "do not add it as a downstream duplicate. Report only minimal independent root fields. "
        "For an ungrounded claim, include unsupported_inference and only independently wrong "
        "populated material fields. Cite exact source substrings or [] when a conflict exists "
        "only in the event. Do not infer system origin, vote, use confidence, keyword rules, "
        "regex, token overlap, embeddings, or prior labels."
    )


def build_v52_prompt(value: Mapping[str, Any]) -> str:
    audit = load_fixture_truth_audit()
    packet = {
        "rubric": audit["mismatch_checklist"],
        "mismatch_precedence": audit["mismatch_precedence"],
        "units": value["units"],
    }
    return (
        "Return each opaque case_id/witness_id exactly once. correct requires "
        "field_issue_fields=[]; incorrect requires one or more minimal root fields. "
        "Every evidence span must be copied verbatim from source_excerpt; use [] instead of "
        "paraphrasing. Do not decide proposition support or alignment.\n\n"
        "# Side-free structured reference units\n"
        + _canonical_json(packet)
        + "\n"
    )


def v52_output_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    units = value.get("units") or []
    case_ids = sorted({str(row["case_id"]) for row in units})
    witness_ids = sorted(str(row["witness_id"]) for row in units)
    row = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "case_id",
            "witness_id",
            "structured_field_verdict",
            "field_issue_fields",
            "field_evidence_spans",
            "field_rationale",
        ],
        "properties": {
            "case_id": {"type": "string", "enum": case_ids},
            "witness_id": {"type": "string", "enum": witness_ids},
            "structured_field_verdict": {
                "type": "string",
                "enum": list(STRUCTURED_FIELD_VERDICTS),
            },
            "field_issue_fields": {
                "type": "array",
                "maxItems": len(CHECKLIST_FIELDS),
                "items": {"type": "string", "enum": list(CHECKLIST_FIELDS)},
            },
            "field_evidence_spans": {
                "type": "array",
                "maxItems": 4,
                "items": {"type": "string", "minLength": 1, "maxLength": 1000},
            },
            "field_rationale": {"type": "string", "minLength": 1, "maxLength": 600},
        },
    }
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["units"],
        "properties": {
            "units": {
                "type": "array",
                "minItems": len(units),
                "maxItems": len(units),
                "items": row,
            }
        },
    }
    if validate_app_server_output_schema_subset(schema):
        raise JudgeV5CalibrationV52ReferenceError("v52 output schema exceeds supported subset")
    return schema


def validate_v52_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping) or set(output) != {"units"} or not isinstance(output.get("units"), list):
        return ["invalid_v52_output_root"]
    expected = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in value.get("units") or []
    }
    seen = set()
    errors = []
    required = {
        "case_id",
        "witness_id",
        "structured_field_verdict",
        "field_issue_fields",
        "field_evidence_spans",
        "field_rationale",
    }
    for index, row in enumerate(output["units"]):
        prefix = f"unit_{index}"
        if not isinstance(row, Mapping) or set(row) != required:
            errors.append(prefix + "_shape")
            continue
        key = (str(row.get("case_id")), str(row.get("witness_id")))
        if key not in expected or key in seen:
            errors.append(prefix + "_identity")
            continue
        seen.add(key)
        verdict = row.get("structured_field_verdict")
        fields = row.get("field_issue_fields")
        spans = row.get("field_evidence_spans")
        source = expected[key]["source_excerpt"]
        if verdict not in STRUCTURED_FIELD_VERDICTS:
            errors.append(prefix + "_verdict")
        if (
            not isinstance(fields, list)
            or len(fields) != len(set(fields))
            or any(field not in CHECKLIST_FIELDS for field in fields)
            or (verdict == "correct" and fields)
            or (verdict == "incorrect" and not fields)
        ):
            errors.append(prefix + "_fields")
        if (
            not isinstance(spans, list)
            or len(spans) > 4
            or len(spans) != len(set(spans))
            or any(not isinstance(span, str) or not span or span not in source for span in spans)
        ):
            errors.append(prefix + "_evidence")
    if seen != set(expected):
        errors.append("v52_output_coverage")
    return errors


def apply_v52_reference(
    canonical_truth: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    patched = deepcopy(canonical_truth)
    for row in output["units"]:
        case = patched["cases"][str(row["case_id"])]
        witness_id = str(row["witness_id"])
        case["structured_fields"][witness_id] = row["structured_field_verdict"]
        case["field_issues"][witness_id] = sorted(
            row["field_issue_fields"], key=CHECKLIST_FIELDS.index
        )
    patched["reference_version"] = V52_TRUTH_VERSION
    patched["reference_frozen_at"] = now_iso()
    patched["structured_reference_owner"] = "fresh_side_free_gpt_5_6_sol_adjudication"
    patched["proposition_truth_source"] = "v50_canonical_unchanged"
    patched["alignment_truth_source"] = "v50_canonical_unchanged"
    return patched


def _validate_predecessors(v50_root: Path, v51_root: Path) -> dict[str, Any]:
    paths = {
        "v50_terminal": v50_root / "terminal.json",
        "v50_receipt": v50_root / "reference-restoration-receipt.json",
        "canonical_truth": v50_root / "canonical-calibration-truth.private.json",
        "v51_terminal": v51_root / "terminal.json",
        "v51_failure": v51_root / "failure.json",
        "v51_spec": v51_root / "fresh-diagnostic-spec.json",
        "v51_pointwise_input": v51_root / "pointwise-input-full.private.json",
        "v51_pointwise_output": v51_root / "pointwise-output-full.private.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    v50 = values["v50_terminal"]
    if (
        v50.get("reference_frozen") is not True
        or v50.get("production_mutated") is not False
        or not _record_matches(v50.get("reference_receipt"), paths["v50_receipt"])
        or not _record_matches(v50.get("canonical_truth"), paths["canonical_truth"])
    ):
        raise JudgeV5CalibrationV52ReferenceError("v50 predecessor drifted")
    v51 = values["v51_terminal"]
    if (
        v51.get("state") != "failed"
        or v51.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or v51.get("accounting_complete") is not True
        or v51.get("usage_status") != "complete"
        or v51.get("production_mutated") is not False
        or v51.get("semantic_retry_count") != 0
        or not _record_matches(v51.get("failure"), paths["v51_failure"])
        or values["v51_failure"].get("failed_turn_name")
        != "bipartite_alignment_base_shard_01"
    ):
        raise JudgeV5CalibrationV52ReferenceError("v51 predecessor is not admissible")
    return {name: _record(path) for name, path in paths.items()}


def _build_capacity_policy(root: Path, predecessors: Mapping[str, Any], v51_root: Path) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    v51 = _load_json(v51_root / "terminal.json", "v51 terminal")
    audit = {
        "schema_version": V52_CAPACITY_AUDIT_VERSION,
        "phase_id": V52_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessors": predecessors,
        "measured_basis": {
            "v51_completed_turn_count": len(v51.get("attempts") or []),
            "v51_total_tokens": (v51.get("usage") or {}).get("total_tokens"),
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v52 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV52ReferenceError("immutable v52 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V52_CAPACITY_POLICY_VERSION,
        "phase_id": V52_PHASE_ID,
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
        prior = _load_json(policy_path, "v52 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV52ReferenceError("immutable v52 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v52_reference(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v50_root: Path = DEFAULT_V50_ROOT,
    v51_root: Path = DEFAULT_V51_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessors = _validate_predecessors(v50_root.resolve(), v51_root.resolve())
    canonical_truth = _load_json(v50_root / "canonical-calibration-truth.private.json", "canonical truth")
    value = build_disputed_structured_input(
        canonical_truth=canonical_truth,
        pointwise_input=_load_json(v51_root / "pointwise-input-full.private.json", "v51 pointwise input"),
        pointwise_output=_load_json(v51_root / "pointwise-output-full.private.json", "v51 pointwise output"),
    )
    prompt = build_v52_prompt(value)
    schema = v52_output_schema(value)
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=value,
        prompt=prompt,
        schema=schema,
    )
    capacity = _build_capacity_policy(root, predecessors, v51_root)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V52_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_turn_side_free_structured_reference_adjudication",
        "case_count": len({row["case_id"] for row in value["units"]}),
        "witness_count": len(value["units"]),
        "turn_plan": [TURN_NAME],
        "retry_count_per_turn": 0,
        "prior_labels_in_model_input": False,
        "expected_answers_in_model_input": False,
        "canonical_proposition_truth_mutable": False,
        "canonical_alignment_truth_mutable": False,
        "fresh_diagnostic_required_after_reference_freeze": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessors": predecessors,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_request": {
            "input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
        },
        "request_byte_caps": {"prompt": MAX_PROMPT_BYTES, "output_schema": MAX_SCHEMA_BYTES},
        "privacy": "private_input_prompt_output_no_source_text_in_terminal",
    }
    spec_path = root / "structured-reference-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v52 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV52ReferenceError("immutable v52 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "canonical_truth": canonical_truth,
        "input": value,
        "prompt": prompt,
        "schema": schema,
        "paths": paths,
        "capacity_policy": capacity["policy"],
    }


async def run_v52_reference(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v50_root: Path = DEFAULT_V50_ROOT,
    v51_root: Path = DEFAULT_V51_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v52 terminal")
    frozen = freeze_v52_reference(
        output_dir=root,
        v50_root=v50_root,
        v51_root=v51_root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
    )
    factory = client_factory or _client_factory
    try:
        async with factory(frozen["capacity_policy"]) as client:
            output, sidecar, adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=v52_base_instructions(),
                model=model,
                effort=reasoning_effort,
                timeout_seconds=timeout_seconds,
                batch_size=len(frozen["input"]["units"]),
                policy_path=frozen["capacity_policy"],
                output_validator=lambda value: validate_v52_output(value, frozen["input"]),
            )
        truth = apply_v52_reference(frozen["canonical_truth"], output)
        truth_path = root / "adjudicated-calibration-truth.private.json"
        _write_immutable(truth_path, truth)
        receipt = {
            "schema_version": V52_RECEIPT_VERSION,
            "created_at": now_iso(),
            "reference_version": V52_TRUTH_VERSION,
            "reference_frozen": True,
            "structured_witness_count": len(output["units"]),
            "canonical_proposition_truth_unchanged": True,
            "canonical_alignment_truth_unchanged": True,
            "adjudicated_truth": _record(truth_path),
            "semantic_output": _record(frozen["paths"]["output"]),
            "sidecar": _record(frozen["paths"]["sidecar"]),
            "fresh_diagnostic_required": True,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
        }
        receipt_path = root / "reference-receipt.json"
        _write_immutable(receipt_path, receipt)
        accounting = _aggregate_usage([sidecar])
        terminal = {
            "schema_version": V52_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": "v52_structured_reference_frozen_fresh_diagnostic_required",
            "overall_evaluation_complete": False,
            "reference_frozen": True,
            "fresh_diagnostic_required": True,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "checkpoint_adopted": adopted,
            "reference_receipt": _record(receipt_path),
            "adjudicated_truth": _record(truth_path),
            "attempts": _record(frozen["paths"]["sidecar"]),
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        error_class = exc.error_class
    except Exception as exc:
        error_class = type(exc).__name__
    sidecar_path = frozen["paths"]["sidecar"]
    usage = None
    usage_status = "unknown"
    accounting_complete = False
    if sidecar_path.exists():
        sidecar = _load_json(sidecar_path, "v52 failed sidecar")
        if sidecar.get("usage_complete") is True and sidecar.get("usage_status") == "measured":
            usage = sidecar.get("usage")
            usage_status = "complete"
            accounting_complete = isinstance(usage, Mapping)
    failure = {
        "schema_version": V52_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": accounting_complete,
        "usage_status": usage_status,
        "usage": usage,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V52_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_frozen": False,
        "fresh_diagnostic_required": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": accounting_complete,
        "usage_status": usage_status,
        "usage": usage,
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v52 structured reference adjudication")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v50-root", default=str(DEFAULT_V50_ROOT))
    parser.add_argument("--v51-root", default=str(DEFAULT_V51_ROOT))
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v52_reference(
            output_dir=Path(args.output_dir),
            v50_root=Path(args.v50_root),
            v51_root=Path(args.v51_root),
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
                "reference_frozen": terminal["reference_frozen"],
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
