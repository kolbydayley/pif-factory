from __future__ import annotations

"""Run a fresh judge-v5 calibration against the frozen fixture reference."""

import argparse
import asyncio
import json
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_runner as calibration_runner
from .app_server_judge_v5 import CHECKLIST_FIELDS, PROTOCOL_VERSION
from .app_server_judge_v5_calibration import (
    CALIBRATION_GATES,
    CALIBRATION_TRUTH_VERSION,
    make_v5_calibration_pool,
    validate_v5_calibration_truth,
)
from .app_server_judge_v5_diagnostic import (
    _record,
    _sha256_file,
    _validate_capacity_checkpoint,
    _validate_usage,
    _write_immutable_json,
)
from .app_server_judge_v5_reference_adjudication import (
    REFERENCE_ADJUDICATION_SPEC_VERSION,
    REFERENCE_ADJUDICATION_TERMINAL_VERSION,
    REFERENCE_RECEIPT_VERSION,
    REFERENCE_VERSION,
)
from .util import now_iso


FRESH_CALIBRATION_SPEC_VERSION = (
    "pif_app_server_judge_v5_reference_v2_calibration_spec_v1"
)
FRESH_CALIBRATION_TERMINAL_VERSION = (
    "pif_app_server_judge_v5_reference_v2_calibration_terminal_v1"
)
DEFAULT_PIPELINE_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
DEFAULT_REFERENCE_ROOT = (
    DEFAULT_PIPELINE_ROOT / "fixture-reference-adjudication-luna-v4"
)
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-reference-v2-v1"
)
ALLOWED_RELATIONS = {"equivalent", "partial", "non_equivalent"}


class FreshCalibrationError(RuntimeError):
    """The frozen reference or fresh calibration contract is unsafe."""


def _load_json(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FreshCalibrationError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise FreshCalibrationError(f"{purpose} is not an object")
    return value


def _verify_record(record: Any, *, purpose: str) -> Path:
    if not isinstance(record, Mapping):
        raise FreshCalibrationError(f"{purpose} record is missing")
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise FreshCalibrationError(f"{purpose} record drifted")
    return path


def _validate_attempts(root: Path, terminal: Mapping[str, Any]) -> dict[str, int]:
    attempts = terminal.get("attempts")
    if not isinstance(attempts, list) or len(attempts) != 12:
        raise FreshCalibrationError("fixture-reference attempt coverage drifted")
    expected_names = {
        *(f"reference_pointwise_shard_{index:02d}" for index in range(9)),
        *(f"reference_alignment_shard_{index:02d}" for index in range(3)),
    }
    names = []
    usage = {
        "input_tokens": 0,
        "cached_input_tokens": 0,
        "output_tokens": 0,
        "reasoning_output_tokens": 0,
        "total_tokens": 0,
    }
    for attempt in attempts:
        if (
            not isinstance(attempt, Mapping)
            or attempt.get("state") != "completed"
            or attempt.get("status") != "completed"
            or attempt.get("usage_status") != "measured"
            or attempt.get("error_class") is not None
        ):
            raise FreshCalibrationError("fixture-reference attempt is incomplete")
        name = str(attempt.get("turn_name") or "")
        names.append(name)
        turn_root = root / "turns" / name.replace("_", "-")
        capacity_path = _verify_record(
            attempt.get("capacity"), purpose="attempt capacity"
        )
        output_path = _verify_record(attempt.get("output"), purpose="attempt output")
        sidecar_path = _verify_record(
            attempt.get("sidecar"), purpose="attempt sidecar"
        )
        if (
            capacity_path != turn_root / "capacity.json"
            or output_path != turn_root / "output.private.json"
            or sidecar_path != turn_root / "sidecar.json"
        ):
            raise FreshCalibrationError("fixture-reference attempt path drifted")
        capacity = _validate_capacity_checkpoint(capacity_path)
        if (
            capacity.get("thread_started") is not False
            or capacity.get("turn_started") is not False
            or capacity.get("sidecar_started") is not False
            or capacity.get("retry_checkpoint_reuse_allowed") is not False
        ):
            raise FreshCalibrationError("fixture-reference capacity boundary drifted")
        sidecar = _load_json(sidecar_path, "fixture-reference sidecar")
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("usage_status") != "measured"
            or sidecar.get("auth_type") != "chatgpt"
            or sidecar.get("transport") != "stdio"
            or sidecar.get("error_class") is not None
            or sidecar.get("recovery_reran_model") is not False
        ):
            raise FreshCalibrationError("fixture-reference sidecar boundary drifted")
        measured = _validate_usage(sidecar)
        for field, value in measured.items():
            usage[field] += value
    if set(names) != expected_names or len(names) != len(set(names)):
        raise FreshCalibrationError("fixture-reference attempt identity drifted")
    return usage


def _validate_unresolved(value: Mapping[str, Any]) -> None:
    required = {
        "pointwise_abstain_witness_ids",
        "alignment_abstain_case_ids",
        "unsupported_without_specific_root_errors",
    }
    if not required <= set(value):
        raise FreshCalibrationError("fixture-reference unresolved schema drifted")
    if any(not isinstance(value[key], list) or value[key] for key in required):
        raise FreshCalibrationError("fixture-reference has unresolved decisions")


def _reference_as_calibration_truth(
    reference: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    pool, mapping, provisional = make_v5_calibration_pool()
    cases = reference.get("cases")
    if (
        reference.get("schema_version") != REFERENCE_VERSION
        or reference.get("case_count") != 66
        or reference.get("witness_count") != 182
        or reference.get("proposition_and_structured_field_truth_separate") is not True
        or reference.get("legacy_joint_support_labels_used") is not False
        or not isinstance(cases, Mapping)
        or set(cases) != set(provisional["cases"])
        or reference.get("canary_case_ids") != provisional["canary_case_ids"]
    ):
        raise FreshCalibrationError("fixture-reference coverage drifted")
    for case_id, truth in cases.items():
        baseline = provisional["cases"][case_id]
        if (
            not isinstance(truth, Mapping)
            or truth.get("base_case_id") != baseline["base_case_id"]
            or truth.get("shape") != baseline["shape"]
            or set(truth.get("proposition") or {}) != set(baseline["proposition"])
            or set(truth.get("structured_fields") or {})
            != set(baseline["structured_fields"])
            or set(truth.get("field_issues") or {}) != set(baseline["field_issues"])
        ):
            raise FreshCalibrationError("fixture-reference witness identity drifted")
        for fields in truth["field_issues"].values():
            if (
                not isinstance(fields, list)
                or len(fields) != len(set(fields))
                or any(field not in CHECKLIST_FIELDS for field in fields)
            ):
                raise FreshCalibrationError("fixture-reference field issue drifted")
        for pair in truth.get("pairs") or []:
            if (
                pair.get("relation") not in ALLOWED_RELATIONS
                or not isinstance(pair.get("mismatch_fields"), list)
                or any(
                    field not in CHECKLIST_FIELDS
                    for field in pair.get("mismatch_fields") or []
                )
            ):
                raise FreshCalibrationError("fixture-reference alignment truth drifted")
    expected = deepcopy(dict(reference))
    expected["schema_version"] = CALIBRATION_TRUTH_VERSION
    expected["fixture_reference_schema_version"] = REFERENCE_VERSION
    expected["fixture_truth_audit_version"] = provisional.get(
        "fixture_truth_audit_version"
    )
    validate_v5_calibration_truth(pool=pool, mapping=mapping, expected=expected)
    return pool, mapping, expected


def load_frozen_fixture_reference(
    reference_root: Path = DEFAULT_REFERENCE_ROOT,
) -> dict[str, Any]:
    root = reference_root.expanduser().resolve()
    terminal_path = root / "terminal.json"
    terminal = _load_json(terminal_path, "fixture-reference terminal")
    if (
        terminal.get("schema_version") != REFERENCE_ADJUDICATION_TERMINAL_VERSION
        or terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "fixture_reference_v2_frozen_fresh_calibration_authorized"
        or terminal.get("reference_frozen") is not True
        or terminal.get("fresh_calibration_authorized") is not True
        or terminal.get("selection_authorized") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("turn_count") != 12
        or terminal.get("production_mutated") is not False
    ):
        raise FreshCalibrationError("fixture-reference terminal is not admissible")
    measured_usage = _validate_attempts(root, terminal)
    receipt_path = _verify_record(terminal.get("receipt"), purpose="reference receipt")
    receipt = _load_json(receipt_path, "fixture-reference receipt")
    if (
        receipt.get("schema_version") != REFERENCE_RECEIPT_VERSION
        or receipt.get("status") != "fixture_reference_v2_frozen"
        or receipt.get("reference_frozen") is not True
        or receipt.get("fresh_calibration_authorized") is not True
        or receipt.get("selection_authorized") is not False
        or receipt.get("semantic_turn_count") != 12
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("usage_status") != "complete"
        or receipt.get("candidate_labels_exposed_to_adjudicator") is not False
        or receipt.get("model_identities_exposed_to_adjudicator") is not False
        or receipt.get("majority_voting_used") is not False
        or receipt.get("production_mutation_performed") is not False
        or receipt.get("usage") != measured_usage
        or terminal.get("usage") != measured_usage
    ):
        raise FreshCalibrationError("fixture-reference receipt is not admissible")
    reference_path = _verify_record(receipt.get("reference"), purpose="fixture reference")
    unresolved_path = _verify_record(receipt.get("unresolved"), purpose="unresolved")
    manifest_path = _verify_record(
        receipt.get("disagreement_manifest"), purpose="disagreement manifest"
    )
    change_path = _verify_record(receipt.get("change_summary"), purpose="change summary")
    if terminal.get("unresolved") != receipt.get("unresolved"):
        raise FreshCalibrationError("terminal and receipt unresolved records disagree")
    _validate_unresolved(_load_json(unresolved_path, "reference unresolved"))
    manifest = _load_json(manifest_path, "reference disagreement manifest")
    if (
        manifest.get("pointwise_disputed_witness_count") != 102
        or manifest.get("pointwise_disputed_case_count") != 49
        or manifest.get("alignment_disputed_case_count") != 18
        or manifest.get("candidate_labels_exposed_to_adjudicator") is not False
        or manifest.get("model_identities_exposed_to_adjudicator") is not False
        or manifest.get("majority_voting_allowed") is not False
    ):
        raise FreshCalibrationError("fixture-reference disagreement manifest drifted")
    reference = _load_json(reference_path, "fixture reference")
    pool, mapping, expected = _reference_as_calibration_truth(reference)
    spec = _load_json(root / "reference-adjudication-spec.json", "reference spec")
    if (
        spec.get("schema_version") != REFERENCE_ADJUDICATION_SPEC_VERSION
        or spec.get("total_turn_count") != 12
        or spec.get("retry_count_per_turn") != 0
        or spec.get("managed_chatgpt_auth_only") is not True
        or spec.get("semantic_turn_maximum_primary_used_percent") != 20
        or spec.get("reference_freeze_requires_zero_abstentions") is not True
        or spec.get("selection_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise FreshCalibrationError("fixture-reference spec drifted")
    return {
        "root": root,
        "pool": pool,
        "mapping": mapping,
        "expected": expected,
        "terminal": terminal,
        "receipt": receipt,
        "records": {
            "terminal": _record(terminal_path),
            "receipt": _record(receipt_path),
            "reference": _record(reference_path),
            "unresolved": _record(unresolved_path),
            "disagreement_manifest": _record(manifest_path),
            "change_summary": _record(change_path),
            "reference_spec": _record(root / "reference-adjudication-spec.json"),
        },
    }


@contextmanager
def _reference_pool_factory(reference: Mapping[str, Any]) -> Iterator[None]:
    original = calibration_runner.make_v5_calibration_pool

    def factory() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        return (
            deepcopy(reference["pool"]),
            deepcopy(reference["mapping"]),
            deepcopy(reference["expected"]),
        )

    calibration_runner.make_v5_calibration_pool = factory
    try:
        yield
    finally:
        calibration_runner.make_v5_calibration_pool = original


async def run_fresh_reference_calibration(
    *,
    reference_root: Path = DEFAULT_REFERENCE_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[], Any] = calibration_runner.CapacityGatedCodexAppServerClient,
) -> dict[str, Any]:
    if model != "gpt-5.6-sol" or reasoning_effort != "high":
        raise FreshCalibrationError("fresh calibration model and effort are frozen")
    if timeout_seconds != 1200.0:
        raise FreshCalibrationError("fresh calibration timeout is frozen")
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "fresh calibration terminal")
    reference = load_frozen_fixture_reference(reference_root)
    root.mkdir(parents=True, exist_ok=True)
    inner_root = root / "fresh-attempt"
    spec = {
        "schema_version": FRESH_CALIBRATION_SPEC_VERSION,
        "state": "frozen_before_fresh_calibration_calls",
        "created_at": now_iso(),
        "protocol_version": PROTOCOL_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "case_count": 66,
        "witness_count": 182,
        "cases_per_shard": 6,
        "minimum_turn_count": 23,
        "maximum_turn_count": 24,
        "retry_count_per_turn": 0,
        "all_semantic_turns_fresh": True,
        "prior_calibration_output_reuse_allowed": False,
        "reference_truth_exposed_to_model": False,
        "reference_binding_verified_before_calls": True,
        "calibration_gates": CALIBRATION_GATES,
        "reference_records": reference["records"],
        "scorer_truth_sha256": _sha256_json(reference["expected"]),
        "adapter_source": _record(Path(__file__).resolve()),
        "inner_output_root": str(inner_root),
        "production_mutation_allowed": False,
        "semantic_model_calls_performed_during_freeze": 0,
    }
    spec_path = root / "fresh-calibration-spec.json"
    _write_immutable_json(spec_path, spec)
    try:
        with _reference_pool_factory(reference):
            inner = await calibration_runner.run_v5_full_calibration(
                output_dir=inner_root,
                execution_purpose="calibration",
                model=model,
                reasoning_effort=reasoning_effort,
                timeout_seconds=timeout_seconds,
                client_factory=client_factory,
            )
        completed = inner.get("state") == "completed"
        attempts = inner.get("attempts")
        turn_count = inner.get("turn_count")
        accounting_valid = (
            inner.get("usage_status") == "complete"
            and inner.get("accounting_complete") is True
            and inner.get("semantic_retry_count") == 0
            and turn_count in {23, 24}
            and isinstance(attempts, list)
            and len(attempts) == turn_count
            and all(
                isinstance(attempt, Mapping)
                and attempt.get("state") == "completed"
                and attempt.get("status") == "completed"
                and attempt.get("usage_status") == "measured"
                and attempt.get("error_class") is None
                for attempt in attempts
            )
        )
        inner_truth_path = inner_root / "calibration-truth.private.json"
        inner_truth = _load_json(inner_truth_path, "inner calibration truth")
        truth_binding_valid = inner_truth == reference["expected"]
        passed = (
            completed
            and accounting_valid
            and truth_binding_valid
            and inner.get("calibration_passed") is True
        )
        selection_authorized = passed and inner.get("selection_authorized") is True
        terminal_state = "completed" if completed and accounting_valid and truth_binding_valid else "failed"
        terminal = {
            "schema_version": FRESH_CALIBRATION_TERMINAL_VERSION,
            "state": terminal_state,
            "terminal_at": now_iso(),
            "terminal_reason": (
                "fresh_reference_calibration_passed_selection_authorized"
                if selection_authorized
                else "fresh_reference_calibration_quality_gate_not_passed"
                if terminal_state == "completed"
                else "infrastructure_or_judge_attempt_failed"
            ),
            "spec_sha256": _sha256_file(spec_path),
            "reference_binding_verified": True,
            "scorer_truth_binding_verified": truth_binding_valid,
            "accounting_contract_verified": accounting_valid,
            "calibration_passed": passed,
            "selection_authorized": selection_authorized,
            "inner_terminal": _record(inner_root / "terminal.json"),
            "inner_truth": _record(inner_truth_path),
            "inner_score": inner.get("score"),
            "attempts": attempts if isinstance(attempts, list) else [],
            "semantic_retry_count": inner.get("semantic_retry_count", 0),
            "production_mutated": False,
            "usage": inner.get("usage"),
            "usage_status": inner.get("usage_status"),
            "accounting_complete": inner.get("accounting_complete", False),
            "turn_count": turn_count,
            "wall_elapsed_seconds_sum": inner.get("wall_elapsed_seconds_sum"),
        }
    except Exception as exc:
        terminal = {
            "schema_version": FRESH_CALIBRATION_TERMINAL_VERSION,
            "state": "failed",
            "terminal_at": now_iso(),
            "terminal_reason": "infrastructure_or_judge_attempt_failed",
            "error_class": type(exc).__name__,
            "spec_sha256": _sha256_file(spec_path),
            "reference_binding_verified": True,
            "calibration_passed": False,
            "selection_authorized": False,
            "semantic_retry_count": 0,
            "production_mutated": False,
            "usage": None,
            "usage_status": "unknown",
            "accounting_complete": False,
        }
    _write_immutable_json(terminal_path, terminal)
    return terminal


def _sha256_json(value: Any) -> str:
    import hashlib

    encoded = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run fresh judge-v5.4 calibration against fixture reference v2"
    )
    parser.add_argument("--reference-root", default=str(DEFAULT_REFERENCE_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_fresh_reference_calibration(
            reference_root=Path(args.reference_root),
            output_dir=Path(args.output_dir),
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
                "calibration_passed": terminal.get("calibration_passed", False),
                "selection_authorized": terminal.get("selection_authorized", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal.get("state") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
