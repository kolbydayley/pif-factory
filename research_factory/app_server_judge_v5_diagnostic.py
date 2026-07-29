from __future__ import annotations

"""Fail-closed runner for the pipeline-v5 judge diagnostic."""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_capacity import (
    CAPACITY_CHECKPOINT_VERSION,
    DEFAULT_MAXIMUM_PRIMARY_USED_PERCENT,
    CapacityGatedCodexAppServerClient,
)
from .app_server_judge_v5 import (
    DIAGNOSTIC_GATES,
    PROTOCOL_VERSION,
    adjudication_alignment_input,
    build_disagreement_adjudication_input,
    build_disagreement_adjudication_prompt,
    build_neutral_alignment_input,
    build_neutral_alignment_prompt,
    build_pointwise_support_input,
    build_pointwise_support_prompt,
    find_observable_alignment_disagreements,
    freeze_support_receipts,
    load_v5_diagnostic_fixture,
    make_v5_diagnostic_pool,
    neutral_alignment_base_instructions,
    neutral_alignment_output_schema,
    pointwise_support_base_instructions,
    pointwise_support_output_schema,
    reconcile_neutral_alignment,
    score_v5_diagnostic,
    validate_neutral_alignment_output,
    validate_pointwise_support_output,
)
from .app_server_v5_diagnostic_v6_reuse import verify_diagnostic_v6_reuse_contract
from .codex_app_server import APP_SERVER_CLIENT_VERSION
from .util import now_iso, sha256_text, write_text_atomic


DIAGNOSTIC_RUN_VERSION = "pif_app_server_judge_v5_diagnostic_run_v6"
DIAGNOSTIC_TERMINAL_VERSION = "pif_app_server_judge_v5_diagnostic_terminal_v6"
DIAGNOSTIC_FAILURE_VERSION = "pif_app_server_judge_v5_diagnostic_failure_v6"
DEFAULT_PIPELINE_V5_ROOT = (
    Path("work/app-server-development-v2/unattended-pipeline-v5").resolve()
)
DEFAULT_REUSE_CONTRACT_PATH = DEFAULT_PIPELINE_V5_ROOT / "reuse-contract-v9.json"
DEFAULT_FIXTURE_AUDIT_RECEIPT_PATH = (
    DEFAULT_PIPELINE_V5_ROOT / "diagnostic-v5-quality-audit-receipt-v1.json"
)
DEFAULT_DIAGNOSTIC_ROOT = DEFAULT_PIPELINE_V5_ROOT / "judge-diagnostic-v6"
MAX_PROMPT_BYTES = 64 * 1024
MAX_OUTPUT_SCHEMA_BYTES = 64 * 1024
USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)


class JudgeV5DiagnosticError(RuntimeError):
    """The diagnostic cannot continue without violating its frozen contract."""


class JudgeV5DiagnosticAttemptFailed(JudgeV5DiagnosticError):
    def __init__(self, *, turn_name: str, error_class: str) -> None:
        self.turn_name = turn_name
        self.error_class = error_class
        super().__init__(f"{turn_name} failed: {error_class}")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, *, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JudgeV5DiagnosticError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise JudgeV5DiagnosticError(f"{purpose} is not an object")
    return value


def _write_immutable_text(path: Path, value: str) -> None:
    target = path.expanduser().resolve()
    if target.exists():
        try:
            current = target.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise JudgeV5DiagnosticError("immutable text artifact is unreadable") from exc
        if current != value:
            raise JudgeV5DiagnosticError(
                f"immutable artifact content drifted: {target}"
            )
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(target, value)


def _write_immutable_json(path: Path, value: Any) -> None:
    _write_immutable_text(
        path,
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise JudgeV5DiagnosticError(f"required artifact is missing: {resolved}")
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _verify_record(record: Mapping[str, Any]) -> None:
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise JudgeV5DiagnosticError("immutable diagnostic artifact drifted")


def _verify_nested_records(value: Any) -> None:
    if isinstance(value, Mapping):
        if set(value) >= {"path", "sha256", "size_bytes"}:
            _verify_record(value)
            return
        for child in value.values():
            _verify_nested_records(child)
    elif isinstance(value, list):
        for child in value:
            _verify_nested_records(child)


def _enforce_request_caps(*, prompt: str, schema: Mapping[str, Any]) -> None:
    prompt_bytes = len(prompt.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if prompt_bytes > MAX_PROMPT_BYTES:
        raise JudgeV5DiagnosticError("diagnostic prompt exceeds frozen byte cap")
    if schema_bytes > MAX_OUTPUT_SCHEMA_BYTES:
        raise JudgeV5DiagnosticError("diagnostic output schema exceeds frozen byte cap")


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "output": turn_root / "output.private.json",
        "sidecar": turn_root / "sidecar.json",
        "capacity": turn_root / "capacity.json",
    }


def _freeze_turn_request(
    *,
    root: Path,
    turn_name: str,
    input_value: Mapping[str, Any],
    prompt: str,
    schema: Mapping[str, Any],
) -> dict[str, Path]:
    _enforce_request_caps(prompt=prompt, schema=schema)
    paths = _turn_paths(root, turn_name)
    _write_immutable_json(paths["input"], input_value)
    _write_immutable_text(paths["prompt"], prompt)
    _write_immutable_json(paths["schema"], schema)
    return paths


def freeze_v5_diagnostic_protocol(
    *,
    output_dir: Path,
    model: str,
    reasoning_effort: str,
    timeout_seconds: float,
    fixture_path: Optional[Path] = None,
    reuse_contract_path: Path = DEFAULT_REUSE_CONTRACT_PATH,
    fixture_audit_receipt_path: Path = DEFAULT_FIXTURE_AUDIT_RECEIPT_PATH,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    reuse_path = reuse_contract_path.expanduser().resolve()
    audit_receipt = fixture_audit_receipt_path.expanduser().resolve()
    verify_diagnostic_v6_reuse_contract(reuse_path)
    if not audit_receipt.is_file():
        raise JudgeV5DiagnosticError("fixture truth audit receipt is missing")

    selected_fixture = (
        fixture_path.expanduser().resolve()
        if fixture_path is not None
        else None
    )
    fixture = (
        load_v5_diagnostic_fixture(selected_fixture)
        if selected_fixture is not None
        else load_v5_diagnostic_fixture()
    )
    pool, mapping, expected = (
        make_v5_diagnostic_pool(fixture_path=selected_fixture)
        if selected_fixture is not None
        else make_v5_diagnostic_pool()
    )
    fixture_source = Path(str(expected["fixture_path"])).resolve()
    pool_path = root / "shared-witness-pool.private.json"
    mapping_path = root / "witness-mapping.private.json"
    expected_path = root / "diagnostic-truth.private.json"
    _write_immutable_json(pool_path, pool)
    _write_immutable_json(mapping_path, mapping)
    _write_immutable_json(expected_path, expected)

    pointwise_input = build_pointwise_support_input(pool)
    pointwise_prompt = build_pointwise_support_prompt(pointwise_input)
    pointwise_schema = pointwise_support_output_schema(pointwise_input)
    pointwise_paths = _freeze_turn_request(
        root=root,
        turn_name="pointwise_support",
        input_value=pointwise_input,
        prompt=pointwise_prompt,
        schema=pointwise_schema,
    )
    protocol_sources = [
        Path(__file__).resolve(),
        Path(__file__).resolve().with_name("app_server_judge_v5.py"),
        Path(__file__).resolve().with_name("app_server_judge_v5_fixture.py"),
        fixture_source,
    ]
    fixture_base_source = Path(str(fixture.get("base_fixture_path") or "")).resolve()
    if not fixture_base_source.is_file():
        raise JudgeV5DiagnosticError("versioned diagnostic base fixture is missing")
    protocol_sources.append(fixture_base_source)
    spec = {
        "schema_version": DIAGNOSTIC_RUN_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "protocol_version": PROTOCOL_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "app_server_client_version": APP_SERVER_CLIENT_VERSION,
        "capacity_gate": {
            "before_every_semantic_turn": True,
            "maximum_primary_used_percent": DEFAULT_MAXIMUM_PRIMARY_USED_PERCENT,
            "reached_type_must_be_null": True,
        },
        "turn_plan": [
            "pointwise_support",
            "neutral_alignment_base",
            "neutral_alignment_canary",
            "disagreement_adjudication_if_observable_disagreement_only",
        ],
        "required_turn_count_without_adjudication": 3,
        "maximum_turn_count": 4,
        "retry_count_per_turn": 0,
        "completed_checkpoint_adoption_is_not_retry": True,
        "partial_failed_or_unknown_usage_checkpoint_blocks_version": True,
        "support_permutations": 1,
        "support_is_side_free": True,
        "alignment_is_origin_neutral": True,
        "permutation_canary_case_count": len(expected["canary_case_ids"]),
        "full_side_labelled_ab_ba_support_allowed": False,
        "majority_voting_allowed": False,
        "verbal_confidence_routing_allowed": False,
        "adjudication_call_cap": 1,
        "legacy_joint_support_truth_allowed": False,
        "proposition_support_and_structured_field_truth_separate": True,
        "empty_event_fields_omitted_only": True,
        "diagnostic_gates": DIAGNOSTIC_GATES,
        "full_calibration_authorized_before_diagnostic_pass": False,
        "request_byte_caps": {
            "prompt": MAX_PROMPT_BYTES,
            "output_schema": MAX_OUTPUT_SCHEMA_BYTES,
        },
        "case_count": len(fixture["cases"]),
        "witness_count": len(pointwise_input["units"]),
        "fixture_source": _record(fixture_source),
        "fixture_base_source": _record(fixture_base_source),
        "fixture_truth_audit_receipt": _record(audit_receipt),
        "predecessor_reuse_contract": _record(reuse_path),
        "protocol_sources": [_record(path) for path in protocol_sources],
        "frozen_inputs": {
            "pool": _record(pool_path),
            "mapping": _record(mapping_path),
            "truth": _record(expected_path),
            "pointwise_input": _record(pointwise_paths["input"]),
            "pointwise_prompt": _record(pointwise_paths["prompt"]),
            "pointwise_schema": _record(pointwise_paths["schema"]),
        },
        "privacy": "private_prompts_inputs_outputs_mapping_and_truth_sanitized_terminal_only",
        "semantic_model_calls_performed_during_freeze": 0,
    }
    spec_path = root / "diagnostic-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, purpose="frozen v5 diagnostic specification")
        stable_fields = dict(spec)
        stable_fields["created_at"] = prior.get("created_at")
        if prior != stable_fields:
            raise JudgeV5DiagnosticError("frozen diagnostic specification drifted")
        spec = prior
    else:
        _write_immutable_json(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "pool": pool,
        "mapping": mapping,
        "expected": expected,
        "pointwise_input": pointwise_input,
        "pointwise_prompt": pointwise_prompt,
        "pointwise_schema": pointwise_schema,
        "pointwise_paths": pointwise_paths,
    }


def _validate_capacity_checkpoint(path: Path) -> dict[str, Any]:
    value = _load_json(path, purpose="capacity checkpoint")
    used = value.get("primary_used_percent")
    if (
        value.get("schema_version") != CAPACITY_CHECKPOINT_VERSION
        or value.get("cleared_for_semantic_turn") is not True
        or value.get("managed_chatgpt_auth_verified") is not True
        or value.get("rate_limit_reached_type") is not None
        or isinstance(used, bool)
        or not isinstance(used, int)
        or used > DEFAULT_MAXIMUM_PRIMARY_USED_PERCENT
        or value.get("maximum_primary_used_percent")
        != DEFAULT_MAXIMUM_PRIMARY_USED_PERCENT
    ):
        raise JudgeV5DiagnosticError("semantic turn capacity checkpoint is invalid")
    return value


def _validate_usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    usage = sidecar.get("usage")
    if sidecar.get("usage_complete") is not True or not isinstance(usage, dict):
        raise JudgeV5DiagnosticError("semantic turn usage is incomplete or unknown")
    result = {}
    for field in USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise JudgeV5DiagnosticError("semantic turn usage is malformed")
        result[field] = value
    if (
        result["cached_input_tokens"] > result["input_tokens"]
        or result["reasoning_output_tokens"] > result["output_tokens"]
        or result["total_tokens"]
        != result["input_tokens"] + result["output_tokens"]
    ):
        raise JudgeV5DiagnosticError("semantic turn usage invariants failed")
    return result


def _validate_completed_turn(
    *,
    paths: Mapping[str, Path],
    prompt: str,
    schema: Mapping[str, Any],
    base_instructions: str,
    model: str,
    reasoning_effort: str,
    output_validator: Callable[[Any], Sequence[str]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    output = _load_json(paths["output"], purpose="semantic turn output")
    sidecar = _load_json(paths["sidecar"], purpose="semantic turn sidecar")
    _validate_capacity_checkpoint(paths["capacity"])
    required = {
        "state": "completed",
        "status": "completed",
        "client_version": APP_SERVER_CLIENT_VERSION,
        "transport": "stdio",
        "auth_type": "chatgpt",
        "thread_mode": "new_thread",
        "model": model,
        "effort": reasoning_effort,
        "prompt_sha256": sha256_text(prompt),
        "base_instructions_sha256": sha256_text(base_instructions),
        "output_schema_sha256": sha256_text(_canonical_json(schema)),
    }
    for key, expected in required.items():
        if sidecar.get(key) != expected:
            raise JudgeV5DiagnosticError(
                f"semantic turn sidecar mismatch: {key}"
            )
    _validate_usage(sidecar)
    output_text = paths["output"].read_text(encoding="utf-8")
    acceptable = {sha256_text(output_text)}
    if output_text.endswith("\n"):
        acceptable.add(sha256_text(output_text[:-1]))
    if sidecar.get("output_sha256") not in acceptable:
        raise JudgeV5DiagnosticError("semantic turn output hash mismatch")
    errors = list(output_validator(output))
    if errors:
        raise JudgeV5DiagnosticError(
            "semantic turn output failed validation: %s" % "; ".join(errors)
        )
    return output, sidecar


def _checkpoint_state(paths: Mapping[str, Path]) -> str:
    present = {
        key: paths[key].exists() for key in ("output", "sidecar", "capacity")
    }
    if not any(present.values()):
        return "absent"
    if all(present.values()):
        sidecar = _load_json(paths["sidecar"], purpose="existing turn sidecar")
        return "completed" if sidecar.get("state") == "completed" else "terminal_noncomplete"
    return "partial"


async def _get_or_run_turn(
    *,
    client: Any,
    turn_name: str,
    paths: Mapping[str, Path],
    prompt: str,
    schema: Mapping[str, Any],
    base_instructions: str,
    model: str,
    reasoning_effort: str,
    timeout_seconds: float,
    batch_size: int,
    output_validator: Callable[[Any], Sequence[str]],
) -> tuple[dict[str, Any], dict[str, Any], bool]:
    checkpoint_state = _checkpoint_state(paths)
    if checkpoint_state == "completed":
        try:
            output, sidecar = _validate_completed_turn(
                paths=paths,
                prompt=prompt,
                schema=schema,
                base_instructions=base_instructions,
                model=model,
                reasoning_effort=reasoning_effort,
                output_validator=output_validator,
            )
        except Exception as exc:
            raise JudgeV5DiagnosticAttemptFailed(
                turn_name=turn_name,
                error_class=type(exc).__name__,
            ) from exc
        return output, sidecar, True
    if checkpoint_state != "absent":
        raise JudgeV5DiagnosticAttemptFailed(
            turn_name=turn_name,
            error_class=f"immutable_{checkpoint_state}_checkpoint",
        )
    try:
        result = await client.run_ephemeral_structured_turn(
            model=model,
            effort=reasoning_effort,
            base_instructions=base_instructions,
            prompt=prompt,
            output_schema=dict(schema),
            cwd=Path.cwd(),
            sidecar_path=paths["sidecar"],
            capacity_checkpoint_path=paths["capacity"],
            output_path=paths["output"],
            batch_size=batch_size,
            thread_mode="new_thread",
            timeout_seconds=timeout_seconds,
        )
    except Exception as exc:
        raise JudgeV5DiagnosticAttemptFailed(
            turn_name=turn_name,
            error_class=type(exc).__name__,
        ) from exc
    if not result.status_ok or not isinstance(result.output, dict):
        raise JudgeV5DiagnosticAttemptFailed(
            turn_name=turn_name,
            error_class=str(result.error_class or result.status or "turn_failed"),
        )
    try:
        output, sidecar = _validate_completed_turn(
            paths=paths,
            prompt=prompt,
            schema=schema,
            base_instructions=base_instructions,
            model=model,
            reasoning_effort=reasoning_effort,
            output_validator=output_validator,
        )
    except Exception as exc:
        raise JudgeV5DiagnosticAttemptFailed(
            turn_name=turn_name,
            error_class=type(exc).__name__,
        ) from exc
    if _canonical_json(output) != _canonical_json(result.output):
        raise JudgeV5DiagnosticAttemptFailed(
            turn_name=turn_name,
            error_class="returned_output_checkpoint_mismatch",
        )
    return output, sidecar, False


def _aggregate_usage(sidecars: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total = {field: 0 for field in USAGE_FIELDS}
    for sidecar in sidecars:
        usage = _validate_usage(sidecar)
        for field in USAGE_FIELDS:
            total[field] += usage[field]
    return {
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": total,
        "turn_count": len(sidecars),
        "wall_elapsed_seconds_sum": round(
            sum(float(sidecar.get("wall_elapsed_seconds") or 0.0) for sidecar in sidecars),
            3,
        ),
    }


def _attempt_records(root: Path) -> list[dict[str, Any]]:
    records = []
    for turn_root in sorted((root / "turns").glob("*")) if (root / "turns").is_dir() else []:
        if not turn_root.is_dir():
            continue
        row: dict[str, Any] = {"turn_name": turn_root.name.replace("-", "_")}
        for key, filename in (
            ("capacity", "capacity.json"),
            ("sidecar", "sidecar.json"),
            ("output", "output.private.json"),
        ):
            path = turn_root / filename
            row[key] = _record(path) if path.is_file() else None
        if row["sidecar"] is not None:
            sidecar = _load_json(turn_root / "sidecar.json", purpose="attempt sidecar")
            row.update(
                {
                    "state": sidecar.get("state"),
                    "status": sidecar.get("status"),
                    "error_class": sidecar.get("error_class"),
                    "usage_status": sidecar.get("usage_status"),
                }
            )
        records.append(row)
    return records


def _write_failure_terminal(
    *,
    root: Path,
    spec_path: Path,
    turn_name: Optional[str],
    error_class: str,
) -> dict[str, Any]:
    attempts = _attempt_records(root)
    known_usage = {field: 0 for field in USAGE_FIELDS}
    known_turn_count = 0
    unknown_usage_turn_count = 0
    partial_attempt_without_sidecar = False
    for attempt in attempts:
        sidecar_record = attempt.get("sidecar")
        if not isinstance(sidecar_record, dict):
            if attempt.get("capacity") is not None or attempt.get("output") is not None:
                partial_attempt_without_sidecar = True
            continue
        sidecar = _load_json(Path(sidecar_record["path"]), purpose="failed attempt sidecar")
        try:
            usage = _validate_usage(sidecar)
        except JudgeV5DiagnosticError:
            unknown_usage_turn_count += 1
            continue
        known_turn_count += 1
        for field in USAGE_FIELDS:
            known_usage[field] += usage[field]
    accounting_complete = bool(
        not partial_attempt_without_sidecar and unknown_usage_turn_count == 0
    )
    usage_status = "complete" if accounting_complete else "unknown"
    usage = known_usage if accounting_complete else None
    failure = {
        "schema_version": DIAGNOSTIC_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "aggregate_or_score_authorized": False,
        "full_calibration_authorized": False,
        "accounting_complete": accounting_complete,
        "usage_status": usage_status,
        "usage": usage,
        "known_usage_lower_bound": known_usage,
        "known_usage_turn_count": known_turn_count,
        "unknown_usage_turn_count": unknown_usage_turn_count,
        "partial_attempt_without_sidecar": partial_attempt_without_sidecar,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable_json(failure_path, failure)
    terminal = {
        "schema_version": DIAGNOSTIC_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "spec_sha256": _sha256_file(spec_path),
        "failure": _record(failure_path),
        "accounting_complete": accounting_complete,
        "usage_status": usage_status,
        "usage": usage,
        "diagnostic_passed": False,
        "full_calibration_authorized": False,
        "production_mutated": False,
        "semantic_retry_allowed": False,
    }
    _write_immutable_json(root / "terminal.json", terminal)
    return terminal


def _verify_terminal(root: Path, terminal: Mapping[str, Any]) -> dict[str, Any]:
    spec_path = root / "diagnostic-spec.json"
    if (
        terminal.get("schema_version") != DIAGNOSTIC_TERMINAL_VERSION
        or terminal.get("spec_sha256") != _sha256_file(spec_path)
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_allowed") is not False
    ):
        raise JudgeV5DiagnosticError("diagnostic terminal drifted")
    spec = _load_json(spec_path, purpose="v5 diagnostic specification")
    if spec.get("schema_version") != DIAGNOSTIC_RUN_VERSION:
        raise JudgeV5DiagnosticError("diagnostic specification version drifted")
    _verify_nested_records(spec)
    _verify_nested_records(terminal)
    return dict(terminal)


async def run_v5_judge_diagnostic(
    *,
    output_dir: Path = DEFAULT_DIAGNOSTIC_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    fixture_path: Optional[Path] = None,
    reuse_contract_path: Path = DEFAULT_REUSE_CONTRACT_PATH,
    fixture_audit_receipt_path: Path = DEFAULT_FIXTURE_AUDIT_RECEIPT_PATH,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _verify_terminal(
            root, _load_json(terminal_path, purpose="v5 diagnostic terminal")
        )
    frozen = freeze_v5_diagnostic_protocol(
        output_dir=root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
        fixture_path=fixture_path,
        reuse_contract_path=reuse_contract_path,
        fixture_audit_receipt_path=fixture_audit_receipt_path,
    )
    spec_path = frozen["spec_path"]
    sidecars: list[dict[str, Any]] = []
    adopted: dict[str, bool] = {}
    try:
        async with client_factory() as client:
            pointwise_output, pointwise_sidecar, adopted_pointwise = await _get_or_run_turn(
                client=client,
                turn_name="pointwise_support",
                paths=frozen["pointwise_paths"],
                prompt=frozen["pointwise_prompt"],
                schema=frozen["pointwise_schema"],
                base_instructions=pointwise_support_base_instructions(),
                model=model,
                reasoning_effort=reasoning_effort,
                timeout_seconds=timeout_seconds,
                batch_size=len(frozen["pointwise_input"]["units"]),
                output_validator=lambda output: validate_pointwise_support_output(
                    output, frozen["pointwise_input"]
                ),
            )
            sidecars.append(pointwise_sidecar)
            adopted["pointwise_support"] = adopted_pointwise
            support_receipts = freeze_support_receipts(
                pointwise_output, frozen["pointwise_input"]
            )
            support_path = root / "support-receipts.private.json"
            _write_immutable_json(support_path, support_receipts)

            base_input = build_neutral_alignment_input(
                frozen["pool"], support_receipts
            )
            base_prompt = build_neutral_alignment_prompt(base_input)
            base_schema = neutral_alignment_output_schema(base_input)
            base_paths = _freeze_turn_request(
                root=root,
                turn_name="neutral_alignment_base",
                input_value=base_input,
                prompt=base_prompt,
                schema=base_schema,
            )
            base_output, base_sidecar, adopted_base = await _get_or_run_turn(
                client=client,
                turn_name="neutral_alignment_base",
                paths=base_paths,
                prompt=base_prompt,
                schema=base_schema,
                base_instructions=neutral_alignment_base_instructions(),
                model=model,
                reasoning_effort=reasoning_effort,
                timeout_seconds=timeout_seconds,
                batch_size=len(base_input["cases"]),
                output_validator=lambda output: validate_neutral_alignment_output(
                    output, base_input
                ),
            )
            sidecars.append(base_sidecar)
            adopted["neutral_alignment_base"] = adopted_base

            canary_input = build_neutral_alignment_input(
                frozen["pool"],
                support_receipts,
                case_ids=frozen["expected"]["canary_case_ids"],
                permutation="balanced_canary",
            )
            canary_prompt = build_neutral_alignment_prompt(canary_input)
            canary_schema = neutral_alignment_output_schema(canary_input)
            canary_paths = _freeze_turn_request(
                root=root,
                turn_name="neutral_alignment_canary",
                input_value=canary_input,
                prompt=canary_prompt,
                schema=canary_schema,
            )
            canary_output, canary_sidecar, adopted_canary = await _get_or_run_turn(
                client=client,
                turn_name="neutral_alignment_canary",
                paths=canary_paths,
                prompt=canary_prompt,
                schema=canary_schema,
                base_instructions=neutral_alignment_base_instructions(),
                model=model,
                reasoning_effort=reasoning_effort,
                timeout_seconds=timeout_seconds,
                batch_size=len(canary_input["cases"]),
                output_validator=lambda output: validate_neutral_alignment_output(
                    output, canary_input
                ),
            )
            sidecars.append(canary_sidecar)
            adopted["neutral_alignment_canary"] = adopted_canary

            disagreements = find_observable_alignment_disagreements(
                base_output=base_output,
                base_input=base_input,
                canary_output=canary_output,
                canary_input=canary_input,
                support_receipts=support_receipts,
            )
            disagreement_path = root / "observable-disagreements.private.json"
            _write_immutable_json(disagreement_path, disagreements)
            adjudication_output = None
            adjudication_alignment = None
            if disagreements["adjudication_required"]:
                adjudication_packet = build_disagreement_adjudication_input(
                    base_input=base_input,
                    base_output=base_output,
                    canary_input=canary_input,
                    canary_output=canary_output,
                    support_receipts=support_receipts,
                )
                adjudication_alignment = adjudication_alignment_input(
                    base_input=base_input,
                    adjudication_input=adjudication_packet,
                )
                adjudication_prompt = build_disagreement_adjudication_prompt(
                    adjudication_input=adjudication_packet,
                    adjudication_alignment=adjudication_alignment,
                )
                adjudication_schema = neutral_alignment_output_schema(
                    adjudication_alignment
                )
                adjudication_paths = _freeze_turn_request(
                    root=root,
                    turn_name="disagreement_adjudication",
                    input_value={
                        "adjudication_packet": adjudication_packet,
                        "alignment_input": adjudication_alignment,
                    },
                    prompt=adjudication_prompt,
                    schema=adjudication_schema,
                )
                adjudication_output, adjudication_sidecar, adopted_adjudication = await _get_or_run_turn(
                    client=client,
                    turn_name="disagreement_adjudication",
                    paths=adjudication_paths,
                    prompt=adjudication_prompt,
                    schema=adjudication_schema,
                    base_instructions=neutral_alignment_base_instructions(),
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(adjudication_alignment["cases"]),
                    output_validator=lambda output: validate_neutral_alignment_output(
                        output, adjudication_alignment
                    ),
                )
                sidecars.append(adjudication_sidecar)
                adopted["disagreement_adjudication"] = adopted_adjudication

        reconciled = reconcile_neutral_alignment(
            base_output=base_output,
            base_input=base_input,
            canary_output=canary_output,
            canary_input=canary_input,
            support_receipts=support_receipts,
            adjudication_output=adjudication_output,
            adjudication_input=adjudication_alignment,
        )
        reconciled_path = root / "reconciled-alignment.private.json"
        _write_immutable_json(reconciled_path, reconciled)
        score = score_v5_diagnostic(
            pointwise_output=pointwise_output,
            pointwise_input=frozen["pointwise_input"],
            reconciled_alignment=reconciled,
            expected=frozen["expected"],
            observable_disagreements=disagreements,
        )
        score_path = root / "diagnostic-score.json"
        _write_immutable_json(score_path, score)
        accounting = _aggregate_usage(sidecars)
        terminal_reason = (
            "judge_diagnostic_passed_full_calibration_authorized"
            if score["passed"]
            else "judge_diagnostic_quality_gate_not_passed"
        )
        terminal = {
            "schema_version": DIAGNOSTIC_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": terminal_reason,
            "spec_sha256": _sha256_file(spec_path),
            "diagnostic_passed": score["passed"],
            "full_calibration_authorized": score["passed"],
            "score": _record(score_path),
            "support_receipts": _record(root / "support-receipts.private.json"),
            "observable_disagreements": _record(
                root / "observable-disagreements.private.json"
            ),
            "reconciled_alignment": _record(reconciled_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": adopted,
            "semantic_retry_count": 0,
            "production_mutated": False,
            "semantic_retry_allowed": False,
            **accounting,
        }
        _write_immutable_json(terminal_path, terminal)
        return terminal
    except JudgeV5DiagnosticAttemptFailed as exc:
        return _write_failure_terminal(
            root=root,
            spec_path=spec_path,
            turn_name=exc.turn_name,
            error_class=exc.error_class,
        )
    except Exception as exc:
        return _write_failure_terminal(
            root=root,
            spec_path=spec_path,
            turn_name=None,
            error_class=type(exc).__name__,
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run pipeline-v5 judge diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_DIAGNOSTIC_ROOT))
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    parser.add_argument("--fixture-path")
    parser.add_argument("--reuse-contract", default=str(DEFAULT_REUSE_CONTRACT_PATH))
    parser.add_argument(
        "--fixture-audit-receipt",
        default=str(DEFAULT_FIXTURE_AUDIT_RECEIPT_PATH),
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    terminal = asyncio.run(
        run_v5_judge_diagnostic(
            output_dir=Path(args.output_dir),
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
            fixture_path=Path(args.fixture_path) if args.fixture_path else None,
            reuse_contract_path=Path(args.reuse_contract),
            fixture_audit_receipt_path=Path(args.fixture_audit_receipt),
        )
    )
    print(
        json.dumps(
            {
                "ok": terminal.get("state") == "completed",
                "state": terminal.get("state"),
                "terminal_reason": terminal.get("terminal_reason"),
                "diagnostic_passed": terminal.get("diagnostic_passed"),
                "full_calibration_authorized": terminal.get(
                    "full_calibration_authorized"
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal.get("state") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
