from __future__ import annotations

"""Reusable checksum-bound runtime for one-turn managed app-server canaries."""

import asyncio
import copy
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Mapping, Sequence

from . import app_server_canonical_v31_bounded_span_canary_runtime as legacy
from . import app_server_canonical_v31_epoch7_input_authority_runtime as capacity
from . import codex_app_server


RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
CONTRACT_VERSION = "pif_one_turn_canary_runtime_contract_v1"
LOCK_VERSION = "pif_one_turn_canary_runtime_lock_v1"
AUTHORIZATION_VERSION = "pif_one_turn_canary_authorization_v1"
MANIFEST_VERSION = "pif_one_turn_canary_manifest_v1"
ATTEMPT_VERSION = "pif_one_turn_canary_attempt_v1"
THREAD_VERSION = "pif_one_turn_canary_thread_v1"
DISPATCH_VERSION = "pif_one_turn_canary_dispatch_v1"
REJECTION_VERSION = "pif_one_turn_canary_rejection_v1"

_FORBIDDEN_AUTH_ENVIRONMENT = (
    "OPENAI_API_KEY",
    "CODEX_API_KEY",
    "OPENAI_ACCESS_TOKEN",
    "OPENAI_AUTH_TOKEN",
    "CHATGPT_ACCESS_TOKEN",
    "OPENAI_SESSION_TOKEN",
    "CHATGPT_SESSION_TOKEN",
    "CODEX_SESSION_TOKEN",
)


class OneTurnCanaryError(RuntimeError):
    pass


class OneTurnCanaryWaiting(OneTurnCanaryError):
    pass


@dataclass(frozen=True)
class OneTurnCanarySpec:
    project_root: Path
    default_root: Path
    directive_path: Path
    plan_path: Path
    directive_sha256: str
    thread_id: str
    plan_epoch: int
    step_id: str
    turn_name: str
    stage: str
    architecture_class: str
    authorization_statement: str
    authorization_state: str
    model: str
    effort: str
    maximum_total_tokens: int
    maximum_wall_seconds: int
    minimum_remaining_reserve_percent: int
    capacity_safety_margin_percent: int
    quota_points_per_million_tokens: int
    authorization_window_seconds: int
    adapter: Any
    validate_directive: Callable[[Mapping[str, Any]], None]
    validate_predecessor: Callable[[bool], None]
    build_request: Callable[[], dict[str, Any]]
    predecessor_records: Callable[[], Mapping[str, Any]]
    runtime_module_paths: Callable[[], Sequence[Path]]
    receipt_metadata: Callable[[Mapping[str, Any]], Mapping[str, Any]]
    pass_next_action: str
    reject_next_action: str
    waiting_next_action: str = "no_replay_bounded_recovery_only"


def _record(path: Path) -> dict[str, Any]:
    return legacy._record(path)  # noqa: SLF001


def _load(path: Path, label: str) -> dict[str, Any]:
    return legacy._load_object(path, label=label)  # noqa: SLF001


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OneTurnCanaryError(f"{label} is unavailable or invalid") from exc


def _write_json(path: Path, value: Any) -> dict[str, Any]:
    return legacy._write_json(path, value)  # noqa: SLF001


def _write_bytes(path: Path, value: bytes) -> dict[str, Any]:
    return legacy._write_bytes(path, value)  # noqa: SLF001


def _canonical_json(value: Any) -> str:
    return legacy._canonical_json(value)  # noqa: SLF001


def _sha256_bytes(value: bytes) -> str:
    return legacy._sha256_bytes(value)  # noqa: SLF001


def _now(value: datetime | None = None) -> datetime:
    return legacy._now(value)  # noqa: SLF001


def _parse_timestamp(value: Any, label: str) -> datetime:
    return legacy._parse_timestamp(value, label=label)  # noqa: SLF001


def _zero_usage(spec: OneTurnCanarySpec) -> dict[str, int]:
    return {field: 0 for field in spec.adapter.USAGE_FIELDS}


def _paths(root: Path) -> dict[str, Path]:
    prepared = root / "prepared-turn"
    turn = root / "turn"
    return {
        "contract": root / "runtime-contract.json",
        "lock": root / "runtime-lock.json",
        "authorization": root / "operator-authorization.json",
        "receipt": root / "plan-step-receipt.json",
        "terminal": root / "terminal.json",
        "rejection": root / "semantic-rejection.json",
        "request": prepared / "request.private.json",
        "prompt": prepared / "prompt.private.md",
        "base": prepared / "base-instructions.private.md",
        "schema": prepared / "schema.json",
        "manifest": prepared / "turn-manifest.json",
        "attempt": turn / "semantic-attempt.json",
        "initial_capacity": turn / "capacity" / "initial",
        "preturn_capacity": turn / "capacity" / "preturn",
        "thread": turn / "thread-start.json",
        "dispatch": turn / "semantic-dispatch.json",
        "sidecar": turn / "sidecar.json",
        "output": turn / "output.private.json",
        "labels": turn / "canonical-labels.private.json",
        "provenance": turn / "evidence-provenance.private.json",
        "fidelity": turn / "semantic-fidelity.json",
    }


def _verify_control_contract(spec: OneTurnCanarySpec) -> tuple[dict[str, Any], dict[str, Any]]:
    directive_record = _record(spec.directive_path)
    if directive_record["sha256"] != spec.directive_sha256:
        raise OneTurnCanaryError("one-turn canary directive drifted")
    directive = _load(spec.directive_path, "one-turn canary directive")
    spec.validate_directive(directive)
    plan = _load(spec.plan_path, "one-turn canary semantic plan")
    step = plan.get("step")
    if (
        set(plan) != {"schema_version", "thread_id", "plan_epoch", "state", "step"}
        or plan.get("schema_version") != "pif_evaluation_semantic_plan_v1"
        or plan.get("thread_id") != spec.thread_id
        or plan.get("plan_epoch") != spec.plan_epoch
        or plan.get("state") != "executable"
        or not isinstance(step, Mapping)
        or set(step)
        != {
            "step_id",
            "state",
            "max_model_calls",
            "max_total_tokens",
            "expected_receipt_path",
            "accepted_receipt_states",
            "directive_path",
            "directive_sha256",
        }
        or step.get("step_id") != spec.step_id
        or step.get("state") != "executable"
        or step.get("max_model_calls") != 1
        or step.get("max_total_tokens") != spec.maximum_total_tokens
        or step.get("expected_receipt_path")
        != str((spec.default_root / "plan-step-receipt.json").resolve())
        or step.get("accepted_receipt_states") != ["passed", "rejected", "waiting"]
        or step.get("directive_path") != str(spec.directive_path.resolve())
        or step.get("directive_sha256") != spec.directive_sha256
    ):
        raise OneTurnCanaryError("one-turn canary semantic plan drifted")
    return directive, plan


def _turn_manifest(spec: OneTurnCanarySpec, root: Path, request: Mapping[str, Any]) -> dict[str, Any]:
    paths = _paths(root)
    return {
        "schema_version": MANIFEST_VERSION,
        "turn_name": spec.turn_name,
        "stage": spec.stage,
        "ordinal": 0,
        "episode_id": request["episode_id"],
        "segment_ids": copy.deepcopy(request["segment_ids"]),
        "batch_id": request["batch_id"],
        "model": spec.model,
        "effort": spec.effort,
        "thread_mode": "new_thread",
        "batch_size": request["effective_batch_size"],
        "request": _record(paths["request"]),
        "prompt": _record(paths["prompt"]),
        "base_instructions": _record(paths["base"]),
        "output_schema": _record(paths["schema"]),
        "semantic_model_call_cap": 1,
        "semantic_retry_count": 0,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
    }


def _runtime_contract(spec: OneTurnCanarySpec, root: Path) -> dict[str, Any]:
    paths = _paths(root)
    request = _load(paths["request"], "prepared one-turn request")
    spec.adapter.validate_prepared_request(request)
    if request != spec.build_request():
        raise OneTurnCanaryError("one-turn canary request differs from frozen sample")
    return {
        "schema_version": CONTRACT_VERSION,
        "thread_id": spec.thread_id,
        "plan_epoch": spec.plan_epoch,
        "step_id": spec.step_id,
        "output_root": str(root.resolve()),
        "directive": _record(spec.directive_path),
        "semantic_plan": _record(spec.plan_path),
        "predecessor": copy.deepcopy(dict(spec.predecessor_records())),
        "adapter_binding": spec.adapter.build_six_arm_matrix_binding(),
        "turns": [
            {
                "ordinal": 0,
                "turn_id": spec.turn_name,
                "episode_id": request["episode_id"],
                "turn_manifest": _record(paths["manifest"]),
            }
        ],
        "model": spec.model,
        "effort": spec.effort,
        "exact_turn_count": 1,
        "maximum_total_tokens_per_turn": spec.maximum_total_tokens,
        "maximum_wall_seconds_per_turn": spec.maximum_wall_seconds,
        "quota_points_per_million_tokens": spec.quota_points_per_million_tokens,
        "minimum_remaining_reserve_percent": spec.minimum_remaining_reserve_percent,
        "capacity_safety_margin_percent": spec.capacity_safety_margin_percent,
        "operator_wall_safety_margin_seconds": 60,
        "semantic_retry_count": 0,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "api_key_billing_allowed": False,
        "raw_session_token_replay_allowed": False,
        "codex_exec_semantic_work_allowed": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _runtime_lock(
    spec: OneTurnCanarySpec, root: Path, contract: Mapping[str, Any]
) -> dict[str, Any]:
    paths = _paths(root)
    modules = [Path(__file__), *spec.runtime_module_paths(), Path(capacity.__file__), Path(codex_app_server.__file__)]
    deduplicated = list(dict.fromkeys(path.resolve() for path in modules))
    return {
        "schema_version": LOCK_VERSION,
        "thread_id": spec.thread_id,
        "plan_epoch": spec.plan_epoch,
        "step_id": spec.step_id,
        "runtime_contract": _record(paths["contract"]),
        "directive": _record(spec.directive_path),
        "semantic_plan": _record(spec.plan_path),
        "python_modules": [_record(path) for path in deduplicated],
        "prepared_artifacts": {
            role: _record(paths[role])
            for role in ("request", "prompt", "base", "schema", "manifest")
        },
        "predecessor_artifacts": copy.deepcopy(contract["predecessor"]),
        "canonical_label_schema": _record(spec.adapter.base.LABEL_SCHEMA_PATH),
        "protocol_schema": _record(codex_app_server.PROTOCOL_SCHEMA_PATH),
        "pinned_codex_cli": _record(spec.adapter.PINNED_CODEX),
        "pinned_codex_cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
        "instruction_source_contract": spec.adapter.expected_instruction_source_contract(),
        "context_control_overlay": spec.adapter.verified_context_control_overlay(),
        "context_control_overlay_sha256": spec.adapter.base.sha256_text(
            _canonical_json(spec.adapter.verified_context_control_overlay())
        ),
    }


def prepare(spec: OneTurnCanarySpec, root: Path | None = None) -> dict[str, Any]:
    _verify_control_contract(spec)
    spec.validate_predecessor(True)
    output_root = (root or spec.default_root).expanduser().resolve()
    if output_root.exists() and any(output_root.iterdir()):
        raise OneTurnCanaryError("one-turn canary output root is not empty")
    output_root.mkdir(parents=True, exist_ok=True)
    paths = _paths(output_root)
    request = spec.build_request()
    spec.adapter.validate_prepared_request(request)
    _write_json(paths["request"], request)
    _write_bytes(paths["prompt"], request["prompt"].encode("utf-8"))
    _write_bytes(paths["base"], request["base_instructions"].encode("utf-8"))
    _write_json(paths["schema"], request["output_schema"])
    _write_json(paths["manifest"], _turn_manifest(spec, output_root, request))
    contract = _runtime_contract(spec, output_root)
    _write_json(paths["contract"], contract)
    _write_json(paths["lock"], _runtime_lock(spec, output_root, contract))
    return verify_runtime(spec, output_root)


def verify_runtime(spec: OneTurnCanarySpec, root: Path | None = None) -> dict[str, Any]:
    _verify_control_contract(spec)
    spec.validate_predecessor(False)
    output_root = (root or spec.default_root).expanduser().resolve()
    paths = _paths(output_root)
    contract = _load(paths["contract"], "one-turn runtime contract")
    if contract != _runtime_contract(spec, output_root):
        raise OneTurnCanaryError("one-turn runtime contract drifted")
    lock = _load(paths["lock"], "one-turn runtime lock")
    if lock != _runtime_lock(spec, output_root, contract):
        raise OneTurnCanaryError("one-turn runtime lock drifted")
    request = _load(paths["request"], "one-turn prepared request")
    spec.adapter.validate_prepared_request(request)
    return {
        "schema_version": "pif_one_turn_canary_runtime_status_v1",
        "state": "verified_zero_call_runtime",
        "runtime_contract": _record(paths["contract"]),
        "runtime_lock": _record(paths["lock"]),
        "request": _record(paths["request"]),
        "prompt_bytes": paths["prompt"].stat().st_size,
        "schema_bytes": paths["schema"].stat().st_size,
        "source_unit_count": sum(
            len(segment["units"]) for segment in request["private_input"]["segments"]
        ),
        "evidence_span_count": sum(
            len(segment["evidence_spans"])
            for segment in request["private_input"]["segments"]
        ),
        "literal_token_count": sum(
            len(segment.get("literal_tokens", []))
            for segment in request["private_input"]["segments"]
        ),
        "semantic_model_call_count": int(paths["dispatch"].is_file()),
    }


def authorize(
    spec: OneTurnCanarySpec,
    *,
    root: Path | None,
    operator_authorization_id: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    if not isinstance(operator_authorization_id, str) or not operator_authorization_id.strip():
        raise OneTurnCanaryError("operator authorization id is required")
    output_root = (root or spec.default_root).expanduser().resolve()
    verify_runtime(spec, output_root)
    paths = _paths(output_root)
    current = _now(now)
    payload = {
        "schema_version": AUTHORIZATION_VERSION,
        "state": spec.authorization_state,
        "thread_id": spec.thread_id,
        "plan_epoch": spec.plan_epoch,
        "step_id": spec.step_id,
        "operator_authorization_id": operator_authorization_id.strip(),
        "authorized_by": "kolby",
        "authority": "direct_user_instruction",
        "authorization_statement": spec.authorization_statement,
        "authorization_statement_sha256": _sha256_bytes(
            spec.authorization_statement.encode("utf-8")
        ),
        "created_at": current.isoformat(),
        "expires_at": (
            current + timedelta(seconds=spec.authorization_window_seconds)
        ).isoformat(),
        "runtime_contract": _record(paths["contract"]),
        "runtime_lock": _record(paths["lock"]),
        "semantic_model_call_cap": 1,
        "semantic_retry_count": 0,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    if paths["authorization"].exists():
        if _load(paths["authorization"], "one-turn authorization") != payload:
            raise OneTurnCanaryError("one-turn authorization already exists and differs")
    else:
        _write_json(paths["authorization"], payload)
    return verify_authorization(
        spec,
        root=output_root,
        expected_authorization_id=operator_authorization_id,
        require_current=True,
        now=current,
    )


def verify_authorization(
    spec: OneTurnCanarySpec,
    *,
    root: Path | None,
    expected_authorization_id: str | None,
    require_current: bool,
    now: datetime | None = None,
) -> dict[str, Any]:
    output_root = (root or spec.default_root).expanduser().resolve()
    verify_runtime(spec, output_root)
    paths = _paths(output_root)
    value = _load(paths["authorization"], "one-turn authorization")
    if (
        value.get("schema_version") != AUTHORIZATION_VERSION
        or value.get("state") != spec.authorization_state
        or value.get("thread_id") != spec.thread_id
        or value.get("plan_epoch") != spec.plan_epoch
        or value.get("step_id") != spec.step_id
        or value.get("authorized_by") != "kolby"
        or value.get("authority") != "direct_user_instruction"
        or value.get("authorization_statement") != spec.authorization_statement
        or value.get("authorization_statement_sha256")
        != _sha256_bytes(spec.authorization_statement.encode("utf-8"))
        or value.get("runtime_contract") != _record(paths["contract"])
        or value.get("runtime_lock") != _record(paths["lock"])
        or value.get("semantic_model_call_cap") != 1
        or value.get("semantic_retry_count") != 0
        or value.get("quality_authorized") is not False
        or value.get("holdout_authorized") is not False
        or value.get("production_mutation_allowed") is not False
    ):
        raise OneTurnCanaryError("one-turn authorization drifted")
    if expected_authorization_id is not None and value.get(
        "operator_authorization_id"
    ) != expected_authorization_id:
        raise OneTurnCanaryError("one-turn authorization id drifted")
    created = _parse_timestamp(value.get("created_at"), "authorization created_at")
    expires = _parse_timestamp(value.get("expires_at"), "authorization expires_at")
    if (expires - created).total_seconds() != spec.authorization_window_seconds:
        raise OneTurnCanaryError("one-turn authorization duration drifted")
    if require_current and not (created <= _now(now) < expires):
        raise OneTurnCanaryWaiting("one-turn authorization is not current")
    return value


def _attempt_payload(
    spec: OneTurnCanarySpec, root: Path, authorization: Mapping[str, Any]
) -> dict[str, Any]:
    paths = _paths(root)
    return {
        "schema_version": ATTEMPT_VERSION,
        "state": "declared_before_capacity_thread_or_turn",
        "turn_name": spec.turn_name,
        "runtime_contract": _record(paths["contract"]),
        "runtime_lock": _record(paths["lock"]),
        "operator_authorization": _record(paths["authorization"]),
        "turn_manifest": _record(paths["manifest"]),
        "operator_authorization_id": authorization["operator_authorization_id"],
        "model": spec.model,
        "effort": spec.effort,
        "semantic_model_call_cap": 1,
        "semantic_retry_count": 0,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
    }


def _capacity_request(
    spec: OneTurnCanarySpec,
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    *,
    boundary: str,
) -> dict[str, Any]:
    return capacity._capacity_request(  # noqa: SLF001
        root=root,
        contract=contract,
        authorization=authorization,
        turn=contract["turns"][0],
        boundary=boundary,
        completed_turn_count=0,
    )


def _verify_capacity_bundles(
    spec: OneTurnCanarySpec,
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    *,
    historical: bool,
) -> None:
    paths = _paths(root)
    for boundary, role in (
        ("initial_before_thread", "initial_capacity"),
        ("preturn_before_turn", "preturn_capacity"),
    ):
        bundle = paths[role]
        if not bundle.exists():
            continue
        capacity._verify_capacity_bundle(  # noqa: SLF001
            bundle,
            expected_request=_capacity_request(
                spec, root, contract, authorization, boundary=boundary
            ),
            contract=contract,
            authorization=authorization,
            historical=historical,
            require_available=False,
        )


def _thread_payload(
    spec: OneTurnCanarySpec,
    thread: Any,
    *,
    root: Path,
    request: Mapping[str, Any],
) -> dict[str, Any]:
    preflight = spec.adapter._verify_started_thread(thread, request)  # noqa: SLF001
    if getattr(thread, "cwd", None) != str(spec.project_root.resolve()):
        raise OneTurnCanaryError("started thread cwd drifted")
    return {
        "schema_version": THREAD_VERSION,
        "state": "ephemeral_thread_started_before_semantic_turn",
        "thread_id": thread.thread_id,
        "model": spec.model,
        "effort": spec.effort,
        "cwd": str(spec.project_root.resolve()),
        "ephemeral": True,
        "base_instructions_sha256": request["base_instructions_sha256"],
        "base_instructions_bytes": len(request["base_instructions"].encode("utf-8")),
        "instruction_sources_sha256": preflight["instruction_sources_sha256"],
        "instruction_sources_count": preflight["instruction_sources_count"],
        "context_control_overlay_sha256": request["context_control_overlay_sha256"],
        "runtime_contract": _record(_paths(root)["contract"]),
        "runtime_lock": _record(_paths(root)["lock"]),
        "operator_authorization": _record(_paths(root)["authorization"]),
        "turn_manifest": _record(_paths(root)["manifest"]),
    }


def _thread_from_payload(
    spec: OneTurnCanarySpec, value: Mapping[str, Any], request: Mapping[str, Any]
) -> Any:
    sources = spec.adapter.expected_instruction_source_contract()
    if (
        value.get("schema_version") != THREAD_VERSION
        or value.get("state") != "ephemeral_thread_started_before_semantic_turn"
        or not isinstance(value.get("thread_id"), str)
        or not value.get("thread_id")
        or value.get("model") != spec.model
        or value.get("effort") != spec.effort
        or value.get("cwd") != str(spec.project_root.resolve())
        or value.get("ephemeral") is not True
        or value.get("base_instructions_sha256") != request["base_instructions_sha256"]
        or value.get("base_instructions_bytes")
        != len(request["base_instructions"].encode("utf-8"))
        or value.get("instruction_sources_sha256")
        != sources["effective_instruction_sources_sha256"]
        or value.get("instruction_sources_count")
        != sources["effective_instruction_sources_count"]
        or value.get("context_control_overlay_sha256")
        != request["context_control_overlay_sha256"]
    ):
        raise OneTurnCanaryError("one-turn thread binding drifted")
    return SimpleNamespace(
        thread_id=value["thread_id"],
        model=spec.model,
        cwd=str(spec.project_root.resolve()),
        ephemeral=True,
        base_instructions_sha256=value["base_instructions_sha256"],
        base_instructions_bytes=value["base_instructions_bytes"],
        instruction_sources_sha256=value["instruction_sources_sha256"],
        instruction_sources_count=value["instruction_sources_count"],
    )


def _capacity_records(paths: Mapping[str, Path], role: str) -> dict[str, Any]:
    return {
        name: _record(paths[role] / filename)
        for name, filename in (
            ("request", "request.json"),
            ("provider_response", "provider-response.private.json"),
            ("measurement", "measurement.json"),
        )
    }


def _artifact_records(root: Path) -> dict[str, Any]:
    paths = _paths(root)
    roles = (
        "contract",
        "lock",
        "authorization",
        "request",
        "prompt",
        "base",
        "schema",
        "manifest",
        "attempt",
        "thread",
        "dispatch",
        "sidecar",
        "output",
        "labels",
        "provenance",
        "fidelity",
        "rejection",
    )
    records = {role: _record(paths[role]) for role in roles if paths[role].is_file()}
    for role in ("initial_capacity", "preturn_capacity"):
        if paths[role].is_dir():
            records[role] = _capacity_records(paths, role)
    return records


def _terminal_sidecar_outcome(
    spec: OneTurnCanarySpec,
    sidecar: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
    thread_id: str,
) -> dict[str, Any]:
    if (
        sidecar.get("schema_version") != codex_app_server.TURN_SIDECAR_SCHEMA_VERSION
        or sidecar.get("client_version") != codex_app_server.APP_SERVER_CLIENT_VERSION
        or sidecar.get("cli_version") != codex_app_server.PINNED_CODEX_CLI_VERSION
        or sidecar.get("protocol_schema_sha256")
        != spec.adapter.base._sha256_file(codex_app_server.PROTOCOL_SCHEMA_PATH)  # noqa: SLF001
        or sidecar.get("transport") != "stdio"
        or sidecar.get("synthetic_debug_errors") is not False
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("thread_id") != thread_id
        or sidecar.get("model") != spec.model
        or sidecar.get("effort") != spec.effort
        or sidecar.get("thread_mode") != "new_thread"
        or sidecar.get("batch_size") != request["effective_batch_size"]
        or sidecar.get("prompt_sha256") != request["prompt_sha256"]
        or sidecar.get("prompt_bytes") != len(request["prompt"].encode("utf-8"))
        or sidecar.get("base_instructions_sha256")
        != request["base_instructions_sha256"]
        or sidecar.get("base_instructions_bytes")
        != len(request["base_instructions"].encode("utf-8"))
        or sidecar.get("output_schema_sha256") != request["output_schema_sha256"]
        or sidecar.get("output_schema_bytes")
        != len(_canonical_json(request["output_schema"]).encode("utf-8"))
        or sidecar.get("recovery_reran_model") is not False
    ):
        raise OneTurnCanaryError("terminal sidecar lineage drifted")
    structured = sidecar.get("error_class") == "structured_output_invalid"
    usage = _zero_usage(spec)
    unknown = 1
    if sidecar.get("usage_status") == "measured" and sidecar.get("usage_complete") is True:
        usage = spec.adapter.base._usage_values(  # noqa: SLF001
            sidecar.get("thread_total_usage"), "thread_total_usage"
        )
        unknown = 0
    return {
        "state": "rejected" if structured else "waiting",
        "reason": (
            f"epoch{spec.plan_epoch}_structured_output_rejected"
            if structured
            else f"epoch{spec.plan_epoch}_interrupted_semantic_attempt_preserved_no_replay"
        ),
        "semantic_model_call_count": 1,
        "unknown_usage_turn_count": unknown,
        "usage": usage,
        "wall_elapsed_seconds": float(sidecar.get("wall_elapsed_seconds") or 0.0),
        "failed_checks": ["structured_output_validity"] if structured else [],
        "diagnostic": {
            "error_class": str(sidecar.get("error_class") or "incomplete_sidecar"),
            "diagnostic_path": (
                "structured_output_invalid"
                if structured
                else "semantic_output_absent_or_accounting_incomplete"
            ),
        },
        "thread_ids": [thread_id],
        "turn_ids": [sidecar["turn_id"]]
        if isinstance(sidecar.get("turn_id"), str) and sidecar.get("turn_id")
        else [],
    }


def _derive_outcome(
    spec: OneTurnCanarySpec, root: Path, *, write_projection: bool
) -> dict[str, Any]:
    paths = _paths(root)
    contract = _load(paths["contract"], "one-turn runtime contract")
    authorization = verify_authorization(
        spec,
        root=root,
        expected_authorization_id=None,
        require_current=False,
    )
    request = _load(paths["request"], "one-turn prepared request")
    spec.adapter.validate_prepared_request(request)
    _verify_capacity_bundles(
        spec, root, contract, authorization, historical=True
    )
    if not paths["attempt"].is_file():
        raise OneTurnCanaryError("terminal cannot exist without a declared attempt")
    if _load(paths["attempt"], "one-turn attempt") != _attempt_payload(
        spec, root, authorization
    ):
        raise OneTurnCanaryError("one-turn semantic attempt drifted")
    if not paths["dispatch"].is_file():
        reason = f"epoch{spec.plan_epoch}_capacity_or_predispatch_waiting_zero_calls"
        if paths["initial_capacity"].is_dir() and _load(
            paths["initial_capacity"] / "measurement.json", "initial capacity"
        ).get("capacity_available") is False:
            reason = f"epoch{spec.plan_epoch}_capacity_waiting_zero_semantic_calls"
        return {
            "state": "waiting",
            "reason": reason,
            "semantic_model_call_count": 0,
            "unknown_usage_turn_count": 0,
            "usage": _zero_usage(spec),
            "wall_elapsed_seconds": 0.0,
            "failed_checks": [],
            "diagnostic": None,
            "thread_ids": [],
            "turn_ids": [],
        }
    if not paths["thread"].is_file():
        raise OneTurnCanaryError("semantic dispatch lacks thread binding")
    thread_payload = _load(paths["thread"], "one-turn thread binding")
    thread = _thread_from_payload(spec, thread_payload, request)
    if (
        thread_payload.get("runtime_contract") != _record(paths["contract"])
        or thread_payload.get("runtime_lock") != _record(paths["lock"])
        or thread_payload.get("operator_authorization") != _record(paths["authorization"])
        or thread_payload.get("turn_manifest") != _record(paths["manifest"])
    ):
        raise OneTurnCanaryError("one-turn thread lineage records drifted")
    dispatch = _load(paths["dispatch"], "one-turn semantic dispatch")
    if (
        dispatch.get("schema_version") != DISPATCH_VERSION
        or dispatch.get("state") != "semantic_turn_dispatch_committed"
        or dispatch.get("turn_name") != spec.turn_name
        or dispatch.get("request") != _record(paths["request"])
        or dispatch.get("runtime_lock") != _record(paths["lock"])
        or dispatch.get("operator_authorization") != _record(paths["authorization"])
        or dispatch.get("thread") != _record(paths["thread"])
        or dispatch.get("initial_capacity") != _capacity_records(paths, "initial_capacity")
        or dispatch.get("preturn_capacity") != _capacity_records(paths, "preturn_capacity")
        or dispatch.get("semantic_retry_count") != 0
    ):
        raise OneTurnCanaryError("one-turn semantic dispatch marker drifted")
    if not paths["sidecar"].is_file():
        return {
            "state": "waiting",
            "reason": f"epoch{spec.plan_epoch}_interrupted_semantic_attempt_preserved_no_replay",
            "semantic_model_call_count": 1,
            "unknown_usage_turn_count": 1,
            "usage": _zero_usage(spec),
            "wall_elapsed_seconds": 0.0,
            "failed_checks": [],
            "diagnostic": None,
            "thread_ids": [thread.thread_id],
            "turn_ids": [],
        }
    sidecar = _load(paths["sidecar"], "one-turn terminal sidecar")
    if not paths["output"].is_file():
        return _terminal_sidecar_outcome(
            spec, sidecar, request=request, thread_id=thread.thread_id
        )
    try:
        telemetry = spec.adapter.validate_turn_sidecar(
            request,
            paths["sidecar"],
            output_path=paths["output"],
            expected_thread=thread,
        )
    except spec.adapter.CanonicalV31TelemetryError:
        if sidecar.get("state") == "completed" and sidecar.get("status") == "completed":
            raise OneTurnCanaryError("completed one-turn sidecar failed integrity validation")
        return _terminal_sidecar_outcome(
            spec, sidecar, request=request, thread_id=thread.thread_id
        )
    raw_output = _load(paths["output"], "one-turn raw semantic output")
    failed_checks: list[str] = []
    diagnostic: dict[str, Any] | None = None
    try:
        projected = spec.adapter.validate_and_project_output(request, raw_output)
    except spec.adapter.CanonicalV31OutputError as exc:
        projected = None
        failed_checks.append("semantic_output_validity")
        diagnostic = {
            "error_class": type(exc).__name__,
            "diagnostic_path": str(exc),
            "diagnostic_sha256": _sha256_bytes(str(exc).encode("utf-8")),
        }
    if telemetry["thread_total_usage"]["total_tokens"] > spec.maximum_total_tokens:
        failed_checks.append("measured_total_token_acceptance_ceiling")
    if projected is not None:
        for role in ("labels", "provenance", "fidelity"):
            value = projected[role]
            if write_projection and not paths[role].exists():
                _write_json(paths[role], value)
            if not paths[role].is_file() or _load_json(paths[role], role) != value:
                raise OneTurnCanaryError(f"{role} artifact drifted")
    state = "passed" if not failed_checks else "rejected"
    if state == "passed":
        reason = f"epoch{spec.plan_epoch}_one_turn_canary_passed"
    elif failed_checks == ["measured_total_token_acceptance_ceiling"]:
        reason = f"epoch{spec.plan_epoch}_one_turn_canary_cost_rejected"
    else:
        reason = f"epoch{spec.plan_epoch}_one_turn_canary_semantic_output_rejected"
    return {
        "state": state,
        "reason": reason,
        "semantic_model_call_count": 1,
        "unknown_usage_turn_count": 0,
        "usage": telemetry["thread_total_usage"],
        "wall_elapsed_seconds": telemetry["wall_elapsed_seconds"],
        "failed_checks": sorted(set(failed_checks)),
        "diagnostic": diagnostic,
        "thread_ids": [telemetry["thread_id"]],
        "turn_ids": [telemetry["turn_id"]],
    }


def _receipt_payload(
    spec: OneTurnCanarySpec,
    root: Path,
    *,
    created_at: str,
    outcome: Mapping[str, Any],
) -> dict[str, Any]:
    paths = _paths(root)
    payload = {
        "schema_version": RECEIPT_VERSION,
        "thread_id": spec.thread_id,
        "plan_epoch": spec.plan_epoch,
        "step_id": spec.step_id,
        "state": outcome["state"],
        "terminal_reason": outcome["reason"],
        "created_at": created_at,
        "output_root": str(root.resolve()),
        "new_semantic_model_call_count": outcome["semantic_model_call_count"],
        "semantic_retry_count": 0,
        "new_unknown_usage_turn_count": outcome["unknown_usage_turn_count"],
        "new_measured_usage": copy.deepcopy(outcome["usage"]),
        "new_wall_elapsed_seconds": outcome["wall_elapsed_seconds"],
        "failed_checks": copy.deepcopy(outcome["failed_checks"]),
        "diagnostic": copy.deepcopy(outcome["diagnostic"]),
        "thread_ids": copy.deepcopy(outcome["thread_ids"]),
        "semantic_turn_ids": copy.deepcopy(outcome["turn_ids"]),
        "runtime_contract": _record(paths["contract"]),
        "runtime_lock": _record(paths["lock"]),
        "operator_authorization": _record(paths["authorization"]),
        "directive": _record(spec.directive_path),
        "semantic_plan": _record(spec.plan_path),
        "artifact_records": _artifact_records(root),
        "architecture_class": spec.architecture_class,
        "winner_frozen": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "next_authorized_action": (
            spec.pass_next_action
            if outcome["state"] == "passed"
            else (
                spec.waiting_next_action
                if outcome["state"] == "waiting"
                else spec.reject_next_action
            )
        ),
    }
    payload.update(copy.deepcopy(dict(spec.receipt_metadata(outcome))))
    return payload


def write_terminal(spec: OneTurnCanarySpec, root: Path | None = None) -> dict[str, Any]:
    output_root = (root or spec.default_root).expanduser().resolve()
    paths = _paths(output_root)
    if paths["receipt"].is_file() != paths["terminal"].is_file():
        source = paths["receipt"] if paths["receipt"].is_file() else paths["terminal"]
        target = paths["terminal"] if source == paths["receipt"] else paths["receipt"]
        payload = _load(source, "single terminal mirror")
        outcome = _derive_outcome(spec, output_root, write_projection=False)
        expected = _receipt_payload(
            spec, output_root, created_at=payload.get("created_at", ""), outcome=outcome
        )
        if payload != expected:
            raise OneTurnCanaryError("single terminal mirror is invalid")
        _write_bytes(target, source.read_bytes())
        return verify_receipt(spec, output_root)
    if paths["receipt"].is_file():
        return verify_receipt(spec, output_root)
    outcome = _derive_outcome(spec, output_root, write_projection=True)
    if outcome["state"] == "rejected" and outcome["diagnostic"] is not None:
        rejection = {
            "schema_version": REJECTION_VERSION,
            "turn_name": spec.turn_name,
            **copy.deepcopy(outcome["diagnostic"]),
        }
        if paths["rejection"].exists():
            if _load(paths["rejection"], "semantic rejection") != rejection:
                raise OneTurnCanaryError("semantic rejection artifact drifted")
        else:
            _write_json(paths["rejection"], rejection)
        outcome = _derive_outcome(spec, output_root, write_projection=True)
    payload = _receipt_payload(
        spec, output_root, created_at=_now().isoformat(), outcome=outcome
    )
    _write_json(paths["receipt"], payload)
    _write_json(paths["terminal"], payload)
    return verify_receipt(spec, output_root)


def verify_receipt(spec: OneTurnCanarySpec, root: Path | None = None) -> dict[str, Any]:
    output_root = (root or spec.default_root).expanduser().resolve()
    paths = _paths(output_root)
    if not paths["receipt"].is_file() or not paths["terminal"].is_file():
        raise OneTurnCanaryError("one-turn terminal mirrors are incomplete")
    receipt = _load(paths["receipt"], "one-turn receipt")
    terminal = _load(paths["terminal"], "one-turn terminal")
    if receipt != terminal:
        raise OneTurnCanaryError("one-turn terminal mirrors differ")
    _parse_timestamp(receipt.get("created_at"), "receipt created_at")
    outcome = _derive_outcome(spec, output_root, write_projection=False)
    expected = _receipt_payload(
        spec, output_root, created_at=receipt["created_at"], outcome=outcome
    )
    if receipt != expected:
        raise OneTurnCanaryError("one-turn receipt drifted")
    return receipt


def _reject_external_auth_material() -> None:
    present = sorted(name for name in _FORBIDDEN_AUTH_ENVIRONMENT if os.environ.get(name))
    if present:
        raise OneTurnCanaryError(
            "managed ChatGPT execution rejects API-key/raw-session auth: "
            + ", ".join(present)
        )


async def execute(
    spec: OneTurnCanarySpec,
    *,
    root: Path | None,
    operator_authorization_id: str,
) -> dict[str, Any]:
    _reject_external_auth_material()
    output_root = (root or spec.default_root).expanduser().resolve()
    paths = _paths(output_root)
    if paths["receipt"].is_file() or paths["terminal"].is_file():
        return write_terminal(spec, output_root)
    verify_runtime(spec, output_root)
    authorization = verify_authorization(
        spec,
        root=output_root,
        expected_authorization_id=operator_authorization_id,
        require_current=True,
    )
    contract = _load(paths["contract"], "one-turn runtime contract")
    request = _load(paths["request"], "one-turn prepared request")
    if paths["attempt"].exists():
        return write_terminal(spec, output_root)
    paths["attempt"].parent.mkdir(parents=True, exist_ok=True)
    _write_json(paths["attempt"], _attempt_payload(spec, output_root, authorization))
    trusted_factory = spec.adapter._client_factory  # noqa: SLF001
    try:
        async with trusted_factory() as client:
            account = getattr(client, "account_summary", None)
            if (
                not isinstance(account, Mapping)
                or account.get("type") != "chatgpt"
                or account.get("plan_type") != "pro"
            ):
                raise OneTurnCanaryError("one-turn canary requires managed ChatGPT Pro auth")
            await capacity._probe_and_publish(  # noqa: SLF001
                client=client,
                bundle_root=paths["initial_capacity"],
                request=_capacity_request(
                    spec,
                    output_root,
                    contract,
                    authorization,
                    boundary="initial_before_thread",
                ),
                contract=contract,
                authorization=authorization,
            )
            thread = await client.start_thread(
                model=spec.model,
                base_instructions=request["base_instructions"],
                cwd=spec.project_root,
                ephemeral=True,
            )
            _write_json(
                paths["thread"],
                _thread_payload(spec, thread, root=output_root, request=request),
            )
            await capacity._probe_and_publish(  # noqa: SLF001
                client=client,
                bundle_root=paths["preturn_capacity"],
                request=_capacity_request(
                    spec,
                    output_root,
                    contract,
                    authorization,
                    boundary="preturn_before_turn",
                ),
                contract=contract,
                authorization=authorization,
            )
            _write_json(
                paths["dispatch"],
                {
                    "schema_version": DISPATCH_VERSION,
                    "state": "semantic_turn_dispatch_committed",
                    "turn_name": spec.turn_name,
                    "request": _record(paths["request"]),
                    "runtime_lock": _record(paths["lock"]),
                    "operator_authorization": _record(paths["authorization"]),
                    "thread": _record(paths["thread"]),
                    "initial_capacity": _capacity_records(paths, "initial_capacity"),
                    "preturn_capacity": _capacity_records(paths, "preturn_capacity"),
                    "semantic_retry_count": 0,
                },
            )
            result = await client.run_structured_turn(
                thread=thread,
                effort=spec.effort,
                prompt=request["prompt"],
                output_schema=request["output_schema"],
                sidecar_path=paths["sidecar"],
                output_path=paths["output"],
                batch_size=request["effective_batch_size"],
                thread_mode="new_thread",
                timeout_seconds=float(spec.maximum_wall_seconds),
            )
            if result.status_ok is not True:
                raise OneTurnCanaryWaiting("semantic turn did not complete")
            telemetry = spec.adapter.validate_turn_sidecar(
                request,
                paths["sidecar"],
                output_path=paths["output"],
                expected_thread=thread,
            )
            raw_output = _load(paths["output"], "one-turn raw output")
            if (
                result.thread_id != telemetry["thread_id"]
                or result.turn_id != telemetry["turn_id"]
                or not isinstance(result.output, Mapping)
                or _canonical_json(result.output) != _canonical_json(raw_output)
            ):
                raise OneTurnCanaryError("in-memory one-turn result lineage drifted")
    except (
        capacity.CanonicalV31Epoch7AuthorityWaiting,
        OneTurnCanaryWaiting,
        codex_app_server.AppServerError,
        asyncio.TimeoutError,
    ):
        return write_terminal(spec, output_root)
    return write_terminal(spec, output_root)


def status(spec: OneTurnCanarySpec, root: Path | None = None) -> dict[str, Any]:
    output_root = (root or spec.default_root).expanduser().resolve()
    paths = _paths(output_root)
    if paths["receipt"].is_file() or paths["terminal"].is_file():
        receipt = write_terminal(spec, output_root)
        return {
            "schema_version": "pif_one_turn_canary_status_v1",
            "state": receipt["state"],
            "reason": receipt["terminal_reason"],
            "semantic_model_call_count": receipt["new_semantic_model_call_count"],
        }
    runtime = verify_runtime(spec, output_root)
    return {
        "schema_version": "pif_one_turn_canary_status_v1",
        "state": "authorized" if paths["authorization"].is_file() else "prepared",
        "reason": "ready_for_exactly_one_managed_semantic_turn",
        "semantic_model_call_count": int(paths["dispatch"].is_file()),
        "runtime_lock": runtime["runtime_lock"],
    }


__all__: Sequence[str] = (
    "OneTurnCanaryError",
    "OneTurnCanarySpec",
    "OneTurnCanaryWaiting",
    "authorize",
    "execute",
    "prepare",
    "status",
    "verify_authorization",
    "verify_receipt",
    "verify_runtime",
    "write_terminal",
)
