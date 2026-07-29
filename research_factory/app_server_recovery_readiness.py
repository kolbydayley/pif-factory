from __future__ import annotations

"""Precommitted provider cooldown and managed-auth launch readiness for pipeline-v4."""

import asyncio
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from .app_server_capacity import parse_rate_limit_snapshot
from .codex_app_server import CodexAppServerClient
from .util import write_text_atomic


READINESS_SPEC_VERSION = "pif_app_server_recovery_readiness_spec_v1"
READINESS_PROBE_VERSION = "pif_app_server_recovery_readiness_probe_v1"
READINESS_TERMINAL_VERSION = "pif_app_server_recovery_readiness_terminal_v1"
READINESS_FAILURE_VERSION = "pif_app_server_recovery_readiness_failure_v1"
OVERLOAD_COOLDOWN_SECONDS = 30 * 60
LAUNCH_MAXIMUM_PRIMARY_USED_PERCENT = 5
SEMANTIC_MAXIMUM_PRIMARY_USED_PERCENT = 20
REQUIRED_CONSECUTIVE_CLEAR_PROBES = 2
MINIMUM_CLEAR_PROBE_INTERVAL_SECONDS = 60
BLOCKED_RECHECK_SECONDS = 300


class RecoveryReadinessError(RuntimeError):
    """The v4 provider-readiness boundary is malformed or failed closed."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path, *, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RecoveryReadinessError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise RecoveryReadinessError(f"{purpose} is not an object")
    return value


def _write_immutable_json(path: Path, value: dict[str, Any]) -> None:
    rendered = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != rendered:
            raise RecoveryReadinessError("immutable readiness artifact changed")
        return
    write_text_atomic(path, rendered)


def _iso_from_epoch(value: float) -> str:
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


def _epoch_from_iso(value: Any) -> float:
    if not isinstance(value, str):
        raise RecoveryReadinessError("overload completion timestamp is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RecoveryReadinessError("overload completion timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise RecoveryReadinessError("overload completion timestamp has no timezone")
    return parsed.timestamp()


def freeze_recovery_readiness_spec(
    *, reuse_contract_path: Path, output_dir: Path
) -> dict[str, Any]:
    from .app_server_v2_reuse import verify_v4_reuse_contract

    contract_path = reuse_contract_path.expanduser().resolve()
    contract = verify_v4_reuse_contract(contract_path)
    incident = contract.get("v3_failed_sharded_calibration_incident") or {}
    if (
        incident.get("classification")
        != "infrastructure_or_judge_attempt_failed"
        or incident.get("provider_error_code") != "serverOverloaded"
        or incident.get("usage_status") != "unknown"
        or incident.get("retry_allowed") is not False
    ):
        raise RecoveryReadinessError("pipeline-v3 overload incident contract drifted")
    overload_finished_epoch = _epoch_from_iso(incident.get("finished_at"))
    not_before_epoch = int(overload_finished_epoch + OVERLOAD_COOLDOWN_SECONDS)
    expected = {
        "schema_version": READINESS_SPEC_VERSION,
        "state": "frozen_before_readiness_probes_or_semantic_calls",
        "reuse_contract_path": str(contract_path),
        "reuse_contract_sha256": _sha256_file(contract_path),
        "source_pipeline_version": "pipeline-v3",
        "target_pipeline_version": "pipeline-v4",
        "provider_failure_classification": "external_infrastructure_overload",
        "provider_error_code": "serverOverloaded",
        "overload_finished_at": incident["finished_at"],
        "overload_cooldown_seconds": OVERLOAD_COOLDOWN_SECONDS,
        "semantic_launch_not_before_epoch": not_before_epoch,
        "semantic_launch_not_before": _iso_from_epoch(not_before_epoch),
        "launch_maximum_primary_used_percent": LAUNCH_MAXIMUM_PRIMARY_USED_PERCENT,
        "semantic_turn_maximum_primary_used_percent": SEMANTIC_MAXIMUM_PRIMARY_USED_PERCENT,
        "required_consecutive_clear_probes": REQUIRED_CONSECUTIVE_CLEAR_PROBES,
        "minimum_clear_probe_interval_seconds": MINIMUM_CLEAR_PROBE_INTERVAL_SECONDS,
        "blocked_recheck_seconds": BLOCKED_RECHECK_SECONDS,
        "managed_chatgpt_auth_only": True,
        "official_codex_app_server_only": True,
        "thread_or_turn_allowed_during_readiness": False,
        "prior_attempt_retry_allowed": False,
        "production_mutation_allowed": False,
        "privacy": "hashes_capacity_percent_reset_status_and_failure_code_only",
    }
    root = output_dir.expanduser().resolve()
    spec_path = root / "readiness-spec.json"
    if spec_path.exists():
        prior = _read_json(spec_path, purpose="readiness specification")
        if prior != expected:
            raise RecoveryReadinessError("frozen readiness specification drifted")
        return prior
    root.mkdir(parents=True, exist_ok=True)
    _write_immutable_json(spec_path, expected)
    return expected


def verify_recovery_readiness(
    *, reuse_contract_path: Path, output_dir: Path
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    spec = freeze_recovery_readiness_spec(
        reuse_contract_path=reuse_contract_path, output_dir=root
    )
    terminal_path = root / "readiness-terminal.json"
    terminal = _read_json(terminal_path, purpose="readiness terminal")
    if (
        terminal.get("schema_version") != READINESS_TERMINAL_VERSION
        or terminal.get("status") != "ready"
        or terminal.get("readiness_spec_sha256")
        != _sha256_file(root / "readiness-spec.json")
        or terminal.get("managed_chatgpt_auth_verified") is not True
        or terminal.get("launch_maximum_primary_used_percent")
        != LAUNCH_MAXIMUM_PRIMARY_USED_PERCENT
        or terminal.get("final_primary_used_percent", 101)
        > LAUNCH_MAXIMUM_PRIMARY_USED_PERCENT
        or terminal.get("rate_limit_reached_type") is not None
        or terminal.get("required_consecutive_clear_probes")
        != REQUIRED_CONSECUTIVE_CLEAR_PROBES
        or terminal.get("minimum_clear_probe_interval_seconds")
        != MINIMUM_CLEAR_PROBE_INTERVAL_SECONDS
        or terminal.get("cooldown_satisfied") is not True
        or terminal.get("thread_started") is not False
        or terminal.get("turn_started") is not False
        or terminal.get("semantic_turns_started") != 0
        or terminal.get("production_mutation_performed") is not False
    ):
        raise RecoveryReadinessError("readiness terminal contract drifted")
    if float(terminal.get("ready_epoch", 0)) < float(
        spec["semantic_launch_not_before_epoch"]
    ):
        raise RecoveryReadinessError("readiness terminal predates overload cooldown")
    probes = terminal.get("qualifying_probes")
    if not isinstance(probes, list) or len(probes) != REQUIRED_CONSECUTIVE_CLEAR_PROBES:
        raise RecoveryReadinessError("readiness terminal probe coverage drifted")
    observed_epochs = []
    for record in probes:
        if not isinstance(record, dict):
            raise RecoveryReadinessError("readiness probe reference is malformed")
        path = Path(str(record.get("path") or "")).expanduser().resolve()
        if not path.is_file() or _sha256_file(path) != record.get("sha256"):
            raise RecoveryReadinessError("readiness probe artifact drifted")
        payload = _read_json(path, purpose="qualifying readiness probe")
        if (
            payload.get("schema_version") != READINESS_PROBE_VERSION
            or payload.get("cleared_for_semantic_work") is not True
            or payload.get("managed_chatgpt_auth_verified") is not True
            or payload.get("primary_used_percent", 101)
            > LAUNCH_MAXIMUM_PRIMARY_USED_PERCENT
            or payload.get("rate_limit_reached_type") is not None
            or payload.get("thread_started") is not False
            or payload.get("turn_started") is not False
        ):
            raise RecoveryReadinessError("qualifying readiness probe is unsafe")
        observed_epochs.append(float(payload["observed_epoch"]))
    if observed_epochs[-1] - observed_epochs[0] < MINIMUM_CLEAR_PROBE_INTERVAL_SECONDS:
        raise RecoveryReadinessError("qualifying readiness probes are too close together")
    return terminal


async def _sleep_with_stop(
    seconds: float,
    *,
    sleep: Callable[[float], Awaitable[None]],
    stop_check: Callable[[], None],
) -> None:
    remaining = max(0.0, float(seconds))
    while remaining > 0:
        stop_check()
        step = min(5.0, remaining)
        await sleep(step)
        remaining -= step
    stop_check()


async def wait_for_recovery_readiness(
    *,
    reuse_contract_path: Path,
    output_dir: Path,
    client_factory: Callable[[], Any] = CodexAppServerClient,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    clock: Callable[[], float] = time.time,
    stop_check: Callable[[], None] = lambda: None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    spec = freeze_recovery_readiness_spec(
        reuse_contract_path=reuse_contract_path, output_dir=root
    )
    terminal_path = root / "readiness-terminal.json"
    if terminal_path.exists():
        return verify_recovery_readiness(
            reuse_contract_path=reuse_contract_path, output_dir=root
        )
    failure_path = root / "readiness-failure.json"
    if failure_path.exists():
        raise RecoveryReadinessError("immutable readiness failure already exists")

    stop_check()
    remaining_cooldown = float(spec["semantic_launch_not_before_epoch"]) - clock()
    if remaining_cooldown > 0:
        await _sleep_with_stop(
            remaining_cooldown, sleep=sleep, stop_check=stop_check
        )

    probes_root = root / "probes"
    existing = sorted(probes_root.glob("probe-*.json")) if probes_root.exists() else []
    for path in existing:
        payload = _read_json(path, purpose="prior readiness probe")
        if (
            payload.get("schema_version") != READINESS_PROBE_VERSION
            or payload.get("thread_started") is not False
            or payload.get("turn_started") is not False
        ):
            raise RecoveryReadinessError("prior readiness probe drifted")
    probe_index = len(existing)
    clear_probes: list[dict[str, Any]] = []
    try:
        async with client_factory() as client:
            account = getattr(client, "account_summary", None)
            if not isinstance(account, dict) or account.get("type") != "chatgpt":
                raise RecoveryReadinessError("readiness requires managed ChatGPT auth")
            while True:
                stop_check()
                response = await client._request("account/rateLimits/read", {})  # noqa: SLF001
                snapshot = parse_rate_limit_snapshot(
                    response,
                    maximum_primary_used_percent=LAUNCH_MAXIMUM_PRIMARY_USED_PERCENT,
                )
                observed_epoch = float(clock())
                record = {
                    "schema_version": READINESS_PROBE_VERSION,
                    "probe_index": probe_index,
                    "observed_at": _iso_from_epoch(observed_epoch),
                    "observed_epoch": observed_epoch,
                    "managed_chatgpt_auth_verified": True,
                    "plan_type": account.get("plan_type"),
                    "limit_id": snapshot["limit_id"],
                    "primary_used_percent": snapshot["primary_used_percent"],
                    "primary_resets_at": snapshot["primary_resets_at"],
                    "rate_limit_reached_type": snapshot["rate_limit_reached_type"],
                    "maximum_primary_used_percent": LAUNCH_MAXIMUM_PRIMARY_USED_PERCENT,
                    "cleared_for_semantic_work": snapshot["cleared_for_semantic_turn"],
                    "thread_started": False,
                    "turn_started": False,
                    "production_mutation_performed": False,
                    "privacy": "managed_auth_plan_limit_percent_reset_and_status_no_credentials",
                }
                probe_path = probes_root / ("probe-%03d.json" % probe_index)
                _write_immutable_json(probe_path, record)
                probe_ref = {
                    "path": str(probe_path),
                    "sha256": _sha256_file(probe_path),
                    "observed_epoch": observed_epoch,
                }
                probe_index += 1
                if record["cleared_for_semantic_work"]:
                    if not clear_probes:
                        clear_probes = [probe_ref]
                    elif (
                        observed_epoch - clear_probes[-1]["observed_epoch"]
                        >= MINIMUM_CLEAR_PROBE_INTERVAL_SECONDS
                    ):
                        clear_probes.append(probe_ref)
                else:
                    clear_probes = []
                if len(clear_probes) >= REQUIRED_CONSECUTIVE_CLEAR_PROBES:
                    qualifying = clear_probes[-REQUIRED_CONSECUTIVE_CLEAR_PROBES:]
                    terminal = {
                        "schema_version": READINESS_TERMINAL_VERSION,
                        "status": "ready",
                        "ready_at": record["observed_at"],
                        "ready_epoch": observed_epoch,
                        "readiness_spec_sha256": _sha256_file(
                            root / "readiness-spec.json"
                        ),
                        "managed_chatgpt_auth_verified": True,
                        "plan_type": account.get("plan_type"),
                        "launch_maximum_primary_used_percent": LAUNCH_MAXIMUM_PRIMARY_USED_PERCENT,
                        "semantic_turn_maximum_primary_used_percent": SEMANTIC_MAXIMUM_PRIMARY_USED_PERCENT,
                        "final_primary_used_percent": record["primary_used_percent"],
                        "primary_resets_at": record["primary_resets_at"],
                        "rate_limit_reached_type": None,
                        "required_consecutive_clear_probes": REQUIRED_CONSECUTIVE_CLEAR_PROBES,
                        "minimum_clear_probe_interval_seconds": MINIMUM_CLEAR_PROBE_INTERVAL_SECONDS,
                        "qualifying_probes": qualifying,
                        "total_probe_count": probe_index,
                        "cooldown_satisfied": observed_epoch
                        >= float(spec["semantic_launch_not_before_epoch"]),
                        "thread_started": False,
                        "turn_started": False,
                        "semantic_turns_started": 0,
                        "production_mutation_performed": False,
                        "privacy": "hashes_capacity_percent_reset_status_and_no_credentials",
                    }
                    _write_immutable_json(terminal_path, terminal)
                    return verify_recovery_readiness(
                        reuse_contract_path=reuse_contract_path, output_dir=root
                    )
                wait_seconds = (
                    MINIMUM_CLEAR_PROBE_INTERVAL_SECONDS
                    if record["cleared_for_semantic_work"]
                    else BLOCKED_RECHECK_SECONDS
                )
                await _sleep_with_stop(
                    wait_seconds, sleep=sleep, stop_check=stop_check
                )
    except Exception as exc:
        if exc.__class__.__name__ == "PipelineStopped":
            raise
        failure = {
            "schema_version": READINESS_FAILURE_VERSION,
            "status": "blocked",
            "classification": "infrastructure_or_auth_readiness_failed",
            "error_class": type(exc).__name__,
            "usage_status": "not_applicable_no_semantic_turn",
            "thread_started": False,
            "turn_started": False,
            "semantic_turns_started": 0,
            "retry_allowed_within_pipeline_v4": False,
            "production_mutation_performed": False,
            "privacy": "error_class_and_status_only_no_error_text_or_credentials",
        }
        _write_immutable_json(failure_path, failure)
        raise RecoveryReadinessError("provider readiness failed closed") from exc
