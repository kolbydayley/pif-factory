from __future__ import annotations

"""Four-turn LLM-only quality runtime for the canonical v3.1 matrix.

This module is intentionally separate from extraction.  It consumes one passed
epoch-7 controller handoff, freezes a blinded all-system witness pool, and runs
exactly four managed app-server turns in one invocation:

1. pointwise source support, AB presentation;
2. pointwise source support, BA presentation;
3. full-field alignment, AB presentation; and
4. full-field alignment, BA presentation.

The epoch-7 controller independently verifies every turn, reconciles only exact
AB/BA agreement, recomputes the shared-reference score, and applies the frozen
historical production-cost denominator.  This runtime never authorizes holdout
or production work.
"""

import argparse
import asyncio
import copy
import fcntl
import functools
import hashlib
import inspect
import json
import math
import os
import stat
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Sequence

from . import app_server_canonical_v31_development_matrix_runtime as extraction_runtime
from . import app_server_canonical_v31_episode_batch as adapter
from . import app_server_canonical_v31_epoch7_controller as controller
from . import app_server_thread_supervisor as supervisor
from . import codex_app_server


PROJECT_ROOT = Path(__file__).resolve().parents[1]

QUALITY_RUNTIME_VERSION = "pif_canonical_v31_four_turn_quality_runtime_v1"
QUALITY_DIRECTIVE_VERSION = "pif_canonical_v31_quality_directive_v1"
QUALITY_PLAN_VERSION = "pif_canonical_v31_quality_plan_v1"
QUALITY_RUNTIME_LOCK_VERSION = "pif_canonical_v31_quality_runtime_lock_v1"
QUALITY_SOURCE_PACKET_VERSION = "pif_canonical_v31_quality_source_packet_v1"
QUALITY_DISPATCH_MARKER_VERSION = "pif_canonical_v31_quality_dispatch_marker_v1"
QUALITY_TURN_TERMINAL_VERSION = "pif_canonical_v31_quality_turn_terminal_v1"
QUALITY_WAITING_VERSION = "pif_canonical_v31_quality_waiting_receipt_v1"
QUALITY_OFFLINE_RECEIPT_VERSION = "pif_canonical_v31_quality_offline_receipt_v1"
QUALITY_STATUS_VERSION = "pif_canonical_v31_quality_status_v1"

DIRECTIVE_FILENAME = "quality-runtime-directive.json"
PLAN_FILENAME = "quality-runtime-plan.json"
RUNTIME_LOCK_FILENAME = "quality-runtime-lock.json"
SOURCE_PACKET_FILENAME = "quality-source-packet.private.json"
WAITING_FILENAME = "quality-runtime-waiting.json"
OFFLINE_RECEIPT_FILENAME = "quality-runtime-offline-receipt.json"
WRITER_LOCK_FILENAME = ".quality-runtime-writer.lock"

STEP_ID = "canonical_v31_four_turn_full_event_quality"
BASELINE_SYSTEM_ID = "canonical-v31-frozen-baseline"
MODEL = "gpt-5.5"
EFFORT = "high"
EXACT_TURN_COUNT = 4
JUDGE_TOTAL_TOKEN_CEILING = 400_000
MAXIMUM_TURN_TOKEN_CEILING = 100_000
OFFLINE_TEST_MODE = "offline_fixture_non_promotable"
OFFICIAL_CLIENT_FACTORY = adapter._client_factory

_TURN_ORDER = tuple(controller._QUALITY_TURN_ORDER)
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


class CanonicalV31QualityRuntimeError(RuntimeError):
    """Quality authority, lineage, accounting, or recovery failed closed."""


class CanonicalV31QualityWaiting(CanonicalV31QualityRuntimeError):
    """The frozen quality attempt cannot safely make another semantic call."""


class CanonicalV31QualityLockUnavailable(CanonicalV31QualityRuntimeError):
    """Another process owns the quality output root."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _is_sha256(value: Any) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _safe_path(path: Path, *, project_root: Path, label: str) -> Path:
    try:
        return controller._safe_path(path, project_root=project_root, label=label)
    except controller.Epoch7ControllerError as exc:
        raise CanonicalV31QualityRuntimeError(str(exc)) from exc


@functools.lru_cache(maxsize=256)
def _cached_record(
    path_value: str,
    device: int,
    inode: int,
    size: int,
    mtime_ns: int,
) -> dict[str, Any]:
    del device, inode, size, mtime_ns
    path = Path(path_value)
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": str(path), "sha256": digest.hexdigest(), "size": path.stat().st_size}


def _record(path: Path, *, allowed_root: Path | None = None) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if allowed_root is not None:
        try:
            resolved.relative_to(allowed_root.expanduser().resolve())
        except ValueError as exc:
            raise CanonicalV31QualityRuntimeError(
                "artifact record escaped its allowed root"
            ) from exc
    try:
        info = resolved.stat()
    except OSError as exc:
        raise CanonicalV31QualityRuntimeError(
            f"required artifact is unavailable: {resolved}"
        ) from exc
    if not stat.S_ISREG(info.st_mode) or resolved.is_symlink():
        raise CanonicalV31QualityRuntimeError(
            f"required artifact is not a real file: {resolved}"
        )
    return copy.deepcopy(
        _cached_record(
            str(resolved),
            info.st_dev,
            info.st_ino,
            info.st_size,
            info.st_mtime_ns,
        )
    )


def _verify_record(
    value: Any,
    *,
    label: str,
    allowed_root: Path | None = None,
) -> Path:
    if not isinstance(value, Mapping) or set(value) != {"path", "sha256", "size"}:
        raise CanonicalV31QualityRuntimeError(f"{label} record is malformed")
    path_value = value.get("path")
    size = value.get("size")
    if (
        not isinstance(path_value, str)
        or not _is_sha256(value.get("sha256"))
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        raise CanonicalV31QualityRuntimeError(f"{label} record fields are malformed")
    path = Path(path_value).expanduser().resolve()
    if _record(path, allowed_root=allowed_root) != dict(value):
        raise CanonicalV31QualityRuntimeError(f"{label} record drifted")
    return path


def _load_json(path: Path, *, label: str) -> Any:
    try:
        return json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalV31QualityRuntimeError(f"{label} is malformed") from exc


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    value = _load_json(path, label=label)
    if not isinstance(value, dict):
        raise CanonicalV31QualityRuntimeError(f"{label} is not an object")
    return value


def _write_immutable_bytes(path: Path, value: bytes) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.is_symlink() or path.read_bytes() != value:
            raise CanonicalV31QualityRuntimeError(f"immutable artifact drifted: {path}")
        return _record(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    _cached_record.cache_clear()
    return _record(path)


def _write_immutable_json(path: Path, value: Any) -> dict[str, Any]:
    return _write_immutable_bytes(path, _canonical_json(value).encode("ascii"))


def _parse_timestamp(value: Any, *, label: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise CanonicalV31QualityRuntimeError(f"{label} is absent")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CanonicalV31QualityRuntimeError(f"{label} is malformed") from exc
    if parsed.tzinfo is None:
        raise CanonicalV31QualityRuntimeError(f"{label} is not timezone-aware")
    return parsed.astimezone(timezone.utc)


def _authorization_id(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not 8 <= len(value) <= 200
        or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:" for character in value)
    ):
        raise CanonicalV31QualityRuntimeError("operator authorization ID is malformed")
    return value


def _usage(value: Any, *, label: str) -> dict[str, int]:
    try:
        return controller._usage(value, label=label)
    except controller.Epoch7ControllerError as exc:
        raise CanonicalV31QualityRuntimeError(str(exc)) from exc


def _sum_usage(values: Sequence[Mapping[str, int]]) -> dict[str, int]:
    return controller._sum_usage(values)


def _receipt_checksum(payload: Mapping[str, Any]) -> str:
    value = dict(payload)
    value.pop("receipt_sha256", None)
    return _sha256_bytes(_canonical_json(value).encode("ascii"))


def _write_receipt(path: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(payload))
    value["receipt_sha256"] = _receipt_checksum(value)
    _write_immutable_json(path, value)
    return value


def _callable_binding(value: Any, label: str) -> dict[str, Any]:
    target = value.__func__ if inspect.ismethod(value) else value
    target = inspect.unwrap(target)
    source_path = inspect.getsourcefile(target)
    if not isinstance(source_path, str):
        raise CanonicalV31QualityRuntimeError(f"{label} is not source-backed")
    try:
        segment = inspect.getsource(target)
    except (OSError, TypeError) as exc:
        raise CanonicalV31QualityRuntimeError(f"{label} source is unreadable") from exc
    payload = {
        "label": label,
        "module": getattr(target, "__module__", None),
        "qualname": getattr(target, "__qualname__", None),
        "source": _record(Path(source_path)),
        "source_segment_sha256": _sha256_bytes(segment.encode("utf-8")),
    }
    return {
        "binding": payload,
        "binding_sha256": _sha256_bytes(_canonical_json(payload).encode("ascii")),
    }


def build_quality_runtime_binding() -> dict[str, Any]:
    dependencies = {
        "quality_runtime": _record(Path(__file__)),
        "epoch7_controller": _record(Path(controller.__file__)),
        "extraction_runtime": _record(Path(extraction_runtime.__file__)),
        "canonical_adapter": _record(Path(adapter.__file__)),
        "codex_app_server_client": _record(Path(codex_app_server.__file__)),
        "protocol_schema": _record(codex_app_server.PROTOCOL_SCHEMA_PATH),
        "pinned_codex_binary": _record(adapter.PINNED_CODEX),
    }
    contract = controller.quality_runtime_evidence_contract()
    payload = {
        "schema_version": QUALITY_RUNTIME_VERSION,
        "dependencies": dependencies,
        "quality_evidence_contract_sha256": contract["contract_sha256"],
        "managed_client_factory": _callable_binding(
            OFFICIAL_CLIENT_FACTORY, "canonical managed app-server client factory"
        ),
        "rate_limit_parser": _callable_binding(
            extraction_runtime.parse_trusted_rate_limit_capacity,
            "trusted official rate-limit parser",
        ),
        "context_control_overlay": adapter.verified_context_control_overlay(),
        "context_control_overlay_sha256": _sha256_bytes(
            _canonical_json(adapter.verified_context_control_overlay()).encode("ascii")
        ),
        "instruction_source_contract": adapter.expected_instruction_source_contract(),
        "official_persistent_codex_app_server_only": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "semantic_retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    return {
        "binding": payload,
        "binding_sha256": _sha256_bytes(_canonical_json(payload).encode("ascii")),
    }


def _load_extraction_outputs(loaded: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    outputs: dict[str, list[dict[str, Any]]] = {}
    arms_root = Path(loaded["extraction_root"]) / "arms"
    for arm in loaded["precommit"]["precommit"]["arms"]:
        variant_id = str(arm["variant_id"])
        arm_dir = arms_root / variant_id
        report = extraction_runtime._load_object(
            arm_dir / "report.json", f"quality source {variant_id} report"
        )
        requests = loaded["preflight"]["requests_by_variant"].get(variant_id)
        if not isinstance(requests, list):
            raise CanonicalV31QualityRuntimeError(
                f"quality source requests are absent for {variant_id}"
            )
        validated = extraction_runtime.load_full_canonical_arm_outputs(
            arm_dir=arm_dir,
            report=report,
            requests=requests,
            request_sha256s=arm["request_sha256s"],
            manifest_info=loaded["preflight"]["manifest_info"],
        )
        outputs[variant_id] = copy.deepcopy(validated["outputs"])
    controller._verify_extraction_receipt(loaded)
    return outputs


def _event_exact(event: Mapping[str, Any], source: str) -> bool:
    text = event.get("evidence")
    start = event.get("evidence_start")
    end = event.get("evidence_end")
    return bool(
        isinstance(text, str)
        and text
        and isinstance(start, int)
        and not isinstance(start, bool)
        and isinstance(end, int)
        and not isinstance(end, bool)
        and 0 <= start < end <= len(source)
        and source[start:end] == text
    )


def _witness_id(
    *, case_id: str, system_position: int, event_index: int, event: Mapping[str, Any]
) -> str:
    identity = {
        "case_id": case_id,
        "system_position": system_position,
        "event_index": event_index,
        "event_sha256": _sha256_bytes(_canonical_json(event).encode("ascii")),
    }
    return "w_" + _sha256_bytes(_canonical_json(identity).encode("ascii"))[:28]


def build_quality_source_and_scoring(
    controller_root: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    """Reconstruct the baseline and every arm without semantic filtering."""

    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    handoff = controller.verify_quality_handoff(controller_root, project_root=project)
    loaded = controller.load_epoch7_controller(controller_root, project_root=project)
    outputs = _load_extraction_outputs(loaded)
    manifest_info = loaded["preflight"]["manifest_info"]
    case_order = list(manifest_info["opaque_case_order"])
    arm_ids = [str(row["variant_id"]) for row in loaded["precommit"]["precommit"]["arms"]]
    system_order = [BASELINE_SYSTEM_ID, *arm_ids]
    output_by_system_case: dict[str, dict[str, dict[str, Any]]] = {
        BASELINE_SYSTEM_ID: {
            case_id: copy.deepcopy(manifest_info["references_by_case"][case_id]["label"])
            for case_id in case_order
        }
    }
    for variant_id in arm_ids:
        rows = outputs[variant_id]
        if [row.get("opaque_case_id") for row in rows] != case_order:
            raise CanonicalV31QualityRuntimeError(
                f"quality source case order drifted for {variant_id}"
            )
        output_by_system_case[variant_id] = {
            str(row["opaque_case_id"]): copy.deepcopy(row["label"])
            for row in rows
        }

    witnesses: list[dict[str, Any]] = []
    prompt_cases: list[dict[str, Any]] = []
    source_by_witness: dict[str, str] = {}
    witness_ids: set[str] = set()
    for case_id in case_order:
        reference = manifest_info["references_by_case"][case_id]
        segment_id = str(reference["segment_id"])
        source_row = manifest_info["source_by_segment"].get(segment_id)
        source = source_row.get("segment_text") if isinstance(source_row, Mapping) else None
        if not isinstance(source, str) or not source:
            raise CanonicalV31QualityRuntimeError(
                f"quality source text is absent for case {case_id}"
            )
        blinded_rows: list[dict[str, Any]] = []
        for system_position, system_id in enumerate(system_order):
            label = output_by_system_case[system_id][case_id]
            events = label.get("discourse_events")
            if not isinstance(events, list):
                raise CanonicalV31QualityRuntimeError(
                    f"quality source events are malformed for {case_id}"
                )
            for event_index, event in enumerate(events):
                if not isinstance(event, Mapping) or not _event_exact(event, source):
                    raise CanonicalV31QualityRuntimeError(
                        f"quality source evidence is not exact for {case_id}"
                    )
                witness_id = _witness_id(
                    case_id=case_id,
                    system_position=system_position,
                    event_index=event_index,
                    event=event,
                )
                if witness_id in witness_ids:
                    raise CanonicalV31QualityRuntimeError("opaque witness ID collision")
                witness_ids.add(witness_id)
                witnesses.append(
                    {
                        "witness_id": witness_id,
                        "case_id": case_id,
                        "system_id": system_id,
                        "submitted_evidence_exact": True,
                    }
                )
                source_by_witness[witness_id] = source
                blinded_rows.append(
                    {
                        "witness_id": witness_id,
                        "event": copy.deepcopy(dict(event)),
                    }
                )
        prompt_cases.append(
            {
                "case_id": case_id,
                "source_excerpt": source,
                "witnesses": blinded_rows,
            }
        )
    if not witnesses:
        raise CanonicalV31QualityRuntimeError("quality source contains no event witnesses")
    observed_systems = {row["system_id"] for row in witnesses}
    observed_cases = {row["case_id"] for row in witnesses}
    if observed_systems != set(system_order) or observed_cases != set(case_order):
        raise CanonicalV31QualityRuntimeError(
            "quality source cannot score a system or case with no submitted witness"
        )
    fields = list(controller._TRUTH_CONDITIONAL_CHECKLIST_FIELDS)
    scoring = {
        "schema_version": controller.QUALITY_SCORING_MANIFEST_VERSION,
        "state": "frozen_before_quality_turns",
        "case_order": case_order,
        "system_order": system_order,
        "witnesses": witnesses,
        "one_shared_augmented_reference": True,
        "reference_system_ids": None,
        "semantic_fields": fields,
        "semantic_fields_sha256": _sha256_bytes(
            _canonical_json(fields).encode("ascii")
        ),
        "deterministic_semantic_matching": False,
        "semantic_pruning": False,
    }
    packet = {
        "schema_version": QUALITY_SOURCE_PACKET_VERSION,
        "state": "blinded_full_event_pool_frozen_before_quality_turns",
        "quality_handoff": handoff["handoff_record"],
        "case_order": case_order,
        "expected_witness_ids": [row["witness_id"] for row in witnesses],
        "source_by_witness": source_by_witness,
        "cases": prompt_cases,
        "system_origin_exposed_to_model": False,
        "every_baseline_and_arm_event_included": True,
        "semantic_prefiltering": False,
        "semantic_pruning": False,
    }
    return {
        "handoff": handoff,
        "loaded": loaded,
        "scoring_manifest": scoring,
        "source_packet": packet,
        "arm_ids": arm_ids,
    }


def _quality_limits(capacity_policy: Mapping[str, Any]) -> dict[str, Any]:
    required_integers = (
        "minimum_remaining_reserve_percent",
        "capacity_safety_margin_percent",
        "quota_points_per_million_tokens",
        "maximum_total_tokens_per_turn",
        "maximum_rate_limit_snapshot_age_seconds",
    )
    values: dict[str, int] = {}
    for field in required_integers:
        value = capacity_policy.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise CanonicalV31QualityRuntimeError(
                f"capacity policy {field} is not positive integer authority"
            )
        values[field] = value
    wall = capacity_policy.get("maximum_wall_seconds_per_turn")
    wall_safety = capacity_policy.get("operator_wall_deadline_safety_margin_seconds")
    if (
        isinstance(wall, bool)
        or not isinstance(wall, (int, float))
        or float(wall) <= 0
        or isinstance(wall_safety, bool)
        or not isinstance(wall_safety, (int, float))
        or float(wall_safety) <= 0
        or values["minimum_remaining_reserve_percent"]
        + values["capacity_safety_margin_percent"]
        >= 100
        or capacity_policy.get("managed_chatgpt_auth_only") is not True
        or capacity_policy.get("managed_chatgpt_plan_type") != "pro"
        or capacity_policy.get("official_persistent_codex_app_server_only") is not True
        or capacity_policy.get("token_capacity_is_estimate_not_reservation") is not True
        or capacity_policy.get("preturn_reprobe_required") is not True
        or capacity_policy.get("postturn_measured_stop_required") is not True
        or capacity_policy.get("semantic_retry_count") != 0
    ):
        raise CanonicalV31QualityRuntimeError("capacity policy contract drifted")
    per_turn = min(
        values["maximum_total_tokens_per_turn"], MAXIMUM_TURN_TOKEN_CEILING
    )
    return {
        "exact_model_call_cap": EXACT_TURN_COUNT,
        "maximum_total_tokens_per_turn": per_turn,
        "exact_total_token_cap": per_turn * EXACT_TURN_COUNT,
        "maximum_wall_seconds_per_turn": float(wall),
        "exact_total_wall_ceiling": float(wall) * EXACT_TURN_COUNT,
        "minimum_remaining_reserve_percent": values[
            "minimum_remaining_reserve_percent"
        ],
        "capacity_safety_margin_percent": values["capacity_safety_margin_percent"],
        "quota_points_per_million_tokens": values["quota_points_per_million_tokens"],
        "maximum_rate_limit_snapshot_age_seconds": values[
            "maximum_rate_limit_snapshot_age_seconds"
        ],
        "operator_wall_deadline_safety_margin_seconds": float(wall_safety),
        "quota_calibration_id": capacity_policy.get("quota_calibration_id"),
    }


def _directive_payload(
    *,
    controller_root: Path,
    quality_root: Path,
    authorization_id: str,
    issued_at: str,
    expires_at: str,
    handoff_record: Mapping[str, Any],
    capacity_policy_record: Mapping[str, Any],
    context_usage_recovery_record: Mapping[str, Any],
    runtime_binding: Mapping[str, Any],
    limits: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": QUALITY_DIRECTIVE_VERSION,
        "state": "frozen_zero_call_quality_authority",
        "thread_id": supervisor.TARGET_THREAD_ID,
        "plan_epoch": controller.PLAN_EPOCH,
        "step_id": STEP_ID,
        "authorized_by": "kolby",
        "operator_authorization_id": authorization_id,
        "issued_at": issued_at,
        "expires_at": expires_at,
        "controller_root": str(controller_root),
        "quality_root": str(quality_root),
        "quality_handoff": copy.deepcopy(dict(handoff_record)),
        "capacity_policy": copy.deepcopy(dict(capacity_policy_record)),
        "context_usage_recovery": copy.deepcopy(dict(context_usage_recovery_record)),
        "runtime_binding_sha256": runtime_binding["binding_sha256"],
        "quality_evidence_contract_sha256": controller.quality_runtime_evidence_contract()[
            "contract_sha256"
        ],
        "model": MODEL,
        "effort": EFFORT,
        "semantic_turn_order": [
            {"stage": stage, "orientation": orientation}
            for stage, orientation in _TURN_ORDER
        ],
        "exact_model_call_cap": EXACT_TURN_COUNT,
        "maximum_total_tokens_per_turn": limits["maximum_total_tokens_per_turn"],
        "exact_total_token_cap": limits["exact_total_token_cap"],
        "judge_total_token_ceiling": JUDGE_TOTAL_TOKEN_CEILING,
        "maximum_wall_seconds_per_turn": limits["maximum_wall_seconds_per_turn"],
        "exact_total_wall_ceiling": limits["exact_total_wall_ceiling"],
        "minimum_remaining_reserve_percent": limits[
            "minimum_remaining_reserve_percent"
        ],
        "capacity_safety_margin_percent": limits["capacity_safety_margin_percent"],
        "quota_points_per_million_tokens": limits["quota_points_per_million_tokens"],
        "maximum_rate_limit_snapshot_age_seconds": limits[
            "maximum_rate_limit_snapshot_age_seconds"
        ],
        "operator_wall_deadline_safety_margin_seconds": limits[
            "operator_wall_deadline_safety_margin_seconds"
        ],
        "quota_calibration_id": limits["quota_calibration_id"],
        "capacity_estimate_not_reservation": True,
        "postturn_measured_stop_required": True,
        "official_persistent_codex_app_server_only": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "caller_supplied_capacity_admission_allowed": False,
        "caller_supplied_client_allowed": False,
        "semantic_retry_count": 0,
        "deterministic_semantic_matching": False,
        "semantic_pruning": False,
        "semantic_relabeling": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _plan_payload(
    *,
    directive_record: Mapping[str, Any],
    quality_root: Path,
    scoring_record: Mapping[str, Any],
    source_record: Mapping[str, Any],
    cost_record: Mapping[str, Any],
    runtime_binding: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": QUALITY_PLAN_VERSION,
        "state": "planned_zero_call",
        "thread_id": supervisor.TARGET_THREAD_ID,
        "plan_epoch": controller.PLAN_EPOCH,
        "step_id": STEP_ID,
        "directive": copy.deepcopy(dict(directive_record)),
        "quality_root": str(quality_root),
        "quality_scoring_manifest": copy.deepcopy(dict(scoring_record)),
        "quality_source_packet": copy.deepcopy(dict(source_record)),
        "quality_cost_authority": copy.deepcopy(dict(cost_record)),
        "runtime_binding_sha256": runtime_binding["binding_sha256"],
        "expected_evidence_receipt_path": str(
            (quality_root / controller.QUALITY_EVIDENCE_RECEIPT_FILENAME).resolve()
        ),
        "expected_terminal_receipt_path": str(
            (quality_root / controller.QUALITY_TERMINAL_RECEIPT_FILENAME).resolve()
        ),
        "semantic_model_call_count": 0,
        "semantic_retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def freeze_quality_runtime_plan(
    *,
    controller_root: Path,
    quality_root: Path,
    context_usage_recovery_path: Path,
    operator_authorization_id: str,
    issued_at: str,
    expires_at: str,
    project_root: Path | None = None,
) -> dict[str, Any]:
    """Freeze all zero-call quality authority and deterministic inputs."""

    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    root = _safe_path(quality_root, project_root=project, label="quality root")
    controller_path = _safe_path(
        controller_root, project_root=project, label="controller root"
    )
    if root.exists() and (root.is_symlink() or not root.is_dir()):
        raise CanonicalV31QualityRuntimeError("quality root is not a real directory")
    root.mkdir(parents=True, exist_ok=True)
    authorization_id = _authorization_id(operator_authorization_id)
    issued = _parse_timestamp(issued_at, label="directive issued_at")
    expires = _parse_timestamp(expires_at, label="directive expires_at")
    if expires <= issued:
        raise CanonicalV31QualityRuntimeError("quality directive expiry is not after issue")

    source = build_quality_source_and_scoring(
        controller_path, project_root=project
    )
    source_record = _write_immutable_json(
        root / SOURCE_PACKET_FILENAME, source["source_packet"]
    )
    scoring_record = _write_immutable_json(
        root / controller.QUALITY_SCORING_MANIFEST_FILENAME,
        source["scoring_manifest"],
    )
    try:
        cost = controller.freeze_quality_cost_authority(
            root,
            context_usage_recovery_path=context_usage_recovery_path,
            project_root=project,
        )
    except controller.Epoch7ControllerError as exc:
        raise CanonicalV31QualityRuntimeError(str(exc)) from exc
    loaded_controller = source["loaded"]
    capacity_policy_path = loaded_controller["records"]["capacity_policy"]
    capacity_policy = _load_object(capacity_policy_path, label="capacity policy")
    limits = _quality_limits(capacity_policy)
    if limits["exact_total_token_cap"] > JUDGE_TOTAL_TOKEN_CEILING:
        raise CanonicalV31QualityRuntimeError("quality judge token cap exceeds 400k")
    runtime_binding = build_quality_runtime_binding()
    directive = _directive_payload(
        controller_root=controller_path,
        quality_root=root,
        authorization_id=authorization_id,
        issued_at=issued.isoformat(),
        expires_at=expires.isoformat(),
        handoff_record=source["handoff"]["handoff_record"],
        capacity_policy_record=_record(capacity_policy_path, allowed_root=project),
        context_usage_recovery_record=_record(
            context_usage_recovery_path, allowed_root=project
        ),
        runtime_binding=runtime_binding,
        limits=limits,
    )
    directive_record = _write_immutable_json(root / DIRECTIVE_FILENAME, directive)
    plan = _plan_payload(
        directive_record=directive_record,
        quality_root=root,
        scoring_record=scoring_record,
        source_record=source_record,
        cost_record=cost["record"],
        runtime_binding=runtime_binding,
    )
    plan_record = _write_immutable_json(root / PLAN_FILENAME, plan)
    lock = {
        "schema_version": QUALITY_RUNTIME_LOCK_VERSION,
        "state": "frozen_zero_call",
        "quality_runtime_binding": runtime_binding["binding"],
        "quality_runtime_binding_sha256": runtime_binding["binding_sha256"],
        "directive": directive_record,
        "plan": plan_record,
        "quality_handoff": source["handoff"]["handoff_record"],
        "quality_scoring_manifest": scoring_record,
        "quality_source_packet": source_record,
        "quality_cost_authority": cost["record"],
        "capacity_policy": _record(capacity_policy_path, allowed_root=project),
        "context_control_overlay_sha256": runtime_binding["binding"][
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract": runtime_binding["binding"][
            "instruction_source_contract"
        ],
        "exact_model_call_cap": EXACT_TURN_COUNT,
        "semantic_model_call_count": 0,
        "semantic_retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_immutable_json(root / RUNTIME_LOCK_FILENAME, lock)
    return load_quality_runtime_plan(root, project_root=project)


def _required_static_names() -> set[str]:
    return {
        DIRECTIVE_FILENAME,
        PLAN_FILENAME,
        RUNTIME_LOCK_FILENAME,
        SOURCE_PACKET_FILENAME,
        controller.QUALITY_SCORING_MANIFEST_FILENAME,
        controller.QUALITY_COST_AUTHORITY_FILENAME,
    }


def load_quality_runtime_plan(
    quality_root: Path,
    *,
    project_root: Path | None = None,
    require_fresh: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    project = (project_root or PROJECT_ROOT).expanduser().resolve()
    root = _safe_path(quality_root, project_root=project, label="quality root")
    if root.is_symlink() or not root.is_dir():
        raise CanonicalV31QualityRuntimeError("quality root is not a real directory")
    for name in _required_static_names():
        path = root / name
        if path.is_symlink() or not path.is_file():
            raise CanonicalV31QualityRuntimeError(f"quality static artifact is absent: {name}")
    directive = _load_object(root / DIRECTIVE_FILENAME, label="quality directive")
    plan = _load_object(root / PLAN_FILENAME, label="quality plan")
    lock = _load_object(root / RUNTIME_LOCK_FILENAME, label="quality runtime lock")
    if directive.get("schema_version") != QUALITY_DIRECTIVE_VERSION:
        raise CanonicalV31QualityRuntimeError("quality directive schema drifted")
    if plan.get("schema_version") != QUALITY_PLAN_VERSION:
        raise CanonicalV31QualityRuntimeError("quality plan schema drifted")
    if lock.get("schema_version") != QUALITY_RUNTIME_LOCK_VERSION:
        raise CanonicalV31QualityRuntimeError("quality runtime lock schema drifted")
    authorization_id = _authorization_id(directive.get("operator_authorization_id"))
    issued = _parse_timestamp(directive.get("issued_at"), label="directive issued_at")
    expires = _parse_timestamp(directive.get("expires_at"), label="directive expires_at")
    current = now or datetime.now(timezone.utc)
    if require_fresh and not issued <= current < expires:
        raise CanonicalV31QualityRuntimeError("quality directive is not currently fresh")
    controller_root = _safe_path(
        Path(str(directive.get("controller_root") or "")),
        project_root=project,
        label="controller root",
    )
    if directive.get("quality_root") != str(root):
        raise CanonicalV31QualityRuntimeError("quality directive root drifted")
    handoff = controller.verify_quality_handoff(controller_root, project_root=project)
    if directive.get("quality_handoff") != handoff["handoff_record"]:
        raise CanonicalV31QualityRuntimeError("quality handoff binding drifted")
    capacity_path = _verify_record(
        directive.get("capacity_policy"), label="capacity policy", allowed_root=project
    )
    capacity_policy = _load_object(capacity_path, label="capacity policy")
    limits = _quality_limits(capacity_policy)
    runtime_binding = build_quality_runtime_binding()
    if (
        directive.get("state") != "frozen_zero_call_quality_authority"
        or directive.get("thread_id") != supervisor.TARGET_THREAD_ID
        or directive.get("plan_epoch") != controller.PLAN_EPOCH
        or directive.get("step_id") != STEP_ID
        or directive.get("authorized_by") != "kolby"
        or directive.get("runtime_binding_sha256") != runtime_binding["binding_sha256"]
        or directive.get("quality_evidence_contract_sha256")
        != controller.quality_runtime_evidence_contract()["contract_sha256"]
        or directive.get("model") != MODEL
        or directive.get("effort") != EFFORT
        or directive.get("semantic_turn_order")
        != [{"stage": stage, "orientation": orientation} for stage, orientation in _TURN_ORDER]
        or directive.get("exact_model_call_cap") != EXACT_TURN_COUNT
        or directive.get("maximum_total_tokens_per_turn")
        != limits["maximum_total_tokens_per_turn"]
        or directive.get("exact_total_token_cap") != limits["exact_total_token_cap"]
        or directive.get("judge_total_token_ceiling") != JUDGE_TOTAL_TOKEN_CEILING
        or directive.get("semantic_retry_count") != 0
        or directive.get("managed_chatgpt_auth_only") is not True
        or directive.get("managed_chatgpt_plan_type") != "pro"
        or directive.get("official_persistent_codex_app_server_only") is not True
        or directive.get("caller_supplied_capacity_admission_allowed") is not False
        or directive.get("caller_supplied_client_allowed") is not False
        or directive.get("holdout_authorized") is not False
        or directive.get("production_mutation_allowed") is not False
    ):
        raise CanonicalV31QualityRuntimeError("quality directive contract drifted")

    source = build_quality_source_and_scoring(controller_root, project_root=project)
    source_path = root / SOURCE_PACKET_FILENAME
    scoring_path = root / controller.QUALITY_SCORING_MANIFEST_FILENAME
    if (
        _load_object(source_path, label="quality source packet") != source["source_packet"]
        or _load_object(scoring_path, label="quality scoring manifest")
        != source["scoring_manifest"]
    ):
        raise CanonicalV31QualityRuntimeError("quality source or scoring manifest drifted")
    cost = controller.verify_quality_cost_authority(root, project_root=project)
    context_recovery = _verify_record(
        directive.get("context_usage_recovery"),
        label="context usage recovery",
        allowed_root=project,
    )
    if Path(cost["context_usage_recovery_record"]["path"]).resolve() != context_recovery:
        raise CanonicalV31QualityRuntimeError("quality cost recovery binding drifted")
    directive_record = _record(root / DIRECTIVE_FILENAME, allowed_root=root)
    expected_plan = _plan_payload(
        directive_record=directive_record,
        quality_root=root,
        scoring_record=_record(scoring_path, allowed_root=root),
        source_record=_record(source_path, allowed_root=root),
        cost_record=cost["record"],
        runtime_binding=runtime_binding,
    )
    if plan != expected_plan:
        raise CanonicalV31QualityRuntimeError("quality plan drifted")
    expected_lock = {
        "schema_version": QUALITY_RUNTIME_LOCK_VERSION,
        "state": "frozen_zero_call",
        "quality_runtime_binding": runtime_binding["binding"],
        "quality_runtime_binding_sha256": runtime_binding["binding_sha256"],
        "directive": directive_record,
        "plan": _record(root / PLAN_FILENAME, allowed_root=root),
        "quality_handoff": handoff["handoff_record"],
        "quality_scoring_manifest": _record(scoring_path, allowed_root=root),
        "quality_source_packet": _record(source_path, allowed_root=root),
        "quality_cost_authority": cost["record"],
        "capacity_policy": _record(capacity_path, allowed_root=project),
        "context_control_overlay_sha256": runtime_binding["binding"][
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract": runtime_binding["binding"][
            "instruction_source_contract"
        ],
        "exact_model_call_cap": EXACT_TURN_COUNT,
        "semantic_model_call_count": 0,
        "semantic_retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    if lock != expected_lock:
        raise CanonicalV31QualityRuntimeError("quality runtime lock drifted")
    return {
        "project_root": project,
        "quality_root": root,
        "controller_root": controller_root,
        "authorization_id": authorization_id,
        "directive": directive,
        "directive_record": directive_record,
        "plan": plan,
        "plan_record": _record(root / PLAN_FILENAME, allowed_root=root),
        "runtime_lock": lock,
        "runtime_lock_record": _record(root / RUNTIME_LOCK_FILENAME, allowed_root=root),
        "runtime_binding": runtime_binding,
        "handoff": handoff,
        "source_packet": source["source_packet"],
        "source_record": _record(source_path, allowed_root=root),
        "scoring_manifest": source["scoring_manifest"],
        "scoring_record": _record(scoring_path, allowed_root=root),
        "cost": cost,
        "capacity_policy": capacity_policy,
        "limits": limits,
        "expires_at": expires,
    }


def _support_base_instructions() -> str:
    return (
        "You are a blinded, side-free source-support judge. Decide every opaque "
        "full event independently against only its case source. A supported event "
        "requires exact quoted evidence and material entailment of the event claim. "
        "Use unsupported when contradicted or not established, and abstain when the "
        "source is genuinely insufficient. Do not infer system origin, compare system "
        "quality, prune events, or rewrite any event. Return the exact structured array."
    )


def _alignment_base_instructions() -> str:
    fields = ", ".join(controller._TRUTH_CONDITIONAL_CHECKLIST_FIELDS)
    return (
        "You are a blinded, side-free full-event equivalence judge. The support "
        "verdicts are frozen. Decide every supplied unordered pair using the complete "
        "truth conditions of both events. Complete all 15 checklist rows in the fixed "
        f"order: {fields}. Equivalent requires every row same; not_equivalent requires "
        "at least one different row; abstain when the relation cannot be established. "
        "Do not infer origin, create mismatch fields, merge, prune, relabel, or rewrite "
        "events. Return the exact structured array."
    )


def _support_prompt(packet: Mapping[str, Any], orientation: str) -> str:
    cases = []
    for case in packet["cases"]:
        witnesses = []
        for index, witness in enumerate(case["witnesses"]):
            side = "A" if index % 2 == 0 else "B"
            if orientation == "ba":
                side = "B" if side == "A" else "A"
            witnesses.append({**copy.deepcopy(witness), "presentation_side": side})
        cases.append(
            {
                "case_id": case["case_id"],
                "source_excerpt": case["source_excerpt"],
                "witnesses": witnesses,
            }
        )
    payload = {
        "stage": "support_first",
        "orientation": orientation,
        "expected_witness_ids": packet["expected_witness_ids"],
        "cases": cases,
        "output_order": "exact expected_witness_ids order",
    }
    return "Judge this blinded support packet:\n" + _canonical_json(payload)


def _expected_alignment_pairs(
    *,
    scoring_manifest: Mapping[str, Any],
    support_verdicts: Mapping[str, str],
) -> list[dict[str, str]]:
    by_case: dict[str, list[str]] = {
        str(case_id): [] for case_id in scoring_manifest["case_order"]
    }
    for row in scoring_manifest["witnesses"]:
        witness_id = str(row["witness_id"])
        if support_verdicts.get(witness_id) == "supported":
            by_case[str(row["case_id"])].append(witness_id)
    pairs: list[dict[str, str]] = []
    for case_id in scoring_manifest["case_order"]:
        values = by_case[str(case_id)]
        for left_index, left in enumerate(values):
            for right in values[left_index + 1 :]:
                identity = {"case_id": case_id, "left": left, "right": right}
                pairs.append(
                    {
                        "pair_id": "p_"
                        + _sha256_bytes(_canonical_json(identity).encode("ascii"))[:28],
                        "left_witness_id": left,
                        "right_witness_id": right,
                    }
                )
    return pairs


def _alignment_prompt(
    packet: Mapping[str, Any],
    pairs: Sequence[Mapping[str, str]],
    support_verdicts: Mapping[str, str],
    orientation: str,
) -> str:
    events = {
        witness["witness_id"]: witness["event"]
        for case in packet["cases"]
        for witness in case["witnesses"]
        if support_verdicts.get(witness["witness_id"]) == "supported"
    }
    presented = []
    for pair in pairs:
        if orientation == "ab":
            left_id = pair["left_witness_id"]
            right_id = pair["right_witness_id"]
        else:
            left_id = pair["right_witness_id"]
            right_id = pair["left_witness_id"]
        presented.append(
            {
                "pair_id": pair["pair_id"],
                "canonical_left_witness_id": pair["left_witness_id"],
                "canonical_right_witness_id": pair["right_witness_id"],
                "presentation_left": {"witness_id": left_id, "event": events[left_id]},
                "presentation_right": {"witness_id": right_id, "event": events[right_id]},
            }
        )
    payload = {
        "stage": "alignment",
        "orientation": orientation,
        "frozen_support_verdicts": dict(support_verdicts),
        "pairs": presented,
        "output_identity": "always use canonical pair and witness IDs",
        "output_order": "exact pairs order",
    }
    return "Judge this blinded full-event alignment packet:\n" + _canonical_json(payload)


def _turn_paths(root: Path, stage: str, orientation: str) -> dict[str, Path]:
    turn_root = root / "turns" / f"{stage}-{orientation}"
    return {
        "root": turn_root,
        "capacity_request": turn_root / "capacity-request.json",
        "capacity_measurement": turn_root / "capacity-measurement.json",
        "capacity_admission": turn_root / "capacity-admission.json",
        "request": turn_root / "request.json",
        "prompt": turn_root / "prompt.private.md",
        "base_instructions": turn_root / "base-instructions.private.md",
        "output_schema": turn_root / "output-schema.json",
        "dispatch": turn_root / "dispatch-marker.json",
        "raw_output": turn_root / "output.private.json",
        "parsed_decisions": turn_root / "parsed-decisions.json",
        "sidecar": turn_root / "sidecar.json",
        "terminal": turn_root / "terminal.json",
    }


def _prepare_turn(
    loaded: Mapping[str, Any],
    *,
    stage: str,
    orientation: str,
    support_consensus_record: Mapping[str, Any] | None,
    support_verdicts: Mapping[str, str] | None,
) -> dict[str, Any]:
    paths = _turn_paths(loaded["quality_root"], stage, orientation)
    if stage == "support_first":
        base = _support_base_instructions()
        prompt = _support_prompt(loaded["source_packet"], orientation)
        schema = controller.pointwise_support_decision_schema()
        case_order_value: Any = loaded["source_packet"]["expected_witness_ids"]
        support_sha = None
    else:
        if support_consensus_record is None or support_verdicts is None:
            raise CanonicalV31QualityRuntimeError(
                "alignment preparation requires frozen support consensus"
            )
        pairs = _expected_alignment_pairs(
            scoring_manifest=loaded["scoring_manifest"],
            support_verdicts=support_verdicts,
        )
        base = _alignment_base_instructions()
        prompt = _alignment_prompt(
            loaded["source_packet"], pairs, support_verdicts, orientation
        )
        schema = controller.alignment_decision_schema()
        case_order_value = pairs
        support_sha = support_consensus_record["sha256"]
    prompt_record = _write_immutable_bytes(paths["prompt"], prompt.encode("utf-8"))
    base_record = _write_immutable_bytes(
        paths["base_instructions"], base.encode("utf-8")
    )
    schema_record = _write_immutable_json(paths["output_schema"], schema)
    request = {
        "schema_version": controller.QUALITY_TURN_REQUEST_VERSION,
        "stage": stage,
        "orientation": orientation,
        "quality_handoff": loaded["handoff"]["handoff_record"],
        "quality_runtime_evidence_contract_sha256": loaded["handoff"]["handoff"][
            "quality_runtime_evidence_contract_sha256"
        ],
        "quality_scoring_manifest": loaded["scoring_record"],
        "decision_case_order_sha256": _sha256_bytes(
            _canonical_json(case_order_value).encode("ascii")
        ),
        "frozen_support_consensus_sha256": support_sha,
        "prompt_sha256": prompt_record["sha256"],
        "base_instructions_sha256": base_record["sha256"],
        "output_schema_sha256": schema_record["sha256"],
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_retry_count": 0,
        "deterministic_semantic_matching": False,
        "semantic_pruning": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    request_record = _write_immutable_json(paths["request"], request)
    return {
        "paths": paths,
        "request": request,
        "request_record": request_record,
        "prompt": prompt,
        "base": base,
        "schema": schema,
        "case_order_value": case_order_value,
        "support_consensus_sha256": support_sha,
    }


async def _maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def _capacity_context(
    loaded: Mapping[str, Any],
    *,
    stage: str,
    orientation: str,
    sequence: int,
    used_calls: int,
    used_tokens: int,
) -> dict[str, Any]:
    remaining = EXACT_TURN_COUNT - used_calls
    return {
        "quality_runtime_lock_sha256": loaded["runtime_lock_record"]["sha256"],
        "stage": stage,
        "orientation": orientation,
        "sequence": sequence,
        "used_calls": used_calls,
        "used_total_tokens": used_tokens,
        "remaining_turn_count": remaining,
        "required_remaining_total_token_ceiling": remaining
        * loaded["limits"]["maximum_total_tokens_per_turn"],
        "required_remaining_wall_seconds_ceiling": remaining
        * loaded["limits"]["maximum_wall_seconds_per_turn"],
    }


def _capacity_records(
    loaded: Mapping[str, Any],
    *,
    response: Mapping[str, Any],
    context: Mapping[str, Any],
    measured_at: datetime,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    try:
        parsed = extraction_runtime.parse_trusted_rate_limit_capacity(response)
    except Exception as exc:
        raise CanonicalV31QualityWaiting("official capacity response is invalid") from exc
    limits = loaded["limits"]
    remaining_percent = int(parsed["minimum_applicable_remaining_percent"])
    usable_points = max(
        0,
        remaining_percent
        - int(limits["minimum_remaining_reserve_percent"])
        - int(limits["capacity_safety_margin_percent"]),
    )
    available_tokens = math.floor(
        usable_points * 1_000_000 / int(limits["quota_points_per_million_tokens"])
    )
    available_wall = max(
        0.0,
        (loaded["expires_at"] - measured_at).total_seconds()
        - float(limits["operator_wall_deadline_safety_margin_seconds"]),
    )
    required_tokens = int(context["required_remaining_total_token_ceiling"])
    required_wall = float(context["required_remaining_wall_seconds_ceiling"])
    cleared = bool(
        parsed["rate_limit_reached_type"] is None
        and available_tokens >= required_tokens
        and available_wall >= required_wall
    )
    request = {
        "schema_version": "pif_canonical_v31_quality_capacity_request_v1",
        "provider_method": "account/rateLimits/read",
        "context": copy.deepcopy(dict(context)),
        "official_app_server_initialized_before_capacity": True,
        "semantic_thread_started": False,
        "semantic_turn_started": False,
        "capacity_estimate_not_reservation": True,
    }
    measurement = {
        "schema_version": "pif_canonical_v31_quality_capacity_measurement_v1",
        "measurement_id": _sha256_bytes(
            _canonical_json(
                {
                    "measured_at": measured_at.isoformat(),
                    "provider_response_sha256": parsed["provider_response_sha256"],
                    "context": context,
                }
            ).encode("ascii")
        ),
        "measured_at": measured_at.isoformat(),
        "expires_at": min(
            loaded["expires_at"],
            measured_at
            + timedelta(seconds=limits["maximum_rate_limit_snapshot_age_seconds"]),
        ).isoformat(),
        "context": copy.deepcopy(dict(context)),
        "provider_capacity": parsed,
        "available_total_tokens": available_tokens,
        "available_wall_seconds": available_wall,
        "minimum_remaining_reserve_percent": limits[
            "minimum_remaining_reserve_percent"
        ],
        "capacity_safety_margin_percent": limits["capacity_safety_margin_percent"],
        "quota_points_per_million_tokens": limits["quota_points_per_million_tokens"],
        "rate_limit_reached_type": parsed["rate_limit_reached_type"],
        "managed_chatgpt_auth_verified": True,
        "plan_type": "pro",
        "capacity_available": cleared,
        "capacity_unknown": False,
        "capacity_estimate_not_reservation": True,
    }
    admission = {
        "schema_version": "pif_canonical_v31_quality_capacity_admission_v1",
        "measurement_id": measurement["measurement_id"],
        "quality_runtime_lock_sha256": loaded["runtime_lock_record"]["sha256"],
        "context": copy.deepcopy(dict(context)),
        "cleared_for_semantic_turn": cleared,
        "managed_chatgpt_auth_verified": True,
        "plan_type": "pro",
        "rate_limit_reached_type": parsed["rate_limit_reached_type"],
        "thread_started": False,
        "turn_started": False,
        "semantic_retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    return request, measurement, admission


async def _probe_capacity(
    loaded: Mapping[str, Any],
    *,
    client: Any,
    provider: Callable[[Mapping[str, Any]], Any] | None,
    offline_test_mode: bool,
    context: Mapping[str, Any],
    now: Callable[[], datetime],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if offline_test_mode:
        if not callable(provider):
            raise CanonicalV31QualityRuntimeError(
                "offline quality mode requires a capacity response provider"
            )
        response = await _maybe_await(provider(context))
    else:
        if provider is not None:
            raise CanonicalV31QualityRuntimeError(
                "live quality mode rejects a caller capacity provider"
            )
        response = await client._request("account/rateLimits/read", {})  # noqa: SLF001
    if not isinstance(response, Mapping):
        raise CanonicalV31QualityWaiting("capacity provider returned no object")
    measured_at = now().astimezone(timezone.utc)
    return _capacity_records(
        loaded,
        response=response,
        context=context,
        measured_at=measured_at,
    )


def _parsed_turn_payload(
    loaded: Mapping[str, Any],
    prepared: Mapping[str, Any],
    raw: Any,
) -> dict[str, Any]:
    stage = prepared["request"]["stage"]
    orientation = prepared["request"]["orientation"]
    if stage == "support_first":
        try:
            decisions = controller.validate_pointwise_support_decisions(
                raw,
                expected_witness_ids=loaded["source_packet"]["expected_witness_ids"],
                source_by_witness=loaded["source_packet"]["source_by_witness"],
            )
        except controller.Epoch7ControllerError as exc:
            raise CanonicalV31QualityWaiting(
                "pointwise support structured output is invalid"
            ) from exc
        return {
            "schema_version": controller.QUALITY_TURN_DECISIONS_VERSION,
            "stage": stage,
            "orientation": orientation,
            "case_order_sha256": prepared["request"]["decision_case_order_sha256"],
            "expected_witness_ids": loaded["source_packet"]["expected_witness_ids"],
            "source_by_witness": loaded["source_packet"]["source_by_witness"],
            "decisions": decisions,
        }
    support = _load_object(
        loaded["quality_root"] / "support-consensus.json",
        label="frozen support consensus",
    )
    verdicts = {row["witness_id"]: row["verdict"] for row in support["decisions"]}
    pairs = _expected_alignment_pairs(
        scoring_manifest=loaded["scoring_manifest"], support_verdicts=verdicts
    )
    try:
        decisions = controller.validate_alignment_decisions(
            raw, expected_pairs=pairs, frozen_support_verdicts=verdicts
        )
    except controller.Epoch7ControllerError as exc:
        raise CanonicalV31QualityWaiting(
            "alignment structured output is invalid"
        ) from exc
    return {
        "schema_version": controller.QUALITY_TURN_DECISIONS_VERSION,
        "stage": stage,
        "orientation": orientation,
        "case_order_sha256": prepared["request"]["decision_case_order_sha256"],
        "expected_pairs": pairs,
        "frozen_support_verdicts": verdicts,
        "support_consensus_sha256": prepared["support_consensus_sha256"],
        "decisions": decisions,
    }


def _turn_evidence_from_paths(
    loaded: Mapping[str, Any], stage: str, orientation: str
) -> dict[str, Any]:
    paths = _turn_paths(loaded["quality_root"], stage, orientation)
    terminal = _load_object(paths["terminal"], label="quality turn terminal")
    usage = _usage(terminal.get("usage"), label="quality turn terminal")
    wall = terminal.get("wall_elapsed_seconds")
    if isinstance(wall, bool) or not isinstance(wall, (int, float)) or wall < 0:
        raise CanonicalV31QualityRuntimeError("quality turn wall time is malformed")
    artifacts = {
        role: _record(paths[role], allowed_root=loaded["quality_root"])
        for role in controller._QUALITY_TURN_ARTIFACT_ROLES
    }
    return {
        "stage": stage,
        "orientation": orientation,
        "model": MODEL,
        "effort": EFFORT,
        "thread_id": terminal["thread_id"],
        "turn_id": terminal["turn_id"],
        "artifacts": artifacts,
        "usage": usage,
        "wall_elapsed_seconds": float(wall),
    }


def _freeze_support_consensus(loaded: Mapping[str, Any]) -> dict[str, Any]:
    values = []
    for orientation in ("ab", "ba"):
        values.append(
            _load_object(
                _turn_paths(loaded["quality_root"], "support_first", orientation)[
                    "parsed_decisions"
                ],
                label=f"support {orientation} decisions",
            )
        )
    decisions = controller.reconcile_balanced_support_decisions(
        values[0]["decisions"],
        values[1]["decisions"],
        expected_witness_ids=loaded["source_packet"]["expected_witness_ids"],
        source_by_witness=loaded["source_packet"]["source_by_witness"],
    )
    payload = {
        "schema_version": controller.QUALITY_SUPPORT_CONSENSUS_VERSION,
        "expected_witness_ids": loaded["source_packet"]["expected_witness_ids"],
        "source_by_witness_sha256": _sha256_bytes(
            _canonical_json(loaded["source_packet"]["source_by_witness"]).encode("ascii")
        ),
        "decisions": decisions,
    }
    record = _write_immutable_json(
        loaded["quality_root"] / "support-consensus.json", payload
    )
    return {"payload": payload, "record": record}


def _freeze_alignment_consensus(
    loaded: Mapping[str, Any], support: Mapping[str, Any]
) -> dict[str, Any]:
    values = []
    for orientation in ("ab", "ba"):
        values.append(
            _load_object(
                _turn_paths(loaded["quality_root"], "alignment", orientation)[
                    "parsed_decisions"
                ],
                label=f"alignment {orientation} decisions",
            )
        )
    verdicts = {
        row["witness_id"]: row["verdict"] for row in support["payload"]["decisions"]
    }
    pairs = _expected_alignment_pairs(
        scoring_manifest=loaded["scoring_manifest"], support_verdicts=verdicts
    )
    decisions = controller.reconcile_balanced_alignment_decisions(
        values[0]["decisions"],
        values[1]["decisions"],
        expected_pairs=pairs,
        frozen_support_verdicts=verdicts,
    )
    payload = {
        "schema_version": controller.QUALITY_ALIGNMENT_CONSENSUS_VERSION,
        "support_consensus_sha256": support["record"]["sha256"],
        "expected_pairs": pairs,
        "decisions": decisions,
    }
    record = _write_immutable_json(
        loaded["quality_root"] / "alignment-consensus.json", payload
    )
    return {"payload": payload, "record": record}


def _freeze_evidence_receipt(
    loaded: Mapping[str, Any],
    support: Mapping[str, Any],
    alignment: Mapping[str, Any],
) -> dict[str, Any]:
    turns = [
        _turn_evidence_from_paths(loaded, stage, orientation)
        for stage, orientation in _TURN_ORDER
    ]
    payload = {
        "schema_version": controller.QUALITY_EVIDENCE_RECEIPT_VERSION,
        "state": "completed_four_turn_evidence_quality_not_scored",
        "thread_id": supervisor.TARGET_THREAD_ID,
        "plan_epoch": controller.PLAN_EPOCH,
        "quality_handoff": loaded["handoff"]["handoff_record"],
        "quality_runtime_evidence_contract_sha256": loaded["handoff"]["handoff"][
            "quality_runtime_evidence_contract_sha256"
        ],
        "quality_scoring_manifest": loaded["scoring_record"],
        "turn_order": [
            {"stage": stage, "orientation": orientation}
            for stage, orientation in _TURN_ORDER
        ],
        "turns": turns,
        "support_consensus": support["record"],
        "alignment_consensus": alignment["record"],
        "aggregate_usage": _sum_usage([row["usage"] for row in turns]),
        "aggregate_wall_elapsed_seconds": sum(
            row["wall_elapsed_seconds"] for row in turns
        ),
        "semantic_model_call_count": EXACT_TURN_COUNT,
        "semantic_retry_count": 0,
        "deterministic_score_recomputation_required": True,
        "quality_gate_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    return _write_receipt(
        loaded["quality_root"] / controller.QUALITY_EVIDENCE_RECEIPT_FILENAME,
        payload,
    )


def _waiting_payload(
    loaded: Mapping[str, Any],
    *,
    reason: str,
    call_count: int,
    usage_values: Sequence[Mapping[str, int]],
    offline_test_mode: bool,
) -> dict[str, Any]:
    complete_count = len(usage_values)
    unknown_attempt_count = max(0, call_count - complete_count)
    return {
        "schema_version": QUALITY_WAITING_VERSION,
        "state": "waiting",
        "terminal_reason": reason,
        "thread_id": supervisor.TARGET_THREAD_ID,
        "plan_epoch": controller.PLAN_EPOCH,
        "step_id": STEP_ID,
        "directive": loaded["directive_record"],
        "runtime_lock": loaded["runtime_lock_record"],
        "semantic_model_call_count": call_count,
        "semantic_retry_count": 0,
        "measured_usage": _sum_usage(list(usage_values)),
        "usage_status": (
            "measured_complete" if unknown_attempt_count == 0 else "unknown_partial"
        ),
        "usage_complete": unknown_attempt_count == 0,
        "known_completed_attempt_count": complete_count,
        "unknown_attempt_count": unknown_attempt_count,
        "offline_test_mode": offline_test_mode,
        "promotable": False,
        "replay_authorized": False,
        "quality_gate_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _verify_waiting(loaded: Mapping[str, Any]) -> dict[str, Any]:
    path = loaded["quality_root"] / WAITING_FILENAME
    value = _load_object(path, label="quality waiting receipt")
    if (
        value.get("schema_version") != QUALITY_WAITING_VERSION
        or value.get("state") != "waiting"
        or value.get("thread_id") != supervisor.TARGET_THREAD_ID
        or value.get("plan_epoch") != controller.PLAN_EPOCH
        or value.get("step_id") != STEP_ID
        or value.get("directive") != loaded["directive_record"]
        or value.get("runtime_lock") != loaded["runtime_lock_record"]
        or not isinstance(value.get("semantic_model_call_count"), int)
        or not 0 <= value["semantic_model_call_count"] <= EXACT_TURN_COUNT
        or value.get("semantic_retry_count") != 0
        or value.get("usage_status")
        not in {"measured_complete", "unknown_partial"}
        or not isinstance(value.get("usage_complete"), bool)
        or not isinstance(value.get("known_completed_attempt_count"), int)
        or not isinstance(value.get("unknown_attempt_count"), int)
        or value["known_completed_attempt_count"] < 0
        or value["unknown_attempt_count"] < 0
        or value["known_completed_attempt_count"]
        + value["unknown_attempt_count"]
        != value["semantic_model_call_count"]
        or value["usage_complete"] is not (value["unknown_attempt_count"] == 0)
        or value["usage_status"]
        != (
            "measured_complete"
            if value["unknown_attempt_count"] == 0
            else "unknown_partial"
        )
        or not isinstance(value.get("offline_test_mode"), bool)
        or value.get("promotable") is not False
        or value.get("replay_authorized") is not False
        or value.get("quality_gate_authorized") is not False
        or value.get("holdout_authorized") is not False
        or value.get("production_mutated") is not False
        or value.get("receipt_sha256") != _receipt_checksum(value)
    ):
        raise CanonicalV31QualityRuntimeError("quality waiting receipt drifted")
    _usage(value.get("measured_usage"), label="quality waiting")
    return value


def _offline_receipt_payload(
    loaded: Mapping[str, Any],
    *,
    support: Mapping[str, Any],
    alignment: Mapping[str, Any],
) -> dict[str, Any]:
    turns = [
        _turn_evidence_from_paths(loaded, stage, orientation)
        for stage, orientation in _TURN_ORDER
    ]
    return {
        "schema_version": QUALITY_OFFLINE_RECEIPT_VERSION,
        "state": "offline_fixture_completed_non_promotable",
        "thread_id": supervisor.TARGET_THREAD_ID,
        "plan_epoch": controller.PLAN_EPOCH,
        "step_id": STEP_ID,
        "directive": loaded["directive_record"],
        "runtime_lock": loaded["runtime_lock_record"],
        "turn_order": [
            {"stage": stage, "orientation": orientation}
            for stage, orientation in _TURN_ORDER
        ],
        "turns": turns,
        "support_consensus": support["record"],
        "alignment_consensus": alignment["record"],
        "aggregate_usage": _sum_usage([row["usage"] for row in turns]),
        "aggregate_wall_elapsed_seconds": sum(
            row["wall_elapsed_seconds"] for row in turns
        ),
        "semantic_model_call_count": EXACT_TURN_COUNT,
        "semantic_retry_count": 0,
        "offline_test_mode": True,
        "promotable": False,
        "quality_gate_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _verify_offline_receipt(loaded: Mapping[str, Any]) -> dict[str, Any]:
    if _complete_turn_count(loaded["quality_root"]) != EXACT_TURN_COUNT:
        raise CanonicalV31QualityRuntimeError("quality offline turn set is incomplete")
    if _completed_dispatch_mode(loaded) != "offline":
        raise CanonicalV31QualityRuntimeError(
            "quality offline receipt is not bound to offline dispatches"
        )
    path = loaded["quality_root"] / OFFLINE_RECEIPT_FILENAME
    value = _load_object(path, label="quality offline receipt")
    support = {
        "record": _record(
            loaded["quality_root"] / "support-consensus.json",
            allowed_root=loaded["quality_root"],
        )
    }
    alignment = {
        "record": _record(
            loaded["quality_root"] / "alignment-consensus.json",
            allowed_root=loaded["quality_root"],
        )
    }
    expected = _offline_receipt_payload(
        loaded, support=support, alignment=alignment
    )
    expected["receipt_sha256"] = _receipt_checksum(expected)
    if value != expected:
        raise CanonicalV31QualityRuntimeError("quality offline receipt drifted")
    return value


class _WriterLock:
    def __init__(self, root: Path) -> None:
        self.path = root / WRITER_LOCK_FILENAME
        self.handle: Any = None

    def __enter__(self) -> "_WriterLock":
        if self.path.exists() and (self.path.is_symlink() or not self.path.is_file()):
            raise CanonicalV31QualityRuntimeError("quality writer lock path is unsafe")
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(self.path, flags, 0o600)
        self.handle = os.fdopen(descriptor, "r+")
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.handle.close()
            self.handle = None
            raise CanonicalV31QualityLockUnavailable(
                "another quality runtime owns this root"
            ) from exc
        return self

    def __exit__(self, *_args: Any) -> None:
        if self.handle is not None:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()
            self.handle = None


def _reject_auth_environment(environ: Mapping[str, str] | None = None) -> None:
    values = environ if environ is not None else os.environ
    if any(values.get(name) for name in _FORBIDDEN_AUTH_ENVIRONMENT):
        raise CanonicalV31QualityRuntimeError(
            "quality runtime rejects API keys and raw session-token auth"
        )


def _existing_dispatch_count(root: Path) -> int:
    return sum(
        _turn_paths(root, stage, orientation)["dispatch"].is_file()
        for stage, orientation in _TURN_ORDER
    )


def _existing_turn_artifact_count(root: Path) -> int:
    turns = root / "turns"
    if not turns.exists():
        return 0
    if turns.is_symlink() or not turns.is_dir():
        raise CanonicalV31QualityRuntimeError("quality turns root is unsafe")
    count = 0
    for path in turns.rglob("*"):
        if path.is_symlink():
            raise CanonicalV31QualityRuntimeError("quality turn artifact is symlinked")
        if path.is_file():
            count += 1
    return count


def _complete_turn_count(root: Path) -> int:
    return sum(
        _turn_paths(root, stage, orientation)["terminal"].is_file()
        for stage, orientation in _TURN_ORDER
    )


def _dispatch_payload(
    loaded: Mapping[str, Any],
    *,
    stage: str,
    orientation: str,
    offline_test_mode: bool,
) -> dict[str, Any]:
    paths = _turn_paths(loaded["quality_root"], stage, orientation)
    return {
        "schema_version": QUALITY_DISPATCH_MARKER_VERSION,
        "state": "semantic_dispatch_committed",
        "stage": stage,
        "orientation": orientation,
        "runtime_lock": loaded["runtime_lock_record"],
        "request": _record(paths["request"], allowed_root=loaded["quality_root"]),
        "capacity_request": _record(
            paths["capacity_request"], allowed_root=loaded["quality_root"]
        ),
        "capacity_measurement": _record(
            paths["capacity_measurement"], allowed_root=loaded["quality_root"]
        ),
        "capacity_admission": _record(
            paths["capacity_admission"], allowed_root=loaded["quality_root"]
        ),
        "model": MODEL,
        "effort": EFFORT,
        "semantic_model_call_count": 1,
        "semantic_retry_count": 0,
        "offline_test_mode": offline_test_mode,
        "promotable": not offline_test_mode,
        "production_mutated": False,
    }


def _verify_dispatch_marker(
    loaded: Mapping[str, Any], stage: str, orientation: str
) -> dict[str, Any]:
    path = _turn_paths(loaded["quality_root"], stage, orientation)["dispatch"]
    marker = _load_object(path, label="quality dispatch marker")
    offline = marker.get("offline_test_mode")
    if not isinstance(offline, bool):
        raise CanonicalV31QualityRuntimeError("quality dispatch mode is malformed")
    expected = _dispatch_payload(
        loaded,
        stage=stage,
        orientation=orientation,
        offline_test_mode=offline,
    )
    if marker != expected:
        raise CanonicalV31QualityRuntimeError("quality dispatch marker drifted")
    return marker


def _completed_dispatch_mode(loaded: Mapping[str, Any]) -> str:
    modes: set[str] = set()
    for stage, orientation in _TURN_ORDER:
        marker = _verify_dispatch_marker(loaded, stage, orientation)
        modes.add("offline" if marker["offline_test_mode"] else "live")
    if len(modes) != 1:
        raise CanonicalV31QualityRuntimeError("quality dispatch modes were mixed")
    return next(iter(modes))


def _existing_dispatch_mode(loaded: Mapping[str, Any]) -> str | None:
    modes: set[str] = set()
    for stage, orientation in _TURN_ORDER:
        path = _turn_paths(loaded["quality_root"], stage, orientation)["dispatch"]
        if not path.is_file():
            continue
        marker = _verify_dispatch_marker(loaded, stage, orientation)
        modes.add("offline" if marker["offline_test_mode"] else "live")
    if len(modes) > 1:
        raise CanonicalV31QualityRuntimeError("quality dispatch modes were mixed")
    return next(iter(modes)) if modes else None


def _assert_runtime_binding_unchanged(loaded: Mapping[str, Any]) -> None:
    current = build_quality_runtime_binding()
    if current != loaded["runtime_binding"]:
        raise CanonicalV31QualityRuntimeError(
            "quality runtime dependency binding drifted before semantic boundary"
        )


def _finalize_complete_turns(loaded: Mapping[str, Any]) -> dict[str, Any]:
    _assert_runtime_binding_unchanged(loaded)
    if (
        _complete_turn_count(loaded["quality_root"]) != EXACT_TURN_COUNT
        or _completed_dispatch_mode(loaded) != "live"
    ):
        raise CanonicalV31QualityRuntimeError(
            "live quality finalization requires four live dispatches"
        )
    support = _freeze_support_consensus(loaded)
    alignment = _freeze_alignment_consensus(loaded, support)
    _freeze_evidence_receipt(loaded, support, alignment)
    try:
        controller.verify_quality_runtime_evidence_receipt(
            loaded["controller_root"],
            loaded["quality_root"] / controller.QUALITY_EVIDENCE_RECEIPT_FILENAME,
            project_root=loaded["project_root"],
        )
        return controller.freeze_quality_terminal_receipt(
            loaded["controller_root"],
            loaded["quality_root"],
            project_root=loaded["project_root"],
        )
    except controller.Epoch7ControllerError as exc:
        raise CanonicalV31QualityRuntimeError(str(exc)) from exc


def _verify_live_quality_terminal(loaded: Mapping[str, Any]) -> dict[str, Any]:
    if (
        _complete_turn_count(loaded["quality_root"]) != EXACT_TURN_COUNT
        or _completed_dispatch_mode(loaded) != "live"
    ):
        raise CanonicalV31QualityRuntimeError(
            "quality terminal is not bound to four live dispatches"
        )
    try:
        return controller.verify_quality_terminal_receipt(
            loaded["controller_root"],
            loaded["quality_root"],
            project_root=loaded["project_root"],
        )
    except controller.Epoch7ControllerError as exc:
        raise CanonicalV31QualityRuntimeError(str(exc)) from exc


def _completed_turn_usage(root: Path) -> list[dict[str, int]]:
    values: list[dict[str, int]] = []
    for stage, orientation in _TURN_ORDER:
        terminal_path = _turn_paths(root, stage, orientation)["terminal"]
        if terminal_path.is_file():
            terminal = _load_object(terminal_path, label="quality turn terminal")
            values.append(_usage(terminal.get("usage"), label="quality turn terminal"))
    return values


async def _run_turn(
    loaded: Mapping[str, Any],
    *,
    client: Any,
    prepared: Mapping[str, Any],
    provider: Callable[[Mapping[str, Any]], Any] | None,
    offline_test_mode: bool,
    used_calls: int,
    used_tokens: int,
    sequence: int,
    now: Callable[[], datetime],
) -> dict[str, Any]:
    _assert_runtime_binding_unchanged(loaded)
    stage = prepared["request"]["stage"]
    orientation = prepared["request"]["orientation"]
    paths = prepared["paths"]
    context = _capacity_context(
        loaded,
        stage=stage,
        orientation=orientation,
        sequence=sequence,
        used_calls=used_calls,
        used_tokens=used_tokens,
    )
    request, measurement, admission = await _probe_capacity(
        loaded,
        client=client,
        provider=provider,
        offline_test_mode=offline_test_mode,
        context=context,
        now=now,
    )
    capacity_request_record = _write_immutable_json(paths["capacity_request"], request)
    capacity_measurement_record = _write_immutable_json(
        paths["capacity_measurement"], measurement
    )
    capacity_admission_record = _write_immutable_json(
        paths["capacity_admission"], admission
    )
    if admission["cleared_for_semantic_turn"] is not True:
        raise CanonicalV31QualityWaiting(
            f"capacity unavailable before {stage}.{orientation}"
        )
    _assert_runtime_binding_unchanged(loaded)
    dispatch = _dispatch_payload(
        loaded,
        stage=stage,
        orientation=orientation,
        offline_test_mode=offline_test_mode,
    )
    dispatch_record = _write_immutable_json(paths["dispatch"], dispatch)
    result = await client.run_ephemeral_structured_turn(
        model=MODEL,
        effort=EFFORT,
        base_instructions=prepared["base"],
        prompt=prepared["prompt"],
        output_schema=prepared["schema"],
        cwd=loaded["project_root"],
        sidecar_path=paths["sidecar"],
        output_path=paths["raw_output"],
        batch_size=1,
        thread_mode="new_thread",
        timeout_seconds=loaded["limits"]["maximum_wall_seconds_per_turn"],
    )
    if getattr(result, "status_ok", False) is not True:
        raise CanonicalV31QualityWaiting(
            f"semantic turn failed before {stage}.{orientation} terminal"
        )
    _assert_runtime_binding_unchanged(loaded)
    raw = _load_json(paths["raw_output"], label="quality raw output")
    parsed = _parsed_turn_payload(loaded, prepared, raw)
    parsed_record = _write_immutable_json(paths["parsed_decisions"], parsed)
    sidecar = _load_object(paths["sidecar"], label="quality sidecar")
    usage = _usage(sidecar.get("usage"), label="quality sidecar")
    wall = sidecar.get("wall_elapsed_seconds")
    thread_id = sidecar.get("thread_id")
    turn_id = sidecar.get("turn_id")
    if (
        usage["total_tokens"] > loaded["limits"]["maximum_total_tokens_per_turn"]
        or isinstance(wall, bool)
        or not isinstance(wall, (int, float))
        or float(wall) > loaded["limits"]["maximum_wall_seconds_per_turn"]
        or not isinstance(thread_id, str)
        or not thread_id
        or not isinstance(turn_id, str)
        or not turn_id
    ):
        raise CanonicalV31QualityWaiting(
            f"postturn measured ceiling or identity failed for {stage}.{orientation}"
        )
    terminal = {
        "schema_version": QUALITY_TURN_TERMINAL_VERSION,
        "state": "completed",
        "stage": stage,
        "orientation": orientation,
        "thread_id": thread_id,
        "turn_id": turn_id,
        "runtime_lock": loaded["runtime_lock_record"],
        "dispatch_marker": dispatch_record,
        "sidecar": _record(paths["sidecar"], allowed_root=loaded["quality_root"]),
        "raw_output": _record(paths["raw_output"], allowed_root=loaded["quality_root"]),
        "parsed_decisions": parsed_record,
        "usage": usage,
        "wall_elapsed_seconds": float(wall),
        "semantic_model_call_count": 1,
        "semantic_retry_count": 0,
        "production_mutated": False,
    }
    _write_immutable_json(paths["terminal"], terminal)
    return {"terminal": terminal, "usage": usage}


def _execution_mode(
    *,
    offline_test_mode: bool,
    client_factory: Callable[[], Any],
    capacity_provider: Callable[[Mapping[str, Any]], Any] | None,
    now: Callable[[], datetime],
) -> None:
    if offline_test_mode:
        if not callable(client_factory) or not callable(capacity_provider) or not callable(now):
            raise CanonicalV31QualityRuntimeError(
                "offline fixture mode requires client, capacity, and clock callables"
            )
        return
    if client_factory is not OFFICIAL_CLIENT_FACTORY:
        raise CanonicalV31QualityRuntimeError(
            "live quality mode rejects an injected app-server client"
        )
    if capacity_provider is not None:
        raise CanonicalV31QualityRuntimeError(
            "live quality mode rejects an injected capacity provider"
        )
    if now is not _utcnow:
        raise CanonicalV31QualityRuntimeError(
            "live quality mode rejects an injected clock"
        )


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def execute_quality_runtime(
    *,
    quality_root: Path,
    operator_authorization_id: str,
    project_root: Path | None = None,
    offline_test_mode: bool = False,
    client_factory: Callable[[], Any] = OFFICIAL_CLIENT_FACTORY,
    capacity_provider: Callable[[Mapping[str, Any]], Any] | None = None,
    now: Callable[[], datetime] = _utcnow,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Run exactly one four-turn attempt or freeze immutable waiting evidence."""

    _execution_mode(
        offline_test_mode=offline_test_mode,
        client_factory=client_factory,
        capacity_provider=capacity_provider,
        now=now,
    )
    if not offline_test_mode:
        _reject_auth_environment(environ)
    loaded = load_quality_runtime_plan(
        quality_root,
        project_root=project_root,
        require_fresh=True,
        now=now(),
    )
    if _authorization_id(operator_authorization_id) != loaded["authorization_id"]:
        raise CanonicalV31QualityRuntimeError(
            "quality execute authorization ID does not match the frozen directive"
        )
    with _WriterLock(loaded["quality_root"]):
        terminal_path = loaded["quality_root"] / controller.QUALITY_TERMINAL_RECEIPT_FILENAME
        evidence_path = loaded["quality_root"] / controller.QUALITY_EVIDENCE_RECEIPT_FILENAME
        waiting_path = loaded["quality_root"] / WAITING_FILENAME
        if terminal_path.is_file():
            return _verify_live_quality_terminal(loaded)
        if waiting_path.is_file():
            return {"receipt": _verify_waiting(loaded), "state": "waiting"}
        if (loaded["quality_root"] / OFFLINE_RECEIPT_FILENAME).is_file():
            return {
                "receipt": _verify_offline_receipt(loaded),
                "state": "offline_fixture_completed_non_promotable",
            }
        existing_dispatches = _existing_dispatch_count(loaded["quality_root"])
        if evidence_path.is_file():
            if (
                _complete_turn_count(loaded["quality_root"]) != EXACT_TURN_COUNT
                or _completed_dispatch_mode(loaded) != "live"
            ):
                raise CanonicalV31QualityRuntimeError(
                    "quality evidence is not bound to four live dispatches"
                )
            try:
                controller.verify_quality_runtime_evidence_receipt(
                    loaded["controller_root"],
                    evidence_path,
                    project_root=loaded["project_root"],
                )
                result = controller.freeze_quality_terminal_receipt(
                    loaded["controller_root"],
                    loaded["quality_root"],
                    project_root=loaded["project_root"],
                )
                return result
            except controller.Epoch7ControllerError as exc:
                raise CanonicalV31QualityRuntimeError(str(exc)) from exc
        if existing_dispatches == EXACT_TURN_COUNT and _complete_turn_count(
            loaded["quality_root"]
        ) == EXACT_TURN_COUNT:
            completed_mode = _completed_dispatch_mode(loaded)
            if completed_mode == "offline":
                support = _freeze_support_consensus(loaded)
                alignment = _freeze_alignment_consensus(loaded, support)
                offline = _offline_receipt_payload(
                    loaded, support=support, alignment=alignment
                )
                _write_receipt(
                    loaded["quality_root"] / OFFLINE_RECEIPT_FILENAME, offline
                )
                return {
                    "receipt": _verify_offline_receipt(loaded),
                    "state": "offline_fixture_completed_non_promotable",
                }
            if offline_test_mode:
                raise CanonicalV31QualityRuntimeError(
                    "offline fixture mode cannot finalize a live quality attempt"
                )
            return _finalize_complete_turns(loaded)
        if existing_dispatches or _existing_turn_artifact_count(loaded["quality_root"]):
            dispatch_mode = _existing_dispatch_mode(loaded)
            usage_values = _completed_turn_usage(loaded["quality_root"])
            waiting = _waiting_payload(
                loaded,
                reason="partial_or_interrupted_quality_attempt_no_replay",
                call_count=existing_dispatches,
                usage_values=usage_values,
                offline_test_mode=(
                    dispatch_mode == "offline"
                    if dispatch_mode is not None
                    else offline_test_mode
                ),
            )
            _write_receipt(waiting_path, waiting)
            return {"receipt": _verify_waiting(loaded), "state": "waiting"}

        calls = 0
        usages: list[dict[str, int]] = []
        try:
            context = client_factory()
            async with context as client:
                account = getattr(client, "account_summary", None)
                if (
                    not isinstance(account, Mapping)
                    or account.get("type") != "chatgpt"
                    or account.get("plan_type") != "pro"
                ):
                    raise CanonicalV31QualityWaiting(
                        "managed ChatGPT Pro auth verification failed"
                    )
                support_result: dict[str, Any] | None = None
                for sequence, (stage, orientation) in enumerate(_TURN_ORDER):
                    if stage == "alignment" and support_result is None:
                        support_result = _freeze_support_consensus(loaded)
                    verdicts = (
                        {
                            row["witness_id"]: row["verdict"]
                            for row in support_result["payload"]["decisions"]
                        }
                        if support_result is not None
                        else None
                    )
                    prepared = _prepare_turn(
                        loaded,
                        stage=stage,
                        orientation=orientation,
                        support_consensus_record=(
                            support_result["record"] if support_result else None
                        ),
                        support_verdicts=verdicts,
                    )
                    result = await _run_turn(
                        loaded,
                        client=client,
                        prepared=prepared,
                        provider=capacity_provider,
                        offline_test_mode=offline_test_mode,
                        used_calls=calls,
                        used_tokens=sum(row["total_tokens"] for row in usages),
                        sequence=sequence,
                        now=now,
                    )
                    calls += 1
                    usages.append(result["usage"])
                    if sum(row["total_tokens"] for row in usages) > loaded["limits"][
                        "exact_total_token_cap"
                    ]:
                        raise CanonicalV31QualityWaiting(
                            "quality aggregate token ceiling exceeded"
                        )
            if calls != EXACT_TURN_COUNT:
                raise CanonicalV31QualityWaiting("quality turn sequence is incomplete")
            support = _freeze_support_consensus(loaded)
            alignment = _freeze_alignment_consensus(loaded, support)
            if offline_test_mode:
                offline = _offline_receipt_payload(
                    loaded, support=support, alignment=alignment
                )
                _write_receipt(
                    loaded["quality_root"] / OFFLINE_RECEIPT_FILENAME, offline
                )
                return {
                    "receipt": _verify_offline_receipt(loaded),
                    "state": "offline_fixture_completed_non_promotable",
                }
            return _finalize_complete_turns(loaded)
        except asyncio.CancelledError:
            waiting = _waiting_payload(
                loaded,
                reason="quality_runtime_cancelled_no_replay",
                call_count=_existing_dispatch_count(loaded["quality_root"]),
                usage_values=_completed_turn_usage(loaded["quality_root"]),
                offline_test_mode=offline_test_mode,
            )
            _write_receipt(waiting_path, waiting)
            raise
        except (
            CanonicalV31QualityWaiting,
            codex_app_server.AppServerError,
            OSError,
        ) as exc:
            waiting = _waiting_payload(
                loaded,
                reason=f"quality_runtime_waiting:{type(exc).__name__}",
                call_count=_existing_dispatch_count(loaded["quality_root"]),
                usage_values=_completed_turn_usage(loaded["quality_root"]),
                offline_test_mode=offline_test_mode,
            )
            _write_receipt(waiting_path, waiting)
            return {"receipt": _verify_waiting(loaded), "state": "waiting"}


def status_quality_runtime(
    quality_root: Path,
    *,
    project_root: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    loaded = load_quality_runtime_plan(
        quality_root, project_root=project_root, now=now
    )
    root = loaded["quality_root"]
    if (root / controller.QUALITY_TERMINAL_RECEIPT_FILENAME).is_file():
        verified = _verify_live_quality_terminal(loaded)
        state = verified["receipt"]["state"]
        reason = "verified_quality_terminal"
    elif (root / WAITING_FILENAME).is_file():
        waiting = _verify_waiting(loaded)
        state = "waiting"
        reason = waiting["terminal_reason"]
    elif (root / OFFLINE_RECEIPT_FILENAME).is_file():
        _verify_offline_receipt(loaded)
        state = "offline_fixture_completed_non_promotable"
        reason = "offline_fixture_never_authorizes_quality"
    elif _existing_dispatch_count(root):
        state = "waiting"
        reason = "partial_quality_attempt_requires_terminal_waiting_receipt"
    else:
        current = now or datetime.now(timezone.utc)
        state = "ready" if current < loaded["expires_at"] else "waiting"
        reason = (
            "ready_for_explicit_authorized_execute"
            if state == "ready"
            else "quality_directive_expired"
        )
    return {
        "schema_version": QUALITY_STATUS_VERSION,
        "state": state,
        "reason": reason,
        "plan_epoch": controller.PLAN_EPOCH,
        "step_id": STEP_ID,
        "runtime_lock": loaded["runtime_lock_record"],
        "semantic_model_call_count_observed": _existing_dispatch_count(root),
        "semantic_retry_count": 0,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def verify_quality_runtime(
    quality_root: Path,
    *,
    project_root: Path | None = None,
) -> dict[str, Any]:
    loaded = load_quality_runtime_plan(quality_root, project_root=project_root)
    root = loaded["quality_root"]
    if (root / controller.QUALITY_TERMINAL_RECEIPT_FILENAME).is_file():
        return _verify_live_quality_terminal(loaded)
    if (root / WAITING_FILENAME).is_file():
        return {"receipt": _verify_waiting(loaded), "state": "waiting"}
    if (root / OFFLINE_RECEIPT_FILENAME).is_file():
        return {
            "receipt": _verify_offline_receipt(loaded),
            "state": "offline_fixture_completed_non_promotable",
        }
    if _existing_dispatch_count(root):
        raise CanonicalV31QualityRuntimeError(
            "partial quality attempt has no immutable waiting receipt"
        )
    return {
        "schema_version": QUALITY_RUNTIME_LOCK_VERSION,
        "state": "verified_zero_call_ready",
        "runtime_lock": loaded["runtime_lock_record"],
        "semantic_model_call_count": 0,
        "semantic_retry_count": 0,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python3 -m research_factory.app_server_canonical_v31_quality_runtime"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--controller-root", type=Path, required=True)
    plan.add_argument("--quality-root", type=Path, required=True)
    plan.add_argument("--context-usage-recovery", type=Path, required=True)
    plan.add_argument("--operator-authorization-id", required=True)
    plan.add_argument("--issued-at", required=True)
    plan.add_argument("--expires-at", required=True)
    status = commands.add_parser("status")
    status.add_argument("--quality-root", type=Path, required=True)
    execute = commands.add_parser("execute")
    execute.add_argument("--quality-root", type=Path, required=True)
    execute.add_argument("--operator-authorization-id", required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("--quality-root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "plan":
            result = freeze_quality_runtime_plan(
                controller_root=args.controller_root,
                quality_root=args.quality_root,
                context_usage_recovery_path=args.context_usage_recovery,
                operator_authorization_id=args.operator_authorization_id,
                issued_at=args.issued_at,
                expires_at=args.expires_at,
            )
        elif args.command == "status":
            result = status_quality_runtime(args.quality_root)
        elif args.command == "execute":
            result = asyncio.run(
                execute_quality_runtime(
                    quality_root=args.quality_root,
                    operator_authorization_id=args.operator_authorization_id,
                )
            )
        else:
            result = verify_quality_runtime(args.quality_root)
    except CanonicalV31QualityRuntimeError as exc:
        print(_pretty_json({"ok": False, "error": str(exc)}), end="", file=sys.stderr)
        return 2
    print(_pretty_json({"ok": True, "result": result}), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
