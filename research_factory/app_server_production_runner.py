from __future__ import annotations

"""Hash-bound managed-app-server executor for the post-holdout label queue.

The module is intentionally independent from the evaluation strategy lineage.
It owns transport, immutable turn artifacts, and queue lifecycle only. Semantic
decisions stay inside the frozen prompt/schema and the model response.
"""

import argparse
import asyncio
import fcntl
import hashlib
import inspect
import json
import math
import os
import sqlite3
import stat
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from . import app_server_canonical_v31_episode_batch as episode_batch
from . import app_server_canonical_v31_production_contract as production_contract
from . import app_server_episode_context_runner as episode_context_runner
from .codex_app_server import (
    APP_SERVER_CLIENT_VERSION,
    TURN_SIDECAR_SCHEMA_VERSION,
    AppServerError,
    AppServerThread,
    AppServerThreadArchiveState,
    CodexAppServerClient,
)
from .efficient_backtest import build_windowed_segment_packet
from .app_server_episode_context_runner import (
    EpisodeContextRunnerError,
)
from .app_server_source_integrity import (
    SOURCE_INTEGRITY_VERSION,
    SourceIntegrityError,
    VerifiedSourceLoader,
)
from .pipeline_babysitter import verify_evaluation_receipt
from .labels import ValidationError
from .util import loads_json, now_iso, stable_id
from .worker import fail_job, submit_label_output


CONTRACT_VERSION = "pif_app_server_production_runner_contract_v4"
RUN_RECEIPT_VERSION = "pif_app_server_production_runner_receipt_v4"
TURN_INPUT_VERSION = "pif_app_server_production_turn_input_v2"
TURN_LAUNCH_VERSION = "pif_app_server_production_turn_launch_v2"
TURN_TERMINAL_VERSION = "pif_app_server_production_turn_terminal_v2"
EPISODE_THREAD_COMPLETION_VERSION = (
    "pif_app_server_production_episode_thread_completion_v1"
)
EPISODE_THREAD_ARCHIVE_VERSION = "pif_app_server_production_episode_thread_archive_v1"
EPISODE_THREAD_ARCHIVE_STATE_VERSION = (
    "pif_app_server_production_episode_thread_archive_state_v1"
)
RESUME_CHECKPOINT_VERSION = "pif_app_server_production_resume_checkpoint_v1"
FROZEN_CONFIGURATION_VERSION = production_contract.FROZEN_CONFIGURATION_VERSION
RUNTIME_CONFIG_VERSION = "pif_production_app_server_runtime_config_v4"
USAGE_TELEMETRY_VERSION = "pif_production_app_server_usage_telemetry_v2"
CLAIM_RECEIPT_VERSION = "pif_app_server_production_batch_claim_v1"
FAILED_LABEL_RECONCILIATION_VERSION = (
    "pif_app_server_failed_label_reconciliation_v1"
)
FAILED_LABEL_RECONCILIATION_POLICY = {
    "schema_version": FAILED_LABEL_RECONCILIATION_VERSION,
    "release_state": "awaiting_downstream_production_contract",
    "scope": "podcast_ai_discourse_v3_1_failed_label_jobs",
    "plan_before_execute": True,
    "separate_execute_authorization_required": True,
    "legacy_semantic_attempts_preserved": True,
    "startup_order": [
        "verified_episode_context_complete",
        "winner_and_holdout_promoted",
        "failed_label_reconciliation_plan",
        "separately_authorized_failed_label_reconciliation_execute",
        "verify_failed_count_24_to_0_and_pending_count_69472",
        "production_label_runner",
    ],
}
CONTEXT_BINDING_VERSION = "pif_app_server_production_episode_context_binding_v2"
TRANSPORT = "official_codex_app_server_stdio_managed_chatgpt_auth"
PINNED_CLI_VERSION = "0.144.1"
ALLOWED_BATCH_SIZES = frozenset({3, 5, 8})
ALLOWED_THREAD_MODES = frozenset(episode_batch.SUPPORTED_THREAD_MODES)
FROZEN_QUEUE_PAYLOAD_MODEL = "gpt-5.5"
FROZEN_LABEL_PACK = "ai_discourse_v3_1"
FROZEN_QUEUE_LANE = "podcast"
LOADED_SEMANTIC_FIELDS = (
    "episode_context.context_summary",
    "episode_context.speaker_map",
    "episode_context.section_map",
    "episode_context.entity_seed",
    "episode_context.concept_seed",
    "extraction_guidance",
    "excluded_source_context",
)
USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
FORBIDDEN_CREDENTIAL_ENVIRONMENT = (
    "OPENAI_API_KEY",
    "CODEX_API_KEY",
    "OPENAI_ACCESS_TOKEN",
    "CHATGPT_ACCESS_TOKEN",
    "CODEX_AUTH_TOKEN",
)
WRITER_LOCK_VERSION = "pif_canonical_v31_process_writer_lock_v1"
CAPACITY_RECEIPT_VERSION = "pif_canonical_v31_numeric_capacity_receipt_v1"


class ProductionRunnerError(RuntimeError):
    pass


class ProductionRunnerWaiting(ProductionRunnerError):
    pass


@dataclass(frozen=True)
class QueueItem:
    job_id: int
    segment_id: str
    episode_id: str
    segment_index: int
    segment_text: str
    label_run_id: str
    output_path: str
    source_name: str = ""
    episode_title: str = ""
    segment_text_sha256: str = ""
    segment_quality: Mapping[str, Any] | None = None
    expected_source_binding: Mapping[str, Any] | None = None


class QueueOperations(Protocol):
    lane: str
    production_mutated: bool

    def preview_episode_batch(
        self, *, limit: int, episode_id: str | None = None
    ) -> list[QueueItem]: ...

    def pending_episode_segment_count(self, *, episode_id: str) -> int: ...

    def unresolved_episode_segment_count(self, *, episode_id: str) -> int: ...

    def unresolved_scoped_job_count(self) -> int: ...

    def reconcile_failed_scoped_jobs(
        self, *, execute: bool
    ) -> Mapping[str, Any]: ...

    def assert_episode_start_allowed(
        self,
        episode_id: str,
        *,
        thread_mode: str,
        output_root: Path,
    ) -> None: ...

    def claim_episode_batch(
        self,
        items: Sequence[QueueItem],
        *,
        prepared_request_path: Path,
        claim_receipt_path: Path,
    ) -> list[QueueItem]: ...

    def stale_episode_batch(self, *, limit: int) -> list[QueueItem]: ...

    def recover_stale_prelaunch_batch(self, *, limit: int) -> Mapping[str, Any]: ...

    def release_verified_prelaunch_batch(
        self,
        items: Sequence[QueueItem],
        *,
        prepared_request_path: Path,
        claim_receipt_path: Path,
        reason: str,
        thread_started_for_batch: bool,
    ) -> Mapping[str, Any]: ...

    def bind_prompt(self, items: Sequence[QueueItem], prompt_path: Path) -> None: ...

    def submit(self, item: QueueItem, output_path: Path) -> Mapping[str, Any]: ...

    def fail(self, item: QueueItem, reason: str) -> None: ...

    def verify_completed_submissions(
        self, turns: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]: ...

    def verify_source_packet(
        self, source_packet: Mapping[str, Any]
    ) -> Mapping[str, Any]: ...


class CapacityAdmissionProvider(Protocol):
    def __call__(
        self,
        *,
        request: Mapping[str, Any],
        remaining_batch_count: int,
        timeout_seconds: float,
    ) -> Any: ...


def _reject_credential_environment() -> None:
    present = [name for name in FORBIDDEN_CREDENTIAL_ENVIRONMENT if os.environ.get(name)]
    if present:
        raise ProductionRunnerError(
            "credential environment is forbidden for managed-auth production: "
            + ", ".join(present)
        )


def _database_identity(queue: QueueOperations) -> dict[str, Any]:
    if isinstance(queue, SQLiteWorkerQueue):
        return dict(queue.database_identity)
    return {
        "kind": "offline_fixture_queue",
        "fixture_class": f"{type(queue).__module__}.{type(queue).__qualname__}",
    }


class ProductionWriterLock:
    """Nonblocking process-wide writer exclusion for one exact database."""

    def __init__(
        self,
        *,
        database_identity: Mapping[str, Any],
        output_root: Path,
        contract_path: Path,
    ) -> None:
        self.database_identity = dict(database_identity)
        resolved_contract = contract_path.expanduser().resolve(strict=True)
        self.payload = {
            "schema_version": WRITER_LOCK_VERSION,
            "database_identity": self.database_identity,
            "output_root": str(output_root.expanduser().resolve()),
            "contract": {
                "path": str(resolved_contract),
                "sha256": _sha256_file(resolved_contract),
                "size_bytes": resolved_contract.stat().st_size,
            },
            "process_id": os.getpid(),
        }
        identity_sha256 = _sha256_bytes(
            _canonical_json(self.database_identity).encode("utf-8")
        )
        self.path = Path(tempfile.gettempdir()).expanduser().resolve() / (
            f"pif-canonical-v31-writer-{identity_sha256}.lock"
        )
        self._descriptor: int | None = None

    def __enter__(self) -> "ProductionWriterLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            parent = self.path.parent.lstat()
        except OSError as exc:
            raise ProductionRunnerError(
                "canonical production writer-lock directory is unavailable"
            ) from exc
        if (
            not stat.S_ISDIR(parent.st_mode)
            or self.path.parent.is_symlink()
        ):
            raise ProductionRunnerError(
                "canonical production writer-lock directory is not a real directory"
            )
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(self.path, flags, 0o600)
        except OSError as exc:
            raise ProductionRunnerError(
                "canonical production writer lock is not a real file"
            ) from exc
        try:
            opened = os.fstat(descriptor)
            lexical = self.path.lstat()
        except OSError as exc:
            os.close(descriptor)
            raise ProductionRunnerError(
                "canonical production writer lock identity is unavailable"
            ) from exc
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(lexical.st_mode)
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (lexical.st_dev, lexical.st_ino)
        ):
            os.close(descriptor)
            raise ProductionRunnerError(
                "canonical production writer lock is not a real file"
            )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise ProductionRunnerWaiting(
                "canonical production writer lock is already held"
            ) from exc
        except OSError as exc:
            os.close(descriptor)
            raise ProductionRunnerError(
                "canonical production writer lock could not be acquired"
            ) from exc
        try:
            locked = self.path.lstat()
            if (
                not stat.S_ISREG(locked.st_mode)
                or (opened.st_dev, opened.st_ino)
                != (locked.st_dev, locked.st_ino)
            ):
                raise ProductionRunnerError(
                    "canonical production writer lock changed during acquisition"
                )
            payload_bytes = _pretty_json(self.payload).encode("utf-8")
            os.ftruncate(descriptor, 0)
            os.lseek(descriptor, 0, os.SEEK_SET)
            offset = 0
            while offset < len(payload_bytes):
                written = os.write(descriptor, payload_bytes[offset:])
                if written <= 0:
                    raise OSError("short writer-lock payload write")
                offset += written
            os.fsync(descriptor)
            final_opened = os.fstat(descriptor)
            final_lexical = self.path.lstat()
            if (
                not stat.S_ISREG(final_opened.st_mode)
                or not stat.S_ISREG(final_lexical.st_mode)
                or final_opened.st_nlink != 1
                or (final_opened.st_dev, final_opened.st_ino)
                != (final_lexical.st_dev, final_lexical.st_ino)
            ):
                raise ProductionRunnerError(
                    "canonical production writer lock changed while held"
                )
        except Exception:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
            raise
        self._descriptor = descriptor
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        descriptor = self._descriptor
        self._descriptor = None
        if descriptor is not None:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)


def _capacity_requirements(
    request: Mapping[str, Any], *, remaining_batch_count: int, timeout_seconds: float
) -> dict[str, int]:
    if remaining_batch_count < 1 or timeout_seconds <= 0:
        raise ProductionRunnerError("capacity requirement boundary is invalid")
    prompt_bytes = len(str(request["prompt"]).encode("utf-8"))
    base_bytes = len(str(request["base_instructions"]).encode("utf-8"))
    schema_bytes = len(_canonical_json(request["output_schema"]).encode("utf-8"))
    # One UTF-8 byte is treated as one input token, then a fixed full-schema
    # completion ceiling is reserved.  This is deliberately conservative.
    maximum_total_tokens_per_batch = prompt_bytes + base_bytes + schema_bytes + 65_536
    return {
        "remaining_batch_count": remaining_batch_count,
        "maximum_total_tokens_per_batch": maximum_total_tokens_per_batch,
        "required_total_tokens": maximum_total_tokens_per_batch * remaining_batch_count,
        "required_wall_seconds": math.ceil(timeout_seconds * remaining_batch_count),
    }


def _operator_deadline(path: Path, *, expected_contract_sha256: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    payload = _load_json(resolved, label="operator capacity deadline")
    expected_keys = {
        "schema_version",
        "deadline_at",
        "production_contract_sha256",
        "issued_by",
        "semantic_authority",
    }
    try:
        deadline = datetime.fromisoformat(str(payload.get("deadline_at")).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProductionRunnerWaiting("operator capacity deadline is malformed") from exc
    if (
        set(payload) != expected_keys
        or payload.get("schema_version") != "pif_canonical_v31_operator_deadline_v1"
        or payload.get("production_contract_sha256") != expected_contract_sha256
        or payload.get("issued_by") != "operator"
        or payload.get("semantic_authority") is not False
        or deadline.tzinfo is None
    ):
        raise ProductionRunnerWaiting("operator capacity deadline drifted")
    return {"payload": payload, "record": _record(resolved), "deadline": deadline}


def _rate_limit_controls(response: Any) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not isinstance(response, Mapping):
        raise ProductionRunnerWaiting("official rate-limit response is malformed")
    fallback = response.get("rateLimits")
    by_id = response.get("rateLimitsByLimitId")
    if not isinstance(fallback, Mapping):
        raise ProductionRunnerWaiting("official rate-limit fallback snapshot is absent")
    if by_id is not None:
        if not isinstance(by_id, Mapping) or not isinstance(by_id.get("codex"), Mapping):
            raise ProductionRunnerWaiting("official Codex multi-bucket snapshot is ambiguous")
        snapshot = dict(by_id["codex"])
    else:
        snapshot = dict(fallback)
    if snapshot.get("limitId") not in {None, "codex"}:
        raise ProductionRunnerWaiting("official Codex rate-limit identity drifted")
    reached = snapshot.get("rateLimitReachedType")
    if reached is not None:
        if not isinstance(reached, str):
            raise ProductionRunnerWaiting("official reached control is malformed")
        raise ProductionRunnerWaiting("official rate-limit control is reached")
    controls: list[dict[str, Any]] = []
    for name in ("primary", "secondary"):
        value = snapshot.get(name)
        if value is None:
            continue
        if not isinstance(value, Mapping):
            raise ProductionRunnerWaiting(f"official {name} rate window is malformed")
        used = value.get("usedPercent")
        if isinstance(used, bool) or not isinstance(used, int) or not 0 <= used <= 100:
            raise ProductionRunnerWaiting(f"official {name} used percentage is malformed")
        resets_at = value.get("resetsAt")
        duration = value.get("windowDurationMins")
        if resets_at is not None and (
            isinstance(resets_at, bool) or not isinstance(resets_at, int)
        ):
            raise ProductionRunnerWaiting(f"official {name} reset is malformed")
        if duration is not None and (
            isinstance(duration, bool) or not isinstance(duration, int) or duration < 0
        ):
            raise ProductionRunnerWaiting(f"official {name} duration is malformed")
        controls.append(
            {
                "name": name,
                "used_percent": used,
                "remaining_percent": 100 - used,
                "resets_at": resets_at,
                "window_duration_minutes": duration,
            }
        )
    individual = snapshot.get("individualLimit")
    if individual is not None:
        if not isinstance(individual, Mapping):
            raise ProductionRunnerWaiting("official individual limit is malformed")
        remaining = individual.get("remainingPercent")
        resets_at = individual.get("resetsAt")
        if (
            isinstance(remaining, bool)
            or not isinstance(remaining, int)
            or not 0 <= remaining <= 100
            or isinstance(resets_at, bool)
            or not isinstance(resets_at, int)
            or not isinstance(individual.get("limit"), str)
            or not isinstance(individual.get("used"), str)
        ):
            raise ProductionRunnerWaiting("official individual limit fields drifted")
        controls.append(
            {
                "name": "individual_limit",
                "used_percent": 100 - remaining,
                "remaining_percent": remaining,
                "resets_at": resets_at,
                "window_duration_minutes": None,
            }
        )
    if not controls or controls[0]["name"] != "primary":
        raise ProductionRunnerWaiting("official primary rate window is absent")
    return snapshot, controls


class OfficialManagedAuthCapacityProvider:
    fixture_only = False

    def __init__(
        self,
        *,
        client: Any,
        deadline_path: Path,
        production_contract_sha256: str,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.client = client
        self.deadline_path = deadline_path
        self.production_contract_sha256 = production_contract_sha256
        self.clock = clock

    async def __call__(
        self,
        *,
        request: Mapping[str, Any],
        remaining_batch_count: int,
        timeout_seconds: float,
    ) -> Mapping[str, Any]:
        account = getattr(self.client, "account_summary", None)
        if not isinstance(account, Mapping) or account.get("type") != "chatgpt" or account.get("plan_type") != "pro":
            raise ProductionRunnerWaiting("managed ChatGPT Pro auth verification failed")
        response = await self.client._request("account/rateLimits/read", {})
        _snapshot, controls = _rate_limit_controls(response)
        policy = production_contract.capacity_admission_contract()
        requirements = _capacity_requirements(
            request,
            remaining_batch_count=remaining_batch_count,
            timeout_seconds=timeout_seconds,
        )
        minimum_remaining = min(int(control["remaining_percent"]) for control in controls)
        usable_percent = max(
            0,
            minimum_remaining
            - int(policy["minimum_reserve_percent"])
            - int(policy["concurrency_and_quantization_margin_percent"]),
        )
        available_tokens = math.floor(
            usable_percent * 1_000_000 / int(policy["quota_points_per_million_tokens"])
        )
        deadline = _operator_deadline(
            self.deadline_path,
            expected_contract_sha256=self.production_contract_sha256,
        )
        available_wall = max(
            0,
            math.floor(deadline["deadline"].timestamp() - float(self.clock()))
            - int(policy["wall_safety_seconds"]),
        )
        admitted = bool(
            available_tokens >= requirements["required_total_tokens"]
            and available_wall >= requirements["required_wall_seconds"]
        )
        payload = {
            "schema_version": CAPACITY_RECEIPT_VERSION,
            "state": (
                "admitted_official_managed_auth_numeric_reserve"
                if admitted
                else "denied_insufficient_numeric_reserve"
            ),
            "production_scoped": True,
            "fixture_only": False,
            "offline_fixture_non_promotable": False,
            "live_capacity_authority": True,
            "official_method": "account/rateLimits/read",
            "request_params_shape": "empty_object_tolerated_by_pinned_client",
            "protocol_schema": _record(
                Path(__file__).resolve().parent
                / "protocol/codex_app_server_0_144_1/v2/GetAccountRateLimitsResponse.json"
            ),
            "raw_snapshot_sha256": _sha256_bytes(
                _canonical_json(response).encode("utf-8")
            ),
            "selected_limit_id": "codex",
            "applicable_controls": controls,
            "reset_credits_counted_as_capacity": False,
            "capacity_policy": policy,
            "capacity_policy_sha256": _sha256_bytes(
                _canonical_json(policy).encode("utf-8")
            ),
            "requirements": requirements,
            "available_total_tokens": available_tokens,
            "available_wall_seconds": available_wall,
            "operator_deadline": deadline["record"],
            "semantic_authority": False,
            "semantic_pruning_or_relabeling_allowed": False,
            "capacity_probe_before_thread_start": True,
            "semantic_thread_started": False,
            "turn_started": False,
            "sidecar_started": False,
        }
        if not admitted:
            raise ProductionRunnerWaiting("official numeric reserve is insufficient")
        return payload


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


_CANONICAL_ARTIFACT_TYPES = frozenset(
    {
        "dialogue_transcript",
        "caption_transcript",
        "article_show_notes",
        "mixed_page",
        "boilerplate",
        "unknown",
    }
)


def _segment_quality_from_db_provenance(
    context: Mapping[str, Any],
    *,
    fallback_segment_word_count: Any,
) -> dict[str, Any]:
    """Map only stored transcript-preparation provenance to canonical quality."""

    artifact_type = context.get("transcript_artifact_type")
    preparation_status = context.get("transcript_preparation_status")
    preparation_id = context.get("transcript_preparation_id")
    substantive_word_count = context.get("transcript_substantive_word_count")
    if substantive_word_count is None:
        substantive_word_count = fallback_segment_word_count
    ratio = context.get("transcript_boilerplate_ratio")
    if (
        artifact_type not in _CANONICAL_ARTIFACT_TYPES
        or not isinstance(preparation_status, str)
        or not preparation_status
        or not isinstance(preparation_id, str)
        or not preparation_id
        or isinstance(substantive_word_count, bool)
        or not isinstance(substantive_word_count, int)
        or substantive_word_count < 0
        or isinstance(ratio, bool)
        or not isinstance(ratio, (int, float))
        or not math.isfinite(float(ratio))
        or not 0.0 <= float(ratio) <= 1.0
    ):
        raise ProductionRunnerWaiting(
            "segment-quality database provenance is incomplete or invalid"
        )
    if artifact_type == "boilerplate" or preparation_status == "low_signal":
        risk = "high"
    elif float(ratio) >= 0.35:
        risk = "high"
    elif float(ratio) >= 0.15:
        risk = "medium"
    else:
        risk = "low"
    return {
        "artifact_type": artifact_type,
        "boilerplate_risk": risk,
        "substantive_word_count": substantive_word_count,
        "transcript_preparation_id": preparation_id,
    }


def _source_binding_sha256(value: Mapping[str, Any]) -> str:
    return _sha256_bytes(_canonical_json(value).encode("utf-8"))


def _validate_expected_source_binding(item: QueueItem) -> dict[str, Any]:
    binding = item.expected_source_binding
    quality = item.segment_quality
    if not isinstance(binding, Mapping) or not isinstance(quality, Mapping):
        raise ProductionRunnerWaiting(
            "verified segment source binding is required before production transport"
        )
    segment = binding.get("segment")
    transcript = binding.get("transcript")
    preparation = binding.get("transcript_preparation")
    source_basis = binding.get("transcript_source_basis")
    if (
        set(binding)
        != {
            "schema_version",
            "episode_id",
            "source_id",
            "transcript",
            "transcript_preparation",
            "transcript_source_basis",
            "segment",
        }
        or binding.get("schema_version") != SOURCE_INTEGRITY_VERSION
        or binding.get("episode_id") != item.episode_id
        or not isinstance(binding.get("source_id"), str)
        or not binding.get("source_id")
        or not isinstance(transcript, Mapping)
        or not isinstance(preparation, Mapping)
        or not isinstance(source_basis, Mapping)
        or not isinstance(segment, Mapping)
        or segment.get("segment_id") != item.segment_id
        or segment.get("segment_index") != item.segment_index
        or segment.get("text_sha256") != item.segment_text_sha256
        or transcript.get("transcript_id") != segment.get("transcript_id")
        or preparation.get("transcript_preparation_id")
        != source_basis.get("transcript_preparation_id")
        or preparation.get("transcript_preparation_status")
        != source_basis.get("transcript_preparation_status")
        or source_basis.get("transcript_id") != segment.get("transcript_id")
        or _sha256_bytes(item.segment_text.encode("utf-8"))
        != item.segment_text_sha256
    ):
        raise ProductionRunnerWaiting("verified segment source binding drifted")
    if dict(quality) != dict(preparation.get("segment_quality") or {}):
        raise ProductionRunnerWaiting("segment quality drifted from its DB provenance")
    return json.loads(_canonical_json(binding))


def _record(path: Path) -> dict[str, Any]:
    source = path.expanduser().resolve()
    if not source.is_file():
        raise ProductionRunnerError(f"required frozen artifact is missing: {source}")
    return {
        "path": str(source),
        "sha256": _sha256_file(source),
        "size_bytes": source.stat().st_size,
    }


def _planned_record(path: Path, content: bytes) -> dict[str, Any]:
    return {
        "path": str(path.expanduser().resolve()),
        "sha256": _sha256_bytes(content),
        "size_bytes": len(content),
    }


def _verify_record(value: Any, *, label: str) -> Path:
    if not isinstance(value, Mapping) or set(value) != {"path", "sha256", "size_bytes"}:
        raise ProductionRunnerError(f"{label} record shape drifted")
    path = Path(str(value.get("path") or "")).expanduser().resolve()
    size = value.get("size_bytes")
    digest = value.get("sha256")
    if (
        not path.is_file()
        or isinstance(size, bool)
        or not isinstance(size, int)
        or path.stat().st_size != size
        or not isinstance(digest, str)
        or _sha256_file(path) != digest
    ):
        raise ProductionRunnerError(f"{label} artifact drifted")
    return path


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProductionRunnerError(f"{label} is missing or malformed") from exc
    if not isinstance(value, dict):
        raise ProductionRunnerError(f"{label} must be a JSON object")
    return value


def _write_immutable(path: Path, content: bytes) -> None:
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if target.is_file() and target.read_bytes() == content:
            return
        raise ProductionRunnerError(
            f"immutable artifact already exists with different bytes: {target}"
        )
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(target, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        target.unlink(missing_ok=True)
        raise


def _verified_evaluation(
    evaluation: Any,
) -> tuple[dict[str, Any], dict[str, Any], Path]:
    if not isinstance(evaluation, Mapping) or set(evaluation) != {
        "root",
        "receipt",
        "evaluation_id",
        "frozen_configuration_sha256",
        "winner_system_id",
        "verified_artifact_hashes",
    }:
        raise ProductionRunnerError("evaluation binding shape drifted")
    evaluation_root = Path(str(evaluation.get("root") or "")).expanduser().resolve()
    if not evaluation_root.is_dir():
        raise ProductionRunnerError("evaluation root is unavailable")
    receipt_path = _verify_record(evaluation.get("receipt"), label="evaluation receipt")
    try:
        verified = verify_evaluation_receipt(receipt_path, evaluation_root)
    except (OSError, ValueError) as exc:
        raise ProductionRunnerError("strict evaluation receipt verification failed") from exc
    if (
        verified.get("evaluation_id") != evaluation.get("evaluation_id")
        or verified.get("artifact_hashes") != evaluation.get("verified_artifact_hashes")
    ):
        raise ProductionRunnerError("verified evaluation artifact bindings drifted")
    development_record = verified["artifact_hashes"].get("development_freeze")
    if not isinstance(development_record, Mapping) or set(development_record) != {
        "path",
        "sha256",
    }:
        raise ProductionRunnerError("verified development winner record drifted")
    development_path = Path(str(development_record["path"])).expanduser().resolve()
    if (
        not development_path.is_file()
        or _sha256_file(development_path) != development_record.get("sha256")
    ):
        raise ProductionRunnerError("verified development winner artifact drifted")
    development = _load_json(development_path, label="verified development winner")
    if (
        development.get("artifact_role") != "development_freeze"
        or development.get("development_frozen") is not True
        or development.get("selection_frozen") is not True
        or development.get("development_winner_frozen") is not True
        or development.get("winner_system_id") != evaluation.get("winner_system_id")
        or development.get("frozen_configuration_sha256")
        != evaluation.get("frozen_configuration_sha256")
        or isinstance(development.get("window_count"), bool)
        or not isinstance(development.get("window_count"), int)
        or int(development["window_count"]) < 1
        or isinstance(development.get("context_chars"), bool)
        or not isinstance(development.get("context_chars"), int)
        or int(development["context_chars"]) < 0
    ):
        raise ProductionRunnerError("verified evaluation winner binding drifted")
    return verified, development, receipt_path


def _validate_frozen_configuration(
    *,
    configuration_path: Path,
    expected_sha256: str,
    expected_winner: str,
) -> dict[str, Any]:
    if _sha256_file(configuration_path) != expected_sha256:
        raise ProductionRunnerError("frozen winner configuration drifted")
    try:
        frozen = production_contract.verify_frozen_configuration(configuration_path)
    except production_contract.CanonicalV31ProductionContractError as exc:
        raise ProductionRunnerError("frozen winner configuration drifted") from exc
    if (
        frozen.get("schema_version") != FROZEN_CONFIGURATION_VERSION
        or frozen.get("winner_system_id") != expected_winner
        or frozen.get("batch_size") not in ALLOWED_BATCH_SIZES
        or frozen.get("thread_mode") not in ALLOWED_THREAD_MODES
        or frozen.get("model") != episode_batch.MODEL
        or frozen.get("effort") != episode_batch.EFFORT
        or frozen.get("managed_chatgpt_auth_only") is not True
        or frozen.get("official_persistent_codex_app_server_only") is not True
        or frozen.get("retry_count") != 0
        or frozen.get("deterministic_semantic_pruning") is not False
        or frozen.get("deterministic_support_filtering") is not False
        or frozen.get("deterministic_deduplication") is not False
        or frozen.get("deterministic_relabeling") is not False
        or frozen.get("capacity_admission_contract")
        != production_contract.capacity_admission_contract()
    ):
        raise ProductionRunnerError("frozen winner configuration drifted")
    return dict(frozen)


def _execution_thread_contract(thread_mode: str) -> str:
    if thread_mode == "same_thread":
        return "one_same_thread_per_episode"
    if thread_mode == "new_thread":
        return "one_ephemeral_thread_per_batch"
    raise ProductionRunnerError("unsupported frozen thread mode")


def _bind_same_sqlite_context_authority(
    queue: QueueOperations,
    *,
    context_contract_path: Path,
    runtime_authorization_path: Path,
    completion_receipt_path: Path,
) -> Any:
    """Bind context verification to the production queue's exact connection."""

    if not isinstance(queue, SQLiteWorkerQueue):
        raise ProductionRunnerError(
            "database-current context verification requires the SQLite label queue"
        )
    if queue.database_identity.get("kind") != "sqlite_main_database":
        raise ProductionRunnerError(
            "database-current context verification requires file-backed SQLite"
        )
    try:
        context_loaded = episode_context_runner.load_episode_context_contract(
            context_contract_path
        )
        runtime = (
            episode_context_runner.load_live_episode_context_runtime_authorization(
                runtime_authorization_path, loaded=context_loaded
            )
        )
        context_queue = episode_context_runner.SQLiteEpisodeContextQueue(
            queue.conn,
            lane=episode_context_runner.LIVE_LANE,
            worker_id="canonical-production-context-verifier",
            label_pack=episode_context_runner.LIVE_LABEL_PACK,
            model=episode_context_runner.LIVE_QUEUE_PAYLOAD_MODEL,
            attempt_root=(
                completion_receipt_path.expanduser().resolve().parent
                / "managed-context-attempts"
            ),
        )
        context_queue.bind_execution_contract(context_loaded)
        manifest_record = runtime["provision_execution"][
            "required_episode_manifest"
        ]
        manifest_path = _verify_record(
            manifest_record, label="live provision required episode manifest"
        )
        manifest = episode_context_runner._validate_required_manifest(
            _load_json(manifest_path, label="live required episode manifest"),
            loaded=context_loaded,
        )
        context_queue.bind_required_episode_manifest(manifest, manifest_record)
        context_database = episode_context_runner._queue_main_database_identity(
            context_queue
        )
    except EpisodeContextRunnerError as exc:
        raise ProductionRunnerError(
            "official-live context SQLite authority could not be bound"
        ) from exc
    production_database = {
        key: queue.database_identity[key] for key in ("path", "device", "inode")
    }
    if (
        context_database != production_database
        or any(
            context_database != frozen_database
            for frozen_database in (
                runtime["cutover"]["database"],
                runtime["provision_plan"]["database"],
                runtime["provision_execution"]["database"],
            )
        )
    ):
        raise ProductionRunnerError(
            "production and official-live context SQLite authorities differ"
        )
    return context_queue


def load_contract(
    path: Path, *, queue: QueueOperations | None = None
) -> dict[str, Any]:
    contract_path = path.expanduser().resolve()
    contract = _load_json(contract_path, label="production runner contract")
    expected_keys = {
        "schema_version",
        "state",
        "thread_id",
        "cutover_id",
        "transport",
        "auth",
        "evaluation",
        "configuration",
        "artifacts",
        "episode_context",
        "execution",
    }
    if set(contract) != expected_keys:
        raise ProductionRunnerError("production runner contract shape drifted")
    auth = contract.get("auth")
    evaluation = contract.get("evaluation")
    config = contract.get("configuration")
    artifacts = contract.get("artifacts")
    episode_context = contract.get("episode_context")
    execution = contract.get("execution")
    if (
        contract.get("schema_version") != CONTRACT_VERSION
        or contract.get("state") != "frozen"
        or not isinstance(contract.get("thread_id"), str)
        or not str(contract["thread_id"]).strip()
        or not isinstance(contract.get("cutover_id"), str)
        or not str(contract["cutover_id"]).strip()
        or contract.get("transport") != TRANSPORT
        or auth
        != {
            "type": "chatgpt",
            "plan_type": "pro",
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
        }
        or not isinstance(config, Mapping)
        or set(config)
        != {
            "frozen_configuration_sha256",
            "winner_system_id",
            "model",
            "effort",
            "batch_size",
            "thread_mode",
            "timeout_seconds",
            "label_pack",
            "lane",
            "queue_payload_model",
            "window_count",
            "context_chars",
        }
        or not isinstance(config.get("frozen_configuration_sha256"), str)
        or len(str(config["frozen_configuration_sha256"])) != 64
        or not isinstance(config.get("winner_system_id"), str)
        or not str(config["winner_system_id"]).strip()
        or not isinstance(config.get("model"), str)
        or not isinstance(config.get("effort"), str)
        or isinstance(config.get("batch_size"), bool)
        or config.get("batch_size") not in ALLOWED_BATCH_SIZES
        or config.get("thread_mode") not in ALLOWED_THREAD_MODES
        or isinstance(config.get("timeout_seconds"), bool)
        or not isinstance(config.get("timeout_seconds"), (int, float))
        or float(config["timeout_seconds"]) <= 0
        or config.get("label_pack") != FROZEN_LABEL_PACK
        or config.get("lane") != FROZEN_QUEUE_LANE
        or not isinstance(config.get("queue_payload_model"), str)
        or not str(config["queue_payload_model"]).strip()
        or config.get("queue_payload_model") != FROZEN_QUEUE_PAYLOAD_MODEL
        or isinstance(config.get("window_count"), bool)
        or not isinstance(config.get("window_count"), int)
        or int(config["window_count"]) < 1
        or isinstance(config.get("context_chars"), bool)
        or not isinstance(config.get("context_chars"), int)
        or int(config["context_chars"]) < 0
        or not isinstance(artifacts, Mapping)
        or set(artifacts)
        != {
            "codex_binary",
            "production_runner",
            "canonical_production_contract",
            "production_promotion_authority",
            "frozen_configuration",
            "episode_batch_adapter",
            "episode_context_verifier",
            "windowing_source",
        }
        or not isinstance(episode_context, Mapping)
        or set(episode_context)
        != {
            "current_phase",
            "required_predecessor_phase",
            "context_contract",
            "runtime_authorization",
            "provision_execution_receipt",
            "live_completion_receipt",
            "completion_verifier",
            "artifact_verifier",
            "database_currentness",
            "overall_queue_completion_authorized",
        }
        or episode_context.get("current_phase") != "label_segment"
        or episode_context.get("required_predecessor_phase") != "episode_context"
        or episode_context.get("completion_verifier")
        != "verify_live_episode_context_completion_receipt"
        or episode_context.get("artifact_verifier")
        != "_verify_managed_context_artifact"
        or episode_context.get("database_currentness")
        != "same_sqlite_connection_queue_authority_at_execution"
        or episode_context.get("overall_queue_completion_authorized") is not False
    ):
        raise ProductionRunnerError("production runner contract values drifted")
    expected_execution = {
        "persistent_app_server_processes": 1,
        "thread_mode": _execution_thread_contract(str(config["thread_mode"])),
        "same_thread_persistence": "durable_non_ephemeral_until_episode_drained",
        "same_thread_recovery": "exact_thread_resume_no_completed_turn_replay",
        "same_thread_archive": "after_durable_episode_completion_receipt",
        "new_thread_persistence": "ephemeral_per_batch",
        "claim_scope": "one_exact_episode_batch",
        "queue_lane": FROZEN_QUEUE_LANE,
        "payload_label_pack_match": "exact",
        "payload_model_policy": "null_or_exact_frozen_queue_model",
        "capacity_admission": production_contract.capacity_admission_contract(),
        "failed_label_reconciliation": FAILED_LABEL_RECONCILIATION_POLICY,
        "semantic_retry_count": 0,
        "ambiguous_retry_allowed": False,
        "deterministic_semantic_pruning": False,
    }
    if execution != expected_execution:
        raise ProductionRunnerError("production runner execution contract drifted")
    artifact_paths = {
        name: _verify_record(artifacts[name], label=name) for name in sorted(artifacts)
    }
    if artifact_paths["codex_binary"].name != "codex":
        raise ProductionRunnerError("pinned Codex binary record is invalid")
    if artifact_paths["production_runner"] != Path(__file__).resolve():
        raise ProductionRunnerError("production runner source binding drifted")
    if artifact_paths["canonical_production_contract"] != Path(
        production_contract.__file__
    ).resolve():
        raise ProductionRunnerError(
            "canonical production contract source binding drifted"
        )
    if artifact_paths["episode_batch_adapter"] != Path(
        episode_batch.__file__
    ).resolve():
        raise ProductionRunnerError("episode batch adapter source binding drifted")
    if artifact_paths["episode_context_verifier"] != Path(
        episode_context_runner.__file__
    ).resolve():
        raise ProductionRunnerError("episode-context verifier source binding drifted")
    if artifact_paths["windowing_source"] != Path(
        build_windowed_segment_packet.__code__.co_filename
    ).resolve():
        raise ProductionRunnerError("windowing source binding drifted")
    verified, development, evaluation_receipt_path = _verified_evaluation(evaluation)
    frozen = _validate_frozen_configuration(
        configuration_path=artifact_paths["frozen_configuration"],
        expected_sha256=str(config["frozen_configuration_sha256"]),
        expected_winner=str(config["winner_system_id"]),
    )
    if (
        config.get("model") != frozen.get("model")
        or config.get("effort") != frozen.get("effort")
        or config.get("batch_size") != frozen.get("batch_size")
        or config.get("thread_mode") != frozen.get("thread_mode")
        or config.get("window_count") != development.get("window_count")
        or config.get("context_chars") != development.get("context_chars")
    ):
        raise ProductionRunnerError("runner configuration does not match accepted winner")
    context_contract_path = _verify_record(
        episode_context.get("context_contract"),
        label="episode-context contract",
    )
    runtime_authorization_path = _verify_record(
        episode_context.get("runtime_authorization"),
        label="episode-context runtime authorization",
    )
    provision_execution_path = _verify_record(
        episode_context.get("provision_execution_receipt"),
        label="episode-context provision execution receipt",
    )
    completion_receipt_path = _verify_record(
        episode_context.get("live_completion_receipt"),
        label="official-live episode-context completion receipt",
    )
    completion_envelope = _load_json(
        completion_receipt_path,
        label="official-live episode-context completion receipt",
    )
    if (
        completion_envelope.get("schema_version")
        != episode_context_runner.LIVE_COMPLETION_RECEIPT_VERSION
        or completion_envelope.get("state") != "passed"
        or completion_envelope.get("phase") != "episode_context"
        or completion_envelope.get("execution_authority") != "official_live"
        or completion_envelope.get("test_only") is not False
        or completion_envelope.get("promotable") is not True
        or completion_envelope.get("managed_chatgpt_auth_only") is not True
        or completion_envelope.get("label_queue_mutated") is not False
    ):
        raise ProductionRunnerError(
            "legacy, fixture-only, or nonpromotable context completion is forbidden"
        )
    context_queue = None
    if queue is not None:
        context_queue = _bind_same_sqlite_context_authority(
            queue,
            context_contract_path=context_contract_path,
            runtime_authorization_path=runtime_authorization_path,
            completion_receipt_path=completion_receipt_path,
        )
    try:
        promotion = production_contract.verify_production_promotion_authority(
            artifact_paths["production_promotion_authority"],
            frozen_configuration_path=artifact_paths["frozen_configuration"],
            episode_context_contract_path=context_contract_path,
            runtime_authorization_path=runtime_authorization_path,
            live_completion_receipt_path=completion_receipt_path,
            queue=context_queue,
        )
    except production_contract.CanonicalV31ProductionContractError as exc:
        raise ProductionRunnerError(
            "canonical production promotion failed strict verification"
        ) from exc
    verified_context = promotion["live_completion"]
    context_loaded = promotion["context_loaded"]
    context_runtime = promotion["runtime"]
    context_configuration = context_loaded["configuration"]
    if (
        context_configuration.get("label_pack") != FROZEN_LABEL_PACK
        or context_configuration.get("lane") != FROZEN_QUEUE_LANE
        or context_configuration.get("queue_payload_model")
        != FROZEN_QUEUE_PAYLOAD_MODEL
        or config.get("lane") != context_configuration.get("lane")
        or context_loaded["verified_evaluation"].get("evaluation_id")
        != verified.get("evaluation_id")
        or context_loaded["contract"]["lineage"].get("evaluation_receipt")
        != _record(evaluation_receipt_path)
        or context_runtime["provision_execution_path"] != provision_execution_path
        or verified_context.get("contract") != _record(context_contract_path)
        or verified_context.get("runtime_authorization")
        != _record(runtime_authorization_path)
        or verified_context.get("provision_execution_receipt")
        != _record(provision_execution_path)
        or verified_context.get("execution_authority") != "official_live"
        or verified_context.get("test_only") is not False
        or verified_context.get("promotable") is not True
    ):
        raise ProductionRunnerError(
            "episode-context lane, label-pack, or evaluation lineage drifted"
        )
    return {
        "path": contract_path,
        "sha256": _sha256_file(contract_path),
        "contract": contract,
        "artifact_paths": artifact_paths,
        "evaluation": verified,
        "evaluation_receipt_path": evaluation_receipt_path,
        "frozen_configuration": frozen,
        "episode_context": verified_context,
        "episode_context_contract": context_loaded,
        "episode_context_runtime": context_runtime,
        "production_promotion": promotion,
        "production_promotion_path": artifact_paths[
            "production_promotion_authority"
        ],
        "episode_context_completion_receipt_path": completion_receipt_path,
        "episode_context_fixture_ready": promotion["database_current"] is False,
        "episode_context_ready": promotion["database_current"] is True,
        "live_production_enabled": promotion["database_current"] is True,
    }


def _winner_fields(loaded: Mapping[str, Any]) -> dict[str, Any]:
    contract = loaded["contract"]
    config = contract["configuration"]
    return {
        "thread_id": contract["thread_id"],
        "cutover_id": contract["cutover_id"],
        "transport": TRANSPORT,
        "auth_mode": "chatgpt",
        "frozen_configuration_sha256": config["frozen_configuration_sha256"],
        "winner_system_id": config["winner_system_id"],
        "model": config["model"],
        "reasoning_effort": config["effort"],
        "batch_size": config["batch_size"],
        "thread_mode": config["thread_mode"],
        "queue_lane": config["lane"],
    }


def _runtime_config(
    loaded: Mapping[str, Any], *, context_control_overlay: Mapping[str, Any]
) -> dict[str, Any]:
    contract = loaded["contract"]
    artifacts = contract["artifacts"]
    winner = _winner_fields(loaded)
    return {
        "schema_version": RUNTIME_CONFIG_VERSION,
        "thread_id": winner["thread_id"],
        "cutover_id": winner["cutover_id"],
        "transport": TRANSPORT,
        "auth_mode": "chatgpt",
        "persistent_transport": True,
        "structured_output": True,
        "production_runner": artifacts["production_runner"],
        "evaluation_receipt": contract["evaluation"]["receipt"],
        "frozen_configuration": artifacts["frozen_configuration"],
        "frozen_configuration_sha256": winner["frozen_configuration_sha256"],
        "episode_batch_adapter": artifacts["episode_batch_adapter"],
        "episode_context_verifier": artifacts["episode_context_verifier"],
        "windowing_source": artifacts["windowing_source"],
        "episode_context_completion_receipt": contract["episode_context"][
            "live_completion_receipt"
        ],
        "production_promotion_authority": contract["artifacts"][
            "production_promotion_authority"
        ],
        "episode_context_artifact_index_sha256": loaded["episode_context"][
            "context_artifact_index_sha256"
        ],
        "winner_system_id": winner["winner_system_id"],
        "model": winner["model"],
        "reasoning_effort": winner["reasoning_effort"],
        "batch_size": winner["batch_size"],
        "thread_mode": contract["configuration"]["thread_mode"],
        "window_count": contract["configuration"]["window_count"],
        "context_chars": contract["configuration"]["context_chars"],
        "queue_label_pack": contract["configuration"]["label_pack"],
        "queue_lane": contract["configuration"]["lane"],
        "queue_payload_model_policy": "null_or_exact_frozen_queue_model",
        "queue_payload_model": contract["configuration"]["queue_payload_model"],
        "capacity_admission_contract": production_contract.capacity_admission_contract(),
        "context_control_overlay": dict(context_control_overlay),
        "context_control_overlay_sha256": loaded["frozen_configuration"][
            "runtime_binding"
        ]["context_control_overlay_sha256"],
        "effective_instruction_sources_sha256": (
            episode_batch.expected_instruction_source_contract()[
                "effective_instruction_sources_sha256"
            ]
        ),
        "effective_instruction_sources_count": (
            episode_batch.expected_instruction_source_contract()[
                "effective_instruction_sources_count"
            ]
        ),
        "live_production_enabled": loaded["live_production_enabled"],
        "promotion_database_current": loaded["production_promotion"][
            "database_current"
        ],
    }


def _receipt_base(loaded: Mapping[str, Any]) -> dict[str, Any]:
    winner = _winner_fields(loaded)
    return {
        "schema_version": RUN_RECEIPT_VERSION,
        "contract_sha256": loaded["sha256"],
        "evaluation_id": loaded["evaluation"]["evaluation_id"],
        **winner,
        "phase_scope": "label_segment_only",
        "required_predecessor_phase": "episode_context",
        "episode_context_completion_receipt": loaded["contract"]["episode_context"][
            "live_completion_receipt"
        ],
        "production_promotion_authority": loaded["contract"]["artifacts"][
            "production_promotion_authority"
        ],
        "episode_context_artifact_index_sha256": loaded["episode_context"][
            "context_artifact_index_sha256"
        ],
        "overall_queue_completion_authorized": False,
        "live_production_enabled": loaded["live_production_enabled"],
        "semantic_retry_count": 0,
    }


class SQLiteWorkerQueue:
    """Short, serialized queue transactions around the existing worker API."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        lane: str,
        worker_id: str,
        label_pack: str,
        model: str,
        output_root: Path,
        semantic_model: str = episode_batch.MODEL,
        source_loader: VerifiedSourceLoader | None = None,
    ) -> None:
        if label_pack != FROZEN_LABEL_PACK:
            raise ProductionRunnerError("production label pack is not the frozen v3.1 lane")
        if lane != FROZEN_QUEUE_LANE:
            raise ProductionRunnerError("production queue lane is not the frozen podcast lane")
        if model != FROZEN_QUEUE_PAYLOAD_MODEL:
            raise ProductionRunnerError("production queue model policy drifted")
        self.conn = conn
        self.lane = lane
        self.worker_id = worker_id
        self.label_pack = label_pack
        self.model = model
        self.semantic_model = semantic_model
        self.output_root = output_root.expanduser().resolve()
        self._source_loader = source_loader or VerifiedSourceLoader(conn)
        self.production_mutated = False
        database_rows = conn.execute("PRAGMA database_list").fetchall()
        main_rows = [row for row in database_rows if str(row[1]) == "main"]
        if len(main_rows) != 1:
            raise ProductionRunnerError("SQLite main database identity is ambiguous")
        if str(main_rows[0][2]):
            database_path = Path(str(main_rows[0][2])).expanduser().resolve(strict=True)
            stat = database_path.stat()
            self.database_identity = {
                "kind": "sqlite_main_database",
                "path": str(database_path),
                "device": int(stat.st_dev),
                "inode": int(stat.st_ino),
            }
        else:
            self.database_identity = {
                "kind": "sqlite_nonfile_test_database",
                "connection_identity": id(conn),
            }

    def _item_from_job(self, job: Mapping[str, Any]) -> QueueItem:
        segment_id = str(job["target_id"])
        try:
            packet = self._source_loader.segment_packet(segment_id)
        except SourceIntegrityError as exc:
            raise ProductionRunnerWaiting(
                "segment source integrity failed before claim or transport"
            ) from exc
        context = packet["context"]
        integrity = packet["source_integrity"]
        segment = integrity["segment"]
        row = self.conn.execute(
            """
            SELECT
              segments.word_count AS segment_word_count,
              segments.text_sha256 AS segment_text_sha256,
              transcripts.id AS transcript_id,
              transcripts.raw_text_path,
              transcripts.raw_text_sha256,
              transcript_preparations.id AS transcript_preparation_id,
              transcript_preparations.cleaned_text_path,
              transcript_preparations.cleaned_text_sha256,
              transcript_preparations.artifact_type,
              transcript_preparations.status AS transcript_preparation_status,
              transcript_preparations.substantive_word_count,
              transcript_preparations.boilerplate_ratio
            FROM segments
            JOIN transcripts ON transcripts.id = segments.transcript_id
            JOIN transcript_preparations
              ON transcript_preparations.transcript_id = segments.transcript_id
            WHERE segments.id = ?
            """,
            (segment_id,),
        ).fetchone()
        if row is None:
            raise ProductionRunnerWaiting(
                "segment database provenance disappeared during source verification"
            )
        try:
            _raw_text, raw_file = self._source_loader._verified_text(  # noqa: SLF001
                row["raw_text_path"],
                row["raw_text_sha256"],
                label="raw transcript lineage",
            )
        except SourceIntegrityError as exc:
            raise ProductionRunnerWaiting(
                "raw transcript lineage failed before claim or transport"
            ) from exc
        segment_text = str(packet["segment_text"])
        segment_text_sha256 = str(row["segment_text_sha256"])
        if (
            segment.get("text_sha256") != segment_text_sha256
            or _sha256_bytes(segment_text.encode("utf-8"))
            != segment_text_sha256
        ):
            raise ProductionRunnerWaiting(
                "segment bytes drifted from segments.text_sha256"
            )
        segment_quality = _segment_quality_from_db_provenance(
            context,
            fallback_segment_word_count=row["segment_word_count"],
        )
        source_binding = {
            "schema_version": SOURCE_INTEGRITY_VERSION,
            "episode_id": str(context["episode_id"]),
            "source_id": str(context["source_id"]),
            "transcript": {
                "transcript_id": str(row["transcript_id"]),
                "raw_text_path": str(row["raw_text_path"]),
                "raw_text_sha256": str(row["raw_text_sha256"]),
                "verified_file": raw_file,
            },
            "transcript_preparation": {
                "transcript_preparation_id": str(
                    row["transcript_preparation_id"]
                ),
                "transcript_preparation_status": str(
                    row["transcript_preparation_status"]
                ),
                "cleaned_text_path": row["cleaned_text_path"],
                "cleaned_text_sha256": row["cleaned_text_sha256"],
                "artifact_type": str(row["artifact_type"]),
                "substantive_word_count": row["substantive_word_count"],
                "boilerplate_ratio": row["boilerplate_ratio"],
                "segment_quality": segment_quality,
            },
            "transcript_source_basis": integrity["transcript_source_basis"],
            "segment": segment,
        }
        return QueueItem(
            job_id=int(job["id"]),
            segment_id=segment_id,
            episode_id=str(context["episode_id"]),
            segment_index=int(context["segment_index"]),
            segment_text=segment_text,
            label_run_id="preview",
            output_path="preview",
            source_name=str(context.get("source_name") or ""),
            episode_title=str(context.get("episode_title") or ""),
            segment_text_sha256=segment_text_sha256,
            segment_quality=segment_quality,
            expected_source_binding=source_binding,
        )

    def _assert_source_items_current(self, items: Sequence[QueueItem]) -> None:
        for expected in items:
            row = self.conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (expected.job_id,)
            ).fetchone()
            if row is None or str(row["target_id"]) != expected.segment_id:
                raise ProductionRunnerWaiting(
                    "verified source job identity changed before atomic claim"
                )
            current = self._item_from_job(row)
            if any(
                getattr(current, field) != getattr(expected, field)
                for field in (
                    "job_id",
                    "segment_id",
                    "episode_id",
                    "segment_index",
                    "segment_text",
                    "source_name",
                    "episode_title",
                    "segment_text_sha256",
                    "segment_quality",
                    "expected_source_binding",
                )
            ):
                raise ProductionRunnerWaiting(
                    "verified source binding changed before atomic claim"
                )

    def verify_source_packet(
        self, source_packet: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        rows = source_packet.get("segments")
        if not isinstance(rows, list) or not rows:
            raise ProductionRunnerError("completed source packet is empty")
        verified_ids: list[str] = []
        for expected in rows:
            if not isinstance(expected, Mapping):
                raise ProductionRunnerError("completed source row is malformed")
            job_id = expected.get("job_id")
            if isinstance(job_id, bool) or not isinstance(job_id, int):
                raise ProductionRunnerError("completed source job id is malformed")
            job = self.conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if job is None:
                raise ProductionRunnerError("completed source job disappeared")
            current = self._item_from_job(job)
            binding = _validate_expected_source_binding(current)
            if (
                current.segment_id != expected.get("segment_id")
                or current.episode_id != source_packet.get("episode_id")
                or current.segment_index != expected.get("segment_index")
                or current.segment_text_sha256
                != expected.get("segment_text_sha256")
                or dict(current.segment_quality or {})
                != expected.get("segment_quality")
                or binding != expected.get("expected_source_binding")
                or _source_binding_sha256(binding)
                != expected.get("expected_source_binding_sha256")
            ):
                raise ProductionRunnerError(
                    "completed source packet drifted from current DB provenance"
                )
            verified_ids.append(current.segment_id)
        return {
            "state": "verified_current_source_packet",
            "segment_ids": verified_ids,
            "segment_count": len(verified_ids),
            "production_mutated": False,
        }

    def _scoped_episode_rows(
        self,
        *,
        limit: int,
        status: str,
        episode_id: str | None = None,
    ) -> list[sqlite3.Row]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ProductionRunnerError("episode batch limit must be positive")
        stale_clause = " AND j.leased_until < ?" if status == "claimed" else ""
        attempts_clause = "" if status == "claimed" else " AND j.attempts < j.max_attempts"
        model_clause = (
            """
                AND (
                  json_extract(j.payload_json, '$.queue_payload_model') = ?
                  OR (
                    json_extract(j.payload_json, '$.queue_payload_model') IS NULL
                    AND (
                      json_extract(j.payload_json, '$.model') IS NULL
                      OR json_extract(j.payload_json, '$.model') = ?
                    )
                  )
                )
            """
            if status == "claimed"
            else """
                AND (
                  json_extract(j.payload_json, '$.model') IS NULL
                  OR json_extract(j.payload_json, '$.model') = ?
                )
            """
        )
        params: list[Any] = []
        for _ in range(2):
            params.extend([self.lane, self.label_pack, self.model])
            if status == "claimed":
                params.append(self.model)
            params.append(status)
            if status == "claimed":
                params.append(now_iso())
        params.extend([episode_id, limit])
        rows = self.conn.execute(
            f"""
            WITH next_episode AS (
              SELECT s.episode_id
              FROM jobs AS j
              JOIN segments AS s ON s.id = j.target_id
              WHERE j.lane = ?
                AND j.job_type = 'label_segment'
                AND json_extract(j.payload_json, '$.label_pack') = ?
                {model_clause}
                AND j.status = ?
                {attempts_clause}
                {stale_clause}
              ORDER BY j.priority, j.id
              LIMIT 1
            )
            SELECT j.*
            FROM jobs AS j
            JOIN segments AS s ON s.id = j.target_id
            WHERE j.lane = ?
              AND j.job_type = 'label_segment'
              AND json_extract(j.payload_json, '$.label_pack') = ?
              {model_clause}
              AND j.status = ?
              {attempts_clause}
              {stale_clause}
              AND s.episode_id = COALESCE(?, (SELECT episode_id FROM next_episode))
            ORDER BY s.segment_index, j.priority, j.id
            LIMIT ?
            """,
            params,
        ).fetchall()
        return list(rows)

    def preview_episode_batch(
        self, *, limit: int, episode_id: str | None = None
    ) -> list[QueueItem]:
        return [self._item_from_job(row) for row in self._scoped_episode_rows(
            limit=limit, status="pending", episode_id=episode_id
        )]

    def pending_episode_segment_count(self, *, episode_id: str) -> int:
        return len(
            self._scoped_episode_rows(
                limit=2_147_483_647,
                status="pending",
                episode_id=episode_id,
            )
        )

    def unresolved_episode_segment_count(self, *, episode_id: str) -> int:
        return self._unresolved_scoped_job_count(episode_id=episode_id)

    def _unresolved_scoped_job_count(
        self, *, episode_id: str | None = None
    ) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM jobs AS j
            JOIN segments AS s ON s.id = j.target_id
            WHERE j.lane = ?
              AND j.job_type = 'label_segment'
              AND (? IS NULL OR s.episode_id = ?)
              AND j.status IN ('pending', 'claimed', 'failed')
              AND json_extract(j.payload_json, '$.label_pack') = ?
              AND (
                json_extract(j.payload_json, '$.queue_payload_model') = ?
                OR (
                  json_extract(j.payload_json, '$.queue_payload_model') IS NULL
                  AND (
                    json_extract(j.payload_json, '$.model') IS NULL
                    OR json_extract(j.payload_json, '$.model') = ?
                  )
                )
              )
            """,
            (
                self.lane,
                episode_id,
                episode_id,
                self.label_pack,
                self.model,
                self.model,
            ),
        ).fetchone()
        return int(row["count"] if row is not None else 0)

    def unresolved_scoped_job_count(self) -> int:
        return self._unresolved_scoped_job_count()

    @staticmethod
    def _reset_failed_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
        reset = dict(payload)
        original_present = reset.pop(
            "queue_payload_model_original_present", None
        )
        original_model = reset.pop("queue_payload_model_original", None)
        if original_present is True:
            reset["model"] = original_model
        elif original_present is False:
            reset.pop("model", None)
        elif "queue_payload_model" in reset:
            raise ProductionRunnerError(
                "failed app-server job lacks original queue-model provenance"
            )
        for key in (
            "label_run_id",
            "output_path",
            "prompt_path",
            "queue_payload_model",
            "app_server_prepared_request_path",
            "app_server_claim_receipt_path",
            "app_server_prepared_request_sha256",
            "app_server_recovered_prelaunch_label_run_id",
            "app_server_recovered_prelaunch_claim_sha256",
            "app_server_launch_path",
            "app_server_sidecar_path",
            "app_server_turn_id",
            "app_server_thread_id",
        ):
            reset.pop(key, None)
        return reset

    def _failed_label_run_provenance(
        self,
        *,
        payload: Mapping[str, Any],
        label_run: Mapping[str, Any],
    ) -> tuple[str, str, str | None]:
        """Return the frozen provenance class, expected run model, and evidence hash."""

        if "queue_payload_model" in payload:
            original_present = payload.get(
                "queue_payload_model_original_present"
            )
            original_model = payload.get("queue_payload_model_original")
            if (
                payload.get("queue_payload_model") != self.model
                or payload.get("model") != self.semantic_model
                or not isinstance(original_present, bool)
                or (original_present and original_model != self.model)
                or (not original_present and original_model is not None)
            ):
                raise ProductionRunnerError(
                    "failed app-server label-run queue provenance drifted"
                )
            return "managed_app_server_attempt", self.semantic_model, None

        if (
            payload.get("model") != self.model
            or "queue_payload_model_original_present" in payload
            or "queue_payload_model_original" in payload
        ):
            raise ProductionRunnerError(
                "failed legacy label-run model provenance is unprovable"
            )
        artifact_records = []
        for field in ("prompt_path", "output_path"):
            raw_path = label_run[field]
            if not isinstance(raw_path, str) or not raw_path:
                raise ProductionRunnerError(
                    "failed legacy label-run artifact lineage is incomplete"
                )
            path = Path(raw_path).expanduser().resolve()
            if not path.is_file():
                raise ProductionRunnerError(
                    "failed legacy label-run artifact is missing"
                )
            artifact_records.append(_record(path))
        return (
            "legacy_semantic_attempts_failed_for_fresh_retry",
            self.model,
            _sha256_bytes(
                _canonical_json(artifact_records).encode("utf-8")
            ),
        )

    def reconcile_failed_scoped_jobs(
        self, *, execute: bool
    ) -> Mapping[str, Any]:
        rows = self.conn.execute(
            """
            SELECT j.id, j.target_id, j.payload_json
            FROM jobs AS j
            WHERE j.lane = ?
              AND j.job_type = 'label_segment'
              AND j.status = 'failed'
              AND json_extract(j.payload_json, '$.label_pack') = ?
              AND (
                json_extract(j.payload_json, '$.queue_payload_model') = ?
                OR (
                  json_extract(j.payload_json, '$.queue_payload_model') IS NULL
                  AND (
                    json_extract(j.payload_json, '$.model') IS NULL
                    OR json_extract(j.payload_json, '$.model') = ?
                  )
                )
              )
            ORDER BY j.id
            """,
            (self.lane, self.label_pack, self.model, self.model),
        ).fetchall()
        planned = []
        claimed_run_count = 0
        managed_claimed_run_count = 0
        legacy_semantic_attempt_count = 0
        legacy_artifact_hashes: list[str] = []
        for row in rows:
            job_id = int(row["id"])
            target_id = str(row["target_id"])
            payload = loads_json(str(row["payload_json"]), {})
            label_run_id = str(payload.get("label_run_id") or "")
            claimed_runs = self.conn.execute(
                "SELECT * FROM label_runs WHERE job_id = ? AND status = 'claimed'",
                (job_id,),
            ).fetchall()
            referenced_run = (
                self.conn.execute(
                    "SELECT * FROM label_runs WHERE id = ?", (label_run_id,)
                ).fetchone()
                if label_run_id
                else None
            )
            if label_run_id:
                if (
                    referenced_run is None
                    or int(referenced_run["job_id"]) != job_id
                    or str(referenced_run["segment_id"]) != target_id
                    or str(referenced_run["label_pack"]) != self.label_pack
                    or str(referenced_run["status"]) not in {"claimed", "failed"}
                    or any(
                        str(run["id"]) != label_run_id for run in claimed_runs
                    )
                ):
                    raise ProductionRunnerError(
                        "failed label-run reconciliation lineage drifted"
                    )
                run_status = str(referenced_run["status"])
                (
                    run_provenance,
                    expected_run_model,
                    legacy_artifact_hash,
                ) = self._failed_label_run_provenance(
                    payload=payload,
                    label_run=referenced_run,
                )
                if str(referenced_run["model"]) != expected_run_model:
                    raise ProductionRunnerError(
                        "failed label-run reconciliation model drifted"
                    )
            elif claimed_runs:
                raise ProductionRunnerError(
                    "failed job has an unbound claimed label run"
                )
            else:
                run_status = "absent"
                run_provenance = "no_bound_semantic_attempt"
                expected_run_model = ""
                legacy_artifact_hash = None
            if run_status == "claimed":
                claimed_run_count += 1
                if run_provenance == "managed_app_server_attempt":
                    managed_claimed_run_count += 1
                elif (
                    run_provenance
                    == "legacy_semantic_attempts_failed_for_fresh_retry"
                ):
                    legacy_semantic_attempt_count += 1
                    if legacy_artifact_hash is None:
                        raise ProductionRunnerError(
                            "legacy semantic-attempt evidence hash is missing"
                        )
                    legacy_artifact_hashes.append(legacy_artifact_hash)
            planned.append(
                (
                    job_id,
                    target_id,
                    _canonical_json(self._reset_failed_payload(payload)),
                    str(row["payload_json"]),
                    label_run_id,
                    run_status,
                    run_provenance,
                    expected_run_model,
                    legacy_artifact_hash,
                )
            )
        job_ids = [job_id for job_id, *_rest in planned]
        result = {
            "schema_version": FAILED_LABEL_RECONCILIATION_VERSION,
            "state": "planned" if planned else "no_change",
            "lane": self.lane,
            "label_pack": self.label_pack,
            "queue_payload_model": self.model,
            "failed_job_count": len(planned),
            "claimed_label_run_count": claimed_run_count,
            "managed_app_server_claimed_label_run_count": (
                managed_claimed_run_count
            ),
            "legacy_semantic_attempts_failed_for_fresh_retry_count": (
                legacy_semantic_attempt_count
            ),
            "legacy_semantic_attempt_artifacts_sha256": _sha256_bytes(
                _canonical_json(sorted(legacy_artifact_hashes)).encode("utf-8")
            ),
            "failed_job_ids_sha256": _sha256_bytes(
                _canonical_json(job_ids).encode("utf-8")
            ),
            "execute": execute,
            "production_mutated": False,
        }
        if not execute or not planned:
            return result
        timestamp = now_iso()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            reconciled_claimed_runs = 0
            for (
                job_id,
                target_id,
                reset_payload,
                prior_payload,
                label_run_id,
                run_status,
                run_provenance,
                expected_run_model,
                legacy_artifact_hash,
            ) in planned:
                current_job = self.conn.execute(
                    "SELECT target_id, payload_json, status FROM jobs WHERE id = ?",
                    (job_id,),
                ).fetchone()
                if (
                    current_job is None
                    or str(current_job["status"]) != "failed"
                    or str(current_job["target_id"]) != target_id
                    or str(current_job["payload_json"]) != prior_payload
                ):
                    raise ProductionRunnerWaiting(
                        "failed label reset changed before atomic commit"
                    )
                if label_run_id:
                    current_run = self.conn.execute(
                        "SELECT * FROM label_runs WHERE id = ?", (label_run_id,)
                    ).fetchone()
                    if (
                        current_run is None
                        or int(current_run["job_id"]) != job_id
                        or str(current_run["segment_id"]) != target_id
                        or str(current_run["label_pack"]) != self.label_pack
                        or str(current_run["model"]) != expected_run_model
                        or str(current_run["status"]) != run_status
                    ):
                        raise ProductionRunnerWaiting(
                            "failed label-run changed before atomic reset"
                        )
                    current_provenance = self._failed_label_run_provenance(
                        payload=loads_json(prior_payload, {}),
                        label_run=current_run,
                    )
                    if current_provenance != (
                        run_provenance,
                        expected_run_model,
                        legacy_artifact_hash,
                    ):
                        raise ProductionRunnerWaiting(
                            "failed label-run provenance changed before atomic reset"
                        )
                    if run_status == "claimed":
                        run_update = self.conn.execute(
                            """
                            UPDATE label_runs
                            SET status = 'failed',
                                error = ?, updated_at = ?
                            WHERE id = ? AND job_id = ? AND status = 'claimed'
                            """,
                            (
                                "Reconciled failed app-server job for bounded retry.",
                                timestamp,
                                label_run_id,
                                job_id,
                            ),
                        )
                        if run_update.rowcount != 1:
                            raise ProductionRunnerWaiting(
                                "claimed label-run reset was not atomic"
                            )
                        reconciled_claimed_runs += 1
                updated = self.conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending', attempts = 0,
                        lease_owner = NULL, leased_until = NULL,
                        completed_at = NULL, error = NULL,
                        payload_json = ?, updated_at = ?
                    WHERE id = ?
                      AND lane = ?
                      AND job_type = 'label_segment'
                      AND status = 'failed'
                      AND payload_json = ?
                    """,
                    (
                        reset_payload,
                        timestamp,
                        job_id,
                        self.lane,
                        prior_payload,
                    ),
                )
                if updated.rowcount != 1:
                    raise ProductionRunnerWaiting(
                        "failed label reset changed before atomic commit"
                    )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        self.production_mutated = True
        return {
            **result,
            "state": "reconciled",
            "reconciled_claimed_label_run_count": reconciled_claimed_runs,
            "production_mutated": True,
        }

    def assert_episode_start_allowed(
        self,
        episode_id: str,
        *,
        thread_mode: str,
        output_root: Path,
    ) -> None:
        if thread_mode != "same_thread":
            return
        expected_root = Path(output_root).expanduser().resolve()
        if expected_root != self.output_root:
            raise ProductionRunnerError(
                "queue output root does not match the active production root"
            )
        rows = self.conn.execute(
            """
            SELECT j.payload_json
            FROM jobs AS j
            JOIN segments AS s ON s.id = j.target_id
            WHERE j.lane = ?
              AND j.job_type = 'label_segment'
              AND j.status = 'completed'
              AND s.episode_id = ?
              AND json_extract(j.payload_json, '$.label_pack') = ?
              AND json_extract(j.payload_json, '$.model') = ?
              AND json_extract(
                    j.payload_json,
                    '$.app_server_prepared_request_path'
                  ) IS NOT NULL
            """,
            (self.lane, episode_id, self.label_pack, self.semantic_model),
        ).fetchall()
        for row in rows:
            payload = loads_json(str(row["payload_json"]), {})
            request_path = Path(
                str(payload.get("app_server_prepared_request_path") or "")
            ).expanduser().resolve()
            try:
                request_path.relative_to(expected_root)
            except ValueError as exc:
                raise ProductionRunnerWaiting(
                    "same-thread episode has completed production lineage in another root"
                ) from exc

    def stale_episode_batch(self, *, limit: int) -> list[QueueItem]:
        return [self._item_from_job(row) for row in self._scoped_episode_rows(
            limit=limit, status="claimed"
        )]

    def claim_episode_batch(
        self,
        items: Sequence[QueueItem],
        *,
        prepared_request_path: Path,
        claim_receipt_path: Path,
    ) -> list[QueueItem]:
        if not items or len(items) > 8:
            raise ProductionRunnerError("claim must contain one bounded episode batch")
        episode_ids = {item.episode_id for item in items}
        if len(episode_ids) != 1:
            raise ProductionRunnerError("claim crossed episode boundaries")
        self._assert_source_items_current(items)
        expected_ids = [item.job_id for item in items]
        prepared_request = _load_json(
            prepared_request_path.expanduser().resolve(),
            label="prepared episode-batch request",
        )
        if (
            prepared_request.get("episode_id") != items[0].episode_id
            or prepared_request.get("segment_ids")
            != [item.segment_id for item in items]
        ):
            raise ProductionRunnerError(
                "prepared request does not match the exact atomic claim"
            )
        request_record = _record(prepared_request_path)
        timestamp = now_iso()
        leased_until = (
            datetime.fromisoformat(timestamp) + timedelta(minutes=45)
        ).isoformat()
        placeholders = ", ".join("?" for _ in expected_ids)
        claimed: list[QueueItem] = []
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            rows = self.conn.execute(
                f"""
                SELECT j.*, s.episode_id AS scoped_episode_id,
                       s.segment_index AS scoped_segment_index
                FROM jobs AS j
                JOIN segments AS s ON s.id = j.target_id
                WHERE j.id IN ({placeholders})
                  AND j.lane = ?
                  AND j.job_type = 'label_segment'
                  AND j.status = 'pending'
                  AND j.attempts < j.max_attempts
                  AND json_extract(j.payload_json, '$.label_pack') = ?
                  AND (
                    json_extract(j.payload_json, '$.model') IS NULL
                    OR json_extract(j.payload_json, '$.model') = ?
                  )
                ORDER BY s.segment_index, j.priority, j.id
                """,
                [*expected_ids, self.lane, self.label_pack, self.model],
            ).fetchall()
            if [int(row["id"]) for row in rows] != expected_ids:
                raise ProductionRunnerWaiting(
                    "prepared episode batch changed before exact atomic claim"
                )
            if any(
                str(row["target_id"]) != preview.segment_id
                or str(row["scoped_episode_id"]) != preview.episode_id
                or int(row["scoped_segment_index"]) != preview.segment_index
                for preview, row in zip(items, rows)
            ):
                raise ProductionRunnerWaiting(
                    "prepared episode batch identity changed before exact atomic claim"
                )
            cursor = self.conn.execute(
                f"""
                UPDATE jobs
                SET status = 'claimed', lease_owner = ?, leased_until = ?,
                    attempts = attempts + 1, updated_at = ?
                WHERE id IN ({placeholders})
                  AND lane = ?
                  AND job_type = 'label_segment'
                  AND status = 'pending'
                  AND attempts < max_attempts
                  AND json_extract(payload_json, '$.label_pack') = ?
                  AND (
                    json_extract(payload_json, '$.model') IS NULL
                    OR json_extract(payload_json, '$.model') = ?
                  )
                """,
                [
                    self.worker_id,
                    leased_until,
                    timestamp,
                    *expected_ids,
                    self.lane,
                    self.label_pack,
                    self.model,
                ],
            )
            if cursor.rowcount != len(expected_ids):
                raise ProductionRunnerWaiting("exact episode batch claim was incomplete")
            for preview, row in zip(items, rows):
                prior_run_count = int(
                    self.conn.execute(
                        "SELECT COUNT(*) FROM label_runs WHERE job_id = ?",
                        (preview.job_id,),
                    ).fetchone()[0]
                )
                run_id = stable_id(
                    str(preview.job_id),
                    self.worker_id,
                    request_record["sha256"],
                    str(prior_run_count),
                    prefix="aspr_",
                )
                output_path = self.output_root / "segment-outputs" / f"{run_id}.json"
                payload = loads_json(str(row["payload_json"]), {})
                original_model_present = "model" in payload
                original_model = payload.get("model")
                payload.update(
                    {
                        "label_pack": self.label_pack,
                        "model": self.semantic_model,
                        "queue_payload_model": self.model,
                        "queue_payload_model_original_present": original_model_present,
                        "queue_payload_model_original": original_model,
                        "label_run_id": run_id,
                        "output_path": str(output_path),
                        "app_server_prepared_request_path": str(
                            prepared_request_path.resolve()
                        ),
                        "app_server_claim_receipt_path": str(
                            claim_receipt_path.resolve()
                        ),
                        "app_server_prepared_request_sha256": request_record["sha256"],
                    }
                )
                self.conn.execute(
                    """
                    INSERT INTO label_runs
                      (id, job_id, segment_id, label_pack, model, output_path, status,
                       claimed_at, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'claimed', ?, ?, ?)
                    """,
                    (
                        run_id,
                        preview.job_id,
                        preview.segment_id,
                        self.label_pack,
                        self.semantic_model,
                        str(output_path),
                        timestamp,
                        timestamp,
                        timestamp,
                    ),
                )
                self.conn.execute(
                    "UPDATE jobs SET payload_json = ?, updated_at = ? WHERE id = ?",
                    (_canonical_json(payload), timestamp, preview.job_id),
                )
                claimed.append(
                    QueueItem(
                        **{
                            **asdict(preview),
                            "label_run_id": run_id,
                            "output_path": str(output_path),
                        }
                    )
                )
            self.conn.commit()
            self.production_mutated = True
        except Exception:
            self.conn.rollback()
            raise
        claim_receipt = {
            "schema_version": CLAIM_RECEIPT_VERSION,
            "state": "claimed",
            "episode_id": items[0].episode_id,
            "job_ids": expected_ids,
            "segment_ids": [item.segment_id for item in items],
            "worker_id": self.worker_id,
            "lane": self.lane,
            "label_pack": self.label_pack,
            "payload_model_policy": "null_or_exact_frozen_queue_model",
            "queue_payload_model": self.model,
            "prepared_request": request_record,
            "claimed_at": timestamp,
            "leased_until": leased_until,
            "semantic_retry_count": 0,
        }
        _write_immutable(
            claim_receipt_path,
            _pretty_json(claim_receipt).encode("utf-8"),
        )
        return claimed

    def recover_stale_prelaunch_batch(self, *, limit: int) -> Mapping[str, Any]:
        """Release only a fully bound stale claim with no semantic-attempt artifact."""

        stale = self.stale_episode_batch(limit=limit)
        if not stale:
            return {
                "state": "none",
                "job_ids": [],
                "semantic_model_call_count": 0,
                "production_mutated": False,
            }
        if len({item.episode_id for item in stale}) != 1:
            raise ProductionRunnerError("stale recovery crossed episode boundaries")
        expected_ids = [item.job_id for item in stale]
        placeholders = ", ".join("?" for _ in expected_ids)
        timestamp = now_iso()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            rows = self.conn.execute(
                f"""
                SELECT j.*, s.episode_id AS scoped_episode_id,
                       s.segment_index AS scoped_segment_index
                FROM jobs AS j
                JOIN segments AS s ON s.id = j.target_id
                WHERE j.id IN ({placeholders})
                  AND j.lane = ?
                  AND j.job_type = 'label_segment'
                  AND j.status = 'claimed'
                  AND j.leased_until < ?
                  AND json_extract(j.payload_json, '$.label_pack') = ?
                  AND (
                    json_extract(j.payload_json, '$.queue_payload_model') = ?
                    OR (
                      json_extract(j.payload_json, '$.queue_payload_model') IS NULL
                      AND (
                        json_extract(j.payload_json, '$.model') IS NULL
                        OR json_extract(j.payload_json, '$.model') = ?
                      )
                    )
                  )
                ORDER BY s.segment_index, j.priority, j.id
                """,
                [
                    *expected_ids,
                    self.lane,
                    timestamp,
                    self.label_pack,
                    self.model,
                    self.model,
                ],
            ).fetchall()
            if [int(row["id"]) for row in rows] != expected_ids:
                raise ProductionRunnerWaiting(
                    "stale claim changed before recovery transaction"
                )
            payloads = [loads_json(str(row["payload_json"]), {}) for row in rows]
            request_paths = {
                str(payload.get("app_server_prepared_request_path") or "")
                for payload in payloads
            }
            request_hashes = {
                str(payload.get("app_server_prepared_request_sha256") or "")
                for payload in payloads
            }
            claim_paths = {
                str(payload.get("app_server_claim_receipt_path") or "")
                for payload in payloads
            }
            if (
                len(request_paths) != 1
                or len(request_hashes) != 1
                or len(claim_paths) != 1
                or "" in request_paths
                or "" in request_hashes
                or "" in claim_paths
            ):
                raise ProductionRunnerError("stale claim artifact binding is incomplete")
            request_path = Path(next(iter(request_paths))).expanduser().resolve()
            request = _load_json(request_path, label="stale prepared request")
            if (
                _sha256_file(request_path) != next(iter(request_hashes))
                or request.get("episode_id") != stale[0].episode_id
                or request.get("segment_ids") != [item.segment_id for item in stale]
            ):
                raise ProductionRunnerError("stale prepared request lineage drifted")
            claim_path = Path(next(iter(claim_paths))).expanduser().resolve()
            claim_sha256 = None
            if claim_path.exists():
                claim = _load_json(claim_path, label="stale claim receipt")
                if (
                    claim.get("schema_version") != CLAIM_RECEIPT_VERSION
                    or claim.get("state") != "claimed"
                    or claim.get("episode_id") != stale[0].episode_id
                    or claim.get("job_ids") != expected_ids
                    or claim.get("segment_ids")
                    != [item.segment_id for item in stale]
                    or claim.get("lane") != self.lane
                    or claim.get("label_pack") != self.label_pack
                    or claim.get("payload_model_policy")
                    != "null_or_exact_frozen_queue_model"
                    or claim.get("queue_payload_model") != self.model
                    or claim.get("prepared_request") != _record(request_path)
                    or claim.get("semantic_retry_count") != 0
                ):
                    raise ProductionRunnerError("stale claim receipt lineage drifted")
                claim_sha256 = _sha256_file(claim_path)
            turn_root = request_path.parent
            semantic_artifacts = [
                turn_root / name
                for name in (
                    "launch.json",
                    "sidecar.json",
                    "output.private.json",
                    "terminal.json",
                )
                if (turn_root / name).exists()
            ]
            if semantic_artifacts:
                self.conn.rollback()
                return {
                    "state": "semantic_attempt_preserved",
                    "job_ids": expected_ids,
                    "semantic_model_call_count": None,
                    "usage": None,
                    "accounting_complete": False,
                    "artifact_paths": [str(path) for path in semantic_artifacts],
                    "production_mutated": False,
                }
            for row, payload in zip(rows, payloads):
                label_run_id = str(payload.get("label_run_id") or "")
                label_run = self.conn.execute(
                    "SELECT * FROM label_runs WHERE id = ?", (label_run_id,)
                ).fetchone()
                if (
                    not label_run_id
                    or label_run is None
                    or int(label_run["job_id"]) != int(row["id"])
                    or str(label_run["segment_id"]) != str(row["target_id"])
                    or str(label_run["status"]) != "claimed"
                    or str(label_run["label_pack"]) != self.label_pack
                    or str(label_run["model"]) != self.semantic_model
                ):
                    raise ProductionRunnerError("stale label-run lineage drifted")
            for row, payload in zip(rows, payloads):
                original_present = payload.pop(
                    "queue_payload_model_original_present", None
                )
                original_model = payload.pop("queue_payload_model_original", None)
                if original_present is True:
                    payload["model"] = original_model
                elif original_present is False:
                    payload.pop("model", None)
                else:
                    raise ProductionRunnerError(
                        "stale claim lacks original queue-model provenance"
                    )
                label_run_id = str(payload.pop("label_run_id"))
                for key in (
                    "output_path",
                    "prompt_path",
                    "queue_payload_model",
                    "app_server_prepared_request_path",
                    "app_server_claim_receipt_path",
                    "app_server_prepared_request_sha256",
                ):
                    payload.pop(key, None)
                payload["app_server_recovered_prelaunch_label_run_id"] = label_run_id
                payload["app_server_recovered_prelaunch_claim_sha256"] = claim_sha256
                self.conn.execute(
                    """
                    UPDATE label_runs
                    SET status = 'failed', updated_at = ?
                    WHERE id = ? AND job_id = ? AND status = 'claimed'
                    """,
                    (timestamp, label_run_id, int(row["id"])),
                )
                updated = self.conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending', lease_owner = NULL, leased_until = NULL,
                        attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
                        payload_json = ?, updated_at = ?
                    WHERE id = ? AND status = 'claimed'
                    """,
                    (_canonical_json(payload), timestamp, int(row["id"])),
                )
                if updated.rowcount != 1:
                    raise ProductionRunnerWaiting(
                        "stale claim release was not atomic"
                    )
            self.conn.commit()
            self.production_mutated = True
        except Exception:
            self.conn.rollback()
            raise
        return {
            "state": "released_verified_prelaunch_claim",
            "job_ids": expected_ids,
            "semantic_model_call_count": 0,
            "claim_receipt_sha256": claim_sha256,
            "production_mutated": True,
        }

    def release_verified_prelaunch_batch(
        self,
        items: Sequence[QueueItem],
        *,
        prepared_request_path: Path,
        claim_receipt_path: Path,
        reason: str,
        thread_started_for_batch: bool,
    ) -> Mapping[str, Any]:
        if not items or not reason or not isinstance(thread_started_for_batch, bool):
            raise ProductionRunnerError("prelaunch release proof is incomplete")
        request_path = prepared_request_path.expanduser().resolve()
        claim_path = claim_receipt_path.expanduser().resolve()
        request_record = _record(request_path)
        claim_record = _record(claim_path)
        expected_ids = [item.job_id for item in items]
        placeholders = ", ".join("?" for _ in expected_ids)
        timestamp = now_iso()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            rows = self.conn.execute(
                f"""
                SELECT * FROM jobs
                WHERE id IN ({placeholders})
                  AND lane = ?
                  AND job_type = 'label_segment'
                  AND status = 'claimed'
                  AND lease_owner = ?
                ORDER BY id
                """,
                [*expected_ids, self.lane, self.worker_id],
            ).fetchall()
            rows_by_id = {int(row["id"]): row for row in rows}
            if set(rows_by_id) != set(expected_ids):
                raise ProductionRunnerWaiting(
                    "prelaunch release could not prove the exact active claim"
                )
            semantic_artifacts = [
                request_path.parent / name
                for name in (
                    "launch.json",
                    "sidecar.json",
                    "output.private.json",
                    "terminal.json",
                )
                if (request_path.parent / name).exists()
            ]
            if semantic_artifacts or any(
                Path(item.output_path).expanduser().resolve().exists() for item in items
            ):
                raise ProductionRunnerWaiting(
                    "prelaunch release refused after possible semantic dispatch"
                )
            payloads: dict[int, dict[str, Any]] = {}
            for item in items:
                row = rows_by_id[item.job_id]
                payload = loads_json(str(row["payload_json"]), {})
                if (
                    payload.get("app_server_prepared_request_path")
                    != str(request_path)
                    or payload.get("app_server_claim_receipt_path") != str(claim_path)
                    or payload.get("app_server_prepared_request_sha256")
                    != request_record["sha256"]
                    or payload.get("label_run_id") != item.label_run_id
                ):
                    raise ProductionRunnerWaiting(
                        "prelaunch release claim lineage drifted"
                    )
                label_run = self.conn.execute(
                    "SELECT * FROM label_runs WHERE id = ?", (item.label_run_id,)
                ).fetchone()
                if (
                    label_run is None
                    or int(label_run["job_id"]) != item.job_id
                    or str(label_run["segment_id"]) != item.segment_id
                    or str(label_run["status"]) != "claimed"
                ):
                    raise ProductionRunnerWaiting(
                        "prelaunch release label-run lineage drifted"
                    )
                payloads[item.job_id] = payload
            for item in items:
                payload = payloads[item.job_id]
                original_present = payload.pop(
                    "queue_payload_model_original_present", None
                )
                original_model = payload.pop("queue_payload_model_original", None)
                if original_present is True:
                    payload["model"] = original_model
                elif original_present is False:
                    payload.pop("model", None)
                else:
                    raise ProductionRunnerError(
                        "prelaunch release lacks original queue-model provenance"
                    )
                label_run_id = str(payload.pop("label_run_id"))
                for key in (
                    "output_path",
                    "prompt_path",
                    "queue_payload_model",
                    "app_server_prepared_request_path",
                    "app_server_claim_receipt_path",
                    "app_server_prepared_request_sha256",
                ):
                    payload.pop(key, None)
                payload["app_server_prelaunch_release_label_run_id"] = label_run_id
                payload["app_server_prelaunch_release_claim_sha256"] = claim_record[
                    "sha256"
                ]
                payload["app_server_prelaunch_release_reason_sha256"] = _sha256_bytes(
                    reason.encode("utf-8")
                )
                updated_run = self.conn.execute(
                    """
                    UPDATE label_runs
                    SET status = 'failed', error = ?, updated_at = ?
                    WHERE id = ? AND job_id = ? AND status = 'claimed'
                    """,
                    ("verified prelaunch source drift", timestamp, label_run_id, item.job_id),
                )
                updated_job = self.conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending', lease_owner = NULL, leased_until = NULL,
                        attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
                        payload_json = ?, updated_at = ?
                    WHERE id = ? AND status = 'claimed' AND lease_owner = ?
                    """,
                    (
                        _canonical_json(payload),
                        timestamp,
                        item.job_id,
                        self.worker_id,
                    ),
                )
                if updated_run.rowcount != 1 or updated_job.rowcount != 1:
                    raise ProductionRunnerWaiting(
                        "prelaunch source-drift release was not atomic"
                    )
            self.conn.commit()
            self.production_mutated = True
        except Exception:
            self.conn.rollback()
            raise
        return {
            "state": "released_verified_prelaunch_source_drift",
            "job_ids": expected_ids,
            "claim_receipt": claim_record,
            "semantic_model_call_count": 0,
            "thread_started_for_batch": thread_started_for_batch,
            "turn_started": False,
            "production_mutated": True,
        }

    def bind_prompt(self, items: Sequence[QueueItem], prompt_path: Path) -> None:
        timestamp = now_iso()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            for item in items:
                row = self.conn.execute(
                    "SELECT payload_json FROM jobs WHERE id = ?", (item.job_id,)
                ).fetchone()
                if row is None:
                    raise ProductionRunnerError("claimed job disappeared before prompt binding")
                payload = loads_json(str(row["payload_json"]), {})
                payload["prompt_path"] = str(prompt_path.resolve())
                self.conn.execute(
                    "UPDATE jobs SET payload_json = ?, updated_at = ? WHERE id = ?",
                    (_canonical_json(payload), timestamp, item.job_id),
                )
                self.conn.execute(
                    "UPDATE label_runs SET prompt_path = ?, updated_at = ? WHERE id = ?",
                    (str(prompt_path.resolve()), timestamp, item.label_run_id),
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def submit(self, item: QueueItem, output_path: Path) -> Mapping[str, Any]:
        try:
            return submit_label_output(
                self.conn,
                job_id=item.job_id,
                output_json_path=output_path,
                worker_id=self.worker_id,
                repair_output=False,
                derive_semantics=False,
            )
        except ValidationError as exc:
            raise ProductionRunnerWaiting(
                "projected model output failed exact worker validation"
            ) from exc

    def verify_completed_submissions(
        self, turns: Sequence[Mapping[str, Any]]
    ) -> Mapping[str, Any]:
        verified_job_ids: list[int] = []
        for turn in turns:
            for segment in turn.get("segment_records", []):
                job_id = int(segment["job_id"])
                segment_id = str(segment["segment_id"])
                submission = segment["submission"]
                output_path = _verify_record(
                    segment["output"], label="completed submitted segment output"
                )
                job = self.conn.execute(
                    "SELECT * FROM jobs WHERE id = ?", (job_id,)
                ).fetchone()
                if job is None:
                    raise ProductionRunnerError("completed submitted job is missing")
                payload = loads_json(str(job["payload_json"]), {})
                label_run_id = str(payload.get("label_run_id") or "")
                label_run = self.conn.execute(
                    "SELECT * FROM label_runs WHERE id = ?", (label_run_id,)
                ).fetchone()
                label = self.conn.execute(
                    "SELECT * FROM labels WHERE id = ?",
                    (str(submission.get("label_id") or ""),),
                ).fetchone()
                if (
                    str(submission.get("job_id") or "") != str(job_id)
                    or str(job["status"]) != "completed"
                    or str(job["target_id"]) != segment_id
                    or payload.get("label_pack") != FROZEN_LABEL_PACK
                    or payload.get("model") != self.semantic_model
                    or Path(str(payload.get("output_path") or "")).expanduser().resolve()
                    != output_path
                    or label_run is None
                    or int(label_run["job_id"]) != job_id
                    or str(label_run["segment_id"]) != segment_id
                    or str(label_run["label_pack"]) != FROZEN_LABEL_PACK
                    or str(label_run["model"]) != self.semantic_model
                    or str(label_run["status"]) != "completed"
                    or Path(str(label_run["output_path"])).expanduser().resolve()
                    != output_path
                    or label is None
                    or str(label["segment_id"]) != segment_id
                    or str(label["label_pack"]) != FROZEN_LABEL_PACK
                    or str(label["model"]) != self.semantic_model
                    or Path(str(label["output_path"])).expanduser().resolve()
                    != output_path
                ):
                    raise ProductionRunnerError(
                        "completed segment submission lineage drifted"
                    )
                persisted_output = loads_json(str(label["output_json"]), None)
                projected_output = _load_json(
                    output_path, label="completed submitted segment output"
                )
                if (
                    not isinstance(persisted_output, dict)
                    or _canonical_json(persisted_output)
                    != _canonical_json(projected_output)
                ):
                    raise ProductionRunnerError(
                        "completed label differs from the projected model output"
                    )
                verified_job_ids.append(job_id)
        if len(verified_job_ids) != len(set(verified_job_ids)):
            raise ProductionRunnerError("completed submission job identity repeated")
        return {
            "state": "verified_completed_submissions",
            "job_ids": verified_job_ids,
            "submitted_segment_count": len(verified_job_ids),
            "production_mutated": bool(verified_job_ids),
        }

    def fail(self, item: QueueItem, reason: str) -> None:
        fail_job(self.conn, item.job_id, reason)
        self.conn.commit()


def _partition(items: Sequence[QueueItem], size: int) -> list[list[QueueItem]]:
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


def _group_batches(
    items: Sequence[QueueItem], batch_size: int
) -> list[tuple[str, list[list[QueueItem]]]]:
    grouped: dict[str, list[QueueItem]] = defaultdict(list)
    for item in items:
        grouped[item.episode_id].append(item)
    result = []
    for episode_id in sorted(grouped):
        ordered = sorted(
            grouped[episode_id], key=lambda item: (item.segment_index, item.segment_id)
        )
        result.append((episode_id, _partition(ordered, batch_size)))
    return result


def _verified_context_for_episode(
    loaded: Mapping[str, Any], episode_id: str
) -> dict[str, Any]:
    artifact_index = loaded["episode_context"].get("context_artifact_index")
    if not isinstance(artifact_index, Mapping):
        raise ProductionRunnerWaiting(
            "official-live episode context artifact index is unavailable"
        )
    artifact_record = artifact_index.get(episode_id)
    if artifact_record is None:
        raise ProductionRunnerWaiting(
            "official-live episode context artifact is absent for the episode"
        )
    try:
        artifact_path = _verify_record(
            artifact_record, label=f"official-live context artifact {episode_id}"
        )
        managed = episode_context_runner._verify_managed_context_artifact(
            artifact_path=artifact_path,
            episode_id=episode_id,
            label_pack=FROZEN_LABEL_PACK,
            model=FROZEN_QUEUE_PAYLOAD_MODEL,
            loaded=loaded["episode_context_contract"],
        )
    except (EpisodeContextRunnerError, ProductionRunnerError) as exc:
        raise ProductionRunnerWaiting(
            "episode context artifact failed strict verification before claim"
        ) from exc
    canonical = managed.get("artifact")
    lineage = managed.get("lineage")
    if not isinstance(canonical, Mapping) or not isinstance(lineage, Mapping):
        raise ProductionRunnerWaiting("episode context artifact verifier drifted")
    verified = {
        "episode_id": episode_id,
        "artifact": dict(artifact_record),
        "artifact_index_sha256": loaded["episode_context"][
            "context_artifact_index_sha256"
        ],
        "frozen_configuration_sha256": loaded["episode_context_contract"][
            "configuration_sha256"
        ],
        "episode_context": canonical.get("episode_context"),
        "extraction_guidance": canonical.get("extraction_guidance"),
        "excluded_source_context": canonical.get("excluded_source_context"),
        "semantic_authority_fields": list(
            episode_context_runner.SEMANTIC_AUTHORITY_FIELDS
        ),
        "deterministic_semantic_pruning": False,
        "artifact_lineage": _record(Path(str(managed["lineage_path"]))),
    }
    if (
        verified.get("artifact_index_sha256")
        != loaded["episode_context"]["context_artifact_index_sha256"]
        or verified.get("deterministic_semantic_pruning") is not False
    ):
        raise ProductionRunnerWaiting("episode context artifact lineage drifted")
    context = verified.get("episode_context")
    guidance = verified.get("extraction_guidance")
    excluded = verified.get("excluded_source_context")
    required_context_fields = {
        "context_summary": str,
        "speaker_map": list,
        "section_map": list,
        "entity_seed": dict,
        "concept_seed": list,
    }
    if (
        not isinstance(context, Mapping)
        or any(
            not isinstance(context.get(field), expected_type)
            for field, expected_type in required_context_fields.items()
        )
        or not isinstance(guidance, str)
        or not isinstance(excluded, list)
        or any(not isinstance(item, str) for item in excluded)
    ):
        raise ProductionRunnerWaiting(
            "verified episode context lacks the frozen semantic maps"
        )
    semantic_fields = {
        "episode_context": dict(context),
        "extraction_guidance": guidance,
        "excluded_source_context": list(excluded),
    }
    return {
        **verified,
        "semantic_fields": semantic_fields,
        "semantic_fields_sha256": _sha256_bytes(
            _canonical_json(semantic_fields).encode("utf-8")
        ),
    }


def _prepare_episode_batch(
    loaded: Mapping[str, Any], items: Sequence[QueueItem]
) -> dict[str, Any]:
    if not items or len({item.episode_id for item in items}) != 1:
        raise ProductionRunnerError("preparation requires one nonempty episode batch")
    config = loaded["contract"]["configuration"]
    episode_id = items[0].episode_id
    verified_context = _verified_context_for_episode(loaded, episode_id)
    context = dict(verified_context["episode_context"])
    source_name = items[0].source_name or str(context.get("source_name") or "")
    episode_title = items[0].episode_title or str(
        context.get("episode_title") or ""
    )
    segments: list[dict[str, Any]] = []
    source_segments: list[dict[str, Any]] = []
    for item in items:
        source_binding = _validate_expected_source_binding(item)
        windows, boundaries = build_windowed_segment_packet(
            item.segment_text,
            window_count=int(config["window_count"]),
            context_chars=int(config["context_chars"]),
        )
        segment = {
            "segment_id": item.segment_id,
            "segment_text": item.segment_text,
            "segment_quality": dict(item.segment_quality or {}),
            "density_stratum": "production_unstratified",
            "boundaries": boundaries,
        }
        segments.append(segment)
        source_segments.append(
            {
                "job_id": item.job_id,
                "segment_id": item.segment_id,
                "segment_index": item.segment_index,
                "segment_text_sha256": item.segment_text_sha256,
                "segment_quality": dict(item.segment_quality or {}),
                "expected_source_binding": source_binding,
                "expected_source_binding_sha256": _source_binding_sha256(
                    source_binding
                ),
                "windows": windows,
                "boundaries": boundaries,
            }
        )
    episode = {
        "episode_id": episode_id,
        "source_name": source_name,
        "episode_title": episode_title,
        "context_summary": context["context_summary"],
        "speaker_map": context["speaker_map"],
        "section_map": context["section_map"],
        "entity_seed": context["entity_seed"],
        "concept_seed": context["concept_seed"],
        "extraction_guidance": verified_context["extraction_guidance"],
        "excluded_source_context": verified_context["excluded_source_context"],
        "segments": segments,
    }
    try:
        requests = episode_batch.prepare_episode_batches(
            episode,
            batch_size=int(config["batch_size"]),
            thread_mode=str(config["thread_mode"]),
        )
    except episode_batch.CanonicalV31EpisodeBatchError as exc:
        raise ProductionRunnerWaiting(
            "canonical v3.1 episode batch preparation failed before claim"
        ) from exc
    if len(requests) != 1:
        raise ProductionRunnerError("one queue batch produced multiple semantic requests")
    try:
        request = episode_batch.validate_prepared_request(requests[0])
    except episode_batch.CanonicalV31EpisodeBatchError as exc:
        raise ProductionRunnerWaiting(
            "canonical v3.1 prepared request failed its own verifier"
        ) from exc
    if request.get("segment_ids") != [item.segment_id for item in items]:
        raise ProductionRunnerWaiting("prepared request segment lineage drifted")
    context_binding = {
        "schema_version": CONTEXT_BINDING_VERSION,
        "episode_id": episode_id,
        "completion_receipt": loaded["contract"]["episode_context"][
            "live_completion_receipt"
        ],
        "context_artifact": verified_context["artifact"],
        "context_artifact_index_sha256": verified_context[
            "artifact_index_sha256"
        ],
        "semantic_fields_sha256": verified_context["semantic_fields_sha256"],
        "loaded_semantic_fields": list(LOADED_SEMANTIC_FIELDS),
        "adapter_consumed_context": request["episode_context"],
        "adapter_consumed_context_sha256": _sha256_bytes(
            _canonical_json(request["episode_context"]).encode("utf-8")
        ),
        "deterministic_semantic_pruning": False,
    }
    return {
        "request": request,
        "source_packet": {
            "schema_version": TURN_INPUT_VERSION,
            "episode_id": episode_id,
            "window_count": config["window_count"],
            "context_chars": config["context_chars"],
            "segments": source_segments,
        },
        "context_binding": context_binding,
    }


def _valid_usage(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise ProductionRunnerWaiting("turn sidecar has unknown usage")
    result: dict[str, int] = {}
    for field in USAGE_FIELDS:
        item = value.get(field)
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise ProductionRunnerWaiting("turn sidecar has malformed usage")
        result[field] = item
    if (
        result["total_tokens"] != result["input_tokens"] + result["output_tokens"]
        or result["cached_input_tokens"] > result["input_tokens"]
        or result["reasoning_output_tokens"] > result["output_tokens"]
    ):
        raise ProductionRunnerWaiting("turn sidecar usage accounting is inconsistent")
    return result


def _validate_completed_turn(
    *,
    request: Mapping[str, Any],
    sidecar_path: Path,
    output_path: Path,
    thread_id: str,
    seen_turn_ids: set[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        validated = episode_batch.validate_prepared_request(request)
    except episode_batch.CanonicalV31EpisodeBatchError as exc:
        raise ProductionRunnerWaiting(
            "completed production turn lineage is invalid"
        ) from exc
    sidecar = _load_json(sidecar_path, label="production turn sidecar")
    instruction_sources = episode_batch.expected_instruction_source_contract()
    try:
        started_at = datetime.fromisoformat(
            str(sidecar.get("started_at") or "").replace("Z", "+00:00")
        )
        finished_at = datetime.fromisoformat(
            str(sidecar.get("finished_at") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ProductionRunnerWaiting(
            "completed production sidecar timestamps are invalid"
        ) from exc
    output_target = output_path.expanduser().resolve()
    try:
        output_text = output_target.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ProductionRunnerWaiting(
            "completed production output is unavailable"
        ) from exc
    output_message = output_text[:-1] if output_text.endswith("\n") else output_text
    usage = _valid_usage(sidecar.get("usage"))
    thread_total_usage = _valid_usage(sidecar.get("thread_total_usage"))
    wall_elapsed = sidecar.get("wall_elapsed_seconds")
    if (
        sidecar.get("schema_version") != TURN_SIDECAR_SCHEMA_VERSION
        or started_at.tzinfo is None
        or finished_at.tzinfo is None
        or finished_at < started_at
        or sidecar.get("client_version") != APP_SERVER_CLIENT_VERSION
        or sidecar.get("cli_version") != PINNED_CLI_VERSION
        or sidecar.get("protocol_schema_sha256")
        != _sha256_file(episode_batch.codex_app_server.PROTOCOL_SCHEMA_PATH)
        or sidecar.get("transport") != "stdio"
        or not isinstance(sidecar.get("app_server_user_agent"), str)
        or not sidecar.get("app_server_user_agent")
        or isinstance(sidecar.get("max_message_bytes"), bool)
        or not isinstance(sidecar.get("max_message_bytes"), int)
        or int(sidecar["max_message_bytes"]) < 64 * 1024
        or sidecar.get("synthetic_debug_errors") is not False
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("thread_id") != thread_id
        or not isinstance(sidecar.get("turn_id"), str)
        or not sidecar.get("turn_id")
        or sidecar.get("model") != validated["model"]
        or sidecar.get("effort") != validated["effort"]
        or sidecar.get("thread_mode") != validated["thread_mode"]
        or sidecar.get("batch_size") != validated["effective_batch_size"]
        or sidecar.get("error_class") is not None
        or isinstance(wall_elapsed, bool)
        or not isinstance(wall_elapsed, (int, float))
        or not math.isfinite(float(wall_elapsed))
        or float(wall_elapsed) < 0
        or sidecar.get("prompt_sha256") != validated["prompt_sha256"]
        or sidecar.get("prompt_bytes")
        != len(str(validated["prompt"]).encode("utf-8"))
        or sidecar.get("base_instructions_sha256")
        != validated["base_instructions_sha256"]
        or sidecar.get("base_instructions_bytes")
        != len(str(validated["base_instructions"]).encode("utf-8"))
        or sidecar.get("instruction_sources_sha256")
        != instruction_sources["effective_instruction_sources_sha256"]
        or sidecar.get("instruction_sources_count")
        != instruction_sources["effective_instruction_sources_count"]
        or sidecar.get("output_schema_sha256")
        != validated["output_schema_sha256"]
        or sidecar.get("output_schema_bytes")
        != len(_canonical_json(validated["output_schema"]).encode("utf-8"))
        or sidecar.get("output_sha256")
        != _sha256_bytes(output_message.encode("utf-8"))
        or Path(str(sidecar.get("output_path") or "")).expanduser().resolve()
        != output_target
        or not isinstance(sidecar.get("stderr_sha256"), str)
        or len(str(sidecar.get("stderr_sha256"))) != 64
        or isinstance(sidecar.get("stderr_bytes"), bool)
        or not isinstance(sidecar.get("stderr_bytes"), int)
        or int(sidecar["stderr_bytes"]) < 0
        or sidecar.get("recovery_reran_model") is not False
        or any(
            thread_total_usage[field] < usage[field] for field in USAGE_FIELDS
        )
        or (
            validated["thread_mode"] == "new_thread"
            and thread_total_usage != usage
        )
    ):
        raise ProductionRunnerWaiting(
            "completed production turn lineage is invalid"
        )
    output = _load_json(output_path, label="production turn output")
    turn_id = str(sidecar["turn_id"])
    if turn_id in seen_turn_ids:
        raise ProductionRunnerWaiting("completed production turn id is duplicated")
    seen_turn_ids.add(turn_id)
    return sidecar, output


def _validate_partial_turn_sidecar(
    request: Mapping[str, Any],
    sidecar_path: Path,
    *,
    output_path: Path | None,
    thread_id: str,
) -> dict[str, Any]:
    sidecar = _load_json(sidecar_path, label="partial production sidecar")
    if sidecar.get("state") == "completed":
        if output_path is None:
            raise ProductionRunnerWaiting(
                "completed partial sidecar lacks its output artifact"
            )
        verified, _output = _validate_completed_turn(
            request=request,
            sidecar_path=sidecar_path,
            output_path=output_path,
            thread_id=thread_id,
            seen_turn_ids=set(),
        )
        return {
            "sidecar": verified,
            "usage": _valid_usage(verified["usage"]),
            "usage_status": "complete",
        }
    try:
        validated = episode_batch.validate_prepared_request(request)
    except episode_batch.CanonicalV31EpisodeBatchError as exc:
        raise ProductionRunnerWaiting("partial prepared request drifted") from exc
    try:
        sources = episode_batch.expected_instruction_source_contract()
    except episode_batch.CanonicalV31EpisodeBatchError as exc:
        raise ProductionRunnerError("frozen app-server context contract drifted") from exc
    if (
        sidecar.get("schema_version") != TURN_SIDECAR_SCHEMA_VERSION
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("thread_id") != thread_id
        or sidecar.get("model") != validated["model"]
        or sidecar.get("effort") != validated["effort"]
        or sidecar.get("thread_mode") != validated["thread_mode"]
        or sidecar.get("batch_size") != validated["effective_batch_size"]
        or sidecar.get("prompt_sha256") != validated["prompt_sha256"]
        or sidecar.get("base_instructions_sha256")
        != validated["base_instructions_sha256"]
        or sidecar.get("output_schema_sha256")
        != validated["output_schema_sha256"]
        or sidecar.get("instruction_sources_sha256")
        != sources["effective_instruction_sources_sha256"]
        or sidecar.get("instruction_sources_count")
        != sources["effective_instruction_sources_count"]
    ):
        raise ProductionRunnerWaiting("partial production sidecar lineage drifted")
    usage_status = (
        "complete"
        if sidecar.get("usage_status") == "measured"
        and sidecar.get("usage_complete") is True
        else "unknown_or_incomplete"
    )
    return {
        "sidecar": sidecar,
        "usage": (
            _valid_usage(sidecar.get("usage")) if usage_status == "complete" else None
        ),
        "usage_status": usage_status,
    }


def _validate_schema_value(schema: Mapping[str, Any], value: Any, *, path: str) -> None:
    expected = schema.get("type")
    if isinstance(expected, list):
        for item_type in expected:
            try:
                _validate_schema_value({**schema, "type": item_type}, value, path=path)
                return
            except ProductionRunnerWaiting:
                pass
        raise ProductionRunnerWaiting(f"{path} does not match any allowed schema type")
    if expected == "object":
        if not isinstance(value, dict):
            raise ProductionRunnerWaiting(f"{path} must be an object")
        properties = schema.get("properties", {})
        for key in schema.get("required", []):
            if key not in value:
                raise ProductionRunnerWaiting(f"{path}.{key} is required")
        if schema.get("additionalProperties") is False:
            extras = set(value) - set(properties)
            if extras:
                raise ProductionRunnerWaiting(f"{path} contains unexpected fields")
        for key, child in properties.items():
            if key in value:
                _validate_schema_value(child, value[key], path=f"{path}.{key}")
    elif expected == "array":
        if not isinstance(value, list):
            raise ProductionRunnerWaiting(f"{path} must be an array")
        if "maxItems" in schema and len(value) > int(schema["maxItems"]):
            raise ProductionRunnerWaiting(f"{path} exceeds maxItems")
        if "minItems" in schema and len(value) < int(schema["minItems"]):
            raise ProductionRunnerWaiting(f"{path} is below minItems")
        for index, item in enumerate(value):
            _validate_schema_value(schema.get("items", {}), item, path=f"{path}[{index}]")
    elif expected == "string" and not isinstance(value, str):
        raise ProductionRunnerWaiting(f"{path} must be a string")
    elif expected == "boolean" and not isinstance(value, bool):
        raise ProductionRunnerWaiting(f"{path} must be a boolean")
    elif expected == "integer" and (isinstance(value, bool) or not isinstance(value, int)):
        raise ProductionRunnerWaiting(f"{path} must be an integer")
    elif expected == "number" and (
        isinstance(value, bool) or not isinstance(value, (int, float))
    ):
        raise ProductionRunnerWaiting(f"{path} must be a number")
    elif expected == "null" and value is not None:
        raise ProductionRunnerWaiting(f"{path} must be null")
    if "enum" in schema and value not in schema["enum"]:
        raise ProductionRunnerWaiting(f"{path} is outside the allowed enum")


def _validate_batch_output(
    output: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
    items: Sequence[QueueItem],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        projected = episode_batch.validate_and_project_output(request, output)
    except episode_batch.CanonicalV31EpisodeBatchError as exc:
        raise ProductionRunnerWaiting(
            "completed output failed canonical v3.1 semantic-surface validation"
        ) from exc
    rows = projected.get("labels")
    expected_ids = [item.segment_id for item in items]
    if not isinstance(rows, list):
        raise ProductionRunnerWaiting("batch output segment collection drifted")
    actual_ids = [row.get("segment_id") if isinstance(row, Mapping) else None for row in rows]
    if (
        actual_ids != expected_ids
        or any(row.get("episode_id") != request["episode_id"] for row in rows)
    ):
        raise ProductionRunnerWaiting("batch output did not preserve exact segment order")
    return [dict(row) for row in rows], projected


def _turn_paths(root: Path, *, episode_id: str, batch_id: str) -> dict[str, Path]:
    turn_root = root / "turns" / episode_id / batch_id
    return {
        "root": turn_root,
        "input": turn_root / "source-packet.private.json",
        "context": turn_root / "episode-context-binding.private.json",
        "request": turn_root / "prepared-request.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity-admission.json",
        "claim": turn_root / "claim-receipt.json",
        "launch": turn_root / "launch.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
        "projected": turn_root / "canonical-labels.private.json",
        "provenance": turn_root / "evidence-provenance.private.json",
        "fidelity": turn_root / "semantic-fidelity.json",
        "terminal": turn_root / "terminal.json",
    }


def _partial_turn_evidence(
    root: Path, loaded: Mapping[str, Any]
) -> list[dict[str, Any]]:
    partial: list[dict[str, Any]] = []
    launches = (
        sorted((root / "turns").glob("*/*/launch.json"))
        if (root / "turns").exists()
        else []
    )
    for launch in launches:
        terminal = launch.parent / "terminal.json"
        if terminal.exists():
            continue
        entry: dict[str, Any] = {
            "turn_root": str(launch.parent.resolve()),
            "launch": _record(launch),
            "lineage_verified": False,
            "semantic_model_call_count": None,
            "usage": None,
            "accounting_complete": False,
        }
        try:
            payload = _load_json(launch, label="partial production launch")
            expected_keys = {
                "schema_version",
                "contract_sha256",
                "evaluation_id",
                "frozen_configuration_sha256",
                "winner_system_id",
                "episode_id",
                "batch_id",
                "job_ids",
                "segment_ids",
                "thread_id",
                "thread_ephemeral",
                "thread_persisted_path_sha256",
                "thread_resumed",
                "prior_completed_turn_ids_sha256",
                "managed_chatgpt_plan_type",
                "context_control_overlay_sha256",
                "resume_source_packet_records_sha256",
                "prior_usage_sha256",
                "model",
                "reasoning_effort",
                "configured_batch_size",
                "thread_mode",
                "turn_batch_size",
                "semantic_retry_count",
                "episode_context_completion_receipt",
                "episode_context_binding",
                "episode_batch_adapter",
                "prepared_request",
                "capacity_admission",
                "context_control_overlay",
                "effective_instruction_sources_sha256",
                "effective_instruction_sources_count",
                "claim_receipt",
                "source_packet",
                "prompt",
                "base_instructions",
                "output_schema",
            }
            request_path = _verify_record(
                payload.get("prepared_request"), label="partial prepared request"
            )
            request = episode_batch.validate_prepared_request(
                _load_json(request_path, label="partial prepared request")
            )
            paths = _turn_paths(
                root,
                episode_id=str(request["episode_id"]),
                batch_id=str(request["batch_id"]),
            )
            record_paths = {
                "episode_context_binding": paths["context"],
                "capacity_admission": paths["capacity"],
                "context_control_overlay": root / "context-control-overlay.json",
                "claim_receipt": paths["claim"],
                "source_packet": paths["input"],
                "prompt": paths["prompt"],
                "base_instructions": paths["base"],
                "output_schema": paths["schema"],
            }
            verified_record_paths = {
                name: _verify_record(payload.get(name), label=f"partial {name}")
                for name in record_paths
            }
            config = loaded["contract"]["configuration"]
            if (
                set(payload) != expected_keys
                or launch.resolve() != paths["launch"].resolve()
                or request_path != paths["request"].resolve()
                or any(
                    verified_record_paths[name] != expected.resolve()
                    for name, expected in record_paths.items()
                )
                or payload.get("schema_version") != TURN_LAUNCH_VERSION
                or payload.get("contract_sha256") != loaded["sha256"]
                or payload.get("evaluation_id")
                != loaded["evaluation"]["evaluation_id"]
                or payload.get("frozen_configuration_sha256")
                != config["frozen_configuration_sha256"]
                or payload.get("winner_system_id") != config["winner_system_id"]
                or payload.get("episode_id") != request["episode_id"]
                or payload.get("batch_id") != request["batch_id"]
                or payload.get("segment_ids") != request["segment_ids"]
                or payload.get("turn_batch_size")
                != request["effective_batch_size"]
                or payload.get("model") != config["model"]
                or payload.get("reasoning_effort") != config["effort"]
                or payload.get("configured_batch_size") != config["batch_size"]
                or payload.get("thread_mode") != request["thread_mode"]
                or payload.get("semantic_retry_count") != 0
                or not isinstance(payload.get("thread_id"), str)
                or not payload.get("thread_id")
                or payload.get("thread_ephemeral")
                != (request["thread_mode"] == "new_thread")
                or (
                    request["thread_mode"] == "same_thread"
                    and (
                        not isinstance(
                            payload.get("thread_persisted_path_sha256"), str
                        )
                        or len(str(payload["thread_persisted_path_sha256"])) != 64
                    )
                )
                or (
                    request["thread_mode"] == "new_thread"
                    and payload.get("thread_persisted_path_sha256") is not None
                )
                or not isinstance(payload.get("thread_resumed"), bool)
                or not isinstance(
                    payload.get("prior_completed_turn_ids_sha256"), str
                )
                or len(str(payload["prior_completed_turn_ids_sha256"])) != 64
                or payload.get("managed_chatgpt_plan_type") != "pro"
                or payload.get("context_control_overlay_sha256")
                != request["context_control_overlay_sha256"]
                or (
                    payload.get("thread_resumed") is True
                    and (
                        not isinstance(
                            payload.get("resume_source_packet_records_sha256"),
                            str,
                        )
                        or len(
                            str(payload["resume_source_packet_records_sha256"])
                        )
                        != 64
                        or not isinstance(payload.get("prior_usage_sha256"), str)
                        or len(str(payload["prior_usage_sha256"])) != 64
                    )
                )
                or (
                    payload.get("thread_resumed") is False
                    and (
                        payload.get("resume_source_packet_records_sha256")
                        is not None
                        or payload.get("prior_usage_sha256") is not None
                    )
                )
                or payload.get("episode_context_completion_receipt")
                != loaded["contract"]["episode_context"]["live_completion_receipt"]
                or payload.get("episode_batch_adapter")
                != loaded["contract"]["artifacts"]["episode_batch_adapter"]
                or payload.get("effective_instruction_sources_sha256")
                != episode_batch.expected_instruction_source_contract()[
                    "effective_instruction_sources_sha256"
                ]
                or payload.get("effective_instruction_sources_count")
                != episode_batch.expected_instruction_source_contract()[
                    "effective_instruction_sources_count"
                ]
            ):
                raise ProductionRunnerError("partial production launch lineage drifted")
            entry["lineage_verified"] = True
            entry["thread_id_sha256"] = _sha256_bytes(
                str(payload["thread_id"]).encode("utf-8")
            )
            sidecar_path = paths["sidecar"]
            output_path = paths["output"]
            if sidecar_path.is_file():
                entry["sidecar"] = _record(sidecar_path)
                try:
                    verified_sidecar = _validate_partial_turn_sidecar(
                        request,
                        sidecar_path,
                        output_path=(output_path if output_path.is_file() else None),
                        thread_id=str(payload["thread_id"]),
                    )
                except ProductionRunnerWaiting:
                    entry["sidecar_lineage_verified"] = False
                else:
                    entry["sidecar_lineage_verified"] = True
                    entry["semantic_model_call_count"] = 1
                    entry["usage"] = verified_sidecar["usage"]
                    entry["accounting_complete"] = (
                        verified_sidecar["usage_status"] == "complete"
                    )
            if output_path.is_file():
                entry["output"] = _record(output_path)
        except (
            ProductionRunnerError,
            episode_batch.CanonicalV31EpisodeBatchError,
            OSError,
        ) as exc:
            entry["lineage_error_class"] = type(exc).__name__
            entry["lineage_error_sha256"] = _sha256_bytes(
                str(exc).encode("utf-8")
            )
        partial.append(entry)
    return partial


def _validate_completed_capacity_admission(
    payload: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
    turn_started_at: Any,
    expected_fixture_mode: bool,
) -> None:
    del turn_started_at
    requirements = payload.get("requirements")
    policy = production_contract.capacity_admission_contract()
    if (
        payload.get("schema_version") != CAPACITY_RECEIPT_VERSION
        or not isinstance(requirements, Mapping)
        or requirements.get("remaining_batch_count", 0) < 1
        or requirements.get("maximum_total_tokens_per_batch")
        != _capacity_requirements(
            request,
            remaining_batch_count=1,
            timeout_seconds=1,
        )["maximum_total_tokens_per_batch"]
        or requirements.get("required_total_tokens")
        != requirements.get("maximum_total_tokens_per_batch")
        * requirements.get("remaining_batch_count")
        or not isinstance(requirements.get("required_wall_seconds"), int)
        or requirements.get("required_wall_seconds") < 1
        or payload.get("capacity_policy") != policy
        or payload.get("capacity_policy_sha256")
        != _sha256_bytes(_canonical_json(policy).encode("utf-8"))
        or payload.get("available_total_tokens", -1)
        < requirements.get("required_total_tokens")
        or payload.get("available_wall_seconds", -1)
        < requirements.get("required_wall_seconds")
        or payload.get("semantic_authority") is not False
        or payload.get("semantic_pruning_or_relabeling_allowed") is not False
        or payload.get("capacity_probe_before_thread_start") is not True
        or payload.get("semantic_thread_started") is not False
        or payload.get("turn_started") is not False
        or payload.get("sidecar_started") is not False
        or payload.get("reset_credits_counted_as_capacity") is not False
        or payload.get("fixture_only") is not expected_fixture_mode
        or payload.get("offline_fixture_non_promotable")
        is not expected_fixture_mode
        or payload.get("live_capacity_authority") is expected_fixture_mode
        or payload.get("state")
        != (
            "admitted_offline_fixture_non_promotable"
            if expected_fixture_mode
            else "admitted_official_managed_auth_numeric_reserve"
        )
    ):
        raise ProductionRunnerError("completed capacity admission drifted")


def _completed_turn_evidence(
    root: Path,
    loaded: Mapping[str, Any],
    *,
    fixture_mode: bool,
    queue: QueueOperations,
) -> list[dict[str, Any]]:
    terminals = (
        sorted((root / "turns").glob("*/*/terminal.json"))
        if (root / "turns").exists()
        else []
    )
    completed: list[dict[str, Any]] = []
    thread_episode_by_id: dict[str, str] = {}
    episode_thread_by_id: dict[str, str] = {}
    seen_turn_ids: set[str] = set()
    config = loaded["contract"]["configuration"]
    for terminal_path in terminals:
        payload = _load_json(terminal_path, label="completed production terminal")
        expected_keys = {
            "schema_version",
            "state",
            "evaluation_id",
            "frozen_configuration_sha256",
            "winner_system_id",
            "model",
            "reasoning_effort",
            "configured_batch_size",
            "turn_batch_size",
            "launch",
            "context",
            "prepared_request",
            "capacity_admission",
            "context_control_overlay",
            "claim_receipt",
            "sidecar",
            "output",
            "source_packet",
            "canonical_labels",
            "evidence_provenance",
            "semantic_fidelity",
            "thread_id",
            "turn_id",
            "usage",
            "wall_elapsed_seconds",
            "segment_records",
            "semantic_retry_count",
        }
        request_path = _verify_record(
            payload.get("prepared_request"), label="completed prepared request"
        )
        request = episode_batch.validate_prepared_request(
            _load_json(request_path, label="completed prepared request")
        )
        paths = _turn_paths(
            root,
            episode_id=str(request["episode_id"]),
            batch_id=str(request["batch_id"]),
        )
        expected_records = {
            "launch": paths["launch"],
            "context": paths["context"],
            "prepared_request": paths["request"],
            "capacity_admission": paths["capacity"],
            "context_control_overlay": root / "context-control-overlay.json",
            "claim_receipt": paths["claim"],
            "sidecar": paths["sidecar"],
            "output": paths["output"],
            "source_packet": paths["input"],
            "canonical_labels": paths["projected"],
            "evidence_provenance": paths["provenance"],
            "semantic_fidelity": paths["fidelity"],
        }
        verified_records = {
            name: _verify_record(payload.get(name), label=f"completed {name}")
            for name in expected_records
        }
        if (
            set(payload) != expected_keys
            or terminal_path.resolve() != paths["terminal"].resolve()
            or any(
                verified_records[name] != expected.resolve()
                for name, expected in expected_records.items()
            )
            or payload.get("schema_version") != TURN_TERMINAL_VERSION
            or payload.get("state") != "passed"
            or payload.get("evaluation_id") != loaded["evaluation"]["evaluation_id"]
            or payload.get("frozen_configuration_sha256")
            != config["frozen_configuration_sha256"]
            or payload.get("winner_system_id") != config["winner_system_id"]
            or payload.get("model") != config["model"]
            or payload.get("reasoning_effort") != config["effort"]
            or payload.get("configured_batch_size") != config["batch_size"]
            or payload.get("turn_batch_size") != request["effective_batch_size"]
            or payload.get("semantic_retry_count") != 0
        ):
            raise ProductionRunnerError("completed production terminal lineage drifted")
        thread_id = payload.get("thread_id")
        turn_id = payload.get("turn_id")
        if (
            not isinstance(thread_id, str)
            or not thread_id
            or not isinstance(turn_id, str)
            or not turn_id
            or turn_id in seen_turn_ids
        ):
            raise ProductionRunnerError("completed production turn identity drifted")
        prior_episode = thread_episode_by_id.get(thread_id)
        if (
            request["thread_mode"] == "new_thread"
            and prior_episode is not None
        ):
            raise ProductionRunnerError("new-thread production identity was reused")
        if prior_episode is not None and prior_episode != request["episode_id"]:
            raise ProductionRunnerError("production thread crossed episode boundaries")
        prior_thread = episode_thread_by_id.get(str(request["episode_id"]))
        if (
            request["thread_mode"] == "same_thread"
            and prior_thread is not None
            and prior_thread != thread_id
        ):
            raise ProductionRunnerError(
                "same-thread production episode used more than one thread"
            )
        thread_episode_by_id[thread_id] = str(request["episode_id"])
        episode_thread_by_id[str(request["episode_id"])] = thread_id
        launch = _load_json(paths["launch"], label="completed production launch")
        expected_launch_keys = {
            "schema_version",
            "contract_sha256",
            "evaluation_id",
            "frozen_configuration_sha256",
            "winner_system_id",
            "episode_id",
            "batch_id",
            "job_ids",
            "segment_ids",
            "thread_id",
            "thread_ephemeral",
            "thread_persisted_path_sha256",
            "thread_resumed",
            "prior_completed_turn_ids_sha256",
            "managed_chatgpt_plan_type",
            "context_control_overlay_sha256",
            "resume_source_packet_records_sha256",
            "prior_usage_sha256",
            "model",
            "reasoning_effort",
            "configured_batch_size",
            "thread_mode",
            "turn_batch_size",
            "semantic_retry_count",
            "episode_context_completion_receipt",
            "episode_context_binding",
            "episode_batch_adapter",
            "prepared_request",
            "capacity_admission",
            "context_control_overlay",
            "effective_instruction_sources_sha256",
            "effective_instruction_sources_count",
            "claim_receipt",
            "source_packet",
            "prompt",
            "base_instructions",
            "output_schema",
        }
        launch_records = {
            "episode_context_binding": paths["context"],
            "prepared_request": paths["request"],
            "capacity_admission": paths["capacity"],
            "context_control_overlay": root / "context-control-overlay.json",
            "claim_receipt": paths["claim"],
            "source_packet": paths["input"],
            "prompt": paths["prompt"],
            "base_instructions": paths["base"],
            "output_schema": paths["schema"],
        }
        verified_launch_records = {
            name: _verify_record(launch.get(name), label=f"completed launch {name}")
            for name in launch_records
        }
        if (
            set(launch) != expected_launch_keys
            or any(
                verified_launch_records[name] != expected.resolve()
                for name, expected in launch_records.items()
            )
            or launch.get("schema_version") != TURN_LAUNCH_VERSION
            or launch.get("contract_sha256") != loaded["sha256"]
            or launch.get("evaluation_id")
            != loaded["evaluation"]["evaluation_id"]
            or launch.get("frozen_configuration_sha256")
            != config["frozen_configuration_sha256"]
            or launch.get("winner_system_id") != config["winner_system_id"]
            or launch.get("thread_id") != thread_id
            or launch.get("thread_ephemeral")
            != (request["thread_mode"] == "new_thread")
            or (
                request["thread_mode"] == "same_thread"
                and (
                    not isinstance(
                        launch.get("thread_persisted_path_sha256"), str
                    )
                    or len(str(launch["thread_persisted_path_sha256"])) != 64
                )
            )
            or (
                request["thread_mode"] == "new_thread"
                and launch.get("thread_persisted_path_sha256") is not None
            )
            or not isinstance(launch.get("thread_resumed"), bool)
            or not isinstance(
                launch.get("prior_completed_turn_ids_sha256"), str
            )
            or len(str(launch["prior_completed_turn_ids_sha256"])) != 64
            or launch.get("managed_chatgpt_plan_type") != "pro"
            or launch.get("context_control_overlay_sha256")
            != request["context_control_overlay_sha256"]
            or (
                launch.get("thread_resumed") is True
                and (
                    not isinstance(
                        launch.get("resume_source_packet_records_sha256"), str
                    )
                    or len(str(launch["resume_source_packet_records_sha256"]))
                    != 64
                    or not isinstance(launch.get("prior_usage_sha256"), str)
                    or len(str(launch["prior_usage_sha256"])) != 64
                )
            )
            or (
                launch.get("thread_resumed") is False
                and (
                    launch.get("resume_source_packet_records_sha256") is not None
                    or launch.get("prior_usage_sha256") is not None
                )
            )
            or launch.get("episode_id") != request["episode_id"]
            or launch.get("batch_id") != request["batch_id"]
            or launch.get("segment_ids") != request["segment_ids"]
            or launch.get("model") != config["model"]
            or launch.get("reasoning_effort") != config["effort"]
            or launch.get("configured_batch_size") != config["batch_size"]
            or launch.get("thread_mode") != request["thread_mode"]
            or launch.get("turn_batch_size") != request["effective_batch_size"]
            or launch.get("episode_context_completion_receipt")
            != loaded["contract"]["episode_context"]["live_completion_receipt"]
            or launch.get("episode_batch_adapter")
            != loaded["contract"]["artifacts"]["episode_batch_adapter"]
            or launch.get("context_control_overlay")
            != payload["context_control_overlay"]
            or launch.get("effective_instruction_sources_sha256")
            != episode_batch.expected_instruction_source_contract()[
                "effective_instruction_sources_sha256"
            ]
            or launch.get("effective_instruction_sources_count")
            != episode_batch.expected_instruction_source_contract()[
                "effective_instruction_sources_count"
            ]
            or launch.get("semantic_retry_count") != 0
        ):
            raise ProductionRunnerError("completed production launch lineage drifted")
        context_binding = _load_json(
            paths["context"], label="completed episode-context binding"
        )
        reverified_context = _verified_context_for_episode(
            loaded, str(request["episode_id"])
        )
        reverified_episode_context = reverified_context["episode_context"]
        consumed_context = request["episode_context"]
        expected_context_semantics = {
            "context_summary": reverified_episode_context["context_summary"],
            "speaker_map": reverified_episode_context["speaker_map"],
            "section_map": reverified_episode_context["section_map"],
            "entity_seed": reverified_episode_context["entity_seed"],
            "concept_seed": reverified_episode_context["concept_seed"],
            "extraction_guidance": reverified_context["extraction_guidance"],
            "excluded_source_context": reverified_context[
                "excluded_source_context"
            ],
        }
        expected_context_keys = {
            "schema_version",
            "episode_id",
            "completion_receipt",
            "context_artifact",
            "context_artifact_index_sha256",
            "semantic_fields_sha256",
            "loaded_semantic_fields",
            "adapter_consumed_context",
            "adapter_consumed_context_sha256",
            "deterministic_semantic_pruning",
        }
        if (
            set(context_binding) != expected_context_keys
            or context_binding.get("schema_version") != CONTEXT_BINDING_VERSION
            or context_binding.get("episode_id") != request["episode_id"]
            or context_binding.get("completion_receipt")
            != loaded["contract"]["episode_context"]["live_completion_receipt"]
            or context_binding.get("context_artifact")
            != reverified_context["artifact"]
            or context_binding.get("context_artifact_index_sha256")
            != reverified_context["artifact_index_sha256"]
            or context_binding.get("semantic_fields_sha256")
            != reverified_context["semantic_fields_sha256"]
            or context_binding.get("loaded_semantic_fields")
            != list(LOADED_SEMANTIC_FIELDS)
            or context_binding.get("adapter_consumed_context")
            != consumed_context
            or context_binding.get("adapter_consumed_context_sha256")
            != _sha256_bytes(
                _canonical_json(consumed_context).encode("utf-8")
            )
            or consumed_context.get("episode_id") != request["episode_id"]
            or any(
                consumed_context.get(field) != value
                for field, value in expected_context_semantics.items()
            )
            or context_binding.get("deterministic_semantic_pruning") is not False
        ):
            raise ProductionRunnerError("completed episode-context binding drifted")
        source_packet = _load_json(paths["input"], label="completed source packet")
        source_segments = source_packet.get("segments")
        request_source_segments = request["private_input"]["segments"]
        if (
            set(source_packet)
            != {
                "schema_version",
                "episode_id",
                "window_count",
                "context_chars",
                "segments",
            }
            or source_packet.get("schema_version") != TURN_INPUT_VERSION
            or source_packet.get("episode_id") != request["episode_id"]
            or source_packet.get("window_count") != config["window_count"]
            or source_packet.get("context_chars") != config["context_chars"]
            or not isinstance(source_segments, list)
            or any(not isinstance(row, Mapping) for row in source_segments)
            or [row.get("segment_id") for row in source_segments]
            != request["segment_ids"]
            or len(source_segments) != len(request_source_segments)
        ):
            raise ProductionRunnerError("completed source packet lineage drifted")
        for source_row, request_row in zip(
            source_segments, request_source_segments
        ):
            binding = source_row.get("expected_source_binding")
            if (
                set(source_row)
                != {
                    "job_id",
                    "segment_id",
                    "segment_index",
                    "segment_text_sha256",
                    "segment_quality",
                    "expected_source_binding",
                    "expected_source_binding_sha256",
                    "windows",
                    "boundaries",
                }
                or not isinstance(binding, Mapping)
                or source_row.get("expected_source_binding_sha256")
                != _source_binding_sha256(binding)
                or source_row.get("segment_text_sha256")
                != _sha256_bytes(
                    str(request_row["segment_text"]).encode("utf-8")
                )
                or source_row.get("segment_quality")
                != request_row["segment_quality"]
                or source_row.get("boundaries") != request_row["boundaries"]
                or binding.get("episode_id") != request["episode_id"]
                or not isinstance(source_row.get("job_id"), int)
                or isinstance(source_row.get("job_id"), bool)
            ):
                raise ProductionRunnerError(
                    "completed expected source binding drifted"
                )
        source_verification = queue.verify_source_packet(source_packet)
        if (
            not isinstance(source_verification, Mapping)
            or source_verification.get("state")
            != "verified_current_source_packet"
            or source_verification.get("segment_ids") != request["segment_ids"]
            or source_verification.get("segment_count")
            != request["effective_batch_size"]
            or source_verification.get("production_mutated") is not False
        ):
            raise ProductionRunnerError(
                "completed source binding re-verification drifted"
            )
        if (
            paths["prompt"].read_text(encoding="utf-8") != request["prompt"]
            or paths["base"].read_text(encoding="utf-8")
            != request["base_instructions"]
            or _load_json(paths["schema"], label="completed output schema")
            != request["output_schema"]
        ):
            raise ProductionRunnerError("completed request artifact content drifted")
        capacity_payload = _load_json(
            paths["capacity"], label="completed capacity admission"
        )
        claim = _load_json(paths["claim"], label="completed claim receipt")
        claim_job_ids = claim.get("job_ids")
        if (
            claim.get("schema_version") != CLAIM_RECEIPT_VERSION
            or claim.get("state") != "claimed"
            or claim.get("episode_id") != request["episode_id"]
            or not isinstance(claim_job_ids, list)
            or [row["job_id"] for row in source_segments] != claim_job_ids
            or claim.get("segment_ids") != request["segment_ids"]
            or claim.get("label_pack") != FROZEN_LABEL_PACK
            or claim.get("lane") != config["lane"]
            or claim.get("payload_model_policy")
            != "null_or_exact_frozen_queue_model"
            or claim.get("queue_payload_model") != FROZEN_QUEUE_PAYLOAD_MODEL
            or claim.get("prepared_request") != _record(paths["request"])
            or claim.get("semantic_retry_count") != 0
        ):
            raise ProductionRunnerError("completed claim receipt lineage drifted")
        sidecar, output = _validate_completed_turn(
            request=request,
            sidecar_path=paths["sidecar"],
            output_path=paths["output"],
            thread_id=thread_id,
            seen_turn_ids=seen_turn_ids,
        )
        _validate_completed_capacity_admission(
            capacity_payload,
            request=request,
            turn_started_at=sidecar.get("started_at"),
            expected_fixture_mode=fixture_mode,
        )
        try:
            projected = episode_batch.validate_and_project_output(request, output)
        except episode_batch.CanonicalV31EpisodeBatchError as exc:
            raise ProductionRunnerError("completed output projection drifted") from exc
        if (
            json.loads(
                paths["projected"].read_text(encoding="utf-8")
            )
            != projected["labels"]
            or _load_json(paths["provenance"], label="completed evidence provenance")
            != projected["provenance"]
            or _load_json(paths["fidelity"], label="completed semantic fidelity")
            != projected["fidelity"]
        ):
            raise ProductionRunnerError("completed projected artifacts drifted")
        usage = _valid_usage(payload.get("usage"))
        segment_records = payload.get("segment_records")
        canonical_rows = projected["labels"]
        if (
            payload.get("turn_id") != sidecar["turn_id"]
            or usage != _valid_usage(sidecar.get("usage"))
            or payload.get("wall_elapsed_seconds")
            != sidecar.get("wall_elapsed_seconds")
            or not isinstance(segment_records, list)
            or len(segment_records) != request["effective_batch_size"]
            or [row.get("segment_id") for row in segment_records]
            != request["segment_ids"]
            or [row.get("job_id") for row in segment_records] != claim_job_ids
            or not isinstance(canonical_rows, list)
            or len(canonical_rows) != len(segment_records)
        ):
            raise ProductionRunnerError("completed production accounting drifted")
        for row, canonical_row in zip(segment_records, canonical_rows):
            if (
                not isinstance(row, Mapping)
                or not isinstance(row.get("job_id"), int)
                or isinstance(row.get("job_id"), bool)
                or not isinstance(row.get("submission"), Mapping)
                or str(row["submission"].get("job_id") or "")
                != str(row["job_id"])
                or not str(row["submission"].get("label_id") or "")
            ):
                raise ProductionRunnerError("completed segment record drifted")
            segment_output = _verify_record(
                row.get("output"), label="completed segment output"
            )
            if _load_json(
                segment_output, label="completed segment output"
            ) != canonical_row:
                raise ProductionRunnerError("completed segment output drifted")
        completed.append(
            {
                "terminal": _record(terminal_path),
                "sidecar": _record(paths["sidecar"]),
                "thread_id_sha256": _sha256_bytes(thread_id.encode("utf-8")),
                "turn_id_sha256": _sha256_bytes(turn_id.encode("utf-8")),
                "thread_persisted_path_sha256": launch.get(
                    "thread_persisted_path_sha256"
                ),
                "thread_resumed": launch.get("thread_resumed"),
                "prior_completed_turn_ids_sha256": launch.get(
                    "prior_completed_turn_ids_sha256"
                ),
                "resume_source_packet_records_sha256": launch.get(
                    "resume_source_packet_records_sha256"
                ),
                "prior_usage_sha256": launch.get("prior_usage_sha256"),
                "managed_chatgpt_plan_type": sidecar.get("plan_type"),
                "context_control_overlay_sha256": request.get(
                    "context_control_overlay_sha256"
                ),
                "effective_instruction_sources_sha256": launch.get(
                    "effective_instruction_sources_sha256"
                ),
                "effective_instruction_sources_count": launch.get(
                    "effective_instruction_sources_count"
                ),
                "source_packet": _record(paths["input"]),
                "source_packet_sha256": _sha256_file(paths["input"]),
                "source_verification": dict(source_verification),
                "thread_total_usage": _valid_usage(
                    sidecar.get("thread_total_usage")
                ),
                "turn_started_at": sidecar.get("started_at"),
                "usage": usage,
                "wall_elapsed_seconds": float(sidecar["wall_elapsed_seconds"]),
                "semantic_model_call_count": 1,
                "accounting_complete": True,
                "segment_records": [dict(row) for row in segment_records],
                "submitted_segment_count": len(segment_records),
                "episode_id": request["episode_id"],
                "batch_id": request["batch_id"],
            }
        )
    return completed


def _verify_existing_run_receipt(
    path: Path,
    *,
    loaded: Mapping[str, Any],
    root: Path,
    runtime_config_path: Path,
    queue: QueueOperations,
    fixture_mode: bool,
) -> dict[str, Any]:
    payload = _load_json(path, label="existing production run receipt")
    base = _receipt_base(loaded)
    if (
        any(payload.get(key) != value for key, value in base.items())
        or payload.get("state") not in {"passed", "waiting"}
        or payload.get("runtime_config") != _record(runtime_config_path)
        or not isinstance(payload.get("production_mutated"), bool)
        or (
            payload.get("semantic_model_call_count") is not None
            and (
                isinstance(payload.get("semantic_model_call_count"), bool)
                or not isinstance(payload.get("semantic_model_call_count"), int)
                or int(payload["semantic_model_call_count"]) < 0
            )
        )
    ):
        raise ProductionRunnerError("existing production run receipt drifted")
    if payload["state"] != "passed":
        return payload
    completed = _completed_turn_evidence(
        root, loaded, fixture_mode=fixture_mode, queue=queue
    )
    call_count = int(payload["semantic_model_call_count"])
    if call_count == 0:
        if (
            completed
            or payload.get("usage") != {field: 0 for field in USAGE_FIELDS}
            or payload.get("episode_thread_archives", []) != []
        ):
            raise ProductionRunnerError("zero-call production receipt drifted")
        return payload
    submission_verification = queue.verify_completed_submissions(completed)
    aggregate = _aggregate_completed_turns(completed)
    if base["thread_mode"] == "same_thread":
        verified_archive_records = _verify_completed_same_thread_lifecycles(
            completed=completed,
            root=root,
            loaded=loaded,
            queue=queue,
            fixture_mode=fixture_mode,
        )
    else:
        lifecycle_root = root / "episodes"
        if lifecycle_root.exists() and any(lifecycle_root.rglob("thread-*.json")):
            raise ProductionRunnerError(
                "new-thread receipt contains durable thread lifecycle artifacts"
            )
        verified_archive_records = []
    usage_path = _verify_record(
        payload.get("usage_telemetry"), label="production usage telemetry"
    )
    telemetry = _load_json(usage_path, label="production usage telemetry")
    expected_usage = aggregate["usage"]
    if (
        usage_path != (root / "usage-telemetry.json").resolve()
        or len(completed) != call_count
        or submission_verification.get("state")
        != "verified_completed_submissions"
        or submission_verification.get("submitted_segment_count")
        != aggregate["submitted_segment_count"]
        or payload.get("usage") != expected_usage
        or payload.get("wall_elapsed_seconds") != aggregate["wall_elapsed_seconds"]
        or payload.get("episode_thread_count") != aggregate["episode_thread_count"]
        or payload.get("unique_turn_count") != aggregate["unique_turn_count"]
        or payload.get("submitted_segment_count")
        != aggregate["submitted_segment_count"]
        or payload.get("production_mutated") is not True
        or payload.get("verified_evaluation_artifact_hashes")
        != loaded["evaluation"]["artifact_hashes"]
        or payload.get("episode_thread_archives")
        != verified_archive_records
        or telemetry.get("schema_version") != USAGE_TELEMETRY_VERSION
        or telemetry.get("measured_turn_count") != call_count
        or any(telemetry.get(field) != expected_usage[field] for field in USAGE_FIELDS)
        or telemetry.get("wall_time_seconds") != aggregate["wall_elapsed_seconds"]
        or telemetry.get("turn_sidecars") != aggregate["turn_sidecars"]
        or telemetry.get("frozen_configuration_sha256")
        != base["frozen_configuration_sha256"]
        or telemetry.get("winner_system_id") != base["winner_system_id"]
        or telemetry.get("model") != base["model"]
        or telemetry.get("reasoning_effort") != base["reasoning_effort"]
        or telemetry.get("batch_size") != base["batch_size"]
        or telemetry.get("thread_mode") != base["thread_mode"]
        or telemetry.get("episode_thread_archives")
        != verified_archive_records
        or isinstance(
            payload.get("remaining_unresolved_scoped_job_count"), bool
        )
        or not isinstance(
            payload.get("remaining_unresolved_scoped_job_count"), int
        )
        or int(payload["remaining_unresolved_scoped_job_count"]) < 0
        or telemetry.get("remaining_unresolved_scoped_job_count")
        != payload.get("remaining_unresolved_scoped_job_count")
    ):
        raise ProductionRunnerError("existing production usage receipt drifted")
    return payload


def _aggregate_completed_turns(
    completed: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    usage_total = Counter()
    wall_total = 0.0
    thread_ids: set[str] = set()
    turn_ids: set[str] = set()
    sidecars: list[dict[str, Any]] = []
    submitted = 0
    for entry in completed:
        usage_total.update(_valid_usage(entry.get("usage")))
        wall = entry.get("wall_elapsed_seconds")
        if (
            isinstance(wall, bool)
            or not isinstance(wall, (int, float))
            or not math.isfinite(float(wall))
            or float(wall) < 0
        ):
            raise ProductionRunnerError("completed production wall time drifted")
        wall_total += float(wall)
        thread_hash = entry.get("thread_id_sha256")
        turn_hash = entry.get("turn_id_sha256")
        if (
            not isinstance(thread_hash, str)
            or len(thread_hash) != 64
            or not isinstance(turn_hash, str)
            or len(turn_hash) != 64
            or turn_hash in turn_ids
        ):
            raise ProductionRunnerError("completed production identity hash drifted")
        thread_ids.add(thread_hash)
        turn_ids.add(turn_hash)
        sidecar = entry.get("sidecar")
        _verify_record(sidecar, label="completed aggregate sidecar")
        sidecars.append(dict(sidecar))
        count = entry.get("submitted_segment_count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ProductionRunnerError("completed submission count drifted")
        submitted += count
    return {
        "semantic_model_call_count": len(completed),
        "episode_thread_count": len(thread_ids),
        "unique_turn_count": len(turn_ids),
        "submitted_segment_count": submitted,
        "usage": {field: int(usage_total[field]) for field in USAGE_FIELDS},
        "wall_elapsed_seconds": round(wall_total, 6),
        "turn_sidecars": sidecars,
    }


def _raw_completed_identity(entry: Mapping[str, Any]) -> dict[str, Any]:
    terminal_path = _verify_record(
        entry.get("terminal"), label="completed resume terminal"
    )
    terminal = _load_json(terminal_path, label="completed resume terminal")
    thread_id = terminal.get("thread_id")
    turn_id = terminal.get("turn_id")
    started_at = entry.get("turn_started_at")
    try:
        parsed_started_at = datetime.fromisoformat(
            str(started_at).replace("Z", "+00:00")
        )
    except (TypeError, ValueError) as exc:
        raise ProductionRunnerError(
            "completed resume turn timestamp drifted"
        ) from exc
    if (
        not isinstance(thread_id, str)
        or not thread_id
        or not isinstance(turn_id, str)
        or not turn_id
        or _sha256_bytes(thread_id.encode("utf-8"))
        != entry.get("thread_id_sha256")
        or _sha256_bytes(turn_id.encode("utf-8"))
        != entry.get("turn_id_sha256")
        or parsed_started_at.tzinfo is None
    ):
        raise ProductionRunnerError("completed resume identity drifted")
    return {
        "terminal": dict(entry["terminal"]),
        "thread_id": thread_id,
        "turn_id": turn_id,
        "started_at": parsed_started_at,
        "persisted_path_sha256": entry.get("thread_persisted_path_sha256"),
        "thread_resumed": entry.get("thread_resumed"),
        "prior_completed_turn_ids_sha256": entry.get(
            "prior_completed_turn_ids_sha256"
        ),
        "resume_source_packet_records_sha256": entry.get(
            "resume_source_packet_records_sha256"
        ),
        "prior_usage_sha256": entry.get("prior_usage_sha256"),
        "managed_chatgpt_plan_type": entry.get("managed_chatgpt_plan_type"),
        "context_control_overlay_sha256": entry.get(
            "context_control_overlay_sha256"
        ),
        "effective_instruction_sources_sha256": entry.get(
            "effective_instruction_sources_sha256"
        ),
        "effective_instruction_sources_count": entry.get(
            "effective_instruction_sources_count"
        ),
        "source_packet": entry.get("source_packet"),
        "source_packet_sha256": entry.get("source_packet_sha256"),
        "usage": _valid_usage(entry.get("usage")),
        "thread_total_usage": _valid_usage(entry.get("thread_total_usage")),
    }


def _same_thread_resume_state(
    completed: Sequence[Mapping[str, Any]],
    *,
    queue: QueueOperations,
) -> dict[str, Any] | None:
    residual = []
    for episode_id in sorted({str(entry["episode_id"]) for entry in completed}):
        remaining = queue.pending_episode_segment_count(episode_id=episode_id)
        if remaining:
            residual.append((episode_id, remaining))
    if not residual:
        return None
    if len(residual) != 1:
        raise ProductionRunnerWaiting(
            "same-thread recovery found multiple residual episode threads"
        )
    episode_id, remaining = residual[0]
    identities = [
        _raw_completed_identity(entry)
        for entry in completed
        if str(entry["episode_id"]) == episode_id
    ]
    identities.sort(key=lambda item: item["started_at"])
    thread_ids = {str(item["thread_id"]) for item in identities}
    persisted_hashes = {
        str(item["persisted_path_sha256"])
        for item in identities
        if isinstance(item.get("persisted_path_sha256"), str)
    }
    if (
        not identities
        or len(thread_ids) != 1
        or len(persisted_hashes) != 1
        or len(next(iter(persisted_hashes))) != 64
    ):
        raise ProductionRunnerWaiting(
            "same-thread durable resume lineage is not unique"
        )
    prior_turn_ids: list[str] = []
    prior_usage = Counter()
    source_packet_records: list[dict[str, Any]] = []
    expected_sources = episode_batch.expected_instruction_source_contract()
    expected_overlay_sha256 = _sha256_bytes(
        _canonical_json(episode_batch.verified_context_control_overlay()).encode(
            "utf-8"
        )
    )
    for identity in identities:
        expected_prior_hash = _sha256_bytes(
            _canonical_json(prior_turn_ids).encode("utf-8")
        )
        if identity.get("prior_completed_turn_ids_sha256") != expected_prior_hash:
            raise ProductionRunnerWaiting(
                "same-thread prior-turn lineage drifted"
            )
        if (
            identity.get("managed_chatgpt_plan_type") != "pro"
            or identity.get("context_control_overlay_sha256")
            != expected_overlay_sha256
            or identity.get("effective_instruction_sources_sha256")
            != expected_sources["effective_instruction_sources_sha256"]
            or identity.get("effective_instruction_sources_count")
            != expected_sources["effective_instruction_sources_count"]
            or not isinstance(identity.get("source_packet"), Mapping)
            or identity.get("source_packet_sha256")
            != identity["source_packet"].get("sha256")
        ):
            raise ProductionRunnerWaiting(
                "same-thread Pro, overlay, or source lineage drifted"
            )
        expected_prior_usage = {
            field: int(prior_usage[field]) for field in USAGE_FIELDS
        }
        expected_prior_usage_sha256 = _sha256_bytes(
            _canonical_json(expected_prior_usage).encode("utf-8")
        )
        expected_source_records_sha256 = _sha256_bytes(
            _canonical_json(source_packet_records).encode("utf-8")
        )
        if identity.get("thread_resumed") is True:
            if (
                identity.get("prior_usage_sha256")
                != expected_prior_usage_sha256
                or identity.get("resume_source_packet_records_sha256")
                != expected_source_records_sha256
            ):
                raise ProductionRunnerWaiting(
                    "same-thread resume source or usage binding drifted"
                )
        elif (
            identity.get("prior_usage_sha256") is not None
            or identity.get("resume_source_packet_records_sha256") is not None
        ):
            raise ProductionRunnerWaiting(
                "non-resumed same-thread turn carried resume-only lineage"
            )
        prior_usage.update(identity["usage"])
        expected_thread_total = {
            field: int(prior_usage[field]) for field in USAGE_FIELDS
        }
        if identity["thread_total_usage"] != expected_thread_total:
            raise ProductionRunnerWaiting(
                "same-thread cumulative usage lineage drifted"
            )
        source_packet_records.append(dict(identity["source_packet"]))
        prior_turn_ids.append(str(identity["turn_id"]))
    if len(set(prior_turn_ids)) != len(prior_turn_ids):
        raise ProductionRunnerWaiting("same-thread completed turn ids are duplicated")
    return {
        "episode_id": episode_id,
        "remaining_scoped_segment_count": remaining,
        "thread_id": next(iter(thread_ids)),
        "thread_id_sha256": _sha256_bytes(
            next(iter(thread_ids)).encode("utf-8")
        ),
        "persisted_path_sha256": next(iter(persisted_hashes)),
        "prior_completed_turn_ids": tuple(prior_turn_ids),
        "prior_completed_turn_ids_sha256": _sha256_bytes(
            _canonical_json(prior_turn_ids).encode("utf-8")
        ),
        "terminal_records": [dict(item["terminal"]) for item in identities],
        "managed_chatgpt_plan_type": "pro",
        "context_control_overlay_sha256": expected_overlay_sha256,
        "effective_instruction_sources_sha256": expected_sources[
            "effective_instruction_sources_sha256"
        ],
        "effective_instruction_sources_count": expected_sources[
            "effective_instruction_sources_count"
        ],
        "source_packet_records": source_packet_records,
        "source_packet_records_sha256": _sha256_bytes(
            _canonical_json(source_packet_records).encode("utf-8")
        ),
        "prior_usage": {field: int(prior_usage[field]) for field in USAGE_FIELDS},
        "prior_usage_sha256": _sha256_bytes(
            _canonical_json(
                {field: int(prior_usage[field]) for field in USAGE_FIELDS}
            ).encode("utf-8")
        ),
        "exact_thread_resume_available": True,
    }


def _episode_thread_paths(root: Path, episode_id: str) -> dict[str, Path]:
    episode_root = root / "episodes" / episode_id
    return {
        "completion": episode_root / "thread-completion.json",
        "archive": episode_root / "thread-archive.json",
    }


def _archive_state_observation(
    state: AppServerThreadArchiveState,
    *,
    expected_thread_id: str,
    expected_state: str,
) -> dict[str, Any]:
    if (
        not isinstance(state, AppServerThreadArchiveState)
        or state.thread_id != expected_thread_id
        or state.state != expected_state
        or isinstance(state.active_match_count, bool)
        or not isinstance(state.active_match_count, int)
        or isinstance(state.archived_match_count, bool)
        or not isinstance(state.archived_match_count, int)
    ):
        raise ProductionRunnerWaiting("durable thread archive state drifted")
    expected_counts = (1, 0) if expected_state == "active" else (0, 1)
    if (state.active_match_count, state.archived_match_count) != expected_counts:
        raise ProductionRunnerWaiting("durable thread archive listing drifted")
    return {
        "schema_version": EPISODE_THREAD_ARCHIVE_STATE_VERSION,
        "state": expected_state,
        "thread_id_sha256": _sha256_bytes(expected_thread_id.encode("utf-8")),
        "active_match_count": state.active_match_count,
        "archived_match_count": state.archived_match_count,
        "observation_method": "thread/list_active_and_archived",
    }


def _episode_thread_lifecycle_payloads(
    *,
    root: Path,
    episode_id: str,
    thread: AppServerThread,
    expected_turn_ids: Sequence[str],
    loaded: Mapping[str, Any],
    queue: QueueOperations,
    fixture_mode: bool,
) -> tuple[dict[str, Path], dict[str, Any], dict[str, Any]]:
    if queue.unresolved_episode_segment_count(episode_id=episode_id) != 0:
        raise ProductionRunnerWaiting(
            "same-thread episode cannot archive with unresolved scoped jobs"
        )
    completed = _completed_turn_evidence(
        root, loaded, fixture_mode=fixture_mode, queue=queue
    )
    episode_entries = [
        entry for entry in completed if str(entry["episode_id"]) == episode_id
    ]
    episode_entries.sort(
        key=lambda entry: datetime.fromisoformat(
            str(entry["turn_started_at"]).replace("Z", "+00:00")
        )
    )
    submissions = queue.verify_completed_submissions(episode_entries)
    identities = [_raw_completed_identity(entry) for entry in episode_entries]
    identities.sort(key=lambda item: item["started_at"])
    actual_turn_ids = [str(item["turn_id"]) for item in identities]
    if (
        not episode_entries
        or {str(item["thread_id"]) for item in identities} != {thread.thread_id}
        or actual_turn_ids != list(expected_turn_ids)
        or thread.ephemeral is not False
        or not isinstance(thread.persisted_path_sha256, str)
        or len(thread.persisted_path_sha256) != 64
        or any(
            entry.get("thread_persisted_path_sha256")
            != thread.persisted_path_sha256
            for entry in episode_entries
        )
        or submissions.get("state") != "verified_completed_submissions"
    ):
        raise ProductionRunnerWaiting(
            "same-thread episode completion lineage failed closed"
        )
    aggregate = _aggregate_completed_turns(episode_entries)
    paths = _episode_thread_paths(root, episode_id)
    completion = {
        "schema_version": EPISODE_THREAD_COMPLETION_VERSION,
        "state": "episode_drained_and_durable",
        "episode_id": episode_id,
        "thread_id": thread.thread_id,
        "thread_id_sha256": _sha256_bytes(thread.thread_id.encode("utf-8")),
        "thread_persisted_path_sha256": thread.persisted_path_sha256,
        "completed_turn_ids": actual_turn_ids,
        "completed_turn_ids_sha256": _sha256_bytes(
            _canonical_json(actual_turn_ids).encode("utf-8")
        ),
        "terminal_records": [dict(item["terminal"]) for item in identities],
        "sidecar_records": list(aggregate["turn_sidecars"]),
        "submitted_segment_count": aggregate["submitted_segment_count"],
        "usage": dict(aggregate["usage"]),
        "wall_elapsed_seconds": aggregate["wall_elapsed_seconds"],
        "remaining_scoped_segment_count": 0,
        "completed_submission_verification": dict(submissions),
        "production_mutated": True,
    }
    archive_base = {
        "schema_version": EPISODE_THREAD_ARCHIVE_VERSION,
        "state": "thread_archived_after_durable_episode_completion",
        "episode_id": episode_id,
        "thread_id_sha256": _sha256_bytes(thread.thread_id.encode("utf-8")),
        "thread_persisted_path_sha256": thread.persisted_path_sha256,
        "completion": _planned_record(
            paths["completion"], _pretty_json(completion).encode("utf-8")
        ),
        "semantic_model_call_count": 0,
        "semantic_retry_count": 0,
        "production_mutated": False,
    }
    return paths, completion, archive_base


def _verify_episode_thread_archive_payload(
    path: Path,
    *,
    expected_base: Mapping[str, Any],
) -> dict[str, Any]:
    payload = _load_json(path, label="episode thread archive")
    observation = payload.get("archive_state_observation")
    if (
        set(payload)
        != set(expected_base)
        | {"archive_state_observation", "archive_rpc_performed"}
        or any(payload.get(key) != value for key, value in expected_base.items())
        or not isinstance(payload.get("archive_rpc_performed"), bool)
        or not isinstance(observation, Mapping)
        or set(observation)
        != {
            "schema_version",
            "state",
            "thread_id_sha256",
            "active_match_count",
            "archived_match_count",
            "observation_method",
        }
        or observation.get("schema_version")
        != EPISODE_THREAD_ARCHIVE_STATE_VERSION
        or observation.get("state") != "archived"
        or observation.get("thread_id_sha256")
        != expected_base["thread_id_sha256"]
        or observation.get("active_match_count") != 0
        or observation.get("archived_match_count") != 1
        or observation.get("observation_method")
        != "thread/list_active_and_archived"
    ):
        raise ProductionRunnerError("episode thread archive receipt drifted")
    return payload


def _verify_episode_thread_lifecycle(
    *,
    completed: Sequence[Mapping[str, Any]],
    root: Path,
    episode_id: str,
    loaded: Mapping[str, Any],
    queue: QueueOperations,
    fixture_mode: bool,
) -> dict[str, Any]:
    thread, turn_ids = _completed_episode_thread(
        completed, episode_id=episode_id, loaded=loaded
    )
    paths, completion, archive_base = _episode_thread_lifecycle_payloads(
        root=root,
        episode_id=episode_id,
        thread=thread,
        expected_turn_ids=turn_ids,
        loaded=loaded,
        queue=queue,
        fixture_mode=fixture_mode,
    )
    if (
        not paths["completion"].is_file()
        or _load_json(
            paths["completion"], label="episode thread completion"
        )
        != completion
        or not paths["archive"].is_file()
    ):
        raise ProductionRunnerError("episode thread lifecycle receipt is incomplete")
    _verify_episode_thread_archive_payload(
        paths["archive"], expected_base=archive_base
    )
    return {
        "completion": _record(paths["completion"]),
        "archive": _record(paths["archive"]),
    }


async def _finalize_same_thread_episode(
    *,
    client: Any,
    root: Path,
    episode_id: str,
    thread: AppServerThread,
    expected_turn_ids: Sequence[str],
    loaded: Mapping[str, Any],
    queue: QueueOperations,
    fixture_mode: bool,
) -> dict[str, Any]:
    paths, completion, archive_base = _episode_thread_lifecycle_payloads(
        root=root,
        episode_id=episode_id,
        thread=thread,
        expected_turn_ids=expected_turn_ids,
        loaded=loaded,
        queue=queue,
        fixture_mode=fixture_mode,
    )
    _write_immutable(
        paths["completion"], _pretty_json(completion).encode("utf-8")
    )
    if paths["archive"].is_file():
        _verify_episode_thread_archive_payload(
            paths["archive"], expected_base=archive_base
        )
    else:
        observed = await client.thread_archive_state(thread.thread_id)
        archive_rpc_performed = False
        if observed.state == "active":
            _archive_state_observation(
                observed,
                expected_thread_id=thread.thread_id,
                expected_state="active",
            )
            await client.archive_thread(thread.thread_id)
            archive_rpc_performed = True
            observed = await client.thread_archive_state(thread.thread_id)
        elif observed.state != "archived":
            raise ProductionRunnerWaiting(
                "durable thread is absent from active and archived listings"
            )
        archive = {
            **archive_base,
            "archive_state_observation": _archive_state_observation(
                observed,
                expected_thread_id=thread.thread_id,
                expected_state="archived",
            ),
            "archive_rpc_performed": archive_rpc_performed,
        }
        _write_immutable(paths["archive"], _pretty_json(archive).encode("utf-8"))
    return {
        "completion": _record(paths["completion"]),
        "archive": _record(paths["archive"]),
    }


def _completed_episode_thread(
    completed: Sequence[Mapping[str, Any]],
    *,
    episode_id: str,
    loaded: Mapping[str, Any],
) -> tuple[AppServerThread, tuple[str, ...]]:
    entries = [
        entry for entry in completed if str(entry["episode_id"]) == episode_id
    ]
    identities = [_raw_completed_identity(entry) for entry in entries]
    identities.sort(key=lambda item: item["started_at"])
    thread_ids = {str(item["thread_id"]) for item in identities}
    persisted_hashes = {
        str(item["persisted_path_sha256"])
        for item in identities
        if isinstance(item.get("persisted_path_sha256"), str)
    }
    if not identities or len(thread_ids) != 1 or len(persisted_hashes) != 1:
        raise ProductionRunnerWaiting(
            "completed same-thread episode lifecycle is not unique"
        )
    last_terminal_path = _verify_record(
        identities[-1]["terminal"], label="episode lifecycle terminal"
    )
    last_terminal = _load_json(
        last_terminal_path, label="episode lifecycle terminal"
    )
    request_path = _verify_record(
        last_terminal.get("prepared_request"),
        label="episode lifecycle prepared request",
    )
    request = episode_batch.validate_prepared_request(
        _load_json(request_path, label="episode lifecycle prepared request")
    )
    frozen = loaded["frozen_configuration"]
    runtime_binding = frozen["runtime_binding"]
    turn_ids = tuple(str(item["turn_id"]) for item in identities)
    return (
        AppServerThread(
            thread_id=next(iter(thread_ids)),
            model=str(loaded["contract"]["configuration"]["model"]),
            cwd=str(Path.cwd().resolve()),
            ephemeral=False,
            instruction_sources_sha256=str(
                frozen["runtime_binding"]["effective_instruction_sources_sha256"]
            ),
            instruction_sources_count=int(
                frozen["runtime_binding"]["effective_instruction_sources_count"]
            ),
            base_instructions_sha256=str(request["base_instructions_sha256"]),
            base_instructions_bytes=len(
                str(request["base_instructions"]).encode("utf-8")
            ),
            reasoning_effort=str(
                loaded["contract"]["configuration"]["effort"]
            ),
            persisted_path_sha256=next(iter(persisted_hashes)),
            prior_completed_turn_ids=turn_ids,
        ),
        turn_ids,
    )


def _verify_completed_same_thread_lifecycles(
    *,
    completed: Sequence[Mapping[str, Any]],
    root: Path,
    loaded: Mapping[str, Any],
    queue: QueueOperations,
    fixture_mode: bool,
) -> list[dict[str, Any]]:
    episode_ids = sorted({str(entry["episode_id"]) for entry in completed})
    expected_completions = {
        _episode_thread_paths(root, episode_id)["completion"].resolve()
        for episode_id in episode_ids
    }
    expected_archives = {
        _episode_thread_paths(root, episode_id)["archive"].resolve()
        for episode_id in episode_ids
    }
    lifecycle_root = root / "episodes"
    actual_completions = (
        {
            path.resolve()
            for path in lifecycle_root.glob("*/thread-completion.json")
        }
        if lifecycle_root.exists()
        else set()
    )
    actual_archives = (
        {path.resolve() for path in lifecycle_root.glob("*/thread-archive.json")}
        if lifecycle_root.exists()
        else set()
    )
    if (
        actual_completions != expected_completions
        or actual_archives != expected_archives
    ):
        raise ProductionRunnerError("episode thread lifecycle set drifted")
    records: list[dict[str, Any]] = []
    thread_ids: set[str] = set()
    for episode_id in episode_ids:
        thread, _turn_ids = _completed_episode_thread(
            completed, episode_id=episode_id, loaded=loaded
        )
        if thread.thread_id in thread_ids:
            raise ProductionRunnerError(
                "durable episode thread was reused across episodes"
            )
        thread_ids.add(thread.thread_id)
        records.append(
            _verify_episode_thread_lifecycle(
                completed=completed,
                root=root,
                episode_id=episode_id,
                loaded=loaded,
                queue=queue,
                fixture_mode=fixture_mode,
            )
        )
    return records


async def _reconcile_completed_same_thread_archives(
    *,
    completed: Sequence[Mapping[str, Any]],
    root: Path,
    loaded: Mapping[str, Any],
    queue: QueueOperations,
    fixture_mode: bool,
    client_factory: Callable[[Path], Any],
    context_control_overlay: Mapping[str, Any],
) -> list[dict[str, Any]]:
    episode_ids = sorted({str(entry["episode_id"]) for entry in completed})
    if not episode_ids:
        return []
    needs_archive = any(
        not _episode_thread_paths(root, episode_id)["archive"].is_file()
        for episode_id in episode_ids
    )
    expected_completions = {
        _episode_thread_paths(root, episode_id)["completion"].resolve()
        for episode_id in episode_ids
    }
    expected_archives = {
        _episode_thread_paths(root, episode_id)["archive"].resolve()
        for episode_id in episode_ids
    }
    actual_completions = {
        path.resolve() for path in (root / "episodes").glob("*/thread-completion.json")
    } if (root / "episodes").exists() else set()
    actual_archives = {
        path.resolve() for path in (root / "episodes").glob("*/thread-archive.json")
    } if (root / "episodes").exists() else set()
    if (
        actual_completions - expected_completions
        or actual_archives - expected_archives
    ):
        raise ProductionRunnerError("episode thread lifecycle set drifted")
    if not needs_archive and expected_completions == actual_completions:
        return _verify_completed_same_thread_lifecycles(
            completed=completed,
            root=root,
            loaded=loaded,
            queue=queue,
            fixture_mode=fixture_mode,
        )
    if not needs_archive:
        class _ArchiveForbidden:
            async def archive_thread(self, _thread_id: str) -> None:
                raise AssertionError("verified archive must not be replayed")

        records = []
        for episode_id in episode_ids:
            thread, turn_ids = _completed_episode_thread(
                completed, episode_id=episode_id, loaded=loaded
            )
            records.append(
                await _finalize_same_thread_episode(
                    client=_ArchiveForbidden(),
                    root=root,
                    episode_id=episode_id,
                    thread=thread,
                    expected_turn_ids=turn_ids,
                    loaded=loaded,
                    queue=queue,
                    fixture_mode=fixture_mode,
                )
            )
        return records
    binary = loaded["artifact_paths"]["codex_binary"]
    client_context = (
        client_factory(binary)
        if fixture_mode
        else _default_client_factory(
            binary, config_overlay=context_control_overlay
        )
    )
    records: list[dict[str, Any]] = []
    async with client_context as client:
        account = getattr(client, "account_summary", None)
        if (
            not isinstance(account, Mapping)
            or account.get("type") != "chatgpt"
            or account.get("plan_type") != "pro"
        ):
            raise ProductionRunnerWaiting(
                "managed ChatGPT Pro auth failed during thread archive recovery"
            )
        for episode_id in episode_ids:
            thread, turn_ids = _completed_episode_thread(
                completed, episode_id=episode_id, loaded=loaded
            )
            records.append(
                await _finalize_same_thread_episode(
                    client=client,
                    root=root,
                    episode_id=episode_id,
                    thread=thread,
                    expected_turn_ids=turn_ids,
                    loaded=loaded,
                    queue=queue,
                    fixture_mode=fixture_mode,
                )
            )
    return records


def _usage_telemetry_payload(
    loaded: Mapping[str, Any], aggregate: Mapping[str, Any]
) -> dict[str, Any]:
    contract = loaded["contract"]
    config = contract["configuration"]
    return {
        "schema_version": USAGE_TELEMETRY_VERSION,
        "thread_id": contract["thread_id"],
        "cutover_id": contract["cutover_id"],
        "transport": TRANSPORT,
        "auth_mode": "chatgpt",
        "persistent_transport": True,
        "structured_output": True,
        "accounting_complete": True,
        "cache_telemetry_complete": True,
        "measured_turn_count": aggregate["semantic_model_call_count"],
        **dict(aggregate["usage"]),
        "wall_time_seconds": aggregate["wall_elapsed_seconds"],
        "turn_sidecars": list(aggregate["turn_sidecars"]),
        "frozen_configuration_sha256": config["frozen_configuration_sha256"],
        "winner_system_id": config["winner_system_id"],
        "model": config["model"],
        "reasoning_effort": config["effort"],
        "batch_size": config["batch_size"],
        "thread_mode": config["thread_mode"],
        "episode_context_artifact_index_sha256": loaded["episode_context"][
            "context_artifact_index_sha256"
        ],
        "semantic_surface": episode_batch.CANDIDATE_SYSTEM_ID,
    }


def _write_prepared_batch_artifacts(
    paths: Mapping[str, Path], prepared: Mapping[str, Any]
) -> None:
    request = prepared["request"]
    _write_immutable(
        paths["input"], _pretty_json(prepared["source_packet"]).encode("utf-8")
    )
    _write_immutable(
        paths["context"],
        _pretty_json(prepared["context_binding"]).encode("utf-8"),
    )
    _write_immutable(paths["request"], _pretty_json(request).encode("utf-8"))
    _write_immutable(paths["prompt"], str(request["prompt"]).encode("utf-8"))
    _write_immutable(
        paths["base"], str(request["base_instructions"]).encode("utf-8")
    )
    _write_immutable(
        paths["schema"], _pretty_json(request["output_schema"]).encode("utf-8")
    )


async def _verify_capacity_admission(
    *,
    provider: CapacityAdmissionProvider | None,
    request: Mapping[str, Any],
    capacity_path: Path,
    fixture_mode: bool,
    remaining_batch_count: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    if provider is None:
        raise ProductionRunnerWaiting(
            "reserve-aware capacity admission is required before claim and turn"
        )
    value = provider(
        request=request,
        remaining_batch_count=remaining_batch_count,
        timeout_seconds=timeout_seconds,
    )
    if inspect.isawaitable(value):
        value = await value
    if isinstance(value, Path):
        payload = _load_json(value.expanduser().resolve(), label="capacity admission")
    elif isinstance(value, Mapping):
        payload = dict(value)
    else:
        raise ProductionRunnerWaiting("capacity provider returned no auditable receipt")
    requirements = _capacity_requirements(
        request,
        remaining_batch_count=remaining_batch_count,
        timeout_seconds=timeout_seconds,
    )
    policy = production_contract.capacity_admission_contract()
    common = (
        payload.get("schema_version") == CAPACITY_RECEIPT_VERSION
        and payload.get("requirements") == requirements
        and payload.get("capacity_policy") == policy
        and payload.get("capacity_policy_sha256")
        == _sha256_bytes(_canonical_json(policy).encode("utf-8"))
        and isinstance(payload.get("available_total_tokens"), int)
        and not isinstance(payload.get("available_total_tokens"), bool)
        and int(payload["available_total_tokens"]) >= requirements["required_total_tokens"]
        and isinstance(payload.get("available_wall_seconds"), int)
        and not isinstance(payload.get("available_wall_seconds"), bool)
        and int(payload["available_wall_seconds"]) >= requirements["required_wall_seconds"]
        and payload.get("reset_credits_counted_as_capacity") is False
        and payload.get("semantic_authority") is False
        and payload.get("semantic_pruning_or_relabeling_allowed") is False
        and payload.get("capacity_probe_before_thread_start") is True
        and payload.get("semantic_thread_started") is False
        and payload.get("turn_started") is False
        and payload.get("sidecar_started") is False
    )
    if fixture_mode:
        valid = bool(
            common
            and payload.get("state") == "admitted_offline_fixture_non_promotable"
            and payload.get("production_scoped") is False
            and payload.get("fixture_only") is True
            and payload.get("offline_fixture_non_promotable") is True
            and payload.get("live_capacity_authority") is False
            and payload.get("official_method") is None
            and payload.get("operator_deadline") is None
            and payload.get("protocol_schema") is None
            and payload.get("raw_snapshot_sha256") is None
            and payload.get("selected_limit_id") is None
            and payload.get("applicable_controls") == []
        )
    else:
        protocol_path = None
        deadline_path = None
        try:
            protocol_path = _verify_record(
                payload.get("protocol_schema"), label="capacity protocol schema"
            )
            deadline_path = _verify_record(
                payload.get("operator_deadline"), label="capacity operator deadline"
            )
        except ProductionRunnerError:
            pass
        valid = bool(
            common
            and payload.get("state")
            == "admitted_official_managed_auth_numeric_reserve"
            and payload.get("production_scoped") is True
            and payload.get("fixture_only") is False
            and payload.get("offline_fixture_non_promotable") is False
            and payload.get("live_capacity_authority") is True
            and payload.get("official_method") == "account/rateLimits/read"
            and payload.get("request_params_shape")
            == "empty_object_tolerated_by_pinned_client"
            and protocol_path
            == Path(__file__).resolve().parent
            / "protocol/codex_app_server_0_144_1/v2/GetAccountRateLimitsResponse.json"
            and deadline_path is not None
            and isinstance(payload.get("raw_snapshot_sha256"), str)
            and len(payload["raw_snapshot_sha256"]) == 64
            and payload.get("selected_limit_id") == "codex"
            and isinstance(payload.get("applicable_controls"), list)
            and bool(payload["applicable_controls"])
        )
    if not valid:
        raise ProductionRunnerWaiting("capacity admission failed closed")
    _write_immutable(capacity_path, _pretty_json(payload).encode("utf-8"))
    expected_sha256 = _sha256_bytes(_canonical_json(payload).encode("utf-8"))
    persisted = _load_json(capacity_path, label="persisted capacity admission")
    if _sha256_bytes(_canonical_json(persisted).encode("utf-8")) != expected_sha256:
        raise ProductionRunnerWaiting("persisted capacity admission drifted")
    return {
        "receipt": payload,
        "receipt_sha256": expected_sha256,
        "record": _record(capacity_path),
        "remaining_batch_count": remaining_batch_count,
    }


def _verified_context_control_overlay(loaded: Mapping[str, Any]) -> dict[str, Any]:
    try:
        overlay = episode_batch.verified_context_control_overlay()
    except episode_batch.CanonicalV31EpisodeBatchError as exc:
        raise ProductionRunnerError("frozen context-control overlay is unavailable") from exc
    if not isinstance(overlay, Mapping):
        raise ProductionRunnerError("frozen context-control overlay is malformed")
    actual_sha256 = _sha256_bytes(_canonical_json(overlay).encode("utf-8"))
    frozen = loaded["frozen_configuration"]
    runtime_binding = frozen["runtime_binding"]
    try:
        sources = episode_batch.expected_instruction_source_contract()
    except episode_batch.CanonicalV31EpisodeBatchError as exc:
        raise ProductionRunnerError("frozen app-server context contract drifted") from exc
    if (
        actual_sha256 != runtime_binding["context_control_overlay_sha256"]
        or sources["effective_instruction_sources_sha256"]
        != runtime_binding["effective_instruction_sources_sha256"]
        or sources["effective_instruction_sources_count"]
        != runtime_binding["effective_instruction_sources_count"]
    ):
        raise ProductionRunnerError("frozen app-server context contract drifted")
    return dict(overlay)


def _validate_started_thread(
    thread: AppServerThread,
    *,
    request: Mapping[str, Any],
    loaded: Mapping[str, Any],
    resumed: bool = False,
    expected_completed_turn_ids: tuple[str, ...] = (),
    expected_persisted_path_sha256: str | None = None,
) -> None:
    instruction_sources = episode_batch.expected_instruction_source_contract()
    same_thread = request["thread_mode"] == "same_thread"
    expected_ephemeral = not same_thread
    if (
        not isinstance(thread.thread_id, str)
        or not thread.thread_id
        or thread.model != request["model"]
        or thread.cwd != str(Path.cwd().resolve())
        or thread.ephemeral is not expected_ephemeral
        or thread.base_instructions_sha256 != request["base_instructions_sha256"]
        or thread.base_instructions_bytes
        != len(str(request["base_instructions"]).encode("utf-8"))
        or thread.instruction_sources_sha256
        != instruction_sources["effective_instruction_sources_sha256"]
        or thread.instruction_sources_count
        != instruction_sources["effective_instruction_sources_count"]
        or (
            same_thread
            and (
                not isinstance(thread.persisted_path_sha256, str)
                or len(thread.persisted_path_sha256) != 64
            )
        )
        or (not same_thread and thread.persisted_path_sha256 is not None)
        or (
            resumed
            and (
                thread.reasoning_effort
                != request["effort"]
                or thread.prior_completed_turn_ids != expected_completed_turn_ids
                or thread.persisted_path_sha256
                != expected_persisted_path_sha256
            )
        )
        or (not resumed and thread.prior_completed_turn_ids)
    ):
        raise ProductionRunnerWaiting(
            "started app-server thread drifted from the frozen instruction contract"
        )


class ProductionCodexAppServerClient(
    episode_batch.CanonicalV31CodexAppServerClient
):
    """Apply the frozen context overlay to durable production resumes too."""

    async def _request(self, method: str, params: dict[str, Any]) -> Any:
        request_params = dict(params)
        if method == "thread/resume":
            if "config" in request_params:
                raise ProductionRunnerError(
                    "thread/resume config overlay was supplied twice"
                )
            request_params["config"] = dict(self._canonical_v31_config_overlay)
            request_params["personality"] = "none"
            request_params["environments"] = []
            request_params["dynamicTools"] = []
        return await super()._request(method, request_params)


def _default_client_factory(
    binary: Path, *, config_overlay: Mapping[str, Any]
) -> CodexAppServerClient:
    return ProductionCodexAppServerClient(
        config_overlay=config_overlay,
        command=[str(binary), "app-server", "--stdio", "--strict-config"],
    )


def _queue_production_mutated(
    queue: QueueOperations, claimed: Sequence[QueueItem]
) -> bool:
    return bool(claimed) or getattr(queue, "production_mutated", False) is True


async def _run_production_queue_locked(
    *,
    contract_path: Path,
    output_root: Path,
    queue: QueueOperations,
    limit: int,
    dry_run: bool = True,
    fixture_mode: bool = False,
    client_factory: Callable[[Path], Any] = _default_client_factory,
    capacity_provider: CapacityAdmissionProvider | None = None,
) -> dict[str, Any]:
    loaded = load_contract(
        contract_path,
        queue=(queue if isinstance(queue, SQLiteWorkerQueue) and not fixture_mode else None),
    )
    contract = loaded["contract"]
    config = contract["configuration"]
    if getattr(queue, "lane", None) != config["lane"]:
        raise ProductionRunnerError(
            "queue lane does not match the frozen production contract"
        )
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ProductionRunnerError("production start limit must be positive")
    root = output_root.expanduser().resolve()
    context_control_overlay = _verified_context_control_overlay(loaded)
    overlay_path = root / "context-control-overlay.json"
    _write_immutable(
        overlay_path,
        _pretty_json(context_control_overlay).encode("utf-8"),
    )
    runtime_config_path = root / "runtime-config.json"
    _write_immutable(
        runtime_config_path,
        _pretty_json(
            _runtime_config(
                loaded,
                context_control_overlay=_record(overlay_path),
            )
        ).encode("utf-8"),
    )
    receipt_base = _receipt_base(loaded)
    if (
        not dry_run
        and not fixture_mode
        and not loaded["live_production_enabled"]
    ):
        receipt = {
            **receipt_base,
            "state": "waiting",
            "terminal_reason": "episode_context_production_contract_not_released",
            "semantic_model_call_count": 0,
            "runtime_config": _record(runtime_config_path),
            "production_mutated": False,
        }
        _write_immutable(
            root / "run-receipt.json", _pretty_json(receipt).encode("utf-8")
        )
        return receipt
    run_receipt_path = root / "run-receipt.json"
    if run_receipt_path.is_file():
        return _verify_existing_run_receipt(
            run_receipt_path,
            loaded=loaded,
            root=root,
            runtime_config_path=runtime_config_path,
            queue=queue,
            fixture_mode=fixture_mode,
        )
    try:
        completed = _completed_turn_evidence(
            root, loaded, fixture_mode=fixture_mode, queue=queue
        )
    except (
        ProductionRunnerError,
        ProductionRunnerWaiting,
        episode_batch.CanonicalV31EpisodeBatchError,
        OSError,
    ) as exc:
        receipt = {
            **receipt_base,
            "state": "waiting",
            "terminal_reason": "completed_turn_integrity_waiting_no_replay",
            "error_class": type(exc).__name__,
            "error_sha256": _sha256_bytes(str(exc).encode("utf-8")),
            "semantic_model_call_count": None,
            "usage": None,
            "accounting_complete": False,
            "runtime_config": _record(runtime_config_path),
            "production_mutated": True,
        }
        _write_immutable(run_receipt_path, _pretty_json(receipt).encode("utf-8"))
        return receipt
    partial = _partial_turn_evidence(root, loaded)
    if partial:
        completed_aggregate = _aggregate_completed_turns(completed)
        try:
            completed_submissions = queue.verify_completed_submissions(completed)
        except (ProductionRunnerError, OSError, sqlite3.Error) as exc:
            completed_submissions = {
                "state": "completed_submission_verification_failed",
                "error_class": type(exc).__name__,
                "error_sha256": _sha256_bytes(str(exc).encode("utf-8")),
            }
        known_call_count = all(
            isinstance(entry.get("semantic_model_call_count"), int)
            and not isinstance(entry.get("semantic_model_call_count"), bool)
            for entry in partial
        )
        accounting_complete = bool(
            known_call_count
            and all(entry.get("accounting_complete") is True for entry in partial)
        )
        partial_usage: dict[str, int] | None = None
        if accounting_complete:
            usage_counter = Counter(completed_aggregate["usage"])
            for entry in partial:
                usage_counter.update(_valid_usage(entry["usage"]))
            partial_usage = {
                field: int(usage_counter[field]) for field in USAGE_FIELDS
            }
        receipt = {
            **receipt_base,
            "state": "waiting",
            "terminal_reason": "partial_turn_preserved_without_replay",
            "semantic_attempt_count": len(completed) + len(partial),
            "semantic_model_call_count": (
                completed_aggregate["semantic_model_call_count"]
                + sum(int(entry["semantic_model_call_count"]) for entry in partial)
                if known_call_count
                else None
            ),
            "usage": partial_usage,
            "accounting_complete": accounting_complete,
            "completed_turns": completed,
            "completed_turn_aggregate": completed_aggregate,
            "completed_submission_verification": completed_submissions,
            "partial_turns": partial,
            "runtime_config": _record(runtime_config_path),
            "production_mutated": True,
        }
        _write_immutable(run_receipt_path, _pretty_json(receipt).encode("utf-8"))
        return receipt
    resume_state: dict[str, Any] | None = None
    prior_aggregate: dict[str, Any] | None = None
    prior_submission_verification: dict[str, Any] | None = None
    completed_archive_records: list[dict[str, Any]] = []
    if completed:
        aggregate = _aggregate_completed_turns(completed)
        try:
            submission_verification = queue.verify_completed_submissions(completed)
        except (ProductionRunnerError, OSError, sqlite3.Error) as exc:
            receipt = {
                **receipt_base,
                "state": "waiting",
                "terminal_reason": "completed_submission_reconciliation_waiting_no_replay",
                "error_class": type(exc).__name__,
                "error_sha256": _sha256_bytes(str(exc).encode("utf-8")),
                **{
                    key: aggregate[key]
                    for key in (
                        "semantic_model_call_count",
                        "episode_thread_count",
                        "unique_turn_count",
                        "submitted_segment_count",
                        "usage",
                        "wall_elapsed_seconds",
                    )
                },
                "accounting_complete": True,
                "completed_turns": completed,
                "recovery_new_semantic_model_call_count": 0,
                "runtime_config": _record(runtime_config_path),
                "production_mutated": True,
            }
            _write_immutable(
                run_receipt_path, _pretty_json(receipt).encode("utf-8")
            )
            return receipt
        if config["thread_mode"] == "same_thread":
            resume_state = _same_thread_resume_state(completed, queue=queue)
        archive_completed = (
            [
                entry
                for entry in completed
                if resume_state is None
                or str(entry["episode_id"]) != str(resume_state["episode_id"])
            ]
            if config["thread_mode"] == "same_thread"
            else []
        )
        if archive_completed:
            try:
                completed_archive_records = (
                    await _reconcile_completed_same_thread_archives(
                        completed=archive_completed,
                        root=root,
                        loaded=loaded,
                        queue=queue,
                        fixture_mode=fixture_mode,
                        client_factory=client_factory,
                        context_control_overlay=context_control_overlay,
                    )
                )
            except (
                AppServerError,
                ProductionRunnerError,
                ProductionRunnerWaiting,
                episode_batch.CanonicalV31EpisodeBatchError,
                OSError,
                sqlite3.Error,
            ) as exc:
                receipt = {
                    **receipt_base,
                    "schema_version": RESUME_CHECKPOINT_VERSION,
                    "state": "waiting",
                    "terminal_reason": (
                        "same_thread_archive_recovery_waiting_no_replay"
                    ),
                    "error_class": type(exc).__name__,
                    "error_sha256": _sha256_bytes(str(exc).encode("utf-8")),
                    **{
                        key: aggregate[key]
                        for key in (
                            "semantic_model_call_count",
                            "episode_thread_count",
                            "unique_turn_count",
                            "submitted_segment_count",
                            "usage",
                            "wall_elapsed_seconds",
                        )
                    },
                    "accounting_complete": True,
                    "runtime_config": _record(runtime_config_path),
                    "production_mutated": True,
                }
                checkpoint = (
                    root
                    / "resume-checkpoints"
                    / (
                        "archive-"
                        f"{aggregate['semantic_model_call_count']:06d}-"
                        f"{receipt['error_sha256'][:12]}.json"
                    )
                )
                _write_immutable(
                    checkpoint, _pretty_json(receipt).encode("utf-8")
                )
                return receipt
        if resume_state is not None:
            prior_aggregate = aggregate
            prior_submission_verification = dict(submission_verification)
        else:
            usage_path = root / "usage-telemetry.json"
            unresolved_scoped_job_count = queue.unresolved_scoped_job_count()
            recovered_usage = _usage_telemetry_payload(loaded, aggregate)
            recovered_usage["episode_thread_archives"] = (
                completed_archive_records
            )
            recovered_usage["remaining_unresolved_scoped_job_count"] = (
                unresolved_scoped_job_count
            )
            _write_immutable(
                usage_path,
                _pretty_json(recovered_usage).encode("utf-8"),
            )
            preview = queue.preview_episode_batch(
                limit=(
                    int(config["batch_size"])
                    if config["thread_mode"] == "same_thread"
                    else min(int(limit), int(config["batch_size"]))
                )
            )
            receipt = {
                **receipt_base,
                "state": (
                    "passed"
                    if not preview and unresolved_scoped_job_count == 0
                    else "waiting"
                ),
                "terminal_reason": (
                    "completed_turns_reconciled_without_replay"
                    if not preview and unresolved_scoped_job_count == 0
                    else (
                        "unresolved_scoped_jobs_require_reconciliation"
                        if not preview
                        else "completed_turns_reconciled_fresh_root_required"
                    )
                ),
                **{
                    key: aggregate[key]
                    for key in (
                        "semantic_model_call_count",
                        "episode_thread_count",
                        "unique_turn_count",
                        "submitted_segment_count",
                        "usage",
                        "wall_elapsed_seconds",
                    )
                },
                "accounting_complete": True,
                "completed_turns": completed,
                "completed_submission_verification": dict(
                    submission_verification
                ),
                "recovery_new_semantic_model_call_count": 0,
                "remaining_unresolved_scoped_job_count": (
                    unresolved_scoped_job_count
                ),
                "episode_thread_archives": completed_archive_records,
                "usage_telemetry": _record(usage_path),
                "verified_evaluation_artifact_hashes": loaded["evaluation"][
                    "artifact_hashes"
                ],
                "runtime_config": _record(runtime_config_path),
                "production_mutated": True,
            }
            _write_immutable(
                run_receipt_path, _pretty_json(receipt).encode("utf-8")
            )
            return receipt
    if dry_run:
        preview = queue.preview_episode_batch(
            limit=(
                int(config["batch_size"])
                if config["thread_mode"] == "same_thread"
                else min(int(limit), int(config["batch_size"]))
            )
        )
        prepared_batch_count = 0
        context_verified = False
        if preview:
            _prepare_episode_batch(loaded, preview)
            prepared_batch_count = 1
            context_verified = True
        receipt = {
            **receipt_base,
            "state": "dry_run",
            "terminal_reason": "production_execution_not_authorized",
            "eligible_job_count": len(preview),
            "episode_count": len({item.episode_id for item in preview}),
            "planned_batch_count": prepared_batch_count,
            "episode_context_verified": context_verified,
            "semantic_surface": episode_batch.CANDIDATE_SYSTEM_ID,
            "semantic_model_call_count": 0,
            "runtime_config": _record(runtime_config_path),
            "production_mutated": False,
        }
        _write_immutable(root / "dry-run-receipt.json", _pretty_json(receipt).encode("utf-8"))
        return receipt
    context_mode_ready = (
        loaded["episode_context_fixture_ready"]
        if fixture_mode
        else loaded["episode_context_ready"]
    )
    if not context_mode_ready:
        receipt = {
            **receipt_base,
            "state": "waiting",
            "terminal_reason": "episode_context_completion_not_production_ready",
            "semantic_model_call_count": 0,
            "runtime_config": _record(runtime_config_path),
            "production_mutated": False,
        }
        _write_immutable(root / "run-receipt.json", _pretty_json(receipt).encode("utf-8"))
        return receipt
    if os.environ.get("OPENAI_API_KEY"):
        raise ProductionRunnerError("OPENAI_API_KEY must be absent for managed-auth production")

    try:
        stale_recovery = queue.recover_stale_prelaunch_batch(
            limit=(
                int(config["batch_size"])
                if config["thread_mode"] == "same_thread"
                else min(int(limit), int(config["batch_size"]))
            )
        )
    except (ProductionRunnerWaiting, OSError, sqlite3.Error) as exc:
        receipt = {
            **receipt_base,
            "state": "waiting",
            "terminal_reason": "stale_claim_recovery_waiting_no_replay",
            "error_class": type(exc).__name__,
            "error_sha256": _sha256_bytes(str(exc).encode("utf-8")),
            "semantic_model_call_count": 0,
            "runtime_config": _record(runtime_config_path),
            "production_mutated": getattr(queue, "production_mutated", False) is True,
        }
        _write_immutable(
            root / "run-receipt.json", _pretty_json(receipt).encode("utf-8")
        )
        return receipt
    if stale_recovery.get("state") == "semantic_attempt_preserved":
        receipt = {
            **receipt_base,
            "state": "waiting",
            "terminal_reason": "stale_semantic_attempt_preserved_without_replay",
            "semantic_model_call_count": stale_recovery.get(
                "semantic_model_call_count"
            ),
            "usage": stale_recovery.get("usage"),
            "accounting_complete": stale_recovery.get("accounting_complete"),
            "stale_prelaunch_recovery": dict(stale_recovery),
            "runtime_config": _record(runtime_config_path),
            "production_mutated": False,
        }
        _write_immutable(
            root / "run-receipt.json", _pretty_json(receipt).encode("utf-8")
        )
        return receipt
    if stale_recovery.get("state") == "released_verified_prelaunch_claim":
        receipt = {
            **receipt_base,
            "state": "waiting",
            "terminal_reason": "stale_prelaunch_claim_released_fresh_root_required",
            "semantic_model_call_count": 0,
            "usage": None,
            "accounting_complete": True,
            "stale_prelaunch_recovery": dict(stale_recovery),
            "runtime_config": _record(runtime_config_path),
            "production_mutated": True,
        }
        _write_immutable(
            root / "run-receipt.json", _pretty_json(receipt).encode("utf-8")
        )
        return receipt

    model = str(config["model"])
    effort = str(config["effort"])
    thread_mode = str(config["thread_mode"])
    timeout = float(config["timeout_seconds"])
    seen_thread_ids: set[str] = (
        {str(resume_state["thread_id"])} if resume_state is not None else set()
    )
    seen_turn_ids: set[str] = (
        set(resume_state["prior_completed_turn_ids"])
        if resume_state is not None
        else set()
    )
    turn_sidecar_records: list[dict[str, Any]] = (
        list(prior_aggregate["turn_sidecars"])
        if prior_aggregate is not None
        else []
    )
    usage_total = Counter(
        prior_aggregate["usage"] if prior_aggregate is not None else {}
    )
    wall_total = float(
        prior_aggregate["wall_elapsed_seconds"]
        if prior_aggregate is not None
        else 0.0
    )
    call_count = int(
        prior_aggregate["semantic_model_call_count"]
        if prior_aggregate is not None
        else 0
    )
    submitted = int(
        prior_aggregate["submitted_segment_count"]
        if prior_aggregate is not None
        else 0
    )
    episode_thread_archives: list[dict[str, Any]] = list(
        completed_archive_records
    )
    claimed: list[QueueItem] = []
    binary = loaded["artifact_paths"]["codex_binary"]
    threads: dict[str, AppServerThread] = {}
    remaining = int(limit)
    preview = queue.preview_episode_batch(
        limit=(
            int(config["batch_size"])
            if thread_mode == "same_thread"
            else min(remaining, int(config["batch_size"]))
        ),
        episode_id=(
            str(resume_state["episode_id"])
            if resume_state is not None
            else None
        ),
    )
    if not preview:
        unresolved_scoped_job_count = queue.unresolved_scoped_job_count()
        receipt = {
            **receipt_base,
            "state": "waiting" if unresolved_scoped_job_count else "passed",
            "terminal_reason": (
                "unresolved_scoped_jobs_require_reconciliation"
                if unresolved_scoped_job_count
                else "no_eligible_scoped_label_segment_jobs"
            ),
            "semantic_model_call_count": 0,
            "usage": {field: 0 for field in USAGE_FIELDS},
            "wall_elapsed_seconds": 0.0,
            "remaining_unresolved_scoped_job_count": (
                unresolved_scoped_job_count
            ),
            "stale_prelaunch_recovery": dict(stale_recovery),
            "runtime_config": _record(runtime_config_path),
            "production_mutated": getattr(queue, "production_mutated", False) is True,
        }
        _write_immutable(root / "run-receipt.json", _pretty_json(receipt).encode("utf-8"))
        return receipt
    prepared_preview = _prepare_episode_batch(loaded, preview)
    episode_start_authorized: set[str] = set()
    if thread_mode == "same_thread":
        first_episode_id = preview[0].episode_id
        queue.assert_episode_start_allowed(
            first_episode_id,
            thread_mode=thread_mode,
            output_root=root,
        )
        episode_start_authorized.add(first_episode_id)
    episode_turn_ids: dict[str, list[str]] = {}
    if resume_state is not None:
        episode_turn_ids[str(resume_state["episode_id"])] = list(
            resume_state["prior_completed_turn_ids"]
        )
    try:
        if fixture_mode and client_factory is _default_client_factory:
            raise ProductionRunnerWaiting(
                "offline fixture execution requires an explicit nonpromotable client"
            )
        client_context = (
            client_factory(binary)
            if fixture_mode
            else _default_client_factory(
                binary,
                config_overlay=context_control_overlay,
            )
        )
        async with client_context as client:
            account = getattr(client, "account_summary", None)
            if (
                not isinstance(account, Mapping)
                or account.get("type") != "chatgpt"
                or account.get("plan_type") != "pro"
            ):
                raise ProductionRunnerWaiting("managed ChatGPT Pro auth verification failed")
            active_capacity_provider = capacity_provider
            if not fixture_mode:
                deadline_value = os.environ.get("PIF_PRODUCTION_DEADLINE_FILE")
                if not deadline_value:
                    raise ProductionRunnerWaiting(
                        "checksum-bound operator deadline is required for live capacity"
                    )
                active_capacity_provider = OfficialManagedAuthCapacityProvider(
                    client=client,
                    deadline_path=Path(deadline_value),
                    production_contract_sha256=loaded["sha256"],
                )
            while preview:
                prepared = prepared_preview or _prepare_episode_batch(loaded, preview)
                prepared_preview = None
                request = prepared["request"]
                episode_id = str(request["episode_id"])
                if (
                    thread_mode == "same_thread"
                    and episode_id not in episode_start_authorized
                ):
                    queue.assert_episode_start_allowed(
                        episode_id,
                        thread_mode=thread_mode,
                        output_root=root,
                    )
                    episode_start_authorized.add(episode_id)
                paths = _turn_paths(
                    root,
                    episode_id=episode_id,
                    batch_id=str(request["batch_id"]),
                )
                _write_prepared_batch_artifacts(paths, prepared)
                if thread_mode == "same_thread":
                    pending_episode_segments = queue.pending_episode_segment_count(
                        episode_id=episode_id
                    )
                    if pending_episode_segments < len(preview):
                        raise ProductionRunnerWaiting(
                            "same-thread pending episode count drifted"
                        )
                    remaining_batch_count = max(
                        1,
                        (
                            pending_episode_segments
                            + int(config["batch_size"])
                            - 1
                        )
                        // int(config["batch_size"]),
                    )
                else:
                    remaining_batch_count = max(
                        1,
                        (remaining + int(config["batch_size"]) - 1)
                        // int(config["batch_size"]),
                    )
                try:
                    capacity = await _verify_capacity_admission(
                        provider=active_capacity_provider,
                        request=request,
                        capacity_path=paths["capacity"],
                        fixture_mode=fixture_mode,
                        remaining_batch_count=remaining_batch_count,
                        timeout_seconds=timeout,
                    )
                except ProductionRunnerWaiting as exc:
                    capacity_checkpoint = {
                        "schema_version": RESUME_CHECKPOINT_VERSION,
                        "state": "waiting",
                        "terminal_reason": "capacity_admission_denied_before_claim_or_turn",
                        "episode_id": episode_id,
                        "batch_id": request["batch_id"],
                        "prepared_request": _record(paths["request"]),
                        "source_packet": _record(paths["input"]),
                        "semantic_model_call_count": 0,
                        "error_class": type(exc).__name__,
                        "error_sha256": _sha256_bytes(str(exc).encode("utf-8")),
                        "production_mutated": False,
                    }
                    checkpoint_path = (
                        root
                        / "capacity-checkpoints"
                        / f"{request['batch_id']}.json"
                    )
                    _write_immutable(
                        checkpoint_path,
                        _pretty_json(capacity_checkpoint).encode("utf-8"),
                    )
                    raise
                thread = threads.get(episode_id) if thread_mode == "same_thread" else None
                prior_completed_turn_ids = tuple(episode_turn_ids.get(episode_id, []))
                resumed_thread = False
                resumed_lineage: Mapping[str, Any] | None = None
                batch_claimed = queue.claim_episode_batch(
                    preview,
                    prepared_request_path=paths["request"],
                    claim_receipt_path=paths["claim"],
                )
                claimed.extend(batch_claimed)
                if [item.job_id for item in batch_claimed] != [
                    item.job_id for item in preview
                ]:
                    raise ProductionRunnerWaiting("exact batch claim lineage drifted")
                try:
                    queue.verify_source_packet(prepared["source_packet"])
                except (ProductionRunnerError, ProductionRunnerWaiting, OSError, sqlite3.Error) as exc:
                    release = queue.release_verified_prelaunch_batch(
                        batch_claimed,
                        prepared_request_path=paths["request"],
                        claim_receipt_path=paths["claim"],
                        reason=str(exc),
                        thread_started_for_batch=False,
                    )
                    claimed = [
                        item
                        for item in claimed
                        if item.job_id not in {row.job_id for row in batch_claimed}
                    ]
                    raise ProductionRunnerWaiting(
                        "postclaim source drift released before semantic dispatch: "
                        f"{release['state']}"
                    ) from exc
                if thread is None:
                    if (
                        resume_state is not None
                        and episode_id == resume_state["episode_id"]
                    ):
                        if prior_completed_turn_ids != tuple(
                            resume_state["prior_completed_turn_ids"]
                        ):
                            raise ProductionRunnerWaiting(
                                "same-thread resume turn lineage drifted before transport"
                            )
                        thread = await client.resume_thread(
                            thread_id=str(resume_state["thread_id"]),
                            model=model,
                            effort=effort,
                            base_instructions=str(request["base_instructions"]),
                            cwd=Path.cwd(),
                            expected_instruction_sources_sha256=(
                                episode_batch.expected_instruction_source_contract()[
                                    "effective_instruction_sources_sha256"
                                ]
                            ),
                            expected_instruction_sources_count=(
                                episode_batch.expected_instruction_source_contract()[
                                    "effective_instruction_sources_count"
                                ]
                            ),
                            expected_completed_turn_ids=prior_completed_turn_ids,
                        )
                        resumed_thread = True
                        resumed_lineage = dict(resume_state)
                    else:
                        thread = await client.start_thread(
                            model=model,
                            base_instructions=str(request["base_instructions"]),
                            cwd=Path.cwd(),
                            ephemeral=thread_mode == "new_thread",
                        )
                    try:
                        _validate_started_thread(
                            thread,
                            request=request,
                            loaded=loaded,
                            resumed=resumed_thread,
                            expected_completed_turn_ids=prior_completed_turn_ids,
                            expected_persisted_path_sha256=(
                                str(resume_state["persisted_path_sha256"])
                                if resumed_thread and resume_state is not None
                                else None
                            ),
                        )
                    except ProductionRunnerWaiting as exc:
                        queue.release_verified_prelaunch_batch(
                            batch_claimed,
                            prepared_request_path=paths["request"],
                            claim_receipt_path=paths["claim"],
                            reason=str(exc),
                            thread_started_for_batch=True,
                        )
                        claimed = [
                            item
                            for item in claimed
                            if item.job_id not in {row.job_id for row in batch_claimed}
                        ]
                        raise
                    if (
                        not thread.thread_id
                        or (
                            not resumed_thread
                            and thread.thread_id in seen_thread_ids
                        )
                    ):
                        raise ProductionRunnerWaiting(
                            "app-server returned a duplicate episode thread id"
                        )
                    seen_thread_ids.add(thread.thread_id)
                    if thread_mode == "same_thread":
                        threads[episode_id] = thread
                    if resumed_thread:
                        resume_state = None
                elif thread.base_instructions_sha256 != request[
                    "base_instructions_sha256"
                ]:
                    raise ProductionRunnerWaiting(
                        "same-episode thread base instructions drifted"
                    )
                launch = {
                        "schema_version": TURN_LAUNCH_VERSION,
                        "contract_sha256": loaded["sha256"],
                        "evaluation_id": loaded["evaluation"]["evaluation_id"],
                        "frozen_configuration_sha256": config["frozen_configuration_sha256"],
                        "winner_system_id": config["winner_system_id"],
                        "episode_id": episode_id,
                        "batch_id": request["batch_id"],
                        "job_ids": [item.job_id for item in batch_claimed],
                        "segment_ids": [item.segment_id for item in batch_claimed],
                        "thread_id": thread.thread_id,
                        "thread_ephemeral": thread.ephemeral,
                        "thread_persisted_path_sha256": (
                            thread.persisted_path_sha256
                        ),
                        "thread_resumed": resumed_thread,
                        "prior_completed_turn_ids_sha256": _sha256_bytes(
                            _canonical_json(
                                list(prior_completed_turn_ids)
                            ).encode("utf-8")
                        ),
                        "managed_chatgpt_plan_type": "pro",
                        "context_control_overlay_sha256": request[
                            "context_control_overlay_sha256"
                        ],
                        "resume_source_packet_records_sha256": (
                            resumed_lineage["source_packet_records_sha256"]
                            if resumed_lineage is not None
                            else None
                        ),
                        "prior_usage_sha256": (
                            resumed_lineage["prior_usage_sha256"]
                            if resumed_lineage is not None
                            else None
                        ),
                        "model": model,
                        "reasoning_effort": effort,
                        "configured_batch_size": config["batch_size"],
                        "thread_mode": request["thread_mode"],
                        "turn_batch_size": len(batch_claimed),
                        "semantic_retry_count": 0,
                        "episode_context_completion_receipt": contract[
                            "episode_context"
                        ]["live_completion_receipt"],
                        "episode_context_binding": _record(paths["context"]),
                        "episode_batch_adapter": contract["artifacts"][
                            "episode_batch_adapter"
                        ],
                        "prepared_request": _record(paths["request"]),
                        "capacity_admission": capacity["record"],
                        "context_control_overlay": _record(overlay_path),
                        "effective_instruction_sources_sha256": (
                            episode_batch.expected_instruction_source_contract()[
                                "effective_instruction_sources_sha256"
                            ]
                        ),
                        "effective_instruction_sources_count": (
                            episode_batch.expected_instruction_source_contract()[
                                "effective_instruction_sources_count"
                            ]
                        ),
                        "claim_receipt": _record(paths["claim"]),
                        "source_packet": _record(paths["input"]),
                        "prompt": _record(paths["prompt"]),
                        "base_instructions": _record(paths["base"]),
                        "output_schema": _record(paths["schema"]),
                    }
                _write_immutable(paths["launch"], _pretty_json(launch).encode("utf-8"))
                queue.bind_prompt(batch_claimed, paths["prompt"])
                call_count += 1
                await client.run_structured_turn(
                        thread=thread,
                        effort=effort,
                        prompt=str(request["prompt"]),
                        output_schema=request["output_schema"],
                        sidecar_path=paths["sidecar"],
                        output_path=paths["output"],
                        batch_size=len(batch_claimed),
                        thread_mode=str(request["thread_mode"]),
                        timeout_seconds=timeout,
                    )
                sidecar, output = _validate_completed_turn(
                        request=request,
                        sidecar_path=paths["sidecar"],
                        output_path=paths["output"],
                        thread_id=thread.thread_id,
                        seen_turn_ids=seen_turn_ids,
                    )
                rows, projected = _validate_batch_output(
                    output,
                    request=request,
                    items=batch_claimed,
                )
                _write_immutable(
                    paths["projected"],
                    _pretty_json(projected["labels"]).encode("utf-8"),
                )
                _write_immutable(
                    paths["provenance"],
                    _pretty_json(projected["provenance"]).encode("utf-8"),
                )
                _write_immutable(
                    paths["fidelity"],
                    _pretty_json(projected["fidelity"]).encode("utf-8"),
                )
                segment_records = []
                for item, row in zip(batch_claimed, rows):
                        segment_path = Path(item.output_path).expanduser().resolve()
                        _write_immutable(segment_path, _pretty_json(row).encode("utf-8"))
                        submission = queue.submit(item, segment_path)
                        submitted += 1
                        segment_records.append(
                            {"job_id": item.job_id, "segment_id": item.segment_id,
                             "output": _record(segment_path), "submission": dict(submission)}
                        )
                usage = _valid_usage(sidecar["usage"])
                usage_total.update(usage)
                wall_total += float(sidecar["wall_elapsed_seconds"])
                turn_sidecar_records.append(_record(paths["sidecar"]))
                terminal = {
                        "schema_version": TURN_TERMINAL_VERSION,
                        "state": "passed",
                        "evaluation_id": loaded["evaluation"]["evaluation_id"],
                        "frozen_configuration_sha256": config["frozen_configuration_sha256"],
                        "winner_system_id": config["winner_system_id"],
                        "model": model,
                        "reasoning_effort": effort,
                        "configured_batch_size": config["batch_size"],
                            "turn_batch_size": len(batch_claimed),
                        "launch": _record(paths["launch"]),
                        "context": _record(paths["context"]),
                        "prepared_request": _record(paths["request"]),
                        "capacity_admission": _record(paths["capacity"]),
                        "context_control_overlay": _record(overlay_path),
                        "claim_receipt": _record(paths["claim"]),
                        "sidecar": _record(paths["sidecar"]),
                        "output": _record(paths["output"]),
                        "source_packet": _record(paths["input"]),
                        "canonical_labels": _record(paths["projected"]),
                        "evidence_provenance": _record(paths["provenance"]),
                        "semantic_fidelity": _record(paths["fidelity"]),
                        "thread_id": thread.thread_id,
                        "turn_id": sidecar["turn_id"],
                        "usage": usage,
                        "wall_elapsed_seconds": sidecar["wall_elapsed_seconds"],
                        "segment_records": segment_records,
                        "semantic_retry_count": 0,
                    }
                _write_immutable(
                    paths["terminal"], _pretty_json(terminal).encode("utf-8")
                )
                episode_turn_ids.setdefault(episode_id, []).append(
                    str(sidecar["turn_id"])
                )
                remaining -= len(batch_claimed)
                if thread_mode == "same_thread":
                    residual = queue.preview_episode_batch(
                        limit=int(config["batch_size"]),
                        episode_id=episode_id,
                    )
                    if residual:
                        preview = residual
                        continue
                    episode_thread_archives.append(
                        await _finalize_same_thread_episode(
                            client=client,
                            root=root,
                            episode_id=episode_id,
                            thread=thread,
                            expected_turn_ids=episode_turn_ids[episode_id],
                            loaded=loaded,
                            queue=queue,
                            fixture_mode=fixture_mode,
                        )
                    )
                    threads.pop(episode_id, None)
                if remaining <= 0:
                    break
                preview = queue.preview_episode_batch(
                    limit=(
                        int(config["batch_size"])
                        if thread_mode == "same_thread"
                        else min(remaining, int(config["batch_size"]))
                    )
                )
    except (
        AppServerError,
        ProductionRunnerWaiting,
        EpisodeContextRunnerError,
        episode_batch.CanonicalV31EpisodeBatchError,
        OSError,
        sqlite3.Error,
    ) as exc:
        if thread_mode == "same_thread":
            try:
                resumable_completed = _completed_turn_evidence(
                    root, loaded, fixture_mode=fixture_mode, queue=queue
                )
                resumable_partial = _partial_turn_evidence(root, loaded)
                resumable_state = (
                    _same_thread_resume_state(resumable_completed, queue=queue)
                    if resumable_completed and not resumable_partial
                    else None
                )
            except (
                ProductionRunnerError,
                ProductionRunnerWaiting,
                episode_batch.CanonicalV31EpisodeBatchError,
                OSError,
            ):
                resumable_state = None
            if resumable_state is not None:
                resumable_aggregate = _aggregate_completed_turns(
                    resumable_completed
                )
                receipt = {
                    **receipt_base,
                    "schema_version": RESUME_CHECKPOINT_VERSION,
                    "state": "waiting",
                    "terminal_reason": (
                        "same_thread_exact_resume_checkpoint_no_replay"
                    ),
                    "error_class": type(exc).__name__,
                    "error_sha256": _sha256_bytes(str(exc).encode("utf-8")),
                    **{
                        key: resumable_aggregate[key]
                        for key in (
                            "semantic_model_call_count",
                            "episode_thread_count",
                            "unique_turn_count",
                            "submitted_segment_count",
                            "usage",
                            "wall_elapsed_seconds",
                        )
                    },
                    "accounting_complete": True,
                    "resume_state": {
                        key: value
                        for key, value in resumable_state.items()
                        if key not in {"thread_id", "prior_completed_turn_ids"}
                    },
                    "runtime_config": _record(runtime_config_path),
                    "production_mutated": True,
                }
                checkpoint_path = (
                    root
                    / "resume-checkpoints"
                    / (
                        f"after-{resumable_aggregate['semantic_model_call_count']:06d}-"
                        f"{receipt['error_sha256'][:12]}.json"
                    )
                )
                _write_immutable(
                    checkpoint_path, _pretty_json(receipt).encode("utf-8")
                )
                return receipt
            if resumable_completed and not resumable_partial:
                recoverable_archive_episodes = sorted(
                    {
                        str(entry["episode_id"])
                        for entry in resumable_completed
                        if _episode_thread_paths(
                            root, str(entry["episode_id"])
                        )["completion"].is_file()
                        and not _episode_thread_paths(
                            root, str(entry["episode_id"])
                        )["archive"].is_file()
                        and queue.unresolved_episode_segment_count(
                            episode_id=str(entry["episode_id"])
                        )
                        == 0
                    }
                )
                if recoverable_archive_episodes:
                    resumable_aggregate = _aggregate_completed_turns(
                        resumable_completed
                    )
                    receipt = {
                        **receipt_base,
                        "schema_version": RESUME_CHECKPOINT_VERSION,
                        "state": "waiting",
                        "terminal_reason": (
                            "same_thread_archive_recovery_checkpoint_no_replay"
                        ),
                        "error_class": type(exc).__name__,
                        "error_sha256": _sha256_bytes(
                            str(exc).encode("utf-8")
                        ),
                        "recoverable_episode_ids_sha256": _sha256_bytes(
                            _canonical_json(
                                recoverable_archive_episodes
                            ).encode("utf-8")
                        ),
                        **{
                            key: resumable_aggregate[key]
                            for key in (
                                "semantic_model_call_count",
                                "episode_thread_count",
                                "unique_turn_count",
                                "submitted_segment_count",
                                "usage",
                                "wall_elapsed_seconds",
                            )
                        },
                        "accounting_complete": True,
                        "runtime_config": _record(runtime_config_path),
                        "production_mutated": True,
                    }
                    checkpoint_path = (
                        root
                        / "resume-checkpoints"
                        / (
                            "archive-after-"
                            f"{resumable_aggregate['semantic_model_call_count']:06d}-"
                            f"{receipt['error_sha256'][:12]}.json"
                        )
                    )
                    _write_immutable(
                        checkpoint_path, _pretty_json(receipt).encode("utf-8")
                    )
                    return receipt
        receipt = {
            **receipt_base,
            "state": "waiting",
            "terminal_reason": "operational_or_lineage_failure_no_replay",
            "error_class": type(exc).__name__,
            "error_sha256": _sha256_bytes(str(exc).encode("utf-8")),
            "semantic_model_call_count": call_count,
            "submitted_segment_count": submitted,
            "usage": dict(usage_total) if usage_total else None,
            "stale_prelaunch_recovery": dict(stale_recovery),
            "runtime_config": _record(runtime_config_path),
            "production_mutated": _queue_production_mutated(queue, claimed),
        }
        _write_immutable(root / "run-receipt.json", _pretty_json(receipt).encode("utf-8"))
        return receipt

    remaining_unresolved_scoped_job_count = queue.unresolved_scoped_job_count()
    usage_telemetry = {
        "schema_version": USAGE_TELEMETRY_VERSION,
        "thread_id": contract["thread_id"],
        "cutover_id": contract["cutover_id"],
        "transport": TRANSPORT,
        "auth_mode": "chatgpt",
        "persistent_transport": True,
        "structured_output": True,
        "accounting_complete": True,
        "cache_telemetry_complete": True,
        "measured_turn_count": call_count,
        **{field: int(usage_total[field]) for field in USAGE_FIELDS},
        "wall_time_seconds": round(wall_total, 6),
        "turn_sidecars": turn_sidecar_records,
        "frozen_configuration_sha256": config["frozen_configuration_sha256"],
        "winner_system_id": config["winner_system_id"],
        "model": model,
        "reasoning_effort": effort,
        "batch_size": config["batch_size"],
        "thread_mode": config["thread_mode"],
        "episode_context_artifact_index_sha256": loaded["episode_context"][
            "context_artifact_index_sha256"
        ],
        "semantic_surface": episode_batch.CANDIDATE_SYSTEM_ID,
        "episode_thread_archives": episode_thread_archives,
        "remaining_unresolved_scoped_job_count": (
            remaining_unresolved_scoped_job_count
        ),
    }
    usage_path = root / "usage-telemetry.json"
    _write_immutable(usage_path, _pretty_json(usage_telemetry).encode("utf-8"))
    receipt = {
        **receipt_base,
        "state": "passed",
        "terminal_reason": "bounded_queue_batch_completed",
        "semantic_model_call_count": call_count,
        "episode_thread_count": len(seen_thread_ids),
        "unique_turn_count": len(seen_turn_ids),
        "submitted_segment_count": submitted,
        "usage": {field: int(usage_total[field]) for field in USAGE_FIELDS},
        "wall_elapsed_seconds": round(wall_total, 6),
        "runtime_config": _record(runtime_config_path),
        "usage_telemetry": _record(usage_path),
        "verified_evaluation_artifact_hashes": loaded["evaluation"]["artifact_hashes"],
        "stale_prelaunch_recovery": dict(stale_recovery),
        "episode_thread_archives": episode_thread_archives,
        "remaining_unresolved_scoped_job_count": (
            remaining_unresolved_scoped_job_count
        ),
        "production_mutated": _queue_production_mutated(queue, claimed),
    }
    _write_immutable(root / "run-receipt.json", _pretty_json(receipt).encode("utf-8"))
    return receipt


async def run_production_queue(
    *,
    contract_path: Path,
    output_root: Path,
    queue: QueueOperations,
    limit: int,
    dry_run: bool = True,
    fixture_mode: bool = False,
    client_factory: Callable[[Path], Any] = _default_client_factory,
    capacity_provider: CapacityAdmissionProvider | None = None,
) -> dict[str, Any]:
    """Run one invocation while holding the exact process-wide writer lock."""

    _reject_credential_environment()
    if not fixture_mode and not dry_run and (
        client_factory is not _default_client_factory or capacity_provider is not None
    ):
        raise ProductionRunnerError(
            "live production rejects injected client or capacity providers"
        )
    if not fixture_mode and not dry_run and not isinstance(queue, SQLiteWorkerQueue):
        raise ProductionRunnerError(
            "live canonical production requires the exact SQLite label queue"
        )
    if (
        isinstance(queue, SQLiteWorkerQueue)
        and queue.database_identity.get("kind") != "sqlite_main_database"
        and not fixture_mode
    ):
        raise ProductionRunnerError(
            "live canonical production requires a real file-backed SQLite database"
        )
    writer_lock = ProductionWriterLock(
        database_identity=_database_identity(queue),
        output_root=output_root,
        contract_path=contract_path,
    )
    with writer_lock:
        return await _run_production_queue_locked(
            contract_path=contract_path,
            output_root=output_root,
            queue=queue,
            limit=limit,
            dry_run=dry_run,
            fixture_mode=fixture_mode,
            client_factory=client_factory,
            capacity_provider=capacity_provider,
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--lane")
    parser.add_argument("--worker-id", default="app-server-production-runner")
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--failed-label-reconciliation",
        choices=("plan", "execute"),
        help="plan or explicitly execute the exact failed v3.1 job reset",
    )
    parser.add_argument(
        "--authorize-failed-label-reset",
        action="store_true",
        help="required in addition to --execute for reconciliation mutation",
    )
    return parser


def _failed_label_reconciliation_receipt(
    loaded: Mapping[str, Any],
    result: Mapping[str, Any],
    *,
    mode: str,
) -> dict[str, Any]:
    return {
        **dict(result),
        "receipt_role": "failed_label_segment_reconciliation",
        "mode": mode,
        "contract_sha256": loaded["sha256"],
        "release_state": "awaiting_downstream_production_contract",
        "startup_order": list(
            FAILED_LABEL_RECONCILIATION_POLICY["startup_order"]
        ),
        "semantic_retry_count": 0,
        "model_call_authorized": False,
    }


def main(
    argv: Sequence[str] | None = None,
    *,
    _offline_test_client_factory: Callable[[Path], Any] | None = None,
    _offline_test_capacity_provider: CapacityAdmissionProvider | None = None,
) -> int:
    args = _parser().parse_args(argv)
    offline_test_mode = (
        _offline_test_client_factory is not None
        or _offline_test_capacity_provider is not None
    )
    if offline_test_mode and (
        _offline_test_client_factory is None
        or _offline_test_capacity_provider is None
    ):
        raise SystemExit(
            "offline CLI test mode requires both nonpromotable providers"
        )
    try:
        _reject_credential_environment()
    except ProductionRunnerError as exc:
        raise SystemExit(str(exc)) from exc
    reconciliation_mode = args.failed_label_reconciliation
    if reconciliation_mode == "plan" and (
        args.execute or args.authorize_failed_label_reset
    ):
        raise SystemExit(
            "failed-label reconciliation plan must be read-only"
        )
    if reconciliation_mode == "execute" and (
        not args.execute or not args.authorize_failed_label_reset
    ):
        raise SystemExit(
            "failed-label reconciliation execute requires --execute and "
            "--authorize-failed-label-reset"
        )
    if reconciliation_mode is None and args.authorize_failed_label_reset:
        raise SystemExit(
            "failed-label reset authorization requires an explicit reconciliation phase"
        )
    loaded = load_contract(args.contract)
    config = loaded["contract"]["configuration"]
    lane = str(config["lane"])
    if args.lane is not None and args.lane != lane:
        raise SystemExit("requested lane does not match the frozen production contract")
    database = args.database.expanduser().resolve()
    if not database.is_file():
        raise SystemExit("database is missing")
    uri = f"file:{database.as_posix()}?mode={'rw' if args.execute else 'ro'}"
    conn = sqlite3.connect(uri, uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    if not args.execute:
        conn.execute("PRAGMA query_only = ON")
    queue = SQLiteWorkerQueue(
        conn,
        lane=lane,
        worker_id=args.worker_id,
        label_pack=str(config["label_pack"]),
        model=str(config["queue_payload_model"]),
        semantic_model=str(config["model"]),
        output_root=args.output_root,
    )
    try:
        if reconciliation_mode is not None:
            with ProductionWriterLock(
                database_identity=_database_identity(queue),
                output_root=args.output_root,
                contract_path=args.contract,
            ):
                loaded = load_contract(args.contract, queue=queue)
                plan_path = (
                    args.output_root.expanduser().resolve()
                    / "failed-label-reconciliation-plan.json"
                )
                current_plan = _failed_label_reconciliation_receipt(
                    loaded,
                    queue.reconcile_failed_scoped_jobs(execute=False),
                    mode="plan",
                )
                if reconciliation_mode == "plan":
                    _write_immutable(
                        plan_path,
                        _pretty_json(current_plan).encode("utf-8"),
                    )
                    result = current_plan
                else:
                    if not plan_path.is_file():
                        raise SystemExit(
                            "failed-label reconciliation execute requires the immutable plan receipt"
                        )
                    frozen_plan = _load_json(
                        plan_path,
                        label="failed-label reconciliation plan receipt",
                    )
                    if frozen_plan != current_plan:
                        raise SystemExit(
                            "failed-label reconciliation plan drifted before execute"
                        )
                    result = _failed_label_reconciliation_receipt(
                        loaded,
                        queue.reconcile_failed_scoped_jobs(execute=True),
                        mode="execute",
                    )
                    _write_immutable(
                        args.output_root.expanduser().resolve()
                        / "failed-label-reconciliation-execution.json",
                        _pretty_json(result).encode("utf-8"),
                    )
            print(json.dumps(result, ensure_ascii=True, sort_keys=True))
            return 0
        result = asyncio.run(
            run_production_queue(
                contract_path=args.contract,
                output_root=args.output_root,
                queue=queue,
                limit=args.limit,
                dry_run=not args.execute,
                fixture_mode=offline_test_mode,
                client_factory=(
                    _offline_test_client_factory
                    if _offline_test_client_factory is not None
                    else _default_client_factory
                ),
                capacity_provider=_offline_test_capacity_provider,
            )
        )
    finally:
        conn.close()
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0 if result.get("state") in {"passed", "dry_run", "waiting"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
