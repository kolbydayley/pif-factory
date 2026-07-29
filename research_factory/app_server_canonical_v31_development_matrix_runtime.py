from __future__ import annotations

"""Checksum-bound live executor for the canonical v3.1 development matrix.

The sibling ``app_server_canonical_v31_development_matrix`` module deliberately
has no dispatch path.  This module is the small, separately checksum-bound
executor for a later operator-authorized extraction epoch.  It consumes (and
never creates or installs) both the supervisor semantic plan and the offline
matrix future-plan binding.  Quality evaluation, holdout work, and production
mutation are intentionally absent.

The only semantic execution surface is the canonical adapter's
``run_episode_batch_arm``.  Live execution accepts only the exact default
managed client and in-module capacity guard.  Explicit offline fixture mode
allows injected probes, clients, runners, environment, and clock state so the
complete coordinator can be tested without auth, network, or model calls.
"""

import asyncio
import copy
import contextvars
import fcntl
import functools
import hashlib
import inspect
import json
import math
import os
import stat
import threading
import time
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import app_server_canonical_v31_development_matrix as offline_matrix
from . import app_server_canonical_v31_episode_batch as adapter
from . import app_server_thread_supervisor as supervisor


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RUNTIME_VERSION = "pif_canonical_v31_development_matrix_runtime_v1"
RUNTIME_BINDING_VERSION = "pif_canonical_v31_development_runtime_binding_v1"
CAPACITY_MEASUREMENT_VERSION = "pif_canonical_v31_capacity_measurement_v1"
EXTRACTION_RECEIPT_VERSION = "pif_canonical_v31_extraction_only_receipt_v1"
EXTRACTION_RECEIPT_FILENAME = "extraction-only-receipt.json"
OFFLINE_TEST_MODE = "offline_fixture_non_promotable"
TRUSTED_CAPACITY_IMPLEMENTATION_STATE = (
    "epoch7_bound_official_rate_limits_estimate_no_reservation_v1"
)

EXPECTED_ARMS = offline_matrix.EXPECTED_ARMS
USAGE_FIELDS = adapter.USAGE_FIELDS

_ARTIFACT_ROLES = (
    "attempt",
    "sidecar",
    "raw_output",
    "canonical_labels",
    "evidence_provenance",
    "semantic_fidelity",
)
_ARM_DIRECT_ARTIFACTS = {
    "matrix-binding.json",
    "run-spec.json",
    "report.json",
    "batches",
}
_BATCH_ARTIFACT_NAMES = {
    "input.private.json",
    "prompt.private.md",
    "base-instructions.private.md",
    "schema.json",
    "semantic-call-attempt.json",
    "sidecar.json",
    "output.private.json",
    "canonical-labels.private.json",
    "evidence-provenance.private.json",
    "semantic-fidelity.json",
    "result.json",
}
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


class CanonicalV31DevelopmentRuntimeError(RuntimeError):
    """Live development authority, lineage, or accounting failed closed."""


class CanonicalV31CapacityUnavailable(CanonicalV31DevelopmentRuntimeError):
    """A no-client capacity measurement denied the next immutable arm."""


class CanonicalV31RecoveryRequired(CanonicalV31DevelopmentRuntimeError):
    """Partial arm evidence forbids replay in the current semantic epoch."""


class CanonicalV31RuntimeLockUnavailable(CanonicalV31DevelopmentRuntimeError):
    """Another writer already owns the exact development output root."""


_ACTIVE_INVOCATION_LOCK: contextvars.ContextVar[dict[str, Any] | None] = (
    contextvars.ContextVar("canonical_v31_runtime_invocation_lock", default=None)
)
_PROCESS_LOCK_GUARD = threading.Lock()
_PROCESS_LOCK_PATHS: set[Path] = set()


def _implementation_binding(value: Any, label: str) -> dict[str, Any]:
    target = value
    if inspect.ismethod(target):
        target = target.__func__
    elif not inspect.isfunction(target):
        target = getattr(type(value), "__call__", None)
    if target is None or not callable(target):
        raise CanonicalV31DevelopmentRuntimeError(
            f"{label} has no inspectable callable implementation"
        )
    target = inspect.unwrap(target)
    source_value = inspect.getsourcefile(target)
    try:
        source_segment = inspect.getsource(target)
    except (OSError, TypeError) as exc:
        raise CanonicalV31DevelopmentRuntimeError(
            f"{label} source implementation cannot be read"
        ) from exc
    if not isinstance(source_value, str):
        raise CanonicalV31DevelopmentRuntimeError(
            f"{label} is not a source-backed Python implementation"
        )
    source = _record(Path(source_value))
    payload = {
        "label": label,
        "module": getattr(target, "__module__", None),
        "qualname": getattr(target, "__qualname__", None),
        "source": source,
        "source_segment_sha256": _sha256_bytes(source_segment.encode("utf-8")),
    }
    return {
        "binding": payload,
        "binding_sha256": _sha256_bytes(_canonical_json(payload).encode("ascii")),
    }


def _validate_execution_mode_inputs(kwargs: Mapping[str, Any]) -> bool:
    offline = kwargs.get("offline_test_mode", False)
    if not isinstance(offline, bool):
        raise CanonicalV31DevelopmentRuntimeError(
            "offline_test_mode must be an explicit boolean"
        )
    runner = kwargs.get("arm_runner", adapter.run_episode_batch_arm)
    client = kwargs.get("client_factory", adapter._client_factory)
    probe = kwargs.get("capacity_probe")
    environ = kwargs.get("environ")
    clock = kwargs.get("now")
    if offline:
        if not callable(runner) or not callable(client) or not callable(probe):
            raise CanonicalV31DevelopmentRuntimeError(
                "offline fixture mode requires injectable runner, client, and probe"
            )
        return True
    if runner is not adapter.run_episode_batch_arm:
        raise CanonicalV31DevelopmentRuntimeError(
            "live mode rejects an injected semantic arm runner"
        )
    if client is not adapter._client_factory:
        raise CanonicalV31DevelopmentRuntimeError(
            "live mode rejects an injected managed-auth client factory"
        )
    if probe is not None:
        raise CanonicalV31DevelopmentRuntimeError(
            "live mode rejects an injected capacity probe"
        )
    if environ is not None or clock is not None:
        raise CanonicalV31DevelopmentRuntimeError(
            "live mode rejects injected environment or clock state"
        )
    return False


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _raw_absolute(path: Path) -> Path:
    expanded = path.expanduser()
    return expanded if expanded.is_absolute() else Path.cwd() / expanded


def _checked_lexical_path(path: Path, label: str) -> Path:
    """Return one lexical absolute path after lstat-rejecting symlink components."""

    raw = _raw_absolute(path)
    if ".." in raw.parts:
        raise CanonicalV31DevelopmentRuntimeError(
            f"{label} contains a parent traversal component"
        )
    current = Path(raw.anchor)
    for part in raw.parts[1:]:
        current = current / part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(mode):
            raise CanonicalV31DevelopmentRuntimeError(
                f"{label} contains a symlink component: {current}"
            )
    return Path(os.path.abspath(os.fspath(raw)))


def _is_real_file(path: Path) -> bool:
    try:
        return stat.S_ISREG(path.lstat().st_mode)
    except FileNotFoundError:
        return False


def _is_real_directory(path: Path) -> bool:
    try:
        return stat.S_ISDIR(path.lstat().st_mode)
    except FileNotFoundError:
        return False


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    source = _checked_lexical_path(path, "checksum source")
    if not _is_real_file(source):
        raise CanonicalV31DevelopmentRuntimeError(
            f"checksum source is not a real file: {source}"
        )
    with source.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _valid_sha256(value: Any) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _require_sha256(value: Any, label: str) -> str:
    if not _valid_sha256(value):
        raise CanonicalV31DevelopmentRuntimeError(
            f"{label} is not a lowercase SHA-256"
        )
    return str(value)


def _inside(path: Path, root: Path) -> bool:
    try:
        path.expanduser().resolve().relative_to(root.expanduser().resolve())
        return True
    except ValueError:
        return False


def _record(path: Path) -> dict[str, Any]:
    source = _checked_lexical_path(path, "artifact")
    if not _is_real_file(source):
        raise CanonicalV31DevelopmentRuntimeError(
            f"artifact is absent, non-file, or symlinked: {source}"
        )
    resolved = source.resolve(strict=True)
    return {
        "path": str(resolved),
        "sha256": _sha256_file(source),
        "size_bytes": source.lstat().st_size,
    }


def _verify_record(
    value: Any,
    *,
    label: str,
    allowed_root: Path,
) -> Path:
    if not isinstance(value, Mapping) or set(value) != {
        "path",
        "sha256",
        "size_bytes",
    }:
        raise CanonicalV31DevelopmentRuntimeError(f"{label} record is malformed")
    path_value = value.get("path")
    size = value.get("size_bytes")
    if (
        not isinstance(path_value, str)
        or not path_value
        or not _valid_sha256(value.get("sha256"))
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        raise CanonicalV31DevelopmentRuntimeError(f"{label} record fields are invalid")
    path = _checked_lexical_path(Path(path_value), label)
    actual = _record(path)
    if not _inside(Path(actual["path"]), allowed_root):
        raise CanonicalV31DevelopmentRuntimeError(f"{label} escapes its allowed root")
    if actual != dict(value):
        raise CanonicalV31DevelopmentRuntimeError(
            f"{label} checksum or size binding failed"
        )
    return Path(actual["path"])


def _load_object(path: Path, label: str) -> dict[str, Any]:
    source = _checked_lexical_path(path, label)
    if not _is_real_file(source):
        raise CanonicalV31DevelopmentRuntimeError(f"{label} is unavailable")
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalV31DevelopmentRuntimeError(
            f"{label} is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(value, dict):
        raise CanonicalV31DevelopmentRuntimeError(f"{label} is not a JSON object")
    return value


def _load_array(path: Path, label: str) -> list[Any]:
    source = _checked_lexical_path(path, label)
    if not _is_real_file(source):
        raise CanonicalV31DevelopmentRuntimeError(f"{label} is unavailable")
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalV31DevelopmentRuntimeError(
            f"{label} is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(value, list):
        raise CanonicalV31DevelopmentRuntimeError(f"{label} is not a JSON array")
    return value


def _write_or_verify_json(path: Path, value: Any) -> dict[str, Any]:
    target = _checked_lexical_path(path, "immutable runtime checkpoint")
    payload = _pretty_json(value)
    if target.exists():
        if (
            not _is_real_file(target)
            or target.read_text(encoding="utf-8") != payload
        ):
            raise CanonicalV31RecoveryRequired(
                f"immutable runtime checkpoint drifted: {target}"
            )
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        _checked_lexical_path(target.parent, "runtime checkpoint parent")
        if not _is_real_directory(target.parent):
            raise CanonicalV31RecoveryRequired(
                f"immutable runtime checkpoint parent is not a real directory: {target.parent}"
            )
        try:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(target, flags, 0o600)
        except FileExistsError:
            if (
                not _is_real_file(target)
                or target.read_text(encoding="utf-8") != payload
            ):
                raise CanonicalV31RecoveryRequired(
                    f"immutable runtime checkpoint raced or drifted: {target}"
                )
        else:
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
            except BaseException:
                # A crash or short write remains visible as partial immutable
                # evidence; recovery must fail closed instead of replacing it.
                raise
    return _record(target)

def _validated_output_root_path(output_root: Path, project_root: Path) -> Path:
    """Reject symlink evidence before canonicalizing the development root."""

    lexical_project = _checked_lexical_path(project_root, "project root")
    lexical_work = lexical_project / "work"
    _checked_lexical_path(lexical_work, "project work root")
    lexical_root = _checked_lexical_path(output_root, "development output root")
    try:
        relative = lexical_root.relative_to(lexical_work)
    except ValueError as exc:
        raise CanonicalV31DevelopmentRuntimeError(
            "development-only output root must stay inside project work"
        ) from exc
    if not relative.parts:
        raise CanonicalV31DevelopmentRuntimeError(
            "development-only output root must name a child of project work"
        )
    resolved_project = project_root.expanduser().resolve()
    resolved = lexical_root.resolve()
    if not _inside(resolved, resolved_project / "work"):
        raise CanonicalV31DevelopmentRuntimeError(
            "development-only output root escapes canonical project work"
        )
    return resolved


class _RuntimeInvocationLock:
    def __init__(self, *, path: Path, binding: Mapping[str, Any]) -> None:
        self.path = path
        self.binding = copy.deepcopy(dict(binding))
        self.binding_sha256 = _sha256_bytes(
            _canonical_json(self.binding).encode("ascii")
        )
        self.handle: Any = None

    def __enter__(self) -> dict[str, Any]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.parent.is_symlink() or not self.path.parent.is_dir():
            raise CanonicalV31RecoveryRequired(
                "persistent runtime lock directory is not a real directory"
            )
        with _PROCESS_LOCK_GUARD:
            if self.path in _PROCESS_LOCK_PATHS:
                raise CanonicalV31RuntimeLockUnavailable(
                    "canonical development runtime already has an active in-process writer"
                )
            _PROCESS_LOCK_PATHS.add(self.path)
        try:
            flags = os.O_RDWR | os.O_CREAT
            if hasattr(os, "O_CLOEXEC"):
                flags |= os.O_CLOEXEC
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(self.path, flags, 0o600)
            try:
                self.handle = os.fdopen(descriptor, "r+", encoding="utf-8")
            except BaseException:
                os.close(descriptor)
                raise
        except BaseException:
            with _PROCESS_LOCK_GUARD:
                _PROCESS_LOCK_PATHS.discard(self.path)
            raise
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException as exc:
            self.handle.close()
            self.handle = None
            with _PROCESS_LOCK_GUARD:
                _PROCESS_LOCK_PATHS.discard(self.path)
            if isinstance(exc, BlockingIOError):
                raise CanonicalV31RuntimeLockUnavailable(
                    "canonical development runtime already has an active writer"
                ) from exc
            raise
        self.handle.seek(0)
        existing = self.handle.read()
        payload = _pretty_json(self.binding)
        if existing:
            if existing != payload:
                root = Path(str(self.binding["development_output_root"]))
                if root.exists() and (not root.is_dir() or any(root.iterdir())):
                    try:
                        prior_binding = json.loads(existing)
                    except json.JSONDecodeError:
                        prior_binding = {}
                    drift = sorted(
                        key
                        for key in set(prior_binding) | set(self.binding)
                        if prior_binding.get(key) != self.binding.get(key)
                    )
                    self.__exit__(None, None, None)
                    raise CanonicalV31RecoveryRequired(
                        "persistent runtime lock binding differs at "
                        + ", ".join(drift)
                        + "; use a new epoch root"
                    )
                self.handle.seek(0)
                self.handle.truncate(0)
                self.handle.write(payload)
                self.handle.flush()
                os.fsync(self.handle.fileno())
        else:
            self.handle.seek(0)
            self.handle.truncate(0)
            self.handle.write(payload)
            self.handle.flush()
            os.fsync(self.handle.fileno())
        try:
            return {
                "binding": copy.deepcopy(self.binding),
                "binding_sha256": self.binding_sha256,
                "record": _record(self.path),
            }
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_args: Any) -> None:
        if self.handle is not None:
            try:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            finally:
                self.handle.close()
                self.handle = None
                with _PROCESS_LOCK_GUARD:
                    _PROCESS_LOCK_PATHS.discard(self.path)


def _preauthority_lock_binding(kwargs: Mapping[str, Any]) -> tuple[Path, dict[str, Any]]:
    output_root = kwargs.get("output_root")
    project_root = kwargs.get("project_root")
    semantic_plan_path = kwargs.get("semantic_plan_path")
    future_plan = kwargs.get("future_plan_binding")
    offline_test_mode = kwargs.get("offline_test_mode", False)
    if not isinstance(output_root, Path) or not isinstance(project_root, Path):
        raise CanonicalV31DevelopmentRuntimeError(
            "runtime lock requires Path output_root and project_root"
        )
    if not isinstance(semantic_plan_path, Path) or not isinstance(future_plan, Mapping):
        raise CanonicalV31DevelopmentRuntimeError(
            "runtime lock requires semantic plan and offline plan binding"
        )
    root = _validated_output_root_path(output_root, project_root)
    plan_path = _checked_lexical_path(semantic_plan_path, "runtime-lock semantic plan")
    plan_payload = _load_object(plan_path, "runtime-lock semantic plan")
    step = plan_payload.get("step")
    directive_path_value = step.get("directive_path") if isinstance(step, Mapping) else None
    if not isinstance(directive_path_value, str) or not directive_path_value:
        raise CanonicalV31DevelopmentRuntimeError(
            "runtime-lock semantic directive path is absent"
        )
    directive_path = _checked_lexical_path(
        Path(directive_path_value), "runtime-lock semantic directive"
    )
    if not _is_real_file(directive_path):
        raise CanonicalV31DevelopmentRuntimeError(
            "runtime-lock semantic directive is unavailable"
        )
    binding = {
        "schema_version": "pif_canonical_v31_runtime_invocation_lock_v1",
        "state": "exclusive_writer_for_exact_semantic_epoch",
        "development_output_root": str(root),
        "semantic_plan_path": str(plan_path),
        "semantic_plan_sha256": _sha256_file(plan_path),
        "semantic_directive_path": str(directive_path),
        "semantic_directive_sha256": _sha256_file(directive_path),
        "offline_future_plan_binding_sha256": _sha256_bytes(
            _canonical_json(future_plan).encode("ascii")
        ),
        "manifest_sha256": kwargs.get("manifest_sha256"),
        "capacity_policy_sha256": kwargs.get("capacity_policy_sha256"),
        "thread_id": kwargs.get("thread_id"),
        "runtime_module_sha256": _sha256_file(Path(__file__)),
        "execution_mode": OFFLINE_TEST_MODE if offline_test_mode else "live",
        "offline_test_mode": offline_test_mode,
        "promotable": False if offline_test_mode else True,
        "arm_runner_implementation": _implementation_binding(
            kwargs.get("arm_runner", adapter.run_episode_batch_arm), "arm_runner"
        ),
        "client_factory_implementation": _implementation_binding(
            kwargs.get("client_factory", adapter._client_factory), "client_factory"
        ),
        "capacity_probe_implementation": (
            _implementation_binding(kwargs.get("capacity_probe"), "capacity_probe")
            if kwargs.get("capacity_probe") is not None
            else None
        ),
        "injected_environment_names": sorted(
            str(name) for name in (kwargs.get("environ") or {})
        ),
        "injected_clock": kwargs.get("now") is not None,
        "extraction_only": True,
    }
    lock_name = _sha256_bytes(str(root).encode("utf-8")) + ".lock"
    lock_path = (
        project_root.expanduser().resolve()
        / "work"
        / ".canonical-v31-runtime-locks"
        / lock_name
    )
    return lock_path, binding


def _exclusive_runtime_invocation(function: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(function)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        if args:
            raise CanonicalV31DevelopmentRuntimeError(
                "canonical runtime accepts keyword arguments only"
            )
        _validate_execution_mode_inputs(kwargs)
        lock_path, binding = _preauthority_lock_binding(kwargs)
        with _RuntimeInvocationLock(path=lock_path, binding=binding) as lock_info:
            token = _ACTIVE_INVOCATION_LOCK.set(lock_info)
            try:
                return await function(**kwargs)
            finally:
                _ACTIVE_INVOCATION_LOCK.reset(token)

    return wrapper


def _usage(value: Any, label: str) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise CanonicalV31DevelopmentRuntimeError(f"{label} usage is absent")
    result: dict[str, int] = {}
    for field in USAGE_FIELDS:
        item = value.get(field)
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise CanonicalV31DevelopmentRuntimeError(
                f"{label}.{field} is invalid"
            )
        result[field] = item
    if (
        result["cached_input_tokens"] > result["input_tokens"]
        or result["reasoning_output_tokens"] > result["output_tokens"]
        or result["total_tokens"]
        != result["input_tokens"] + result["output_tokens"]
    ):
        raise CanonicalV31DevelopmentRuntimeError(f"{label} usage is inconsistent")
    return result


def _sum_usage(values: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    total = {field: 0 for field in USAGE_FIELDS}
    for index, value in enumerate(values):
        observed = _usage(value, f"usage row {index}")
        for field in USAGE_FIELDS:
            total[field] += observed[field]
    return total


def _positive_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CanonicalV31DevelopmentRuntimeError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise CanonicalV31DevelopmentRuntimeError(
            f"{label} must be finite and positive"
        )
    return result


def _nonnegative_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CanonicalV31DevelopmentRuntimeError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise CanonicalV31DevelopmentRuntimeError(
            f"{label} must be finite and nonnegative"
        )
    return result


def _parse_timestamp(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value or value != value.strip():
        raise CanonicalV31DevelopmentRuntimeError(
            f"{label} is not timezone-aware ISO-8601"
        )
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CanonicalV31DevelopmentRuntimeError(
            f"{label} is not timezone-aware ISO-8601"
        ) from exc
    if result.tzinfo is None:
        raise CanonicalV31DevelopmentRuntimeError(
            f"{label} is not timezone-aware ISO-8601"
        )
    return result.astimezone(timezone.utc)


def _now_value(value: datetime | Callable[[], datetime] | None) -> datetime:
    observed = value() if callable(value) else value
    result = observed or datetime.now(timezone.utc)
    if not isinstance(result, datetime) or result.tzinfo is None:
        raise CanonicalV31DevelopmentRuntimeError("runtime clock is not timezone-aware")
    return result.astimezone(timezone.utc)


async def _maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def _reject_external_auth_material(environ: Mapping[str, str] | None = None) -> None:
    """Reject API keys and raw/session-token authentication before any probe."""

    values = os.environ if environ is None else environ
    present = sorted(name for name in _FORBIDDEN_AUTH_ENVIRONMENT if values.get(name))
    if present:
        raise CanonicalV31DevelopmentRuntimeError(
            "managed ChatGPT app-server execution rejects API-key/raw/session-token auth: "
            + ", ".join(present)
        )


def parse_trusted_rate_limit_capacity(response: Mapping[str, Any]) -> dict[str, Any]:
    """Parse the pinned official rate-limit snapshot without claiming a reservation."""

    if not isinstance(response, Mapping):
        raise CanonicalV31CapacityUnavailable(
            "official account/rateLimits/read response is malformed"
        )
    fallback = response.get("rateLimits")
    if not isinstance(fallback, Mapping):
        raise CanonicalV31CapacityUnavailable(
            "official rate-limit fallback snapshot is absent"
        )
    by_id = response.get("rateLimitsByLimitId")
    if by_id is None:
        snapshot = copy.deepcopy(dict(fallback))
    elif isinstance(by_id, Mapping):
        direct = by_id.get("codex")
        if not isinstance(direct, Mapping):
            raise CanonicalV31CapacityUnavailable(
                "official Codex rate-limit snapshot is absent or ambiguous"
            )
        snapshot = copy.deepcopy(dict(direct))
    else:
        raise CanonicalV31CapacityUnavailable(
            "official multi-bucket rate-limit snapshot is malformed"
        )
    if snapshot.get("limitId") not in {None, "codex"}:
        raise CanonicalV31CapacityUnavailable(
            "official Codex rate-limit identity drifted"
        )

    windows: dict[str, dict[str, Any]] = {}
    for name in ("primary", "secondary"):
        value = snapshot.get(name)
        if value is None and name == "secondary":
            continue
        if not isinstance(value, Mapping):
            raise CanonicalV31CapacityUnavailable(
                f"official Codex {name} rate-limit window is absent"
            )
        used = value.get("usedPercent")
        resets = value.get("resetsAt")
        duration = value.get("windowDurationMins")
        if (
            isinstance(used, bool)
            or not isinstance(used, int)
            or not 0 <= used <= 100
            or (
                resets is not None
                and (isinstance(resets, bool) or not isinstance(resets, int))
            )
            or (
                duration is not None
                and (
                    isinstance(duration, bool)
                    or not isinstance(duration, int)
                    or duration <= 0
                )
            )
        ):
            raise CanonicalV31CapacityUnavailable(
                f"official Codex {name} rate-limit window is malformed"
            )
        windows[name] = {
            "used_percent": used,
            "remaining_percent": 100 - used,
            "resets_at": resets,
            "window_duration_minutes": duration,
        }

    applicable_remaining = [row["remaining_percent"] for row in windows.values()]
    individual = snapshot.get("individualLimit")
    individual_record = None
    if individual is not None:
        if not isinstance(individual, Mapping):
            raise CanonicalV31CapacityUnavailable(
                "official Codex individual limit is malformed"
            )
        remaining = individual.get("remainingPercent")
        resets = individual.get("resetsAt")
        limit = individual.get("limit")
        used_value = individual.get("used")
        if (
            isinstance(remaining, bool)
            or not isinstance(remaining, int)
            or not 0 <= remaining <= 100
            or isinstance(resets, bool)
            or not isinstance(resets, int)
            or not isinstance(limit, str)
            or not isinstance(used_value, str)
        ):
            raise CanonicalV31CapacityUnavailable(
                "official Codex individual remaining limit is malformed"
            )
        individual_record = {
            "remaining_percent": remaining,
            "resets_at": resets,
        }
        applicable_remaining.append(remaining)
    reached = snapshot.get("rateLimitReachedType")
    if reached is not None and (not isinstance(reached, str) or not reached):
        raise CanonicalV31CapacityUnavailable(
            "official Codex reached-limit type is malformed"
        )
    reset_credits = response.get("rateLimitResetCredits")
    available_reset_credits = None
    if reset_credits is not None:
        if not isinstance(reset_credits, Mapping):
            raise CanonicalV31CapacityUnavailable(
                "official reset-credit metadata is malformed"
            )
        available_reset_credits = reset_credits.get("availableCount")
        if (
            isinstance(available_reset_credits, bool)
            or not isinstance(available_reset_credits, int)
            or available_reset_credits < 0
        ):
            raise CanonicalV31CapacityUnavailable(
                "official reset-credit count is malformed"
            )
    return {
        "schema_version": "pif_canonical_v31_trusted_rate_limit_parse_v1",
        "provider_method": "account/rateLimits/read",
        "limit_id": snapshot.get("limitId") or "codex",
        "windows": windows,
        "individual_limit": individual_record,
        "minimum_applicable_remaining_percent": min(applicable_remaining),
        "rate_limit_reached_type": reached,
        "available_reset_credit_count": available_reset_credits,
        "provider_response_sha256": _sha256_bytes(
            _canonical_json(dict(response)).encode("ascii")
        ),
        "provider_reports_tokens_remaining": False,
        "provider_reports_wall_seconds_available": False,
        "capacity_is_reservation": False,
    }


def build_trusted_capacity_measurement(
    response: Mapping[str, Any],
    *,
    expected_context: Mapping[str, Any],
    limits: Mapping[str, Any],
    now: datetime,
    directive_expires_at: datetime,
    sequence: int,
    semantic_thread_started: bool = False,
    semantic_thread_resumed: bool = False,
) -> dict[str, Any]:
    """Derive a conservative estimate from one official no-thread/no-turn probe."""

    parsed = parse_trusted_rate_limit_capacity(response)
    current = _now_value(now)
    if directive_expires_at.tzinfo is None:
        raise CanonicalV31DevelopmentRuntimeError(
            "directive expiry is not timezone-aware"
        )
    expires_authority = directive_expires_at.astimezone(timezone.utc)
    reserve = int(limits["minimum_remaining_reserve_percent"])
    safety = int(limits["capacity_safety_margin_percent"])
    quota_rate = int(limits["quota_points_per_million_tokens"])
    remaining_percent = int(parsed["minimum_applicable_remaining_percent"])
    usable_points = max(0, remaining_percent - reserve - safety)
    available_tokens = math.floor(usable_points * 1_000_000 / quota_rate)
    wall_safety = float(limits["operator_wall_deadline_safety_margin_seconds"])
    available_wall = max(
        0.0, (expires_authority - current).total_seconds() - wall_safety
    )
    freshness = timedelta(
        seconds=int(limits["maximum_rate_limit_snapshot_age_seconds"])
    )
    expires = min(expires_authority, current + freshness)
    required_tokens = expected_context.get("required_remaining_total_token_ceiling")
    required_wall = expected_context.get("required_remaining_wall_seconds_ceiling")
    available = bool(
        parsed["rate_limit_reached_type"] is None
        and isinstance(required_tokens, int)
        and not isinstance(required_tokens, bool)
        and available_tokens >= required_tokens
        and isinstance(required_wall, (int, float))
        and not isinstance(required_wall, bool)
        and available_wall >= float(required_wall)
        and expires > current
    )
    identity = {
        "sequence": sequence,
        "measured_at": current.isoformat(),
        "provider_response_sha256": parsed["provider_response_sha256"],
        "context": dict(expected_context),
    }
    return {
        "schema_version": CAPACITY_MEASUREMENT_VERSION,
        "measurement_id": _sha256_bytes(
            _canonical_json(identity).encode("ascii")
        ),
        "sequence": sequence,
        "measured_at": current.isoformat(),
        "expires_at": expires.isoformat(),
        "state": (
            "available_before_semantic_thread_or_turn"
            if available
            else "unavailable_before_semantic_thread_or_turn"
        ),
        **copy.deepcopy(dict(expected_context)),
        "available_total_tokens": available_tokens,
        "available_wall_seconds": available_wall,
        "capacity_available": available,
        "capacity_unknown": False,
        "official_app_server_initialized_before_capacity": True,
        "semantic_thread_started": semantic_thread_started,
        "semantic_thread_resumed": semantic_thread_resumed,
        "semantic_turn_started": False,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "extraction_only": True,
        "quality_evaluation_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "offline_test_mode": False,
        "live_capacity_authority": True,
        "capacity_estimate_not_reservation": True,
        "single_turn_token_cap_prospective_only": True,
        "postturn_measured_stop_required": True,
        "provider_capacity": parsed,
        "quota_calibration_id": limits["quota_calibration_id"],
        "quota_points_per_million_tokens": quota_rate,
        "minimum_remaining_reserve_percent": reserve,
        "capacity_safety_margin_percent": safety,
        "usable_quota_points": usable_points,
    }


class _BorrowedClientContext:
    """Expose one already-entered official client without closing it per arm."""

    def __init__(self, client: Any) -> None:
        self.client = client

    async def __aenter__(self) -> Any:
        return self.client

    async def __aexit__(self, *_args: Any) -> None:
        return None


class _LiveCapacityGuardedClient:
    """Re-probe and enforce measured stop gates around one borrowed official client."""

    def __init__(
        self,
        *,
        inner: Any,
        limits: Mapping[str, Any],
        directive_expires_at: datetime,
        context_factory: Callable[[], Mapping[str, Any]],
        budget_state: dict[str, Any],
        now_factory: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.inner = inner
        self.limits = copy.deepcopy(dict(limits))
        self.directive_expires_at = directive_expires_at
        self.context_factory = context_factory
        self.budget_state = budget_state
        self.now_factory = now_factory
        self.monotonic = monotonic
        self.sequence = 0
        self.evidence_root: Path | None = None
        self.initial_admission_ready = False
        self.initial_admission_consumed = False
        self.semantic_thread_started = False
        self.semantic_thread_resumed = False
        self.reprobe_records: list[dict[str, Any]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def _check_local_stop_gates(self) -> None:
        remaining_wall = float(self.budget_state["deadline_monotonic"]) - float(
            self.monotonic()
        )
        if remaining_wall <= float(
            self.limits["operator_wall_deadline_safety_margin_seconds"]
        ):
            raise CanonicalV31CapacityUnavailable(
                "monotonic semantic wall deadline has no safe remaining headroom"
            )
        used_calls = int(self.budget_state["used_calls"])
        used_tokens = int(self.budget_state["used_total_tokens"])
        if (
            used_calls >= int(self.limits["exact_model_call_cap"])
            or used_tokens + int(self.limits["maximum_total_tokens_per_turn"])
            > int(self.limits["exact_total_token_cap"])
        ):
            raise CanonicalV31CapacityUnavailable(
                "measured global call or prospective token stop gate is closed"
            )

    async def _probe(self, boundary: str) -> dict[str, Any]:
        self._check_local_stop_gates()
        context = copy.deepcopy(dict(self.context_factory()))
        response = await self.inner._request(  # noqa: SLF001 - exact official no-turn RPC
            "account/rateLimits/read", {}
        )
        measured_at = _now_value(self.now_factory)
        measurement = build_trusted_capacity_measurement(
            response,
            expected_context=context,
            limits=self.limits,
            now=measured_at,
            directive_expires_at=self.directive_expires_at,
            sequence=self.sequence,
            semantic_thread_started=self.semantic_thread_started,
            semantic_thread_resumed=self.semantic_thread_resumed,
        )
        measurement["boundary"] = boundary
        request_payload = {
            "schema_version": "pif_canonical_v31_capacity_probe_request_v1",
            "sequence": self.sequence,
            "boundary": boundary,
            "provider_method": "account/rateLimits/read",
            "context": context,
            "official_app_server_initialized_before_capacity": True,
            "semantic_thread_started": self.semantic_thread_started,
            "semantic_thread_resumed": self.semantic_thread_resumed,
            "semantic_turn_started": False,
            "offline_test_mode": False,
            "live_capacity_authority": True,
            "capacity_estimate_not_reservation": True,
            "promotable": True,
        }
        result = {
            "request": request_payload,
            "response": copy.deepcopy(dict(response)),
            "measurement": measurement,
        }
        if self.evidence_root is not None:
            root = self.evidence_root / "reprobes" / f"{self.sequence:04d}-{boundary}"
            records = {
                "request": _write_or_verify_json(root / "request.json", request_payload),
                "measurement": _write_or_verify_json(
                    root / "measurement.json", measurement
                ),
            }
            result["records"] = records
            self.reprobe_records.append(
                {
                    "request": copy.deepcopy(request_payload),
                    "measurement": copy.deepcopy(measurement),
                    "records": copy.deepcopy(records),
                }
            )
        self.sequence += 1
        if measurement["capacity_available"] is not True:
            raise CanonicalV31CapacityUnavailable(
                f"trusted estimated capacity is unavailable before {boundary}"
            )
        return result

    async def initial_probe(self) -> dict[str, Any]:
        if self.initial_admission_ready:
            raise CanonicalV31DevelopmentRuntimeError(
                "initial trusted capacity probe cannot be replayed"
            )
        result = await self._probe("initial_arm_admission")
        self.initial_admission_ready = True
        return result

    def activate_admission(self, *, evidence_root: Path) -> None:
        if not self.initial_admission_ready or self.initial_admission_consumed:
            raise CanonicalV31DevelopmentRuntimeError(
                "trusted initial admission is absent or already consumed"
            )
        self.evidence_root = evidence_root

    async def start_thread(self, *args: Any, **kwargs: Any) -> Any:
        self._check_local_stop_gates()
        if not self.initial_admission_consumed:
            if not self.initial_admission_ready:
                raise CanonicalV31CapacityUnavailable(
                    "semantic thread start has no trusted initial admission"
                )
            self.initial_admission_consumed = True
        else:
            await self._probe("thread_start")
        thread = await self.inner.start_thread(*args, **kwargs)
        self.semantic_thread_started = True
        return thread

    async def resume_thread(self, *args: Any, **kwargs: Any) -> Any:
        await self._probe("thread_resume")
        thread = await self.inner.resume_thread(*args, **kwargs)
        self.semantic_thread_started = True
        self.semantic_thread_resumed = True
        return thread

    async def run_structured_turn(self, *args: Any, **kwargs: Any) -> Any:
        await self._probe("turn_start")
        remaining_wall = float(self.budget_state["deadline_monotonic"]) - float(
            self.monotonic()
        )
        requested_timeout = kwargs.get("timeout_seconds")
        configured_timeout = (
            float(requested_timeout)
            if requested_timeout is not None
            else float(self.limits["maximum_wall_seconds_per_turn"])
        )
        safe_remaining_wall = remaining_wall - float(
            self.limits["operator_wall_deadline_safety_margin_seconds"]
        )
        kwargs["timeout_seconds"] = min(
            configured_timeout,
            float(self.limits["maximum_wall_seconds_per_turn"]),
            max(0.001, safe_remaining_wall),
        )
        result = await self.inner.run_structured_turn(*args, **kwargs)
        sidecar_path = kwargs.get("sidecar_path")
        sidecar = _load_object(Path(sidecar_path), "postturn measured sidecar")
        usage = _usage(sidecar.get("usage"), "postturn measured sidecar")
        wall = _nonnegative_number(
            sidecar.get("wall_elapsed_seconds"), "postturn measured wall"
        )
        self.budget_state["used_calls"] = int(self.budget_state["used_calls"]) + 1
        self.budget_state["used_total_tokens"] = int(
            self.budget_state["used_total_tokens"]
        ) + usage["total_tokens"]
        if (
            usage["total_tokens"]
            > int(self.limits["maximum_total_tokens_per_turn"])
            or wall > float(self.limits["maximum_wall_seconds_per_turn"])
            or int(self.budget_state["used_calls"])
            > int(self.limits["exact_model_call_cap"])
            or int(self.budget_state["used_total_tokens"])
            > int(self.limits["exact_total_token_cap"])
            or float(self.budget_state["deadline_monotonic"])
            - float(self.monotonic())
            <= 0
        ):
            raise CanonicalV31DevelopmentRuntimeError(
                "postturn measured call, token, or wall stop gate was exceeded"
            )
        return result


def build_runtime_dependency_binding() -> dict[str, Any]:
    """Checksum-bind this executor and its three exact executable dependencies."""

    adapter_runtime = offline_matrix._verify_runtime_binding()  # noqa: SLF001
    canonical_runner = _implementation_binding(
        adapter.run_episode_batch_arm, "canonical_adapter.run_episode_batch_arm"
    )
    official_client = _implementation_binding(
        adapter._client_factory, "canonical_adapter._client_factory"
    )
    trusted_parser = _implementation_binding(
        parse_trusted_rate_limit_capacity, "trusted_capacity.rate_limit_parser"
    )
    trusted_measurement = _implementation_binding(
        build_trusted_capacity_measurement, "trusted_capacity.measurement_builder"
    )
    trusted_turn_guard = _implementation_binding(
        _LiveCapacityGuardedClient.run_structured_turn,
        "trusted_capacity.preturn_and_postturn_guard",
    )
    borrowed_client = _implementation_binding(
        _BorrowedClientContext.__aenter__, "trusted_capacity.borrowed_client_context"
    )
    dependencies = {
        "canonical_adapter": _record(Path(str(adapter.__file__))),
        "offline_matrix": _record(Path(str(offline_matrix.__file__))),
        "supervisor_semantic_plan": _record(Path(str(supervisor.__file__))),
    }
    binding = {
        "schema_version": RUNTIME_BINDING_VERSION,
        "state": "checksum_bound_extraction_only_runtime",
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "runtime_module": _record(Path(__file__)),
        "exact_dependency_roles": list(dependencies),
        "exact_dependencies": dependencies,
        "adapter_runtime_binding_sha256": adapter_runtime["binding_sha256"],
        "context_control_overlay_sha256": adapter_runtime["binding"][
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": adapter_runtime["binding"][
            "instruction_source_contract_sha256"
        ],
        "semantic_execution_surface": "canonical_adapter.run_episode_batch_arm",
        "canonical_arm_runner_implementation": canonical_runner["binding"],
        "canonical_arm_runner_implementation_sha256": canonical_runner[
            "binding_sha256"
        ],
        "official_managed_auth_client_implementation": official_client["binding"],
        "official_managed_auth_client_implementation_sha256": official_client[
            "binding_sha256"
        ],
        "trusted_capacity_implementation_state": (
            TRUSTED_CAPACITY_IMPLEMENTATION_STATE
        ),
        "trusted_rate_limit_parser_implementation": trusted_parser["binding"],
        "trusted_rate_limit_parser_implementation_sha256": trusted_parser[
            "binding_sha256"
        ],
        "trusted_capacity_measurement_implementation": trusted_measurement["binding"],
        "trusted_capacity_measurement_implementation_sha256": trusted_measurement[
            "binding_sha256"
        ],
        "trusted_preturn_postturn_guard_implementation": trusted_turn_guard[
            "binding"
        ],
        "trusted_preturn_postturn_guard_implementation_sha256": trusted_turn_guard[
            "binding_sha256"
        ],
        "borrowed_official_client_context_implementation": borrowed_client["binding"],
        "borrowed_official_client_context_implementation_sha256": borrowed_client[
            "binding_sha256"
        ],
        "live_dispatch_available": True,
        "capacity_is_conservative_estimate_not_reservation": True,
        "single_turn_token_cap_is_prospective_only": True,
        "preturn_reprobe_and_postturn_measured_stop": True,
        "offline_test_mode": OFFLINE_TEST_MODE,
        "offline_test_receipts_promotable": False,
        "extraction_only": True,
        "quality_evaluation_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    return {
        "binding": binding,
        "binding_sha256": _sha256_bytes(_canonical_json(binding).encode("ascii")),
    }


def semantic_plan_contract(plan_payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the acyclic semantic-plan projection bound by its directive.

    The full supervisor plan byte-hash includes ``step.directive_sha256``.  A
    directive cannot also contain that full hash without a checksum cycle.  The
    directive therefore binds this exact projection (all plan authority except
    the directive hash), while the full plan bytes bind the directive hash.
    """

    if not isinstance(plan_payload, Mapping) or not isinstance(
        plan_payload.get("step"), Mapping
    ):
        raise CanonicalV31DevelopmentRuntimeError("semantic plan payload is malformed")
    projection = copy.deepcopy(dict(plan_payload))
    step = copy.deepcopy(dict(projection["step"]))
    if "directive_sha256" not in step:
        raise CanonicalV31DevelopmentRuntimeError(
            "semantic plan directive checksum is absent"
        )
    step.pop("directive_sha256")
    projection["step"] = step
    return {
        "contract": projection,
        "contract_sha256": _sha256_bytes(_canonical_json(projection).encode("ascii")),
    }


def _capacity_limits(preflight: Mapping[str, Any]) -> dict[str, Any]:
    capacity = preflight.get("capacity_info") if isinstance(preflight, Mapping) else None
    policy = capacity.get("payload") if isinstance(capacity, Mapping) else None
    if not isinstance(policy, Mapping):
        raise CanonicalV31DevelopmentRuntimeError("verified capacity policy is absent")
    per_turn_tokens = policy.get("maximum_total_tokens_per_turn")
    if (
        isinstance(per_turn_tokens, bool)
        or not isinstance(per_turn_tokens, int)
        or per_turn_tokens <= 0
    ):
        raise CanonicalV31DevelopmentRuntimeError(
            "capacity policy maximum_total_tokens_per_turn is not positive numeric authority"
        )
    per_turn_wall = _positive_number(
        policy.get("maximum_wall_seconds_per_turn"),
        "capacity policy maximum_wall_seconds_per_turn",
    )
    integer_fields = (
        "minimum_remaining_reserve_percent",
        "capacity_safety_margin_percent",
        "quota_points_per_million_tokens",
        "maximum_rate_limit_snapshot_age_seconds",
    )
    integers: dict[str, int] = {}
    for field in integer_fields:
        observed = policy.get(field)
        if isinstance(observed, bool) or not isinstance(observed, int) or observed <= 0:
            raise CanonicalV31DevelopmentRuntimeError(
                f"capacity policy {field} is not positive numeric authority"
            )
        integers[field] = observed
    wall_safety = _positive_number(
        policy.get("operator_wall_deadline_safety_margin_seconds"),
        "capacity policy operator wall safety margin",
    )
    if (
        integers["minimum_remaining_reserve_percent"]
        + integers["capacity_safety_margin_percent"]
        >= 100
        or policy.get("token_capacity_is_estimate_not_reservation") is not True
        or policy.get("single_turn_token_cap_is_prospective_only") is not True
        or policy.get("preturn_reprobe_required") is not True
        or policy.get("postturn_measured_stop_required") is not True
    ):
        raise CanonicalV31DevelopmentRuntimeError(
            "capacity policy estimate, reserve, or measured-stop contract drifted"
        )
    exact_calls = preflight.get("receipt", {}).get("exact_request_count")
    if isinstance(exact_calls, bool) or not isinstance(exact_calls, int) or exact_calls < 1:
        raise CanonicalV31DevelopmentRuntimeError(
            "dry preflight exact request count is invalid"
        )
    return {
        "exact_model_call_cap": exact_calls,
        "maximum_total_tokens_per_turn": per_turn_tokens,
        "maximum_wall_seconds_per_turn": per_turn_wall,
        "exact_total_token_cap": exact_calls * per_turn_tokens,
        "exact_total_wall_ceiling": exact_calls * per_turn_wall,
        **integers,
        "operator_wall_deadline_safety_margin_seconds": wall_safety,
        "quota_calibration_id": policy.get("quota_calibration_id"),
    }


def _read_canonical_directive(
    plan: supervisor.SemanticPlan,
) -> tuple[dict[str, Any], bytes, str]:
    supervisor.validate_semantic_directive(plan)
    try:
        raw = plan.directive_path.read_bytes()
        payload = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalV31DevelopmentRuntimeError(
            "supervisor semantic directive is malformed"
        ) from exc
    if not isinstance(payload, dict):
        raise CanonicalV31DevelopmentRuntimeError(
            "supervisor semantic directive is not an object"
        )
    canonical = _canonical_json(payload).encode("ascii")
    if raw != canonical:
        raise CanonicalV31DevelopmentRuntimeError(
            "supervisor semantic directive is not exact canonical JSON bytes"
        )
    checksum = _sha256_bytes(raw)
    if checksum != plan.directive_sha256:
        raise CanonicalV31DevelopmentRuntimeError(
            "supervisor semantic directive byte checksum drifted"
        )
    return payload, raw, checksum


def _validate_semantic_epoch_authority(
    *,
    semantic_plan_path: Path,
    semantic_plan: supervisor.SemanticPlan,
    semantic_plan_payload: Mapping[str, Any],
    directive: Mapping[str, Any],
    directive_sha256: str,
    preflight: Mapping[str, Any],
    precommit: Mapping[str, Any],
    future_plan_binding: Mapping[str, Any],
    runtime_binding: Mapping[str, Any],
    output_root: Path,
    limits: Mapping[str, Any],
    now: datetime,
    offline_test_mode: bool,
) -> dict[str, Any]:
    """Re-prove the one fresh extraction-only directive before any probe."""

    try:
        directive_info = offline_matrix.validate_future_execution_directive(
            directive,
            expected_sha256=directive_sha256,
            plan_binding=future_plan_binding,
            now=now,
        )
    except offline_matrix.CanonicalV31DevelopmentMatrixError as exc:
        raise CanonicalV31DevelopmentRuntimeError(
            "offline future execution directive failed exact validation"
        ) from exc
    plan_contract = semantic_plan_contract(semantic_plan_payload)
    runtime = runtime_binding.get("binding")
    if not isinstance(runtime, Mapping):
        raise CanonicalV31DevelopmentRuntimeError("runtime binding payload is absent")
    receipt_path = (output_root / EXTRACTION_RECEIPT_FILENAME).resolve()
    expected = {
        "authorized_by": "kolby",
        "thread_id": semantic_plan_payload.get("thread_id"),
        "plan_epoch": semantic_plan.plan_epoch,
        "step_id": semantic_plan.step_id,
        "expected_receipt_path": str(receipt_path),
        "semantic_plan_path": str(semantic_plan_path.expanduser().resolve()),
        "semantic_plan_contract_sha256": plan_contract["contract_sha256"],
        "max_model_calls": limits["exact_model_call_cap"],
        "max_total_tokens": limits["exact_total_token_cap"],
        "maximum_total_tokens_per_turn": limits[
            "maximum_total_tokens_per_turn"
        ],
        "maximum_wall_seconds_per_turn": limits[
            "maximum_wall_seconds_per_turn"
        ],
        "minimum_remaining_reserve_percent": limits[
            "minimum_remaining_reserve_percent"
        ],
        "capacity_safety_margin_percent": limits[
            "capacity_safety_margin_percent"
        ],
        "quota_points_per_million_tokens": limits[
            "quota_points_per_million_tokens"
        ],
        "quota_calibration_id": limits["quota_calibration_id"],
        "maximum_rate_limit_snapshot_age_seconds": limits[
            "maximum_rate_limit_snapshot_age_seconds"
        ],
        "operator_wall_deadline_safety_margin_seconds": limits[
            "operator_wall_deadline_safety_margin_seconds"
        ],
        "token_capacity_is_estimate_not_reservation": True,
        "single_turn_token_cap_is_prospective_only": True,
        "preturn_reprobe_required": True,
        "postturn_measured_stop_required": True,
        "exact_request_count": limits["exact_model_call_cap"],
        "development_output_root": str(output_root),
        "live_runtime_binding_sha256": runtime_binding.get("binding_sha256"),
        "runtime_module_sha256": runtime.get("runtime_module", {}).get("sha256"),
        "offline_matrix_module_sha256": runtime.get("exact_dependencies", {})
        .get("offline_matrix", {})
        .get("sha256"),
        "canonical_adapter_module_sha256": runtime.get("exact_dependencies", {})
        .get("canonical_adapter", {})
        .get("sha256"),
        "supervisor_module_sha256": runtime.get("exact_dependencies", {})
        .get("supervisor_semantic_plan", {})
        .get("sha256"),
        "extraction_only": True,
        "quality_evaluation_authorized": False,
        "quality_selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "api_key_auth_allowed": False,
        "raw_session_token_auth_allowed": False,
        "offline_test_mode_authorized": offline_test_mode,
        "live_pass_receipt_authorized": not offline_test_mode,
        "canonical_arm_runner_implementation_sha256": runtime.get(
            "canonical_arm_runner_implementation_sha256"
        ),
        "official_managed_auth_client_implementation_sha256": runtime.get(
            "official_managed_auth_client_implementation_sha256"
        ),
        "trusted_capacity_implementation_state": runtime.get(
            "trusted_capacity_implementation_state"
        ),
        "trusted_rate_limit_parser_implementation_sha256": runtime.get(
            "trusted_rate_limit_parser_implementation_sha256"
        ),
        "trusted_capacity_measurement_implementation_sha256": runtime.get(
            "trusted_capacity_measurement_implementation_sha256"
        ),
        "trusted_preturn_postturn_guard_implementation_sha256": runtime.get(
            "trusted_preturn_postturn_guard_implementation_sha256"
        ),
        "borrowed_official_client_context_implementation_sha256": runtime.get(
            "borrowed_official_client_context_implementation_sha256"
        ),
        "capacity_measurement_required_before_each_arm": True,
        "per_arm_capacity_admission_required": True,
        "new_empty_development_root_required": True,
    }
    drift = [key for key, value in expected.items() if directive.get(key) != value]
    if drift:
        raise CanonicalV31DevelopmentRuntimeError(
            "semantic epoch authority lineage drifted: " + ", ".join(sorted(drift))
        )
    if (
        semantic_plan.state != "executable"
        or semantic_plan.step_state != "executable"
        or semantic_plan.max_model_calls != limits["exact_model_call_cap"]
        or semantic_plan.max_total_tokens != limits["exact_total_token_cap"]
        or semantic_plan.expected_receipt_path != receipt_path
        or "passed" not in semantic_plan.accepted_receipt_states
        or directive.get("precommit_sha256") != precommit.get("precommit_sha256")
        or directive.get("manifest_sha256")
        != preflight.get("receipt", {}).get("manifest_sha256")
        or directive.get("context_set_sha256")
        != preflight.get("receipt", {}).get("context_set_sha256")
        or directive.get("runtime_binding_sha256")
        != preflight.get("receipt", {}).get("runtime_binding_sha256")
        or directive.get("context_control_overlay_sha256")
        != runtime.get("context_control_overlay_sha256")
        or directive.get("instruction_source_contract_sha256")
        != runtime.get("instruction_source_contract_sha256")
        or directive.get("capacity_policy_sha256")
        != preflight.get("receipt", {}).get("capacity_policy_sha256")
        or directive.get("managed_chatgpt_auth_only") is not True
        or directive.get("managed_chatgpt_plan_type") != "pro"
        or directive.get("official_persistent_codex_app_server_only") is not True
        or directive.get("live_semantic_dispatch_authorized") is not True
        or directive.get("semantic_retry_count") != 0
    ):
        raise CanonicalV31DevelopmentRuntimeError(
            "semantic plan, directive, precommit, or runtime authority drifted"
        )
    return {
        "directive_info": directive_info,
        "semantic_plan_contract": plan_contract,
        "semantic_plan_sha256": semantic_plan.sha256,
        "directive_sha256": directive_sha256,
        "thread_id": semantic_plan_payload.get("thread_id"),
    }


def _revalidate_epoch_sources(
    *,
    semantic_plan_path: Path,
    semantic_plan: supervisor.SemanticPlan,
    semantic_authority: Mapping[str, Any],
) -> None:
    if _sha256_file(semantic_plan_path) != semantic_authority.get(
        "semantic_plan_sha256"
    ):
        raise CanonicalV31RecoveryRequired(
            "supervisor semantic plan changed during the locked invocation"
        )
    try:
        directive, raw, checksum = _read_canonical_directive(semantic_plan)
    except Exception as exc:
        raise CanonicalV31RecoveryRequired(
            "canonical semantic directive changed during the locked invocation"
        ) from exc
    if (
        checksum != semantic_authority.get("directive_sha256")
        or _sha256_bytes(raw) != checksum
        or directive
        != semantic_authority.get("directive_info", {}).get("directive")
    ):
        raise CanonicalV31RecoveryRequired(
            "canonical semantic directive changed during the locked invocation"
        )


def _validate_capacity_probe_request(
    value: Mapping[str, Any],
    *,
    expected_context: Mapping[str, Any],
    expected_sequence: int,
    expected_boundary: str,
    expected_semantic_thread_started: bool,
    expected_semantic_thread_resumed: bool,
) -> dict[str, Any]:
    """Validate the immutable request-side record for one capacity snapshot."""

    if not isinstance(value, Mapping):
        raise CanonicalV31DevelopmentRuntimeError(
            "capacity probe request record is absent"
        )
    payload = copy.deepcopy(dict(value))
    offline_mode = expected_context.get("offline_test_mode") is True
    expected = {
        "schema_version": "pif_canonical_v31_capacity_probe_request_v1",
        "sequence": expected_sequence,
        "boundary": expected_boundary,
        "provider_method": (
            "offline_fixture_probe"
            if offline_mode
            else "account/rateLimits/read"
        ),
        "context": copy.deepcopy(dict(expected_context)),
        "official_app_server_initialized_before_capacity": True,
        "semantic_thread_started": expected_semantic_thread_started,
        "semantic_thread_resumed": expected_semantic_thread_resumed,
        "semantic_turn_started": False,
        "offline_test_mode": offline_mode,
        "live_capacity_authority": not offline_mode,
        "capacity_estimate_not_reservation": not offline_mode,
        "promotable": not offline_mode,
    }
    if payload != expected:
        raise CanonicalV31DevelopmentRuntimeError(
            "capacity probe request boundary, lifecycle, or context drifted"
        )
    return payload


def _validate_live_provider_projection(
    payload: Mapping[str, Any], *, limits: Mapping[str, Any]
) -> None:
    """Validate the sanitized official snapshot and conservative calibration."""

    provider = payload.get("provider_capacity")
    if not isinstance(provider, Mapping):
        raise CanonicalV31DevelopmentRuntimeError(
            "live capacity provider projection is absent"
        )
    windows = provider.get("windows")
    if not isinstance(windows, Mapping) or "primary" not in windows:
        raise CanonicalV31DevelopmentRuntimeError(
            "live capacity provider windows are absent"
        )
    remaining_values: list[int] = []
    for name, row in windows.items():
        if name not in {"primary", "secondary"} or not isinstance(row, Mapping):
            raise CanonicalV31DevelopmentRuntimeError(
                "live capacity provider window projection drifted"
            )
        used = row.get("used_percent")
        remaining = row.get("remaining_percent")
        resets = row.get("resets_at")
        duration = row.get("window_duration_minutes")
        if (
            isinstance(used, bool)
            or not isinstance(used, int)
            or not 0 <= used <= 100
            or isinstance(remaining, bool)
            or not isinstance(remaining, int)
            or remaining != 100 - used
            or (
                resets is not None
                and (isinstance(resets, bool) or not isinstance(resets, int))
            )
            or (
                duration is not None
                and (
                    isinstance(duration, bool)
                    or not isinstance(duration, int)
                    or duration <= 0
                )
            )
        ):
            raise CanonicalV31DevelopmentRuntimeError(
                "live capacity provider window values drifted"
            )
        remaining_values.append(remaining)
    individual = provider.get("individual_limit")
    if individual is not None:
        if not isinstance(individual, Mapping):
            raise CanonicalV31DevelopmentRuntimeError(
                "live capacity individual-limit projection drifted"
            )
        individual_remaining = individual.get("remaining_percent")
        individual_resets = individual.get("resets_at")
        if (
            isinstance(individual_remaining, bool)
            or not isinstance(individual_remaining, int)
            or not 0 <= individual_remaining <= 100
            or isinstance(individual_resets, bool)
            or not isinstance(individual_resets, int)
        ):
            raise CanonicalV31DevelopmentRuntimeError(
                "live capacity individual-limit values drifted"
            )
        remaining_values.append(individual_remaining)
    provider_sha = provider.get("provider_response_sha256")
    minimum_remaining = provider.get("minimum_applicable_remaining_percent")
    reset_count = provider.get("available_reset_credit_count")
    if (
        provider.get("schema_version")
        != "pif_canonical_v31_trusted_rate_limit_parse_v1"
        or provider.get("provider_method") != "account/rateLimits/read"
        or provider.get("limit_id") != "codex"
        or not _valid_sha256(provider_sha)
        or minimum_remaining != min(remaining_values)
        or provider.get("rate_limit_reached_type") is not None
        or (
            reset_count is not None
            and (
                isinstance(reset_count, bool)
                or not isinstance(reset_count, int)
                or reset_count < 0
            )
        )
        or provider.get("provider_reports_tokens_remaining") is not False
        or provider.get("provider_reports_wall_seconds_available") is not False
        or provider.get("capacity_is_reservation") is not False
    ):
        raise CanonicalV31DevelopmentRuntimeError(
            "live capacity provider authority projection drifted"
        )
    reserve = int(limits["minimum_remaining_reserve_percent"])
    safety = int(limits["capacity_safety_margin_percent"])
    quota_rate = int(limits["quota_points_per_million_tokens"])
    usable_points = max(0, int(minimum_remaining) - reserve - safety)
    expected_tokens = math.floor(usable_points * 1_000_000 / quota_rate)
    if (
        payload.get("quota_calibration_id") != limits["quota_calibration_id"]
        or payload.get("quota_points_per_million_tokens") != quota_rate
        or payload.get("minimum_remaining_reserve_percent") != reserve
        or payload.get("capacity_safety_margin_percent") != safety
        or payload.get("usable_quota_points") != usable_points
        or payload.get("available_total_tokens") != expected_tokens
    ):
        raise CanonicalV31DevelopmentRuntimeError(
            "live capacity conservative calibration drifted"
        )


def validate_capacity_measurement(
    value: Mapping[str, Any],
    *,
    expected_context: Mapping[str, Any],
    expected_sha256: str,
    now: datetime,
    historical: bool = False,
    expected_sequence: int = 0,
    expected_boundary: str = "initial_arm_admission",
    expected_semantic_thread_started: bool = False,
    expected_semantic_thread_resumed: bool = False,
    limits: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate one numeric no-client reserve measurement for the next arm."""

    if not isinstance(value, Mapping):
        raise CanonicalV31DevelopmentRuntimeError("capacity measurement is absent")
    payload = copy.deepcopy(dict(value))
    checksum = _sha256_bytes(_canonical_json(payload).encode("ascii"))
    if checksum != _require_sha256(expected_sha256, "capacity measurement SHA-256"):
        raise CanonicalV31DevelopmentRuntimeError(
            "capacity measurement checksum binding failed"
        )
    measured = _parse_timestamp(payload.get("measured_at"), "capacity measured_at")
    expires = _parse_timestamp(payload.get("expires_at"), "capacity expires_at")
    if expires <= measured or (not historical and (measured > now or now >= expires)):
        raise CanonicalV31DevelopmentRuntimeError(
            "capacity measurement is not fresh"
        )
    measurement_id = payload.get("measurement_id")
    available_tokens = payload.get("available_total_tokens")
    if (
        not isinstance(measurement_id, str)
        or not measurement_id
        or measurement_id != measurement_id.strip()
        or isinstance(available_tokens, bool)
        or not isinstance(available_tokens, int)
        or available_tokens < 0
    ):
        raise CanonicalV31DevelopmentRuntimeError(
            "capacity measurement identity or token value is not numeric"
        )
    available_wall = _nonnegative_number(
        payload.get("available_wall_seconds"), "capacity available wall"
    )
    exact_fields = (
        "candidate_system_id",
        "variant_id",
        "batch_size",
        "thread_mode",
        "request_count",
        "request_set_sha256",
        "future_plan_binding_sha256",
        "future_directive_sha256",
        "precommit_sha256",
        "manifest_sha256",
        "context_set_sha256",
        "adapter_runtime_binding_sha256",
        "live_runtime_binding_sha256",
        "context_control_overlay_sha256",
        "instruction_source_contract_sha256",
        "capacity_policy_sha256",
        "remaining_model_calls",
        "global_remaining_total_token_budget",
        "required_remaining_total_token_ceiling",
        "required_remaining_wall_seconds_ceiling",
        "arm_model_calls",
        "arm_total_token_ceiling",
        "arm_wall_seconds_ceiling",
        "maximum_total_tokens_per_turn",
        "maximum_wall_seconds_per_turn",
        "offline_test_mode",
        "live_capacity_authority",
    )
    drift = [
        field
        for field in exact_fields
        if payload.get(field) != expected_context.get(field)
    ]
    available = payload.get("capacity_available")
    unknown = payload.get("capacity_unknown")
    expected_state = (
        "available_before_semantic_thread_or_turn"
        if available is True
        else "unavailable_before_semantic_thread_or_turn"
    )
    offline_mode = expected_context.get("offline_test_mode") is True
    if (
        drift
        or payload.get("schema_version") != CAPACITY_MEASUREMENT_VERSION
        or payload.get("state") != expected_state
        or not isinstance(available, bool)
        or not isinstance(unknown, bool)
        or payload.get("official_app_server_initialized_before_capacity") is not True
        or payload.get("semantic_thread_started")
        is not expected_semantic_thread_started
        or payload.get("semantic_thread_resumed")
        is not expected_semantic_thread_resumed
        or payload.get("semantic_turn_started") is not False
        or payload.get("managed_chatgpt_auth_only") is not True
        or payload.get("managed_chatgpt_plan_type") != "pro"
        or payload.get("extraction_only") is not True
        or payload.get("quality_evaluation_authorized") is not False
        or payload.get("holdout_authorized") is not False
        or payload.get("production_mutation_allowed") is not False
        or payload.get("offline_test_mode") is not offline_mode
        or payload.get("live_capacity_authority") is not (not offline_mode)
        or (
            not offline_mode
            and (
                payload.get("capacity_estimate_not_reservation") is not True
                or payload.get("single_turn_token_cap_prospective_only") is not True
                or payload.get("postturn_measured_stop_required") is not True
                or not isinstance(payload.get("provider_capacity"), Mapping)
            )
        )
    ):
        raise CanonicalV31DevelopmentRuntimeError(
            "capacity measurement lineage or closed-state drifted"
        )
    if not offline_mode:
        if (
            payload.get("sequence") != expected_sequence
            or payload.get("boundary") != expected_boundary
            or limits is None
        ):
            raise CanonicalV31DevelopmentRuntimeError(
                "live capacity measurement sequence or boundary drifted"
            )
        _validate_live_provider_projection(payload, limits=limits)
        identity = {
            "sequence": expected_sequence,
            "measured_at": payload["measured_at"],
            "provider_response_sha256": payload["provider_capacity"][
                "provider_response_sha256"
            ],
            "context": dict(expected_context),
        }
        if measurement_id != _sha256_bytes(
            _canonical_json(identity).encode("ascii")
        ):
            raise CanonicalV31DevelopmentRuntimeError(
                "live capacity measurement identity drifted"
            )
    required_tokens = expected_context.get("required_remaining_total_token_ceiling")
    required_wall = expected_context.get("required_remaining_wall_seconds_ceiling")
    if (
        isinstance(required_tokens, bool)
        or not isinstance(required_tokens, int)
        or required_tokens <= 0
        or _positive_number(required_wall, "required remaining wall") <= 0
    ):
        raise CanonicalV31DevelopmentRuntimeError(
            "capacity request is not positive numeric authority"
        )
    if (
        available is not True
        or unknown is not False
        or available_tokens < required_tokens
        or available_wall < float(required_wall)
    ):
        raise CanonicalV31CapacityUnavailable(
            "reserve capacity is unavailable before semantic client/thread/turn"
        )
    return {
        "measurement": payload,
        "measurement_sha256": checksum,
        "measurement_id": measurement_id,
        "available_total_tokens": available_tokens,
        "available_wall_seconds": available_wall,
    }


def _capacity_bundle(value: Any) -> tuple[dict[str, Any], str]:
    if not isinstance(value, Mapping):
        raise CanonicalV31DevelopmentRuntimeError(
            "capacity probe must return one measurement object"
        )
    measurement = copy.deepcopy(dict(value))
    return measurement, _sha256_bytes(_canonical_json(measurement).encode("ascii"))


def _admission_from_measurement(
    *,
    measurement_info: Mapping[str, Any],
    arm: Mapping[str, Any],
    preflight: Mapping[str, Any],
    precommit: Mapping[str, Any],
    future_plan_binding: Mapping[str, Any],
    directive_info: Mapping[str, Any],
    runtime_binding: Mapping[str, Any],
    limits: Mapping[str, Any],
) -> dict[str, Any]:
    measurement = measurement_info["measurement"]
    offline_mode = measurement.get("offline_test_mode") is True
    plan = future_plan_binding["binding"]
    admission_id = _sha256_bytes(
        f"{arm['variant_id']}:{measurement_info['measurement_id']}".encode("utf-8")
    )
    payload = {
        "schema_version": offline_matrix.CAPACITY_ADMISSION_VERSION,
        "state": (
            "admitted_after_official_app_server_initialization_"
            "before_semantic_thread_or_turn"
        ),
        "admission_id": admission_id,
        "issued_at": measurement["measured_at"],
        "expires_at": measurement["expires_at"],
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "variant_id": arm["variant_id"],
        "batch_size": arm["batch_size"],
        "thread_mode": arm["thread_mode"],
        "request_count": arm["request_count"],
        "request_set_sha256": arm["request_set_sha256"],
        "future_plan_binding_sha256": future_plan_binding["binding_sha256"],
        "future_directive_sha256": directive_info["directive_sha256"],
        "precommit_sha256": precommit["precommit_sha256"],
        "manifest_sha256": plan["manifest_sha256"],
        "runtime_binding_sha256": plan["runtime_binding_sha256"],
        "live_runtime_binding_sha256": runtime_binding["binding_sha256"],
        "context_control_overlay_sha256": plan[
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": plan[
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_sha256": plan["capacity_policy_sha256"],
        "capacity_measurement_sha256": measurement_info["measurement_sha256"],
        "requested_total_token_ceiling": arm["request_count"]
        * limits["maximum_total_tokens_per_turn"],
        "available_total_tokens": measurement_info["available_total_tokens"],
        "requested_wall_seconds_ceiling": arm["request_count"]
        * limits["maximum_wall_seconds_per_turn"],
        "available_wall_seconds": measurement_info["available_wall_seconds"],
        "capacity_available": True,
        "capacity_unknown": False,
        "official_app_server_initialized_before_capacity": True,
        "capacity_admission_required_before_thread_start": True,
        "capacity_admission_required_before_thread_resume": True,
        "capacity_admission_required_before_turn_start": True,
        "semantic_thread_started": False,
        "semantic_turn_started": False,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "semantic_retry_count": 0,
        "extraction_only": True,
        "quality_evaluation_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "offline_test_mode": offline_mode,
        "live_capacity_authority": not offline_mode,
        "capacity_estimate_not_reservation": not offline_mode,
        "single_turn_token_cap_prospective_only": not offline_mode,
        "postturn_measured_stop_required": not offline_mode,
    }
    return {
        "admission": payload,
        "admission_sha256": _sha256_bytes(_canonical_json(payload).encode("ascii")),
        "admission_id": admission_id,
    }


def _historical_validation_time(payload: Mapping[str, Any]) -> datetime:
    issued = _parse_timestamp(payload.get("issued_at"), "admission issued_at")
    expires = _parse_timestamp(payload.get("expires_at"), "admission expires_at")
    if expires <= issued:
        raise CanonicalV31DevelopmentRuntimeError(
            "historical admission interval is invalid"
        )
    return issued + min(timedelta(microseconds=1), (expires - issued) / 2)


def load_full_canonical_arm_outputs(
    *,
    arm_dir: Path,
    report: Mapping[str, Any],
    requests: Sequence[Mapping[str, Any]],
    request_sha256s: Sequence[str],
    manifest_info: Mapping[str, Any],
) -> dict[str, Any]:
    """Load full opaque outputs while rechecking every adapter artifact record."""

    root = _checked_lexical_path(arm_dir, "canonical arm root")
    if not _is_real_directory(root):
        raise CanonicalV31DevelopmentRuntimeError(
            "canonical arm root is not a real directory"
        )
    results = report.get("results") if isinstance(report, Mapping) else None
    if not isinstance(results, list) or len(results) != len(requests):
        raise CanonicalV31DevelopmentRuntimeError(
            "adapter report result count differs from exact requests"
        )
    if len(request_sha256s) != len(requests):
        raise CanonicalV31DevelopmentRuntimeError(
            "preflight request hash count drifted"
        )
    matrix_binding = _load_object(root / "matrix-binding.json", "matrix binding")
    if matrix_binding != adapter.build_six_arm_matrix_binding():
        raise CanonicalV31DevelopmentRuntimeError(
            "adapter matrix runtime binding drifted"
        )
    run_spec = _load_object(root / "run-spec.json", "adapter run specification")
    if (
        run_spec.get("schema_version") != adapter.ADAPTER_SCHEMA_VERSION
        or run_spec.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or run_spec.get("batch_size") != report.get("batch_size")
        or run_spec.get("thread_mode") != report.get("thread_mode")
        or run_spec.get("batch_count") != len(requests)
        or run_spec.get("model") != adapter.MODEL
        or run_spec.get("effort") != adapter.EFFORT
        or run_spec.get("managed_chatgpt_auth_only") is not True
        or run_spec.get("managed_chatgpt_plan_type") != "pro"
        or run_spec.get("official_persistent_codex_app_server_only") is not True
        or run_spec.get("app_server_process_count") != 1
        or run_spec.get("retry_count") != 0
        or run_spec.get("semantic_postprocessing") is not False
        or run_spec.get("project_instruction_content_byte_budget") != 0
        or run_spec.get("production_mutation_allowed") is not False
    ):
        raise CanonicalV31DevelopmentRuntimeError(
            "adapter run specification is not exact extraction-only authority"
        )
    case_by_segment = {
        str(row["segment_id"]): str(row["opaque_case_id"])
        for row in manifest_info.get("rows") or []
        if isinstance(row, Mapping)
    }
    outputs: list[dict[str, Any]] = []
    artifact_rows: list[dict[str, Any]] = []
    seen_artifacts: set[Path] = set()
    verified_turns: list[dict[str, Any]] = []
    expected_instruction_sources = adapter.expected_instruction_source_contract()
    role_names = {
        "attempt": "semantic-call-attempt.json",
        "sidecar": "sidecar.json",
        "raw_output": "output.private.json",
        "canonical_labels": "canonical-labels.private.json",
        "evidence_provenance": "evidence-provenance.private.json",
        "semantic_fidelity": "semantic-fidelity.json",
    }
    for index, (request, expected_request_sha, result) in enumerate(
        zip(requests, request_sha256s, results)
    ):
        if (
            not isinstance(result, Mapping)
            or result.get("batch_id") != request.get("batch_id")
            or _sha256_bytes(_canonical_json(request).encode("ascii"))
            != expected_request_sha
        ):
            raise CanonicalV31DevelopmentRuntimeError(
                "adapter artifact/request hash or batch order drifted"
            )
        batch_root = root / "batches" / str(request["batch_id"])
        if not _inside(batch_root, root) or not _is_real_directory(batch_root):
            raise CanonicalV31DevelopmentRuntimeError(
                "adapter batch artifact root is absent or escapes the arm"
            )
        input_path = batch_root / "input.private.json"
        prompt_path = batch_root / "prompt.private.md"
        base_path = batch_root / "base-instructions.private.md"
        schema_path = batch_root / "schema.json"
        try:
            input_payload = json.loads(input_path.read_text(encoding="utf-8"))
            schema_payload = json.loads(schema_path.read_text(encoding="utf-8"))
            prompt = prompt_path.read_text(encoding="utf-8")
            base = base_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CanonicalV31DevelopmentRuntimeError(
                "prepared adapter request artifacts are unreadable"
            ) from exc
        if (
            input_payload != request.get("private_input")
            or prompt != request.get("prompt")
            or base != request.get("base_instructions")
            or schema_payload != request.get("output_schema")
        ):
            raise CanonicalV31DevelopmentRuntimeError(
                "prepared adapter request artifacts differ from preflight"
            )
        records = result.get("records")
        if not isinstance(records, Mapping) or set(records) != set(_ARTIFACT_ROLES):
            raise CanonicalV31DevelopmentRuntimeError(
                "adapter result artifact record set is incomplete"
            )
        verified_records: dict[str, dict[str, Any]] = {}
        for role in _ARTIFACT_ROLES:
            path = _verify_record(
                records[role],
                label=f"arm result {index} {role}",
                allowed_root=root,
            )
            if path in seen_artifacts:
                raise CanonicalV31DevelopmentRuntimeError(
                    "adapter artifact was reused across batch results"
                )
            seen_artifacts.add(path)
            if path != (batch_root / role_names[role]).resolve():
                raise CanonicalV31DevelopmentRuntimeError(
                    "adapter artifact role was rebound to another path"
                )
            verified_records[role] = _record(path)
        attempt = _load_object(
            Path(verified_records["attempt"]["path"]),
            "semantic call attempt",
        )
        attempt_thread_id = attempt.get("thread_id")
        attempt_preflight = attempt.get("preflight")
        expected_preflight = {
            "thread_id": attempt_thread_id,
            "instruction_sources_sha256": expected_instruction_sources[
                "effective_instruction_sources_sha256"
            ],
            "instruction_sources_count": expected_instruction_sources[
                "effective_instruction_sources_count"
            ],
            "project_instruction_content_byte_budget": 0,
            "project_instruction_content_included": False,
            "context_control_overlay_sha256": request[
                "context_control_overlay_sha256"
            ],
        }
        if (
            attempt.get("schema_version") != adapter.ATTEMPT_RECEIPT_VERSION
            or attempt.get("state") != "semantic_call_dispatch_committed"
            or attempt.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
            or attempt.get("batch_id") != request.get("batch_id")
            or attempt.get("episode_id") != request.get("episode_id")
            or attempt.get("thread_mode") != request.get("thread_mode")
            or not isinstance(attempt_thread_id, str)
            or not attempt_thread_id
            or attempt.get("retry_count") != 0
            or attempt_preflight != expected_preflight
        ):
            raise CanonicalV31DevelopmentRuntimeError(
                "semantic call attempt is incomplete, retried, or unbound"
            )
        _parse_timestamp(attempt.get("created_at"), "semantic attempt created_at")
        expected_thread = SimpleNamespace(
            model=adapter.MODEL,
            ephemeral=True,
            base_instructions_sha256=request["base_instructions_sha256"],
            base_instructions_bytes=len(
                request["base_instructions"].encode("utf-8")
            ),
            thread_id=attempt_thread_id,
            instruction_sources_sha256=expected_instruction_sources[
                "effective_instruction_sources_sha256"
            ],
            instruction_sources_count=expected_instruction_sources[
                "effective_instruction_sources_count"
            ],
        )
        try:
            telemetry = adapter.validate_turn_sidecar(
                request,
                Path(verified_records["sidecar"]["path"]),
                output_path=Path(verified_records["raw_output"]["path"]),
                expected_thread=expected_thread,
            )
        except adapter.CanonicalV31EpisodeBatchError as exc:
            raise CanonicalV31DevelopmentRuntimeError(
                "managed-auth turn sidecar semantic validation failed"
            ) from exc
        raw_output = _load_object(
            Path(verified_records["raw_output"]["path"]),
            "sidecar-bound raw output",
        )
        try:
            projected = adapter.validate_and_project_output(request, raw_output)
        except adapter.CanonicalV31EpisodeBatchError as exc:
            raise CanonicalV31DevelopmentRuntimeError(
                "raw output is not a full canonical v3.1 projection"
            ) from exc
        labels = _load_array(
            Path(verified_records["canonical_labels"]["path"]),
            "canonical label artifact",
        )
        provenance = _load_object(
            Path(verified_records["evidence_provenance"]["path"]),
            "evidence provenance artifact",
        )
        fidelity = _load_object(
            Path(verified_records["semantic_fidelity"]["path"]),
            "semantic fidelity artifact",
        )
        result_artifact = _load_object(
            batch_root / "result.json", "adapter result artifact"
        )
        if (
            labels != projected["labels"]
            or provenance != projected["provenance"]
            or fidelity != projected["fidelity"]
            or result_artifact != dict(result)
            or result.get("state") != "validated"
            or result.get("failure_class") is not None
            or result.get("thread_reusable") is not True
            or result.get("thread_id") != telemetry["thread_id"]
            or result.get("turn_id") != telemetry["turn_id"]
            or result.get("usage") != telemetry["usage"]
            or result.get("wall_elapsed_seconds")
            != telemetry["wall_elapsed_seconds"]
            or result.get("emitted_event_count")
            != fidelity.get("emitted_event_count")
            or result.get("emitted_concept_candidate_count")
            != fidelity.get("emitted_concept_candidate_count")
        ):
            raise CanonicalV31DevelopmentRuntimeError(
                "adapter result, raw output, provenance, or fidelity semantics drifted"
            )
        verified_turns.append(
            {
                "batch_id": request["batch_id"],
                "episode_id": request["episode_id"],
                "thread_id": telemetry["thread_id"],
                "turn_id": telemetry["turn_id"],
                "usage": telemetry["usage"],
                "wall_elapsed_seconds": telemetry["wall_elapsed_seconds"],
            }
        )
        expected_segment_ids = list(request.get("segment_ids") or [])
        observed_segment_ids = [
            row.get("segment_id") for row in labels if isinstance(row, Mapping)
        ]
        if observed_segment_ids != expected_segment_ids or len(labels) != len(
            expected_segment_ids
        ):
            raise CanonicalV31DevelopmentRuntimeError(
                "canonical label artifact membership or order drifted"
            )
        for label in labels:
            if not isinstance(label, dict):
                raise CanonicalV31DevelopmentRuntimeError(
                    "canonical label artifact row is malformed"
                )
            case_id = case_by_segment.get(str(label.get("segment_id")))
            if case_id is None:
                raise CanonicalV31DevelopmentRuntimeError(
                    "canonical label has no opaque manifest identity"
                )
            outputs.append(
                {"opaque_case_id": case_id, "label": copy.deepcopy(label)}
            )
        artifact_rows.append(
            {
                "batch_id": request["batch_id"],
                "request_sha256": expected_request_sha,
                "prepared_input": _record(input_path),
                "prepared_prompt": _record(prompt_path),
                "prepared_base_instructions": _record(base_path),
                "prepared_output_schema": _record(schema_path),
                "result": _record(batch_root / "result.json"),
                "result_records": verified_records,
            }
        )
    expected_order = manifest_info.get("opaque_case_order")
    if [row["opaque_case_id"] for row in outputs] != expected_order:
        raise CanonicalV31DevelopmentRuntimeError(
            "full canonical output opaque order drifted"
        )
    turn_ids = [str(row["turn_id"]) for row in verified_turns]
    thread_ids = [str(row["thread_id"]) for row in verified_turns]
    if len(turn_ids) != len(set(turn_ids)):
        raise CanonicalV31DevelopmentRuntimeError(
            "verified semantic turn identity was duplicated within an arm"
        )
    thread_mode = report.get("thread_mode")
    if thread_mode == "new_thread":
        thread_lineage_valid = len(thread_ids) == len(set(thread_ids))
    else:
        by_episode: dict[str, set[str]] = {}
        for row in verified_turns:
            by_episode.setdefault(str(row["episode_id"]), set()).add(
                str(row["thread_id"])
            )
        selected = [next(iter(values)) for values in by_episode.values() if len(values) == 1]
        thread_lineage_valid = (
            set(by_episode) == {str(request["episode_id"]) for request in requests}
            and all(len(values) == 1 for values in by_episode.values())
            and len(selected) == len(set(selected))
        )
    verified_usage = _sum_usage([row["usage"] for row in verified_turns])
    verified_wall = sum(float(row["wall_elapsed_seconds"]) for row in verified_turns)
    if (
        not thread_lineage_valid
        or report.get("schema_version") != adapter.RUN_REPORT_VERSION
        or report.get("state") != "passed"
        or report.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or report.get("model") != adapter.MODEL
        or report.get("effort") != adapter.EFFORT
        or report.get("requested_calls") != len(requests)
        or report.get("attempted_calls") != len(verified_turns)
        or report.get("validated_calls") != len(verified_turns)
        or report.get("rejected_calls") != 0
        or report.get("failed_calls") != 0
        or report.get("usage_status") != "complete"
        or report.get("usage") != verified_usage
        or report.get("retry_count") != 0
        or report.get("ambiguous_retry_count") != 0
        or report.get("semantic_postprocessing") is not False
        or report.get("turn_ids_unique") is not True
        or report.get("thread_lineage_valid") is not True
        or report.get("observed_turn_count") != len(turn_ids)
        or report.get("observed_thread_count") != len(set(thread_ids))
        or report.get("managed_chatgpt_auth_verified") is not True
        or report.get("managed_chatgpt_plan_type") != "pro"
        or report.get("project_instruction_content_byte_budget") != 0
        or report.get("production_mutated") is not False
        or report.get("holdout_authorized") is not False
    ):
        raise CanonicalV31DevelopmentRuntimeError(
            "adapter report is not derived from exact successful zero-retry attempts"
        )
    return {
        "outputs": outputs,
        "outputs_sha256": _sha256_bytes(_canonical_json(outputs).encode("ascii")),
        "opaque_case_order_sha256": manifest_info.get("opaque_case_order_sha256"),
        "request_set_sha256": _sha256_bytes(
            _canonical_json(list(request_sha256s)).encode("ascii")
        ),
        "artifacts": artifact_rows,
        "verified_attempt_count": len(verified_turns),
        "verified_usage": verified_usage,
        "verified_turn_wall_elapsed_seconds": verified_wall,
        "verified_turns": verified_turns,
    }


def _validate_runtime_report_caps(
    report: Mapping[str, Any],
    *,
    arm: Mapping[str, Any],
    limits: Mapping[str, Any],
    artifact_validation: Mapping[str, Any],
) -> dict[str, Any]:
    usage = _usage(
        artifact_validation.get("verified_usage"),
        f"{arm['variant_id']} verified attempts",
    )
    rows = artifact_validation.get("verified_turns")
    if (
        not isinstance(rows, list)
        or len(rows) != arm["request_count"]
        or artifact_validation.get("verified_attempt_count") != arm["request_count"]
    ):
        raise CanonicalV31DevelopmentRuntimeError(
            "verified arm attempt count drifted"
        )
    turn_usage: list[dict[str, int]] = []
    turn_wall = 0.0
    for row in rows:
        if not isinstance(row, Mapping):
            raise CanonicalV31DevelopmentRuntimeError("arm result row is malformed")
        observed = _usage(row.get("usage"), "arm turn")
        wall = _nonnegative_number(row.get("wall_elapsed_seconds"), "arm turn wall")
        if (
            observed["total_tokens"] > limits["maximum_total_tokens_per_turn"]
            or wall > limits["maximum_wall_seconds_per_turn"]
        ):
            raise CanonicalV31DevelopmentRuntimeError(
                "arm turn exceeded the exact semantic-plan capacity cap"
            )
        turn_usage.append(observed)
        turn_wall += wall
    if _sum_usage(turn_usage) != usage:
        raise CanonicalV31DevelopmentRuntimeError(
            "verified arm usage differs from per-turn accounting"
        )
    if _usage(report.get("usage"), f"{arm['variant_id']} report") != usage:
        raise CanonicalV31DevelopmentRuntimeError(
            "arm report usage differs from verified attempt accounting"
        )
    arm_token_cap = arm["request_count"] * limits["maximum_total_tokens_per_turn"]
    arm_wall_cap = arm["request_count"] * limits["maximum_wall_seconds_per_turn"]
    if usage["total_tokens"] > arm_token_cap or turn_wall > arm_wall_cap:
        raise CanonicalV31DevelopmentRuntimeError(
            "arm aggregate exceeded its exact admission cap"
        )
    return {"usage": usage, "turn_wall_elapsed_seconds": turn_wall}


def _probe_context(
    *,
    arm: Mapping[str, Any],
    preflight: Mapping[str, Any],
    precommit: Mapping[str, Any],
    future_plan_binding: Mapping[str, Any],
    directive_info: Mapping[str, Any],
    runtime_binding: Mapping[str, Any],
    limits: Mapping[str, Any],
    used_calls: int,
    used_total_tokens: int,
    offline_test_mode: bool,
    remaining_arm_calls: int | None = None,
) -> dict[str, Any]:
    remaining_calls = limits["exact_model_call_cap"] - used_calls
    global_remaining = limits["exact_total_token_cap"] - used_total_tokens
    required_remaining = remaining_calls * limits["maximum_total_tokens_per_turn"]
    arm_calls = arm["request_count"] if remaining_arm_calls is None else remaining_arm_calls
    if (
        isinstance(arm_calls, bool)
        or not isinstance(arm_calls, int)
        or arm_calls <= 0
        or arm_calls > arm["request_count"]
        or remaining_calls < arm_calls
        or global_remaining < required_remaining
        or used_calls < 0
        or used_total_tokens < 0
    ):
        raise CanonicalV31DevelopmentRuntimeError(
            "global semantic-plan budget cannot admit the remaining matrix"
        )
    plan = future_plan_binding["binding"]
    return {
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "variant_id": arm["variant_id"],
        "batch_size": arm["batch_size"],
        "thread_mode": arm["thread_mode"],
        "request_count": arm["request_count"],
        "request_set_sha256": arm["request_set_sha256"],
        "future_plan_binding_sha256": future_plan_binding["binding_sha256"],
        "future_directive_sha256": directive_info["directive_sha256"],
        "precommit_sha256": precommit["precommit_sha256"],
        "manifest_sha256": plan["manifest_sha256"],
        "context_set_sha256": plan["context_set_sha256"],
        "adapter_runtime_binding_sha256": plan["runtime_binding_sha256"],
        "live_runtime_binding_sha256": runtime_binding["binding_sha256"],
        "context_control_overlay_sha256": plan[
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": plan[
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_sha256": plan["capacity_policy_sha256"],
        "remaining_model_calls": remaining_calls,
        "global_remaining_total_token_budget": global_remaining,
        "required_remaining_total_token_ceiling": required_remaining,
        "required_remaining_wall_seconds_ceiling": remaining_calls
        * limits["maximum_wall_seconds_per_turn"],
        "arm_model_calls": arm_calls,
        "arm_total_token_ceiling": arm_calls
        * limits["maximum_total_tokens_per_turn"],
        "arm_wall_seconds_ceiling": arm_calls
        * limits["maximum_wall_seconds_per_turn"],
        "maximum_total_tokens_per_turn": limits[
            "maximum_total_tokens_per_turn"
        ],
        "maximum_wall_seconds_per_turn": limits[
            "maximum_wall_seconds_per_turn"
        ],
        "offline_test_mode": offline_test_mode,
        "live_capacity_authority": not offline_test_mode,
    }


def _directory_entries(path: Path, label: str) -> dict[str, Path]:
    _checked_lexical_path(path, label)
    if not _is_real_directory(path):
        raise CanonicalV31RecoveryRequired(f"{label} is not a real directory")
    entries = {entry.name: entry for entry in path.iterdir()}
    if len(entries) != len(list(path.iterdir())):
        raise CanonicalV31RecoveryRequired(f"{label} has duplicate directory entries")
    return entries


def _validate_capacity_tree(capacity_root: Path, expected_variants: set[str]) -> None:
    entries = _directory_entries(capacity_root, "capacity root")
    unknown = set(entries) - expected_variants
    if unknown:
        raise CanonicalV31RecoveryRequired(
            "capacity root contains foreign variants: " + ", ".join(sorted(unknown))
        )
    for variant_id, variant_root in entries.items():
        evidence = _directory_entries(variant_root, f"{variant_id} capacity evidence")
        if len(evidence) != 1:
            raise CanonicalV31RecoveryRequired(
                f"{variant_id} capacity evidence is partial or ambiguous"
            )
        evidence_root = next(iter(evidence.values()))
        records = _directory_entries(evidence_root, f"{variant_id} capacity record set")
        offline_files = {"request.json", "measurement.json", "admission.json"}
        live_files = offline_files | {"reprobes"}
        if frozenset(records) not in {frozenset(offline_files), frozenset(live_files)}:
            raise CanonicalV31RecoveryRequired(
                f"{variant_id} capacity record set is not exact"
            )
        if any(
            not _is_real_file(path)
            for name, path in records.items()
            if name != "reprobes"
        ):
            raise CanonicalV31RecoveryRequired(
                f"{variant_id} capacity record set contains a non-file"
            )
        if "reprobes" in records:
            reprobes = _directory_entries(
                records["reprobes"], f"{variant_id} capacity reprobes"
            )
            for name, reprobe_root in reprobes.items():
                reprobe_records = _directory_entries(
                    reprobe_root, f"{variant_id} capacity reprobe {name}"
                )
                if set(reprobe_records) != {
                    "request.json",
                    "measurement.json",
                } or any(not _is_real_file(path) for path in reprobe_records.values()):
                    raise CanonicalV31RecoveryRequired(
                        f"{variant_id} capacity reprobe record set is not exact"
                    )


def _validate_arm_directory_contents(
    arm_dir: Path,
    requests: Sequence[Mapping[str, Any]],
    *,
    envelope_required: bool,
) -> None:
    entries = _directory_entries(arm_dir, "adapter arm root")
    expected = set(_ARM_DIRECT_ARTIFACTS)
    if envelope_required:
        expected.add("arm-envelope.json")
    if set(entries) != expected:
        raise CanonicalV31RecoveryRequired(
            "adapter arm root contains partial or foreign artifacts"
        )
    for name in expected - {"batches"}:
        if not _is_real_file(entries[name]):
            raise CanonicalV31RecoveryRequired(
                f"adapter arm artifact is not a real file: {name}"
            )
    batches = _directory_entries(entries["batches"], "adapter batch root")
    expected_batch_ids = [str(request["batch_id"]) for request in requests]
    if set(batches) != set(expected_batch_ids) or len(batches) != len(
        expected_batch_ids
    ):
        raise CanonicalV31RecoveryRequired(
            "adapter batch directory membership drifted"
        )
    for batch_id in expected_batch_ids:
        artifacts = _directory_entries(
            batches[batch_id], f"adapter batch {batch_id} artifact root"
        )
        if set(artifacts) != _BATCH_ARTIFACT_NAMES or any(
            not _is_real_file(path) for path in artifacts.values()
        ):
            raise CanonicalV31RecoveryRequired(
                f"adapter batch {batch_id} artifact set is not exact"
            )


def _arm_state(arm_dir: Path, capacity_variant_root: Path) -> str:
    arm_exists = arm_dir.exists()
    capacity_exists = capacity_variant_root.exists()
    if not arm_exists and not capacity_exists:
        return "absent"
    if arm_exists != capacity_exists:
        raise CanonicalV31RecoveryRequired(
            "orphan arm or capacity admission forbids replay; authorize a new epoch"
        )
    if not _is_real_directory(arm_dir) or not _is_real_directory(
        capacity_variant_root
    ):
        raise CanonicalV31RecoveryRequired("arm root is not a real directory")
    report = arm_dir / "report.json"
    envelope = arm_dir / "arm-envelope.json"
    if _is_real_file(report) and _is_real_file(envelope):
        return "complete_candidate"
    raise CanonicalV31RecoveryRequired(
        "partial or nonempty arm evidence forbids replay; authorize a new epoch"
    )


def _initialize_runtime_root(
    *,
    root: Path,
    preflight: Mapping[str, Any],
    precommit: Mapping[str, Any],
    runtime_binding: Mapping[str, Any],
) -> dict[str, Any]:
    if root.exists() and not _is_real_directory(root):
        raise CanonicalV31RecoveryRequired(
            "development output root is not a real directory"
        )
    root.mkdir(parents=True, exist_ok=True)
    _checked_lexical_path(root, "development output root")
    if not _is_real_directory(root):
        raise CanonicalV31RecoveryRequired(
            "development output root is not a real directory"
        )
    allowed = {
        "runtime-binding.json",
        "offline-preflight.json",
        "matrix-precommit.json",
        "arms",
        "capacity",
        EXTRACTION_RECEIPT_FILENAME,
    }
    root_entries = _directory_entries(root, "development output root")
    unknown = sorted(name for name in root_entries if name not in allowed)
    if unknown:
        raise CanonicalV31RecoveryRequired(
            "development-only root contains foreign artifacts: " + ", ".join(unknown)
        )
    control_paths = {
        "runtime_binding": root / "runtime-binding.json",
        "offline_preflight": root / "offline-preflight.json",
        "matrix_precommit": root / "matrix-precommit.json",
    }
    any_control = any(path.exists() for path in control_paths.values())
    all_control = all(_is_real_file(path) for path in control_paths.values())
    has_progress = any(
        name in {"arms", "capacity", EXTRACTION_RECEIPT_FILENAME}
        for name in root_entries
    )
    if (any_control or has_progress) and not all_control:
        raise CanonicalV31RecoveryRequired(
            "runtime control plane is partial; authorize a new epoch"
        )
    records = {
        "runtime_binding": _write_or_verify_json(
            control_paths["runtime_binding"], runtime_binding["binding"]
        ),
        "offline_preflight": _write_or_verify_json(
            control_paths["offline_preflight"], preflight["receipt"]
        ),
        "matrix_precommit": _write_or_verify_json(
            control_paths["matrix_precommit"], precommit["precommit"]
        ),
    }
    arms_root = root / "arms"
    capacity_root = root / "capacity"
    arms_root.mkdir(exist_ok=True)
    capacity_root.mkdir(exist_ok=True)
    expected_variants = {
        str(row["variant_id"]) for row in precommit["precommit"]["arms"]
    }
    arm_entries = _directory_entries(arms_root, "arms root")
    unknown_arms = set(arm_entries) - expected_variants
    if unknown_arms:
        raise CanonicalV31RecoveryRequired(
            "arms root contains foreign variants: "
            + ", ".join(sorted(unknown_arms))
        )
    if any(not _is_real_directory(path) for path in arm_entries.values()):
        raise CanonicalV31RecoveryRequired(
            "arms root contains a non-directory or symlinked variant"
        )
    _validate_capacity_tree(capacity_root, expected_variants)
    return records


def _arm_envelope_payload(
    *,
    arm: Mapping[str, Any],
    report: Mapping[str, Any],
    outputs: Mapping[str, Any],
    measurement_info: Mapping[str, Any],
    capacity_request_record: Mapping[str, Any],
    measurement_record: Mapping[str, Any],
    admission_info: Mapping[str, Any],
    admission_record: Mapping[str, Any],
    preflight: Mapping[str, Any],
    precommit: Mapping[str, Any],
    future_plan_binding: Mapping[str, Any],
    directive_info: Mapping[str, Any],
    semantic_authority: Mapping[str, Any],
    runtime_binding: Mapping[str, Any],
    capacity_reprobes: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    plan = future_plan_binding["binding"]
    return {
        "schema_version": offline_matrix.ARM_ENVELOPE_VERSION,
        "state": "completed_full_canonical_v31_arm",
        "variant_id": arm["variant_id"],
        "precommit_sha256": precommit["precommit_sha256"],
        "future_plan_binding_sha256": future_plan_binding["binding_sha256"],
        "future_directive_sha256": directive_info["directive_sha256"],
        "capacity_admission_sha256": admission_info["admission_sha256"],
        "manifest_sha256": plan["manifest_sha256"],
        "context_set_sha256": plan["context_set_sha256"],
        "runtime_binding_sha256": plan["runtime_binding_sha256"],
        "context_control_overlay_sha256": plan[
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": plan[
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_sha256": plan["capacity_policy_sha256"],
        "semantic_plan_sha256": semantic_authority["semantic_plan_sha256"],
        "semantic_plan_contract_sha256": semantic_authority[
            "semantic_plan_contract"
        ]["contract_sha256"],
        "live_runtime_binding_sha256": runtime_binding["binding_sha256"],
        "capacity_measurement_sha256": measurement_info["measurement_sha256"],
        "capacity_request_record": copy.deepcopy(dict(capacity_request_record)),
        "capacity_measurement_record": copy.deepcopy(dict(measurement_record)),
        "capacity_admission_record": copy.deepcopy(dict(admission_record)),
        "capacity_reprobes": copy.deepcopy(list(capacity_reprobes)),
        "offline_test_mode": measurement_info["measurement"].get(
            "offline_test_mode"
        ),
        "capacity_estimate_not_reservation": measurement_info["measurement"].get(
            "capacity_estimate_not_reservation", False
        ),
        "admission_id": admission_info["admission_id"],
        "artifact_validation": copy.deepcopy(dict(outputs)),
        "adapter_report": copy.deepcopy(dict(report)),
        "outputs": copy.deepcopy(outputs["outputs"]),
        "extraction_only": True,
        "quality_evaluation_performed": False,
        "holdout_inspected": False,
        "production_mutated": False,
    }


def _validate_envelope_runtime_extensions(
    envelope: Mapping[str, Any],
    *,
    semantic_authority: Mapping[str, Any],
    runtime_binding: Mapping[str, Any],
    arm_root: Path,
    capacity_root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    request_path = _verify_record(
        envelope.get("capacity_request_record"),
        label="capacity request",
        allowed_root=capacity_root,
    )
    measurement_path = _verify_record(
        envelope.get("capacity_measurement_record"),
        label="capacity measurement",
        allowed_root=capacity_root,
    )
    admission_path = _verify_record(
        envelope.get("capacity_admission_record"),
        label="capacity admission",
        allowed_root=capacity_root,
    )
    measurement = _load_object(measurement_path, "capacity measurement")
    admission = _load_object(admission_path, "capacity admission")
    request = _load_object(request_path, "capacity request")
    artifact_validation = envelope.get("artifact_validation")
    reprobes = envelope.get("capacity_reprobes")
    variant_id = envelope.get("variant_id")
    measurement_id = measurement.get("measurement_id")
    if (
        not isinstance(variant_id, str)
        or not variant_id
        or not isinstance(measurement_id, str)
        or not measurement_id
    ):
        raise CanonicalV31RecoveryRequired(
            "completed arm capacity evidence identity is absent"
        )
    expected_evidence_root = _checked_lexical_path(
        capacity_root
        / variant_id
        / _sha256_bytes(measurement_id.encode("utf-8")),
        "completed arm capacity evidence root",
    )
    if (
        not isinstance(artifact_validation, Mapping)
        or not isinstance(reprobes, list)
        or request_path.parent != expected_evidence_root
        or measurement_path.parent != expected_evidence_root
        or admission_path.parent != expected_evidence_root
        or envelope.get("semantic_plan_sha256")
        != semantic_authority["semantic_plan_sha256"]
        or envelope.get("semantic_plan_contract_sha256")
        != semantic_authority["semantic_plan_contract"]["contract_sha256"]
        or envelope.get("live_runtime_binding_sha256")
        != runtime_binding["binding_sha256"]
        or envelope.get("capacity_measurement_sha256")
        != _sha256_bytes(_canonical_json(measurement).encode("ascii"))
        or envelope.get("capacity_admission_sha256")
        != _sha256_bytes(_canonical_json(admission).encode("ascii"))
        or envelope.get("admission_id") != admission.get("admission_id")
        or envelope.get("offline_test_mode")
        is not (measurement.get("offline_test_mode") is True)
        or envelope.get("capacity_estimate_not_reservation")
        is not (measurement.get("offline_test_mode") is not True)
        or envelope.get("extraction_only") is not True
        or envelope.get("quality_evaluation_performed") is not False
        or envelope.get("holdout_inspected") is not False
        or envelope.get("production_mutated") is not False
    ):
        raise CanonicalV31RecoveryRequired(
            "completed arm runtime extension lineage drifted"
        )
    for index, reprobe in enumerate(reprobes):
        if not isinstance(reprobe, Mapping) or not isinstance(
            reprobe.get("records"), Mapping
        ):
            raise CanonicalV31RecoveryRequired(
                "completed arm capacity reprobe receipt is malformed"
            )
        request_value = reprobe.get("request")
        measurement_value = reprobe.get("measurement")
        if not isinstance(request_value, Mapping) or not isinstance(
            measurement_value, Mapping
        ):
            raise CanonicalV31RecoveryRequired(
                "completed arm capacity reprobe payload is malformed"
            )
        sequence = request_value.get("sequence")
        boundary = request_value.get("boundary")
        if (
            isinstance(sequence, bool)
            or not isinstance(sequence, int)
            or sequence != index + 1
            or boundary not in {"thread_start", "thread_resume", "turn_start"}
        ):
            raise CanonicalV31RecoveryRequired(
                "completed arm capacity reprobe sequence drifted"
            )
        expected_reprobe_root = (
            expected_evidence_root / "reprobes" / f"{sequence:04d}-{boundary}"
        )
        loaded: dict[str, dict[str, Any]] = {}
        for role in ("request", "measurement"):
            record_path = _verify_record(
                reprobe["records"].get(role),
                label=f"capacity reprobe {index} {role}",
                allowed_root=capacity_root,
            )
            if record_path.parent != expected_reprobe_root:
                raise CanonicalV31RecoveryRequired(
                    "completed arm capacity reprobe path drifted"
                )
            loaded[role] = _load_object(
                record_path, f"capacity reprobe {index} {role}"
            )
        if (
            loaded["request"] != dict(request_value)
            or loaded["measurement"] != dict(measurement_value)
        ):
            raise CanonicalV31RecoveryRequired(
                "completed arm capacity reprobe record differs from envelope"
            )
    for row in artifact_validation.get("artifacts") or []:
        if not isinstance(row, Mapping):
            raise CanonicalV31RecoveryRequired("arm artifact receipt is malformed")
        for key, value in row.items():
            if key in {"batch_id", "request_sha256"}:
                continue
            if key == "result_records":
                if not isinstance(value, Mapping):
                    raise CanonicalV31RecoveryRequired(
                        "arm result record receipt is malformed"
                    )
                for role, record in value.items():
                    _verify_record(
                        record,
                        label=f"adopted {role}",
                        allowed_root=arm_root,
                    )
            else:
                _verify_record(
                    value,
                    label=f"adopted {key}",
                    allowed_root=arm_root,
                )
    return measurement, admission, dict(artifact_validation), {
        "request": request,
        "request_record": _record(request_path),
        "measurement_record": _record(measurement_path),
        "admission_record": _record(admission_path),
        "reprobes": copy.deepcopy(reprobes),
    }


def _historical_measurement_validation_time(
    payload: Mapping[str, Any],
) -> datetime:
    measured = _parse_timestamp(payload.get("measured_at"), "capacity measured_at")
    expires = _parse_timestamp(payload.get("expires_at"), "capacity expires_at")
    if expires <= measured:
        raise CanonicalV31DevelopmentRuntimeError(
            "historical capacity measurement interval is invalid"
        )
    return measured + min(timedelta(microseconds=1), (expires - measured) / 2)


def _validate_capacity_reprobes(
    reprobes: Sequence[Mapping[str, Any]],
    *,
    limits: Mapping[str, Any],
    offline_test_mode: bool,
) -> list[str]:
    """Revalidate every persisted pre-thread/pre-turn live capacity snapshot."""

    if offline_test_mode:
        if reprobes:
            raise CanonicalV31DevelopmentRuntimeError(
                "offline fixture arm cannot contain live capacity reprobes"
            )
        return []
    measurement_ids: list[str] = []
    for index, row in enumerate(reprobes):
        if not isinstance(row, Mapping):
            raise CanonicalV31DevelopmentRuntimeError(
                "live capacity reprobe receipt is malformed"
            )
        request = row.get("request")
        measurement = row.get("measurement")
        if not isinstance(request, Mapping) or not isinstance(measurement, Mapping):
            raise CanonicalV31DevelopmentRuntimeError(
                "live capacity reprobe payload is absent"
            )
        boundary = request.get("boundary")
        thread_started = request.get("semantic_thread_started")
        thread_resumed = request.get("semantic_thread_resumed")
        if (
            boundary not in {"thread_start", "thread_resume", "turn_start"}
            or not isinstance(thread_started, bool)
            or not isinstance(thread_resumed, bool)
        ):
            raise CanonicalV31DevelopmentRuntimeError(
                "live capacity reprobe lifecycle is malformed"
            )
        context = request.get("context")
        if not isinstance(context, Mapping):
            raise CanonicalV31DevelopmentRuntimeError(
                "live capacity reprobe context is absent"
            )
        _validate_capacity_probe_request(
            request,
            expected_context=context,
            expected_sequence=index + 1,
            expected_boundary=boundary,
            expected_semantic_thread_started=thread_started,
            expected_semantic_thread_resumed=thread_resumed,
        )
        checksum = _sha256_bytes(_canonical_json(dict(measurement)).encode("ascii"))
        info = validate_capacity_measurement(
            measurement,
            expected_context=context,
            expected_sha256=checksum,
            now=_historical_measurement_validation_time(measurement),
            historical=True,
            expected_sequence=index + 1,
            expected_boundary=boundary,
            expected_semantic_thread_started=thread_started,
            expected_semantic_thread_resumed=thread_resumed,
            limits=limits,
        )
        measurement_id = str(info["measurement_id"])
        if measurement_id in measurement_ids:
            raise CanonicalV31DevelopmentRuntimeError(
                "live capacity reprobe measurement identity was replayed"
            )
        measurement_ids.append(measurement_id)
    return measurement_ids


def _validate_capacity_reprobe_coverage(
    reprobes: Sequence[Mapping[str, Any]],
    *,
    artifact_validation: Mapping[str, Any],
    offline_test_mode: bool,
) -> None:
    if offline_test_mode:
        if reprobes:
            raise CanonicalV31DevelopmentRuntimeError(
                "offline fixture arm cannot claim live capacity coverage"
            )
        return
    boundaries = [
        row.get("request", {}).get("boundary")
        if isinstance(row, Mapping) and isinstance(row.get("request"), Mapping)
        else None
        for row in reprobes
    ]
    verified_turns = artifact_validation.get("verified_turns")
    if not isinstance(verified_turns, list):
        raise CanonicalV31DevelopmentRuntimeError(
            "live capacity coverage has no verified turns"
        )
    thread_ids = {
        str(row.get("thread_id"))
        for row in verified_turns
        if isinstance(row, Mapping)
    }
    expected_additional_thread_starts = max(0, len(thread_ids) - 1)
    if (
        boundaries.count("turn_start") != len(verified_turns)
        or boundaries.count("thread_start") != expected_additional_thread_starts
        or boundaries.count("thread_resume") != 0
        or len(boundaries)
        != len(verified_turns) + expected_additional_thread_starts
    ):
        raise CanonicalV31DevelopmentRuntimeError(
            "live capacity reprobes do not cover every semantic turn and thread start"
        )


def _receipt_static_fields(
    *,
    semantic_plan: supervisor.SemanticPlan,
    semantic_authority: Mapping[str, Any],
    preflight: Mapping[str, Any],
    precommit: Mapping[str, Any],
    future_plan_binding: Mapping[str, Any],
    runtime_binding: Mapping[str, Any],
    limits: Mapping[str, Any],
    usage: Mapping[str, Any],
    wall_seconds: float,
    full_output_validation: Mapping[str, Any],
    arm_receipts: Sequence[Mapping[str, Any]],
    verified_attempt_count: int,
    offline_test_mode: bool,
) -> dict[str, Any]:
    lock_info = _ACTIVE_INVOCATION_LOCK.get()
    if not isinstance(lock_info, Mapping):
        raise CanonicalV31DevelopmentRuntimeError(
            "exclusive runtime invocation lock is not held"
        )
    live_mode = not offline_test_mode
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    measured_calls = verified_attempt_count if live_mode else 0
    measured_usage = copy.deepcopy(dict(usage)) if live_mode else zero_usage
    verified_fixture_calls = verified_attempt_count if offline_test_mode else 0
    verified_fixture_usage = (
        copy.deepcopy(dict(usage)) if offline_test_mode else zero_usage
    )
    return {
        "schema_version": supervisor.SEMANTIC_STEP_RECEIPT_SCHEMA_VERSION,
        "receipt_contract_version": EXTRACTION_RECEIPT_VERSION,
        "thread_id": semantic_authority["thread_id"],
        "plan_epoch": semantic_plan.plan_epoch,
        "step_id": semantic_plan.step_id,
        "state": "waiting" if offline_test_mode else "passed",
        "waiting_reason": (
            "offline fixture extraction is non-promotable and has no live pass authority"
            if offline_test_mode
            else None
        ),
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "semantic_plan_sha256": semantic_authority["semantic_plan_sha256"],
        "semantic_plan_contract_sha256": semantic_authority[
            "semantic_plan_contract"
        ]["contract_sha256"],
        "future_plan_binding_sha256": future_plan_binding["binding_sha256"],
        "future_directive_sha256": semantic_authority["directive_sha256"],
        "precommit_sha256": precommit["precommit_sha256"],
        "manifest_sha256": preflight["receipt"]["manifest_sha256"],
        "context_set_sha256": preflight["receipt"]["context_set_sha256"],
        "adapter_runtime_binding_sha256": preflight["receipt"][
            "runtime_binding_sha256"
        ],
        "live_runtime_binding_sha256": runtime_binding["binding_sha256"],
        "invocation_lock_binding_sha256": lock_info["binding_sha256"],
        "invocation_lock_record": copy.deepcopy(dict(lock_info["record"])),
        "context_control_overlay_sha256": preflight["receipt"][
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": preflight["receipt"][
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_sha256": preflight["receipt"][
            "capacity_policy_sha256"
        ],
        "exact_model_call_cap": limits["exact_model_call_cap"],
        "measured_model_calls": measured_calls,
        "verified_fixture_attempt_count": verified_fixture_calls,
        "remaining_model_calls": limits["exact_model_call_cap"] - measured_calls,
        "exact_total_token_cap": limits["exact_total_token_cap"],
        "measured_usage": measured_usage,
        "verified_fixture_usage": verified_fixture_usage,
        "remaining_total_token_budget": (
            limits["exact_total_token_cap"] - int(measured_usage["total_tokens"])
        ),
        "measured_turn_wall_elapsed_seconds": (
            round(wall_seconds, 6) if live_mode else 0.0
        ),
        "verified_fixture_turn_wall_elapsed_seconds": (
            round(wall_seconds, 6) if offline_test_mode else 0.0
        ),
        "arm_count": len(EXPECTED_ARMS),
        "arms": copy.deepcopy(list(arm_receipts)),
        "full_output_validation": copy.deepcopy(dict(full_output_validation)),
        "all_six_arms_complete": True,
        "opaque_case_order_preserved": True,
        "cross_arm_thread_reuse": False,
        "cross_arm_turn_reuse": False,
        "cross_arm_admission_reuse": False,
        "execution_mode": OFFLINE_TEST_MODE if offline_test_mode else "live",
        "offline_test_mode": offline_test_mode,
        "promotable": live_mode,
        "live_pass_authority": live_mode,
        "live_dispatch_performed": live_mode,
        "capacity_is_conservative_estimate_not_reservation": live_mode,
        "single_turn_token_cap_is_prospective_only": live_mode,
        "preturn_reprobe_and_postturn_measured_stop": live_mode,
        "trusted_capacity_implementation_state": (
            TRUSTED_CAPACITY_IMPLEMENTATION_STATE
        ),
        "extraction_only": True,
        "quality_evaluation_performed": False,
        "quality_evaluation_authorized": False,
        "quality_selection_authorized": False,
        "holdout_inspected": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "production_mutation_allowed": False,
    }


def semantic_plan_payload_thread_id(plan: supervisor.SemanticPlan) -> str:
    """Return the target thread after supervisor parsing has fixed its identity."""

    # ``SemanticPlan`` intentionally does not repeat the thread ID.  The reader
    # only constructs it after matching the caller-supplied exact thread.
    directive = _load_object(plan.directive_path, "semantic directive")
    value = directive.get("thread_id")
    if not isinstance(value, str) or not value:
        raise CanonicalV31DevelopmentRuntimeError("semantic directive thread is absent")
    return value


def _validate_existing_receipt(
    receipt: Mapping[str, Any],
    *,
    static: Mapping[str, Any],
) -> dict[str, Any]:
    for key, expected in static.items():
        if key == "completed_at":
            continue
        if receipt.get(key) != expected:
            raise CanonicalV31RecoveryRequired(
                f"existing extraction-only receipt drifted at {key}"
            )
    _parse_timestamp(receipt.get("completed_at"), "receipt completed_at")
    if set(receipt) != set(static) | {"completed_at"}:
        raise CanonicalV31RecoveryRequired(
            "existing extraction-only receipt schema drifted"
        )
    return copy.deepcopy(dict(receipt))


@_exclusive_runtime_invocation
async def run_canonical_v31_development_matrix_runtime(
    *,
    output_root: Path,
    semantic_plan_path: Path,
    thread_id: str,
    project_root: Path,
    manifest_path: Path,
    manifest_sha256: str,
    episodes: Sequence[Mapping[str, Any]],
    capacity_policy_path: Path,
    capacity_policy_sha256: str,
    future_plan_binding: Mapping[str, Any],
    capacity_probe: Callable[[Mapping[str, Any]], Any] | None = None,
    arm_runner: Callable[..., Any] = adapter.run_episode_batch_arm,
    client_factory: Callable[[], Any] = adapter._client_factory,
    timeout_seconds: float | None = None,
    allowed_root: Path | None = None,
    environ: Mapping[str, str] | None = None,
    now: datetime | Callable[[], datetime] | None = None,
    offline_test_mode: bool = False,
) -> dict[str, Any]:
    """Execute or adopt the exact six extraction arms under one fresh epoch.

    No capacity probe is called until every checksum, semantic-plan budget, and
    extraction-only authority field has passed.  A denied probe never receives
    the semantic client factory.  Existing arms are adoptable only when both a
    complete report and a fully revalidated envelope exist; every other arm
    directory is terminal partial evidence and is never replayed.
    """

    _reject_external_auth_material(environ)
    current = _now_value(now)
    resolved_project = project_root.expanduser().resolve()
    root = _validated_output_root_path(output_root, project_root)
    lock_info = _ACTIVE_INVOCATION_LOCK.get()
    if (
        not isinstance(lock_info, Mapping)
        or lock_info.get("binding", {}).get("development_output_root") != str(root)
    ):
        raise CanonicalV31DevelopmentRuntimeError(
            "exclusive runtime lock is absent or bound to another root"
        )
    if offline_test_mode and not callable(capacity_probe):
        raise CanonicalV31DevelopmentRuntimeError(
            "offline fixture capacity probe is not callable"
        )
    if not offline_test_mode and capacity_probe is not None:
        raise CanonicalV31DevelopmentRuntimeError(
            "live mode accepts only the in-module trusted capacity provider"
        )
    if not callable(arm_runner) or not callable(client_factory):
        raise CanonicalV31DevelopmentRuntimeError(
            "semantic arm runner or client factory is not callable"
        )
    if timeout_seconds is not None:
        _positive_number(timeout_seconds, "adapter timeout_seconds")

    preflight = offline_matrix.build_matrix_dry_preflight(
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha256,
        episodes=episodes,
        capacity_policy_path=capacity_policy_path,
        capacity_policy_sha256=capacity_policy_sha256,
        allowed_root=(allowed_root or resolved_project),
    )
    precommit = offline_matrix.build_matrix_precommit(preflight)
    expected_future_plan = offline_matrix.build_future_plan_binding(
        precommit, output_root=root
    )
    if _canonical_json(future_plan_binding) != _canonical_json(expected_future_plan):
        raise CanonicalV31DevelopmentRuntimeError(
            "offline future-plan binding differs from exact preflight/precommit"
        )
    limits = _capacity_limits(preflight)
    effective_timeout_seconds = limits["maximum_wall_seconds_per_turn"]
    if timeout_seconds is not None:
        if float(timeout_seconds) > effective_timeout_seconds:
            raise CanonicalV31DevelopmentRuntimeError(
                "adapter timeout exceeds the exact directive per-turn wall cap"
            )
        effective_timeout_seconds = float(timeout_seconds)
    runtime_binding = build_runtime_dependency_binding()

    plan_path = semantic_plan_path.expanduser().resolve()
    try:
        semantic_plan = supervisor.read_semantic_plan(
            plan_path,
            thread_id=thread_id,
            project_root=resolved_project,
        )
    except Exception as exc:
        raise CanonicalV31DevelopmentRuntimeError(
            "supervisor semantic plan failed exact validation"
        ) from exc
    semantic_plan_payload = _load_object(plan_path, "supervisor semantic plan")
    directive, _directive_raw, directive_sha = _read_canonical_directive(semantic_plan)
    semantic_authority = _validate_semantic_epoch_authority(
        semantic_plan_path=plan_path,
        semantic_plan=semantic_plan,
        semantic_plan_payload=semantic_plan_payload,
        directive=directive,
        directive_sha256=directive_sha,
        preflight=preflight,
        precommit=precommit,
        future_plan_binding=future_plan_binding,
        runtime_binding=runtime_binding,
        output_root=root,
        limits=limits,
        now=current,
        offline_test_mode=offline_test_mode,
    )

    try:
        gate_reason, prior_step_receipt = supervisor.evaluate_semantic_plan(semantic_plan)
    except Exception as exc:
        raise CanonicalV31DevelopmentRuntimeError(
            "supervisor semantic plan evaluation failed"
        ) from exc
    if gate_reason is not None:
        raise CanonicalV31DevelopmentRuntimeError(
            f"supervisor semantic plan is not executable: {gate_reason}"
        )
    if prior_step_receipt is not None and not semantic_plan.expected_receipt_path.is_file():
        raise CanonicalV31RecoveryRequired(
            "semantic plan reports a receipt but the exact receipt is absent"
        )

    control_records = _initialize_runtime_root(
        root=root,
        preflight=preflight,
        precommit=precommit,
        runtime_binding=runtime_binding,
    )
    arms_root = root / "arms"
    capacity_root = root / "capacity"
    arm_specs = precommit["precommit"]["arms"]
    if [
        (row.get("batch_size"), row.get("thread_mode")) for row in arm_specs
    ] != list(EXPECTED_ARMS):
        raise CanonicalV31DevelopmentRuntimeError("six-arm precommit order drifted")
    states = [
        _arm_state(
            arms_root / str(arm["variant_id"]),
            capacity_root / str(arm["variant_id"]),
        )
        for arm in arm_specs
    ]
    absent_seen = False
    for state in states:
        if state == "absent":
            absent_seen = True
        elif absent_seen:
            raise CanonicalV31RecoveryRequired(
                "completed arms are not a contiguous prefix; replay is prohibited"
            )
    if prior_step_receipt is not None and any(state == "absent" for state in states):
        raise CanonicalV31RecoveryRequired(
            "terminal semantic receipt exists before all six arms"
        )

    all_turn_ids: set[str] = set()
    all_thread_ids: set[str] = set()
    all_measurement_ids: set[str] = set()
    all_admission_ids: set[str] = set()
    outputs_by_variant: dict[str, list[dict[str, Any]]] = {}
    usage_values: list[Mapping[str, Any]] = []
    total_turn_wall = 0.0
    used_calls = 0
    fresh_arms = 0
    adopted_arms = 0
    arm_receipts: list[dict[str, Any]] = []
    live_budget_state = {
        "used_calls": 0,
        "used_total_tokens": 0,
        "deadline_monotonic": time.monotonic()
        + float(limits["exact_total_wall_ceiling"]),
    }
    directive_expires_at = _parse_timestamp(
        directive.get("expires_at"), "semantic directive expires_at"
    )

    for arm, state in zip(arm_specs, states):
        _revalidate_epoch_sources(
            semantic_plan_path=plan_path,
            semantic_plan=semantic_plan,
            semantic_authority=semantic_authority,
        )
        variant_id = str(arm["variant_id"])
        arm_dir = arms_root / variant_id
        requests = preflight["requests_by_variant"].get(variant_id)
        if not isinstance(requests, list) or len(requests) != arm["request_count"]:
            raise CanonicalV31DevelopmentRuntimeError(
                "preflight requests are absent for the exact arm"
            )
        probe_context = _probe_context(
            arm=arm,
            preflight=preflight,
            precommit=precommit,
            future_plan_binding=future_plan_binding,
            directive_info=semantic_authority["directive_info"],
            runtime_binding=runtime_binding,
            limits=limits,
            used_calls=used_calls,
            used_total_tokens=sum(
                int(value["total_tokens"]) for value in usage_values
            ),
            offline_test_mode=offline_test_mode,
        )

        if state == "absent":
            if prior_step_receipt is not None:
                raise CanonicalV31RecoveryRequired(
                    "terminal semantic receipt forbids a fresh arm"
                )
            official_context: Any = None
            live_guard: _LiveCapacityGuardedClient | None = None
            capacity_reprobes: list[Mapping[str, Any]] = []
            try:
                if offline_test_mode:
                    measurement_value = await _maybe_await(
                        capacity_probe(probe_context)  # type: ignore[misc]
                    )
                    measurement, measurement_sha = _capacity_bundle(
                        measurement_value
                    )
                    capacity_request = {
                        "schema_version": "pif_canonical_v31_capacity_probe_request_v1",
                        "sequence": 0,
                        "boundary": "initial_arm_admission",
                        "provider_method": "offline_fixture_probe",
                        "context": copy.deepcopy(probe_context),
                        "official_app_server_initialized_before_capacity": True,
                        "semantic_thread_started": False,
                        "semantic_thread_resumed": False,
                        "semantic_turn_started": False,
                        "offline_test_mode": True,
                        "live_capacity_authority": False,
                        "capacity_estimate_not_reservation": False,
                        "promotable": False,
                    }
                else:
                    official_context = client_factory()
                    official_client = await official_context.__aenter__()
                    account = getattr(official_client, "account_summary", None)
                    if (
                        not isinstance(account, Mapping)
                        or account.get("type") != "chatgpt"
                        or account.get("plan_type") != "pro"
                    ):
                        raise CanonicalV31DevelopmentRuntimeError(
                            "trusted live capacity requires official managed ChatGPT Pro auth"
                        )
                    live_budget_state["used_calls"] = used_calls
                    live_budget_state["used_total_tokens"] = sum(
                        int(value["total_tokens"]) for value in usage_values
                    )

                    def live_context() -> Mapping[str, Any]:
                        completed_in_arm = int(live_budget_state["used_calls"]) - used_calls
                        return _probe_context(
                            arm=arm,
                            preflight=preflight,
                            precommit=precommit,
                            future_plan_binding=future_plan_binding,
                            directive_info=semantic_authority["directive_info"],
                            runtime_binding=runtime_binding,
                            limits=limits,
                            used_calls=int(live_budget_state["used_calls"]),
                            used_total_tokens=int(
                                live_budget_state["used_total_tokens"]
                            ),
                            offline_test_mode=False,
                            remaining_arm_calls=(
                                int(arm["request_count"]) - completed_in_arm
                            ),
                        )

                    live_guard = _LiveCapacityGuardedClient(
                        inner=official_client,
                        limits=limits,
                        directive_expires_at=directive_expires_at,
                        context_factory=live_context,
                        budget_state=live_budget_state,
                    )
                    initial_probe = await live_guard.initial_probe()
                    capacity_request = initial_probe["request"]
                    measurement, measurement_sha = _capacity_bundle(
                        initial_probe["measurement"]
                    )
                    probe_context = copy.deepcopy(dict(live_context()))

                capacity_request = _validate_capacity_probe_request(
                    capacity_request,
                    expected_context=probe_context,
                    expected_sequence=0,
                    expected_boundary="initial_arm_admission",
                    expected_semantic_thread_started=False,
                    expected_semantic_thread_resumed=False,
                )
                measurement_info = validate_capacity_measurement(
                    measurement,
                    expected_context=probe_context,
                    expected_sha256=measurement_sha,
                    now=_now_value(now),
                    limits=limits,
                )
                if measurement_info["measurement_id"] in all_measurement_ids:
                    raise CanonicalV31DevelopmentRuntimeError(
                        "capacity measurement was replayed across arms"
                    )
                admission_info = _admission_from_measurement(
                    measurement_info=measurement_info,
                    arm=arm,
                    preflight=preflight,
                    precommit=precommit,
                    future_plan_binding=future_plan_binding,
                    directive_info=semantic_authority["directive_info"],
                    runtime_binding=runtime_binding,
                    limits=limits,
                )
                validated_admission = offline_matrix.validate_capacity_admission(
                    admission_info["admission"],
                    expected_sha256=admission_info["admission_sha256"],
                    plan_binding=future_plan_binding,
                    directive_info=semantic_authority["directive_info"],
                    precommit_bundle=precommit,
                    variant_id=variant_id,
                    now=_now_value(now),
                )
                admission_info["admission"] = validated_admission["admission"]
                admission_info["admission_sha256"] = validated_admission[
                    "admission_sha256"
                ]
                if admission_info["admission_id"] in all_admission_ids:
                    raise CanonicalV31DevelopmentRuntimeError(
                        "capacity admission was replayed across arms"
                    )
                evidence_root = (
                    capacity_root
                    / variant_id
                    / _sha256_bytes(
                        measurement_info["measurement_id"].encode("utf-8")
                    )
                )
                capacity_request_record = _write_or_verify_json(
                    evidence_root / "request.json", capacity_request
                )
                measurement_record = _write_or_verify_json(
                    evidence_root / "measurement.json",
                    measurement_info["measurement"],
                )
                admission_record = _write_or_verify_json(
                    evidence_root / "admission.json", admission_info["admission"]
                )
                if live_guard is not None:
                    live_guard.activate_admission(evidence_root=evidence_root)
                _revalidate_epoch_sources(
                    semantic_plan_path=plan_path,
                    semantic_plan=semantic_plan,
                    semantic_authority=semantic_authority,
                )
                effective_client_factory = (
                    (lambda: _BorrowedClientContext(live_guard))
                    if live_guard is not None
                    else client_factory
                )
                arm_value = arm_runner(
                    episodes,
                    output_dir=arm_dir,
                    batch_size=arm["batch_size"],
                    thread_mode=arm["thread_mode"],
                    timeout_seconds=effective_timeout_seconds,
                    client_factory=effective_client_factory,
                )
                report_value = (
                    await asyncio.wait_for(
                        arm_value,
                        timeout=(
                            arm["request_count"]
                            * limits["maximum_wall_seconds_per_turn"]
                        ),
                    )
                    if inspect.isawaitable(arm_value)
                    else arm_value
                )
                if live_guard is not None:
                    capacity_reprobes = copy.deepcopy(live_guard.reprobe_records)
            except asyncio.TimeoutError as exc:
                raise CanonicalV31DevelopmentRuntimeError(
                    "arm dispatch exceeded its exact directive wall ceiling"
                ) from exc
            finally:
                if official_context is not None:
                    await official_context.__aexit__(None, None, None)
            if not isinstance(report_value, Mapping):
                raise CanonicalV31DevelopmentRuntimeError(
                    f"{variant_id} adapter returned no report"
                )
            report = copy.deepcopy(dict(report_value))
            report_path = arm_dir / "report.json"
            if not report_path.is_file() or _load_object(
                report_path, f"{variant_id} adapter report"
            ) != report:
                raise CanonicalV31DevelopmentRuntimeError(
                    "returned adapter report differs from immutable report artifact"
                )
            _validate_arm_directory_contents(
                arm_dir, requests, envelope_required=False
            )
            loaded = load_full_canonical_arm_outputs(
                arm_dir=arm_dir,
                report=report,
                requests=requests,
                request_sha256s=arm["request_sha256s"],
                manifest_info=preflight["manifest_info"],
            )
            if live_guard is not None:
                expected_guard_calls = used_calls + int(
                    loaded["verified_attempt_count"]
                )
                expected_guard_tokens = sum(
                    int(value["total_tokens"]) for value in usage_values
                ) + int(loaded["verified_usage"]["total_tokens"])
                if (
                    int(live_budget_state["used_calls"]) != expected_guard_calls
                    or int(live_budget_state["used_total_tokens"])
                    != expected_guard_tokens
                ):
                    raise CanonicalV31DevelopmentRuntimeError(
                        "live postturn measured budget differs from verified artifacts"
                    )
            reprobe_measurement_ids = _validate_capacity_reprobes(
                capacity_reprobes,
                limits=limits,
                offline_test_mode=offline_test_mode,
            )
            _validate_capacity_reprobe_coverage(
                capacity_reprobes,
                artifact_validation=loaded,
                offline_test_mode=offline_test_mode,
            )
            envelope = _arm_envelope_payload(
                arm=arm,
                report=report,
                outputs=loaded,
                measurement_info=measurement_info,
                capacity_request_record=capacity_request_record,
                measurement_record=measurement_record,
                admission_info=admission_info,
                admission_record=admission_record,
                preflight=preflight,
                precommit=precommit,
                future_plan_binding=future_plan_binding,
                directive_info=semantic_authority["directive_info"],
                semantic_authority=semantic_authority,
                runtime_binding=runtime_binding,
                capacity_reprobes=capacity_reprobes,
            )
            envelope_sha = _sha256_bytes(_canonical_json(envelope).encode("ascii"))
            envelope_info = offline_matrix.validate_arm_envelope(
                envelope,
                expected_sha256=envelope_sha,
                precommit_bundle=precommit,
                plan_binding=future_plan_binding,
                directive_info=semantic_authority["directive_info"],
                admission_info=admission_info,
                manifest_info=preflight["manifest_info"],
            )
            envelope_record = _write_or_verify_json(
                arm_dir / "arm-envelope.json", envelope
            )
            _validate_arm_directory_contents(
                arm_dir, requests, envelope_required=True
            )
            fresh_arms += 1
        else:
            report_path = arm_dir / "report.json"
            envelope_path = arm_dir / "arm-envelope.json"
            report = _load_object(report_path, f"{variant_id} adopted report")
            envelope = _load_object(envelope_path, f"{variant_id} adopted envelope")
            _validate_arm_directory_contents(
                arm_dir, requests, envelope_required=True
            )
            (
                measurement,
                admission,
                prior_artifact_validation,
                capacity_records,
            ) = _validate_envelope_runtime_extensions(
                envelope,
                semantic_authority=semantic_authority,
                runtime_binding=runtime_binding,
                arm_root=arm_dir,
                capacity_root=capacity_root,
            )
            measurement_sha = _sha256_bytes(_canonical_json(measurement).encode("ascii"))
            capacity_request = _validate_capacity_probe_request(
                capacity_records["request"],
                expected_context=probe_context,
                expected_sequence=0,
                expected_boundary="initial_arm_admission",
                expected_semantic_thread_started=False,
                expected_semantic_thread_resumed=False,
            )
            measurement_info = validate_capacity_measurement(
                measurement,
                expected_context=probe_context,
                expected_sha256=measurement_sha,
                now=_historical_validation_time(admission),
                historical=True,
                limits=limits,
            )
            admission_sha = _sha256_bytes(_canonical_json(admission).encode("ascii"))
            admission_id = admission.get("admission_id")
            if not isinstance(admission_id, str) or not admission_id:
                raise CanonicalV31RecoveryRequired(
                    "adopted capacity admission has no unique identity"
                )
            admission_info = {
                "admission": admission,
                "admission_sha256": admission_sha,
                "admission_id": admission_id,
            }
            validated_admission = offline_matrix.validate_capacity_admission(
                admission,
                expected_sha256=admission_sha,
                plan_binding=future_plan_binding,
                directive_info=semantic_authority["directive_info"],
                precommit_bundle=precommit,
                variant_id=variant_id,
                now=_historical_validation_time(admission),
            )
            admission_info["admission"] = validated_admission["admission"]
            loaded = load_full_canonical_arm_outputs(
                arm_dir=arm_dir,
                report=report,
                requests=requests,
                request_sha256s=arm["request_sha256s"],
                manifest_info=preflight["manifest_info"],
            )
            if prior_artifact_validation != loaded:
                raise CanonicalV31RecoveryRequired(
                    "adopted arm artifact validation receipt drifted"
                )
            reprobe_measurement_ids = _validate_capacity_reprobes(
                capacity_records["reprobes"],
                limits=limits,
                offline_test_mode=offline_test_mode,
            )
            _validate_capacity_reprobe_coverage(
                capacity_records["reprobes"],
                artifact_validation=loaded,
                offline_test_mode=offline_test_mode,
            )
            envelope_sha = _sha256_bytes(_canonical_json(envelope).encode("ascii"))
            envelope_info = offline_matrix.validate_arm_envelope(
                envelope,
                expected_sha256=envelope_sha,
                precommit_bundle=precommit,
                plan_binding=future_plan_binding,
                directive_info=semantic_authority["directive_info"],
                admission_info=admission_info,
                manifest_info=preflight["manifest_info"],
            )
            measurement_record = capacity_records["measurement_record"]
            admission_record = capacity_records["admission_record"]
            capacity_request_record = capacity_records["request_record"]
            capacity_reprobes = capacity_records["reprobes"]
            envelope_record = _record(envelope_path)
            adopted_arms += 1

        arm_measurement_ids = {
            str(measurement_info["measurement_id"]),
            *reprobe_measurement_ids,
        }
        if (
            len(arm_measurement_ids) != 1 + len(reprobe_measurement_ids)
            or all_measurement_ids & arm_measurement_ids
        ):
            raise CanonicalV31DevelopmentRuntimeError(
                "capacity measurement was replayed across arms"
            )
        if admission_info["admission_id"] in all_admission_ids:
            raise CanonicalV31DevelopmentRuntimeError(
                "capacity admission was replayed across arms"
            )
        all_measurement_ids.update(arm_measurement_ids)
        all_admission_ids.add(admission_info["admission_id"])

        cap_accounting = _validate_runtime_report_caps(
            report,
            arm=arm,
            limits=limits,
            artifact_validation=loaded,
        )
        result_rows = loaded["verified_turns"]
        turn_ids = {row["turn_id"] for row in result_rows}
        thread_ids = {row["thread_id"] for row in result_rows}
        if all_turn_ids & turn_ids or all_thread_ids & thread_ids:
            raise CanonicalV31DevelopmentRuntimeError(
                "thread or turn identity was replayed across arms"
            )
        all_turn_ids.update(turn_ids)
        all_thread_ids.update(thread_ids)
        used_calls += int(loaded["verified_attempt_count"])
        usage_values.append(cap_accounting["usage"])
        total_turn_wall += cap_accounting["turn_wall_elapsed_seconds"]
        if (
            used_calls > limits["exact_model_call_cap"]
            or sum(int(value["total_tokens"]) for value in usage_values)
            > limits["exact_total_token_cap"]
        ):
            raise CanonicalV31DevelopmentRuntimeError(
                "global semantic-plan call or token budget was exceeded"
            )
        outputs_by_variant[variant_id] = copy.deepcopy(envelope["outputs"])
        arm_receipts.append(
            {
                "variant_id": variant_id,
                "batch_size": arm["batch_size"],
                "thread_mode": arm["thread_mode"],
                "request_count": arm["request_count"],
                "request_set_sha256": arm["request_set_sha256"],
                "capacity_request": copy.deepcopy(
                    dict(capacity_request_record)
                ),
                "capacity_measurement": copy.deepcopy(dict(measurement_record)),
                "capacity_admission": copy.deepcopy(dict(admission_record)),
                "capacity_reprobes": copy.deepcopy(list(capacity_reprobes)),
                "offline_test_mode": offline_test_mode,
                "report": _record(arm_dir / "report.json"),
                "arm_envelope": copy.deepcopy(dict(envelope_record)),
                "arm_envelope_sha256": envelope_info["envelope_sha256"],
                "outputs_sha256": loaded["outputs_sha256"],
                "usage": cap_accounting["usage"],
                "verified_attempt_count": loaded["verified_attempt_count"],
                "verified_turn_ids": [
                    row["turn_id"] for row in loaded["verified_turns"]
                ],
            }
        )

    if used_calls != limits["exact_model_call_cap"]:
        raise CanonicalV31DevelopmentRuntimeError(
            "completed matrix did not consume the exact preflight call surface"
        )
    full_outputs = offline_matrix.validate_full_v31_outputs(
        outputs_by_variant, manifest_info=preflight["manifest_info"]
    )
    total_usage = _sum_usage(usage_values)
    _revalidate_epoch_sources(
        semantic_plan_path=plan_path,
        semantic_plan=semantic_plan,
        semantic_authority=semantic_authority,
    )
    static_receipt = _receipt_static_fields(
        semantic_plan=semantic_plan,
        semantic_authority=semantic_authority,
        preflight=preflight,
        precommit=precommit,
        future_plan_binding=future_plan_binding,
        runtime_binding=runtime_binding,
        limits=limits,
        usage=total_usage,
        wall_seconds=total_turn_wall,
        full_output_validation=full_outputs,
        arm_receipts=arm_receipts,
        verified_attempt_count=used_calls,
        offline_test_mode=offline_test_mode,
    )
    receipt_path = root / EXTRACTION_RECEIPT_FILENAME
    if receipt_path.exists():
        receipt = _validate_existing_receipt(
            _load_object(receipt_path, "extraction-only receipt"),
            static=static_receipt,
        )
    else:
        receipt = {
            **static_receipt,
            "completed_at": _now_value(now).isoformat(),
        }
        _write_or_verify_json(receipt_path, receipt)
    _revalidate_epoch_sources(
        semantic_plan_path=plan_path,
        semantic_plan=semantic_plan,
        semantic_authority=semantic_authority,
    )
    receipt_record = _record(receipt_path)
    return {
        "receipt": receipt,
        "receipt_record": receipt_record,
        "runtime_control_records": control_records,
        "fresh_arm_count": fresh_arms,
        "adopted_arm_count": adopted_arms,
        "semantic_calls_started_by_this_invocation": (
            sum(
                int(arm["request_count"])
                for arm, state in zip(arm_specs, states)
                if state == "absent"
            )
            if not offline_test_mode
            else 0
        ),
        "fixture_attempts_started_by_this_invocation": (
            sum(
                int(arm["request_count"])
                for arm, state in zip(arm_specs, states)
                if state == "absent"
            )
            if offline_test_mode
            else 0
        ),
    }


# A concise discoverable alias for callers that do not need the versioned name.
run_live_development_extraction = run_canonical_v31_development_matrix_runtime
