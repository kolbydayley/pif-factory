from __future__ import annotations

"""Fast fail-closed verification for immutable app-server runtime locks."""

import contextlib
import hashlib
import inspect
import json
import stat
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Iterator, Mapping, Sequence


RECORD_KEYS = frozenset({"path", "sha256", "size_bytes"})
READ_CHUNK_BYTES = 4 * 1024 * 1024


class RuntimeVerificationError(RuntimeError):
    """A runtime lock, closure, or pinned file failed verification."""


@dataclass(frozen=True)
class StatIdentity:
    path: str
    device: int
    inode: int
    size: int
    mtime_ns: int


@dataclass(frozen=True)
class VerificationResult:
    manifest: dict[str, Any]
    manifest_record: dict[str, Any]
    closure_digest: str
    direct_record_count: int
    content_hash_reads: int
    cache_entries: int


class ContentHashCache:
    """Cache a content digest only while a file's verified stat identity is stable."""

    def __init__(self) -> None:
        self._digests: dict[StatIdentity, str] = {}
        self._lock = threading.RLock()
        self._content_hash_reads = 0

    @property
    def content_hash_reads(self) -> int:
        with self._lock:
            return self._content_hash_reads

    @property
    def cache_entries(self) -> int:
        with self._lock:
            return len(self._digests)

    @staticmethod
    def identity(path: Path) -> StatIdentity:
        resolved = path.expanduser().resolve(strict=True)
        observed = resolved.stat()
        if not stat.S_ISREG(observed.st_mode):
            raise RuntimeVerificationError(f"runtime record is not a regular file: {resolved}")
        return StatIdentity(
            path=str(resolved),
            device=int(observed.st_dev),
            inode=int(observed.st_ino),
            size=int(observed.st_size),
            mtime_ns=int(observed.st_mtime_ns),
        )

    def digest(self, path: Path) -> tuple[StatIdentity, str]:
        before = self.identity(path)
        with self._lock:
            cached = self._digests.get(before)
        if cached is not None:
            return before, cached

        hasher = hashlib.sha256()
        with Path(before.path).open("rb") as handle:
            while True:
                chunk = handle.read(READ_CHUNK_BYTES)
                if not chunk:
                    break
                hasher.update(chunk)
        after = self.identity(Path(before.path))
        if after != before:
            raise RuntimeVerificationError(f"runtime file changed while hashing: {before.path}")
        digest = hasher.hexdigest()
        with self._lock:
            prior = self._digests.get(before)
            if prior is None:
                self._digests[before] = digest
                self._content_hash_reads += 1
            elif prior != digest:
                raise RuntimeVerificationError(f"cached runtime digest conflict: {before.path}")
        return before, digest

    def record(self, path: Path) -> dict[str, Any]:
        identity, digest = self.digest(path)
        return {
            "path": identity.path,
            "sha256": digest,
            "size_bytes": identity.size,
        }

    def verify_record(self, record: Mapping[str, Any]) -> bool:
        try:
            normalized = normalize_record(record)
            return self.record(Path(normalized["path"])) == normalized
        except (OSError, RuntimeVerificationError, TypeError, ValueError):
            return False


GLOBAL_CONTENT_HASH_CACHE = ContentHashCache()


def normalize_record(record: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(record, Mapping) or not RECORD_KEYS.issubset(record):
        raise RuntimeVerificationError("runtime record is malformed")
    path = record.get("path")
    digest = record.get("sha256")
    size = record.get("size_bytes")
    if (
        not isinstance(path, str)
        or not path
        or not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        raise RuntimeVerificationError("runtime record fields are malformed")
    return {
        "path": str(Path(path).expanduser().resolve()),
        "sha256": digest,
        "size_bytes": size,
    }


def manifest_records(value: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []

    def visit(node: Any) -> None:
        if isinstance(node, Mapping):
            if RECORD_KEYS.issubset(node):
                found.append(normalize_record(node))
                return
            for child in node.values():
                visit(child)
        elif isinstance(node, Sequence) and not isinstance(node, (str, bytes, bytearray)):
            for child in node:
                visit(child)

    visit(value)
    by_path: dict[str, dict[str, Any]] = {}
    for record in found:
        prior = by_path.setdefault(record["path"], record)
        if prior != record:
            raise RuntimeVerificationError(
                f"runtime manifest has conflicting records: {record['path']}"
            )
    return [by_path[path] for path in sorted(by_path)]


def _load_manifest(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeVerificationError(f"cannot read runtime manifest: {path}") from exc
    if not isinstance(value, dict):
        raise RuntimeVerificationError(f"runtime manifest is not an object: {path}")
    return value


def _is_runtime_manifest(path: Path) -> bool:
    return path.suffix == ".json" and "runtime-lock" in path.name


def _metadata_closure_digest(
    manifest_path: Path,
    *,
    cache: ContentHashCache,
    memo: dict[tuple[str, str], str],
    active: set[str],
) -> str:
    manifest_record = cache.record(manifest_path)
    memo_key = (manifest_record["path"], manifest_record["sha256"])
    if memo_key in memo:
        return memo[memo_key]
    if manifest_record["path"] in active:
        raise RuntimeVerificationError("runtime manifest closure has a cycle")
    active.add(manifest_record["path"])
    manifest = _load_manifest(Path(manifest_record["path"]))
    records = manifest_records(manifest)
    child_closures = []
    for record in records:
        child = Path(record["path"])
        if child.resolve() == Path(manifest_record["path"]):
            continue
        if _is_runtime_manifest(child):
            if not cache.verify_record(record):
                raise RuntimeVerificationError(
                    f"pinned predecessor manifest drifted: {record['path']}"
                )
            child_closures.append(
                {
                    "manifest": record,
                    "closure_digest": _metadata_closure_digest(
                        child, cache=cache, memo=memo, active=active
                    ),
                }
            )
    payload = {
        "manifest": manifest_record,
        "records": records,
        "predecessor_closures": sorted(
            child_closures, key=lambda row: row["manifest"]["path"]
        ),
    }
    digest = hashlib.sha256(
        json.dumps(
            payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    active.remove(manifest_record["path"])
    memo[memo_key] = digest
    return digest


def verify_runtime_lock(
    path: Path,
    *,
    cache: ContentHashCache = GLOBAL_CONTENT_HASH_CACHE,
    expected_manifest_record: Mapping[str, Any] | None = None,
    expected_closure_digest: str | None = None,
    required_fields: Mapping[str, Any] | None = None,
    required_record_paths: Sequence[Path] = (),
) -> VerificationResult:
    manifest_path = path.expanduser().resolve(strict=True)
    manifest_record = cache.record(manifest_path)
    if expected_manifest_record is not None and manifest_record != normalize_record(
        expected_manifest_record
    ):
        raise RuntimeVerificationError("runtime manifest digest or stat record drifted")
    manifest = _load_manifest(manifest_path)
    for key, expected in (required_fields or {}).items():
        if manifest.get(key) != expected:
            raise RuntimeVerificationError(f"runtime manifest field drifted: {key}")
    records = manifest_records(manifest)
    required = {str(item.expanduser().resolve()) for item in required_record_paths}
    actual = {record["path"] for record in records}
    if not required.issubset(actual):
        raise RuntimeVerificationError("runtime manifest required record coverage drifted")
    for record in records:
        if not cache.verify_record(record):
            raise RuntimeVerificationError(f"immutable runtime record drifted: {record['path']}")
    closure_digest = _metadata_closure_digest(
        manifest_path, cache=cache, memo={}, active=set()
    )
    if expected_closure_digest is not None and closure_digest != expected_closure_digest:
        raise RuntimeVerificationError("runtime metadata closure digest drifted")
    return VerificationResult(
        manifest=manifest,
        manifest_record=manifest_record,
        closure_digest=closure_digest,
        direct_record_count=len(records),
        content_hash_reads=cache.content_hash_reads,
        cache_entries=cache.cache_entries,
    )


def module_lock_required_fields(module: ModuleType) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for attribute, key in (
        ("RUNTIME_LOCK_VERSION", "schema_version"),
        ("PHASE_ID", "phase_id"),
        ("MODEL", "model"),
        ("EFFORT", "effort"),
    ):
        if hasattr(module, attribute):
            fields[key] = getattr(module, attribute)
    return fields


def verify_module_runtime_lock(
    module: ModuleType,
    path: Path,
    *,
    cache: ContentHashCache = GLOBAL_CONTENT_HASH_CACHE,
) -> dict[str, Any]:
    result = verify_runtime_lock(
        path,
        cache=cache,
        required_fields=module_lock_required_fields(module),
    )
    lock = result.manifest
    if hasattr(module, "PINNED_CODEX_0_144_1"):
        expected_cli = cache.record(Path(getattr(module, "PINNED_CODEX_0_144_1")))
        if lock.get("pinned_codex_cli") != expected_cli:
            raise RuntimeVerificationError("pinned Codex CLI record drifted")
    runtime_files = getattr(module, "_runtime_files", None)
    if callable(runtime_files) and "runtime_files" in lock:
        expected_paths = {str(Path(item).expanduser().resolve()) for item in runtime_files()}
        actual_paths = {
            normalize_record(item)["path"] for item in lock.get("runtime_files") or []
        }
        if actual_paths != expected_paths:
            raise RuntimeVerificationError("runtime file coverage drifted")
    return lock


@contextlib.contextmanager
def install_fast_module_verifiers(
    *,
    cache: ContentHashCache = GLOBAL_CONTENT_HASH_CACHE,
    module_prefix: str = "research_factory.app_server",
) -> Iterator[None]:
    """Temporarily route loaded app-server modules through the shared verifier."""

    originals: list[tuple[ModuleType, str, Any]] = []
    modules = [
        module
        for name, module in list(sys.modules.items())
        if name.startswith(module_prefix) and isinstance(module, ModuleType)
    ]
    for module in modules:
        record = getattr(module, "_record", None)
        if callable(record):
            try:
                parameter_count = len(inspect.signature(record).parameters)
            except (TypeError, ValueError):
                parameter_count = -1
            if parameter_count == 1:
                originals.append((module, "_record", record))
                setattr(module, "_record", cache.record)
        verify_record = getattr(module, "_verify_record", None)
        if callable(verify_record):
            try:
                parameter_count = len(inspect.signature(verify_record).parameters)
            except (TypeError, ValueError):
                parameter_count = -1
            if parameter_count == 1:
                originals.append((module, "_verify_record", verify_record))
                setattr(module, "_verify_record", cache.verify_record)
        # Historical lock schemas enforce version-specific invariants in their
        # own verifier. Cache their file-record reads, but keep those checks.
    try:
        yield
    finally:
        for module, attribute, original in reversed(originals):
            setattr(module, attribute, original)


def closure_receipt(
    path: Path,
    *,
    cache: ContentHashCache = GLOBAL_CONTENT_HASH_CACHE,
) -> dict[str, Any]:
    result = verify_runtime_lock(path, cache=cache)
    return {
        "schema_version": "pif_app_server_runtime_verifier_receipt_v1",
        "manifest": result.manifest_record,
        "closure_digest": result.closure_digest,
        "direct_record_count": result.direct_record_count,
        "content_hash_reads": result.content_hash_reads,
        "cache_entries": result.cache_entries,
    }
