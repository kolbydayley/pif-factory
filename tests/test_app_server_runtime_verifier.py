from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import app_server_runtime_verifier as verifier


def _write(path: Path, value: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")
    return path


def _record(cache: verifier.ContentHashCache, path: Path) -> dict:
    return cache.record(path)


def _fixture(tmp_path: Path) -> tuple[Path, dict[str, Path]]:
    cache = verifier.ContentHashCache()
    files = {
        "prompt": _write(tmp_path / "prompt.private.md", "frozen prompt\n"),
        "schema": _write(tmp_path / "schema.json", '{"type":"object"}\n'),
        "runtime": _write(tmp_path / "runtime.py", "VALUE = 1\n"),
        "binary": _write(tmp_path / "codex", "synthetic pinned binary\n"),
    }
    predecessor_leaf = _write(tmp_path / "predecessor-prompt.md", "prior prompt\n")
    predecessor = tmp_path / "predecessor-runtime-lock.json"
    predecessor.write_text(
        json.dumps(
            {
                "schema_version": "prior_v1",
                "runtime_files": [_record(cache, predecessor_leaf)],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    files["predecessor"] = predecessor
    lock = tmp_path / "runtime-lock.json"
    lock.write_text(
        json.dumps(
            {
                "schema_version": "current_v1",
                "phase_id": "test",
                "pinned_codex_cli": _record(cache, files["binary"]),
                "runtime_files": [_record(cache, files["runtime"])],
                "prompt": _record(cache, files["prompt"]),
                "schema": _record(cache, files["schema"]),
                "predecessor": _record(cache, predecessor),
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return lock, files


def test_content_hash_is_cached_by_verified_stat_identity(tmp_path: Path) -> None:
    cache = verifier.ContentHashCache()
    path = _write(tmp_path / "large.bin", "x" * 1_000_000)
    first = cache.record(path)
    second = cache.record(path)
    assert first == second
    assert cache.content_hash_reads == 1
    assert cache.cache_entries == 1


@pytest.mark.parametrize("name", ["prompt", "schema", "runtime", "binary"])
def test_direct_file_mutation_fails_closed(tmp_path: Path, name: str) -> None:
    lock, files = _fixture(tmp_path)
    cache = verifier.ContentHashCache()
    verified = verifier.verify_runtime_lock(
        lock,
        cache=cache,
        required_fields={"schema_version": "current_v1", "phase_id": "test"},
    )
    files[name].write_text(files[name].read_text(encoding="utf-8") + "drift\n", encoding="utf-8")
    with pytest.raises(verifier.RuntimeVerificationError):
        verifier.verify_runtime_lock(
            lock,
            cache=cache,
            expected_closure_digest=verified.closure_digest,
        )


def test_predecessor_manifest_mutation_fails_closed(tmp_path: Path) -> None:
    lock, files = _fixture(tmp_path)
    cache = verifier.ContentHashCache()
    verifier.verify_runtime_lock(lock, cache=cache)
    files["predecessor"].write_text(
        files["predecessor"].read_text(encoding="utf-8") + " ", encoding="utf-8"
    )
    with pytest.raises(verifier.RuntimeVerificationError):
        verifier.verify_runtime_lock(lock, cache=cache)


def test_manifest_and_closure_expectations_are_enforced(tmp_path: Path) -> None:
    lock, _files = _fixture(tmp_path)
    cache = verifier.ContentHashCache()
    result = verifier.verify_runtime_lock(lock, cache=cache)
    repeated = verifier.verify_runtime_lock(
        lock,
        cache=cache,
        expected_manifest_record=result.manifest_record,
        expected_closure_digest=result.closure_digest,
    )
    assert repeated.closure_digest == result.closure_digest
    assert repeated.content_hash_reads == result.content_hash_reads


def test_actual_pinned_codex_binary_is_hashed_once() -> None:
    path = (
        Path.home()
        / ".codex/packages/standalone/releases/0.144.1-aarch64-apple-darwin"
        / "bin/codex"
    )
    cache = verifier.ContentHashCache()
    first = cache.record(path)
    second = cache.record(path)
    assert first == second
    assert first["size_bytes"] > 250_000_000
    assert cache.content_hash_reads == 1
