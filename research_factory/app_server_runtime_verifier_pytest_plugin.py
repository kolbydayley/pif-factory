from __future__ import annotations

"""Pytest plugin for benchmarking unchanged numbered suites with the fast verifier."""

from typing import Any

from .app_server_runtime_verifier import (
    ContentHashCache,
    install_fast_module_verifiers,
)


_CACHE = ContentHashCache()
_CONTEXT: Any = None


def pytest_collection_finish(session: Any) -> None:
    del session
    global _CONTEXT
    _CONTEXT = install_fast_module_verifiers(cache=_CACHE)
    _CONTEXT.__enter__()


def pytest_unconfigure(config: Any) -> None:
    del config
    global _CONTEXT
    if _CONTEXT is not None:
        _CONTEXT.__exit__(None, None, None)
        _CONTEXT = None


def pytest_terminal_summary(terminalreporter: Any) -> None:
    terminalreporter.write_line(
        "runtime-verifier: content_hash_reads=%d cache_entries=%d"
        % (_CACHE.content_hash_reads, _CACHE.cache_entries)
    )
