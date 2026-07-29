from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

from research_factory import app_server_typed_event_set_capacity_timeout_recovery as recovery


def test_capacity_timeout_classification_is_exact() -> None:
    message = "app-server request timed out: account/rateLimits/read"
    assert len(message.encode()) == 53
    assert hashlib.sha256(message.encode()).hexdigest() == recovery.CAPACITY_TIMEOUT_SHA256
    assert recovery.CAPACITY_TIMEOUT_SHA256 == (
        "3f0e8d134186216ac804645bee76955387985d9ad287d696f69fec175898d6f5"
    )


def test_live_predecessor_is_presemantic_and_probe_recovered() -> None:
    paths = recovery._predecessor_paths()  # noqa: SLF001
    recovery._validate_predecessor(paths)  # noqa: SLF001

    terminal = json.loads(paths["predecessor_terminal"].read_text())
    probe = json.loads(paths["live_capacity_probe"].read_text())
    turn = recovery._turn_paths(recovery.PREDECESSOR_ROOT)  # noqa: SLF001
    assert terminal["semantic_attempt_count"] == 0
    assert not turn["capacity"].exists()
    assert not turn["sidecar"].exists()
    assert not turn["output"].exists()
    assert probe["cleared_for_semantic_work"] is True
    assert probe["thread_started"] is False
    assert probe["turn_started"] is False


def test_inner_client_uses_extended_request_timeout() -> None:
    client = recovery._inner_factory()  # noqa: SLF001
    assert client.request_timeout_seconds == 90.0


def test_existing_terminal_prevents_replay(tmp_path: Path) -> None:
    root = tmp_path / "recovery"
    root.mkdir()
    terminal = {
        "terminal_reason": "infrastructure_or_extraction_attempt_failed",
        "support_alignment_authorized": False,
    }
    (root / "terminal.json").write_text(json.dumps(terminal), encoding="utf-8")

    result = asyncio.run(recovery.run_recovery(root / "experiment-config.json"))

    assert result == terminal
