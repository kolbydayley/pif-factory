from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

from research_factory.signal_desk_rebuild_dispatch import (
    enqueue_task,
    get_task,
    initialize_dispatch_schema,
)
from research_factory.signal_desk_rebuild_fleet import (
    GLOBAL_GLM_CEILING,
    LANES,
    FleetRunner,
)


def database(tmp_path: Path) -> tuple[Path, callable]:
    path = tmp_path / "dispatch.sqlite"
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    initialize_dispatch_schema(conn)
    conn.close()

    def connect() -> sqlite3.Connection:
        return sqlite3.connect(path, timeout=10)

    return path, connect


def enqueue(path: Path, key: str, lane: str, prompt: str = "Extract this.") -> None:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    enqueue_task(
        conn,
        task_key=key,
        task_type="extract_window",
        payload={"lane": lane, "prompt": prompt},
    )
    conn.close()


def successful(label=None):
    def adapter(prompt, lane):
        return {
            "ok": True,
            "label": label if label is not None else {"claims": [], "lane": lane.name},
            "calls": 1,
        }

    return adapter


def test_lane_limits_are_12_3_8_and_global_23() -> None:
    assert LANES["glm-zai"].max_concurrency == 12
    assert LANES["glm"].max_concurrency == 3
    assert LANES["glm-zai-flash"].max_concurrency == 8
    assert sum(lane.max_concurrency for lane in LANES.values()) == 23
    assert GLOBAL_GLM_CEILING == 23


def test_success_completes_lease_with_deterministic_receipt(tmp_path) -> None:
    hashes = []
    for suffix in ("a", "b"):
        run_root = tmp_path / suffix
        run_root.mkdir()
        path, connect = database(run_root)
        enqueue(path, "window:1", "glm-zai", "same prompt")
        runner = FleetRunner(
            connection_factory=connect,
            adapters={"glm-zai": successful({"claims": [{"claim": "x"}]})},
        )
        report = runner.run_until_idle()
        conn = connect()
        conn.row_factory = sqlite3.Row
        task = get_task(conn, "window:1")
        output = json.loads(
            conn.execute(
                "SELECT output_json FROM signal_desk_rebuild_attempts WHERE id = ?",
                (task["current_attempt_id"],),
            ).fetchone()[0]
        )
        conn.close()
        hashes.append(output["receipt"]["receipt_sha256"])
        assert report["counts"]["succeeded"] == 1
        assert task["status"] == "succeeded"
        assert output["receipt"]["lane"] == "glm-zai"
    assert hashes[0] == hashes[1]


def test_transient_failures_back_off_reduce_lane_then_recover(tmp_path) -> None:
    path, connect = database(tmp_path)
    enqueue(path, "window:retry", "glm-zai")
    responses = iter(
        [
            {"ok": False, "error": "http_429", "error_class": "provider"},
            {"ok": False, "error": "timeout", "error_class": "timeout"},
            {"ok": True, "label": {"claims": []}, "calls": 1},
        ]
    )
    clock = [0.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += seconds

    runner = FleetRunner(
        connection_factory=connect,
        adapters={"glm-zai": lambda prompt, lane: next(responses)},
        clock=lambda: clock[0],
        sleeper=sleep,
        backoff_base_seconds=2,
        max_transient_retries=2,
    )
    report = runner.run_until_idle()

    assert sleeps == [2, 4]
    assert report["counts"]["succeeded"] == 1
    assert report["lanes"]["glm-zai"]["effective_limit"] == 3
    assert report["lanes"]["glm-zai"]["consecutive_transient"] == 0


def test_exhausted_transients_open_circuit_and_requeue_same_attempt(tmp_path) -> None:
    path, connect = database(tmp_path)
    enqueue(path, "window:fail", "glm")
    clock = [0.0]

    def sleep(seconds):
        clock[0] += seconds

    runner = FleetRunner(
        connection_factory=connect,
        adapters={
            "glm": lambda prompt, lane: {
                "ok": False,
                "error": "http_429",
                "error_class": "provider",
            }
        },
        clock=lambda: clock[0],
        sleeper=sleep,
        max_transient_retries=2,
        circuit_threshold=2,
        circuit_seconds=20,
    )
    report = runner.run_until_idle()
    conn = connect()
    conn.row_factory = sqlite3.Row
    task = get_task(conn, "window:fail")
    conn.close()

    assert report["counts"]["terminal_failed"] == 0
    assert task["status"] == "pending"
    assert task["attempt_number"] == 1
    assert report["lanes"]["glm"]["effective_limit"] == 1
    assert report["lanes"]["glm"]["circuit_open"] is True


def test_flash_requires_explicit_separate_qualification_and_never_falls_back(tmp_path) -> None:
    path, connect = database(tmp_path)
    enqueue(path, "window:flash", "glm-zai-flash")
    called = []
    runner = FleetRunner(
        connection_factory=connect,
        adapters={
            "glm-zai-flash": lambda prompt, lane: called.append(lane.name),
            "glm-zai": successful(),
        },
    )
    report = runner.run_until_idle()
    conn = connect()
    conn.row_factory = sqlite3.Row
    task = get_task(conn, "window:flash")
    conn.close()

    assert called == []
    assert report["counts"]["terminal_failed"] == 1
    assert task["semantic_failure_code"] == "lane_not_qualified"


def test_unknown_lane_terminalizes_without_using_another_adapter(tmp_path) -> None:
    path, connect = database(tmp_path)
    enqueue(path, "window:bad", "glm-mystery")
    calls = []
    runner = FleetRunner(
        connection_factory=connect,
        adapters={"glm-zai": lambda prompt, lane: calls.append(prompt)},
    )
    runner.run_until_idle()
    assert calls == []
    conn = connect()
    conn.row_factory = sqlite3.Row
    assert get_task(conn, "window:bad")["semantic_failure_code"] == "unknown_lane"
    conn.close()


def test_thread_pool_never_exceeds_each_lane_or_global_ceiling(tmp_path) -> None:
    path, connect = database(tmp_path)
    for lane, count in (("glm-zai", 18), ("glm", 8), ("glm-zai-flash", 12)):
        for index in range(count):
            enqueue(path, f"{lane}:{index}", lane)

    lock = threading.Lock()
    active = {lane: 0 for lane in LANES}
    peaks = {lane: 0 for lane in LANES}
    global_active = 0
    global_peak = 0

    def adapter(prompt, lane):
        nonlocal global_active, global_peak
        with lock:
            active[lane.name] += 1
            peaks[lane.name] = max(peaks[lane.name], active[lane.name])
            global_active += 1
            global_peak = max(global_peak, global_active)
        time.sleep(0.01)
        with lock:
            active[lane.name] -= 1
            global_active -= 1
        return {"ok": True, "label": {"claims": []}, "calls": 1}

    runner = FleetRunner(
        connection_factory=connect,
        adapters={lane: adapter for lane in LANES},
        qualified_lanes=set(LANES),
    )
    report = runner.run_until_idle()

    assert report["counts"]["succeeded"] == 38
    assert all(peaks[name] <= LANES[name].max_concurrency for name in LANES)
    assert global_peak <= GLOBAL_GLM_CEILING
