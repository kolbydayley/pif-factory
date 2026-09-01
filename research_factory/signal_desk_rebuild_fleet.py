"""Concurrent GLM execution fleet for the Signal Desk clean-corpus rebuild.

The fleet is deliberately transport-agnostic.  Production adapters delegate to
``cheap_lane_adapters``; tests inject ordinary callables.  Tasks must name an
exact lane, so a provider failure can never trigger a silent fallback.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, MutableMapping, Optional

from .cheap_lane_adapters import GLM_JSON_INSTRUCTION, draft_glm, draft_glm_http
from .lane_profiles import LANE_PROFILES
from .signal_desk_rebuild_dispatch import (
    LostLease,
    acquire_lease,
    complete_attempt,
    fail_attempt_semantically,
    release_attempt_for_retry,
)


FLEET_SCHEMA_VERSION = "pif_signal_desk_rebuild_fleet_v1"
GLOBAL_GLM_CEILING = 23


@dataclass(frozen=True)
class LaneSpec:
    name: str
    model: str
    transport: str
    max_concurrency: int
    separately_qualified: bool = False


LANES: Mapping[str, LaneSpec] = {
    "glm-zai": LaneSpec(
        "glm-zai", LANE_PROFILES["glm-zai"]["model"], "http", 12
    ),
    "glm": LaneSpec("glm", LANE_PROFILES["glm"]["model"], "opencode", 3),
    "glm-zai-flash": LaneSpec(
        "glm-zai-flash",
        LANE_PROFILES["glm-zai-flash"]["model"],
        "http",
        8,
        separately_qualified=True,
    ),
}

if sum(lane.max_concurrency for lane in LANES.values()) != GLOBAL_GLM_CEILING:
    raise RuntimeError("GLM lane limits do not equal the global fleet ceiling")


Adapter = Callable[[str, LaneSpec], Mapping[str, Any]]
ConnectionFactory = Callable[[], sqlite3.Connection]


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def build_default_adapters(state_root: Path) -> dict[str, Adapter]:
    """Bind the qualified transports without copying auth or HTTP logic."""

    state_root = Path(state_root)

    def direct(prompt: str, lane: LaneSpec) -> Mapping[str, Any]:
        return draft_glm_http(
            prompt + GLM_JSON_INSTRUCTION,
            model=lane.model.split("/")[-1],
        )

    def opencode(prompt: str, lane: LaneSpec) -> Mapping[str, Any]:
        return draft_glm(
            prompt + GLM_JSON_INSTRUCTION,
            state_root,
            model=lane.model,
        )

    return {
        "glm-zai": direct,
        "glm": opencode,
        "glm-zai-flash": direct,
    }


class _LaneRuntime:
    """Lane-local adaptive concurrency, cooldown, and circuit state."""

    def __init__(
        self,
        spec: LaneSpec,
        *,
        clock: Callable[[], float],
        sleeper: Callable[[float], None],
        backoff_base_seconds: float,
        backoff_cap_seconds: float,
        circuit_threshold: int,
        circuit_seconds: float,
        recovery_successes: int,
    ) -> None:
        self.spec = spec
        self.clock = clock
        self.sleeper = sleeper
        self.backoff_base_seconds = backoff_base_seconds
        self.backoff_cap_seconds = backoff_cap_seconds
        self.circuit_threshold = circuit_threshold
        self.circuit_seconds = circuit_seconds
        self.recovery_successes = recovery_successes
        self._condition = threading.Condition()
        self._active = 0
        self.effective_limit = spec.max_concurrency
        self.consecutive_transient = 0
        self.success_streak = 0
        self.cooldown_until = 0.0

    def enter(self) -> None:
        while True:
            delay = 0.0
            with self._condition:
                now = self.clock()
                if self.cooldown_until > now:
                    delay = self.cooldown_until - now
                elif self._active < self.effective_limit:
                    self._active += 1
                    return
                else:
                    self._condition.wait(timeout=0.05)
                    continue
            self.sleeper(delay)

    def leave(self) -> None:
        with self._condition:
            self._active -= 1
            self._condition.notify_all()

    def transient_failure(self) -> float:
        with self._condition:
            self.consecutive_transient += 1
            self.success_streak = 0
            self.effective_limit = max(1, self.effective_limit // 2)
            delay = min(
                self.backoff_cap_seconds,
                self.backoff_base_seconds * (2 ** (self.consecutive_transient - 1)),
            )
            if self.consecutive_transient >= self.circuit_threshold:
                delay = max(delay, self.circuit_seconds)
            self.cooldown_until = max(self.cooldown_until, self.clock() + delay)
            self._condition.notify_all()
            return delay

    def success(self) -> None:
        with self._condition:
            self.consecutive_transient = 0
            self.cooldown_until = 0.0
            self.success_streak += 1
            if (
                self.success_streak >= self.recovery_successes
                and self.effective_limit < self.spec.max_concurrency
            ):
                self.effective_limit += 1
                self.success_streak = 0
            self._condition.notify_all()

    def snapshot(self) -> dict[str, Any]:
        with self._condition:
            return {
                "max_concurrency": self.spec.max_concurrency,
                "effective_limit": self.effective_limit,
                "consecutive_transient": self.consecutive_transient,
                "circuit_open": self.cooldown_until > self.clock(),
            }


def _transient(result: Mapping[str, Any]) -> bool:
    error_class = str(result.get("error_class") or "").lower()
    error = str(result.get("error") or "").lower()
    return error_class == "timeout" or "429" in error or error_class == "rate_limit"


class FleetRunner:
    """Drain leased rebuild tasks through the exact lane named by each task."""

    def __init__(
        self,
        *,
        connection_factory: ConnectionFactory,
        adapters: Mapping[str, Adapter],
        qualified_lanes: Optional[set[str]] = None,
        lane_specs: Mapping[str, LaneSpec] = LANES,
        worker_prefix: str = "signal-desk-glm",
        lease_seconds: float = 900,
        max_transient_retries: int = 2,
        backoff_base_seconds: float = 1.0,
        backoff_cap_seconds: float = 30.0,
        circuit_threshold: int = 3,
        circuit_seconds: float = 30.0,
        recovery_successes: int = 4,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if sum(spec.max_concurrency for spec in lane_specs.values()) > GLOBAL_GLM_CEILING:
            raise ValueError("configured lanes exceed global GLM ceiling")
        if lease_seconds <= 0 or max_transient_retries < 0:
            raise ValueError("invalid lease or retry configuration")
        self.connection_factory = connection_factory
        self.adapters = dict(adapters)
        self.lane_specs = dict(lane_specs)
        self.qualified_lanes = set(qualified_lanes or {"glm-zai", "glm"})
        self.worker_prefix = worker_prefix
        self.lease_seconds = lease_seconds
        self.max_transient_retries = max_transient_retries
        self._clock = clock
        self._runtimes = {
            name: _LaneRuntime(
                spec,
                clock=clock,
                sleeper=sleeper,
                backoff_base_seconds=backoff_base_seconds,
                backoff_cap_seconds=backoff_cap_seconds,
                circuit_threshold=circuit_threshold,
                circuit_seconds=circuit_seconds,
                recovery_successes=recovery_successes,
            )
            for name, spec in self.lane_specs.items()
        }
        self._count_lock = threading.Lock()
        self._halt = threading.Event()
        self._summary: MutableMapping[str, int] = {
            "leased": 0,
            "succeeded": 0,
            "terminal_failed": 0,
            "lost_lease": 0,
        }

    def _increment(self, key: str) -> None:
        with self._count_lock:
            self._summary[key] += 1

    def _terminal(
        self,
        conn: sqlite3.Connection,
        lease: Mapping[str, Any],
        code: str,
        detail: str,
    ) -> None:
        fail_attempt_semantically(
            conn,
            attempt_id=int(lease["current_attempt_id"]),
            lease_owner=str(lease["lease_owner"]),
            lease_generation=int(lease["lease_generation"]),
            failure_code=code,
            failure_detail=detail[:1000],
        )
        self._increment("terminal_failed")

    def _execute(self, conn: sqlite3.Connection, lease: Mapping[str, Any]) -> bool:
        payload = lease.get("payload") or {}
        lane_name = payload.get("lane")
        prompt = payload.get("prompt")
        if lane_name not in self.lane_specs:
            self._terminal(conn, lease, "unknown_lane", f"unknown lane: {lane_name!r}")
            return True
        spec = self.lane_specs[lane_name]
        if spec.separately_qualified and lane_name not in self.qualified_lanes:
            self._terminal(conn, lease, "lane_not_qualified", lane_name)
            return True
        if lane_name not in self.adapters:
            self._terminal(conn, lease, "adapter_unavailable", lane_name)
            return True
        if not isinstance(prompt, str) or not prompt.strip():
            self._terminal(conn, lease, "invalid_prompt", "prompt must be non-empty")
            return True

        runtime = self._runtimes[lane_name]
        adapter = self.adapters[lane_name]
        calls = 0
        provider_calls = 0
        result: Mapping[str, Any] = {}
        while calls <= self.max_transient_retries:
            runtime.enter()
            try:
                calls += 1
                try:
                    result = adapter(prompt, spec)
                except Exception as exc:  # provider adapters are an isolation boundary
                    result = {
                        "ok": False,
                        "error": f"adapter_exception:{type(exc).__name__}:{exc}",
                        "error_class": "provider",
                        "calls": 1,
                    }
            finally:
                runtime.leave()
            provider_calls += int(result.get("calls", 1))
            if result.get("ok"):
                runtime.success()
                break
            if not _transient(result):
                break
            runtime.transient_failure()
            if calls > self.max_transient_retries:
                break

        if not result.get("ok"):
            code = str(result.get("error_class") or "provider_failure")
            if _transient(result):
                release_attempt_for_retry(
                    conn,
                    attempt_id=int(lease["current_attempt_id"]),
                    lease_owner=str(lease["lease_owner"]),
                    lease_generation=int(lease["lease_generation"]),
                    failure_code="transient_retries_exhausted",
                    failure_detail=str(result.get("error") or "transient provider failure")[:1000],
                )
                self._halt.set()
                return False
            self._terminal(conn, lease, code, str(result.get("error") or code))
            return True

        label = result.get("label")
        output_sha = _sha256(_canonical_json(label))
        receipt_without_hash = {
            "schema_version": FLEET_SCHEMA_VERSION,
            "task_key": lease["task_key"],
            "attempt_id": int(lease["current_attempt_id"]),
            "attempt_number": int(lease["attempt_number"]),
            "lane": lane_name,
            "model": spec.model,
            "transport": spec.transport,
            "prompt_sha256": _sha256(prompt),
            "output_sha256": output_sha,
            "fleet_calls": calls,
            "provider_calls": provider_calls,
            "status": "succeeded",
        }
        receipt = dict(receipt_without_hash)
        receipt["receipt_sha256"] = _sha256(_canonical_json(receipt_without_hash))
        complete_attempt(
            conn,
            attempt_id=int(lease["current_attempt_id"]),
            lease_owner=str(lease["lease_owner"]),
            lease_generation=int(lease["lease_generation"]),
            output={"label": label, "receipt": receipt},
        )
        self._increment("succeeded")
        return True

    def _worker(self, worker_number: int) -> None:
        conn = self.connection_factory()
        conn.row_factory = sqlite3.Row
        try:
            while True:
                if self._halt.is_set():
                    return
                lease = acquire_lease(
                    conn,
                    lease_owner=f"{self.worker_prefix}-{worker_number}",
                    lease_seconds=self.lease_seconds,
                )
                if lease is None:
                    return
                self._increment("leased")
                try:
                    if not self._execute(conn, lease):
                        return
                except LostLease:
                    self._increment("lost_lease")
        finally:
            conn.close()

    def run_until_idle(self) -> dict[str, Any]:
        """Drain currently leasable work with the fixed global worker ceiling."""

        with ThreadPoolExecutor(max_workers=GLOBAL_GLM_CEILING) as pool:
            futures = [pool.submit(self._worker, index) for index in range(GLOBAL_GLM_CEILING)]
            for future in futures:
                future.result()
        with self._count_lock:
            counts = dict(self._summary)
        return {
            "schema_version": FLEET_SCHEMA_VERSION,
            "global_ceiling": GLOBAL_GLM_CEILING,
            "counts": counts,
            "lanes": {name: runtime.snapshot() for name, runtime in self._runtimes.items()},
        }
