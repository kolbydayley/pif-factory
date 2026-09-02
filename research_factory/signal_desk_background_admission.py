"""Foreground-safe admission for background GPT-5.6-Sol work.

Signal Desk's Gold, A1, and A2 jobs share the same managed Codex model pool as
the desktop client.  Background throughput must never make an interactive
Codex turn compete with an unattended rebuild.  This module deliberately
collects only local interaction *state* (idle duration and whether the Codex
desktop app is foreground), never window titles, messages, or transcript text.

The policy is conservative:

* no new background model call while ChatGPT/Codex is foreground;
* no new call for two minutes after local input;
* a background lane begins at one provider admission and only expands during
  increasingly long unattended periods; and
* an in-flight background turn is interrupted as soon as foreground activity
  resumes, preserving the leased task for a later retry.

It complements, rather than replaces, the durable provider-capacity circuit:
this gate protects foreground work before a provider overload has to occur.
"""

from __future__ import annotations

import asyncio
import math
import platform
import re
import subprocess
from collections.abc import Awaitable, Callable
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator


SCHEMA_VERSION = "pif_signal_desk_foreground_safe_background_admission_v1"
MINIMUM_IDLE_SECONDS = 120.0
ONE_SLOT_UNTIL_IDLE_SECONDS = 15 * 60.0
TWO_SLOTS_UNTIL_IDLE_SECONDS = 30 * 60.0
FOUR_SLOTS_UNTIL_IDLE_SECONDS = 45 * 60.0
POLL_SECONDS = 2.0
CURRENT_TURN_FOREGROUND_OVERRIDE_MAX_CONFIGURED_CONCURRENCY = 4
CURRENT_TURN_FOREGROUND_OVERRIDE_PROVIDER_CAP = 3


@dataclass(frozen=True)
class BackgroundAdmission:
    """A privacy-safe decision about whether background work may start."""

    allowed: bool
    reason: str
    retry_after_seconds: int
    provider_concurrency_cap: int
    input_idle_seconds: float | None


@dataclass(frozen=True)
class CurrentTurnForegroundOverride:
    """A deliberately narrow, process-local operator authorization.

    This is not a general foreground bypass.  It exists only for an explicitly
    authorized current-turn Gold launch, and it never permits more than three
    provider calls.  The context is reset when the invoking process exits its
    ``with`` block; it is not read from a file, environment variable, or any
    durable setting.
    """

    source: str
    configured_concurrency_cap: int = (
        CURRENT_TURN_FOREGROUND_OVERRIDE_MAX_CONFIGURED_CONCURRENCY
    )
    provider_concurrency_cap: int = CURRENT_TURN_FOREGROUND_OVERRIDE_PROVIDER_CAP


_CURRENT_TURN_FOREGROUND_OVERRIDE: ContextVar[CurrentTurnForegroundOverride | None] = (
    ContextVar("signal_desk_current_turn_foreground_override", default=None)
)


class BackgroundWorkDeferred(RuntimeError):
    """Raised when a background model turn must yield to foreground use."""

    def __init__(self, admission: BackgroundAdmission) -> None:
        self.admission = admission
        super().__init__(
            f"background GPT-5.6-Sol work deferred: {admission.reason}; "
            f"retry after {admission.retry_after_seconds}s"
        )


class BackgroundWorkPreempted(BackgroundWorkDeferred):
    """Foreground activity resumed after a background turn had started."""


def _bounded(configured_concurrency: int) -> int:
    value = int(configured_concurrency)
    if not 1 <= value <= 8:
        raise ValueError("configured background GPT-5.6-Sol concurrency must be 1-8")
    return value


def _validate_current_turn_override(
    *, source: str, configured_concurrency: int,
) -> CurrentTurnForegroundOverride:
    """Build the non-persistent foreground override after strict bounds checks."""

    label = str(source).strip()
    if not label:
        raise ValueError("current-turn foreground override requires a source label")
    # The CLI also applies this contract.  Keeping it here prevents a future
    # programmatic caller from silently broadening the three-call safety bound.
    if int(configured_concurrency) != CURRENT_TURN_FOREGROUND_OVERRIDE_MAX_CONFIGURED_CONCURRENCY:
        raise ValueError(
            "current-turn foreground override requires configured concurrency "
            f"{CURRENT_TURN_FOREGROUND_OVERRIDE_MAX_CONFIGURED_CONCURRENCY}"
        )
    return CurrentTurnForegroundOverride(source=label)


@contextmanager
def current_turn_foreground_override(
    *, source: str, configured_concurrency: int,
) -> Iterator[CurrentTurnForegroundOverride]:
    """Temporarily allow throttled Gold admissions despite foreground use.

    The normal policy remains unchanged outside this lexical scope.  This
    context is intentionally local to the running process, so a later
    unattended invocation must supply a new explicit authorization rather than
    inheriting this one.
    """

    override = _validate_current_turn_override(
        source=source,
        configured_concurrency=configured_concurrency,
    )
    token = _CURRENT_TURN_FOREGROUND_OVERRIDE.set(override)
    try:
        yield override
    finally:
        _CURRENT_TURN_FOREGROUND_OVERRIDE.reset(token)


def current_turn_foreground_override_admission(
    *, configured_concurrency: int,
) -> BackgroundAdmission | None:
    """Return the bounded admission decision only inside the explicit scope."""

    override = _CURRENT_TURN_FOREGROUND_OVERRIDE.get()
    if override is None:
        return None
    configured = _bounded(configured_concurrency)
    # ``configured`` is normally the adaptive effective limit, which can only
    # be less than the CLI's fixed cap. Never return a cap greater than three.
    return BackgroundAdmission(
        allowed=True,
        reason="operator_current_turn_foreground_override",
        retry_after_seconds=0,
        provider_concurrency_cap=min(
            configured,
            override.provider_concurrency_cap,
        ),
        input_idle_seconds=None,
    )


def _is_codex_foreground(frontmost_application: str | None) -> bool:
    value = str(frontmost_application or "").strip().casefold()
    return value in {"chatgpt", "codex"} or value.startswith("chatgpt ")


def evaluate_background_admission(
    *,
    configured_concurrency: int,
    input_idle_seconds: float | None,
    frontmost_application: str | None,
) -> BackgroundAdmission:
    """Evaluate the policy from injectable, privacy-safe local signals."""

    configured = _bounded(configured_concurrency)
    if _is_codex_foreground(frontmost_application):
        return BackgroundAdmission(
            allowed=False,
            reason="foreground_codex_active",
            retry_after_seconds=int(MINIMUM_IDLE_SECONDS),
            provider_concurrency_cap=0,
            input_idle_seconds=input_idle_seconds,
        )
    if input_idle_seconds is None or not math.isfinite(float(input_idle_seconds)):
        return BackgroundAdmission(
            allowed=False,
            reason="foreground_state_unknown",
            retry_after_seconds=60,
            provider_concurrency_cap=0,
            input_idle_seconds=None,
        )
    idle = max(0.0, float(input_idle_seconds))
    if idle < MINIMUM_IDLE_SECONDS:
        return BackgroundAdmission(
            allowed=False,
            reason="recent_local_input",
            retry_after_seconds=max(1, int(math.ceil(MINIMUM_IDLE_SECONDS - idle))),
            provider_concurrency_cap=0,
            input_idle_seconds=idle,
        )
    if idle < ONE_SLOT_UNTIL_IDLE_SECONDS:
        cap = 1
    elif idle < TWO_SLOTS_UNTIL_IDLE_SECONDS:
        cap = 2
    elif idle < FOUR_SLOTS_UNTIL_IDLE_SECONDS:
        cap = 4
    else:
        cap = configured
    return BackgroundAdmission(
        allowed=True,
        reason="background_window_available",
        retry_after_seconds=0,
        provider_concurrency_cap=min(configured, cap),
        input_idle_seconds=idle,
    )


def _run_local(command: list[str]) -> str | None:
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout


def macos_input_idle_seconds() -> float | None:
    """Return only the local HID idle duration; fail closed when unavailable."""

    if platform.system() != "Darwin":
        return None
    output = _run_local(["/usr/sbin/ioreg", "-c", "IOHIDSystem", "-d", "4"])
    if not output:
        return None
    values = [
        int(value)
        for value in re.findall(r'"HIDIdleTime"\\s*=\\s*(\\d+)', output)
    ]
    if not values:
        return None
    # IORegistry reports nanoseconds.  A system may expose more than one
    # matching object; the most recently active device is the conservative one.
    return min(values) / 1_000_000_000.0


def macos_frontmost_application() -> str | None:
    """Return the application name only for a foreground Codex check."""

    if platform.system() != "Darwin":
        return None
    output = _run_local([
        "/usr/bin/osascript",
        "-e",
        'tell application "System Events" to get name of first application process whose frontmost is true',
    ])
    value = str(output or "").strip()
    return value or None


def local_background_admission(*, configured_concurrency: int) -> BackgroundAdmission:
    """Read current local interaction state without exposing any user content."""

    override = current_turn_foreground_override_admission(
        configured_concurrency=configured_concurrency,
    )
    if override is not None:
        return override

    return evaluate_background_admission(
        configured_concurrency=configured_concurrency,
        input_idle_seconds=macos_input_idle_seconds(),
        frontmost_application=macos_frontmost_application(),
    )


async def run_foreground_preemptible(
    operation: Callable[[], Awaitable[Any]],
    *,
    configured_concurrency: int,
    admission: Callable[..., BackgroundAdmission] = local_background_admission,
    poll_seconds: float = POLL_SECONDS,
) -> Any:
    """Run one turn only while it remains safe for foreground Codex use.

    Cancelling ``operation`` lets :class:`CodexAppServerClient` issue its
    protocol-level turn interrupt and write a recoverable cancelled sidecar.
    Callers must still settle provider reservations conservatively because a
    remote turn may have consumed tokens before the interrupt arrived.
    """

    if poll_seconds <= 0:
        raise ValueError("foreground admission poll interval must be positive")
    initial = admission(configured_concurrency=configured_concurrency)
    if not initial.allowed:
        raise BackgroundWorkDeferred(initial)
    task = asyncio.create_task(operation())
    try:
        while True:
            done, _pending = await asyncio.wait({task}, timeout=poll_seconds)
            if done:
                return task.result()
            current = admission(configured_concurrency=configured_concurrency)
            if not current.allowed:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                raise BackgroundWorkPreempted(current)
    finally:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
