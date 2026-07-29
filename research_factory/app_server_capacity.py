from __future__ import annotations

"""Capacity-gated managed Codex app-server client.

This wrapper deliberately lives outside the pinned transport implementation.
It owns one persistent inner :class:`CodexAppServerClient`, probes the official
``account/rateLimits/read`` method before every semantic turn, and delegates the
turn only after the Codex primary window is at or below the frozen threshold and
no reached type is active.  A probe never creates a thread, turn, or sidecar.
"""

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional

from .codex_app_server import CodexAppServerClient
from .util import now_iso, write_text_atomic


CAPACITY_GATE_VERSION = "pif_app_server_capacity_gate_v1"
CAPACITY_CHECKPOINT_VERSION = "pif_app_server_capacity_checkpoint_v1"
DEFAULT_MAXIMUM_PRIMARY_USED_PERCENT = 20
DEFAULT_RECHECK_SECONDS = 300.0


class AppServerCapacityError(RuntimeError):
    """Raised when live managed-app-server capacity cannot be verified."""


def parse_rate_limit_snapshot(
    response: Any,
    *,
    maximum_primary_used_percent: int = DEFAULT_MAXIMUM_PRIMARY_USED_PERCENT,
) -> Dict[str, Any]:
    if not isinstance(response, dict):
        raise AppServerCapacityError("app-server rate-limit response is malformed")
    snapshot = None
    by_id = response.get("rateLimitsByLimitId")
    if isinstance(by_id, dict):
        direct = by_id.get("codex")
        if isinstance(direct, dict):
            snapshot = direct
        if snapshot is None:
            for value in by_id.values():
                if isinstance(value, dict) and value.get("limitId") == "codex":
                    snapshot = value
                    break
    if snapshot is None and isinstance(response.get("rateLimits"), dict):
        snapshot = response["rateLimits"]
    if not isinstance(snapshot, dict):
        raise AppServerCapacityError("app-server returned no Codex rate-limit snapshot")
    primary = snapshot.get("primary")
    if not isinstance(primary, dict):
        raise AppServerCapacityError("Codex rate-limit snapshot has no primary window")
    used_percent = primary.get("usedPercent")
    resets_at = primary.get("resetsAt")
    if isinstance(used_percent, bool) or not isinstance(used_percent, int):
        raise AppServerCapacityError("Codex primary used percentage is malformed")
    if used_percent < 0 or used_percent > 100:
        raise AppServerCapacityError("Codex primary used percentage is outside 0-100")
    if resets_at is not None and (
        isinstance(resets_at, bool) or not isinstance(resets_at, int)
    ):
        raise AppServerCapacityError("Codex primary reset timestamp is malformed")
    reached_type = snapshot.get("rateLimitReachedType")
    if reached_type is not None and not isinstance(reached_type, str):
        raise AppServerCapacityError("Codex reached type is malformed")
    maximum = int(maximum_primary_used_percent)
    if maximum < 0 or maximum > 100:
        raise ValueError("maximum primary used percentage must be between 0 and 100")
    return {
        "schema_version": CAPACITY_GATE_VERSION,
        "limit_id": snapshot.get("limitId") or "codex",
        "primary_used_percent": used_percent,
        "primary_resets_at": resets_at,
        "rate_limit_reached_type": reached_type,
        "maximum_primary_used_percent": maximum,
        "cleared_for_semantic_turn": bool(
            reached_type is None and used_percent <= maximum
        ),
        "thread_started": False,
        "turn_started": False,
        "sidecar_started": False,
    }


class CapacityGatedCodexAppServerClient:
    """Persistent app-server client that gates each semantic turn on live quota."""

    def __init__(
        self,
        *,
        inner_factory: Callable[[], Any] = CodexAppServerClient,
        maximum_primary_used_percent: int = DEFAULT_MAXIMUM_PRIMARY_USED_PERCENT,
        recheck_seconds: float = DEFAULT_RECHECK_SECONDS,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if maximum_primary_used_percent != DEFAULT_MAXIMUM_PRIMARY_USED_PERCENT:
            raise ValueError("semantic capacity threshold is frozen at 20 percent")
        if recheck_seconds <= 0:
            raise ValueError("capacity recheck interval must be positive")
        self.inner_factory = inner_factory
        self.maximum_primary_used_percent = maximum_primary_used_percent
        self.recheck_seconds = float(recheck_seconds)
        self.sleep = sleep
        self.clock = clock
        self.inner_context: Optional[Any] = None
        self.inner: Optional[Any] = None
        self.capacity_checks: list[Dict[str, Any]] = []
        self._semantic_turn_lock = asyncio.Lock()

    async def __aenter__(self) -> "CapacityGatedCodexAppServerClient":
        if self.inner is not None:
            raise AppServerCapacityError("capacity-gated client cannot be entered twice")
        self.inner_context = self.inner_factory()
        self.inner = await self.inner_context.__aenter__()
        account = getattr(self.inner, "account_summary", None)
        if not isinstance(account, dict) or account.get("type") != "chatgpt":
            await self.inner_context.__aexit__(None, None, None)
            self.inner = None
            self.inner_context = None
            raise AppServerCapacityError("capacity gate requires managed ChatGPT auth")
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        context = self.inner_context
        self.inner = None
        self.inner_context = None
        if context is not None:
            await context.__aexit__(exc_type, exc, traceback)

    def __getattr__(self, name: str) -> Any:
        # Thread lifecycle and non-turn transport methods remain available.  The
        # two semantic turn entrypoints below cannot be bypassed by delegation.
        inner = self.__dict__.get("inner")
        if inner is None:
            raise AttributeError(name)
        return getattr(inner, name)

    async def wait_for_semantic_capacity(self) -> Dict[str, Any]:
        if self.inner is None:
            raise AppServerCapacityError("capacity-gated client is not started")
        account = getattr(self.inner, "account_summary", None)
        if not isinstance(account, dict) or account.get("type") != "chatgpt":
            raise AppServerCapacityError("capacity gate lost managed ChatGPT auth")
        while True:
            response = await self.inner._request(  # noqa: SLF001 - official no-turn method
                "account/rateLimits/read", {}
            )
            snapshot = parse_rate_limit_snapshot(
                response,
                maximum_primary_used_percent=self.maximum_primary_used_percent,
            )
            record = {
                **snapshot,
                "managed_chatgpt_auth_verified": True,
                "plan_type": account.get("plan_type"),
            }
            self.capacity_checks.append(record)
            if snapshot["cleared_for_semantic_turn"]:
                return record
            wait_seconds = self.recheck_seconds
            reset_at = snapshot.get("primary_resets_at")
            now = int(self.clock())
            if isinstance(reset_at, int) and reset_at > now:
                wait_seconds = min(wait_seconds, max(1.0, float(reset_at - now + 1)))
            await self.sleep(wait_seconds)

    def _write_capacity_checkpoint(
        self, path: Any, snapshot: Dict[str, Any]
    ) -> None:
        if path is None:
            return
        target = Path(path).expanduser().resolve()
        payload = {
            "schema_version": CAPACITY_CHECKPOINT_VERSION,
            "checked_at": now_iso(),
            "limit_id": snapshot["limit_id"],
            "primary_used_percent": snapshot["primary_used_percent"],
            "primary_resets_at": snapshot["primary_resets_at"],
            "rate_limit_reached_type": snapshot["rate_limit_reached_type"],
            "maximum_primary_used_percent": snapshot[
                "maximum_primary_used_percent"
            ],
            "cleared_for_semantic_turn": snapshot[
                "cleared_for_semantic_turn"
            ],
            "managed_chatgpt_auth_verified": snapshot[
                "managed_chatgpt_auth_verified"
            ],
            "plan_type": snapshot.get("plan_type"),
            "thread_started": False,
            "turn_started": False,
            "sidecar_started": False,
            "retry_checkpoint_reuse_allowed": False,
            "privacy": "capacity_status_only_no_prompt_output_email_credentials_or_thread_ids",
        }
        rendered = json.dumps(
            payload, ensure_ascii=True, indent=2, sort_keys=True
        ) + "\n"
        if target.exists():
            raise AppServerCapacityError(
                "capacity checkpoint already exists; implicit semantic retry is prohibited"
            )
        write_text_atomic(target, rendered)

    async def run_ephemeral_structured_turn(self, *args: Any, **kwargs: Any) -> Any:
        async with self._semantic_turn_lock:
            checkpoint_path = kwargs.pop("capacity_checkpoint_path", None)
            snapshot = await self.wait_for_semantic_capacity()
            self._write_capacity_checkpoint(checkpoint_path, snapshot)
            if self.inner is None:  # pragma: no cover - guarded above
                raise AppServerCapacityError("capacity-gated client stopped before turn")
            return await self.inner.run_ephemeral_structured_turn(*args, **kwargs)

    async def run_structured_turn(self, *args: Any, **kwargs: Any) -> Any:
        async with self._semantic_turn_lock:
            checkpoint_path = kwargs.pop("capacity_checkpoint_path", None)
            snapshot = await self.wait_for_semantic_capacity()
            self._write_capacity_checkpoint(checkpoint_path, snapshot)
            if self.inner is None:  # pragma: no cover - guarded above
                raise AppServerCapacityError("capacity-gated client stopped before turn")
            return await self.inner.run_structured_turn(*args, **kwargs)


CapacityGatedAppServerClient = CapacityGatedCodexAppServerClient


def capacity_gated_client_factory() -> CapacityGatedCodexAppServerClient:
    """Default reusable factory for managed semantic work."""

    return CapacityGatedCodexAppServerClient()
