from __future__ import annotations

"""Persist a managed-ChatGPT no-thread/no-turn launch-capacity clearance."""

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, Sequence

from .app_server_interrupted_arm_recovery import probe_app_server_rate_limits
from .codex_app_server import CodexAppServerClient
from .util import now_iso, write_text_atomic


LAUNCH_CAPACITY_VERSION = "pif_app_server_launch_capacity_clearance_v1"
PINNED_CODEX = (
    Path.home()
    / ".codex/packages/standalone/releases/0.144.1-aarch64-apple-darwin/bin/codex"
).resolve()


class LaunchCapacityError(RuntimeError):
    pass


def _pinned_client_factory() -> CodexAppServerClient:
    return CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


async def probe_pinned_app_server_rate_limits(
    *, maximum_primary_used_percent: int = 20
) -> dict[str, Any]:
    return await probe_app_server_rate_limits(
        client_factory=_pinned_client_factory,
        maximum_primary_used_percent=maximum_primary_used_percent,
    )


async def wait_for_launch_capacity(
    *,
    output_path: Path,
    probe: Callable[..., Awaitable[dict[str, Any]]] = probe_pinned_app_server_rate_limits,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    clock: Callable[[], float] = time.time,
    maximum_primary_used_percent: int = 20,
    recheck_seconds: float = 300.0,
) -> dict[str, Any]:
    if maximum_primary_used_percent != 20:
        raise ValueError("launch semantic capacity threshold is frozen at 20 percent")
    if recheck_seconds <= 0:
        raise ValueError("launch capacity recheck interval must be positive")
    target = output_path.expanduser().resolve()
    if target.exists():
        try:
            prior = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LaunchCapacityError("existing launch capacity checkpoint is invalid") from exc
        if (
            not isinstance(prior, dict)
            or prior.get("schema_version") != LAUNCH_CAPACITY_VERSION
            or prior.get("managed_chatgpt_auth_verified") is not True
            or prior.get("maximum_primary_used_percent") != 20
            or prior.get("cleared_for_semantic_work") is not True
            or prior.get("thread_started") is not False
            or prior.get("turn_started") is not False
        ):
            raise LaunchCapacityError("existing launch capacity checkpoint is unsafe")
        return prior
    probe_count = 0
    while True:
        snapshot = await probe(maximum_primary_used_percent=20)
        probe_count += 1
        if (
            not isinstance(snapshot, dict)
            or snapshot.get("managed_chatgpt_auth_verified") is not True
            or snapshot.get("maximum_primary_used_percent") != 20
            or snapshot.get("thread_started") is not False
            or snapshot.get("turn_started") is not False
        ):
            raise LaunchCapacityError("live launch capacity probe violated its auth boundary")
        if snapshot.get("cleared_for_semantic_work") is True:
            payload = {
                "schema_version": LAUNCH_CAPACITY_VERSION,
                "observed_at": now_iso(),
                "managed_chatgpt_auth_verified": True,
                "plan_type": snapshot.get("plan_type"),
                "limit_id": snapshot.get("limit_id"),
                "primary_used_percent": snapshot.get("primary_used_percent"),
                "primary_resets_at": snapshot.get("primary_resets_at"),
                "rate_limit_reached_type": snapshot.get("rate_limit_reached_type"),
                "maximum_primary_used_percent": 20,
                "cleared_for_semantic_work": True,
                "probe_count": probe_count,
                "thread_started": False,
                "turn_started": False,
                "production_mutation_performed": False,
                "privacy": "managed_auth_plan_limit_percent_reset_and_status_no_credentials",
            }
            write_text_atomic(
                target,
                json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            )
            return payload
        wait_seconds = float(recheck_seconds)
        reset_at = snapshot.get("primary_resets_at")
        now = int(clock())
        if isinstance(reset_at, int) and reset_at > now:
            wait_seconds = min(wait_seconds, max(1.0, float(reset_at - now + 1)))
        await sleep(wait_seconds)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Wait for managed app-server launch capacity")
    parser.add_argument("--output", required=True)
    parser.add_argument("--recheck-seconds", type=float, default=300.0)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    os.environ.pop("OPENAI_API_KEY", None)
    try:
        result = asyncio.run(
            wait_for_launch_capacity(
                output_path=Path(args.output),
                recheck_seconds=args.recheck_seconds,
            )
        )
    except (LaunchCapacityError, OSError):
        print(json.dumps({"ok": False, "status": "failed_closed"}, sort_keys=True))
        return 1
    print(json.dumps({"ok": True, **result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
