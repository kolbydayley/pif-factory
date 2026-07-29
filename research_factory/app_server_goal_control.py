from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from .codex_app_server import AppServerError, CodexAppServerClient


DEFAULT_CODEX_BINARY = (
    Path.home()
    / ".codex"
    / "packages"
    / "standalone"
    / "releases"
    / "0.144.1-aarch64-apple-darwin"
    / "bin"
    / "codex"
)


def _goal_result(
    *,
    thread_id: str,
    before: Any,
    after: Any,
    action: str,
    auth_mode: Any,
) -> dict[str, Any]:
    return {
        "ok": action != "not_resumable",
        "thread_id": thread_id,
        "action": action,
        "before_status": before.status,
        "after_status": after.status,
        "auth_mode": auth_mode,
        "objective_preserved": after.objective == before.objective,
        "token_budget_preserved": after.token_budget == before.token_budget,
        "accounting_preserved": (
            after.tokens_used == before.tokens_used
            and after.time_used_seconds == before.time_used_seconds
        ),
    }


async def inspect_thread_goal(
    thread_id: str,
    *,
    binary: str | Path = DEFAULT_CODEX_BINARY,
) -> dict[str, Any]:
    """Read one goal through managed app-server auth without mutating it."""

    binary_path = Path(binary).expanduser().resolve()
    if not binary_path.is_file():
        raise ValueError("pinned Codex app-server binary is unavailable")
    command = [str(binary_path), "app-server", "--stdio", "--strict-config"]
    async with CodexAppServerClient(command=command) as client:
        before = await client.get_thread_goal(thread_id)
        if before is None:
            raise ValueError("target thread has no goal to inspect")
        after = await client.get_thread_goal(thread_id)
        if after is None:
            raise ValueError("target goal disappeared during verification")
        if after.status != before.status:
            raise ValueError("target goal changed during read-only verification")
        return _goal_result(
            thread_id=thread_id,
            before=before,
            after=after,
            action="inspected",
            auth_mode=(client.account_summary or {}).get("type"),
        )


async def _resume_thread_goal_status(
    thread_id: str,
    *,
    resumable_status: str,
    resumed_action: str,
    binary: str | Path,
) -> dict[str, Any]:
    """Set one predeclared goal status active and verify status-only mutation.

    The mutation is deliberately status-only. Objective, budget, and usage
    accounting are read and validated by the client but never emitted or sent
    back in the set request.
    """

    binary_path = Path(binary).expanduser().resolve()
    if not binary_path.is_file():
        raise ValueError("pinned Codex app-server binary is unavailable")
    command = [str(binary_path), "app-server", "--stdio", "--strict-config"]
    async with CodexAppServerClient(command=command) as client:
        before = await client.get_thread_goal(thread_id)
        if before is None:
            raise ValueError("target thread has no goal to resume")

        before_status = before.status
        if before_status == resumable_status:
            await client.set_thread_goal_status(thread_id, "active")
            action = resumed_action
        elif before_status == "active":
            action = "already_active"
        elif before_status == "complete":
            action = "complete"
        else:
            # Manual pauses and system usage/budget limits are different from a
            # blocked work audit and must not be silently overridden.
            action = "not_resumable"

        after = await client.get_thread_goal(thread_id)
        if after is None:
            raise ValueError("target goal disappeared during verification")
        after_status = after.status
        if action == resumed_action and after_status != "active":
            raise ValueError("app-server did not verify the resumed goal as active")
        if action == "already_active" and after_status != "active":
            raise ValueError("active goal changed during verification")
        if action == "complete" and after_status != "complete":
            raise ValueError("complete goal changed during verification")

        return _goal_result(
            thread_id=thread_id,
            before=before,
            after=after,
            action=action,
            auth_mode=(client.account_summary or {}).get("type"),
        )


async def resume_blocked_thread_goal(
    thread_id: str,
    *,
    binary: str | Path = DEFAULT_CODEX_BINARY,
) -> dict[str, Any]:
    """Resume one blocked goal through the official managed-auth app-server."""

    return await _resume_thread_goal_status(
        thread_id,
        resumable_status="blocked",
        resumed_action="resumed",
        binary=binary,
    )


async def resume_paused_thread_goal(
    thread_id: str,
    *,
    binary: str | Path = DEFAULT_CODEX_BINARY,
) -> dict[str, Any]:
    """Recover one accidental pause after the supervisor rejects hold authority."""

    return await _resume_thread_goal_status(
        thread_id,
        resumable_status="paused",
        resumed_action="resumed_paused",
        binary=binary,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Control a Codex thread goal through the official app-server."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    resume = subparsers.add_parser("resume")
    resume.add_argument("--thread-id", required=True)
    resume.add_argument("--binary", default=str(DEFAULT_CODEX_BINARY))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = asyncio.run(
            resume_blocked_thread_goal(args.thread_id, binary=args.binary)
        )
    except (AppServerError, OSError, ValueError) as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "action": "resume_failed",
                    "error_class": type(exc).__name__,
                },
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
