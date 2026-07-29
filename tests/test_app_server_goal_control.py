from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from research_factory.app_server_goal_control import (
    inspect_thread_goal,
    resume_blocked_thread_goal,
    resume_paused_thread_goal,
)
from research_factory.codex_app_server import AppServerThreadGoal


def goal(status: str, *, updated_at: int = 20) -> AppServerThreadGoal:
    return AppServerThreadGoal(
        thread_id="thread-goal",
        objective="Finish the evaluation.",
        status=status,
        token_budget=100_000,
        tokens_used=41_000,
        time_used_seconds=120,
        created_at=10,
        updated_at=updated_at,
    )


class FakeClient:
    def __init__(self, goals: list[AppServerThreadGoal]):
        self.goals = iter(goals)
        self.get_thread_goal = AsyncMock(side_effect=self.goals)
        self.set_thread_goal_status = AsyncMock(return_value=goal("active", updated_at=21))
        self.account_summary = {"type": "chatgpt"}

    async def __aenter__(self) -> FakeClient:
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:  # noqa: ANN001
        return None


class AppServerGoalControlTests(unittest.IsolatedAsyncioTestCase):
    async def run_control(self, fake: FakeClient, control=resume_blocked_thread_goal) -> dict:
        with tempfile.TemporaryDirectory() as temp:
            binary = Path(temp) / "codex"
            binary.touch()
            with patch(
                "research_factory.app_server_goal_control.CodexAppServerClient",
                return_value=fake,
            ):
                return await control(
                    "thread-goal",
                    binary=binary,
                )

    async def test_blocked_goal_is_resumed_and_verified(self) -> None:
        fake = FakeClient([goal("blocked"), goal("active", updated_at=21)])
        result = await self.run_control(fake)

        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "resumed")
        self.assertEqual(result["after_status"], "active")
        self.assertEqual(result["auth_mode"], "chatgpt")
        self.assertTrue(result["objective_preserved"])
        self.assertTrue(result["token_budget_preserved"])
        self.assertTrue(result["accounting_preserved"])
        fake.set_thread_goal_status.assert_awaited_once_with("thread-goal", "active")

    async def test_active_goal_is_verified_without_mutation(self) -> None:
        fake = FakeClient([goal("active"), goal("active")])
        result = await self.run_control(fake)

        self.assertEqual(result["action"], "already_active")
        fake.set_thread_goal_status.assert_not_awaited()

    async def test_manual_pause_is_not_overridden(self) -> None:
        fake = FakeClient([goal("paused"), goal("paused")])
        result = await self.run_control(fake)

        self.assertFalse(result["ok"])
        self.assertEqual(result["action"], "not_resumable")
        fake.set_thread_goal_status.assert_not_awaited()

    async def test_paused_goal_is_resumed_only_by_explicit_pause_control(self) -> None:
        fake = FakeClient([goal("paused"), goal("active", updated_at=21)])
        result = await self.run_control(fake, resume_paused_thread_goal)

        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "resumed_paused")
        self.assertEqual(result["before_status"], "paused")
        self.assertEqual(result["after_status"], "active")
        fake.set_thread_goal_status.assert_awaited_once_with("thread-goal", "active")

    async def test_read_only_goal_inspection_never_mutates(self) -> None:
        fake = FakeClient([goal("paused"), goal("paused")])
        result = await self.run_control(fake, inspect_thread_goal)

        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "inspected")
        self.assertEqual(result["after_status"], "paused")
        fake.set_thread_goal_status.assert_not_awaited()

    async def test_resume_fails_closed_when_verification_is_not_active(self) -> None:
        fake = FakeClient([goal("blocked"), goal("blocked", updated_at=21)])
        with self.assertRaisesRegex(ValueError, "verify"):
            await self.run_control(fake)


if __name__ == "__main__":
    unittest.main()
