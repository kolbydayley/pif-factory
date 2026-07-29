from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from research_factory.app_server_capacity_probe import (
    LaunchCapacityError,
    PINNED_CODEX,
    _pinned_client_factory,
    wait_for_launch_capacity,
)


def snapshot(used: int, *, cleared: bool, auth: bool = True) -> dict:
    return {
        "managed_chatgpt_auth_verified": auth,
        "plan_type": "pro",
        "limit_id": "codex",
        "primary_used_percent": used,
        "primary_resets_at": 101,
        "rate_limit_reached_type": None,
        "maximum_primary_used_percent": 20,
        "cleared_for_semantic_work": cleared,
        "thread_started": False,
        "turn_started": False,
    }


class LaunchCapacityProbeTest(unittest.IsolatedAsyncioTestCase):
    async def test_default_client_uses_repository_pinned_codex(self) -> None:
        client = _pinned_client_factory()
        self.assertEqual(client.command[0], str(PINNED_CODEX))
        self.assertEqual(client.command[1:], ["app-server", "--stdio", "--strict-config"])

    async def test_waits_rechecks_and_persists_only_cleared_no_turn_probe(self) -> None:
        responses = [snapshot(21, cleared=False), snapshot(20, cleared=True)]
        sleeps = []

        async def probe(**_kwargs):
            return responses.pop(0)

        async def sleep(seconds):
            sleeps.append(seconds)

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "capacity.json"
            result = await wait_for_launch_capacity(
                output_path=output,
                probe=probe,
                sleep=sleep,
                clock=lambda: 100,
                recheck_seconds=60,
            )
            self.assertEqual(result["primary_used_percent"], 20)
            self.assertEqual(result["probe_count"], 2)
            self.assertEqual(sleeps, [2.0])
            self.assertFalse(result["thread_started"])
            self.assertFalse(result["turn_started"])
            self.assertTrue(output.is_file())

            async def forbidden_probe(**_kwargs):
                self.fail("an immutable cleared launch checkpoint should be adopted")

            adopted = await wait_for_launch_capacity(
                output_path=output,
                probe=forbidden_probe,
            )
            self.assertEqual(adopted, result)

    async def test_auth_drift_fails_without_writing_clearance(self) -> None:
        async def probe(**_kwargs):
            return snapshot(0, cleared=True, auth=False)

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "capacity.json"
            with self.assertRaisesRegex(LaunchCapacityError, "auth boundary"):
                await wait_for_launch_capacity(output_path=output, probe=probe)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
