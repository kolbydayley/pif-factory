from __future__ import annotations

import asyncio
import inspect
import json
import tempfile
import unittest
from pathlib import Path

from research_factory.app_server_capacity import (
    AppServerCapacityError,
    CapacityGatedCodexAppServerClient,
    parse_rate_limit_snapshot,
)
from research_factory.app_server_dev_selection import (
    run_app_server_dev_selection,
    run_full_judge_shards,
)
from research_factory.app_server_holdout_judge import (
    run_app_server_holdout_judge,
    run_holdout_judge_shards,
)
from research_factory.app_server_holdout_execution import (
    run_frozen_holdout_execution,
    run_holdout_baseline_phase,
    run_holdout_candidate_phase,
    run_holdout_context_phase,
)
from research_factory.app_server_holdout_stratifier import (
    run_holdout_reference_stratifier,
)
from research_factory.app_server_interrupted_arm_recovery import InterruptedArmRecovery
from research_factory.app_server_llm_judge import (
    run_app_server_judge_calibration,
    run_app_server_semantic_judge,
)
from research_factory.app_server_sharded_calibration import (
    run_app_server_sharded_judge_calibration,
)
from research_factory.app_server_sharded_selection import (
    run_app_server_sharded_selection,
)


def rate_limit(used: int, reached=None, resets_at: int = 500):
    return {
        "rateLimits": {
            "limitId": "codex",
            "primary": {"usedPercent": used, "resetsAt": resets_at},
            "rateLimitReachedType": reached,
        }
    }


class FakeInnerClient:
    def __init__(self, responses, *, account=None):
        self.responses = list(responses)
        self.account_summary = account or {"type": "chatgpt", "plan_type": "pro"}
        self.events = []
        self.entered = 0
        self.exited = 0
        self.turns = 0

    async def __aenter__(self):
        self.entered += 1
        self.events.append("inner_enter")
        return self

    async def __aexit__(self, *_args):
        self.exited += 1
        self.events.append("inner_exit")

    async def _request(self, method, params):
        self.events.append((method, params))
        if not self.responses:
            raise AssertionError("unexpected capacity probe")
        return self.responses.pop(0)

    async def run_ephemeral_structured_turn(self, *args, **kwargs):
        self.turns += 1
        self.events.append(("ephemeral_turn", args, kwargs))
        return {"ok": True}

    async def run_structured_turn(self, *args, **kwargs):
        self.turns += 1
        self.events.append(("warm_turn", args, kwargs))
        return {"ok": True}


class CapacityGateTest(unittest.IsolatedAsyncioTestCase):
    async def test_immutable_capacity_checkpoint_is_written_before_turn_and_blocks_retry(self):
        inner = FakeInnerClient([rate_limit(0), rate_limit(0)])
        client = CapacityGatedCodexAppServerClient(inner_factory=lambda: inner)
        with tempfile.TemporaryDirectory() as directory:
            checkpoint = Path(directory) / "ab.capacity.json"
            async with client:
                await client.run_ephemeral_structured_turn(
                    capacity_checkpoint_path=checkpoint,
                    prompt="first",
                )
                payload = json.loads(checkpoint.read_text(encoding="utf-8"))
                self.assertTrue(payload["cleared_for_semantic_turn"])
                self.assertEqual(payload["maximum_primary_used_percent"], 20)
                self.assertFalse(payload["thread_started"])
                self.assertFalse(payload["turn_started"])
                self.assertFalse(payload["sidecar_started"])
                with self.assertRaisesRegex(
                    AppServerCapacityError, "implicit semantic retry"
                ):
                    await client.run_ephemeral_structured_turn(
                        capacity_checkpoint_path=checkpoint,
                        prompt="retry",
                    )
        self.assertEqual(inner.turns, 1)

    async def test_waits_and_rechecks_before_delegating_without_early_turn(self):
        inner = FakeInnerClient(
            [
                rate_limit(25, reached=None, resets_at=101),
                rate_limit(20, reached=None, resets_at=500),
            ]
        )
        sleeps = []

        async def sleep(seconds):
            sleeps.append(seconds)
            inner.events.append(("sleep", seconds))

        client = CapacityGatedCodexAppServerClient(
            inner_factory=lambda: inner,
            recheck_seconds=60,
            sleep=sleep,
            clock=lambda: 100,
        )
        async with client:
            result = await client.run_ephemeral_structured_turn(sidecar_path="never-created-early")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(inner.entered, 1)
        self.assertEqual(inner.exited, 1)
        self.assertEqual(inner.turns, 1)
        self.assertEqual(len(sleeps), 1)
        self.assertEqual(
            inner.events[1:5],
            [
                ("account/rateLimits/read", {}),
                ("sleep", 2.0),
                ("account/rateLimits/read", {}),
                ("ephemeral_turn", (), {"sidecar_path": "never-created-early"}),
            ],
        )

    async def test_every_ephemeral_and_warm_turn_gets_a_fresh_live_probe(self):
        inner = FakeInnerClient([rate_limit(1), rate_limit(2)])
        client = CapacityGatedCodexAppServerClient(inner_factory=lambda: inner)
        async with client:
            await client.run_ephemeral_structured_turn(prompt="one")
            await client.run_structured_turn(prompt="two")
        probes = [event for event in inner.events if event == ("account/rateLimits/read", {})]
        self.assertEqual(len(probes), 2)
        self.assertEqual(inner.turns, 2)
        self.assertEqual(len(client.capacity_checks), 2)
        self.assertTrue(all(item["managed_chatgpt_auth_verified"] for item in client.capacity_checks))

    async def test_concurrent_callers_are_serialized_so_second_probe_is_after_first_turn(self):
        inner = FakeInnerClient([rate_limit(20), rate_limit(20)])

        original_request = inner._request
        original_turn = inner.run_ephemeral_structured_turn

        async def yielding_request(method, params):
            await asyncio.sleep(0)
            return await original_request(method, params)

        async def yielding_turn(*args, **kwargs):
            await asyncio.sleep(0)
            return await original_turn(*args, **kwargs)

        inner._request = yielding_request
        inner.run_ephemeral_structured_turn = yielding_turn
        client = CapacityGatedCodexAppServerClient(inner_factory=lambda: inner)
        async with client:
            await asyncio.gather(
                client.run_ephemeral_structured_turn(prompt="ab"),
                client.run_ephemeral_structured_turn(prompt="ba"),
            )
        semantic_events = [
            item[0]
            for item in inner.events
            if isinstance(item, tuple)
            and item[0] in {"account/rateLimits/read", "ephemeral_turn"}
        ]
        self.assertEqual(
            semantic_events,
            [
                "account/rateLimits/read",
                "ephemeral_turn",
                "account/rateLimits/read",
                "ephemeral_turn",
            ],
        )

    async def test_reached_type_blocks_even_when_primary_percent_is_zero(self):
        inner = FakeInnerClient(
            [rate_limit(0, reached="rate_limit_reached"), rate_limit(0, reached=None)]
        )
        sleeps = []

        async def sleep(seconds):
            sleeps.append(seconds)

        client = CapacityGatedCodexAppServerClient(
            inner_factory=lambda: inner,
            recheck_seconds=1,
            sleep=sleep,
        )
        async with client:
            await client.run_ephemeral_structured_turn()
        self.assertEqual(sleeps, [1.0])
        self.assertEqual(inner.turns, 1)

    async def test_malformed_capacity_or_non_chatgpt_auth_fails_before_turn(self):
        malformed = FakeInnerClient([{"rateLimits": {}}])
        client = CapacityGatedCodexAppServerClient(inner_factory=lambda: malformed)
        async with client:
            with self.assertRaises(AppServerCapacityError):
                await client.run_ephemeral_structured_turn()
        self.assertEqual(malformed.turns, 0)

        api = FakeInnerClient([], account={"type": "apiKey"})
        with self.assertRaises(AppServerCapacityError):
            async with CapacityGatedCodexAppServerClient(inner_factory=lambda: api):
                pass
        self.assertEqual(api.turns, 0)
        self.assertEqual(api.exited, 1)

    def test_parser_accepts_by_limit_id_and_is_frozen_at_twenty_percent(self):
        parsed = parse_rate_limit_snapshot(
            {
                "rateLimitsByLimitId": {
                    "other": {"limitId": "other"},
                    "codex": {
                        "limitId": "codex",
                        "primary": {"usedPercent": 20, "resetsAt": 10},
                        "rateLimitReachedType": None,
                    },
                }
            }
        )
        self.assertTrue(parsed["cleared_for_semantic_turn"])
        self.assertFalse(parsed["thread_started"])
        self.assertFalse(parsed["turn_started"])
        self.assertFalse(parsed["sidecar_started"])

    def test_all_semantic_entrypoint_defaults_use_capacity_gate(self):
        functions = (
            run_app_server_semantic_judge,
            run_app_server_judge_calibration,
            run_full_judge_shards,
            run_app_server_dev_selection,
            run_app_server_sharded_judge_calibration,
            run_app_server_sharded_selection,
            run_holdout_judge_shards,
            run_app_server_holdout_judge,
            run_holdout_reference_stratifier,
            run_holdout_context_phase,
            run_holdout_candidate_phase,
            run_holdout_baseline_phase,
            run_frozen_holdout_execution,
        )
        for function in functions:
            default = inspect.signature(function).parameters["client_factory"].default
            self.assertIs(default, CapacityGatedCodexAppServerClient, function.__name__)
        recovery_default = inspect.signature(InterruptedArmRecovery).parameters[
            "client_factory"
        ].default
        self.assertIs(recovery_default, CapacityGatedCodexAppServerClient)


if __name__ == "__main__":
    unittest.main()
