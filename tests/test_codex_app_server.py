from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, call

from research_factory.codex_app_server import (
    AppServerProcessDied,
    AppServerProtocolError,
    AppServerRecoveryRequired,
    AppServerStructuredOutputError,
    AppServerThreadGoal,
    AppServerTurnTimeout,
    CodexAppServerClient,
    PROTOCOL_SCHEMA_SHA256,
    TokenUsage,
    finalize_cancelled_turn_sidecar,
    verify_protocol_schema,
)
from research_factory.util import sha256_text


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "fake_codex_app_server.py"
MODEL = "gpt-5.6-sol"
BASE_INSTRUCTIONS = "Return only the requested structured value."
PROMPT = "Process synthetic fixture input only."
OUTPUT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["ok", "thread_id", "turn_id"],
    "properties": {
        "ok": {"type": "boolean"},
        "thread_id": {"type": "string"},
        "turn_id": {"type": "string"},
    },
}


def thread_goal_payload(
    *,
    thread_id: str = "thread-goal",
    status: str = "blocked",
    updated_at: int = 20,
) -> dict[str, object]:
    return {
        "threadId": thread_id,
        "objective": "Finish the synthetic evaluation.",
        "status": status,
        "tokenBudget": 100_000,
        "tokensUsed": 41_000,
        "timeUsedSeconds": 120,
        "createdAt": 10,
        "updatedAt": updated_at,
    }


class CodexAppServerClientTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tempdir.name)

    def tearDown(self) -> None:
        self._tempdir.cleanup()

    def client(self, scenario: str = "success") -> CodexAppServerClient:
        command = [
            sys.executable,
            str(FIXTURE),
            "--scenario",
            scenario,
            "--log",
            str(self.root / f"{scenario}-methods.jsonl"),
        ]
        return CodexAppServerClient(
            command=command,
            verify_cli=False,
            request_timeout_seconds=1.0,
            interrupt_timeout_seconds=0.5,
            usage_grace_seconds=0.05,
        )

    def methods(self, scenario: str) -> list[str]:
        path = self.root / f"{scenario}-methods.jsonl"
        if not path.exists():
            return []
        return [json.loads(line)["method"] for line in path.read_text(encoding="utf-8").splitlines()]

    async def run_turn(
        self,
        client: CodexAppServerClient,
        *,
        stem: str,
        timeout_seconds: float = 1.0,
    ):
        return await client.run_ephemeral_structured_turn(
            model=MODEL,
            effort="low",
            base_instructions=BASE_INSTRUCTIONS,
            prompt=PROMPT,
            output_schema=OUTPUT_SCHEMA,
            cwd=self.root,
            sidecar_path=self.root / f"{stem}.sidecar.json",
            output_path=self.root / f"{stem}.output.json",
            batch_size=3,
            thread_mode="new_thread",
            timeout_seconds=timeout_seconds,
        )

    async def test_initialization_pins_schema_and_redacts_account_email(self) -> None:
        self.assertEqual(verify_protocol_schema(), PROTOCOL_SCHEMA_SHA256)
        async with self.client() as client:
            self.assertEqual(
                client.account_summary,
                {
                    "type": "chatgpt",
                    "plan_type": "pro",
                    "requires_openai_auth": True,
                },
            )
            self.assertNotIn("email", client.account_summary or {})
            self.assertEqual(client.app_server_user_agent, "fake-codex-app-server/0.144.1")
            self.assertEqual(
                {model["id"] for model in client.models},
                {"gpt-5.6-sol", "gpt-5.4-mini", "gpt-5.3-codex-spark"},
            )
        self.assertEqual(
            self.methods("success")[:4],
            ["initialize", "initialized", "account/read", "model/list"],
        )

    async def test_live_weekly_rate_limit_read_is_numeric_and_account_safe(self) -> None:
        async with self.client() as client:
            rate_limit = await client.read_weekly_rate_limit()
        self.assertEqual(
            rate_limit,
            {
                "used_percent": 17.0,
                "resets_at": 1_788_748_260,
                "window_minutes": 10_080,
                "source": "app_server_live",
            },
        )
        self.assertIn("account/rateLimits/read", self.methods("success"))

    async def test_goal_status_control_uses_official_rpc_without_rewriting_goal(self) -> None:
        request = AsyncMock(
            side_effect=[
                {"goal": thread_goal_payload()},
                {"goal": thread_goal_payload(status="active", updated_at=21)},
            ]
        )
        async with self.client() as client:
            client._request = request
            before = await client.get_thread_goal("thread-goal")
            resumed = await client.set_thread_goal_status("thread-goal", "active")

        self.assertEqual(
            before,
            AppServerThreadGoal(
                thread_id="thread-goal",
                objective="Finish the synthetic evaluation.",
                status="blocked",
                token_budget=100_000,
                tokens_used=41_000,
                time_used_seconds=120,
                created_at=10,
                updated_at=20,
            ),
        )
        self.assertEqual(resumed.status, "active")
        self.assertEqual(resumed.objective, before.objective)
        self.assertEqual(resumed.token_budget, before.token_budget)
        request.assert_has_awaits(
            [
                call("thread/goal/get", {"threadId": "thread-goal"}),
                call(
                    "thread/goal/set",
                    {"threadId": "thread-goal", "status": "active"},
                ),
            ]
        )

    async def test_get_thread_goal_returns_none_when_server_has_no_goal(self) -> None:
        request = AsyncMock(return_value={"goal": None})
        async with self.client() as client:
            client._request = request
            self.assertIsNone(await client.get_thread_goal("thread-goal"))
        request.assert_awaited_once_with("thread/goal/get", {"threadId": "thread-goal"})

    async def test_goal_control_rejects_invalid_inputs_before_rpc(self) -> None:
        request = AsyncMock()
        async with self.client() as client:
            client._request = request
            with self.assertRaisesRegex(ValueError, "thread id"):
                await client.get_thread_goal(" thread-goal")
            with self.assertRaisesRegex(ValueError, "thread goal status"):
                await client.set_thread_goal_status("thread-goal", "running")
        request.assert_not_awaited()

    async def test_goal_control_fails_closed_on_malformed_or_unconfirmed_goal(self) -> None:
        malformed = thread_goal_payload()
        malformed["tokensUsed"] = True
        request = AsyncMock(
            side_effect=[
                {"goal": malformed},
                {"goal": thread_goal_payload(status="blocked")},
            ]
        )
        async with self.client() as client:
            client._request = request
            with self.assertRaisesRegex(AppServerProtocolError, "integer field"):
                await client.get_thread_goal("thread-goal")
            with self.assertRaisesRegex(AppServerProtocolError, "requested status"):
                await client.set_thread_goal_status("thread-goal", "active")

    async def test_structured_output_usage_and_private_sidecar_are_captured(self) -> None:
        async with self.client() as client:
            result = await self.run_turn(client, stem="success")

        self.assertTrue(result.status_ok)
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.output["thread_id"], result.thread_id)
        self.assertEqual(result.output["turn_id"], result.turn_id)
        self.assertEqual(
            result.usage,
            TokenUsage(
                input_tokens=101,
                cached_input_tokens=20,
                output_tokens=10,
                reasoning_output_tokens=3,
                total_tokens=111,
            ),
        )
        self.assertEqual(result.thread_total_usage, result.usage)
        sidecar_text = (self.root / "success.sidecar.json").read_text(encoding="utf-8")
        sidecar = json.loads(sidecar_text)
        self.assertEqual(sidecar["state"], "completed")
        self.assertEqual(sidecar["app_server_user_agent"], "fake-codex-app-server/0.144.1")
        self.assertEqual(sidecar["usage"]["cached_input_tokens"], 20)
        self.assertEqual(sidecar["output_sha256"], sha256_text(result.output_text or ""))
        self.assertEqual(sidecar["prompt_sha256"], sha256_text(PROMPT))
        self.assertEqual(sidecar["prompt_bytes"], len(PROMPT.encode("utf-8")))
        self.assertEqual(sidecar["instruction_sources_count"], 1)
        self.assertEqual(sidecar["base_instructions_sha256"], sha256_text(BASE_INSTRUCTIONS))
        self.assertNotIn(PROMPT, sidecar_text)
        self.assertNotIn("must-not-appear@example.test", sidecar_text)
        self.assertEqual(
            json.loads((self.root / "success.output.json").read_text(encoding="utf-8")),
            result.output,
        )

    async def test_notifications_are_routed_across_concurrent_ephemeral_threads(self) -> None:
        async with self.client("interleaved") as client:
            first, second = await asyncio.gather(
                self.run_turn(client, stem="first"),
                self.run_turn(client, stem="second"),
            )

        self.assertNotEqual(first.thread_id, second.thread_id)
        self.assertNotEqual(first.turn_id, second.turn_id)
        for result in (first, second):
            self.assertTrue(result.status_ok)
            self.assertEqual(result.output["thread_id"], result.thread_id)
            self.assertEqual(result.output["turn_id"], result.turn_id)
        self.assertEqual(
            sorted(result.usage.input_tokens for result in (first, second) if result.usage),
            [101, 102],
        )

    async def test_large_irrelevant_notification_does_not_break_json_rpc_stream(self) -> None:
        async with self.client("large_notification") as client:
            result = await self.run_turn(client, stem="large-notification")
        self.assertTrue(result.status_ok)
        sidecar = json.loads(
            (self.root / "large-notification.sidecar.json").read_text(encoding="utf-8")
        )
        self.assertEqual(sidecar["max_message_bytes"], 32 * 1024 * 1024)

    async def test_persistent_process_supports_two_warm_turns_on_one_thread(self) -> None:
        async with self.client() as client:
            thread = await client.start_thread(
                model=MODEL,
                base_instructions=BASE_INSTRUCTIONS,
                cwd=self.root,
            )
            first = await client.run_structured_turn(
                thread=thread,
                effort="low",
                prompt=PROMPT,
                output_schema=OUTPUT_SCHEMA,
                sidecar_path=self.root / "warm-1.sidecar.json",
                batch_size=3,
            )
            second = await client.run_structured_turn(
                thread=thread,
                effort="low",
                prompt=PROMPT,
                output_schema=OUTPUT_SCHEMA,
                sidecar_path=self.root / "warm-2.sidecar.json",
                batch_size=5,
            )

        self.assertEqual(first.thread_id, second.thread_id)
        self.assertNotEqual(first.turn_id, second.turn_id)
        self.assertEqual(self.methods("success").count("thread/start"), 1)
        self.assertEqual(self.methods("success").count("turn/start"), 2)

    async def test_durable_thread_resumes_exact_completed_lineage_and_archives(self) -> None:
        expected_sources = sha256_text(
            json.dumps(
                ["/tmp/fake/AGENTS.md"],
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        async with self.client() as client:
            thread = await client.start_thread(
                model=MODEL,
                base_instructions=BASE_INSTRUCTIONS,
                cwd=self.root,
                ephemeral=False,
            )
            self.assertFalse(thread.ephemeral)
            self.assertIsNotNone(thread.persisted_path_sha256)
            first = await client.run_structured_turn(
                thread=thread,
                effort="low",
                prompt=PROMPT,
                output_schema=OUTPUT_SCHEMA,
                sidecar_path=self.root / "durable-1.sidecar.json",
                output_path=self.root / "durable-1.output.json",
                batch_size=3,
            )
            resumed = await client.resume_thread(
                thread_id=thread.thread_id,
                model=MODEL,
                effort="low",
                base_instructions=BASE_INSTRUCTIONS,
                cwd=self.root,
                expected_instruction_sources_sha256=expected_sources,
                expected_instruction_sources_count=1,
                expected_completed_turn_ids=(first.turn_id,),
            )
            self.assertEqual(resumed.thread_id, thread.thread_id)
            self.assertEqual(resumed.reasoning_effort, "low")
            self.assertEqual(resumed.prior_completed_turn_ids, (first.turn_id,))
            self.assertFalse(resumed.ephemeral)
            active = await client.thread_archive_state(resumed.thread_id)
            self.assertEqual(active.state, "active")
            self.assertEqual(active.active_match_count, 1)
            self.assertEqual(active.archived_match_count, 0)
            await client.archive_thread(resumed.thread_id)
            archived = await client.thread_archive_state(resumed.thread_id)
            self.assertEqual(archived.state, "archived")
            self.assertEqual(archived.active_match_count, 0)
            self.assertEqual(archived.archived_match_count, 1)

        methods = self.methods("success")
        self.assertEqual(methods.count("thread/start"), 1)
        self.assertEqual(methods.count("thread/resume"), 1)
        self.assertEqual(methods.count("turn/start"), 1)
        self.assertEqual(methods.count("thread/archive"), 1)
        self.assertEqual(methods.count("thread/list"), 4)

    async def test_timeout_interrupts_once_and_requires_recovery(self) -> None:
        with self.assertRaises(AppServerTurnTimeout):
            async with self.client("timeout") as client:
                await self.run_turn(client, stem="timeout", timeout_seconds=0.03)

        sidecar = json.loads((self.root / "timeout.sidecar.json").read_text(encoding="utf-8"))
        self.assertEqual(sidecar["state"], "interrupted")
        self.assertEqual(sidecar["status"], "timeout")
        self.assertEqual(sidecar["error_class"], "turn_timeout")
        self.assertFalse(sidecar["recovery_reran_model"])
        self.assertEqual(self.methods("timeout").count("turn/interrupt"), 1)
        self.assertEqual(self.methods("timeout").count("turn/start"), 1)

    async def test_child_process_death_fails_turn_and_preserves_sidecar(self) -> None:
        with self.assertRaises(AppServerProcessDied):
            async with self.client("death") as client:
                await self.run_turn(client, stem="death")

        sidecar = json.loads((self.root / "death.sidecar.json").read_text(encoding="utf-8"))
        self.assertEqual(sidecar["state"], "failed")
        self.assertEqual(sidecar["error_class"], "AppServerProcessDied")
        self.assertFalse(sidecar["recovery_reran_model"])

    async def test_malformed_json_fails_closed_and_preserves_sidecar(self) -> None:
        with self.assertRaises(AppServerProtocolError):
            async with self.client("malformed") as client:
                await self.run_turn(client, stem="malformed")

        sidecar = json.loads((self.root / "malformed.sidecar.json").read_text(encoding="utf-8"))
        self.assertEqual(sidecar["state"], "failed")
        self.assertEqual(sidecar["error_class"], "AppServerProtocolError")
        self.assertNotIn("error_detail_text", sidecar)
        self.assertEqual(
            sidecar["error_detail_sha256"],
            sha256_text("app-server emitted malformed JSON"),
        )
        self.assertIn("stderr_sha256", sidecar)

    async def test_failed_turn_is_terminal_and_not_silently_retried(self) -> None:
        async with self.client("failed") as client:
            result = await self.run_turn(client, stem="failed")

        self.assertFalse(result.status_ok)
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.error_class, "turn_failed")
        self.assertEqual(self.methods("failed").count("turn/start"), 1)
        sidecar = json.loads((self.root / "failed.sidecar.json").read_text(encoding="utf-8"))
        self.assertEqual(sidecar["state"], "failed")
        self.assertNotIn("message_text", sidecar["turn_error"])
        self.assertEqual(
            sidecar["turn_error"]["message_sha256"],
            sha256_text("fixture failure"),
        )
        self.assertFalse(sidecar["recovery_reran_model"])

    async def test_invalid_structured_output_fails_closed(self) -> None:
        with self.assertRaises(AppServerStructuredOutputError):
            async with self.client("invalid_output") as client:
                await self.run_turn(client, stem="invalid")

        sidecar = json.loads((self.root / "invalid.sidecar.json").read_text(encoding="utf-8"))
        self.assertEqual(sidecar["state"], "failed")
        self.assertEqual(sidecar["error_class"], "structured_output_invalid")
        self.assertFalse((self.root / "invalid.output.json").exists())

    async def test_existing_started_sidecar_blocks_any_new_thread_or_turn(self) -> None:
        sidecar = self.root / "existing.sidecar.json"
        sidecar.write_text('{"state":"started"}\n', encoding="utf-8")
        async with self.client() as client:
            with self.assertRaises(AppServerRecoveryRequired):
                await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort="low",
                    base_instructions=BASE_INSTRUCTIONS,
                    prompt=PROMPT,
                    output_schema=OUTPUT_SCHEMA,
                    cwd=self.root,
                    sidecar_path=sidecar,
                )

        methods = self.methods("success")
        self.assertNotIn("thread/start", methods)
        self.assertNotIn("turn/start", methods)

    async def test_task_cancellation_interrupts_once_and_writes_terminal_sidecar(self) -> None:
        sidecar_path = self.root / "cancelled.sidecar.json"
        async with self.client("timeout") as client:
            task = asyncio.create_task(
                client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort="low",
                    base_instructions=BASE_INSTRUCTIONS,
                    prompt=PROMPT,
                    output_schema=OUTPUT_SCHEMA,
                    cwd=self.root,
                    sidecar_path=sidecar_path,
                    timeout_seconds=10,
                )
            )
            for _ in range(100):
                if sidecar_path.exists():
                    state = json.loads(sidecar_path.read_text(encoding="utf-8")).get("state")
                    if state == "in_progress":
                        break
                await asyncio.sleep(0.01)
            else:
                self.fail("turn did not reach in_progress")
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
        self.assertEqual(sidecar["state"], "cancelled")
        self.assertEqual(sidecar["usage_status"], "unknown")
        self.assertTrue(sidecar["interrupt_attempted"])
        self.assertTrue(sidecar["interrupt_acknowledged"])
        self.assertTrue(sidecar["protocol_interrupt_sent"])
        self.assertEqual(self.methods("timeout").count("turn/interrupt"), 1)

    def test_orphaned_signal_cancellation_is_finalized_without_claiming_protocol_interrupt(self) -> None:
        sidecar_path = self.root / "orphaned.sidecar.json"
        sidecar_path.write_text(
            json.dumps(
                {
                    "state": "in_progress",
                    "thread_id": "thread-1",
                    "turn_id": "turn-1",
                    "usage": None,
                    "usage_complete": False,
                }
            ),
            encoding="utf-8",
        )
        payload = finalize_cancelled_turn_sidecar(
            sidecar_path,
            interruption_method="single_process_group_sigint",
            parent_exit_code=130,
            protocol_interrupt_sent=False,
        )
        self.assertEqual(payload["state"], "cancelled")
        self.assertEqual(payload["usage_status"], "unknown")
        self.assertFalse(payload["protocol_interrupt_sent"])
        with self.assertRaises(AppServerRecoveryRequired):
            finalize_cancelled_turn_sidecar(
                sidecar_path,
                interruption_method="second_attempt",
                parent_exit_code=130,
                protocol_interrupt_sent=False,
            )


if __name__ == "__main__":
    unittest.main()
