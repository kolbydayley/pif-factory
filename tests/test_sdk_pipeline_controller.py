from __future__ import annotations

import datetime as dt
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory.sdk_pipeline_controller import (
    ACTIVATION_SCHEMA_VERSION,
    FRESH_THREAD_STRATEGY,
    LEGACY_RESUME_STRATEGY,
    AuthBoundaryError,
    ControllerStore,
    OfficialCodexSDKTransport,
    SDKPipelineController,
    TurnOutcome,
    TurnReconciliation,
    activation_status,
    build_parser,
    build_recovery_capsule,
    controller_from_args,
    default_checkpoint,
    load_checkpoint,
    numeric_usage,
)


NOW = dt.datetime(2026, 7, 14, 1, 0, tzinfo=dt.timezone(dt.timedelta(hours=-4)))


class MutableClock:
    def __init__(self, value: dt.datetime = NOW) -> None:
        self.value = value

    def __call__(self) -> dt.datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += dt.timedelta(seconds=seconds)


def observation(*, active: bool = False, complete: bool = False) -> dict:
    return {
        "phase": "complete" if complete else "evaluation_watch",
        "thread_id": "legacy-thread-1",
        "thread": {
            "turn_in_progress": active,
            "session_turn_in_progress": active,
            "recent_sha256": "session-sha",
            "control_process": {"alive": active},
            "raw_private_transcript": "private transcript marker",
        },
        "goal": {"status": "blocked", "raw_goal": "private goal marker"},
        "milestones": {
            "snapshot_sha256": "milestone-sha",
            "newest_path": "work/evaluation/v26/terminal.json",
        },
        "queue_sha256": None,
        "workflow_complete": complete,
        "session_name": "pif-evaluation-legacy-thread-1",
        "prior_exit_receipt": {"status": "exited", "exit_code": 0, "finished_at": None},
    }


def write_activation(path: Path, project_root: Path, *, expires_in_hours: int = 1) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": ACTIVATION_SCHEMA_VERSION,
                "enabled": True,
                "project_root": str(project_root.resolve()),
                "expires_at": (NOW + dt.timedelta(hours=expires_in_hours)).isoformat(),
            }
        ),
        encoding="utf-8",
    )


def default_outcome(
    *, thread_id: str = "fresh-thread-1", turn_id: str = "turn-1"
) -> TurnOutcome:
    return TurnOutcome(
        thread_id=thread_id,
        turn_id=turn_id,
        status="completed",
        final_response="private final response marker",
        usage={
            "last": {
                "inputTokens": 10,
                "cachedInputTokens": 3,
                "outputTokens": 2,
                "reasoningOutputTokens": 1,
                "totalTokens": 13,
            },
            "modelContextWindow": 200000,
        },
        duration_ms=250,
    )


class FakeTransport:
    def __init__(
        self,
        *,
        outcome: TurnOutcome | None = None,
        reconcile: TurnReconciliation | None = None,
        fail_after_accept: bool = False,
    ) -> None:
        self.outcome = outcome or default_outcome()
        self.reconcile = reconcile or TurnReconciliation(
            state="terminal", turn_status="completed"
        )
        self.fail_after_accept = fail_after_accept
        self.open_count = 0
        self.close_count = 0
        self.start_calls: list[dict] = []
        self.resume_calls: list[dict] = []
        self.reconcile_calls: list[dict] = []

    def open(self) -> None:
        self.open_count += 1

    def close(self) -> None:
        self.close_count += 1

    def start_and_run(
        self,
        *,
        prompt: str,
        cwd: Path,
        on_thread_ready=None,
        on_turn_started=None,
    ) -> TurnOutcome:
        self.start_calls.append({"prompt": prompt, "cwd": cwd})
        if on_thread_ready is not None:
            on_thread_ready(self.outcome.thread_id)
        if on_turn_started is not None:
            on_turn_started(self.outcome.thread_id, self.outcome.turn_id)
        if self.fail_after_accept:
            raise ConnectionError("synthetic disconnect after turn acceptance")
        return self.outcome

    def resume_and_run(
        self,
        *,
        thread_id: str,
        prompt: str,
        cwd: Path,
        on_thread_ready=None,
        on_turn_started=None,
    ) -> TurnOutcome:
        self.resume_calls.append({"thread_id": thread_id, "prompt": prompt, "cwd": cwd})
        outcome = default_outcome(thread_id=thread_id, turn_id=self.outcome.turn_id)
        if on_thread_ready is not None:
            on_thread_ready(outcome.thread_id)
        if on_turn_started is not None:
            on_turn_started(outcome.thread_id, outcome.turn_id)
        if self.fail_after_accept:
            raise ConnectionError("synthetic disconnect after turn acceptance")
        return outcome

    def reconcile_turn(
        self,
        *,
        thread_id: str,
        turn_id: str | None,
        cwd: Path,
        discover_single_turn: bool = False,
    ) -> TurnReconciliation:
        self.reconcile_calls.append(
            {"thread_id": thread_id, "turn_id": turn_id, "cwd": cwd}
        )
        return self.reconcile


class SDKPipelineControllerTests(unittest.TestCase):
    def make_controller(
        self,
        root: Path,
        *,
        observed: dict | None = None,
        observer=None,
        allow: bool,
        transport: FakeTransport,
        strategy: str = FRESH_THREAD_STRATEGY,
        clock: MutableClock | None = None,
    ) -> SDKPipelineController:
        return SDKPipelineController(
            project_root=root,
            store=ControllerStore(
                checkpoint_path=root / "checkpoint.json",
                ledger_path=root / "ledger.jsonl",
            ),
            activation_path=root / "activation.json",
            allow_model_turns=allow,
            observer=observer or (lambda: observed or observation()),
            transport_factory=lambda: transport,
            thread_strategy=strategy,
            clock=clock or MutableClock(),
        )

    def test_default_is_fail_closed_and_does_not_open_transport(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            transport = FakeTransport()
            result = self.make_controller(
                root, observed=observation(), allow=False, transport=transport
            ).tick()
            self.assertEqual(result["status"], "disabled")
            self.assertEqual(result["activation_reason"], "model_turn_flag_absent")
            self.assertEqual(transport.open_count, 0)
            self.assertEqual(transport.start_calls, [])
            self.assertEqual(load_checkpoint(root / "checkpoint.json")["phase"], "disabled")

    def test_active_external_turn_is_never_interrupted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_activation(root / "activation.json", root)
            transport = FakeTransport()
            result = self.make_controller(
                root, observed=observation(active=True), allow=True, transport=transport
            ).tick()
            self.assertEqual(result["status"], "observing_active_turn")
            self.assertEqual(transport.open_count, 0)

    def test_complete_pipeline_never_starts_transport(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_activation(root / "activation.json", root)
            transport = FakeTransport()
            result = self.make_controller(
                root, observed=observation(complete=True), allow=True, transport=transport
            ).tick()
            self.assertEqual(result["status"], "complete")
            self.assertEqual(transport.open_count, 0)

    def test_fresh_bounded_thread_is_default_and_capsule_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_activation(root / "activation.json", root)
            transport = FakeTransport()
            result = self.make_controller(
                root, observed=observation(), allow=True, transport=transport
            ).tick()
            self.assertEqual(result["status"], "turn_completed")
            self.assertEqual(len(transport.start_calls), 1)
            self.assertEqual(transport.resume_calls, [])
            prompt = transport.start_calls[0]["prompt"]
            self.assertIn("RECOVERY_CAPSULE_JSON", prompt)
            self.assertIn("managed ChatGPT auth", prompt)
            self.assertNotIn("legacy-thread-1", prompt)
            self.assertNotIn("private transcript marker", prompt)
            self.assertNotIn("private goal marker", prompt)
            checkpoint = load_checkpoint(root / "checkpoint.json")
            self.assertEqual(checkpoint["thread_strategy"], FRESH_THREAD_STRATEGY)
            self.assertEqual(checkpoint["last_controller_thread_id"], "fresh-thread-1")
            self.assertIsNone(checkpoint["pending_turn"])
            self.assertEqual(
                checkpoint["last_turn_usage"]["last"]["cachedInputTokens"], 3
            )
            ledger = (root / "ledger.jsonl").read_text(encoding="utf-8")
            self.assertNotIn("private final response marker", ledger)
            self.assertNotIn("fresh-thread-1", ledger)
            self.assertIn("sdk_turn_accepted", ledger)

    def test_legacy_exact_resume_requires_explicit_strategy(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_activation(root / "activation.json", root)
            transport = FakeTransport()
            result = self.make_controller(
                root,
                observed=observation(),
                allow=True,
                transport=transport,
                strategy=LEGACY_RESUME_STRATEGY,
            ).tick()
            self.assertEqual(result["status"], "turn_completed")
            self.assertEqual(transport.start_calls, [])
            self.assertEqual(transport.resume_calls[0]["thread_id"], "legacy-thread-1")

    def test_recovery_capsule_is_bounded_and_drops_unapproved_fields_and_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            observed = observation()
            observed["thread"]["raw_private_transcript"] = "LEAK-ME" * 10000
            observed["milestones"]["newest_path"] = "/private/outside/secret.json"
            capsule = build_recovery_capsule(
                observed,
                default_checkpoint(NOW),
                recovery_id="recovery-1",
                project_root=root,
                created_at=NOW,
            )
            encoded = json.dumps(capsule, sort_keys=True).encode("utf-8")
            self.assertLessEqual(len(encoded), 6 * 1024)
            self.assertNotIn(b"LEAK-ME", encoded)
            self.assertNotIn(b"/private/outside", encoded)
            self.assertIsNone(
                capsule["latest_immutable_milestone"]["relative_path"]
            )
            self.assertIsNotNone(
                capsule["pipeline"]["registered_thread_id_sha256"]
            )

    def test_disconnect_after_acceptance_persists_ambiguous_turn(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_activation(root / "activation.json", root)
            transport = FakeTransport(fail_after_accept=True)
            result = self.make_controller(
                root, observed=observation(), allow=True, transport=transport
            ).tick()
            self.assertEqual(result["status"], "turn_outcome_ambiguous")
            checkpoint = load_checkpoint(root / "checkpoint.json")
            self.assertEqual(checkpoint["pending_turn"]["thread_id"], "fresh-thread-1")
            self.assertEqual(checkpoint["pending_turn"]["turn_id"], "turn-1")
            self.assertEqual(checkpoint["phase"], "reconciliation_required")

    def test_restart_reconciles_active_ambiguous_turn_without_resend(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_activation(root / "activation.json", root)
            clock = MutableClock()
            first = FakeTransport(fail_after_accept=True)
            self.make_controller(
                root,
                observed=observation(),
                allow=True,
                transport=first,
                clock=clock,
            ).tick()
            clock.advance(61)
            second = FakeTransport(
                reconcile=TurnReconciliation(state="active", turn_status="inProgress")
            )
            result = self.make_controller(
                root,
                observed=observation(),
                allow=True,
                transport=second,
                clock=clock,
            ).tick()
            self.assertEqual(result["status"], "turn_still_active")
            self.assertEqual(second.start_calls, [])
            self.assertEqual(second.resume_calls, [])
            self.assertEqual(len(second.reconcile_calls), 1)
            self.assertIsNotNone(load_checkpoint(root / "checkpoint.json")["pending_turn"])

    def test_restart_reconciles_terminal_turn_then_waits_for_next_tick(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_activation(root / "activation.json", root)
            clock = MutableClock()
            first = FakeTransport(fail_after_accept=True)
            self.make_controller(
                root,
                observed=observation(),
                allow=True,
                transport=first,
                clock=clock,
            ).tick()
            clock.advance(61)
            second = FakeTransport(
                reconcile=TurnReconciliation(state="terminal", turn_status="completed")
            )
            result = self.make_controller(
                root,
                observed=observation(),
                allow=True,
                transport=second,
                clock=clock,
            ).tick()
            self.assertEqual(result["status"], "turn_reconciled_terminal")
            self.assertEqual(second.start_calls, [])
            checkpoint = load_checkpoint(root / "checkpoint.json")
            self.assertIsNone(checkpoint["pending_turn"])
            self.assertEqual(checkpoint["last_turn_status"], "completed")

    def test_unknown_reconciliation_never_resends(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_activation(root / "activation.json", root)
            checkpoint = default_checkpoint(NOW)
            checkpoint["pending_turn"] = {
                "thread_id": "fresh-thread-1",
                "turn_id": "turn-1",
                "state": "outcome_ambiguous",
            }
            ControllerStore(
                checkpoint_path=root / "checkpoint.json",
                ledger_path=root / "ledger.jsonl",
            ).save(checkpoint, NOW)
            transport = FakeTransport(
                reconcile=TurnReconciliation(state="unknown", error_class="turn_not_found")
            )
            result = self.make_controller(
                root, observed=observation(), allow=True, transport=transport
            ).tick()
            self.assertEqual(result["status"], "turn_reconciliation_unknown")
            self.assertEqual(transport.start_calls, [])
            self.assertEqual(transport.resume_calls, [])
            self.assertIsNotNone(load_checkpoint(root / "checkpoint.json")["pending_turn"])

    def test_fresh_thread_ready_without_turn_is_reconciled_without_resend(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_activation(root / "activation.json", root)
            checkpoint = default_checkpoint(NOW)
            checkpoint["pending_turn"] = {
                "thread_id": "fresh-thread-1",
                "turn_id": None,
                "thread_strategy": FRESH_THREAD_STRATEGY,
                "state": "turn_acceptance_unknown",
            }
            ControllerStore(
                checkpoint_path=root / "checkpoint.json",
                ledger_path=root / "ledger.jsonl",
            ).save(checkpoint, NOW)
            transport = FakeTransport(
                reconcile=TurnReconciliation(state="not_started")
            )
            result = self.make_controller(
                root, observed=observation(), allow=True, transport=transport
            ).tick()
            self.assertEqual(result["status"], "turn_not_started_reconciled")
            self.assertEqual(transport.start_calls, [])
            self.assertEqual(transport.resume_calls, [])
            self.assertIsNone(load_checkpoint(root / "checkpoint.json")["pending_turn"])

    def test_ambiguous_turn_without_activation_is_preserved_without_sdk_open(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            checkpoint = default_checkpoint(NOW)
            checkpoint["pending_turn"] = {
                "thread_id": "fresh-thread-1",
                "turn_id": "turn-1",
                "state": "outcome_ambiguous",
            }
            ControllerStore(
                checkpoint_path=root / "checkpoint.json",
                ledger_path=root / "ledger.jsonl",
            ).save(checkpoint, NOW)
            transport = FakeTransport()
            result = self.make_controller(
                root, observed=observation(), allow=False, transport=transport
            ).tick()
            self.assertEqual(result["status"], "ambiguous_turn_requires_activation")
            self.assertEqual(transport.open_count, 0)
            self.assertIsNotNone(load_checkpoint(root / "checkpoint.json")["pending_turn"])

    def test_bounded_observation_failure_cannot_start_sdk(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_activation(root / "activation.json", root)
            transport = FakeTransport()

            def fail_observation():
                raise TimeoutError("bounded observation timed out")

            result = self.make_controller(
                root,
                observer=fail_observation,
                allow=True,
                transport=transport,
            ).tick()
            self.assertEqual(result["status"], "observation_failed")
            self.assertEqual(transport.open_count, 0)

    def test_controller_factory_routes_observation_through_deadline_helper(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            args = build_parser().parse_args(
                [
                    "--project-root",
                    str(root),
                    "--checkpoint",
                    str(root / "checkpoint.json"),
                    "--ledger",
                    str(root / "ledger.jsonl"),
                    "--observation-timeout-seconds",
                    "7",
                    "once",
                ]
            )
            with patch(
                "research_factory.sdk_pipeline_controller.observe_with_deadline",
                return_value=observation(),
            ) as bounded:
                controller = controller_from_args(args)
                controller.observer()
            self.assertEqual(bounded.call_args.kwargs["timeout_seconds"], 7)

    def test_activation_requires_matching_project_and_future_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            other = root / "other"
            other.mkdir()
            write_activation(root / "activation.json", other)
            status = activation_status(
                allow_model_turns=True,
                activation_path=root / "activation.json",
                project_root=root,
                now=NOW,
            )
            self.assertFalse(status.active)
            self.assertEqual(status.reason, "activation_project_mismatch")


class OfficialSDKTransportTests(unittest.TestCase):
    def test_nested_sdk_usage_keeps_only_numeric_telemetry(self) -> None:
        usage = types.SimpleNamespace(
            model_dump=lambda **_kwargs: {
                "last": {
                    "inputTokens": 12,
                    "cachedInputTokens": 5,
                    "provider": "openai",
                },
                "modelContextWindow": 200000,
                "debug": True,
            }
        )
        self.assertEqual(
            numeric_usage(usage),
            {
                "last": {"inputTokens": 12, "cachedInputTokens": 5},
                "modelContextWindow": 200000,
            },
        )

    def test_api_key_account_is_rejected_before_any_thread_call(self) -> None:
        events: list[str] = []

        class FakeCodexConfig:
            def __init__(self, **kwargs) -> None:
                self.kwargs = kwargs

        class FakeCodex:
            def __init__(self, config) -> None:
                self.config = config

            def __enter__(self):
                events.append("enter")
                return self

            def __exit__(self, *_args):
                events.append("exit")

            def account(self, *, refresh_token: bool):
                return types.SimpleNamespace(
                    account=types.SimpleNamespace(root=types.SimpleNamespace(type="apiKey"))
                )

        fake_module = types.SimpleNamespace(Codex=FakeCodex, CodexConfig=FakeCodexConfig)
        transport = OfficialCodexSDKTransport(
            project_root=Path.cwd(), module_loader=lambda _name: fake_module
        )
        with self.assertRaises(AuthBoundaryError):
            transport.open()
        self.assertEqual(events, ["enter", "exit"])

    def test_chatgpt_transport_starts_fresh_thread_and_persists_ids(self) -> None:
        events: list[object] = []

        class FakeCodexConfig:
            def __init__(self, **kwargs) -> None:
                events.append(("config", kwargs))

        class FakeTurnHandle:
            id = "sdk-turn-42"

            def run(self):
                events.append("collect")
                return types.SimpleNamespace(
                    id=self.id,
                    status=types.SimpleNamespace(value="completed"),
                    final_response="done",
                    usage=types.SimpleNamespace(
                        model_dump=lambda **_kwargs: {
                            "last": {"inputTokens": 9, "cachedInputTokens": 4}
                        }
                    ),
                    started_at=1,
                    completed_at=2,
                    duration_ms=1000,
                )

        class FakeThread:
            id = "sdk-thread-7"

            def turn(self, prompt, **kwargs):
                events.append(("turn", prompt, kwargs))
                return FakeTurnHandle()

        class FakeCodex:
            def __init__(self, config) -> None:
                self.config = config

            def __enter__(self):
                events.append("enter")
                return self

            def __exit__(self, *_args):
                events.append("exit")

            def account(self, *, refresh_token: bool):
                events.append(("account", refresh_token))
                return types.SimpleNamespace(
                    account=types.SimpleNamespace(
                        root=types.SimpleNamespace(type="chatgpt")
                    )
                )

            def thread_start(self, **kwargs):
                events.append(("start", kwargs))
                return FakeThread()

        fake_module = types.SimpleNamespace(
            Codex=FakeCodex,
            CodexConfig=FakeCodexConfig,
            ApprovalMode=types.SimpleNamespace(deny_all="deny_all"),
            Sandbox=types.SimpleNamespace(workspace_write="workspace_write"),
        )
        accepted: list[tuple[str, str]] = []
        transport = OfficialCodexSDKTransport(
            project_root=Path.cwd(), module_loader=lambda _name: fake_module
        )
        outcome = transport.start_and_run(
            prompt="continue",
            cwd=Path.cwd(),
            on_turn_started=lambda thread_id, turn_id: accepted.append(
                (thread_id, turn_id)
            ),
        )
        transport.close()
        self.assertEqual(accepted, [("sdk-thread-7", "sdk-turn-42")])
        self.assertEqual(outcome.thread_id, "sdk-thread-7")
        self.assertEqual(outcome.turn_id, "sdk-turn-42")
        self.assertEqual(outcome.usage["last"]["cachedInputTokens"], 4)
        self.assertLess(events.index(("account", False)), events.index("collect"))
        self.assertEqual(events[-1], "exit")

    def test_reconciliation_reads_turn_history_without_starting_turn(self) -> None:
        events: list[str] = []

        class FakeCodexConfig:
            def __init__(self, **_kwargs) -> None:
                pass

        class FakeThread:
            id = "sdk-thread-7"

            def read(self, *, include_turns: bool):
                events.append("read")
                return types.SimpleNamespace(
                    thread=types.SimpleNamespace(
                        turns=[
                            types.SimpleNamespace(
                                id="sdk-turn-42",
                                status=types.SimpleNamespace(value="completed"),
                            )
                        ]
                    )
                )

            def turn(self, *_args, **_kwargs):
                events.append("turn")
                raise AssertionError("reconciliation must not start a turn")

        class FakeCodex:
            def __init__(self, _config) -> None:
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                pass

            def account(self, *, refresh_token: bool):
                return types.SimpleNamespace(
                    account=types.SimpleNamespace(
                        root=types.SimpleNamespace(type="chatgpt")
                    )
                )

            def thread_resume(self, _thread_id, **_kwargs):
                events.append("resume")
                return FakeThread()

        fake_module = types.SimpleNamespace(
            Codex=FakeCodex,
            CodexConfig=FakeCodexConfig,
            ApprovalMode=types.SimpleNamespace(deny_all="deny_all"),
            Sandbox=types.SimpleNamespace(workspace_write="workspace_write"),
        )
        transport = OfficialCodexSDKTransport(
            project_root=Path.cwd(), module_loader=lambda _name: fake_module
        )
        result = transport.reconcile_turn(
            thread_id="sdk-thread-7",
            turn_id="sdk-turn-42",
            cwd=Path.cwd(),
        )
        transport.close()
        self.assertEqual(result.state, "terminal")
        self.assertEqual(result.turn_status, "completed")
        self.assertEqual(events, ["resume", "read"])


if __name__ == "__main__":
    unittest.main()
