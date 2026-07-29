from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from research_factory.app_server_recovery_readiness import (
    LAUNCH_MAXIMUM_PRIMARY_USED_PERCENT,
    RecoveryReadinessError,
    freeze_recovery_readiness_spec,
    verify_recovery_readiness,
    wait_for_recovery_readiness,
)
from research_factory.app_server_v2_reuse import build_v4_reuse_contract


def _rate_limit_response(used_percent: int, reached_type=None):
    return {
        "rateLimitsByLimitId": {
            "codex": {
                "limitId": "codex",
                "primary": {
                    "usedPercent": used_percent,
                    "resetsAt": 1784487606,
                },
                "rateLimitReachedType": reached_type,
            }
        }
    }


class FakeClock:
    def __init__(self, value: float):
        self.value = float(value)

    def __call__(self) -> float:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.value += seconds


class FakeReadinessClient:
    def __init__(self, responses, *, auth_type="chatgpt"):
        self.responses = list(responses)
        self.account_summary = {"type": auth_type, "plan_type": "pro"}
        self.request_count = 0
        self.thread_start_count = 0
        self.turn_start_count = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def _request(self, method, params):
        self.assert_no_semantic_request(method, params)
        self.request_count += 1
        if not self.responses:
            raise AssertionError("unexpected readiness recheck")
        return self.responses.pop(0)

    def assert_no_semantic_request(self, method, params):
        if method != "account/rateLimits/read" or params != {}:
            raise AssertionError("readiness attempted a thread or semantic turn")


class RecoveryReadinessTest(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repo = Path(__file__).resolve().parents[1]
        if not (
            cls.repo
            / "work/app-server-development-v2/unattended-pipeline-v3/pipeline-terminal.json"
        ).is_file():
            raise unittest.SkipTest("immutable pipeline-v3 overload evidence unavailable")

    async def test_cooldown_wait_and_two_clear_probes_precede_semantic_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            contract_path = root / "unattended-pipeline-v4/reuse-contract-v3.json"
            build_v4_reuse_contract(
                repo_root=self.repo,
                output_path=contract_path,
            )
            readiness_root = root / "unattended-pipeline-v4/provider-readiness-v1"
            spec = freeze_recovery_readiness_spec(
                reuse_contract_path=contract_path,
                output_dir=readiness_root,
            )
            clock = FakeClock(spec["semantic_launch_not_before_epoch"] - 10)
            client = FakeReadinessClient(
                [
                    _rate_limit_response(10),
                    _rate_limit_response(1),
                    _rate_limit_response(1),
                ]
            )
            terminal = await wait_for_recovery_readiness(
                reuse_contract_path=contract_path,
                output_dir=readiness_root,
                client_factory=lambda: client,
                sleep=clock.sleep,
                clock=clock,
            )
            self.assertEqual(terminal["status"], "ready")
            self.assertEqual(
                terminal["launch_maximum_primary_used_percent"],
                LAUNCH_MAXIMUM_PRIMARY_USED_PERCENT,
            )
            self.assertEqual(terminal["final_primary_used_percent"], 1)
            self.assertEqual(terminal["total_probe_count"], 3)
            self.assertEqual(len(terminal["qualifying_probes"]), 2)
            self.assertTrue(terminal["cooldown_satisfied"])
            self.assertEqual(terminal["semantic_turns_started"], 0)
            self.assertEqual(client.request_count, 3)
            self.assertEqual(client.thread_start_count, 0)
            self.assertEqual(client.turn_start_count, 0)
            self.assertEqual(
                verify_recovery_readiness(
                    reuse_contract_path=contract_path,
                    output_dir=readiness_root,
                ),
                terminal,
            )

            def forbidden_client():
                self.fail("a terminal readiness version must be adopted without transport")

            adopted = await wait_for_recovery_readiness(
                reuse_contract_path=contract_path,
                output_dir=readiness_root,
                client_factory=forbidden_client,
                sleep=clock.sleep,
                clock=clock,
            )
            self.assertEqual(adopted, terminal)

    async def test_auth_drift_fails_immutably_without_thread_or_turn(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            contract_path = root / "unattended-pipeline-v4/reuse-contract-v3.json"
            build_v4_reuse_contract(
                repo_root=self.repo,
                output_path=contract_path,
            )
            readiness_root = root / "unattended-pipeline-v4/provider-readiness-v1"
            spec = freeze_recovery_readiness_spec(
                reuse_contract_path=contract_path,
                output_dir=readiness_root,
            )
            clock = FakeClock(spec["semantic_launch_not_before_epoch"] + 1)
            client = FakeReadinessClient([], auth_type="apikey")
            with self.assertRaisesRegex(RecoveryReadinessError, "failed closed"):
                await wait_for_recovery_readiness(
                    reuse_contract_path=contract_path,
                    output_dir=readiness_root,
                    client_factory=lambda: client,
                    sleep=clock.sleep,
                    clock=clock,
                )
            failure_path = readiness_root / "readiness-failure.json"
            self.assertTrue(failure_path.is_file())
            self.assertEqual(client.request_count, 0)
            self.assertEqual(client.thread_start_count, 0)
            self.assertEqual(client.turn_start_count, 0)

            def forbidden_client():
                self.fail("immutable readiness failure must never reopen transport")

            with self.assertRaisesRegex(RecoveryReadinessError, "already exists"):
                await wait_for_recovery_readiness(
                    reuse_contract_path=contract_path,
                    output_dir=readiness_root,
                    client_factory=forbidden_client,
                    sleep=clock.sleep,
                    clock=clock,
                )


if __name__ == "__main__":
    unittest.main()
