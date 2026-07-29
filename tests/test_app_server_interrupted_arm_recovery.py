from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory.app_server_interrupted_arm_recovery import (
    InterruptedArmRecovery,
    RecoveryError,
    RecoveryStopped,
    finalize_operator_interrupted_sidecar,
    inspect_batch_inventory,
    probe_app_server_rate_limits,
    _read_only_connection,
)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def usage(seed: int = 1) -> dict:
    return {
        "input_tokens": 100 * seed,
        "cached_input_tokens": 0,
        "output_tokens": 10 * seed,
        "reasoning_output_tokens": seed,
        "total_tokens": 110 * seed,
    }


class InterruptedArmRecoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.repo = Path(self.tempdir.name) / "repo"
        self.repo.mkdir()
        self.matrix = self.repo / "work" / "matrix-v1"
        self.arm = self.matrix / "batch-5" / "same_thread"
        self.arm.mkdir(parents=True)
        self.spec_path = self.repo / "run-spec-v2.json"
        self.spec_path.write_text(
            json.dumps(
                {
                    "candidate": {
                        "model": "gpt-5.6-sol",
                        "reasoning_effort": "low",
                    }
                }
            )
            + "\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _recovery(self, **kwargs) -> InterruptedArmRecovery:
        return InterruptedArmRecovery(
            repo_root=self.repo,
            run_spec_path=self.spec_path,
            matrix_root=self.matrix,
            not_before_epoch=100,
            boundary_grace_seconds=10,
            **kwargs,
        )

    def _mapping_fixture(self):
        batches = []
        for index in range(8):
            batch_id = f"batch_{index}"
            sidecar = self.arm / "sidecars" / f"{batch_id}.json"
            raw = self.arm / "raw_outputs" / f"{batch_id}.json"
            normalized = self.arm / "normalized_outputs" / f"{batch_id}.json"
            batches.append(
                {
                    "batch_id": batch_id,
                    "episode_id": f"episode_{index // 2}",
                    "segment_ids": [f"segment_{index}_{item}" for item in range(4)],
                    "sidecar_path": str(sidecar),
                    "raw_output_path": str(raw),
                    "normalized_output_path": str(normalized),
                }
            )
            if index < 4:
                sidecar.parent.mkdir(parents=True, exist_ok=True)
                raw.parent.mkdir(parents=True, exist_ok=True)
                normalized.parent.mkdir(parents=True, exist_ok=True)
                raw.write_text(json.dumps({"raw": index}) + "\n", encoding="utf-8")
                normalized.write_text(
                    json.dumps({"segments": [{"events": []} for _ in range(4)]}) + "\n",
                    encoding="utf-8",
                )
                sidecar.write_text(
                    json.dumps(
                        {
                            "schema_version": "pif_codex_app_server_turn_v2",
                            "state": "completed",
                            "usage_complete": True,
                            "usage": usage(index + 1),
                            "output_sha256": sha(raw),
                            "instruction_sources_sha256": "a" * 64,
                            "instruction_sources_count": 1,
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
            elif index == 4:
                sidecar.parent.mkdir(parents=True, exist_ok=True)
                sidecar.write_text(
                    json.dumps(
                        {
                            "schema_version": "pif_codex_app_server_turn_v2",
                            "state": "cancelled",
                            "turn_id": "turn_interrupted",
                            "usage": None,
                            "usage_complete": False,
                            "usage_status": "unknown",
                            "recovery_reran_model": False,
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
        mapping = {
            "schema_version": "pif_app_server_core_arm_v3",
            "batch_size": 5,
            "thread_mode": "same_thread",
            "batches": batches,
        }
        mapping_path = self.arm / "private-mapping.json"
        mapping_path.write_text(json.dumps(mapping) + "\n", encoding="utf-8")
        return mapping

    def _configure_clean_batch8_fixture(self, recovery: InterruptedArmRecovery) -> str:
        source_sha = "c" * 64
        recovery.spec = {
            "candidate": {
                "model": "gpt-5.6-sol",
                "reasoning_effort": "low",
                "concurrency": 1,
                "timeout_seconds": 60,
                "window_count": 4,
                "context_chars": 900,
                "event_cap": 32,
            },
            "manifest": {
                "artifact_path": str(self.repo / "manifest.json"),
                "artifact_sha256": "d" * 64,
            },
            "frozen_artifacts": {
                "candidate_guideline": {
                    "artifact_path": str(self.repo / "guideline.json"),
                }
            },
            "instruction_contract": {
                "expected_path_set_sha256": source_sha,
                "sources": [{"path": str(self.repo / "AGENTS.md")}],
            },
        }
        recovery.recovery_root.mkdir(parents=True, exist_ok=True)
        recovery._load_state()
        return source_sha

    def test_finalize_in_progress_sidecar_preserves_unknown_usage_without_model_work(self) -> None:
        sidecar = self.arm / "sidecars" / "interrupted.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(
            json.dumps(
                {
                    "schema_version": "pif_codex_app_server_turn_v2",
                    "state": "in_progress",
                    "turn_id": "turn_1",
                    "usage": None,
                    "usage_complete": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        original_sha = sha(sidecar)

        result = finalize_operator_interrupted_sidecar(
            sidecar,
            interruption_evidence_sha256="b" * 64,
            finished_at="2026-07-12T05:20:00+00:00",
        )

        self.assertEqual(result["state"], "cancelled")
        self.assertIsNone(result["usage"])
        self.assertFalse(result["usage_complete"])
        self.assertEqual(result["usage_status"], "unknown")
        self.assertFalse(result["recovery_reran_model"])
        self.assertEqual(result["pre_recovery_sidecar_sha256"], original_sha)

    def test_inventory_adopts_four_completed_one_cancelled_and_three_unstarted(self) -> None:
        mapping = self._mapping_fixture()

        inventory = inspect_batch_inventory(mapping=mapping, arm_dir=self.arm)

        statuses = [item["status"] for item in inventory]
        self.assertEqual(statuses.count("completed_adopted"), 4)
        self.assertEqual(statuses.count("operator_interrupted_usage_unknown"), 1)
        self.assertEqual(statuses.count("unstarted"), 3)

    def test_intent_to_treat_artifact_freezes_exact_original_split(self) -> None:
        mapping = self._mapping_fixture()
        recovery = self._recovery()
        supervisor = self.matrix / "supervisor"
        supervisor.mkdir()
        (supervisor / "state.json").write_text("{}\n", encoding="utf-8")
        (supervisor / "journal.jsonl").write_text("{}\n", encoding="utf-8")
        inventory = inspect_batch_inventory(mapping=mapping, arm_dir=self.arm)

        recovery._write_intent_to_treat_provenance(mapping, inventory)

        payload = json.loads(recovery.itt_provenance_path.read_text(encoding="utf-8"))
        self.assertEqual(
            payload["schema_version"],
            "pif_app_server_arm_intent_to_treat_interruption_v1",
        )
        self.assertEqual(payload["attempted_calls"], 5)
        self.assertEqual(payload["completed_measured_calls"], 4)
        self.assertEqual(payload["cancelled_unknown_usage_calls"], 1)
        self.assertEqual(payload["not_started_calls"], 3)
        self.assertFalse(payload["selection_eligible"])
        self.assertIsNone(payload["usage"])

    def test_unstarted_batch_recovery_is_unconditionally_prohibited(self) -> None:
        recovery = self._recovery()
        with self.assertRaisesRegex(RecoveryError, "terminal"):
            asyncio.run(recovery._recover_unstarted({}, []))

    def test_boundary_wait_does_not_release_early(self) -> None:
        current = {"value": 100.0}
        sleeps = []

        def clock():
            return current["value"]

        def sleep(seconds):
            sleeps.append(seconds)
            current["value"] += seconds

        recovery = self._recovery(clock=clock, sleep=sleep)
        recovery.recovery_root.mkdir(parents=True)
        recovery._load_state()

        recovery._wait_for_boundary()

        self.assertGreaterEqual(current["value"], 110)
        self.assertTrue(sleeps)
        self.assertEqual(recovery.state["status"], "preflighting_clean_batch8")

    def test_recovery_database_connection_is_uri_read_only(self) -> None:
        database = self.repo / "factory.sqlite"
        connection = sqlite3.connect(database)
        connection.execute("CREATE TABLE evidence (id INTEGER PRIMARY KEY)")
        connection.commit()
        connection.close()
        with patch(
            "research_factory.app_server_interrupted_arm_recovery.db_path",
            return_value=database,
        ):
            read_only = _read_only_connection()
        try:
            self.assertEqual(read_only.execute("PRAGMA query_only").fetchone()[0], 1)
            with self.assertRaises(sqlite3.OperationalError):
                read_only.execute("INSERT INTO evidence DEFAULT VALUES")
        finally:
            read_only.close()

    def test_public_rate_limit_probe_uses_managed_auth_without_thread_or_turn(self) -> None:
        calls = []

        class FakeClient:
            account_summary = {"type": "chatgpt", "plan_type": "pro"}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def _request(self, method, params):
                calls.append((method, params))
                return {
                    "rateLimits": {
                        "limitId": "codex",
                        "primary": {"usedPercent": 4, "resetsAt": 200},
                        "rateLimitReachedType": None,
                    }
                }

        probe = asyncio.run(
            probe_app_server_rate_limits(client_factory=FakeClient)
        )

        self.assertEqual(calls, [("account/rateLimits/read", {})])
        self.assertTrue(probe["managed_chatgpt_auth_verified"])
        self.assertTrue(probe["cleared_for_semantic_work"])
        self.assertFalse(probe["thread_started"])
        self.assertFalse(probe["turn_started"])

    def test_live_rate_limit_gate_waits_and_rechecks_before_clearance(self) -> None:
        probes = [
            {
                "managed_chatgpt_auth_verified": True,
                "primary_used_percent": 96,
                "primary_resets_at": 130,
                "rate_limit_reached_type": "rate_limit_reached",
                "cleared_for_semantic_work": False,
                "thread_started": False,
                "turn_started": False,
            },
            {
                "managed_chatgpt_auth_verified": True,
                "primary_used_percent": 0,
                "primary_resets_at": 500,
                "rate_limit_reached_type": None,
                "cleared_for_semantic_work": True,
                "thread_started": False,
                "turn_started": False,
            },
        ]
        sleeps = []
        current = {"value": 110.0}

        def probe(**_kwargs):
            return probes.pop(0)

        def sleep(seconds):
            sleeps.append(seconds)
            current["value"] += seconds

        recovery = self._recovery(
            clock=lambda: current["value"],
            sleep=sleep,
            rate_limit_probe=probe,
            rate_limit_recheck_seconds=60,
        )
        recovery.recovery_root.mkdir(parents=True)
        recovery._load_state()

        result = recovery._wait_for_live_rate_limit_clear()

        self.assertTrue(result["cleared_for_semantic_work"])
        self.assertEqual(len(sleeps), 1)
        self.assertTrue(recovery.rate_limit_clearance_path.is_file())

    def test_real_async_batch8_boundary_waits_then_reaches_runner_without_nested_event_loop(self) -> None:
        events = []
        responses = [
            {
                "rateLimits": {
                    "limitId": "codex",
                    "primary": {"usedPercent": 91, "resetsAt": None},
                    "rateLimitReachedType": "rate_limit_reached",
                }
            },
            {
                "rateLimits": {
                    "limitId": "codex",
                    "primary": {"usedPercent": 0, "resetsAt": None},
                    "rateLimitReachedType": None,
                }
            },
        ]

        class FakeManagedClient:
            account_summary = {"type": "chatgpt", "plan_type": "pro"}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def _request(self, method, params):
                self.assert_request(method, params)
                response = responses.pop(0)
                events.append(f"rate_probe_{response['rateLimits']['primary']['usedPercent']}")
                return response

            @staticmethod
            def assert_request(method, params):
                if (method, params) != ("account/rateLimits/read", {}):
                    raise AssertionError("unexpected app-server request")

        async def async_sleep(seconds):
            events.append(f"sleep_{int(seconds)}")

        class ReachedSemanticRunner(RuntimeError):
            pass

        async def full_arm_runner(*_args, **_kwargs):
            events.append("semantic_runner")
            raise ReachedSemanticRunner

        class FakeConnection:
            def close(self):
                events.append("connection_closed")

        recovery = self._recovery(
            client_factory=FakeManagedClient,
            full_arm_runner=full_arm_runner,
            async_sleep=async_sleep,
            rate_limit_recheck_seconds=30,
        )
        source_sha = self._configure_clean_batch8_fixture(recovery)

        async def instruction_probe(*, model, cwd):
            self.assertEqual(model, "gpt-5.6-sol")
            self.assertEqual(cwd.resolve(), self.repo.resolve())
            events.append("instruction_probe")
            return source_sha, 1

        with patch(
            "research_factory.app_server_interrupted_arm_recovery._read_only_connection",
            return_value=FakeConnection(),
        ), patch(
            "research_factory.unattended_app_server_eval._probe_instruction_sources_async",
            new=instruction_probe,
        ):
            with self.assertRaises(ReachedSemanticRunner):
                asyncio.run(recovery._run_clean_batch8_arms())

        self.assertEqual(
            events,
            [
                "rate_probe_91",
                "sleep_30",
                "rate_probe_0",
                "instruction_probe",
                "semantic_runner",
                "connection_closed",
            ],
        )
        self.assertEqual(responses, [])
        self.assertTrue(recovery.rate_limit_clearance_path.is_file())

    def test_async_batch8_stop_prevents_capacity_probe_and_semantic_runner(self) -> None:
        calls = {"clients": 0, "runners": 0}

        class NeverClient:
            def __init__(self):
                calls["clients"] += 1

        async def full_arm_runner(*_args, **_kwargs):
            calls["runners"] += 1

        class FakeConnection:
            def close(self):
                return None

        recovery = self._recovery(
            client_factory=NeverClient,
            full_arm_runner=full_arm_runner,
        )
        self._configure_clean_batch8_fixture(recovery)
        recovery.stop_path.write_text("stop\n", encoding="utf-8")

        with patch(
            "research_factory.app_server_interrupted_arm_recovery._read_only_connection",
            return_value=FakeConnection(),
        ):
            with self.assertRaises(RecoveryStopped):
                asyncio.run(recovery._run_clean_batch8_arms())

        self.assertEqual(calls, {"clients": 0, "runners": 0})

    def test_async_batch8_auth_drift_fails_before_request_or_semantic_runner(self) -> None:
        calls = {"requests": 0, "runners": 0}

        class ApiKeyClient:
            account_summary = {"type": "apiKey", "plan_type": None}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def _request(self, _method, _params):
                calls["requests"] += 1
                return {}

        async def full_arm_runner(*_args, **_kwargs):
            calls["runners"] += 1

        class FakeConnection:
            def close(self):
                return None

        recovery = self._recovery(
            client_factory=ApiKeyClient,
            full_arm_runner=full_arm_runner,
        )
        self._configure_clean_batch8_fixture(recovery)

        with patch(
            "research_factory.app_server_interrupted_arm_recovery._read_only_connection",
            return_value=FakeConnection(),
        ):
            with self.assertRaisesRegex(
                RecoveryError, "live no-turn app-server rate-limit probe failed"
            ):
                asyncio.run(recovery._run_clean_batch8_arms())

        self.assertEqual(calls, {"requests": 0, "runners": 0})

    def test_recovered_matrix_contains_five_clean_arms_and_nonselectable_itt_slot(self) -> None:
        recovery = self._recovery()
        recovery.recovery_root.mkdir(parents=True)
        recovery.consolidated_path.write_text(
            json.dumps({"schema_version": "pif_app_server_interrupted_arm_consolidated_v1"})
            + "\n",
            encoding="utf-8",
        )
        recovery.itt_provenance_path.write_text(
            json.dumps(
                {
                    "schema_version": "pif_app_server_arm_intent_to_treat_interruption_v1"
                }
            )
            + "\n",
            encoding="utf-8",
        )
        for batch_size, modes in ((3, ("new_thread", "same_thread")), (5, ("new_thread",)), (8, ("new_thread", "same_thread"))):
            for mode in modes:
                path = self.matrix / f"batch-{batch_size}" / mode / "report.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(
                    json.dumps(
                        {
                            "schema_version": "pif_app_server_core_arm_v3",
                            "batch_size_ceiling": batch_size,
                            "thread_mode": mode,
                            "accounting_complete": True,
                            "usage_status": "complete",
                            "usage_unknown_attempts": 0,
                            "validated_segments": 32,
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )

        matrix = recovery._build_recovered_matrix(
            {"schema_version": "pif_app_server_interrupted_arm_consolidated_v1"}
        )

        self.assertEqual(matrix["clean_arm_count"], 5)
        self.assertTrue(matrix["five_clean_arm_selection_eligible"])
        self.assertTrue(matrix["selection_eligible"])
        self.assertFalse(matrix["interrupted_arm"]["selection_eligible"])
        self.assertFalse(matrix["clean_six_arm_matrix_achieved"])


if __name__ == "__main__":
    unittest.main()
