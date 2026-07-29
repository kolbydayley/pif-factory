from __future__ import annotations

import hashlib
import json
import shlex
import signal
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from research_factory.app_server_evaluation import (
    APP_SERVER_CORE_ARM_VERSION,
    APP_SERVER_EPISODE_BATCH_SCHEMA_VERSION,
)
from research_factory.codex_app_server import (
    APP_SERVER_CLIENT_VERSION,
    PINNED_CODEX_CLI_VERSION,
    PROTOCOL_SCHEMA_SHA256,
)
from research_factory.unattended_app_server_eval import (
    RUN_SPEC_VERSION,
    Arm,
    LockUnavailable,
    ProcessRecord,
    SupervisorError,
    SupervisorLock,
    SupervisorStopped,
    UnattendedAppServerMatrixSupervisor,
    _HANDOFF_GUARD,
    _canonical_json_sha256,
)


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class FakeProcess:
    def __init__(self, *, pid: int = 4242, return_code: int = 0):
        self.pid = pid
        self.return_code = return_code

    def wait(self, timeout: float) -> int:
        return self.return_code


class UnattendedSupervisorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.repo = Path(self.tempdir.name).resolve()
        self.work_root = self.repo / "work" / "app-server-development-v2"
        self.matrix_root = self.work_root / "matrix-v1"
        self.work_root.mkdir(parents=True)
        self.instruction_path = self.repo / "AGENTS.md"
        self.instruction_path.write_text("Synthetic frozen instructions.\n", encoding="utf-8")

        parent = self._artifact("work/app-server-development-v2/run-spec-v1.json", b"{}\n")
        manifest = self._artifact("work/app-server-development-v2/manifest.json", b"{}\n")
        reference = self._artifact(
            "work/app-server-development-v2/shared-reference-seed-v1.json", b"{}\n"
        )
        noise = self._artifact(
            "work/app-server-development-v2/reference-noise-v1.json", b"{}\n"
        )
        guideline = self._artifact(
            "research_factory/prompt_guidelines/windowed_event_core_v1.json", b"{}\n"
        )
        self.instruction_set_sha = _canonical_json_sha256([str(self.instruction_path)])
        self.spec = {
            "schema_version": RUN_SPEC_VERSION,
            "parent_run_spec": parent,
            "manifest": {
                **manifest,
                "segment_count": 32,
                "episode_count": 4,
                "source_count": 4,
            },
            "shared_reference_seed": reference,
            "reference_noise": noise,
            "frozen_artifacts": {"candidate_guideline": guideline},
            "candidate": {
                "model": "gpt-5.6-sol",
                "reasoning_effort": "low",
                "batch_sizes": [3, 5, 8],
                "thread_modes": ["new_thread", "same_thread"],
                "concurrency": 1,
                "retry_count": 0,
                "fallback_policy": "none",
                "timeout_seconds": 600,
                "window_count": 4,
                "context_chars": 900,
                "event_cap": 32,
                "core_instructions_sha256": "core-sha",
                "episode_base_instructions_set_sha256": "episode-base-sha",
                "output_schema_version": APP_SERVER_EPISODE_BATCH_SCHEMA_VERSION,
                "core_arm_version": APP_SERVER_CORE_ARM_VERSION,
            },
            "transport": {
                "interface": "official_codex_app_server_stdio_json_rpc",
                "managed_chatgpt_auth_only": True,
                "public_openai_api_allowed": False,
                "api_key_billing_allowed": False,
                "raw_session_token_access_allowed": False,
                "codex_exec_semantic_calls_allowed": False,
                "cli_version": PINNED_CODEX_CLI_VERSION,
                "client_version": APP_SERVER_CLIENT_VERSION,
                "protocol_schema_sha256": PROTOCOL_SCHEMA_SHA256,
                "max_message_bytes": 32 * 1024 * 1024,
                "approval_policy": "never",
                "sandbox": "read_only",
                "dynamic_tools": False,
                "silent_retry": False,
            },
            "instruction_contract": {
                "expected_path_set_sha256": self.instruction_set_sha,
                "sources": [
                    {
                        "path": str(self.instruction_path),
                        "content_sha256": file_sha256(self.instruction_path),
                        "size_bytes": self.instruction_path.stat().st_size,
                    }
                ],
            },
            "supervision": {
                "lock_scope": "full_process_lifetime",
                "adopt_exact_live_process": True,
                "rerun_partial_or_interrupted_arm": False,
                "stop_on_unknown_usage": True,
                "stop_on_incomplete_accounting": True,
                "continue_on_audited_metric_grounding_diagnostics": True,
                "production_changes_allowed": False,
            },
            "retrospective_adoptions": [
                "batch_3_new_thread",
                "batch_3_same_thread",
                "batch_5_new_thread",
            ],
            "effective_from_arm": "batch_5_same_thread",
            "execution_order": [
                "batch_3_new_thread",
                "batch_3_same_thread",
                "batch_5_new_thread",
                "batch_5_same_thread",
                "batch_8_new_thread",
                "batch_8_same_thread",
            ],
            "selection_eligible": False,
        }
        self.run_spec_path = self.work_root / "run-spec-v2.json"
        self.run_spec_path.write_text(
            json.dumps(self.spec, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _artifact(self, relative_path: str, content: bytes) -> dict[str, str]:
        path = self.repo / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return {"artifact_path": relative_path, "artifact_sha256": file_sha256(path)}

    def _arms(self) -> list[Arm]:
        return [
            Arm(f"batch_{size}_{mode}", size, mode)
            for size in (3, 5, 8)
            for mode in ("new_thread", "same_thread")
        ]

    def _arm_dir(self, arm: Arm) -> Path:
        return self.matrix_root / f"batch-{arm.batch_size}" / arm.thread_mode

    def _report(self, arm: Arm) -> dict:
        return {
            "schema_version": APP_SERVER_CORE_ARM_VERSION,
            "evaluation_role": "retrospective_development_not_acceptance",
            "manifest_sha256": self.spec["manifest"]["artifact_sha256"],
            "core_instructions_sha256": "core-sha",
            "episode_base_instructions_set_sha256": "episode-base-sha",
            "guideline_artifact_sha256": self.spec["frozen_artifacts"][
                "candidate_guideline"
            ]["artifact_sha256"],
            "output_schema_version": APP_SERVER_EPISODE_BATCH_SCHEMA_VERSION,
            "transport_client_version": APP_SERVER_CLIENT_VERSION,
            "model": "gpt-5.6-sol",
            "reasoning_effort": "low",
            "batch_size_ceiling": arm.batch_size,
            "thread_mode": arm.thread_mode,
            "concurrency": 1,
            "retry_count": 0,
            "window_count": 4,
            "context_chars": 900,
            "max_events_per_segment": 32,
            "requested_segments": 32,
            "requested_calls": 1,
            "attempted_calls": 1,
            "terminal_sidecars": 1,
            "transport_completed_calls": 1,
            "validator_clean_calls": 1,
            "failed_or_not_started_calls": 0,
            "usage_status": "complete",
            "usage": {
                "input_tokens": 100,
                "cached_input_tokens": 0,
                "output_tokens": 20,
                "reasoning_output_tokens": 5,
                "total_tokens": 120,
            },
            "usage_measured_attempts": 1,
            "usage_unknown_attempts": 0,
            "accounting_complete": True,
            "instruction_source_sets": [
                {
                    "instruction_sources_sha256": self.instruction_set_sha,
                    "instruction_sources_count": 1,
                    "thread_count": 1,
                }
            ],
            "semantic_quality_status": (
                "pending_calibrated_llm_support_and_alignment_adjudication"
            ),
            "selection_eligible": False,
        }

    def _write_report(self, arm: Arm) -> Path:
        path = self._arm_dir(arm) / "report.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self._report(arm), ensure_ascii=True, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        return path

    def _supervisor(self, **overrides) -> UnattendedAppServerMatrixSupervisor:
        values = {
            "repo_root": self.repo,
            "run_spec_path": self.run_spec_path,
            "matrix_root": self.matrix_root,
            "process_reader": lambda: [],
            "popen_factory": Mock(side_effect=AssertionError("unexpected model process")),
            "codex_version_reader": lambda: PINNED_CODEX_CLI_VERSION,
            "instruction_probe": lambda **_kwargs: (self.instruction_set_sha, 1),
            "poll_seconds": 0.01,
            "heartbeat_seconds": 0.01,
        }
        values.update(overrides)
        return UnattendedAppServerMatrixSupervisor(**values)

    def test_adopts_six_complete_reports_and_aggregates_without_launch(self) -> None:
        for arm in self._arms():
            self._write_report(arm)
        popen = Mock(side_effect=AssertionError("completed reports must not relaunch"))
        supervisor = self._supervisor(popen_factory=popen)

        matrix = supervisor.run()

        self.assertEqual(matrix["arm_count"], 6)
        self.assertFalse(matrix["selection_eligible"])
        self.assertFalse(popen.called)
        state = json.loads(supervisor.state_path.read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "completed")
        self.assertTrue(
            all(item["status"] == "completed" for item in state["arms"].values())
        )

    def test_guard_is_removed_only_by_supervisor_before_single_launch(self) -> None:
        arms = self._arms()
        missing = arms[3]
        for arm in arms:
            if arm != missing:
                self._write_report(arm)
        guard_path = self._arm_dir(missing) / "private-mapping.json"
        guard_path.parent.mkdir(parents=True, exist_ok=True)
        guard_path.write_text(
            json.dumps(_HANDOFF_GUARD, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        commands = []

        def launch(command, **_kwargs):
            commands.append(command)
            self._write_report(missing)
            return FakeProcess()

        matrix = self._supervisor(popen_factory=launch).run()

        self.assertEqual(matrix["arm_count"], 6)
        self.assertEqual(len(commands), 1)
        self.assertIn("--thread-mode", commands[0])
        self.assertIn("same_thread", commands[0])
        self.assertFalse(guard_path.exists())

    def test_nonzero_child_is_never_retried_on_supervisor_restart(self) -> None:
        for arm in self._arms()[:3]:
            self._write_report(arm)
        missing = self._arms()[3]
        guard_path = self._arm_dir(missing) / "private-mapping.json"
        guard_path.parent.mkdir(parents=True, exist_ok=True)
        guard_path.write_text(
            json.dumps(_HANDOFF_GUARD, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        launches = []

        def fail_launch(command, **_kwargs):
            launches.append(command)
            return FakeProcess(return_code=9)

        first = self._supervisor(popen_factory=fail_launch)
        with self.assertRaisesRegex(SupervisorError, "exited nonzero"):
            first.run()
        first_state = json.loads(first.state_path.read_text(encoding="utf-8"))
        self.assertEqual(first_state["arms"][missing.name]["status"], "failed")
        self.assertTrue(
            first_state["arms"][missing.name]["automatic_retry_prohibited"]
        )

        second = self._supervisor(popen_factory=fail_launch)
        with self.assertRaisesRegex(SupervisorError, "automatic rerun is prohibited"):
            second.run()
        self.assertEqual(len(launches), 1)

    def test_orphaned_running_state_requires_explicit_recovery(self) -> None:
        supervisor = self._supervisor()
        supervisor._load_or_initialize_state()
        arm = self._arms()[3]
        supervisor.state["arms"][arm.name] = {"status": "running", "pid": 99999}
        supervisor.state_path.write_text(
            json.dumps(supervisor.state, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        for prior in self._arms()[:3]:
            self._write_report(prior)

        restarted = self._supervisor()
        with self.assertRaisesRegex(SupervisorError, "automatic rerun is prohibited"):
            restarted.run()

    def test_unknown_partial_output_blocks_launch(self) -> None:
        for arm in self._arms()[:3]:
            self._write_report(arm)
        arm = self._arms()[3]
        partial = self._arm_dir(arm) / "unexpected.json"
        partial.parent.mkdir(parents=True, exist_ok=True)
        partial.write_text("{}\n", encoding="utf-8")
        popen = Mock(side_effect=AssertionError("partial output must block launch"))

        with self.assertRaisesRegex(SupervisorError, "partial or orphaned arm output"):
            self._supervisor(popen_factory=popen).run()
        self.assertFalse(popen.called)

    def test_stop_sentinel_prevents_any_arm_launch(self) -> None:
        supervisor = self._supervisor()
        supervisor.stop_path.parent.mkdir(parents=True, exist_ok=True)
        supervisor.stop_path.write_text("stop\n", encoding="utf-8")

        with self.assertRaises(SupervisorStopped):
            supervisor.run()
        self.assertEqual(supervisor.state["status"], "stopped")

    def test_artifact_drift_fails_before_launch(self) -> None:
        (self.repo / self.spec["manifest"]["artifact_path"]).write_text(
            '{"drift":true}\n', encoding="utf-8"
        )
        popen = Mock(side_effect=AssertionError("drift must block launch"))

        with self.assertRaisesRegex(SupervisorError, "artifact drift"):
            self._supervisor(popen_factory=popen).run()
        self.assertFalse(popen.called)

    def test_multiple_exact_live_processes_fail_closed(self) -> None:
        template = self._supervisor()
        arm = self._arms()[0]
        command = " ".join(shlex.quote(item) for item in template._command(arm))
        processes = [
            ProcessRecord(pid=100, ppid=1, command=command),
            ProcessRecord(pid=101, ppid=1, command=command),
        ]
        supervisor = self._supervisor(process_reader=lambda: processes)

        with self.assertRaisesRegex(SupervisorError, "multiple app-server core-arm"):
            supervisor._assert_single_expected_process(arm)

    def test_owned_interrupt_is_forwarded_exactly_once(self) -> None:
        supervisor = self._supervisor()
        supervisor._active_pid = 5151
        supervisor._active_owned = True
        supervisor._record = Mock()

        with patch("research_factory.unattended_app_server_eval.os.killpg") as killpg:
            supervisor._forward_interrupt(signal.SIGINT, "test")
            supervisor._forward_interrupt(signal.SIGINT, "test-again")

        killpg.assert_called_once_with(5151, signal.SIGINT)
        self.assertEqual(supervisor._record.call_count, 1)

    def test_lifetime_lock_rejects_second_supervisor(self) -> None:
        lock_path = self.matrix_root / "supervisor" / "supervisor.lock"
        with SupervisorLock(lock_path):
            with self.assertRaises(LockUnavailable):
                with SupervisorLock(lock_path):
                    self.fail("second lock unexpectedly acquired")


if __name__ == "__main__":
    unittest.main()
