from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory.pipeline_watchdog import (
    _archive_project_probe,
    _matching_owned_receipt,
    default_state,
    launch_resume,
    load_watchdog_state,
    recovery_message,
    recycle_control_owner,
    run_cycle,
    save_watchdog_state,
    update_progress_state,
)


def observation(*, active: bool, complete: bool = False, raw_open: bool | None = None) -> dict:
    return {
        "phase": "complete" if complete else "evaluation_watch",
        "thread_id": "thread-1",
        "thread": {
            "found": True,
            "turn_in_progress": active,
            "session_turn_in_progress": active if raw_open is None else raw_open,
            "recent_sha256": "session-a",
            "latest_task_completed_at": None,
            "latest_task_started_at": None,
            "control_process": {
                "available": True,
                "alive": active,
                "pids": [10] if active else [],
                "runtime_verified_pids": [10] if active else [],
            },
        },
        "goal": {"available": True, "found": True, "status": "blocked"},
        "milestones": {"available": True, "snapshot_sha256": "milestone-a", "newest_path": "v25/terminal.json"},
        "queue_sha256": None,
        "workflow_complete": complete,
        "session_name": "pif-evaluation-thread-1",
        "control_launch": {
            "launch_id": "launch-1",
            "thread_id": "thread-1",
            "session_name": "pif-evaluation-thread-1",
            "exit_receipt_path": None,
        },
        "prior_exit_receipt": {
            "schema_version": "pif_control_exit_v1",
            "launch_id": "launch-1",
            "status": "exited",
            "stage": "codex_exited",
            "started_at": "2026-07-13T19:00:00-04:00",
            "exit_code": 0,
            "finished_at": "2026-07-13T20:00:00-04:00",
            "runner_pid": 9,
            "codex_pid": 10,
            "process_group_id": 10,
        },
    }


class PipelineWatchdogTests(unittest.TestCase):
    def test_active_turn_is_observed_without_interference(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state_path = Path(temp) / "state.json"
            with (
                patch("research_factory.pipeline_watchdog.observe", return_value=observation(active=True)),
                patch("research_factory.pipeline_watchdog.append_event"),
                patch("research_factory.pipeline_watchdog.launch_resume") as launch,
            ):
                result = run_cycle(watchdog_state_path=state_path)
            self.assertEqual(result["status"], "active_progress")
            launch.assert_not_called()
            self.assertEqual(load_watchdog_state(state_path)["consecutive_inactive_checks"], 0)

    def test_two_inactive_checks_launch_one_resume(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state_path = Path(temp) / "state.json"
            inactive = observation(active=False)
            with (
                patch("research_factory.pipeline_watchdog.observe", return_value=inactive),
                patch("research_factory.pipeline_watchdog.append_event"),
                patch("research_factory.pipeline_watchdog.tmux_session_alive", return_value=False),
                patch("research_factory.pipeline_watchdog.launch_resume", return_value={"ok": True, "exit_code": 0, "receipt": {}}) as launch,
            ):
                first = run_cycle(watchdog_state_path=state_path)
                second = run_cycle(watchdog_state_path=state_path)
            self.assertEqual(first["status"], "inactive_debounce")
            self.assertEqual(second["status"], "resume_launched")
            self.assertEqual(launch.call_count, 1)
            self.assertEqual(load_watchdog_state(state_path)["launch_count"], 1)

    def test_recent_raw_open_turn_never_launches_duplicate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state_path = Path(temp) / "state.json"
            raw_open = observation(active=False, raw_open=True)
            with (
                patch("research_factory.pipeline_watchdog.observe", return_value=raw_open),
                patch("research_factory.pipeline_watchdog.append_event"),
                patch("research_factory.pipeline_watchdog.launch_resume") as launch,
            ):
                result = run_cycle(watchdog_state_path=state_path, debounce_checks=1)
            self.assertEqual(result["status"], "raw_open_wait")
            launch.assert_not_called()

    def test_unavailable_process_liveness_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state_path = Path(temp) / "state.json"
            unknown = observation(active=False)
            unknown["thread"]["control_process"] = {"available": False, "alive": None}
            with (
                patch("research_factory.pipeline_watchdog.observe", return_value=unknown),
                patch("research_factory.pipeline_watchdog.append_event"),
                patch("research_factory.pipeline_watchdog.launch_resume") as launch,
            ):
                result = run_cycle(watchdog_state_path=state_path, debounce_checks=1)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "liveness_unavailable")
            launch.assert_not_called()

    def test_proven_stale_orphan_reaches_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state_path = Path(temp) / "state.json"
            orphan = observation(active=False, raw_open=True)
            orphan["thread"]["orphaned_open_turn"] = True
            orphan["thread"]["turn_in_progress"] = False
            with (
                patch("research_factory.pipeline_watchdog.observe", return_value=orphan),
                patch("research_factory.pipeline_watchdog.append_event"),
                patch("research_factory.pipeline_watchdog.tmux_session_alive", return_value=False),
                patch(
                    "research_factory.pipeline_watchdog.launch_resume",
                    return_value={"ok": True, "exit_code": 0, "receipt": {}},
                ) as launch,
            ):
                result = run_cycle(watchdog_state_path=state_path, debounce_checks=1)
            self.assertEqual(result["status"], "resume_launched")
            launch.assert_called_once()

    def test_live_stall_recycles_exact_owned_process_after_frozen_progress_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state_path = Path(temp) / "state.json"
            state = default_state()
            state["last_session_sha256"] = "session-a"
            state["last_milestone_sha256"] = "milestone-a"
            state["last_progress_at"] = (dt.datetime.now().astimezone() - dt.timedelta(hours=2)).isoformat()
            save_watchdog_state(state, state_path)
            with (
                patch("research_factory.pipeline_watchdog.observe", return_value=observation(active=True)),
                patch("research_factory.pipeline_watchdog.append_event"),
                patch(
                    "research_factory.pipeline_watchdog.recycle_control_owner",
                    return_value={"ok": True, "method": "receipt_owned_process_group"},
                ) as recycle,
            ):
                result = run_cycle(watchdog_state_path=state_path, live_stall_seconds=3600)
            self.assertEqual(result["status"], "recycled_live_stall")
            recycle.assert_called_once()
            self.assertEqual(recycle.call_args.kwargs["reason"], "live_stall")

    def test_previous_turn_completion_cannot_recycle_new_preturn_launch(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state_path = Path(temp) / "state.json"
            state = default_state()
            state["last_session_sha256"] = "session-a"
            state["last_milestone_sha256"] = "milestone-a"
            state["last_progress_at"] = dt.datetime.now().astimezone().isoformat()
            save_watchdog_state(state, state_path)
            current = observation(active=True)
            current["prior_exit_receipt"].update(
                {
                    "status": "running",
                    "stage": "codex_dispatched",
                    "started_at": dt.datetime.now().astimezone().isoformat(),
                    "finished_at": None,
                }
            )
            current["thread"]["latest_task_completed_at"] = (
                dt.datetime.now().astimezone() - dt.timedelta(hours=1)
            ).isoformat()
            with (
                patch("research_factory.pipeline_watchdog.observe", return_value=current),
                patch("research_factory.pipeline_watchdog.append_event"),
                patch("research_factory.pipeline_watchdog.recycle_control_owner") as recycle,
            ):
                result = run_cycle(
                    watchdog_state_path=state_path,
                    post_completion_hang_seconds=1,
                    startup_stall_seconds=3600,
                    live_stall_seconds=3600,
                )
            self.assertEqual(result["status"], "active_wait")
            recycle.assert_not_called()

    def test_missing_tmux_recycles_exact_receipt_owned_process_group(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state_root = Path(temp) / "pif-pipeline-babysitter"
            receipt_dir = state_root / "control-receipts"
            receipt_dir.mkdir(parents=True)
            receipt_path = receipt_dir / "launch.json"
            receipt = {
                "schema_version": "pif_control_exit_v1",
                "launch_id": "launch-1",
                "status": "running",
                "stage": "codex_dispatched",
                "started_at": "2026-07-14T06:00:00Z",
                "runner_pid": 409,
                "codex_pid": 410,
                "process_group_id": 410,
            }
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            current = observation(active=True)
            current["control_launch"]["exit_receipt_path"] = str(receipt_path)
            current["prior_exit_receipt"] = dict(receipt)
            current["thread"]["control_process"] = {
                "available": True,
                "alive": True,
                "pids": [410, 411],
                "runtime_verified_pids": [410, 411],
            }
            with (
                patch("research_factory.pipeline_watchdog.STATE_ROOT", state_root),
                patch("research_factory.pipeline_watchdog.os.getpgid", return_value=410),
                patch("research_factory.pipeline_watchdog.os.getpgrp", return_value=999),
                patch("research_factory.pipeline_watchdog._process_start_signature", return_value="identity"),
                patch("research_factory.pipeline_watchdog._process_group_alive", side_effect=[False, False, False]),
                patch("research_factory.pipeline_watchdog.os.killpg") as killpg,
                patch("research_factory.pipeline_watchdog.kill_tmux_session", return_value=False),
            ):
                result = recycle_control_owner(current, reason="startup_stall")
            self.assertTrue(result["ok"])
            self.assertEqual(result["method"], "receipt_owned_process_group")
            killpg.assert_called_once_with(410, 15)
            terminal = json.loads(receipt_path.read_text(encoding="utf-8"))
            self.assertEqual(terminal["status"], "exited")
            self.assertEqual(terminal["stage"], "watchdog_recycled")

    def test_foreign_or_pid_reused_process_is_never_signalled(self) -> None:
        current = observation(active=True)
        current["prior_exit_receipt"].update(
            {
                "status": "running",
                "stage": "codex_dispatched",
                "launch_id": "different-launch",
            }
        )
        with patch("research_factory.pipeline_watchdog.os.killpg") as killpg:
            ownership = _matching_owned_receipt(current)
        self.assertFalse(ownership["ok"])
        self.assertEqual(ownership["error_class"], "recycle_owner_unverified")
        killpg.assert_not_called()

    def test_complete_phase_disables_watchdog(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state_path = Path(temp) / "state.json"
            with (
                patch("research_factory.pipeline_watchdog.observe", return_value=observation(active=False, complete=True)),
                patch("research_factory.pipeline_watchdog.append_event"),
                patch("research_factory.pipeline_watchdog.disable_self", return_value={"ok": True, "exit_code": 0}) as disable,
            ):
                result = run_cycle(watchdog_state_path=state_path)
            self.assertEqual(result["status"], "complete_disabled")
            disable.assert_called_once()

    def test_recovery_message_preserves_transport_and_immutability_rules(self) -> None:
        text = recovery_message(observation(active=False), "recovery-1")
        self.assertIn("managed ChatGPT auth", text)
        self.assertIn("Never use API-key billing", text)
        self.assertIn("new immutable version", text)
        self.assertIn("Do not advance untouched holdout early", text)
        self.assertIn("checkpoint, not a terminal outcome", text)
        self.assertIn("historical blocked value is not permission to stop", text)
        self.assertNotIn("blocker has a concrete next experiment prepared", text)

    def test_launch_timeout_is_a_structured_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            message = Path(temp) / "message.md"
            message.write_text("continue\n", encoding="utf-8")
            with patch(
                "research_factory.pipeline_watchdog._run_bounded_command",
                return_value={
                    "exit_code": 124,
                    "stdout": "",
                    "stderr": "",
                    "timed_out": True,
                    "error_class": "process_timeout",
                },
            ):
                result = launch_resume(observation(active=False), message)
        self.assertFalse(result["ok"])
        self.assertEqual(result["exit_code"], 124)
        self.assertEqual(result["error_class"], "launch_resume_timeout")

    def test_watchdog_launch_explicitly_requests_proven_orphan_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            message = Path(temp) / "message.md"
            message.write_text("continue\n", encoding="utf-8")
            with patch(
                "research_factory.pipeline_watchdog._run_bounded_command",
                return_value={
                    "exit_code": 0,
                    "stdout": '{"status":"launched"}',
                    "stderr": "",
                    "timed_out": False,
                    "error_class": None,
                },
            ) as run:
                result = launch_resume(observation(active=False), message)
        self.assertTrue(result["ok"])
        self.assertIn("--recover-orphaned-open-turn", run.call_args.args[0])

    def test_packaged_launch_never_imports_babysitter_from_project_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            archive = Path(temp) / "runtime.pyz"
            archive.write_bytes(b"zipapp")
            message = Path(temp) / "message.md"
            message.write_text("continue\n", encoding="utf-8")
            completed = {
                "exit_code": 0,
                "stdout": '{"status":"launched"}',
                "stderr": "",
                "timed_out": False,
                "error_class": None,
            }
            with (
                patch.dict(os.environ, {"PIF_WATCHDOG_ARCHIVE": str(archive)}),
                patch("research_factory.pipeline_watchdog._run_bounded_command", return_value=completed) as run,
            ):
                result = launch_resume(observation(active=False), message)
        self.assertTrue(result["ok"])
        command = run.call_args.args[0]
        self.assertEqual(command[:2], [str(archive), "babysitter"])
        self.assertNotIn(str(Path.cwd()), command[:2])

    def test_project_probe_timeout_is_sanitized_and_nonfatal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            archive = Path(temp) / "runtime.pyz"
            archive.write_bytes(b"zipapp")
            with (
                patch.dict(os.environ, {"PIF_WATCHDOG_ARCHIVE": str(archive)}),
                patch(
                    "research_factory.pipeline_watchdog._run_bounded_command",
                    return_value={
                        "exit_code": 124,
                        "stdout": "",
                        "stderr": "",
                        "timed_out": True,
                        "error_class": "process_timeout",
                    },
                ),
            ):
                result = _archive_project_probe("milestone-snapshot", Path(temp) / "project")
        self.assertEqual(
            result,
            {
                "available": False,
                "snapshot_sha256": None,
                "error_class": "milestone_snapshot_timeout",
            },
        )

    def test_project_probe_accepts_only_json_objects(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            archive = Path(temp) / "runtime.pyz"
            archive.write_bytes(b"zipapp")
            with (
                patch.dict(os.environ, {"PIF_WATCHDOG_ARCHIVE": str(archive)}),
                patch(
                    "research_factory.pipeline_watchdog._run_bounded_command",
                    return_value={
                        "exit_code": 0,
                        "stdout": json.dumps({"snapshot_sha256": "abc"}),
                        "stderr": "",
                        "timed_out": False,
                        "error_class": None,
                    },
                ),
            ):
                result = _archive_project_probe("queue-snapshot", Path(temp) / "factory.sqlite")
        self.assertEqual(result, {"snapshot_sha256": "abc"})

    def test_unavailable_probe_preserves_last_known_milestone(self) -> None:
        state = default_state()
        state["last_session_sha256"] = "session-a"
        state["last_milestone_sha256"] = "milestone-known"
        current = observation(active=True)
        current["milestones"] = {
            "available": False,
            "snapshot_sha256": None,
            "error_class": "milestone_snapshot_timeout",
        }

        changed = update_progress_state(state, current, dt.datetime.now().astimezone())

        self.assertFalse(changed)
        self.assertEqual(state["last_milestone_sha256"], "milestone-known")


if __name__ == "__main__":
    unittest.main()
