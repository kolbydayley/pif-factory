from __future__ import annotations

import argparse
import contextlib
import json
import hashlib
import io
import os
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory.pipeline_babysitter import (
    CONTROL_WORKING_DIRECTORY,
    DEFAULT_DB_PATH,
    DEFAULT_EVALUATION_ROOT,
    OPEN_TURN_ACTIVITY_GRACE_SECONDS,
    _detached_new_runner_text,
    _detached_runner_text,
    _pending_control_observation,
    _reconcile_pending_control_launch,
    _safe_orphan_override,
    _thread_started_from_events,
    codex_runtime_observation,
    completion_audit,
    control_process_observation,
    cmd_accept_evaluation,
    cmd_launch_resume,
    cmd_launch_extraction,
    default_state,
    evaluation_milestone_snapshot,
    goal_observation,
    load_state,
    managed_chatgpt_auth_observation,
    phase_progress_observation,
    record_steering,
    save_state,
    session_observation,
    verify_evaluation_receipt,
)


class PipelineBabysitterTests(unittest.TestCase):
    def test_scheduler_facing_defaults_are_absolute(self) -> None:
        self.assertTrue(DEFAULT_DB_PATH.is_absolute())
        self.assertTrue(DEFAULT_EVALUATION_ROOT.is_absolute())
        self.assertEqual(DEFAULT_DB_PATH.name, "factory.sqlite")

    def test_control_process_observation_ignores_shell_commands_that_only_mention_resume(self) -> None:
        output = """\
 101 /bin/zsh /bin/zsh -c ps | rg 'codex exec resume thread-1'
 102 /Users/test/.local/bin/codex codex exec resume thread-1 -
 103 /usr/bin/caffeinate /usr/bin/caffeinate -i /Users/test/.local/bin/codex exec resume thread-1 -
 104 /Users/test/.local/bin/codex codex exec resume another-thread -
"""
        completed = subprocess.CompletedProcess(args=[], returncode=0, stdout=output, stderr="")
        with patch("research_factory.pipeline_babysitter.subprocess.run", return_value=completed):
            observation = control_process_observation("thread-1")
        self.assertTrue(observation["alive"])
        self.assertEqual(observation["pids"], [102, 103])

    def test_control_process_observation_accepts_truncated_macos_comm(self) -> None:
        output = """\
 201 /Users/test/.l /Users/test/.local/bin/codex exec resume --json thread-1 -
 202 /usr/bin/caffei /usr/bin/caffeinate -i /Users/test/.local/bin/codex exec resume --json thread-1 -
 203 /bin/zsh /bin/zsh -c '/Users/test/.local/bin/codex exec resume thread-1 -'
"""
        completed = subprocess.CompletedProcess(args=[], returncode=0, stdout=output, stderr="")
        with patch("research_factory.pipeline_babysitter.subprocess.run", return_value=completed):
            observation = control_process_observation("thread-1")
        self.assertTrue(observation["alive"])
        self.assertEqual(observation["pids"], [201, 202])

    def test_control_process_observation_accepts_cd_before_resume(self) -> None:
        output = """\
 301 /Users/test/.l /Users/test/.local/bin/codex exec --cd /tmp/project --json resume thread-1 -
 302 /usr/bin/caffei /usr/bin/caffeinate -i /Users/test/.local/bin/codex exec --cd /tmp/project resume thread-1 -
"""
        completed = subprocess.CompletedProcess(args=[], returncode=0, stdout=output, stderr="")
        with patch("research_factory.pipeline_babysitter.subprocess.run", return_value=completed):
            observation = control_process_observation("thread-1")
        self.assertTrue(observation["alive"])
        self.assertEqual(observation["pids"], [301, 302])

    def test_evaluation_milestone_snapshot_ignores_heartbeat_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "state.json").write_text('{"heartbeat": 1}', encoding="utf-8")
            first = evaluation_milestone_snapshot(root)
            (root / "state.json").write_text('{"heartbeat": 2}', encoding="utf-8")
            second = evaluation_milestone_snapshot(root)
            self.assertEqual(first["snapshot_sha256"], second["snapshot_sha256"])
            (root / "phase").mkdir()
            (root / "phase" / "sidecar.json").write_text('{"usage_complete": true}', encoding="utf-8")
            third = evaluation_milestone_snapshot(root)
            self.assertNotEqual(second["snapshot_sha256"], third["snapshot_sha256"])
            self.assertEqual(third["artifact_count"], 1)
            time.sleep(0.01)
            os.utime(root / "phase" / "sidecar.json", None)
            touched = evaluation_milestone_snapshot(root)
            self.assertEqual(third["snapshot_sha256"], touched["snapshot_sha256"])
            (root / "phase" / "sidecar.json").write_text('{"usage_complete": false}', encoding="utf-8")
            changed = evaluation_milestone_snapshot(root)
            self.assertNotEqual(touched["snapshot_sha256"], changed["snapshot_sha256"])

    def test_session_observation_tracks_live_turn_without_interpreting_goal_text(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "rollout-thread-1.jsonl"
            events = [
                {
                    "timestamp": "2026-07-13T12:42:57Z",
                    "type": "response_item",
                    "payload": {
                        "type": "custom_tool_call",
                        "name": "exec",
                        "input": 'const r = await tools.update_goal({status:"blocked"});',
                    },
                },
                {
                    "timestamp": "2026-07-13T14:28:20Z",
                    "type": "event_msg",
                    "payload": {"type": "task_started", "turn_id": "turn-live"},
                },
            ]
            with path.open("w", encoding="utf-8") as handle:
                handle.write(json.dumps(events[0]) + "\n")
                # Push the goal transition well beyond the prior 512 KB tail.
                for index in range(5_000):
                    handle.write(json.dumps({"timestamp": f"filler-{index}", "payload": {"type": "token_count", "data": "x" * 120}}) + "\n")
                handle.write(json.dumps(events[1]) + "\n")
            with patch("research_factory.pipeline_babysitter.session_path", return_value=path):
                observation = session_observation("thread-1")
            self.assertNotIn("observed_goal_status", observation)
            self.assertTrue(observation["turn_in_progress"])
            self.assertEqual(observation["current_turn_id"], "turn-live")

    def test_goal_observation_reads_authoritative_sanitized_state_and_refreshes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "goals.sqlite"
            conn = sqlite3.connect(path)
            conn.execute(
                """
                CREATE TABLE thread_goals (
                    thread_id TEXT PRIMARY KEY NOT NULL, goal_id TEXT NOT NULL,
                    objective TEXT NOT NULL, status TEXT NOT NULL,
                    tokens_used INTEGER NOT NULL, time_used_seconds INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO thread_goals VALUES (?,?,?,?,?,?,?)",
                ("thread-1", "goal-1", "private objective", "blocked", 123, 45, 1_789_000_000_000),
            )
            conn.commit()
            first = goal_observation("thread-1", path)
            self.assertEqual(first["status"], "blocked")
            self.assertNotIn("objective", first)
            conn.execute("UPDATE thread_goals SET status='active', updated_at_ms=updated_at_ms+1 WHERE thread_id='thread-1'")
            conn.commit()
            conn.close()
            self.assertEqual(goal_observation("thread-1", path)["status"], "active")
            self.assertEqual(goal_observation("missing", path), {"available": True, "found": False})

    def test_session_observation_treats_aborted_turn_as_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "rollout-thread-1.jsonl"
            events = [
                {"timestamp": "1", "type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-aborted"}},
                {"timestamp": "2", "type": "event_msg", "payload": {"type": "turn_aborted", "turn_id": "turn-aborted"}},
            ]
            path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            with patch("research_factory.pipeline_babysitter.session_path", return_value=path):
                observation = session_observation("thread-1")
            self.assertFalse(observation["turn_in_progress"])
            self.assertIsNone(observation["current_turn_id"])

    def test_session_observation_marks_stale_unmatched_turn_as_orphaned(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "rollout-thread-1.jsonl"
            path.write_text(
                json.dumps({"timestamp": "1", "type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-orphan"}}) + "\n",
                encoding="utf-8",
            )
            stale = time.time() - OPEN_TURN_ACTIVITY_GRACE_SECONDS - 60
            os.utime(path, (stale, stale))
            with (
                patch("research_factory.pipeline_babysitter.session_path", return_value=path),
                patch(
                    "research_factory.pipeline_babysitter.control_process_observation",
                    return_value={"available": True, "alive": False, "process_count": 0, "pids": []},
                ),
            ):
                observation = session_observation("thread-1")
            self.assertTrue(observation["session_turn_in_progress"])
            self.assertFalse(observation["turn_in_progress"])
            self.assertTrue(observation["orphaned_open_turn"])

    def test_session_observation_fails_closed_when_process_liveness_is_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "rollout-thread-1.jsonl"
            path.write_text(
                json.dumps({"timestamp": "1", "type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-unknown"}}) + "\n",
                encoding="utf-8",
            )
            stale = time.time() - OPEN_TURN_ACTIVITY_GRACE_SECONDS - 60
            os.utime(path, (stale, stale))
            with (
                patch("research_factory.pipeline_babysitter.session_path", return_value=path),
                patch(
                    "research_factory.pipeline_babysitter.control_process_observation",
                    return_value={"available": False, "alive": None, "error_class": "process_observation_unavailable"},
                ),
            ):
                observation = session_observation("thread-1")
            self.assertTrue(observation["turn_in_progress"])
            self.assertTrue(observation["liveness_unknown"])
            self.assertFalse(observation["orphaned_open_turn"])

    def test_session_observation_keeps_old_turn_open_when_process_is_alive(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "rollout-thread-1.jsonl"
            path.write_text(
                json.dumps({"timestamp": "1", "type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-live"}}) + "\n",
                encoding="utf-8",
            )
            stale = time.time() - OPEN_TURN_ACTIVITY_GRACE_SECONDS - 60
            os.utime(path, (stale, stale))
            with (
                patch("research_factory.pipeline_babysitter.session_path", return_value=path),
                patch(
                    "research_factory.pipeline_babysitter.control_process_observation",
                    return_value={"available": True, "alive": True, "process_count": 1, "pids": [123]},
                ),
            ):
                observation = session_observation("thread-1")
            self.assertTrue(observation["turn_in_progress"])
            self.assertFalse(observation["orphaned_open_turn"])

    def test_session_observation_treats_pre_event_resume_process_as_live(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "rollout-thread-1.jsonl"
            path.write_text(
                json.dumps({"timestamp": "1", "type": "event_msg", "payload": {"type": "task_complete", "turn_id": "old"}}) + "\n",
                encoding="utf-8",
            )
            with (
                patch("research_factory.pipeline_babysitter.session_path", return_value=path),
                patch(
                    "research_factory.pipeline_babysitter.control_process_observation",
                    return_value={"available": True, "alive": True, "process_count": 1, "pids": [123]},
                ),
            ):
                observation = session_observation("thread-1")
            self.assertFalse(observation["session_turn_in_progress"])
            self.assertTrue(observation["turn_in_progress"])

    def test_phase_progress_ignores_queue_churn_during_evaluation(self) -> None:
        progress = {
            "goal_status_changed": False,
            "semantic_milestone_changed": False,
            "queue_state_changed": True,
        }
        observed, basis = phase_progress_observation("evaluation_watch", progress)
        self.assertFalse(observed)
        self.assertEqual(basis, ["goal_status_changed", "semantic_milestone_changed"])
        extraction_observed, extraction_basis = phase_progress_observation("extraction_watch", progress)
        self.assertTrue(extraction_observed)
        self.assertEqual(extraction_basis, ["goal_status_changed", "queue_state_changed"])

    def test_detached_runner_unsets_api_keys_and_writes_exit_receipt(self) -> None:
        root = Path("/tmp/pif-control-test")
        runner = _detached_runner_text(
            thread_id="thread-1",
            message_path=root / "message.md",
            events_path=root / "events.jsonl",
            errors_path=root / "stderr.log",
            exit_receipt_path=root / "exit.json",
            model="gpt-5.5",
            reasoning_effort="xhigh",
            launch_id="launch-test-1",
        )
        self.assertIn("-u OPENAI_API_KEY", runner)
        self.assertIn("codex exec --cd", runner)
        self.assertIn(str(CONTROL_WORKING_DIRECTORY), runner)
        self.assertIn("os.setsid()", runner)
        self.assertIn("process_group_id", runner)
        self.assertIn("terminate_codex_group", runner)
        self.assertLess(runner.index("--cd"), runner.index("resume"))
        self.assertIn("pif_control_exit_v1", runner)
        self.assertIn('"launch_id":"launch-test-1"', runner)
        self.assertIn("EXIT_CODE", runner)
        self.assertIn("codex-cron run pif-pipeline-watchdog", runner)
        self.assertIn("codex_runtime", runner)
        syntax = subprocess.run(["/bin/zsh", "-n"], input=runner, text=True, capture_output=True, check=False)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)

    def test_detached_new_runner_uses_managed_auth_preflight_and_valid_shell(self) -> None:
        root = Path("/tmp/pif-control-test")
        runner = _detached_new_runner_text(
            working_directory=Path("/tmp/repo"),
            message_path=root / "message.md",
            events_path=root / "events.jsonl",
            errors_path=root / "stderr.log",
            exit_receipt_path=root / "exit.json",
            model="gpt-5.5",
            reasoning_effort="xhigh",
        )
        self.assertIn("login status", runner)
        self.assertIn("Logged in using ChatGPT", runner)
        self.assertIn("--cd /tmp/repo", runner)
        syntax = subprocess.run(["/bin/zsh", "-n"], input=runner, text=True, capture_output=True, check=False)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)

    def test_thread_started_parser_accepts_only_uuid_events(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "events.jsonl"
            path.write_text(
                "\n".join(
                    [
                        "not-json",
                        json.dumps({"type": "thread.started", "thread_id": "not-a-uuid"}),
                        json.dumps({"type": "thread.started", "thread_id": "019f4cf1-c46e-7db3-acd2-bf03c4459a10"}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            self.assertEqual(_thread_started_from_events(path), "019f4cf1-c46e-7db3-acd2-bf03c4459a10")

    def test_detached_extraction_launch_registers_started_thread(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_path = root / "state.json"
            handoff_path = root / "handoff.md"
            handoff_text = "Process the extraction queue using the frozen app-server winner.\n"
            handoff_path.write_text(handoff_text, encoding="utf-8")
            state = default_state("evaluation-thread")
            state["phase"] = "extraction_handoff"
            state["evaluation_receipt"] = {"verified": True}
            state["handoff"] = {
                "path": str(handoff_path.resolve()),
                "sha256": hashlib.sha256(handoff_text.encode()).hexdigest(),
            }
            save_state(state, state_path)
            args = argparse.Namespace(
                state=str(state_path),
                message_file=str(handoff_path),
                session_name="pif-extraction-handoff",
                model="gpt-5.5",
                reasoning_effort="xhigh",
            )
            launched = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
            with (
                patch("research_factory.pipeline_babysitter.STATE_ROOT", root / "control"),
                patch(
                    "research_factory.pipeline_babysitter.managed_chatgpt_auth_observation",
                    return_value={"available": True, "authenticated": True, "mode": "managed_chatgpt"},
                ),
                patch(
                    "research_factory.pipeline_babysitter.codex_runtime_observation",
                    return_value={"available": True, "verified": True, "version": "test"},
                ),
                patch("research_factory.pipeline_babysitter._tmux_has_session", side_effect=[False, True]),
                patch("research_factory.pipeline_babysitter.subprocess.run", return_value=launched),
                patch(
                    "research_factory.pipeline_babysitter._thread_started_from_events",
                    return_value="019f4cf1-c46e-7db3-acd2-bf03c4459a10",
                ),
            ):
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(cmd_launch_extraction(args), 0)
            launched_state = load_state(state_path)
            self.assertEqual(launched_state["phase"], "extraction_watch")
            self.assertEqual(launched_state["extraction_thread_id"], "019f4cf1-c46e-7db3-acd2-bf03c4459a10")
            self.assertIsNone(launched_state["pending_extraction_launch"])

    def test_pinned_runtime_hash_and_signature_must_match(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = root / "codex"
            codesign = root / "codesign"
            binary.write_bytes(b"verified codex runtime")
            codesign.write_text("", encoding="utf-8")
            expected_sha = hashlib.sha256(binary.read_bytes()).hexdigest()
            version = subprocess.CompletedProcess([], 0, "codex-cli test\n", "")
            signature = subprocess.CompletedProcess(
                [],
                0,
                "",
                "Authority=Developer ID Application: OpenAI OpCo, LLC (2DC432GLL2)\nTeamIdentifier=2DC432GLL2\n",
            )
            with (
                patch("research_factory.pipeline_babysitter.CODEX_BIN", binary),
                patch("research_factory.pipeline_babysitter.CODESIGN_PATH", codesign),
                patch("research_factory.pipeline_babysitter.CODEX_RUNTIME_VERSION", "codex-cli test"),
                patch("research_factory.pipeline_babysitter.CODEX_RUNTIME_SHA256", expected_sha),
                patch("research_factory.pipeline_babysitter.subprocess.run", side_effect=[version, signature]),
            ):
                self.assertTrue(codex_runtime_observation()["verified"])
            with (
                patch("research_factory.pipeline_babysitter.CODEX_BIN", binary),
                patch("research_factory.pipeline_babysitter.CODESIGN_PATH", codesign),
                patch("research_factory.pipeline_babysitter.CODEX_RUNTIME_VERSION", "codex-cli test"),
                patch("research_factory.pipeline_babysitter.CODEX_RUNTIME_SHA256", "0" * 64),
                patch("research_factory.pipeline_babysitter.subprocess.run", side_effect=[version, signature]),
            ):
                result = codex_runtime_observation()
            self.assertFalse(result["verified"])
            self.assertEqual(result["error_class"], "pinned_runtime_drift")

    def test_orphan_override_requires_dead_owner_and_rejects_later_manual_turn(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            receipt = Path(temp) / "exit.json"
            receipt.write_text(
                json.dumps(
                    {
                        "status": "exited",
                        "runner_pid": 123,
                        "finished_at": "2026-07-14T04:10:00Z",
                    }
                ),
                encoding="utf-8",
            )
            state = {
                "last_control_launch": {
                    "thread_id": "thread-1",
                    "session_name": "pif-evaluation-thread",
                    "exit_receipt_path": str(receipt),
                }
            }
            process = {"available": True, "alive": False}
            with (
                patch("research_factory.pipeline_babysitter._tmux_has_session", return_value=False),
                patch("research_factory.pipeline_babysitter._pid_alive", return_value=False),
            ):
                self.assertTrue(
                    _safe_orphan_override(
                        state,
                        {"thread_id": "thread-1", "latest_task_started_at": "2026-07-14T04:09:00Z"},
                        process,
                        "pif-evaluation-thread",
                    )
                )
                self.assertFalse(
                    _safe_orphan_override(
                        state,
                        {"thread_id": "thread-1", "latest_task_started_at": "2026-07-14T04:11:00Z"},
                        process,
                        "pif-evaluation-thread",
                    )
                )

    def test_orphan_override_rejects_stale_running_or_wrong_owner(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            receipt = Path(temp) / "exit.json"
            receipt.write_text(json.dumps({"status": "running", "runner_pid": 123}), encoding="utf-8")
            process = {"available": True, "alive": False}
            matching = {
                "last_control_launch": {
                    "thread_id": "thread-1",
                    "session_name": "pif-evaluation-thread",
                    "exit_receipt_path": str(receipt),
                }
            }
            wrong_owner = {
                "last_control_launch": {
                    "thread_id": "other-thread",
                    "session_name": "pif-evaluation-thread",
                    "exit_receipt_path": str(receipt),
                }
            }
            observation = {
                "thread_id": "thread-1",
                "latest_task_started_at": "2026-07-14T04:09:00Z",
            }
            with patch("research_factory.pipeline_babysitter._tmux_has_session", return_value=False):
                self.assertFalse(_safe_orphan_override(matching, observation, process, "pif-evaluation-thread"))
                self.assertFalse(_safe_orphan_override(wrong_owner, observation, process, "pif-evaluation-thread"))

    def test_resume_reserves_before_tmux_and_commits_exact_process(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_path = root / "state.json"
            message_path = root / "recovery.md"
            message_path.write_text("continue the bounded experiment\n", encoding="utf-8")
            save_state(default_state("thread-1"), state_path)
            args = argparse.Namespace(
                state=str(state_path),
                message_file=str(message_path),
                session_name="pif-evaluation-thread-1",
                model="gpt-5.5",
                reasoning_effort="xhigh",
                recover_orphaned_open_turn=False,
                observed_milestone_sha256="a" * 64,
                observed_queue_sha256=None,
                goals_db=str(root / "goals.sqlite"),
            )
            launched = subprocess.CompletedProcess([], 0, "", "")
            observed_pending = []

            def launch_and_inspect(command, **_kwargs):
                reservation = load_state(state_path).get("pending_control_launch")
                observed_pending.append(reservation)
                Path(reservation["exit_receipt_path"]).write_text(
                    json.dumps(
                        {
                            "schema_version": "pif_control_exit_v1",
                            "launch_id": reservation["launch_id"],
                            "status": "running",
                            "stage": "codex_dispatched",
                            "codex_pid": 123,
                        }
                    ),
                    encoding="utf-8",
                )
                return launched

            with (
                patch("research_factory.pipeline_babysitter.STATE_ROOT", root / "control"),
                patch(
                    "research_factory.pipeline_babysitter.codex_runtime_observation",
                    return_value={"available": True, "verified": True, "version": "test"},
                ),
                patch(
                    "research_factory.pipeline_babysitter.session_observation",
                    return_value={"thread_id": "thread-1", "turn_in_progress": False, "recent_sha256": "session-new"},
                ),
                patch(
                    "research_factory.pipeline_babysitter.control_process_observation",
                    side_effect=[
                        {"available": True, "alive": False, "pids": []},
                        {"available": True, "alive": True, "pids": [123]},
                    ],
                ),
                patch("research_factory.pipeline_babysitter._tmux_has_session", side_effect=[False, True]),
                patch(
                    "research_factory.pipeline_babysitter.managed_chatgpt_auth_observation",
                    return_value={"available": True, "authenticated": True, "mode": "managed_chatgpt"},
                ),
                patch(
                    "research_factory.pipeline_babysitter.goal_observation",
                    return_value={"available": True, "found": True, "status": "blocked", "updated_at": "now"},
                ),
                patch("research_factory.pipeline_babysitter.subprocess.run", side_effect=launch_and_inspect) as run,
                patch("research_factory.pipeline_babysitter.time.sleep"),
            ):
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(cmd_launch_resume(args), 0)
            self.assertIsInstance(observed_pending[0], dict)
            self.assertEqual(observed_pending[0]["status"], "reserved")
            command = run.call_args.args[0]
            self.assertEqual(command[command.index("-c") + 1], str(Path.home()))
            launched_state = load_state(state_path)
            self.assertIsNone(launched_state["pending_control_launch"])
            self.assertEqual(launched_state["last_control_launch"]["accepted_via"], "exact_process")

    def test_ambiguous_pending_control_launch_is_never_relaunched(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_path = root / "state.json"
            message_path = root / "recovery.md"
            message_path.write_text("continue\n", encoding="utf-8")
            state = default_state("thread-1")
            state["pending_control_launch"] = {
                "launch_id": "ambiguous-1",
                "thread_id": "thread-1",
                "session_name": "pif-evaluation-thread-1",
                "events_path": str(root / "events.jsonl"),
                "exit_receipt_path": str(root / "exit.json"),
            }
            save_state(state, state_path)
            args = argparse.Namespace(
                state=str(state_path),
                message_file=str(message_path),
                session_name="pif-evaluation-thread-1",
                model="gpt-5.5",
                reasoning_effort="xhigh",
                recover_orphaned_open_turn=False,
                observed_milestone_sha256=None,
                observed_queue_sha256=None,
                goals_db=str(root / "goals.sqlite"),
            )
            with (
                patch(
                    "research_factory.pipeline_babysitter.codex_runtime_observation",
                    return_value={"available": True, "verified": True},
                ),
                patch(
                    "research_factory.pipeline_babysitter._pending_control_observation",
                    return_value={
                        "accepted": False,
                        "retryable_predispatch_failure": False,
                        "receipt_status": "running",
                        "tmux_alive": False,
                        "process": {"available": True, "alive": False},
                    },
                ),
                patch("research_factory.pipeline_babysitter.subprocess.run") as launch,
            ):
                with self.assertRaisesRegex(ValueError, "ambiguous"):
                    cmd_launch_resume(args)
            launch.assert_not_called()
            self.assertEqual(load_state(state_path)["pending_control_launch"]["launch_id"], "ambiguous-1")

    def test_crash_after_turn_acceptance_reconciles_without_relaunch(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_path = root / "state.json"
            events = root / "events.jsonl"
            events.write_text(
                json.dumps({"type": "thread.started", "thread_id": "thread-1"})
                + "\n"
                + json.dumps({"type": "turn.started"})
                + "\n",
                encoding="utf-8",
            )
            pending = {
                "schema_version": "pif_pending_control_launch_v1",
                "launch_id": "launch-accepted",
                "reserved_at": "now",
                "phase": "evaluation_watch",
                "thread_id": "thread-1",
                "session_name": "pif-evaluation-thread-1",
                "steering_sha256": "a" * 64,
                "steering_evidence_sha256": "b" * 64,
                "prompt_path": str(root / "prompt.md"),
                "events_path": str(events),
                "errors_path": str(root / "errors.log"),
                "exit_receipt_path": str(root / "exit.json"),
                "runner_path": str(root / "runner.zsh"),
                "auth_policy": "managed_chatgpt_auth_api_keys_unset",
                "auth_preflight": {"authenticated": True},
                "codex_runtime": {"verified": True},
                "prior_last_steering_sha256": None,
                "prior_last_steering_evidence_sha256": None,
            }
            state = default_state("thread-1")
            state["pending_control_launch"] = pending
            save_state(state, state_path)
            with (
                patch(
                    "research_factory.pipeline_babysitter.control_process_observation",
                    return_value={"available": True, "alive": False, "pids": []},
                ),
                patch("research_factory.pipeline_babysitter._tmux_has_session", return_value=False),
            ):
                result = _reconcile_pending_control_launch(state_path)
            self.assertEqual(result["status"], "promoted")
            reconciled = load_state(state_path)
            self.assertIsNone(reconciled["pending_control_launch"])
            self.assertEqual(reconciled["last_control_launch"]["accepted_via"], "turn_started")

    def test_auth_78_before_turn_start_is_retryable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state_path = root / "state.json"
            receipt = root / "exit.json"
            receipt.write_text(
                json.dumps(
                    {
                        "schema_version": "pif_control_exit_v1",
                        "launch_id": "launch-auth-fail",
                        "status": "exited",
                        "stage": "auth_failed",
                        "exit_code": 78,
                    }
                ),
                encoding="utf-8",
            )
            state = default_state("thread-1")
            state["pending_control_launch"] = {
                "launch_id": "launch-auth-fail",
                "thread_id": "thread-1",
                "session_name": "pif-evaluation-thread-1",
                "events_path": str(root / "events.jsonl"),
                "exit_receipt_path": str(receipt),
                "steering_sha256": "a" * 64,
                "steering_evidence_sha256": "b" * 64,
            }
            save_state(state, state_path)
            with (
                patch(
                    "research_factory.pipeline_babysitter.control_process_observation",
                    return_value={"available": True, "alive": False, "pids": []},
                ),
                patch("research_factory.pipeline_babysitter._tmux_has_session", return_value=False),
            ):
                result = _reconcile_pending_control_launch(state_path)
            self.assertEqual(result["status"], "retryable_predispatch_failure")
            reconciled = load_state(state_path)
            self.assertIsNone(reconciled["pending_control_launch"])
            self.assertIsNone(reconciled["last_steering_sha256"])

    def test_managed_auth_observation_rejects_non_chatgpt_login(self) -> None:
        api_key = subprocess.CompletedProcess(args=[], returncode=0, stdout="Logged in using API key\n", stderr="")
        with patch("research_factory.pipeline_babysitter.subprocess.run", return_value=api_key):
            self.assertFalse(managed_chatgpt_auth_observation()["authenticated"])
        chatgpt = subprocess.CompletedProcess(args=[], returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
        with patch("research_factory.pipeline_babysitter.subprocess.run", return_value=chatgpt):
            self.assertTrue(managed_chatgpt_auth_observation()["authenticated"])

    def test_goal_observation_fails_closed_when_database_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            result = goal_observation("thread-1", Path(temp) / "missing.sqlite")
            self.assertEqual(result["error_class"], "goals_db_unavailable")
            self.assertFalse(result["available"])

    def test_state_round_trip_and_duplicate_steering_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "state.json"
            state = default_state("thread-1")
            first = record_steering(state, "continue from the failed judge shard")
            save_state(state, path)
            self.assertEqual(load_state(path)["last_steering_sha256"], first["sha256"])
            with self.assertRaisesRegex(ValueError, "duplicate"):
                record_steering(state, "continue from the failed judge shard")

    @staticmethod
    def _write_valid_evaluation_receipt(
        root: Path,
    ) -> tuple[Path, dict[str, object], dict[str, Path]]:
        root.mkdir(parents=True, exist_ok=True)
        path = root / "receipt.json"
        common = {
            "evaluation_id": "evaluation-1",
            "runtime_lock_sha256": "1" * 64,
            "frozen_configuration_sha256": "2" * 64,
            "reference_sha256": "3" * 64,
            "holdout_manifest_sha256": "4" * 64,
        }
        artifacts = {
            "judge_gate": {
                "gate_passed": True,
                "semantic_noninferior_or_better": True,
                "ab_ba_order_balanced": True,
                "shared_augmented_reference": True,
                "abstention_enabled": True,
            },
            "development_freeze": {
                "development_frozen": True,
                "selection_frozen": True,
                "development_winner_frozen": True,
                "winner_system_id": "candidate-1",
            },
            "untouched_holdout": {
                "untouched_holdout": True,
                "holdout_passed": True,
                "semantic_noninferior_or_better": True,
                "winner_system_id": "candidate-1",
                "baseline_system_id": "baseline-1",
                "holdout_item_count": 60,
                "paired_bootstrap_unit": "source_cluster",
                "density_stratified": True,
                "intent_to_treat_failures_included": True,
                "paired_bootstrap_ci_lower": -0.03,
                "paired_bootstrap_confidence": 0.95,
                "exact_evidence_rate": 1.0,
            },
            "usage_telemetry": {
                "input_tokens": 10,
                "cached_input_tokens": 2,
                "output_tokens": 5,
                "reasoning_output_tokens": 1,
                "total_tokens": 15,
                "production_amortized_total_tokens": 28,
                "production_baseline_total_tokens": 100,
                "expected_turn_count": 1,
                "measured_turn_count": 1,
                "unknown_usage_turn_count": 0,
                "winner_system_id": "candidate-1",
                "baseline_system_id": "baseline-1",
                "paired_item_count": 60,
                "baseline_item_count": 60,
                "candidate_item_count": 60,
                "wall_time_seconds": 1.5,
                "usage_complete": True,
                "cache_telemetry_complete": True,
                "failed_and_retried_calls_included": True,
                "same_exact_items": True,
                "same_concurrency_and_workers": True,
                "same_retry_and_fallback_policy": True,
                "same_cache_policy": True,
                "same_quota_window": True,
                "same_measurement_boundary": True,
                "episode_context_generation_accounted": True,
            },
            "production_integrity": {
                "managed_app_server_auth_only": True,
                "production_unchanged": True,
                "raw_session_token_replay": False,
                "api_key_billing": False,
            },
        }
        bindings = {}
        artifact_paths = {}
        for role, role_payload in artifacts.items():
            artifact_path = root / f"{role}.json"
            artifact_path.write_text(
                json.dumps({"artifact_role": role, **common, **role_payload}),
                encoding="utf-8",
            )
            artifact_paths[role] = artifact_path
            bindings[role] = {
                "path": artifact_path.name,
                "sha256": hashlib.sha256(artifact_path.read_bytes()).hexdigest(),
            }
        receipt = {
            "schema_version": "pif_pipeline_evaluation_receipt_v2",
            **common,
            "goal_complete": True,
            "judge_gate_passed": True,
            "development_frozen": True,
            "untouched_holdout_passed": True,
            "semantic_noninferior_or_better": True,
            "managed_app_server_auth_only": True,
            "usage_and_cache_telemetry_complete": True,
            "production_unchanged_during_evaluation": True,
            "production_amortized_total_token_ratio": 0.28,
            "artifact_hashes": bindings,
        }
        path.write_text(json.dumps(receipt), encoding="utf-8")
        return path, receipt, artifact_paths

    def test_evaluation_receipt_requires_all_gates_and_ratio(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path, receipt, _ = self._write_valid_evaluation_receipt(root)
            self.assertEqual(
                verify_evaluation_receipt(path, root)[
                    "production_amortized_total_token_ratio"
                ],
                0.28,
            )
            receipt["production_amortized_total_token_ratio"] = 0.281
            path.write_text(json.dumps(receipt), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "token_ratio"):
                verify_evaluation_receipt(path, root)
            for invalid_ratio in (-0.1, True, float("nan")):
                receipt["production_amortized_total_token_ratio"] = invalid_ratio
                path.write_text(json.dumps(receipt), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "token_ratio"):
                    verify_evaluation_receipt(path, root)
            receipt["production_amortized_total_token_ratio"] = 0.28
            receipt["artifact_hashes"]["judge_gate"]["sha256"] = "f" * 64
            path.write_text(json.dumps(receipt), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                verify_evaluation_receipt(path, root)

    def test_evaluation_receipt_requires_bound_winner_holdout_and_usage(self) -> None:
        mutations = (
            ("development_freeze", "development_winner_frozen", False, "development freeze"),
            ("development_freeze", "winner_system_id", "", "winner identity"),
            ("untouched_holdout", "semantic_noninferior_or_better", False, "untouched holdout"),
            ("untouched_holdout", "winner_system_id", "candidate-2", "winner identity"),
            ("untouched_holdout", "baseline_system_id", "candidate-1", "baseline identity"),
            ("untouched_holdout", "paired_bootstrap_unit", "segment", "bootstrap design"),
            ("untouched_holdout", "density_stratified", False, "untouched holdout"),
            ("untouched_holdout", "intent_to_treat_failures_included", False, "untouched holdout"),
            ("untouched_holdout", "paired_bootstrap_ci_lower", -0.031, "noninferiority"),
            ("untouched_holdout", "paired_bootstrap_ci_lower", 1.01, "noninferiority"),
            ("untouched_holdout", "paired_bootstrap_confidence", 0.9, "confidence"),
            ("untouched_holdout", "exact_evidence_rate", 0.999, "exact evidence"),
            ("usage_telemetry", "unknown_usage_turn_count", 1, "coverage"),
            ("usage_telemetry", "unknown_usage_turn_count", False, "unknown_usage_turn_count"),
            ("usage_telemetry", "usage_complete", False, "coverage"),
            ("usage_telemetry", "failed_and_retried_calls_included", False, "coverage"),
            ("usage_telemetry", "same_exact_items", False, "coverage"),
            ("usage_telemetry", "same_concurrency_and_workers", False, "coverage"),
            ("usage_telemetry", "same_retry_and_fallback_policy", False, "coverage"),
            ("usage_telemetry", "same_cache_policy", False, "coverage"),
            ("usage_telemetry", "same_quota_window", False, "coverage"),
            ("usage_telemetry", "same_measurement_boundary", False, "coverage"),
            ("usage_telemetry", "episode_context_generation_accounted", False, "coverage"),
            ("usage_telemetry", "winner_system_id", "candidate-2", "system identity"),
            ("usage_telemetry", "baseline_system_id", "baseline-2", "system identity"),
            ("usage_telemetry", "candidate_item_count", 59, "paired item coverage"),
            ("usage_telemetry", "measured_turn_count", 0, "measured_turn_count"),
        )
        with tempfile.TemporaryDirectory() as temp:
            parent = Path(temp)
            for index, (role, field, value, expected_error) in enumerate(mutations):
                with self.subTest(role=role, field=field):
                    root = parent / str(index)
                    path, receipt, artifact_paths = self._write_valid_evaluation_receipt(root)
                    artifact_path = artifact_paths[role]
                    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
                    payload[field] = value
                    artifact_path.write_text(json.dumps(payload), encoding="utf-8")
                    receipt["artifact_hashes"][role]["sha256"] = hashlib.sha256(
                        artifact_path.read_bytes()
                    ).hexdigest()
                    path.write_text(json.dumps(receipt), encoding="utf-8")
                    with self.assertRaisesRegex(ValueError, expected_error):
                        verify_evaluation_receipt(path, root)

    def test_evaluation_receipt_rejects_binding_drift_and_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            parent = Path(temp)
            root = parent / "binding"
            path, receipt, artifact_paths = self._write_valid_evaluation_receipt(root)
            development_path = artifact_paths["development_freeze"]
            development = json.loads(development_path.read_text(encoding="utf-8"))
            development["reference_sha256"] = "5" * 64
            development_path.write_text(json.dumps(development), encoding="utf-8")
            receipt["artifact_hashes"]["development_freeze"]["sha256"] = hashlib.sha256(
                development_path.read_bytes()
            ).hexdigest()
            path.write_text(json.dumps(receipt), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "binding mismatch"):
                verify_evaluation_receipt(path, root)

            symlink_root = parent / "symlink"
            symlink_path, symlink_receipt, symlink_artifacts = (
                self._write_valid_evaluation_receipt(symlink_root)
            )
            judge_path = symlink_artifacts["judge_gate"]
            real_path = parent / "outside-judge.json"
            judge_path.replace(real_path)
            judge_path.symlink_to(real_path)
            symlink_receipt["artifact_hashes"]["judge_gate"]["sha256"] = hashlib.sha256(
                real_path.read_bytes()
            ).hexdigest()
            symlink_path.write_text(json.dumps(symlink_receipt), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "escapes|symlinks"):
                verify_evaluation_receipt(symlink_path, symlink_root)

    def test_accept_evaluation_requires_authoritative_complete_goal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            evaluation_root = root / "evaluation"
            receipt_path, _, _ = self._write_valid_evaluation_receipt(evaluation_root)
            state_path = root / "state.json"
            save_state(default_state("evaluation-thread"), state_path)
            goals_path = root / "goals.sqlite"
            conn = sqlite3.connect(goals_path)
            conn.execute(
                """
                CREATE TABLE thread_goals (
                    thread_id TEXT PRIMARY KEY NOT NULL, goal_id TEXT NOT NULL,
                    objective TEXT NOT NULL, status TEXT NOT NULL,
                    tokens_used INTEGER NOT NULL, time_used_seconds INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO thread_goals VALUES (?,?,?,?,?,?,?)",
                (
                    "evaluation-thread",
                    "goal-1",
                    "private objective",
                    "active",
                    1,
                    1,
                    1_789_000_000_000,
                ),
            )
            conn.commit()
            args = argparse.Namespace(
                receipt=str(receipt_path),
                evaluation_root=str(evaluation_root),
                state=str(state_path),
                goals_db=str(goals_path),
            )
            with self.assertRaisesRegex(ValueError, "goal is not complete"):
                cmd_accept_evaluation(args)
            self.assertEqual(load_state(state_path)["phase"], "evaluation_watch")

            conn.execute(
                "UPDATE thread_goals SET status='complete', updated_at_ms=updated_at_ms+1"
            )
            conn.commit()
            conn.close()
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(cmd_accept_evaluation(args), 0)
            accepted = load_state(state_path)
            self.assertEqual(accepted["phase"], "extraction_handoff")
            self.assertEqual(
                accepted["evaluation_receipt"]["evaluation_id"], "evaluation-1"
            )

    def test_completion_requires_zero_queue_and_manifest_flags(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "factory.sqlite"
            conn = sqlite3.connect(db)
            conn.executescript(
                """
                CREATE TABLE jobs (id INTEGER PRIMARY KEY, job_type TEXT, status TEXT, payload_json TEXT, leased_until TEXT);
                CREATE TABLE episodes (id TEXT);
                CREATE TABLE segments (id TEXT, episode_id TEXT);
                CREATE TABLE labels (id TEXT, segment_id TEXT, label_pack TEXT, status TEXT);
                CREATE TABLE episode_context_runs (id TEXT, episode_id TEXT, label_pack TEXT, status TEXT);
                """
            )
            conn.commit()
            conn.close()
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "all_app_server_usage_accounted": True,
                "all_outputs_validated": True,
                "no_unresolved_audit_failures": True,
                "terminal_quarantine_audited": True,
            }), encoding="utf-8")
            self.assertTrue(completion_audit(db, manifest)["pass"])
            conn = sqlite3.connect(db)
            conn.execute("INSERT INTO jobs (job_type,status,payload_json,leased_until) VALUES ('label_segment','pending',?,NULL)", (json.dumps({"label_pack": "ai_discourse_v3_1"}),))
            conn.execute("INSERT INTO jobs (job_type,status,payload_json,leased_until) VALUES ('manual_transcript_required','pending','{}',NULL)")
            conn.execute("INSERT INTO jobs (job_type,status,payload_json,leased_until) VALUES ('manual_transcript_required','claimed','{}','2099-01-01T00:00:00+00:00')")
            conn.execute("INSERT INTO jobs (job_type,status,payload_json,leased_until) VALUES ('manual_transcript_required','failed','{}',NULL)")
            conn.execute("INSERT INTO jobs (job_type,status,payload_json,leased_until) VALUES ('manual_transcript_required','completed','{}',NULL)")
            conn.execute("INSERT INTO episodes (id) VALUES ('episode-1')")
            conn.execute("INSERT INTO segments (id,episode_id) VALUES ('segment-1','episode-1')")
            conn.commit()
            conn.close()
            failed = completion_audit(db, manifest)
            self.assertFalse(failed["pass"])
            self.assertEqual(failed["blocking_counts"]["pending_manual_transcript_required"], 1)
            self.assertEqual(failed["blocking_counts"]["unresolved_manual_transcript_required"], 3)
            self.assertEqual(failed["blocking_counts"]["eligible_segments_without_v31_output"], 1)
            self.assertEqual(failed["blocking_counts"]["segmented_episodes_without_completed_v31_context"], 1)


if __name__ == "__main__":
    unittest.main()
