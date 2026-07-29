from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory import app_server_judge_v5_selection_continuation as continuation
from research_factory import app_server_runtime_lock_v18 as runtime_lock
from research_factory.app_server_capacity_probe import LAUNCH_CAPACITY_VERSION
from research_factory.app_server_judge_v5_diagnostic import _record
from research_factory.app_server_runtime_lock_v18 import (
    RUNTIME_LOCK_VERSION,
    RuntimeLockV18Error,
    build_runtime_lock_v18,
    prepare_wait_intent,
    verify_runtime_lock_v18,
)
from tests.test_app_server_judge_v5_selection import (
    _fresh_calibration_fixture,
    _write_json,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
REUSE_CONTRACT = (
    REPO_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v4.json"
)


def _launch_capacity(path: Path) -> None:
    _write_json(
        path,
        {
            "schema_version": LAUNCH_CAPACITY_VERSION,
            "observed_at": "2026-07-13T00:00:00+00:00",
            "managed_chatgpt_auth_verified": True,
            "plan_type": "pro",
            "limit_id": "codex",
            "primary_used_percent": 3,
            "primary_resets_at": None,
            "rate_limit_reached_type": None,
            "maximum_primary_used_percent": 20,
            "cleared_for_semantic_work": True,
            "probe_count": 1,
            "thread_started": False,
            "turn_started": False,
            "production_mutation_performed": False,
        },
    )


class RuntimeLockV18Test(unittest.TestCase):
    def test_wait_intent_binds_exact_predecessor_and_selection_paths(self):
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            root = Path(directory)
            intent = root / "control/wait-intent-v18.json"
            selection_root = root / "selection"
            predecessor = root / "v17-terminal.json"
            value = prepare_wait_intent(
                repo_root=REPO_ROOT,
                output_path=intent,
                selection_root=selection_root,
                calibration_continuation_terminal=predecessor,
            )
            self.assertEqual(value["selection_root"], str(selection_root.resolve()))
            self.assertEqual(
                value["fresh_calibration_terminal_path"], str(predecessor.resolve())
            )
            with self.assertRaisesRegex(RuntimeLockV18Error, "selection path drifted"):
                runtime_lock.verify_wait_intent(
                    repo_root=REPO_ROOT,
                    path=intent,
                    expected_selection_root=root / "different-selection",
                    expected_calibration_terminal=predecessor,
                )

    def test_lock_binds_fresh_attempts_five_arms_and_runtime_sources(self):
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            root = Path(directory)
            calibration, predecessor = _fresh_calibration_fixture(root / "fresh")
            selection_root = root / "selection"
            control = root / "control"
            intent = control / "wait-intent-v18.json"
            capacity = control / "prelaunch-capacity.json"
            manifest = root / "runtime-lock-v18.json"
            prepare_wait_intent(
                repo_root=REPO_ROOT,
                output_path=intent,
                selection_root=selection_root,
                calibration_continuation_terminal=predecessor,
            )
            _launch_capacity(capacity)
            v17_manifest = root / "runtime-lock-v17.json"
            v17_control = root / "v17-control"
            _write_json(v17_manifest, {"schema_version": "synthetic-v17"})
            for name in (
                "wait-intent-v17.json",
                "prelaunch-capacity.json",
                "launch-receipt-v17.json",
            ):
                _write_json(v17_control / name, {"name": name})
            original_manifest = runtime_lock.V17_MANIFEST
            original_control = runtime_lock.V17_CONTROL_ROOT
            runtime_lock.V17_MANIFEST = v17_manifest
            runtime_lock.V17_CONTROL_ROOT = v17_control
            try:
                with patch.object(
                    runtime_lock, "verify_runtime_lock_v17", return_value={}
                ):
                    value = build_runtime_lock_v18(
                        repo_root=REPO_ROOT,
                        manifest_path=manifest,
                        wait_intent_path=intent,
                        launch_capacity_path=capacity,
                        calibration_root=calibration,
                        continuation_terminal_path=predecessor,
                        reuse_contract_path=REUSE_CONTRACT,
                        selection_root=selection_root,
                    )
                self.assertEqual(value["schema_version"], RUNTIME_LOCK_VERSION)
                self.assertEqual(value["supersedes"]["clean_arm_count"], 5)
                self.assertEqual(
                    len(value["supersedes"]["artifacts"]), 27 + 6 * 23
                )
                labels = {
                    row["label"] for row in value["supersedes"]["artifacts"]
                }
                self.assertIn("interrupted_batch_5_same_thread", labels)
                self.assertIn("clean_arm_batch_8_same_thread", labels)
                self.assertIn("fresh_pointwise_support_shard_00_sidecar", labels)
                self.assertIn("fresh_neutral_alignment_canary_schema", labels)

                tampered = json.loads(manifest.read_text(encoding="utf-8"))
                tampered["files"][-1]["sha256"] = "0" * 64
                _write_json(manifest, tampered)
                with self.assertRaisesRegex(RuntimeLockV18Error, "runtime file drifted"):
                    verify_runtime_lock_v18(
                        repo_root=REPO_ROOT, manifest_path=manifest
                    )
            finally:
                runtime_lock.V17_MANIFEST = original_manifest
                runtime_lock.V17_CONTROL_ROOT = original_control


class SelectionContinuationTest(unittest.IsolatedAsyncioTestCase):
    def test_terminal_state_distinguishes_infrastructure_from_measured_gate_failure(self):
        self.assertEqual(
            continuation._terminal_state_for_selection(
                winner_frozen=False,
                terminal_reason="infrastructure_or_judge_attempt_failed",
            ),
            "failed",
        )
        self.assertEqual(
            continuation._terminal_state_for_selection(
                winner_frozen=False,
                terminal_reason="development_quality_or_cost_gate_not_passed",
            ),
            "stopped",
        )
        self.assertEqual(
            continuation._terminal_state_for_selection(
                winner_frozen=True,
                terminal_reason="five_arm_selection_passed_winner_frozen",
            ),
            "completed",
        )

    async def test_nonadmissible_calibration_stops_before_capacity_or_selection(self):
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            root = Path(directory)
            calibration = root / "calibration"
            selection_root = root / "selection"
            control = root / "control"
            predecessor = root / "v17-terminal.json"
            _write_json(predecessor, {"state": "failed"})
            _write_json(calibration / "terminal.json", {"state": "failed"})
            prepare_wait_intent(
                repo_root=REPO_ROOT,
                output_path=control / "wait-intent-v18.json",
                selection_root=selection_root,
                calibration_continuation_terminal=predecessor,
            )

            async def forbidden_capacity(**_kwargs):
                self.fail("capacity probe must not run after failed calibration")

            with patch.object(
                continuation, "wait_for_launch_capacity", forbidden_capacity
            ):
                terminal = await continuation.run_continuation(
                    repo_root=REPO_ROOT,
                    calibration_root=calibration,
                    calibration_continuation_terminal=predecessor,
                    reuse_contract_path=REUSE_CONTRACT,
                    selection_root=selection_root,
                    control_root=control,
                    manifest_path=root / "runtime-lock-v18.json",
                    poll_seconds=0.01,
                )
            self.assertEqual(terminal["state"], "stopped")
            self.assertFalse(terminal["semantic_attempt_started"])
            self.assertFalse((control / "prelaunch-capacity.json").exists())
            self.assertFalse(selection_root.exists())

    async def test_capacity_lock_receipt_precede_one_selection_attempt(self):
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            root = Path(directory)
            calibration = root / "calibration"
            calibration.mkdir()
            predecessor = root / "v17-terminal.json"
            _write_json(predecessor, {"state": "completed"})
            selection_root = root / "selection"
            control = root / "control"
            manifest = root / "runtime-lock-v18.json"
            prepare_wait_intent(
                repo_root=REPO_ROOT,
                output_path=control / "wait-intent-v18.json",
                selection_root=selection_root,
                calibration_continuation_terminal=predecessor,
            )
            order = []

            def fake_fresh(**_kwargs):
                order.append("fresh")
                return {
                    "records": {"outer_terminal": _record(predecessor)},
                    "terminal": {"selection_authorized": True},
                }

            def fake_contract(_path):
                order.append("contract")
                return {"clean_arms": [{} for _ in range(5)]}

            async def fake_capacity(*, output_path, **_kwargs):
                order.append("capacity")
                _launch_capacity(Path(output_path))
                return json.loads(Path(output_path).read_text())

            def fake_lock(**_kwargs):
                order.append("lock")
                _write_json(manifest, {"schema_version": RUNTIME_LOCK_VERSION})
                return {"schema_version": RUNTIME_LOCK_VERSION}

            def fake_receipt(*, output_path, **_kwargs):
                order.append("receipt")
                _write_json(Path(output_path), {"status": "declared"})
                return {"status": "declared"}

            async def fake_selection(**_kwargs):
                order.append("selection")
                self.assertTrue(manifest.is_file())
                self.assertTrue((control / "prelaunch-capacity.json").is_file())
                self.assertTrue((control / "launch-receipt-v18.json").is_file())
                _write_json(
                    selection_root / "selection/full-judge/turns/turn-00/capacity.json",
                    {"cleared": True},
                )
                _write_json(
                    selection_root / "v5-selection-terminal.json",
                    {
                        "state": "completed",
                        "terminal_reason": "five_arm_selection_passed_winner_frozen",
                        "winner_frozen": True,
                    },
                )
                return {
                    "state": "completed",
                    "terminal_reason": "five_arm_selection_passed_winner_frozen",
                    "winner_frozen": True,
                }

            with (
                patch.object(
                    continuation,
                    "verify_fresh_calibration_for_selection",
                    fake_fresh,
                ),
                patch.object(
                    continuation, "verify_v5_reuse_contract", fake_contract
                ),
                patch.object(
                    continuation, "wait_for_launch_capacity", fake_capacity
                ),
                patch.object(continuation, "build_runtime_lock_v18", fake_lock),
                patch.object(continuation, "write_launch_receipt", fake_receipt),
                patch.object(
                    continuation, "run_v5_five_arm_selection", fake_selection
                ),
            ):
                terminal = await continuation.run_continuation(
                    repo_root=REPO_ROOT,
                    calibration_root=calibration,
                    calibration_continuation_terminal=predecessor,
                    reuse_contract_path=REUSE_CONTRACT,
                    selection_root=selection_root,
                    control_root=control,
                    manifest_path=manifest,
                    poll_seconds=0.01,
                )
            self.assertEqual(
                order,
                ["fresh", "contract", "capacity", "lock", "receipt", "selection"],
            )
            self.assertEqual(terminal["state"], "completed")
            self.assertTrue(terminal["semantic_attempt_started"])
            self.assertTrue(terminal["winner_frozen"])
            self.assertTrue(terminal["holdout_preparation_authorized"])
            self.assertFalse(terminal["holdout_model_calls_authorized"])


if __name__ == "__main__":
    unittest.main()
