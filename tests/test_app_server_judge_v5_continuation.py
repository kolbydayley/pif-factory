from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research_factory import app_server_judge_v5_continuation as continuation
from research_factory import app_server_runtime_lock_v17 as runtime_lock
from research_factory.app_server_capacity_probe import LAUNCH_CAPACITY_VERSION
from research_factory.app_server_judge_v5_diagnostic import _record
from research_factory.app_server_judge_v5_fresh_calibration import FreshCalibrationError
from research_factory.app_server_judge_v5_continuation import run_continuation
from research_factory.app_server_runtime_lock_v17 import (
    RUNTIME_LOCK_VERSION,
    build_runtime_lock_v17,
    prepare_wait_intent,
    verify_runtime_lock_v17,
)
from research_factory.util import write_text_atomic
from tests.test_app_server_judge_v5_fresh_calibration import _make_reference_root


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(
        path,
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
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


class RuntimeLockV17Test(unittest.TestCase):
    def test_lock_binds_reference_sidecars_and_rejects_runtime_drift(self):
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            temp = Path(directory)
            reference_root = temp / "reference"
            control_root = temp / "control"
            wait_intent = control_root / "wait-intent-v17.json"
            capacity = control_root / "prelaunch-capacity.json"
            manifest = temp / "runtime-lock-v17.json"
            original_reference = runtime_lock.DEFAULT_REFERENCE_ROOT
            runtime_lock.DEFAULT_REFERENCE_ROOT = reference_root
            try:
                prepare_wait_intent(repo_root=REPO_ROOT, output_path=wait_intent)
                _make_reference_root(reference_root)
                _launch_capacity(capacity)
                value = build_runtime_lock_v17(
                    repo_root=REPO_ROOT,
                    manifest_path=manifest,
                    wait_intent_path=wait_intent,
                    launch_capacity_path=capacity,
                    reference_root=reference_root,
                )
                verified = verify_runtime_lock_v17(
                    repo_root=REPO_ROOT, manifest_path=manifest
                )
                self.assertEqual(value["schema_version"], RUNTIME_LOCK_VERSION)
                self.assertEqual(verified["schema_version"], RUNTIME_LOCK_VERSION)
                self.assertEqual(len(value["supersedes"]["artifacts"]), 47)
                tampered = json.loads(manifest.read_text())
                tampered["files"][-1]["sha256"] = "0" * 64
                _write_json(manifest, tampered)
                with self.assertRaisesRegex(
                    runtime_lock.RuntimeLockV17Error, "runtime file drifted"
                ):
                    verify_runtime_lock_v17(
                        repo_root=REPO_ROOT, manifest_path=manifest
                    )
            finally:
                runtime_lock.DEFAULT_REFERENCE_ROOT = original_reference


class ContinuationSupervisorTest(unittest.IsolatedAsyncioTestCase):
    async def test_nonadmissible_reference_stops_before_capacity_or_calibration(self):
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            temp = Path(directory)
            reference_root = temp / "reference"
            control_root = temp / "control"
            calibration_root = temp / "calibration"
            original_reference = runtime_lock.DEFAULT_REFERENCE_ROOT
            runtime_lock.DEFAULT_REFERENCE_ROOT = reference_root
            try:
                prepare_wait_intent(
                    repo_root=REPO_ROOT,
                    output_path=control_root / "wait-intent-v17.json",
                )
                _write_json(reference_root / "terminal.json", {"state": "failed"})
                terminal = await run_continuation(
                    repo_root=REPO_ROOT,
                    reference_root=reference_root,
                    calibration_root=calibration_root,
                    control_root=control_root,
                    manifest_path=temp / "runtime-lock-v17.json",
                    poll_seconds=0.01,
                )
            finally:
                runtime_lock.DEFAULT_REFERENCE_ROOT = original_reference
        self.assertEqual(terminal["state"], "stopped")
        self.assertEqual(
            terminal["terminal_reason"],
            "fixture_reference_not_admissible_fresh_calibration_not_started",
        )
        self.assertFalse(terminal["semantic_attempt_started"])
        self.assertFalse(calibration_root.exists())
        self.assertFalse((control_root / "prelaunch-capacity.json").exists())

    async def test_capacity_lock_and_receipt_precede_fresh_calibration(self):
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "work") as directory:
            temp = Path(directory)
            reference_root = temp / "reference"
            control_root = temp / "control"
            calibration_root = temp / "calibration"
            manifest = temp / "runtime-lock-v17.json"
            original_reference = runtime_lock.DEFAULT_REFERENCE_ROOT
            runtime_lock.DEFAULT_REFERENCE_ROOT = reference_root
            prepare_wait_intent(
                repo_root=REPO_ROOT,
                output_path=control_root / "wait-intent-v17.json",
            )
            _write_json(reference_root / "terminal.json", {"state": "completed"})
            order = []

            async def fake_capacity(*, output_path, **_kwargs):
                order.append("capacity")
                _launch_capacity(Path(output_path))
                return json.loads(Path(output_path).read_text())

            def fake_reference(_path):
                order.append("reference")
                return {"records": {"terminal": _record(reference_root / "terminal.json")}}

            def fake_lock(**_kwargs):
                order.append("lock")
                _write_json(manifest, {"schema_version": RUNTIME_LOCK_VERSION})
                return {"schema_version": RUNTIME_LOCK_VERSION}

            async def fake_calibration(**_kwargs):
                order.append("calibration")
                self.assertTrue((control_root / "launch-receipt-v17.json").is_file())
                self.assertTrue((control_root / "prelaunch-capacity.json").is_file())
                _write_json(calibration_root / "terminal.json", {"state": "completed"})
                return {
                    "state": "completed",
                    "terminal_reason": (
                        "fresh_reference_calibration_passed_selection_authorized"
                    ),
                    "calibration_passed": True,
                    "selection_authorized": True,
                }

            originals = (
                continuation.wait_for_launch_capacity,
                continuation.load_frozen_fixture_reference,
                continuation.build_runtime_lock_v17,
                continuation.run_fresh_reference_calibration,
            )
            continuation.wait_for_launch_capacity = fake_capacity
            continuation.load_frozen_fixture_reference = fake_reference
            continuation.build_runtime_lock_v17 = fake_lock
            continuation.run_fresh_reference_calibration = fake_calibration
            try:
                terminal = await run_continuation(
                    repo_root=REPO_ROOT,
                    reference_root=reference_root,
                    calibration_root=calibration_root,
                    control_root=control_root,
                    manifest_path=manifest,
                    poll_seconds=0.01,
                )
            finally:
                (
                    continuation.wait_for_launch_capacity,
                    continuation.load_frozen_fixture_reference,
                    continuation.build_runtime_lock_v17,
                    continuation.run_fresh_reference_calibration,
                ) = originals
                runtime_lock.DEFAULT_REFERENCE_ROOT = original_reference
        self.assertEqual(order, ["reference", "capacity", "lock", "calibration"])
        self.assertEqual(terminal["state"], "completed")
        self.assertTrue(terminal["selection_authorized"])


if __name__ == "__main__":
    unittest.main()
