from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v54_diagnostic import (
    V54_SPEC_VERSION,
    freeze_v54_diagnostic,
    run_v54_diagnostic,
    v54_alignment_instructions,
    v54_pointwise_instructions,
)
from tests.test_app_server_judge_v5_calibration_v26_diagnostic import FakeV26StagedClient


class JudgeV5CalibrationV54DiagnosticTests(unittest.IsolatedAsyncioTestCase):
    async def test_freeze_and_perfect_fresh_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "v54"
            frozen = freeze_v54_diagnostic(output_dir=root)
            again = freeze_v54_diagnostic(output_dir=root)
            self.assertEqual(again["spec"], frozen["spec"])
            self.assertEqual(frozen["spec"]["schema_version"], V54_SPEC_VERSION)
            self.assertFalse(frozen["spec"]["v51_semantic_outputs_reused_for_promotion"])
            self.assertFalse(list(root.glob("turns/*/sidecar.json")))
            client = FakeV26StagedClient(frozen["truth"])

            def factory(_policy_path: Path) -> FakeV26StagedClient:
                return client

            terminal = await run_v54_diagnostic(output_dir=root, client_factory=factory)
            self.assertTrue(terminal["diagnostic_passed"])
            self.assertTrue(terminal["full_calibration_authorized"])
            self.assertFalse(terminal["holdout_authorized"])
            self.assertEqual(terminal["semantic_retry_count"], 0)
            self.assertEqual(len(client.calls), 6)

    async def test_serialization_rules_are_explicit_and_topic_neutral(self) -> None:
        pointwise = v54_pointwise_instructions().lower()
        alignment = v54_alignment_instructions().lower()
        self.assertIn("independently contradicts", pointwise)
        self.assertIn("exact verbatim substring", alignment)
        self.assertIn("return []", alignment)
        for topic in ("database", "legal", "language tutor", "audio production"):
            self.assertNotIn(topic, pointwise)
            self.assertNotIn(topic, alignment)


if __name__ == "__main__":
    unittest.main()
