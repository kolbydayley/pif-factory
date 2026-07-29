from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_PROMPT_BYTES,
    MAX_SCHEMA_BYTES,
)
from research_factory.app_server_judge_v5_calibration_v51_diagnostic import (
    CONTROL_CASE_IDS,
    DIAGNOSTIC_CASE_IDS,
    RESIDUAL_CASE_IDS,
    V51_SPEC_VERSION,
    freeze_v51_diagnostic,
    run_v51_diagnostic,
    v51_alignment_instructions,
    v51_pointwise_instructions,
)
from tests.test_app_server_judge_v5_calibration_v26_diagnostic import (
    FakeV26StagedClient,
)


class JudgeV5CalibrationV51DiagnosticTests(unittest.TestCase):
    def test_case_selection_is_balanced_disjoint_and_frozen(self) -> None:
        self.assertEqual(len(DIAGNOSTIC_CASE_IDS), 12)
        self.assertEqual(len(set(DIAGNOSTIC_CASE_IDS)), 12)
        self.assertEqual(len(RESIDUAL_CASE_IDS), 6)
        self.assertEqual(len(CONTROL_CASE_IDS), 6)
        self.assertFalse(set(RESIDUAL_CASE_IDS) & set(CONTROL_CASE_IDS))

    def test_instructions_are_topic_neutral_and_preserve_semantic_boundary(self) -> None:
        pointwise = v51_pointwise_instructions().lower()
        alignment = v51_alignment_instructions().lower()
        self.assertIn("merge versus split", pointwise)
        self.assertIn("entire source excerpt", pointwise)
        self.assertIn("semantic event identity", alignment)
        self.assertIn("nearest wording", alignment)
        for topic in ("database", "legal", "language tutor", "audio production"):
            self.assertNotIn(topic, pointwise)
            self.assertNotIn(topic, alignment)

    def test_freeze_is_zero_call_idempotent_and_capacity_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "v51"
            frozen = freeze_v51_diagnostic(output_dir=root)
            again = freeze_v51_diagnostic(output_dir=root)
            self.assertEqual(again["spec"], frozen["spec"])
            spec = frozen["spec"]
            self.assertEqual(spec["schema_version"], V51_SPEC_VERSION)
            self.assertEqual(spec["case_count"], 12)
            self.assertEqual(spec["semantic_model_calls_performed_during_freeze"], 0)
            self.assertEqual(len(spec["turn_plan"]), 7)
            self.assertFalse(spec["full_calibration_authorized_before_diagnostic_pass"])
            self.assertFalse(spec["holdout_authorized"])
            self.assertFalse(list(root.glob("turns/*/capacity.json")))
            self.assertFalse(list(root.glob("turns/*/sidecar.json")))
            for shard in spec["frozen_inputs"]["pointwise_shards"]:
                self.assertLessEqual(shard["prompt"]["size_bytes"], MAX_PROMPT_BYTES)
                self.assertLessEqual(shard["schema"]["size_bytes"], MAX_SCHEMA_BYTES)
            policy = json.loads((root / "capacity-policy.json").read_text())
            self.assertEqual(policy["retry_count_per_turn"], 0)
            self.assertEqual(policy["minimum_remaining_reserve_percent"], 20)
            self.assertTrue(policy["managed_chatgpt_auth_only"])


class JudgeV5CalibrationV51ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_perfect_fake_attempt_passes_without_adjudication_or_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "v51"
            frozen = freeze_v51_diagnostic(output_dir=root)
            client = FakeV26StagedClient(frozen["truth"])

            def factory(_policy_path: Path) -> FakeV26StagedClient:
                return client

            terminal = await run_v51_diagnostic(
                output_dir=root,
                client_factory=factory,
            )
            self.assertEqual(terminal["state"], "completed")
            self.assertTrue(terminal["diagnostic_passed"])
            self.assertTrue(terminal["full_calibration_authorized"])
            self.assertFalse(terminal["holdout_authorized"])
            self.assertFalse(terminal["production_mutated"])
            self.assertEqual(terminal["semantic_retry_count"], 0)
            self.assertEqual(len(client.calls), 6)
            self.assertEqual(terminal["turn_count"], 6)
            self.assertEqual(terminal["usage"]["total_tokens"], 7200)


if __name__ == "__main__":
    unittest.main()
