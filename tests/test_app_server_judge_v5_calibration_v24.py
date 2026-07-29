from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v24 import (
    DEFAULT_SOURCE_ROOT,
    JudgeV5CalibrationV24Error,
    build_v24_scoreable_recovery,
)


class JudgeV5CalibrationV24Tests(unittest.TestCase):
    def test_v24_adopts_v23_outputs_without_new_semantic_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            terminal = build_v24_scoreable_recovery(
                source_root=DEFAULT_SOURCE_ROOT,
                output_root=Path(tmp) / "v24",
            )
        self.assertEqual(terminal["state"], "completed")
        self.assertEqual(
            terminal["terminal_reason"], "judge_full_calibration_quality_gate_not_passed"
        )
        self.assertFalse(terminal["calibration_passed"])
        self.assertFalse(terminal["selection_authorized"])
        self.assertFalse(terminal["production_mutated"])
        self.assertTrue(terminal["accounting_complete"])
        self.assertEqual(terminal["usage_status"], "complete")
        self.assertEqual(terminal["source_v23_usage"]["total_tokens"], 790731)
        self.assertEqual(terminal["new_recovery_usage"]["total_tokens"], 0)
        self.assertEqual(terminal["cumulative_usage"]["total_tokens"], 790731)
        self.assertEqual(
            terminal["failed_quality_gates"],
            [
                "alignment_f1",
                "structured_field_accuracy",
                "support_specificity",
                "unpaired_exact_case_rate",
            ],
        )
        self.assertEqual(terminal["metrics"]["order_bias"], 0.0)
        self.assertEqual(
            terminal["observable_disagreements"]["sha256"],
            "9f2878effb4afae3e170e0ab4ebf22ac112ebe3b78f7cb29c147a17504c7b0ae",
        )

    def test_v24_is_idempotent_after_terminal_freeze(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "v24"
            first = build_v24_scoreable_recovery(
                source_root=DEFAULT_SOURCE_ROOT,
                output_root=output,
            )
            second = build_v24_scoreable_recovery(
                source_root=DEFAULT_SOURCE_ROOT,
                output_root=output,
            )
        self.assertEqual(first, second)

    def test_v24_rejects_missing_source_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(JudgeV5CalibrationV24Error):
                build_v24_scoreable_recovery(
                    source_root=Path(tmp) / "missing-v23",
                    output_root=Path(tmp) / "v24",
                )


if __name__ == "__main__":
    unittest.main()
