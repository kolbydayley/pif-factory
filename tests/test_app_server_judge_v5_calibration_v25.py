from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v25 import (
    DEFAULT_V23_ROOT,
    DEFAULT_V24_ROOT,
    JudgeV5CalibrationV25Error,
    build_v25_presemantic_recovery,
)


class JudgeV5CalibrationV25Tests(unittest.TestCase):
    def test_v25_writes_incomplete_recovery_terminal_without_semantic_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            terminal = build_v25_presemantic_recovery(output_root=Path(tmp) / "v25")
            root = Path(tmp) / "v25"
            taxonomy = json.loads(
                (root / "sanitized-error-taxonomy.json").read_text(encoding="utf-8")
            )
            clearance = json.loads(
                (root / "minimal-clearance-analysis.json").read_text(encoding="utf-8")
            )
            diagnostic = json.loads(
                (root / "diagnostic-spec.json").read_text(encoding="utf-8")
            )

        self.assertEqual(terminal["state"], "inactive")
        self.assertEqual(
            terminal["terminal_reason"], "inactive_incomplete_recovery_required"
        )
        self.assertEqual(
            terminal["babysitter_status"], "inactive_incomplete_recovery_required"
        )
        self.assertFalse(terminal["development_quality_gate_failure_is_overall_completion"])
        self.assertFalse(terminal["semantic_attempt_started"])
        self.assertEqual(terminal["semantic_turn_count"], 0)
        self.assertEqual(terminal["new_semantic_usage"]["total_tokens"], 0)
        self.assertEqual(terminal["predecessor_usage"]["total_tokens"], 790731)
        self.assertFalse(terminal["selection_authorized"])
        self.assertFalse(terminal["holdout_authorized"])
        self.assertFalse(terminal["production_mutated"])

        self.assertTrue(taxonomy["privacy"]["sanitized"])
        self.assertFalse(taxonomy["privacy"]["contains_transcript_text"])
        self.assertFalse(taxonomy["privacy"]["contains_prompt_text"])
        self.assertFalse(taxonomy["privacy"]["contains_model_rationale_text"])
        self.assertEqual(taxonomy["summary"]["case_count"], 66)
        self.assertEqual(taxonomy["summary"]["witness_count"], 182)
        self.assertEqual(
            taxonomy["summary"]["by_direction"]["structured_false_positive"], 105
        )
        self.assertEqual(
            taxonomy["summary"]["by_direction"]["support_false_positive"], 1
        )
        self.assertEqual(
            taxonomy["summary"]["by_direction"]["unpaired_case_mismatch"], 5
        )
        self.assertEqual(
            taxonomy["summary"]["alignment_pair_counts"], {"fn": 4, "fp": 5, "tp": 71}
        )

        structured = clearance["failed_gate_clearance"]["structured_field_accuracy"]
        self.assertEqual(structured["current_correct"], 143)
        self.assertEqual(structured["required_correct"], 173)
        self.assertEqual(structured["minimum_corrected_witness_decisions"], 30)
        support = clearance["failed_gate_clearance"]["support_specificity"]
        self.assertEqual(support["false_positive_count"], 1)
        self.assertEqual(support["minimum_corrected_support_decisions"], 1)
        unpaired = clearance["failed_gate_clearance"]["unpaired_exact_case_rate"]
        self.assertEqual(unpaired["minimum_corrected_case_partitions"], 2)
        alignment = clearance["failed_gate_clearance"]["alignment_f1"]
        self.assertEqual(
            alignment["minimum_operation_models"][
                "convert_false_pair_to_missing_expected_pair"
            ]["minimum_operations"],
            1,
        )
        self.assertFalse(clearance["shared_root_cause"])

        self.assertFalse(diagnostic["semantic_attempt_authorized_by_this_artifact"])
        self.assertEqual(diagnostic["case_count"], 18)
        self.assertEqual(len({case["case_id"] for case in diagnostic["cases"]}), 18)
        self.assertEqual(diagnostic["turn_plan"]["retry_policy"], "zero_retries_one_attempt_per_turn")
        self.assertEqual(diagnostic["turn_plan"]["expected_total_token_ceiling"], 420000)

    def test_v25_is_idempotent_after_terminal_freeze(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "v25"
            first = build_v25_presemantic_recovery(output_root=root)
            second = build_v25_presemantic_recovery(output_root=root)
        self.assertEqual(first, second)

    def test_v25_rejects_non_v24_predecessor_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            v24 = Path(tmp) / "v24"
            v24.mkdir()
            terminal = json.loads((DEFAULT_V24_ROOT / "terminal.json").read_text())
            terminal["terminal_reason"] = "judge_full_calibration_passed_selection_authorized"
            (v24 / "terminal.json").write_text(json.dumps(terminal), encoding="utf-8")
            with self.assertRaises(JudgeV5CalibrationV25Error):
                build_v25_presemantic_recovery(
                    v23_root=DEFAULT_V23_ROOT,
                    v24_root=v24,
                    output_root=Path(tmp) / "v25",
                )


if __name__ == "__main__":
    unittest.main()
