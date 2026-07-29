from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research_factory import (
    app_server_judge_v5_selection_v231_incremental_nonacceptance as v231,
)


class V231IncrementalNonacceptanceTests(unittest.TestCase):
    def test_best_case_ceiling_cannot_pass_frozen_quality_gate(self) -> None:
        predecessor = v231.validate_predecessors()
        gate = v231.build_gate(predecessor)

        self.assertFalse(gate["passed"])
        self.assertEqual(gate["metrics"]["best_case_macro_f1"], 0.764706)
        self.assertEqual(
            gate["metrics"]["best_case_source_macro_f1"],
            {"js-party": 0.705882, "the-changelog": 0.82353},
        )
        self.assertIn("best_case_macro_f1_gte_0_97", gate["failed_checks"])
        self.assertIn("frozen_permutation_projection_exact", gate["failed_checks"])
        self.assertTrue(gate["architecture_level_redesign_authorized"])
        self.assertFalse(gate["further_isolated_field_repair_authorized"])
        self.assertEqual(gate["new_model_usage"]["total_tokens"], 0)

    def test_freeze_is_immutable_and_never_authorizes_holdout(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "v231"
            first = v231.freeze_v231(root)
            second = v231.freeze_v231(root)

            self.assertEqual(first, second)
            self.assertEqual(first["state"], "incremental_strategy_not_accepted")
            self.assertEqual(first["semantic_attempt_count"], 0)
            self.assertFalse(first["holdout_authorized"])
            self.assertFalse(first["production_mutated"])
            gate = json.loads((root / "alignment-ceiling-gate.json").read_text())
            self.assertFalse(gate["semantic_alignment_of_new_witnesses_measured"])


if __name__ == "__main__":
    unittest.main()
