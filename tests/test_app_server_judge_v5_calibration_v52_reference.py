from __future__ import annotations

import json
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v52_reference import (
    V52_SPEC_VERSION,
    apply_v52_reference,
    build_disputed_structured_input,
    freeze_v52_reference,
    validate_v52_output,
)
from research_factory.app_server_judge_v5_calibration_v50_reference_restore import (
    DEFAULT_OUTPUT_ROOT as V50_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v51_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V51_ROOT,
)


class JudgeV5CalibrationV52ReferenceTests(unittest.TestCase):
    def _input(self):
        truth = json.loads((V50_ROOT / "canonical-calibration-truth.private.json").read_text())
        pointwise_input = json.loads((V51_ROOT / "pointwise-input-full.private.json").read_text())
        pointwise_output = json.loads((V51_ROOT / "pointwise-output-full.private.json").read_text())
        return truth, build_disputed_structured_input(
            canonical_truth=truth,
            pointwise_input=pointwise_input,
            pointwise_output=pointwise_output,
        )

    def test_dispute_selection_contains_no_prior_labels_or_alignment(self) -> None:
        _truth, value = self._input()
        self.assertEqual(len(value["units"]), 16)
        self.assertFalse(value["prior_labels_present"])
        self.assertFalse(value["expected_answers_present"])
        self.assertFalse(value["alignment_outputs_present"])
        for unit in value["units"]:
            self.assertEqual(
                set(unit), {"case_id", "witness_id", "source_excerpt", "structured_event"}
            )

    def test_output_validation_and_patch_change_only_structured_truth(self) -> None:
        truth, value = self._input()
        output = {
            "units": [
                {
                    "case_id": unit["case_id"],
                    "witness_id": unit["witness_id"],
                    "structured_field_verdict": "incorrect",
                    "field_issue_fields": ["actor", "speaker"],
                    "field_evidence_spans": [],
                    "field_rationale": "Independent field audit.",
                }
                for unit in value["units"]
            ]
        }
        self.assertEqual(validate_v52_output(output, value), [])
        patched = apply_v52_reference(truth, output)
        self.assertEqual(patched["cases"].keys(), truth["cases"].keys())
        for case_id in truth["cases"]:
            self.assertEqual(
                patched["cases"][case_id]["proposition"], truth["cases"][case_id]["proposition"]
            )
            self.assertEqual(patched["cases"][case_id]["pairs"], truth["cases"][case_id]["pairs"])
            self.assertEqual(
                patched["cases"][case_id]["equivalence_groups"],
                truth["cases"][case_id]["equivalence_groups"],
            )

    def test_validator_rejects_nonexact_evidence(self) -> None:
        _truth, value = self._input()
        unit = value["units"][0]
        output = {
            "units": [
                {
                    "case_id": row["case_id"],
                    "witness_id": row["witness_id"],
                    "structured_field_verdict": "incorrect",
                    "field_issue_fields": ["actor"],
                    "field_evidence_spans": ["not an exact source span"] if row is unit else [],
                    "field_rationale": "Audit.",
                }
                for row in value["units"]
            ]
        }
        self.assertIn("unit_0_evidence", validate_v52_output(output, value))

    def test_freeze_is_idempotent_and_presemantic(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "v52"
            frozen = freeze_v52_reference(output_dir=root)
            again = freeze_v52_reference(output_dir=root)
            self.assertEqual(again["spec"], frozen["spec"])
            self.assertEqual(frozen["spec"]["schema_version"], V52_SPEC_VERSION)
            self.assertEqual(frozen["spec"]["witness_count"], 16)
            self.assertFalse(frozen["spec"]["prior_labels_in_model_input"])
            self.assertFalse(frozen["spec"]["canonical_alignment_truth_mutable"])
            self.assertFalse(list(root.glob("turns/*/capacity.json")))
            self.assertFalse(list(root.glob("turns/*/sidecar.json")))
            policy = json.loads((root / "capacity-policy.json").read_text())
            self.assertEqual(policy["ordered_turn_names"], ["structured_reference_adjudication"])
            self.assertEqual(policy["retry_count_per_turn"], 0)


if __name__ == "__main__":
    unittest.main()
