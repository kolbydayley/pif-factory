from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research_factory.extractor_gate import (
    ExtractorGateError,
    initialize_gate,
    read_gate,
    record_development_attempt,
    record_holdout_attempt,
    record_shadow_attempt,
)


EVALUATOR = "e" * 64


class FinalExtractorGateTest(unittest.TestCase):
    def _write(self, path: Path, value: dict[str, object]) -> Path:
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def _passing(self, **extra: object) -> dict[str, object]:
        return {
            "evaluator_sha256": EVALUATOR,
            "exact_evidence_rate": 1.0,
            "terminal_schema_completion": 0.95,
            "supported_claim_precision": 0.9,
            "speaker_attribution_precision": 0.9,
            "dense_medium_recall": 0.8,
            "no_signal_false_positive_rate": 0.05,
            "paired_f1_delta": -0.05,
            **extra,
        }

    def test_one_repair_one_holdout_and_one_shadow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "gate.json"
            dev = self._write(root / "dev.json", {"segment_count": 60, "source_count": 22, "source_ids": ["a"], "episode_ids": ["d"]})
            holdout = self._write(root / "holdout.json", {"source_count": 1, "episode_count": 1, "source_ids": ["b"], "episode_ids": ["h"]})
            initialize_gate(state, development_manifest=dev, holdout_manifest=holdout, evaluator_sha256=EVALUATOR)
            failed = self._passing(dense_medium_recall=0.79)
            record_development_attempt(state, self._write(root / "failed.json", failed))
            self.assertEqual(read_gate(state)["state"], "development")
            record_development_attempt(state, self._write(root / "repair.json", self._passing()))
            self.assertEqual(read_gate(state)["state"], "holdout_ready")
            with self.assertRaises(ExtractorGateError):
                record_development_attempt(state, root / "repair.json")
            record_holdout_attempt(state, self._write(root / "holdout-metrics.json", self._passing()))
            shadow = self._passing(episode_count=20, network_count=10, audited_claim_count=200)
            result = record_shadow_attempt(state, self._write(root / "shadow.json", shadow))
            self.assertEqual(result["state"], "promotion_eligible")
            self.assertTrue(result["promotion_eligible"])
            self.assertFalse(result["production_switch_allowed"])

    def test_evaluator_cannot_change_after_holdout_starts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "gate.json"
            dev = self._write(root / "dev.json", {"segment_count": 60, "source_count": 22})
            holdout = self._write(root / "holdout.json", {"source_count": 1, "episode_count": 1})
            initialize_gate(state, development_manifest=dev, holdout_manifest=holdout, evaluator_sha256=EVALUATOR)
            bad = self._passing(evaluator_sha256="f" * 64)
            with self.assertRaisesRegex(ExtractorGateError, "evaluator changed"):
                record_development_attempt(state, self._write(root / "bad.json", bad))


if __name__ == "__main__":
    unittest.main()
