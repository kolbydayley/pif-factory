from __future__ import annotations

import unittest

from research_factory.cohort import EXPECTED_EPISODES, EXPECTED_SHOWS, load_production_cohort


class ProductionCohortTests(unittest.TestCase):
    def test_frozen_cohort_has_expected_shape_and_cross_show_speaker_evidence(self) -> None:
        cohort = load_production_cohort()
        self.assertEqual(len(cohort["episode_ids"]), EXPECTED_EPISODES)
        self.assertEqual(len({item["source_id"] for item in cohort["episodes"]}), EXPECTED_SHOWS)
        self.assertEqual(
            cohort["constraint_evidence"]["repeated_raw_speaker"]["name"],
            "Eric Topol",
        )
        self.assertGreaterEqual(
            len(cohort["constraint_evidence"]["audited_pilot_episode_ids"]),
            8,
        )


if __name__ == "__main__":
    unittest.main()


class Tier100CohortShapeTest(unittest.TestCase):
    def _cohort_payload(self, shows, per_show):
        episodes = [
            {"id": f"ep_{s}_{i}", "source_id": f"show-{s}",
             "published_at": "2026-01-01T00:00:00+00:00"}
            for s in range(shows) for i in range(per_show)]
        return {"schema_version": "pif_production_cohort_v1",
                "cohort_id": "pif-gold-test", "episodes": episodes}

    def _load(self, payload):
        import json, tempfile
        from research_factory.cohort import load_production_cohort
        with tempfile.NamedTemporaryFile("w", suffix=".json",
                                         delete=False) as fh:
            json.dump(payload, fh)
            path = fh.name
        return load_production_cohort(path)

    def test_balanced_100_cohort_loads(self):
        cohort = self._load(self._cohort_payload(10, 10))
        self.assertEqual(len(cohort["episode_ids"]), 100)

    def test_unbalanced_100_cohort_rejected(self):
        from research_factory.cohort import CohortValidationError
        payload = self._cohort_payload(10, 10)
        payload["episodes"][0]["source_id"] = "show-1"  # 9/11 imbalance
        with self.assertRaises(CohortValidationError):
            self._load(payload)

    def test_unrecognized_size_rejected(self):
        from research_factory.cohort import CohortValidationError
        with self.assertRaises(CohortValidationError):
            self._load(self._cohort_payload(6, 10))
