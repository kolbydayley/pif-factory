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
