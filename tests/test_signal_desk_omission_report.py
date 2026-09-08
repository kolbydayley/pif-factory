import copy
import unittest
from research_factory.signal_desk_omission_review import packets
from research_factory.signal_desk_omission_report import summarize


class OmissionReportTests(unittest.TestCase):
    def fixture(self):
        ps = packets(window_id="dev", source="Demand fell this year.", structure="flattened",
            cases=[{"case_id": "old", "origin": "legacy_C", "kind": "candidate", "event": None}],
            current=[], provenance={}, token_count=lambda _: 1)
        sha = ps[0]["packet_sha256"]
        plan = {"packets": [sha], "ledger": {"dev": {"cases": 1, "case_ids": ["old"]}}}
        reviews = {sha: {"decisions": [{"case_id": "old", "verdict": "supported", "utility": "consequential",
            "coverage": "missing", "current_C_ids": [], "correction": "not_applicable",
            "source_quotes": ["Demand fell this year."], "rationale": "The decline is explicit."}]}}
        return plan, {sha: ps[0]}, reviews

    def test_omission_without_promotion(self):
        r = summarize(*self.fixture())
        self.assertTrue(r["complete_diagnostic"])
        self.assertFalse(r["gold_accepted"])
        self.assertFalse(r["gate_eligible"])
        self.assertEqual(r["expected_cases"], 1)
        self.assertEqual(r["actions"][0]["reasons"], ["potential_consequential_omission"])
        self.assertEqual(r["actions"][0]["source_locations"], [{"start": 0, "end": 22}])

    def test_partial_preserves_unreviewed_denominator(self):
        plan, ps, _ = self.fixture()
        with self.assertRaises(ValueError): summarize(plan, ps, {})
        r = summarize(plan, ps, {}, allow_partial=True)
        self.assertEqual(r["by_origin"]["legacy_C"]["expected_cases"], 1)
        self.assertEqual(r["by_origin"]["legacy_C"]["reviewed_cases"], 0)
        self.assertFalse(r["complete_diagnostic"])

    def test_tampered_packet(self):
        plan, ps, rs = self.fixture()
        next(iter(ps.values()))["transcript_window"] = "An unrelated transcript"
        with self.assertRaises(ValueError): summarize(plan, ps, rs)

    def test_denominator_loss(self):
        plan, ps, rs = self.fixture()
        plan["ledger"]["dev"]["case_ids"] = ["other"]
        with self.assertRaises(ValueError): summarize(plan, ps, rs)

    def test_foreign_review(self):
        plan, ps, rs = self.fixture(); rs["foreign"] = next(iter(rs.values()))
        with self.assertRaises(ValueError): summarize(plan, ps, rs)


if __name__ == "__main__": unittest.main()
