import copy
import unittest
from research_factory.signal_desk_omission_review import packets, validate


class OmissionReviewTests(unittest.TestCase):
    def packet(self, count=1, kind="candidate", token_count=lambda _: 1):
        return packets(window_id="dev", source="Ada: Industry demand fell sharply this year.", structure="speaker_turn",
            cases=[{"case_id": str(i), "kind": kind, "event": None} for i in range(count)],
            current=[{"event_id": "c1", "claim_text": "Demand fell", "evidence_text": "Industry demand fell sharply this year."}],
            provenance={"original": "immutable"}, token_count=token_count)

    def response(self):
        return {"decisions": [{"case_id": "0", "verdict": "supported", "utility": "consequential",
            "coverage": "covered", "current_C_ids": ["c1"], "correction": "not_applicable",
            "source_quotes": ["Industry demand fell sharply this year."], "rationale": "Source explicitly describes falling demand."}]}

    def test_valid(self):
        self.assertEqual(validate(self.response(), self.packet()[0]), self.response())

    def test_denominator_and_full_source(self):
        ps = self.packet(61)
        self.assertEqual([len(p["cases"]) for p in ps], [25, 25, 11])
        self.assertEqual(len({c["case_id"] for p in ps for c in p["cases"]}), 61)
        self.assertTrue(all(p["transcript_window"] == ps[0]["transcript_window"] for p in ps))

    def test_source_never_truncated(self):
        with self.assertRaises(ValueError): self.packet(token_count=lambda _: 12000)

    def test_reject_missing_duplicate_or_foreign(self):
        for decisions in ([], self.response()["decisions"] * 2):
            with self.assertRaises(ValueError): validate({"decisions": decisions}, self.packet()[0])
        v = self.response(); v["decisions"][0]["current_C_ids"] = ["invented"]
        with self.assertRaises(ValueError): validate(v, self.packet()[0])

    def test_requires_unique_exact_evidence(self):
        v = self.response(); v["decisions"][0]["source_quotes"] = ["Demand increased"]
        with self.assertRaises(ValueError): validate(v, self.packet()[0])
        p = self.packet()[0]; p["transcript_window"] *= 2
        with self.assertRaises(ValueError): validate(self.response(), p)

    def test_correction_and_coverage_consistency(self):
        with self.assertRaises(ValueError): validate(self.response(), self.packet(kind="correction")[0])
        v = self.response(); v["decisions"][0]["correction"] = "equivalent"
        validate(v, self.packet(kind="correction")[0])
        v = self.response(); v["decisions"][0]["coverage"] = "missing"
        with self.assertRaises(ValueError): validate(v, self.packet()[0])

    def test_duplicate_case_population(self):
        p = self.packet()[0]
        with self.assertRaises(ValueError):
            packets(window_id="dev", source=p["transcript_window"], structure="speaker_turn", cases=p["cases"] * 2,
                current=[], provenance={}, token_count=lambda _: 1)


if __name__ == "__main__": unittest.main()
