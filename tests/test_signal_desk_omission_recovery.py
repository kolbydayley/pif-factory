from copy import deepcopy
import unittest
from research_factory.signal_desk_omission_review import packets
from research_factory.signal_desk_omission_recovery import partition, repair_packets, reconcile
from research_factory.signal_desk_rubric_reference_packets import digest


class RecoveryTests(unittest.TestCase):
    def fixture(self):
        p = packets(window_id="dev", source="Demand fell this year.", structure="flattened",
            cases=[{"case_id": str(i), "kind": "candidate", "origin": "legacy_C", "event": None} for i in range(2)],
            current=[], provenance={}, token_count=lambda _: 1)[0]
        good = {"case_id": "0", "verdict": "supported", "utility": "consequential", "coverage": "missing",
            "current_C_ids": [], "correction": "not_applicable", "source_quotes": ["Demand fell this year."], "rationale": "Explicit decline."}
        bad = dict(deepcopy(good), case_id="1", source_quotes=["Demand declined this year."])
        return p, {"decisions": [good, bad]}

    def test_target_invalid_only(self):
        p, raw = self.fixture(); parts, ps = repair_packets(p, raw, token_count=lambda _: 1)
        self.assertEqual(len(parts["retained"]), 1)
        self.assertEqual([c["case_id"] for q in ps for c in q["cases"]], ["1"])
        corrected = {"decisions": [dict(raw["decisions"][0], case_id="1")]}
        result, receipt = reconcile(p, raw, [(ps[0], corrected)])
        self.assertEqual(result["decisions"][0], raw["decisions"][0])
        self.assertTrue(receipt["first_pass_failure_preserved"])
        self.assertFalse(receipt["gold_accepted"])

    def test_extra_preserved_not_accepted(self):
        p, raw = self.fixture(); raw["decisions"][1] = dict(raw["decisions"][0], case_id="1")
        raw["decisions"].append(dict(raw["decisions"][0], case_id="unsolicited"))
        result, receipt = reconcile(p, raw, [])
        self.assertEqual(len(result["decisions"]), 2)
        self.assertEqual(receipt["partition"]["out_of_scope_suggestions"], [raw["decisions"][-1]])

    def test_duplicate_requires_repair(self):
        p, raw = self.fixture(); raw["decisions"].append(raw["decisions"][0])
        self.assertEqual(len(partition(p, raw)["failures"]), 2)

    def test_no_incomplete_acceptance(self):
        p, raw = self.fixture()
        with self.assertRaises(ValueError): reconcile(p, raw, [])

    def test_context_and_candidate_tamper(self):
        p, raw = self.fixture(); _, ps = repair_packets(p, raw, token_count=lambda _: 1)
        response = {"decisions": [dict(raw["decisions"][0], case_id="1")]}
        for key, value in (("transcript_window", "Other source"), ("original_output_sha256", "wrong")):
            q = deepcopy(ps[0]); q[key] = value; q["packet_sha256"] = digest({k:v for k,v in q.items() if k != "packet_sha256"})
            with self.assertRaises(ValueError): reconcile(p, raw, [(q, response)])
        q = deepcopy(ps[0]); q["cases"][0]["origin"] = "other"
        q["packet_sha256"] = digest({k:v for k,v in q.items() if k != "packet_sha256"})
        with self.assertRaises(ValueError): reconcile(p, raw, [(q, response)])

    def test_no_truncation(self):
        p, raw = self.fixture()
        with self.assertRaises(ValueError): repair_packets(p, raw, token_count=lambda _: 12000)


if __name__ == "__main__": unittest.main()
