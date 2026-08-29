"""Tests for the attribution lab's measurement primitives.

The lab's verdicts feed a trust decision — the scorer itself must be
verifiably correct.
"""
import importlib.util
import unittest
from pathlib import Path

_P = Path.home() / "pif-factory" / "scripts" / "attribution_lab.py"
spec = importlib.util.spec_from_file_location("attribution_lab", _P)
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)


class ParseTurnsTest(unittest.TestCase):
    def test_parses_named_turns_and_maps_spans(self):
        text = ("Nathan Labenz:\nWelcome to the show.\n"
                "Bronson Schoen:\nThanks for having me. Glad to be here.\n"
                "Nathan Labenz:\nLet's dive in.")
        turns = lab.parse_turns(text)
        self.assertEqual([t["speaker"] for t in turns],
                         ["Nathan Labenz", "Bronson Schoen",
                          "Nathan Labenz"])
        stripped, spans = lab.strip_and_map(turns)
        self.assertNotIn("Nathan Labenz:", stripped)
        # truth lookup lands on the right speaker mid-text
        i = stripped.index("Glad to be here")
        self.assertEqual(lab.truth_at(spans, i, i + 10), "Bronson Schoen")

    def test_boundary_overlap_majority_wins(self):
        spans = [(0, 100, "A"), (100, 200, "B")]
        self.assertEqual(lab.truth_at(spans, 90, 160), "B")
        self.assertEqual(lab.truth_at(spans, 90, 120), "B")
        self.assertEqual(lab.truth_at(spans, 80, 115), "A")


class NameMatchTest(unittest.TestCase):
    def test_exact_and_variant_matches(self):
        self.assertTrue(lab.name_match("Nathan Labenz", "Nathan Labenz"))
        self.assertTrue(lab.name_match("Labenz", "Nathan Labenz"))
        self.assertTrue(lab.name_match("Dr. Nathan Labenz",
                                       "Nathan Labenz"))

    def test_different_people_do_not_match(self):
        self.assertFalse(lab.name_match("Nathan Labenz",
                                        "Bronson Schoen"))
        self.assertFalse(lab.name_match("Nathan Fielder",
                                        "Nathan Labenz"))
        self.assertFalse(lab.name_match("", "Nathan Labenz"))


if __name__ == "__main__":
    unittest.main()
