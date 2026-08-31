"""Contract tests for the split Signal Desk V5 renderer."""

import importlib.util
import unittest
from pathlib import Path


_BUILD = Path.home() / "pif-factory" / "scripts" / "pif_dashboard_build.py"
spec = importlib.util.spec_from_file_location("pif_dashboard_build", _BUILD)
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)


def _payload(detectors=None, topics=None, people=None):
    return {
        "generated_at": "2026-08-27T21:30:00",
        "detectors": detectors or {"emerging": [], "shifting": [],
                                   "contested": [], "fading": []},
        "topics": topics or {},
        "people": people or [],
    }


class RendererV5Test(unittest.TestCase):
    def test_primary_navigation_is_decision_oriented(self):
        for label in ("Briefing", "Issues", "Voices", "Ask"):
            self.assertIn(f">{label}</button>", build.TEMPLATE)
        self.assertIn("Coverage &amp; Trust", build.TEMPLATE)
        self.assertNotIn(">Podcast Funnel</button>", build.TEMPLATE)

    def test_build_is_split_and_does_not_embed_corpus(self):
        self.assertNotIn("__DATA__", build.TEMPLATE)
        self.assertIn("./signal-desk.js", build.TEMPLATE)
        self.assertEqual(
            set(build.PAYLOAD_FILES),
            {"index", "issues", "voices", "network", "coverage"},
        )

    def test_mobile_primary_navigation_stays_four_items(self):
        bottom = build.TEMPLATE.split('<nav class="bottom-nav"', 1)[1]
        bottom = bottom.split("</nav>", 1)[0]
        self.assertEqual(bottom.count('class="nav-link"'), 4)


class ComputeDiffTest(unittest.TestCase):
    def test_no_prev_returns_none(self):
        self.assertIsNone(build.compute_diff(_payload(), None))

    def test_new_upgraded_and_gone_detectors(self):
        prev = _payload(detectors={
            "emerging": [{"topic": "old news", "tier": "moderate"},
                         {"topic": "steady", "tier": "moderate"}],
            "shifting": [], "contested": [], "fading": []})
        cur = _payload(detectors={
            "emerging": [{"topic": "fresh", "tier": "strong"},
                         {"topic": "steady", "tier": "strong"}],
            "shifting": [], "contested": [], "fading": []})
        diff = build.compute_diff(cur, prev)
        changes = {(c["topic"], c["change"]) for c in diff["detectors"]}
        self.assertIn(("fresh", "new"), changes)
        self.assertIn(("old news", "gone"), changes)
        upgraded = next(c for c in diff["detectors"]
                        if c["topic"] == "steady")
        self.assertEqual((upgraded["from"], upgraded["to"]),
                         ("moderate", "strong"))

    def test_top_movers_by_share_delta(self):
        def topic(share):
            return {"series": [{"month": "2026-07", "vol": 10,
                                "share_smooth": share}]}
        prev = _payload(topics={"a": topic(0.10), "b": topic(0.10)})
        cur = _payload(topics={"a": topic(0.30), "b": topic(0.11)})
        diff = build.compute_diff(cur, prev)
        self.assertEqual(diff["movers"][0]["topic"], "a")
        self.assertAlmostEqual(diff["movers"][0]["delta"], 0.20, places=6)


if __name__ == "__main__":
    unittest.main()
