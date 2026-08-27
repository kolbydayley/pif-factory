"""Smoke tests for the Signal Desk renderer template.

The renderer is a single inlined-JS template; these pin that the V4
confidence-tier UI is present: tier badges exist, and the front-page card
picker excludes weak-tier detector hits.
"""
import importlib.util
import json
import unittest
from pathlib import Path

_BUILD = Path.home() / "pif-factory" / "scripts" / "pif_dashboard_build.py"
spec = importlib.util.spec_from_file_location("pif_dashboard_build", _BUILD)
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)


class RendererTierTest(unittest.TestCase):
    def test_template_has_tier_badges(self):
        self.assertIn("tier-badge ${", build.TEMPLATE)  # dynamic badge class
        for css in (".tier-badge.strong", ".tier-badge.moderate",
                    ".tier-badge.weak"):
            self.assertIn(css, build.TEMPLATE)

    def test_front_page_excludes_weak_tier(self):
        # The signalCards picker must filter weak-tier detector hits.
        self.assertIn('tier)!=="weak"', build.TEMPLATE.replace(" ", ""))

    def test_render_with_tiered_fixture(self):
        data = {
            "schema_version": "signal_desk_v4",
            "data_through": "2026-07-01",
            "generated_at": "2026-08-27T00:00:00",
            "weeks": ["2026-W26"], "week_totals": [100],
            "corpus": {"episodes": 1, "labels": 1, "shows": 1,
                       "coverage": {}},
            "topics": {}, "people": [],
            "detectors": {"emerging": [
                {"topic": "x", "tier": "strong", "p_value": 0.001,
                 "pulse_vol": 9, "base_rate": 0.1, "episodes": 3,
                 "shows": 3, "effect": 5.0}],
                "shifting": [], "contested": [], "fading": []},
        }
        html = build.TEMPLATE.replace("__DATA__", json.dumps(data))
        self.assertIn("tier-badge", html)


def _payload(detectors=None, topics=None, people=None):
    return {
        "generated_at": "2026-08-27T21:30:00",
        "detectors": detectors or {"emerging": [], "shifting": [],
                                   "contested": [], "fading": []},
        "topics": topics or {},
        "people": people or [],
    }


class AskLayerTest(unittest.TestCase):
    def test_ask_route_is_wired(self):
        self.assertIn('r.kind==="ask"', build.TEMPLATE)
        self.assertIn("function renderAsk", build.TEMPLATE)

    def test_ask_box_present_on_home(self):
        self.assertIn('id="ask-input"', build.TEMPLATE)

    def test_answers_are_extractive_with_honest_empty_state(self):
        # The ask layer must reuse the existing extractive cards and say
        # plainly when the corpus cannot answer.
        self.assertIn("cannot answer", build.TEMPLATE)
        self.assertIn("function askSearch", build.TEMPLATE)

    def test_copy_prompt_button_exists(self):
        self.assertIn("copyAskPrompt", build.TEMPLATE)


class EvidenceContextPageTest(unittest.TestCase):
    def test_evidence_page_shows_surrounding_transcript(self):
        self.assertIn("context_before", build.TEMPLATE)
        self.assertIn("context_after", build.TEMPLATE)

    def test_evidence_page_lists_same_episode_evidence(self):
        self.assertIn("Also from this episode", build.TEMPLATE)
        self.assertIn("function episodeSiblings", build.TEMPLATE)

    def test_shift_signal_gets_pooled_comparison(self):
        self.assertIn("function shiftCompare", build.TEMPLATE)
        self.assertIn("What the shift signal compares", build.TEMPLATE)


class NetworkMapTest(unittest.TestCase):
    def test_network_route_and_renderer_exist(self):
        self.assertIn('r.kind==="network"', build.TEMPLATE)
        self.assertIn("function renderNetwork", build.TEMPLATE)

    def test_people_tab_links_to_map(self):
        self.assertIn('data-route="network"', build.TEMPLATE)

    def test_nodes_are_clickable_person_links(self):
        # every node group routes to the person page
        self.assertIn('netNode', build.TEMPLATE)


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
            return {"series": [{"week": "2026-W26", "vol": 10,
                                "share_smooth": share}]}
        prev = _payload(topics={"a": topic(0.10), "b": topic(0.10)})
        cur = _payload(topics={"a": topic(0.30), "b": topic(0.11)})
        diff = build.compute_diff(cur, prev)
        self.assertEqual(diff["movers"][0]["topic"], "a")
        self.assertAlmostEqual(diff["movers"][0]["delta"], 0.20, places=6)

    def test_new_people_listed(self):
        prev = _payload(people=[{"name": "Old Hand"}])
        cur = _payload(people=[{"name": "Old Hand"}, {"name": "Newcomer"}])
        diff = build.compute_diff(cur, prev)
        self.assertEqual(diff["new_people"], ["Newcomer"])


if __name__ == "__main__":
    unittest.main()
