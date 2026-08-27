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


if __name__ == "__main__":
    unittest.main()
