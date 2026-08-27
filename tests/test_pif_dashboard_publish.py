"""Tests for the Signal Desk publish guard (scripts/pif_dashboard_publish.py).

Publishing is a one-line authenticated PUT; the value here is the guard that
refuses to push a stale or broken artifact to the public host.
"""
import datetime as dt
import importlib.util
import unittest
from pathlib import Path

_PUB = Path.home() / "pif-factory" / "scripts" / "pif_dashboard_publish.py"
spec = importlib.util.spec_from_file_location("pif_dashboard_publish", _PUB)
pub = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pub)

_TODAY = dt.date(2026, 8, 27)


def _data(**kw):
    base = {
        "schema_version": "signal_desk_v4",
        "generated_at": "2026-08-27T00:30:00",
        "data_through": "2026-07-01",
        "detectors": {"emerging": [], "shifting": [],
                      "contested": [], "fading": []},
        "topics": {"a": {}},
        "corpus": {"labels": 1000},
    }
    base.update(kw)
    return base


class PublishGuardTest(unittest.TestCase):
    def test_fresh_build_passes(self):
        ok, reason = pub.should_publish(_data(), today=_TODAY)
        self.assertTrue(ok, reason)

    def test_stale_generated_at_refuses(self):
        ok, reason = pub.should_publish(
            _data(generated_at="2026-08-20T00:30:00"), today=_TODAY)
        self.assertFalse(ok)
        self.assertIn("stale", reason)

    def test_wrong_schema_refuses(self):
        ok, reason = pub.should_publish(
            _data(schema_version="signal_desk_v3"), today=_TODAY)
        self.assertFalse(ok)
        self.assertIn("schema", reason)

    def test_empty_topics_refuses(self):
        ok, reason = pub.should_publish(_data(topics={}), today=_TODAY)
        self.assertFalse(ok)
        self.assertIn("empty", reason)

    def test_funnel_payload_is_published_beside_the_page(self):
        self.assertEqual(pub.SITE_FUNNEL_FILE.name,
                         "pif-signal-desk-funnel.json")
        self.assertEqual(pub.FUNNEL_JSON.name,
                         "pif-signal-desk-funnel.json")


if __name__ == "__main__":
    unittest.main()
