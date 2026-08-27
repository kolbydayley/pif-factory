from __future__ import annotations

import importlib.util
import sqlite3
import unittest
from pathlib import Path

from research_factory.pif_discourse_aggregates import collect_funnel


ROOT = Path(__file__).resolve().parents[1]
BUILD_PATH = ROOT / "scripts" / "pif_dashboard_build.py"
SPEC = importlib.util.spec_from_file_location("pif_dashboard_build_funnel", BUILD_PATH)
BUILD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILD)


class PodcastFunnelAggregateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(
            """
            CREATE TABLE sources (
              id TEXT PRIMARY KEY, name TEXT, category TEXT, rss_url TEXT,
              enabled INTEGER
            );
            CREATE TABLE episodes (id TEXT PRIMARY KEY, source_id TEXT);
            CREATE TABLE transcripts (
              id TEXT PRIMARY KEY, episode_id TEXT, status TEXT
            );
            CREATE TABLE segments (id TEXT PRIMARY KEY, episode_id TEXT);
            CREATE TABLE labels (id TEXT PRIMARY KEY, segment_id TEXT, status TEXT);
            INSERT INTO sources VALUES
              ('show-a', 'Show A', 'ai', 'https://example.com/a.xml', 1),
              ('show-b', 'Show B', 'tech', 'https://example.com/b.xml', 1);
            INSERT INTO episodes VALUES
              ('a1', 'show-a'), ('a2', 'show-a'), ('a3', 'show-a'),
              ('b1', 'show-b');
            INSERT INTO transcripts VALUES
              ('t1', 'a1', 'ready'), ('t2', 'a2', 'quarantined'),
              ('t3', 'b1', 'ready');
            INSERT INTO segments VALUES ('s1', 'a1'), ('s2', 'b1');
            INSERT INTO labels VALUES
              ('l1', 's1', 'ready'), ('l2', 's2', 'ready');
            """
        )

    def tearDown(self) -> None:
        self.conn.close()

    def test_funnel_separates_inventory_from_processed_episodes(self) -> None:
        funnel = collect_funnel(self.conn)

        self.assertEqual(funnel["enrolled_shows"], 2)
        self.assertEqual(funnel["quarantined_transcript_episodes"], 1)
        self.assertEqual(
            [(stage["episodes"], stage["shows"])
             for stage in funnel["stages"]],
            [(4, 2), (3, 2), (2, 2), (2, 2)],
        )
        show_a = next(show for show in funnel["shows"]
                      if show["id"] == "show-a")
        self.assertEqual(show_a["stage"], "intelligence_ready")
        self.assertEqual(show_a["transcript_quarantined"], 1)


class PodcastFunnelRendererTest(unittest.TestCase):
    def test_funnel_is_a_primary_route(self) -> None:
        self.assertIn('data-route="funnel"', BUILD.TEMPLATE)
        self.assertIn('r.kind==="funnel"', BUILD.TEMPLATE)
        self.assertIn("function renderFunnel", BUILD.TEMPLATE)

    def test_roster_and_enrollment_controls_exist(self) -> None:
        self.assertIn('id="show-results"', BUILD.TEMPLATE)
        self.assertIn('id="enroll-form"', BUILD.TEMPLATE)
        self.assertIn("Open private enrollment request", BUILD.TEMPLATE)
        self.assertIn("github.com/kolbydayley/pif-factory/issues/new", BUILD.TEMPLATE)

    def test_public_page_does_not_claim_anonymous_production_write(self) -> None:
        self.assertIn("rather than writing anonymously", BUILD.TEMPLATE)
        self.assertIn("Feed verification remains the gate", BUILD.TEMPLATE)


if __name__ == "__main__":
    unittest.main()
