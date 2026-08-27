from __future__ import annotations

import datetime as dt
import sqlite3
import unittest
from collections import Counter

from research_factory.pif_discourse_aggregates import (
    build_topic_canon,
    collect,
    short_excerpt,
)


class DiscourseAggregateBreadthTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE episodes (
              id TEXT PRIMARY KEY,
              source_id TEXT NOT NULL,
              title TEXT NOT NULL,
              published_at TEXT,
              url TEXT,
              audio_url TEXT
            );
            CREATE TABLE segments (
              id TEXT PRIMARY KEY,
              episode_id TEXT NOT NULL
            );
            CREATE TABLE labels (
              segment_id TEXT NOT NULL,
              output_json TEXT NOT NULL
            );
            CREATE TABLE actor_positions (
              id TEXT PRIMARY KEY,
              segment_id TEXT NOT NULL,
              actor_name TEXT,
              actor_type TEXT,
              concept_name TEXT,
              stance TEXT,
              claim_type TEXT,
              confidence REAL,
              evidence_json TEXT NOT NULL
            );
            CREATE TABLE canonical_people (
              id TEXT PRIMARY KEY,
              display_name TEXT
            );
            CREATE TABLE expert_authority_scores (
              canonical_person_id TEXT,
              score REAL,
              status TEXT
            );
            """
        )
        self.conn.executemany(
            """
            INSERT INTO episodes
              (id, source_id, title, published_at, url, audio_url)
            VALUES (?, ?, ?, ?, ?, NULL)
            """,
            (
                ("ep_one", "show_one", "Episode One", "2026-06-15",
                 "https://example.com/episode-one"),
                ("ep_two", "show_two", "Episode Two", "2026-06-22",
                 "https://example.com/episode-two"),
            ),
        )
        self.conn.executemany(
            "INSERT INTO segments (id, episode_id) VALUES (?, ?)",
            (
                ("seg_one_a", "ep_one"),
                ("seg_one_b", "ep_one"),
                ("seg_two_a", "ep_two"),
                ("seg_two_b", "ep_two"),
            ),
        )
        positions = []
        for segment_id in ("seg_one_a", "seg_one_b", "seg_two_a", "seg_two_b"):
            for occurrence in range(2):
                positions.append(
                    (
                        f"pos_{segment_id}_{occurrence}",
                        segment_id,
                        f"Speaker {segment_id} {occurrence}",
                        "guest",
                        "single_episode_phrase",
                        "supportive",
                        "observation",
                        0.8,
                        '{"evidence":"A short exact quote."}',
                    )
                )
        self.conn.executemany(
            """
            INSERT INTO actor_positions
              (id, segment_id, actor_name, actor_type, concept_name, stance,
               claim_type, confidence, evidence_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            positions,
        )

    def tearDown(self) -> None:
        self.conn.close()

    def test_multiple_segments_in_two_episodes_do_not_pass_three_episode_gate(self) -> None:
        payload = collect(self.conn, now=dt.date(2026, 7, 1))

        topic = payload["topics"]["single episode phrase"]
        self.assertEqual(topic["pulse_vol"], 8)
        self.assertEqual(topic["pulse_episodes"], 2)
        self.assertEqual(topic["pulse_shows"], 2)
        self.assertEqual(payload["detectors"]["emerging"], [])
        active_weeks = [s for s in topic["series"] if s["vol"]]
        self.assertTrue(active_weeks)
        self.assertTrue(all("share_smooth" in s for s in topic["series"]))
        self.assertTrue(all(s["week_total"] >= s["vol"] for s in active_weeks))
        self.assertEqual(len(topic["evidence"]), 8)
        self.assertEqual(
            {item["source_url"] for item in topic["evidence"]},
            {"https://example.com/episode-one",
             "https://example.com/episode-two"},
        )

    def test_topic_related_issues_are_episode_breadth_ranked(self) -> None:
        self.conn.execute(
            """
            INSERT INTO actor_positions
              (id, segment_id, actor_name, actor_type, concept_name, stance,
               claim_type, confidence, evidence_json)
            VALUES ('pos_related', 'seg_one_a', 'Related Speaker', 'guest',
                    'adjacent_issue', 'skeptical', 'observation', 0.7,
                    '{"evidence":"A related issue quote."}')
            """
        )

        payload = collect(self.conn, now=dt.date(2026, 7, 1))

        self.assertEqual(
            payload["topics"]["single episode phrase"]["related"][0],
            {"topic": "adjacent issue", "shared_episodes": 1,
             "shared_shows": 1},
        )

    def test_moves_require_different_episodes_at_least_14_days_apart(self) -> None:
        self.conn.executemany(
            """
            INSERT INTO actor_positions
              (id, segment_id, actor_name, actor_type, concept_name, stance,
               claim_type, confidence, evidence_json)
            VALUES (?, ?, 'Mover', 'guest', 'forecast', ?, 'prediction', 0.8,
                    '{"evidence":"A dated forecast."}')
            """,
            (("move_one", "seg_one_a", "supportive"),
             ("move_two", "seg_two_a", "skeptical")),
        )

        too_close = collect(self.conn, now=dt.date(2026, 7, 1))
        mover = next(p for p in too_close["people"] if p["name"] == "Mover")
        self.assertEqual(mover["moves"], [])

        self.conn.execute(
            "UPDATE episodes SET published_at = '2026-07-01' WHERE id = 'ep_two'"
        )
        separated = collect(self.conn, now=dt.date(2026, 7, 15))
        mover = next(p for p in separated["people"] if p["name"] == "Mover")
        self.assertEqual(len(mover["moves"]), 1)

    def test_topic_clustering_junk_filter_and_excerpt_cleanup(self) -> None:
        canon = build_topic_canon(Counter({
            "other": 999,
            "agents": 100,
            "agentic ai systems": 30,
            "enterprise ai": 80,
            "enterprise ai adoption": 20,
        }))

        self.assertNotIn("other", canon)
        self.assertEqual(canon["agentic ai systems"], "agents")
        self.assertEqual(canon["enterprise ai adoption"], "enterprise ai")
        self.assertEqual(
            short_excerpt("Speaker 1: First line.\nSpeaker 2: Second line."),
            "First line. Second line.",
        )


if __name__ == "__main__":
    unittest.main()
