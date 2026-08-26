from __future__ import annotations

import datetime as dt
import sqlite3
import unittest

from research_factory.pif_discourse_aggregates import collect


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
              published_at TEXT
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
              segment_id TEXT NOT NULL,
              actor_name TEXT,
              actor_type TEXT,
              concept_name TEXT,
              stance TEXT,
              claim_type TEXT,
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
            INSERT INTO episodes (id, source_id, title, published_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                ("ep_one", "show_one", "Episode One", "2026-06-15"),
                ("ep_two", "show_two", "Episode Two", "2026-06-22"),
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
                        segment_id,
                        f"Speaker {segment_id} {occurrence}",
                        "guest",
                        "single_episode_phrase",
                        "supportive",
                        "observation",
                        "{}",
                    )
                )
        self.conn.executemany(
            """
            INSERT INTO actor_positions
              (segment_id, actor_name, actor_type, concept_name, stance,
               claim_type, evidence_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
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


if __name__ == "__main__":
    unittest.main()
