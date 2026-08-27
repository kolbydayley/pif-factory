from __future__ import annotations

import datetime as dt
import json
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
              episode_id TEXT NOT NULL,
              text_path TEXT
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


_SCHEMA = """
CREATE TABLE episodes (
  id TEXT PRIMARY KEY, source_id TEXT NOT NULL, title TEXT NOT NULL,
  published_at TEXT, url TEXT, audio_url TEXT
);
CREATE TABLE segments (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, text_path TEXT);
CREATE TABLE labels (segment_id TEXT NOT NULL, output_json TEXT NOT NULL);
CREATE TABLE actor_positions (
  id TEXT PRIMARY KEY, segment_id TEXT NOT NULL, actor_name TEXT,
  actor_type TEXT, concept_name TEXT, stance TEXT, claim_type TEXT,
  confidence REAL, evidence_json TEXT NOT NULL
);
CREATE TABLE canonical_people (id TEXT PRIMARY KEY, display_name TEXT);
CREATE TABLE expert_authority_scores (
  canonical_person_id TEXT, score REAL, status TEXT
);
"""

# now=2026-07-01 -> 26-week frame W02..W27; Mondays Jan 5 .. Jun 29.
_NOW = dt.date(2026, 7, 1)
_BASE_MONDAYS = [dt.date(2026, 1, 5) + dt.timedelta(weeks=k)
                 for k in range(22)]
_PULSE_MONDAYS = [dt.date(2026, 6, 8) + dt.timedelta(weeks=k)
                  for k in range(4)]


class DiscourseDetectorSignificanceTest(unittest.TestCase):
    """Detectors must carry p-values/tiers and ignore coverage artifacts."""

    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(_SCHEMA)
        self._n = 0

    def tearDown(self) -> None:
        self.conn.close()

    def _add_labeled(self, day: dt.date, topic: str, count: int,
                     stance: str = "neutral", source_id: str = "show_bg",
                     episode_id: str | None = None) -> None:
        self._n += 1
        eid = episode_id or f"ep_{self._n}"
        row = self.conn.execute(
            "SELECT 1 FROM episodes WHERE id = ?", (eid,)).fetchone()
        if not row:
            self.conn.execute(
                "INSERT INTO episodes (id, source_id, title, published_at, url)"
                " VALUES (?, ?, ?, ?, ?)",
                (eid, source_id, eid, day.isoformat(),
                 f"https://example.com/{eid}"))
        sid = f"seg_{self._n}"
        self.conn.execute(
            "INSERT INTO segments (id, episode_id) VALUES (?, ?)", (sid, eid))
        topics = [{"topic": topic, "stance": stance, "intensity": 1}] * count
        self.conn.execute(
            "INSERT INTO labels (segment_id, output_json) VALUES (?, ?)",
            (sid, json.dumps({"topics": topics})))

    def _fill_background(self, per_base_week: int, per_pulse_week: int,
                         topic: str = "corpus filler noise") -> None:
        for day in _BASE_MONDAYS:
            self._add_labeled(day, topic, per_base_week)
        for day in _PULSE_MONDAYS:
            self._add_labeled(day, topic, per_pulse_week)

    def test_coverage_driven_jump_fires_at_most_weak(self) -> None:
        # Corpus coverage jumps 3->40 mentions/week in the pulse window.
        # The target topic's raw count jumps with it, but its RATE does not.
        self._fill_background(per_base_week=3, per_pulse_week=40)
        self._add_labeled(dt.date(2026, 3, 9), "spiky term", 1,
                          source_id="show_a")
        self._add_labeled(dt.date(2026, 4, 13), "spiky term", 1,
                          source_id="show_b")
        for i, day in enumerate(_PULSE_MONDAYS[:3]):
            self._add_labeled(day, "spiky term", 4,
                              source_id=f"show_{'abc'[i]}")

        payload = collect(self.conn, now=_NOW)
        hits = [d for d in payload["detectors"]["emerging"]
                if d["topic"] == "spiky term"]
        self.assertTrue(hits, "legacy rule should still surface it as weak")
        self.assertEqual(hits[0]["tier"], "weak")
        self.assertIn("p_value", hits[0])

    def test_genuine_rate_surge_fires_strong(self) -> None:
        # Coverage is FLAT (40/week throughout); the topic goes 0 -> 30.
        self._fill_background(per_base_week=40, per_pulse_week=40)
        for i, day in enumerate(_PULSE_MONDAYS[:3]):
            self._add_labeled(day, "brand new thing", 10,
                              source_id=f"show_{'abc'[i]}")

        payload = collect(self.conn, now=_NOW)
        hits = [d for d in payload["detectors"]["emerging"]
                if d["topic"] == "brand new thing"]
        self.assertTrue(hits)
        self.assertEqual(hits[0]["tier"], "strong")
        self.assertLess(hits[0]["p_value"], 0.01)

    def test_tiny_stance_flip_fires_at_most_weak_shifting(self) -> None:
        # 10 baseline vs 8 pulse observations with TV ~0.33 — the legacy
        # 0.3 threshold fired on this; it must now be weak at best.
        self._fill_background(per_base_week=5, per_pulse_week=5)
        base_days = [dt.date(2026, 2, 9), dt.date(2026, 3, 9),
                     dt.date(2026, 4, 13)]
        for i, day in enumerate(base_days):
            self._add_labeled(day, "flip topic", 1, stance="supportive",
                              source_id=f"show_{'abc'[i]}")
        for i, day in enumerate(base_days):
            self._add_labeled(day, "flip topic", 2 if i < 2 else 3,
                              stance="skeptical", source_id=f"show_{'abc'[i]}")
        for i, day in enumerate(_PULSE_MONDAYS[:3]):
            self._add_labeled(day, "flip topic",
                              2 if i < 2 else 1, stance="supportive",
                              source_id=f"show_{'abc'[i]}")
        self._add_labeled(_PULSE_MONDAYS[0], "flip topic", 3,
                          stance="skeptical", source_id="show_a")

        payload = collect(self.conn, now=_NOW)
        hits = [d for d in payload["detectors"]["shifting"]
                if d["topic"] == "flip topic"]
        if hits:  # may legitimately fire under the legacy rule
            self.assertEqual(hits[0]["tier"], "weak")

    def test_low_sample_flag_fires_on_thin_weeks(self) -> None:
        # 20 mentions/week everywhere except one pulse week with 2.
        for day in _BASE_MONDAYS:
            self._add_labeled(day, "corpus filler noise", 20)
        for day in _PULSE_MONDAYS[:3]:
            self._add_labeled(day, "corpus filler noise", 20)
        self._add_labeled(_PULSE_MONDAYS[3], "corpus filler noise", 2)

        payload = collect(self.conn, now=_NOW)
        series = payload["topics"]["corpus filler noise"]["series"]
        flags = [s["low_sample"] for s in series if s["week_total"] > 0]
        self.assertEqual(sum(flags), 1)
        thin_week = next(s for s in series if s["week_total"] == 2)
        self.assertTrue(thin_week["low_sample"])

    def test_inflections_mark_significant_rate_surges(self) -> None:
        # Flat coverage, topic goes 0 -> 30 in the pulse window: the series
        # must carry a surge inflection inside the pulse weeks, and the
        # steady background topic must carry none.
        self._fill_background(per_base_week=40, per_pulse_week=40)
        for i, day in enumerate(_PULSE_MONDAYS[:3]):
            self._add_labeled(day, "brand new thing", 10,
                              source_id=f"show_{'abc'[i]}")
        payload = collect(self.conn, now=_NOW)
        inflections = payload["topics"]["brand new thing"]["inflections"]
        self.assertTrue(inflections)
        self.assertEqual(inflections[0]["kind"], "surge")
        self.assertIn(inflections[0]["week"],
                      [s["week"] for s in
                       payload["topics"]["brand new thing"]["series"][-4:]])
        background = payload["topics"]["corpus filler noise"]
        self.assertEqual([i for i in background.get("inflections", [])
                          if i["kind"] == "surge"], [])

    def test_payload_reports_latest_episode_for_freshness_gap(self) -> None:
        self._fill_background(per_base_week=5, per_pulse_week=5)
        payload = collect(self.conn, now=_NOW)
        self.assertEqual(payload["latest_episode"],
                         _PULSE_MONDAYS[-1].isoformat())

    def _add_position(self, episode_id: str, day: dt.date, source_id: str,
                      person: str, topic: str = "shared topic") -> None:
        self._n += 1
        if not self.conn.execute("SELECT 1 FROM episodes WHERE id = ?",
                                 (episode_id,)).fetchone():
            self.conn.execute(
                "INSERT INTO episodes (id, source_id, title, published_at, url)"
                " VALUES (?, ?, ?, ?, ?)",
                (episode_id, source_id, episode_id, day.isoformat(),
                 f"https://example.com/{episode_id}"))
        sid = f"pseg_{self._n}"
        self.conn.execute(
            "INSERT INTO segments (id, episode_id) VALUES (?, ?)",
            (sid, episode_id))
        self.conn.execute(
            "INSERT INTO actor_positions (id, segment_id, actor_name,"
            " actor_type, concept_name, stance, claim_type, confidence,"
            " evidence_json) VALUES (?, ?, ?, 'guest', ?, 'supportive',"
            " 'observation', 0.8, '{\"evidence\":\"quote\"}')",
            (f"pp_{self._n}", sid, person, topic))

    def test_trust_is_pagerank_over_coappearance_network(self) -> None:
        d1, d2, d3 = (dt.date(2026, 6, 8), dt.date(2026, 6, 15),
                      dt.date(2026, 6, 22))
        # Connector co-appears with two fans across three episodes.
        for eid, day, src in (("net_e1", d1, "show_a"),
                              ("net_e2", d2, "show_b"),
                              ("net_e3", d3, "show_c")):
            self._add_position(eid, day, src, "Connector")
        for eid, day, src in (("net_e1", d1, "show_a"),
                              ("net_e2", d2, "show_b")):
            self._add_position(eid, day, src, "Fan One")
        for eid, day, src in (("net_e2", d2, "show_b"),
                              ("net_e3", d3, "show_c")):
            self._add_position(eid, day, src, "Fan Two")
        # Isolated pair only ever appears together.
        for eid, day in (("net_e4", d1), ("net_e5", d2)):
            self._add_position(eid, day, "show_d", "Pair A")
            self._add_position(eid, day, "show_d", "Pair B")

        payload = collect(self.conn, now=_NOW)
        by_name = {p["name"]: p for p in payload["people"]}
        for name in ("Connector", "Fan One", "Pair A"):
            trust = by_name[name]["trust"]
            for key in ("score", "tier", "n_links", "basis"):
                self.assertIn(key, trust, name)
        self.assertGreater(by_name["Connector"]["trust"]["score"],
                           by_name["Pair A"]["trust"]["score"])
        self.assertEqual(by_name["Connector"]["trust"]["n_links"], 2)
        self.assertIn("pagerank", by_name["Connector"]["trust"]["basis"])

    def test_evidence_carries_surrounding_transcript_context(self) -> None:
        import tempfile
        text = ("Earlier discussion setting the scene for the claim. "
                "The quoted sentence lives here in the middle. "
                "And afterwards the host pushes back on the framing.")
        start = text.index("The quoted")
        end = text.index(" And afterwards")
        tmp = tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False)
        tmp.write(text)
        tmp.close()
        day = dt.date(2026, 6, 15)
        self.conn.execute(
            "INSERT INTO episodes (id, source_id, title, published_at, url)"
            " VALUES ('ctx_ep', 'show_ctx', 'Context Episode', ?,"
            " 'https://example.com/ctx')", (day.isoformat(),))
        self.conn.execute(
            "INSERT INTO segments (id, episode_id, text_path)"
            " VALUES ('ctx_seg', 'ctx_ep', ?)", (tmp.name,))
        self.conn.execute(
            "INSERT INTO actor_positions (id, segment_id, actor_name,"
            " actor_type, concept_name, stance, claim_type, confidence,"
            " evidence_json) VALUES ('ctx_pos', 'ctx_seg', 'Quoted Guest',"
            " 'guest', 'context topic', 'supportive', 'observation', 0.9, ?)",
            (json.dumps({"evidence": text[start:end],
                         "start": start, "end": end}),))

        payload = collect(self.conn, now=_NOW)
        ev = payload["topics"]["context topic"]["evidence"][0]
        self.assertIn("Earlier discussion", ev["context_before"])
        self.assertIn("host pushes back", ev["context_after"])
        # local filesystem paths must never leak into the public payload
        self.assertNotIn(tmp.name, json.dumps(payload))

    def test_payload_carries_people_network_graph(self) -> None:
        d1, d2 = dt.date(2026, 6, 8), dt.date(2026, 6, 15)
        for eid, day, src in (("net_e1", d1, "show_a"),
                              ("net_e2", d2, "show_b")):
            self._add_position(eid, day, src, "Connector")
            self._add_position(eid, day, src, "Fan One")
        self._add_position("net_e1", d1, "show_a", "Fan Two")
        self._add_position("net_e2", d2, "show_b", "Fan Two")

        payload = collect(self.conn, now=_NOW)
        net = payload["network"]
        names = {n["name"] for n in net["nodes"]}
        self.assertIn("Connector", names)
        for n in net["nodes"]:
            for key in ("name", "score", "tier", "n_links"):
                self.assertIn(key, n)
        edge = next(e for e in net["edges"]
                    if {e["a"], e["b"]} == {"Connector", "Fan One"})
        self.assertEqual(edge["w"], 2)  # two shared episodes
        # undirected: each pair appears once
        pairs = [frozenset((e["a"], e["b"])) for e in net["edges"]]
        self.assertEqual(len(pairs), len(set(pairs)))

    def test_every_detector_hit_carries_p_value_and_tier(self) -> None:
        self._fill_background(per_base_week=40, per_pulse_week=40)
        for i, day in enumerate(_PULSE_MONDAYS[:3]):
            self._add_labeled(day, "brand new thing", 10,
                              source_id=f"show_{'abc'[i]}")
        payload = collect(self.conn, now=_NOW)
        for family, entries in payload["detectors"].items():
            for entry in entries:
                self.assertIn("p_value", entry, f"{family}: {entry}")
                self.assertIn("tier", entry, f"{family}: {entry}")


if __name__ == "__main__":
    unittest.main()
