from __future__ import annotations

import json
import sqlite3
import unittest
from collections import Counter

from research_factory.db import TOPIC_ISSUE_REGISTRY_V6
from research_factory.topic_canonicalizer import (
    build_precision_first_canon,
    canonicalize_topics,
    classify_topic_pair,
    load_accepted_topic_registry,
    normalize_topic_surface,
)


class TopicCanonicalizerTest(unittest.TestCase):
    def test_normalization_repairs_possessives_without_erasing_scope(self) -> None:
        self.assertEqual(normalize_topic_surface("AI’s impact/on_jobs"), "ai impact on jobs")

    def test_only_identity_level_variants_auto_merge(self) -> None:
        plural = classify_topic_pair("AI models", "AI model")
        self.assertEqual(plural.relation, "same_issue")
        self.assertTrue(plural.auto_merge)

        scoped = classify_topic_pair("enterprise AI adoption", "enterprise AI")
        self.assertEqual(scoped.relation, "narrower_than")
        self.assertFalse(scoped.auto_merge)

        one_token = classify_topic_pair("agents", "AI coding agents")
        self.assertEqual(one_token.relation, "broader_than")
        self.assertFalse(one_token.auto_merge)

    def test_precision_first_map_is_deterministic_and_preserves_scoped_issues(self) -> None:
        counts = Counter(
            {
                "agents": 100,
                "agent": 40,
                "ai coding agents": 30,
                "enterprise ai": 20,
                "enterprise ai adoption": 10,
            }
        )
        first = build_precision_first_canon(counts)
        second = build_precision_first_canon(counts)
        self.assertEqual(first, second)
        self.assertEqual(first["agent"], "agents")
        self.assertEqual(first["ai coding agents"], "ai coding agents")
        self.assertEqual(first["enterprise ai adoption"], "enterprise ai adoption")

    def test_registry_persists_stable_assignments_and_candidate_hierarchy(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE segments (id TEXT PRIMARY KEY, episode_id TEXT, source_id TEXT);
            CREATE TABLE labels (segment_id TEXT, output_json TEXT);
            CREATE TABLE actor_positions (segment_id TEXT, concept_name TEXT);
            INSERT INTO segments VALUES ('s1', 'e1', 'show1');
            INSERT INTO segments VALUES ('s2', 'e2', 'show2');
            """
        )
        for statement in TOPIC_ISSUE_REGISTRY_V6:
            conn.execute(statement)
        conn.executemany(
            "INSERT INTO labels (segment_id, output_json) VALUES (?, ?)",
            (
                (
                    "s1",
                    json.dumps(
                        {
                            "topics": [
                                {"topic": "enterprise AI"},
                                {"topic": "enterprise AI adoption"},
                                {"topic": "AI models"},
                            ]
                        }
                    ),
                ),
                (
                    "s2",
                    json.dumps({"topics": [{"topic": "AI model"}]}),
                ),
            ),
        )

        result = canonicalize_topics(conn, limit=20)
        registry = load_accepted_topic_registry(conn)

        self.assertTrue(result["ok"])
        self.assertEqual(registry["ai model"]["issue_id"], registry["ai models"]["issue_id"])
        stable_issue_id = registry["ai model"]["issue_id"]
        self.assertNotEqual(
            registry["enterprise ai"]["issue_id"],
            registry["enterprise ai adoption"]["issue_id"],
        )
        relation = conn.execute(
            """
            SELECT relation_type, status
            FROM canonical_issue_relations
            WHERE relation_type = 'narrower_than'
            """
        ).fetchone()
        self.assertIsNotNone(relation)
        self.assertEqual(relation["status"], "candidate")

        # A later frequency reversal cannot rename or re-key an accepted issue.
        conn.execute(
            "UPDATE labels SET output_json = ? WHERE segment_id = 's1'",
            (json.dumps({"topics": [{"topic": "AI models"}] * 20}),),
        )
        canonicalize_topics(conn, limit=20)
        refreshed = load_accepted_topic_registry(conn)
        self.assertEqual(refreshed["ai model"]["issue_id"], stable_issue_id)
        self.assertEqual(refreshed["ai models"]["issue_id"], stable_issue_id)
        conn.close()


if __name__ == "__main__":
    unittest.main()
