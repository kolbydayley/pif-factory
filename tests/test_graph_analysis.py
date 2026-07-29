from __future__ import annotations

import sqlite3
import unittest

from research_factory.graph_analysis import (
    accepted_graph_bundle,
    coappearance_graph,
    corpus_contrarian_records,
    cross_network_speaker_graph,
    cross_speaker_disagreements,
    same_speaker_position_changes,
    shared_guest_graph,
)


AS_OF = "2026-07-20T12:00:00Z"


class AcceptedGraphAnalysisTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(
            """
            CREATE TABLE current_accepted_person_appearances (
              id TEXT PRIMARY KEY,
              corpus_release_id TEXT NOT NULL,
              canonical_person_id TEXT NOT NULL,
              source_id TEXT NOT NULL,
              episode_id TEXT NOT NULL,
              role TEXT NOT NULL,
              appeared_at TEXT NOT NULL
            );
            CREATE TABLE current_accepted_source_affiliations (
              id TEXT PRIMARY KEY,
              corpus_release_id TEXT NOT NULL,
              source_id TEXT NOT NULL,
              affiliation_kind TEXT NOT NULL,
              affiliation_key TEXT NOT NULL,
              affiliation_name TEXT NOT NULL,
              valid_from TEXT,
              valid_to TEXT
            );
            CREATE TABLE current_accepted_atomic_claims (
              id TEXT PRIMARY KEY,
              corpus_release_id TEXT NOT NULL,
              source_id TEXT NOT NULL,
              episode_id TEXT NOT NULL
            );
            CREATE TABLE current_accepted_position_observations (
              id TEXT PRIMARY KEY,
              corpus_release_id TEXT NOT NULL,
              atomic_claim_id TEXT NOT NULL,
              canonical_person_id TEXT NOT NULL,
              subject_id TEXT NOT NULL,
              variant_id TEXT NOT NULL,
              position TEXT NOT NULL,
              observed_at TEXT NOT NULL
            );
            CREATE TABLE current_accepted_claim_relations (
              id TEXT PRIMARY KEY,
              corpus_release_id TEXT NOT NULL,
              pipeline_run_id TEXT NOT NULL,
              source_claim_id TEXT NOT NULL,
              target_claim_id TEXT NOT NULL,
              relation TEXT NOT NULL,
              temporal_scope TEXT NOT NULL,
              confidence REAL NOT NULL,
              rationale TEXT NOT NULL,
              judge_model TEXT NOT NULL,
              decided_at TEXT NOT NULL
            );
            CREATE TABLE current_accepted_contrarian_snapshots (
              id TEXT PRIMARY KEY,
              corpus_release_id TEXT NOT NULL,
              pipeline_run_id TEXT NOT NULL,
              target_claim_id TEXT NOT NULL,
              consensus_snapshot_id TEXT,
              calculation_version TEXT NOT NULL,
              as_of TEXT NOT NULL,
              window_start TEXT NOT NULL,
              window_end TEXT NOT NULL,
              window_days INTEGER NOT NULL,
              people_count INTEGER NOT NULL,
              show_count INTEGER NOT NULL,
              network_count INTEGER NOT NULL,
              dominant_bucket TEXT,
              dominant_share REAL,
              target_share REAL,
              classification TEXT NOT NULL,
              is_contrarian INTEGER NOT NULL,
              exclusion_reason TEXT,
              inputs_sha256 TEXT NOT NULL,
              included_claim_ids_json TEXT NOT NULL
            );
            """
        )
        self.conn.executemany(
            """
            INSERT INTO current_accepted_person_appearances
              (id, corpus_release_id, canonical_person_id, source_id,
               episode_id, role, appeared_at)
            VALUES (?, 'release_1', ?, ?, ?, ?, ?)
            """,
            (
                ("app_guest_a", "person_1", "source_a", "episode_a", "guest", "2026-01-05T00:00:00Z"),
                ("app_guest_b", "person_1", "source_b", "episode_b", "guest", "2026-02-05T00:00:00Z"),
                ("app_host", "person_2", "source_a", "episode_pair", "host", "2026-03-05T00:00:00Z"),
                ("app_panelist", "person_3", "source_a", "episode_pair", "panelist", "2026-03-05T00:00:00Z"),
                ("app_future", "person_1", "source_c", "episode_future", "guest", "2027-01-01T00:00:00Z"),
            ),
        )
        self.conn.executemany(
            """
            INSERT INTO current_accepted_source_affiliations
              (id, corpus_release_id, source_id, affiliation_kind,
               affiliation_key, affiliation_name, valid_from, valid_to)
            VALUES (?, 'release_1', ?, ?, ?, ?, ?, ?)
            """,
            (
                ("aff_a", "source_a", "network", "alpha", "Alpha Network", None, None),
                ("aff_b", "source_b", "publisher", "beta", "Beta Media", "2026-01-01", None),
                ("aff_b_expired", "source_b", "network", "expired", "Old Network", None, "2025-12-31"),
                ("aff_c", "source_c", "owner", "gamma", "Gamma Group", None, None),
            ),
        )
        self.conn.executemany(
            """
            INSERT INTO current_accepted_atomic_claims
              (id, corpus_release_id, source_id, episode_id)
            VALUES (?, 'release_1', ?, ?)
            """,
            (
                ("claim_1", "source_a", "episode_a"),
                ("claim_2", "source_b", "episode_b"),
                ("claim_3", "source_a", "episode_later"),
                ("claim_4", "source_a", "episode_same"),
            ),
        )
        self.conn.executemany(
            """
            INSERT INTO current_accepted_position_observations
              (id, corpus_release_id, atomic_claim_id, canonical_person_id,
               subject_id, variant_id, position, observed_at)
            VALUES (?, 'release_1', ?, ?, 'subject_1', ?, ?, ?)
            """,
            (
                ("position_1", "claim_1", "person_1", "variant_a", "supports", "2026-01-05T00:00:00Z"),
                ("position_2", "claim_2", "person_2", "variant_b", "opposes", "2026-02-05T00:00:00Z"),
                ("position_3", "claim_3", "person_1", "variant_b", "opposes", "2026-04-05T00:00:00Z"),
                ("position_4", "claim_4", "person_1", "variant_b", "opposes", "2026-01-05T00:00:00Z"),
            ),
        )
        self.conn.executemany(
            """
            INSERT INTO current_accepted_claim_relations
              (id, corpus_release_id, pipeline_run_id, source_claim_id,
               target_claim_id, relation, temporal_scope, confidence,
               rationale, judge_model, decided_at)
            VALUES (?, 'release_1', 'run_relations', ?, ?, 'contradicts',
                    'during the shared resolution window', 0.95,
                    'Accepted LLM contradiction judgment.', 'gpt-5.5', ?)
            """,
            (
                ("relation_cross", "claim_1", "claim_2", "2026-02-06T00:00:00Z"),
                ("relation_change", "claim_1", "claim_3", "2026-04-06T00:00:00Z"),
                ("relation_self", "claim_1", "claim_4", "2026-01-06T00:00:00Z"),
            ),
        )
        self.conn.execute(
            """
            INSERT INTO current_accepted_contrarian_snapshots
              (id, corpus_release_id, pipeline_run_id, target_claim_id,
               consensus_snapshot_id, calculation_version, as_of,
               window_start, window_end, window_days, people_count,
               show_count, network_count, dominant_bucket, dominant_share,
               target_share, classification, is_contrarian, exclusion_reason,
               inputs_sha256, included_claim_ids_json)
            VALUES ('contrarian_1', 'release_1', 'run_consensus', 'claim_1',
                    'consensus_1', 'contrarian_v1', '2026-01-06T00:00:00Z',
                    '2025-10-08T00:00:00Z', '2026-01-06T00:00:00Z', 90,
                    7, 4, 2, 'aligned', 0.70, 0.14, 'contrarian', 1, NULL,
                    ?, '["claim_2"]')
            """,
            ("a" * 64,),
        )

    def tearDown(self) -> None:
        self.conn.close()

    def test_appearance_graphs_use_only_accepted_temporal_evidence(self) -> None:
        shared = shared_guest_graph(self.conn, as_of=AS_OF)
        self.assertTrue(shared["available"])
        self.assertEqual(shared["row_count"], 1)
        self.assertEqual(shared["rows"][0]["source_a_id"], "source_a")
        self.assertEqual(shared["rows"][0]["source_b_id"], "source_b")
        self.assertEqual(shared["rows"][0]["person_ids"], ["person_1"])
        self.assertEqual(
            shared["rows"][0]["evidence_appearance_ids"],
            ["app_guest_a", "app_guest_b"],
        )
        self.assertEqual(shared["rows"][0]["corpus_release_id"], "release_1")
        self.assertEqual(shared["rows"][0]["as_of"], AS_OF)

        coappearances = coappearance_graph(self.conn, as_of=AS_OF)
        pair = next(
            row
            for row in coappearances["rows"]
            if row["person_a_id"] == "person_2" and row["person_b_id"] == "person_3"
        )
        self.assertEqual(pair["coappearance_count"], 1)
        self.assertEqual(pair["evidence_episode_ids"], ["episode_pair"])
        self.assertEqual(pair["appearance_roles"], ["host:panelist"])

        networks = cross_network_speaker_graph(self.conn, as_of=AS_OF)
        self.assertEqual(networks["row_count"], 1)
        edge = networks["rows"][0]
        self.assertEqual(edge["endpoint_a"]["key"], "network:alpha")
        self.assertEqual(edge["endpoint_b"]["key"], "publisher:beta")
        self.assertEqual(edge["person_ids"], ["person_1"])
        self.assertEqual(edge["evidence_affiliation_ids"], ["aff_a", "aff_b"])
        self.assertNotIn("aff_b_expired", edge["evidence_affiliation_ids"])
        self.assertNotIn("source_c", edge["source_ids"])

    def test_disagreement_and_same_speaker_records_remain_distinct(self) -> None:
        disagreement = cross_speaker_disagreements(self.conn, as_of=AS_OF)
        self.assertTrue(disagreement["available"])
        self.assertEqual(
            [row["relation_id"] for row in disagreement["rows"]],
            ["relation_cross"],
        )
        record = disagreement["rows"][0]
        self.assertEqual(record["record_type"], "cross_speaker_disagreement")
        self.assertEqual(record["source_person_id"], "person_1")
        self.assertEqual(record["target_person_id"], "person_2")
        self.assertEqual(record["pipeline_run_id"], "run_relations")
        self.assertEqual(record["evidence_ids"]["relation_id"], "relation_cross")

        changes = same_speaker_position_changes(self.conn, as_of=AS_OF)
        self.assertEqual(changes["row_count"], 2)
        by_relation = {row["relation_id"]: row for row in changes["rows"]}
        self.assertEqual(by_relation["relation_change"]["record_type"], "position_change")
        self.assertEqual(by_relation["relation_change"]["earlier_claim_id"], "claim_1")
        self.assertEqual(by_relation["relation_change"]["later_claim_id"], "claim_3")
        self.assertEqual(by_relation["relation_self"]["record_type"], "self_contradiction")
        self.assertEqual(by_relation["relation_self"]["canonical_person_id"], "person_1")

    def test_contrarian_records_name_corpus_consensus_and_preserve_lineage(self) -> None:
        result = corpus_contrarian_records(self.conn, as_of=AS_OF)
        self.assertTrue(result["available"])
        self.assertEqual(result["row_count"], 1)
        record = result["rows"][0]
        self.assertTrue(record["is_contrarian"])
        self.assertTrue(record["consensus_is_not_truth"])
        self.assertEqual(record["classification"], "contrarian")
        self.assertEqual(record["canonical_person_id"], "person_1")
        self.assertEqual(record["corpus_release_id"], "release_1")
        self.assertEqual(record["pipeline_run_id"], "run_consensus")
        self.assertEqual(
            record["evidence_ids"],
            {
                "contrarian_snapshot_id": "contrarian_1",
                "consensus_snapshot_id": "consensus_1",
                "target_claim_id": "claim_1",
                "position_id": "position_1",
                "preceding_consensus_claim_ids": ["claim_2"],
            },
        )

    def test_bundle_reports_all_projections_without_legacy_fallback(self) -> None:
        bundle = accepted_graph_bundle(self.conn, as_of=AS_OF, limit=25)
        self.assertTrue(bundle["available"])
        self.assertEqual(bundle["authority"], "current_accepted_only")
        self.assertEqual(
            set(bundle["projections"]),
            {
                "shared_guest_graph",
                "coappearance_graph",
                "cross_network_speaker_graph",
                "cross_speaker_disagreements",
                "same_speaker_position_changes",
                "corpus_contrarian_records",
            },
        )

    def test_missing_or_malformed_current_surfaces_fail_closed(self) -> None:
        legacy = sqlite3.connect(":memory:")
        try:
            legacy.executescript(
                """
                CREATE TABLE person_appearances (
                  id TEXT, canonical_person_id TEXT, source_id TEXT,
                  episode_id TEXT, role TEXT, appeared_at TEXT
                );
                INSERT INTO person_appearances VALUES
                  ('legacy_1', 'legacy_person', 'legacy_a', 'legacy_episode',
                   'guest', '2026-01-01T00:00:00Z');
                """
            )
            result = shared_guest_graph(legacy, as_of=AS_OF)
            self.assertFalse(result["available"])
            self.assertEqual(result["rows"], [])
            self.assertEqual(
                result["missing_surfaces"],
                ["current_accepted_person_appearances"],
            )
        finally:
            legacy.close()

        malformed = sqlite3.connect(":memory:")
        try:
            malformed.execute(
                "CREATE TABLE current_accepted_person_appearances (id TEXT PRIMARY KEY)"
            )
            result = shared_guest_graph(malformed, as_of=AS_OF)
            self.assertFalse(result["available"])
            self.assertEqual(result["rows"], [])
            self.assertEqual(result["missing_surfaces"], [])
            self.assertIn("contract", result["unavailable_reason"])
        finally:
            malformed.close()

    def test_as_of_requires_a_timezone(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone"):
            shared_guest_graph(self.conn, as_of="2026-07-20T12:00:00")


if __name__ == "__main__":
    unittest.main()
