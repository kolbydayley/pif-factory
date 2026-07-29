from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from research_factory.intelligence import (
    IntelligenceValidationError,
    compute_consensus_snapshot,
    compute_contrarian_snapshot,
    record_claim_relation_judgment,
    store_contrarian_snapshot,
)


TS = "2026-07-20T12:00:00+00:00"


class RelationTemporalScopeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE corpus_releases (
              id TEXT PRIMARY KEY,
              status TEXT NOT NULL
            );
            CREATE TABLE pipeline_runs (
              id TEXT PRIMARY KEY,
              corpus_release_id TEXT NOT NULL,
              status TEXT NOT NULL
            );
            CREATE TABLE atomic_claims (
              id TEXT PRIMARY KEY,
              corpus_release_id TEXT NOT NULL,
              pipeline_run_id TEXT NOT NULL,
              review_status TEXT NOT NULL
            );
            CREATE TABLE claim_relation_judgments (
              id TEXT PRIMARY KEY,
              schema_version TEXT NOT NULL,
              relation_lineage_id TEXT NOT NULL,
              revision INTEGER NOT NULL,
              supersedes_judgment_id TEXT,
              corpus_release_id TEXT NOT NULL,
              source_claim_id TEXT NOT NULL,
              target_claim_id TEXT NOT NULL,
              relation TEXT NOT NULL,
              temporal_scope TEXT NOT NULL CHECK(length(trim(temporal_scope)) > 0),
              confidence REAL NOT NULL,
              rationale TEXT NOT NULL,
              evidence_json TEXT NOT NULL,
              judge_model TEXT NOT NULL,
              pipeline_run_id TEXT NOT NULL,
              review_status TEXT NOT NULL,
              decided_at TEXT NOT NULL,
              created_at TEXT NOT NULL,
              UNIQUE(relation_lineage_id, revision)
            );
            CREATE TABLE current_accepted_atomic_claims (
              id TEXT PRIMARY KEY,
              claim_lineage_id TEXT,
              raw_speaker TEXT,
              source_id TEXT,
              episode_id TEXT,
              observed_at TEXT,
              created_at TEXT,
              corpus_release_id TEXT
            );
            CREATE TABLE current_accepted_position_observations (
              id TEXT PRIMARY KEY,
              atomic_claim_id TEXT NOT NULL,
              canonical_person_id TEXT NOT NULL,
              variant_id TEXT NOT NULL,
              observed_at TEXT NOT NULL,
              decided_at TEXT NOT NULL
            );
            CREATE TABLE current_accepted_people (id TEXT PRIMARY KEY);
            CREATE TABLE current_accepted_source_affiliations (
              source_id TEXT NOT NULL,
              affiliation_kind TEXT NOT NULL,
              affiliation_key TEXT NOT NULL,
              valid_from TEXT,
              valid_to TEXT
            );
            CREATE TABLE current_accepted_claim_relations (
              id TEXT PRIMARY KEY,
              source_claim_id TEXT NOT NULL,
              target_claim_id TEXT NOT NULL,
              relation TEXT NOT NULL,
              temporal_scope TEXT NOT NULL,
              confidence REAL NOT NULL,
              decided_at TEXT NOT NULL
            );
            CREATE TABLE contrarian_snapshots (
              id TEXT PRIMARY KEY,
              schema_version TEXT NOT NULL,
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
              dominant_threshold REAL NOT NULL,
              target_share_threshold REAL NOT NULL,
              minimum_people INTEGER NOT NULL,
              minimum_shows INTEGER NOT NULL,
              minimum_networks INTEGER NOT NULL,
              classification TEXT NOT NULL CHECK(
                classification IN ('contrarian', 'not_contrarian', 'insufficient_coverage')
              ),
              is_contrarian INTEGER NOT NULL,
              exclusion_reason TEXT,
              inputs_sha256 TEXT NOT NULL,
              included_claim_ids_json TEXT NOT NULL,
              metrics_json TEXT NOT NULL,
              review_status TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            """
        )
        self.conn.execute(
            "INSERT INTO corpus_releases (id, status) VALUES ('release_1', 'accepted')"
        )
        self.conn.execute(
            """
            INSERT INTO pipeline_runs (id, corpus_release_id, status)
            VALUES ('run_1', 'release_1', 'running')
            """
        )
        self.conn.executemany(
            """
            INSERT INTO atomic_claims
              (id, corpus_release_id, pipeline_run_id, review_status)
            VALUES (?, 'release_1', 'run_1', 'accepted')
            """,
            (("claim_source",), ("claim_target",)),
        )

    def tearDown(self) -> None:
        self.conn.close()

    def test_relation_requires_and_preserves_llm_temporal_scope_verbatim(self) -> None:
        common = {
            "source_claim_id": "claim_source",
            "target_claim_id": "claim_target",
            "relation": "qualifies",
            "confidence": 0.91,
            "rationale": "The source narrows the target during the stated forecast window.",
            "evidence": {"source": "claim_source", "target": "claim_target"},
            "judge_model": "gpt-5.5",
            "corpus_release_id": "release_1",
            "pipeline_run_id": "run_1",
            "decided_at": TS,
        }
        with self.assertRaisesRegex(IntelligenceValidationError, "temporal_scope"):
            record_claim_relation_judgment(self.conn, **common)

        expected = "applies only to forecasts resolving during calendar 2027"
        stored = record_claim_relation_judgment(
            self.conn,
            temporal_scope=expected,
            **common,
        )
        self.assertEqual(stored["temporal_scope"], expected)
        self.assertEqual(
            self.conn.execute(
                "SELECT temporal_scope FROM claim_relation_judgments WHERE id = ?",
                (stored["id"],),
            ).fetchone()["temporal_scope"],
            expected,
        )

    def test_consensus_counts_all_approved_affiliation_kinds_and_hashes_scope(self) -> None:
        claims = (
            ("claim_target", "person_target", "source_target", "episode_target"),
            ("claim_source", "person_source", "source_1", "episode_source"),
        )
        for claim_id, person_id, source_id, episode_id in claims:
            self.conn.execute(
                """
                INSERT INTO current_accepted_atomic_claims
                  (id, claim_lineage_id, raw_speaker, source_id, episode_id,
                   observed_at, created_at, corpus_release_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'release_1')
                """,
                (
                    claim_id,
                    f"lineage_{claim_id}",
                    person_id,
                    source_id,
                    episode_id,
                    TS,
                    TS,
                ),
            )
            self.conn.execute(
                "INSERT INTO current_accepted_people (id) VALUES (?)",
                (person_id,),
            )
            self.conn.execute(
                """
                INSERT INTO current_accepted_position_observations
                  (id, atomic_claim_id, canonical_person_id, variant_id, observed_at, decided_at)
                VALUES (?, ?, ?, 'variant_1', ?, ?)
                """,
                (f"position_{claim_id}", claim_id, person_id, TS, TS),
            )
        for kind, key in (
            ("network", "network-a"),
            ("publisher", "publisher-a"),
            ("owner", "owner-a"),
            ("independent", "independent-a"),
        ):
            self.conn.execute(
                """
                INSERT INTO current_accepted_source_affiliations
                  (source_id, affiliation_kind, affiliation_key, valid_from, valid_to)
                VALUES ('source_1', ?, ?, NULL, NULL)
                """,
                (kind, key),
            )
        self.conn.execute(
            """
            INSERT INTO current_accepted_claim_relations
              (id, source_claim_id, target_claim_id, relation, temporal_scope,
               confidence, decided_at)
            VALUES ('relation_1', 'claim_source', 'claim_target', 'supports', ?, 0.9, ?)
            """,
            ("during the shared forecast window", TS),
        )

        snapshot = compute_consensus_snapshot(
            self.conn,
            focal_claim_id="claim_target",
            as_of=TS,
            exclude_claim_id="claim_target",
        )
        self.assertEqual(snapshot["network_count"], 4)
        self.assertEqual(
            snapshot["positions"][0]["network_keys"],
            [
                "independent:independent-a",
                "network:network-a",
                "owner:owner-a",
                "publisher:publisher-a",
            ],
        )
        initial_hash = snapshot["inputs_sha256"]
        self.conn.execute(
            """
            UPDATE current_accepted_claim_relations
            SET temporal_scope = 'only after the target observation'
            WHERE id = 'relation_1'
            """
        )
        changed = compute_consensus_snapshot(
            self.conn,
            focal_claim_id="claim_target",
            as_of=TS,
            exclude_claim_id="claim_target",
        )
        self.assertNotEqual(initial_hash, changed["inputs_sha256"])

    def test_contrarian_classification_distinguishes_low_coverage(self) -> None:
        base = {
            "inputs_sha256": "a" * 64,
            "as_of": TS,
            "window_start": "2026-04-21T12:00:00+00:00",
            "window_end": TS,
            "window_days": 90,
            "people_count": 4,
            "show_count": 3,
            "network_count": 2,
            "aligned_count": 0,
            "qualified_count": 0,
            "opposed_count": 4,
            "dominant_bucket": "opposed",
            "dominant_share": 1.0,
            "included_claim_ids": ["claim_source"],
        }
        with patch(
            "research_factory.intelligence.compute_consensus_snapshot",
            return_value=base,
        ):
            low_coverage = compute_contrarian_snapshot(
                self.conn, target_claim_id="claim_target"
            )
        self.assertEqual(low_coverage["classification"], "insufficient_coverage")
        self.assertFalse(low_coverage["is_contrarian"])

        accepted_coverage = {**base, "people_count": 5}
        with patch(
            "research_factory.intelligence.compute_consensus_snapshot",
            return_value=accepted_coverage,
        ):
            contrarian = compute_contrarian_snapshot(
                self.conn, target_claim_id="claim_target"
            )
        self.assertEqual(contrarian["classification"], "contrarian")
        self.assertTrue(contrarian["is_contrarian"])

        self.conn.execute(
            """
            INSERT INTO current_accepted_atomic_claims
              (id, corpus_release_id)
            VALUES ('claim_target', 'release_1')
            """
        )
        stored = store_contrarian_snapshot(
            self.conn,
            contrarian,
            corpus_release_id="release_1",
            pipeline_run_id="run_1",
            created_at=TS,
        )
        self.assertEqual(stored["classification"], "contrarian")
        with self.assertRaisesRegex(IntelligenceValidationError, "must agree"):
            store_contrarian_snapshot(
                self.conn,
                {**contrarian, "classification": "not_contrarian"},
                corpus_release_id="release_1",
                pipeline_run_id="run_1",
                created_at=TS,
            )


if __name__ == "__main__":
    unittest.main()
