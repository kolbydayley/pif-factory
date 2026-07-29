from __future__ import annotations

import sqlite3
import unittest

from research_factory.accuracy import (
    AccuracyDataError,
    AccuracyUnavailableError,
    person_accuracy_history,
    person_accuracy_rankings,
    person_contrarian_success,
)


RELEASE_ID = "release-1"
FINAL_AS_OF = "2026-04-01T00:00:00+00:00"


class AcceptedAccuracyMetricsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self._create_surfaces()
        self._load_fixture()

    def tearDown(self) -> None:
        self.conn.close()

    def _create_surfaces(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE fixture_releases (
              id TEXT PRIMARY KEY,
              release_version INTEGER NOT NULL,
              manifest_sha256 TEXT NOT NULL,
              promoted_at TEXT NOT NULL,
              accepted INTEGER NOT NULL
            );
            CREATE VIEW current_accepted_corpus_releases AS
            SELECT id, release_version, manifest_sha256, promoted_at
            FROM fixture_releases WHERE accepted = 1;

            CREATE TABLE fixture_people (
              id TEXT PRIMARY KEY,
              display_name TEXT NOT NULL,
              accepted INTEGER NOT NULL
            );
            CREATE VIEW current_accepted_people AS
            SELECT id, display_name FROM fixture_people WHERE accepted = 1;

            CREATE TABLE fixture_claims (
              id TEXT PRIMARY KEY,
              corpus_release_id TEXT NOT NULL,
              canonical_person_id TEXT,
              forecast_probability REAL,
              observed_at TEXT NOT NULL,
              created_at TEXT NOT NULL,
              accepted INTEGER NOT NULL
            );
            CREATE VIEW current_accepted_atomic_claims AS
            SELECT id, corpus_release_id, canonical_person_id,
                   forecast_probability, observed_at, created_at
            FROM fixture_claims WHERE accepted = 1;

            CREATE TABLE fixture_positions (
              id TEXT PRIMARY KEY,
              corpus_release_id TEXT NOT NULL,
              atomic_claim_id TEXT NOT NULL,
              canonical_person_id TEXT NOT NULL,
              accepted INTEGER NOT NULL
            );
            CREATE VIEW current_accepted_position_observations AS
            SELECT id, corpus_release_id, atomic_claim_id, canonical_person_id
            FROM fixture_positions WHERE accepted = 1;

            CREATE TABLE fixture_outcomes (
              id TEXT PRIMARY KEY,
              claim_id TEXT NOT NULL,
              corpus_release_id TEXT NOT NULL,
              outcome TEXT NOT NULL,
              categorical_score REAL,
              brier_score REAL,
              resolved_at TEXT NOT NULL,
              as_of TEXT NOT NULL,
              created_at TEXT NOT NULL,
              accepted INTEGER NOT NULL
            );
            CREATE VIEW current_accepted_outcome_resolutions AS
            SELECT id, claim_id, corpus_release_id, outcome, categorical_score,
                   brier_score, resolved_at, as_of, created_at
            FROM fixture_outcomes WHERE accepted = 1;

            CREATE TABLE fixture_contrarian (
              id TEXT PRIMARY KEY,
              target_claim_id TEXT NOT NULL,
              corpus_release_id TEXT NOT NULL,
              classification TEXT NOT NULL,
              as_of TEXT NOT NULL,
              created_at TEXT NOT NULL,
              accepted INTEGER NOT NULL
            );
            CREATE VIEW current_accepted_contrarian_snapshots AS
            SELECT id, target_claim_id, corpus_release_id, classification,
                   as_of, created_at
            FROM fixture_contrarian WHERE accepted = 1;

            -- A deliberately tempting legacy surface. Accuracy must ignore it.
            CREATE TABLE authority_scores (
              canonical_person_id TEXT,
              score REAL,
              status TEXT
            );
            """
        )

    def _load_fixture(self) -> None:
        self.conn.execute(
            "INSERT INTO fixture_releases VALUES (?, 1, ?, ?, 1)",
            (RELEASE_ID, "a" * 64, "2026-01-01T00:00:00+00:00"),
        )
        self.conn.executemany(
            "INSERT INTO fixture_people VALUES (?, ?, ?)",
            [
                ("alice", "Alice", 1),
                ("bob", "Bob", 1),
                ("legacy", "Legacy Candidate", 0),
            ],
        )
        self.conn.execute(
            "INSERT INTO authority_scores VALUES ('legacy', 1.0, 'accepted')"
        )

        probabilities = {"a0": 0.8, "a1": 0.2, "a2": 0.7}
        for index in range(13):
            claim_id = f"a{index}"
            self._claim(
                claim_id,
                "alice",
                f"2026-01-{index + 1:02d}T00:00:00+00:00",
                probabilities.get(claim_id),
            )
        for index in range(9):
            self._claim(
                f"b{index}",
                "bob",
                f"2026-01-{index + 1:02d}T12:00:00+00:00",
                None,
            )
        self._claim(
            "legacy-claim",
            "alice",
            "2026-01-01T00:00:00+00:00",
            None,
            accepted=0,
        )

        alice_outcomes = [
            ("a0", "true", 1.0, 0.04),
            ("a1", "false", 0.0, 0.04),
            ("a2", "mixed", 0.5, None),
            ("a3", "true", 1.0, None),
            ("a4", "true", 1.0, None),
            ("a5", "true", 1.0, None),
            ("a6", "true", 1.0, None),
            ("a7", "false", 0.0, None),
            ("a8", "false", 0.0, None),
            ("a9", "true", 1.0, None),
            ("a10", "unresolved", None, None),
            ("a11", "unverifiable", None, None),
        ]
        for index, (claim_id, outcome, categorical, brier) in enumerate(
            alice_outcomes, start=1
        ):
            day = index
            self._insert_outcome(
                claim_id,
                outcome,
                categorical,
                brier,
                f"2026-02-{day:02d}T00:00:00+00:00",
            )
        # Retrospective evidence cannot leak before the accepted row existed.
        self._insert_outcome(
            "a12",
            "true",
            1.0,
            None,
            "2026-01-15T00:00:00+00:00",
            created_at="2026-03-01T00:00:00+00:00",
        )
        for index in range(9):
            self._insert_outcome(
                f"b{index}",
                "true",
                1.0,
                None,
                f"2026-02-{index + 1:02d}T12:00:00+00:00",
            )
        self._insert_outcome(
            "legacy-claim",
            "true",
            1.0,
            None,
            "2026-02-01T00:00:00+00:00",
            accepted=0,
        )

        self._snapshot("a0", "contrarian", "2026-01-20T00:00:00+00:00")
        self._snapshot("a1", "not_contrarian", "2026-01-20T00:00:00+00:00")
        self._snapshot(
            "a2", "insufficient_coverage", "2026-01-20T00:00:00+00:00"
        )
        # This accepted contrarian judgment came after its outcome and is pending
        # a later outcome, so it cannot count as a success or failure.
        self._snapshot("a3", "contrarian", "2026-03-05T00:00:00+00:00")
        self._snapshot("a4", "contrarian", "2026-01-25T00:00:00+00:00")
        self._snapshot("a7", "contrarian", "2026-01-25T00:00:00+00:00")
        self._snapshot("a10", "contrarian", "2026-01-25T00:00:00+00:00")
        self._snapshot(
            "a8",
            "contrarian",
            "2026-01-25T00:00:00+00:00",
            accepted=0,
        )
        self.conn.commit()

    def _claim(
        self,
        claim_id: str,
        person_id: str,
        observed_at: str,
        probability: float | None,
        *,
        accepted: int = 1,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO fixture_claims
              (id, corpus_release_id, canonical_person_id, forecast_probability,
               observed_at, created_at, accepted)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                claim_id,
                RELEASE_ID,
                person_id,
                probability,
                observed_at,
                observed_at,
                accepted,
            ),
        )
        self.conn.execute(
            "INSERT INTO fixture_positions VALUES (?, ?, ?, ?, ?)",
            (f"position-{claim_id}", RELEASE_ID, claim_id, person_id, accepted),
        )

    def _insert_outcome(
        self,
        claim_id: str,
        outcome: str,
        categorical: float | None,
        brier: float | None,
        as_of: str,
        *,
        created_at: str | None = None,
        accepted: int = 1,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO fixture_outcomes
              (id, claim_id, corpus_release_id, outcome, categorical_score,
               brier_score, resolved_at, as_of, created_at, accepted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"outcome-{claim_id}",
                claim_id,
                RELEASE_ID,
                outcome,
                categorical,
                brier,
                as_of,
                as_of,
                created_at or as_of,
                accepted,
            ),
        )

    def _snapshot(
        self,
        claim_id: str,
        classification: str,
        as_of: str,
        *,
        accepted: int = 1,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO fixture_contrarian
              (id, target_claim_id, corpus_release_id, classification, as_of,
               created_at, accepted)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"contra-{claim_id}-{classification}",
                claim_id,
                RELEASE_ID,
                classification,
                as_of,
                as_of,
                accepted,
            ),
        )

    def test_history_separates_scores_and_exposes_all_coverage(self) -> None:
        result = person_accuracy_history(self.conn, "alice", as_of=FINAL_AS_OF)

        self.assertEqual(result["release"]["release_id"], RELEASE_ID)
        self.assertEqual(result["accepted_claim_count"], 13)
        self.assertEqual(result["accepted_resolution_count"], 13)
        self.assertEqual(result["no_resolution_count"], 0)
        self.assertEqual(result["categorical"]["numerator"], 7.5)
        self.assertEqual(result["categorical"]["denominator"], 11)
        self.assertAlmostEqual(result["categorical"]["accuracy"], 7.5 / 11)
        self.assertAlmostEqual(result["brier"]["numerator"], 0.08)
        self.assertEqual(result["brier"]["denominator"], 2)
        self.assertAlmostEqual(result["brier"]["mean"], 0.04)
        self.assertEqual(result["coverage"]["unresolved_count"], 1)
        self.assertEqual(result["coverage"]["unverifiable_count"], 1)
        self.assertEqual(result["coverage"]["unresolved_coverage_numerator"], 2)
        self.assertEqual(result["coverage"]["unresolved_coverage_denominator"], 13)
        self.assertTrue(result["ranking_eligibility"]["eligible"])
        self.assertEqual(result["history"][-1]["running_categorical_numerator"], 7.5)
        self.assertTrue(all(row["corpus_release_id"] == RELEASE_ID for row in result["history"]))

    def test_as_of_is_a_conservative_knowledge_cutoff_and_period_is_claim_time(self) -> None:
        early = person_accuracy_history(
            self.conn,
            "alice",
            as_of="2026-02-05T23:59:59+00:00",
        )
        self.assertEqual(early["accepted_resolution_count"], 5)
        self.assertEqual(early["categorical"]["denominator"], 5)
        self.assertNotIn("a12", {row["claim_id"] for row in early["history"]})

        period = person_accuracy_history(
            self.conn,
            "alice",
            as_of=FINAL_AS_OF,
            observed_from="2026-01-01T00:00:00+00:00",
            observed_through="2026-01-10T23:59:59+00:00",
        )
        self.assertEqual(period["accepted_claim_count"], 10)
        self.assertEqual(period["categorical"]["denominator"], 10)
        self.assertNotIn("a12", {row["claim_id"] for row in period["history"]})

    def test_rankings_exclude_people_below_ten_resolved_claims(self) -> None:
        result = person_accuracy_rankings(self.conn, as_of=FINAL_AS_OF)

        self.assertEqual(result["accepted_person_count"], 2)
        self.assertEqual(result["eligible_person_count"], 1)
        self.assertEqual(result["excluded_below_minimum_count"], 1)
        self.assertEqual([row["person_id"] for row in result["rankings"]], ["alice"])
        ranking = result["rankings"][0]
        self.assertEqual(ranking["categorical_numerator"], 7.5)
        self.assertEqual(ranking["categorical_denominator"], 11)
        self.assertEqual(ranking["unresolved_count"], 1)
        self.assertEqual(ranking["unverifiable_count"], 1)
        self.assertEqual(ranking["corpus_release_id"], RELEASE_ID)
        self.assertEqual(ranking["as_of"], FINAL_AS_OF)

    def test_contrarian_success_requires_accepted_prior_classification_and_later_outcome(self) -> None:
        result = person_contrarian_success(self.conn, "alice", as_of=FINAL_AS_OF)

        self.assertEqual(result["observation_count"], 5)
        self.assertEqual(result["later_accepted_outcome_count"], 4)
        self.assertEqual(result["pending_later_outcome_count"], 1)
        self.assertEqual(result["numerator"], 2.0)
        self.assertEqual(result["denominator"], 3)
        self.assertAlmostEqual(result["rate"], 2 / 3)
        self.assertEqual(result["unresolved_count"], 1)
        self.assertEqual(
            {case["claim_id"] for case in result["cases"]},
            {"a0", "a4", "a7", "a10"},
        )

    def test_missing_or_ambiguous_current_authority_fails_closed(self) -> None:
        self.conn.execute("DROP VIEW current_accepted_contrarian_snapshots")
        with self.assertRaisesRegex(AccuracyUnavailableError, "contrarian"):
            person_accuracy_history(self.conn, "alice", as_of=FINAL_AS_OF)

        self.conn.close()
        self.setUp()
        self.conn.execute(
            "INSERT INTO fixture_releases VALUES (?, 2, ?, ?, 1)",
            ("release-2", "b" * 64, "2026-03-01T00:00:00+00:00"),
        )
        with self.assertRaisesRegex(AccuracyUnavailableError, "exactly one"):
            person_accuracy_rankings(self.conn, as_of=FINAL_AS_OF)

    def test_inconsistent_accepted_score_fails_closed(self) -> None:
        self.conn.execute(
            "UPDATE fixture_outcomes SET categorical_score = 0 WHERE id = 'outcome-a0'"
        )
        with self.assertRaisesRegex(AccuracyDataError, "categorical_score"):
            person_accuracy_history(self.conn, "alice", as_of=FINAL_AS_OF)


if __name__ == "__main__":
    unittest.main()
