from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from research_factory import db
from research_factory.intelligence import (
    IntelligenceValidationError,
    accept_pipeline_run,
    build_atomic_claims_from_release,
    create_corpus_release,
    create_pipeline_run,
    promote_corpus_release,
    record_claim_subject,
    reject_pipeline_run,
    supersede_pipeline_run,
    transition_pipeline_run,
)


TS = "2026-07-20T12:00:00+00:00"


class AcceptedRunAuthorityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.conn = db.connect(self.root / "factory.sqlite")
        db.init_db(self.conn)
        self.release = self._seed_release()
        self.release_run = self._completed_run(
            "release_run",
            run_type="release_build",
            run_schema="corpus_release_build",
            run_schema_version="corpus_release_build_v1",
        )
        promote_corpus_release(
            self.conn,
            self.release["id"],
            pipeline_run_id=self.release_run["id"],
            promoted_by="fixture-release-reviewer",
            rationale="The immutable fixture release passed verification.",
            created_at=TS,
        )

    def tearDown(self) -> None:
        self.conn.close()
        self.temp.cleanup()

    def _seed_release(self) -> dict:
        evidence = "A precisely evidenced forecast."
        transcript_path = self.root / "transcript.txt"
        segment_path = self.root / "segment.txt"
        transcript_path.write_text(evidence, encoding="utf-8")
        segment_path.write_text(evidence, encoding="utf-8")
        self.conn.execute(
            "INSERT INTO sources (id, name, created_at, updated_at) VALUES ('source', 'Show', ?, ?)",
            (TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO episodes
              (id, source_id, guid, title, published_at, created_at, updated_at)
            VALUES ('episode', 'source', 'guid', 'Episode', ?, ?, ?)
            """,
            (TS, TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO transcripts
              (id, episode_id, source_kind, raw_text_path, raw_text_sha256,
               status, word_count, created_at, updated_at)
            VALUES ('transcript', 'episode', 'official', ?, 'raw-sha', 'ready', 4, ?, ?)
            """,
            (str(transcript_path), TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO segments
              (id, transcript_id, episode_id, source_id, segment_index, start_char,
               end_char, text_path, text_sha256, word_count, created_at)
            VALUES ('segment', 'transcript', 'episode', 'source', 0, 0, ?, ?, 'segment-sha', 4, ?)
            """,
            (len(evidence), str(segment_path), TS),
        )
        self.conn.execute(
            """
            INSERT INTO episode_context_runs
              (id, episode_id, transcript_id, label_pack, model, status,
               created_at, updated_at, completed_at)
            VALUES ('context', 'episode', 'transcript', 'ai_discourse_v3_1',
                    'gpt-5.5', 'completed', ?, ?, ?)
            """,
            (TS, TS, TS),
        )
        output = {
            "schema_version": "ai_discourse_v3_1",
            "events": [
                {
                    "event_type": "prediction",
                    "claim_text": "A precisely evidenced forecast.",
                    "evidence_text": evidence,
                    "evidence_start": 0,
                    "evidence_end": len(evidence),
                }
            ],
        }
        self.conn.execute(
            """
            INSERT INTO labels
              (id, segment_id, label_pack, label_pack_version, model, status,
               output_json, confidence, needs_review, created_at)
            VALUES ('label', 'segment', 'ai_discourse_v3_1', '3.1', 'gpt-5.5',
                    'ready', ?, 0.99, 0, ?)
            """,
            (json.dumps(output, sort_keys=True), TS),
        )
        self.conn.execute(
            """
            INSERT INTO label_runs
              (id, segment_id, label_pack, model, status, created_at, updated_at, completed_at)
            VALUES ('label-run', 'segment', 'ai_discourse_v3_1', 'gpt-5.5',
                    'completed', ?, ?, ?)
            """,
            (TS, TS, TS),
        )
        self.conn.execute(
            """
            INSERT INTO discourse_events
              (id, label_id, segment_id, event_index, event_type, actor_name,
               stance, claim_text, claim_type, certainty, temporal_horizon,
               confidence, evidence_text, evidence_start, evidence_end, created_at)
            VALUES ('event', 'label', 'segment', 0, 'forecast', 'Speaker',
                    'affirming', 'A precisely evidenced forecast.', 'prediction',
                    'high', 'near_term', 0.99, ?, 0, ?, ?)
            """,
            (evidence, len(evidence), TS),
        )
        self.conn.commit()
        return create_corpus_release(
            self.conn,
            pilot_id="authority-fixture-v1",
            episode_ids=["episode"],
            transcript_ids=["transcript"],
            segment_ids=["segment"],
            accepted_label_ids=["label"],
            manifest_metadata={"audited_pilot_episode_ids": ["episode"]},
            cutoff_at=TS,
            created_at=TS,
        )

    def _completed_run(
        self,
        run_id: str,
        *,
        run_type: str,
        run_schema: str,
        run_schema_version: str = "1",
        prompt_version: str | None = "fixture-v1",
    ) -> dict:
        create_pipeline_run(
            self.conn,
            run_id=run_id,
            run_type=run_type,
            run_schema=run_schema,
            run_schema_version=run_schema_version,
            corpus_release_id=self.release["id"],
            model="gpt-5.5",
            model_version="gpt-5.5",
            prompt_version=prompt_version,
            status="running",
            created_at=TS,
        )
        return transition_pipeline_run(
            self.conn,
            run_id,
            status="succeeded",
            expected_status="running",
            at=TS,
        )

    def _claims_run(self, run_id: str) -> dict:
        return self._completed_run(
            run_id,
            run_type="semantic_reconcile_claims",
            run_schema="pif_semantic_reconciliation_output_v2",
        )

    def _subject(self, run_id: str, *, revision: int = 1, supersedes: str | None = None) -> dict:
        return record_claim_subject(
            self.conn,
            id=f"subject-{revision}",
            subject_lineage_id="subject-lineage",
            revision=revision,
            supersedes_subject_id=supersedes,
            subject_text=f"Audited subject revision {revision}",
            subject_type="forecast",
            judge_model="gpt-5.5",
            judge_schema_version="pif_semantic_judge_v1",
            confidence=0.99,
            rationale="Explicit fixture judgment.",
            evidence={"item_ids": ["event"]},
            review_status="accepted",
            corpus_release_id=self.release["id"],
            pipeline_run_id=run_id,
            decided_at=TS,
        )

    def test_success_is_not_authority_and_latest_decision_controls_visibility(self) -> None:
        run = self._claims_run("claims-run")
        subject = self._subject(run["id"])
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM current_accepted_claim_subjects").fetchone()[0],
            0,
        )

        accepted = accept_pipeline_run(
            self.conn,
            run["id"],
            stage="claims",
            reviewed_by="fixture-reviewer",
            rationale="Schema and evidence review passed.",
            decided_at=TS,
        )
        self.assertEqual(accepted["decision"], "accepted")
        self.assertEqual(
            self.conn.execute("SELECT id FROM current_accepted_claim_subjects").fetchone()[0],
            subject["id"],
        )

        reject_pipeline_run(
            self.conn,
            run["id"],
            stage="claims",
            reviewed_by="fixture-reviewer",
            rationale="A later audit rejected this run.",
            decided_at="2026-07-20T12:01:00+00:00",
        )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM current_accepted_claim_subjects").fetchone()[0],
            0,
        )
        accept_pipeline_run(
            self.conn,
            run["id"],
            stage="claims",
            reviewed_by="fixture-reviewer-2",
            rationale="The corrected audit accepted this immutable run.",
            decided_at="2026-07-20T12:02:00+00:00",
        )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM current_accepted_claim_subjects").fetchone()[0],
            1,
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "UPDATE pipeline_run_authority_decisions SET decision = 'rejected' WHERE id = ?",
                (accepted["id"],),
            )

    def test_superseding_run_atomically_replaces_stage_authority(self) -> None:
        old_run = self._claims_run("claims-old")
        old_subject = self._subject(old_run["id"])
        accept_pipeline_run(
            self.conn,
            old_run["id"],
            stage="claims",
            reviewed_by="fixture-reviewer",
            rationale="Initial accepted claim-grouping run.",
        )
        replacement = self._claims_run("claims-replacement")
        new_subject = self._subject(
            replacement["id"], revision=2, supersedes=old_subject["id"]
        )
        self.assertEqual(
            self.conn.execute("SELECT id FROM current_accepted_claim_subjects").fetchone()[0],
            old_subject["id"],
        )

        result = supersede_pipeline_run(
            self.conn,
            old_run["id"],
            replacement_run_id=replacement["id"],
            stage="claims",
            reviewed_by="fixture-reviewer",
            rationale="The replacement corrects the accepted grouping.",
        )
        self.assertEqual(result["superseded"]["decision"], "superseded")
        self.assertEqual(
            self.conn.execute("SELECT id FROM current_accepted_claim_subjects").fetchone()[0],
            new_subject["id"],
        )

    def test_atomic_import_is_accepted_only_at_exact_validation_boundary(self) -> None:
        run = create_pipeline_run(
            self.conn,
            run_id="atomic-run",
            run_type="atomic_claim_import",
            run_schema="atomic_claim_v1",
            run_schema_version="atomic_claim_import_v1",
            corpus_release_id=self.release["id"],
            model="gpt-5.5",
            model_version="gpt-5.5",
            status="running",
            input_count=1,
            created_at=TS,
        )
        result = build_atomic_claims_from_release(
            self.conn, self.release["id"], pipeline_run_id=run["id"]
        )
        authority = self.conn.execute(
            """
            SELECT * FROM pipeline_run_authority_decisions
            WHERE pipeline_run_id = ? AND stage = 'atomic_claims'
            """,
            (run["id"],),
        ).fetchone()
        self.assertEqual(result["run_authority_decision_id"], authority["id"])
        self.assertEqual(authority["run_status"], "running")
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM current_accepted_atomic_claims").fetchone()[0],
            0,
        )
        transition_pipeline_run(
            self.conn,
            run["id"],
            status="succeeded",
            expected_status="running",
            input_count=1,
            output_count=1,
            at=TS,
        )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM current_accepted_atomic_claims").fetchone()[0],
            1,
        )

    def test_contract_allowlist_blocks_lab_and_snapshot_drift(self) -> None:
        lab = self._completed_run(
            "lab-run", run_type="lab", run_schema="pif_semantic_reconciliation_output_v2"
        )
        with self.assertRaises(IntelligenceValidationError):
            accept_pipeline_run(
                self.conn,
                lab["id"],
                stage="claims",
                reviewed_by="fixture-reviewer",
                rationale="A lab result must never become canonical.",
            )

        run = self._claims_run("drift-run")
        self._subject(run["id"])
        accept_pipeline_run(
            self.conn,
            run["id"],
            stage="claims",
            reviewed_by="fixture-reviewer",
            rationale="Accepted before the simulated metadata drift.",
        )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM current_accepted_claim_subjects").fetchone()[0],
            1,
        )
        self.conn.execute(
            "UPDATE pipeline_runs SET prompt_version = 'mutated-after-acceptance' WHERE id = ?",
            (run["id"],),
        )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM current_accepted_claim_subjects").fetchone()[0],
            0,
        )

    def test_forward_migration_backfills_only_exact_promoted_release_contract(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            conn.executescript(db.SCHEMA)
            db.migrate_schema(conn)
            for _version, _name, statements in db.SCHEMA_MIGRATIONS[:3]:
                for statement in statements:
                    conn.execute(statement)
            conn.execute(
                """
                INSERT INTO corpus_releases
                  (id, schema_version, release_version, cutoff_at, manifest_sha256,
                   manifest_json, status, created_at)
                VALUES ('legacy-release', 'corpus_release_v1', 1, ?, ?, '{}', 'accepted', ?)
                """,
                (TS, "a" * 64, TS),
            )
            for run_id, run_type, run_schema, run_schema_version in (
                (
                    "verified-release-run",
                    "release_build",
                    "corpus_release_build",
                    "corpus_release_build_v1",
                ),
                (
                    "verified-atomic-promotion-run",
                    "atomic_claim_import",
                    "atomic_claim_v1",
                    "atomic_claim_import_v1",
                ),
                ("lab-release-run", "lab_release_build", "corpus_release_build", "1"),
            ):
                conn.execute(
                    """
                    INSERT INTO pipeline_runs
                      (id, schema_version, run_type, run_schema, run_schema_version,
                       corpus_release_id, configuration_sha256, status, created_at, updated_at)
                    VALUES (?, 'pipeline_run_v1', ?, ?, ?, 'legacy-release', ?, 'succeeded', ?, ?)
                    """,
                    (run_id, run_type, run_schema, run_schema_version, "b" * 64, TS, TS),
                )
            conn.execute(
                """
                INSERT INTO corpus_release_promotions
                  (id, schema_version, promotion_revision, corpus_release_id,
                   pipeline_run_id, action, promoted_by, rationale, created_at)
                VALUES ('verified-promotion', 'corpus_release_promotion_v1', 1,
                        'legacy-release', 'verified-atomic-promotion-run', 'promote',
                        'legacy-reviewer', 'Previously verified.', ?)
                """,
                (TS,),
            )
            conn.execute(
                """
                INSERT INTO corpus_release_promotions
                  (id, schema_version, promotion_revision, corpus_release_id,
                   pipeline_run_id, action, promoted_by, rationale, created_at)
                VALUES ('lab-promotion', 'corpus_release_promotion_v1', 2,
                        'legacy-release', 'lab-release-run', 'promote',
                        'lab-reviewer', 'Must not be trusted.', ?)
                """,
                (TS,),
            )
            for statement in db.SCHEMA_MIGRATIONS[3][2]:
                conn.execute(statement)
            rows = conn.execute(
                "SELECT pipeline_run_id FROM pipeline_run_authority_decisions ORDER BY pipeline_run_id"
            ).fetchall()
            self.assertEqual(
                [row[0] for row in rows], ["verified-atomic-promotion-run"]
            )
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
