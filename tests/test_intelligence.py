from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from research_factory import db
from research_factory.intelligence import (
    IntelligenceValidationError,
    brier_score,
    build_atomic_claims_from_release,
    categorical_outcome_score,
    compute_consensus_snapshot,
    compute_contrarian_snapshot,
    create_corpus_release,
    create_pipeline_run,
    outcome_score_summary,
    promote_corpus_release,
    record_canonical_person_decision,
    record_claim_relation_judgment,
    record_claim_subject,
    record_identity_resolution_judgment,
    record_outcome_resolution,
    record_position_observation,
    record_proposition_variant,
    record_source_affiliation,
    store_atomic_claim_v1,
    transition_pipeline_run,
    verify_corpus_release,
)


TS = "2026-07-20T12:00:00+00:00"


class IntelligenceSchemaTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.conn = db.connect(self.root / "factory.sqlite")
        db.init_db(self.conn)
        self._seed_corpus()

    def tearDown(self) -> None:
        self.conn.close()
        self.temp.cleanup()

    def _seed_corpus(self) -> None:
        for source_index in range(3):
            self.conn.execute(
                """
                INSERT INTO sources (id, name, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (f"source_{source_index}", f"Show {source_index}", TS, TS),
            )
        self.episode_ids: list[str] = []
        self.transcript_ids: list[str] = []
        self.segment_ids: list[str] = []
        self.label_ids: list[str] = []
        self.event_ids: list[str] = []
        self.mention_ids: list[str] = []
        self.evidence: dict[str, str] = {}
        for index in range(6):
            source_id = f"source_{index % 3}"
            episode_id = f"episode_{index}"
            transcript_id = f"transcript_{index}"
            segment_id = f"segment_{index}"
            label_id = f"label_{index}"
            event_id = f"event_{index}"
            mention_id = f"mention_{index}"
            published_at = f"2026-07-{10 + index:02d}T12:00:00+00:00"
            text = f"Evidence statement {index}."
            text_path = self.root / f"segment-{index}.txt"
            text_path.write_text(text, encoding="utf-8")
            transcript_path = self.root / f"transcript-{index}.txt"
            transcript_path.write_text(text, encoding="utf-8")
            output = {
                "schema_version": "ai_discourse_v3_1",
                "events": [
                    {
                        "event_type": "prediction",
                        "claim_text": f"Forecast claim {index}",
                        "evidence_text": text,
                        "evidence_start": 0,
                        "evidence_end": len(text),
                    }
                ],
            }
            self.conn.execute(
                """
                INSERT INTO episodes
                  (id, source_id, guid, title, published_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (episode_id, source_id, f"guid-{index}", f"Episode {index}", published_at, TS, TS),
            )
            self.conn.execute(
                """
                INSERT INTO transcripts
                  (id, episode_id, source_kind, raw_text_path, raw_text_sha256,
                   status, word_count, created_at, updated_at)
                VALUES (?, ?, 'official', ?, ?, 'ready', 3, ?, ?)
                """,
                (transcript_id, episode_id, str(transcript_path), f"raw-{index}", TS, TS),
            )
            self.conn.execute(
                """
                INSERT INTO segments
                  (id, transcript_id, episode_id, source_id, segment_index,
                   start_char, end_char, text_path, text_sha256, word_count, created_at)
                VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, 3, ?)
                """,
                (segment_id, transcript_id, episode_id, source_id, len(text), str(text_path), f"seg-{index}", TS),
            )
            self.conn.execute(
                """
                INSERT INTO episode_context_runs
                  (id, episode_id, transcript_id, label_pack, model, status,
                   created_at, updated_at, completed_at)
                VALUES (?, ?, ?, 'ai_discourse_v3_1', 'gpt-5.5', 'completed', ?, ?, ?)
                """,
                (f"context_{index}", episode_id, transcript_id, TS, TS, TS),
            )
            self.conn.execute(
                """
                INSERT INTO labels
                  (id, segment_id, label_pack, label_pack_version, model, status,
                   output_json, confidence, needs_review, created_at)
                VALUES (?, ?, 'ai_discourse_v3_1', '3.1', 'gpt-5.5', 'ready',
                        ?, 0.95, 0, ?)
                """,
                (label_id, segment_id, json.dumps(output, sort_keys=True), TS),
            )
            self.conn.execute(
                """
                INSERT INTO label_runs
                  (id, segment_id, label_pack, model, status, created_at, updated_at, completed_at)
                VALUES (?, ?, 'ai_discourse_v3_1', 'gpt-5.5', 'completed', ?, ?, ?)
                """,
                (f"label_run_{index}", segment_id, TS, TS, TS),
            )
            self.conn.execute(
                """
                INSERT INTO discourse_events
                  (id, label_id, segment_id, event_index, event_type, actor_name,
                   stance, claim_text, claim_type, certainty, temporal_horizon,
                   confidence, evidence_text, evidence_start, evidence_end, created_at)
                VALUES (?, ?, ?, 0, 'forecast', ?, 'affirming', ?, 'prediction',
                        'high', 'near_term', 0.9, ?, 0, ?, ?)
                """,
                (event_id, label_id, segment_id, f"Person {index}", f"Forecast claim {index}", text, len(text), TS),
            )
            self.conn.execute(
                """
                INSERT INTO raw_speaker_mentions
                  (id, episode_id, segment_id, discourse_event_id, surface_name,
                   resolution_status, confidence, created_at)
                VALUES (?, ?, ?, ?, ?, 'unresolved', 0.9, ?)
                """,
                (mention_id, episode_id, segment_id, event_id, f"Person {index}", TS),
            )
            self.episode_ids.append(episode_id)
            self.transcript_ids.append(transcript_id)
            self.segment_ids.append(segment_id)
            self.label_ids.append(label_id)
            self.event_ids.append(event_id)
            self.mention_ids.append(mention_id)
            self.evidence[segment_id] = text
        self.conn.commit()

    def _release(self, *, audited: list[str] | None = None) -> dict:
        return create_corpus_release(
            self.conn,
            pilot_id="fixture-v1",
            episode_ids=self.episode_ids,
            transcript_ids=self.transcript_ids,
            segment_ids=self.segment_ids,
            accepted_label_ids=self.label_ids,
            manifest_metadata={"audited_pilot_episode_ids": audited or []},
            cutoff_at=TS,
            created_at=TS,
        )

    def _run(self, release_id: str) -> dict:
        return create_pipeline_run(
            self.conn,
            run_id="pipeline_1",
            run_type="intelligence",
            run_schema="versioned_intelligence",
            run_schema_version="1",
            corpus_release_id=release_id,
            model="gpt-5.5",
            model_version="gpt-5.5",
            prompt_version="fixture-v1",
            status="running",
            input_count=100,
            created_at=TS,
        )

    def _claim_payload(self, index: int) -> dict:
        return {
            "id": f"atomic_{index}",
            "claim_lineage_id": f"atomic_lineage_{index}",
            "revision": 1,
            "claim_text": f"Forecast claim {index}",
            "claim_type": "prediction",
            "raw_speaker": f"Person {index}",
            "canonical_person_id": f"person_{index}",
            "stance": "affirming",
            "certainty": "high",
            "time_horizon": "near_term",
            "discourse_event_id": self.event_ids[index],
            "segment_id": self.segment_ids[index],
            "evidence_unit_type": "segment",
            "evidence_unit_id": self.segment_ids[index],
            "evidence_text": self.evidence[self.segment_ids[index]],
            "evidence_start": 0,
            "evidence_end": len(self.evidence[self.segment_ids[index]]),
            "extractor_model": "gpt-5.5",
            "extractor_schema": "ai_discourse_v3_1",
            "extractor_schema_version": "3.1",
            "provenance": {"label_id": self.label_ids[index]},
            "confidence": 0.9,
            "review_status": "accepted",
            "reviewed_by_model": "fixture-reviewer",
            "reviewed_at": TS,
            "forecast_probability": 0.8,
            "observed_at": f"2026-07-{10 + index:02d}T12:00:00+00:00",
            "created_at": TS,
        }

    def test_migration_release_integrity_and_audited_projection_gate(self) -> None:
        self.assertEqual(db.schema_migration_version(self.conn), 4)
        db.init_db(self.conn)
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0], 4
        )
        attempt_columns = {
            row["name"]
            for row in self.conn.execute("PRAGMA table_info(transcript_acquisition_attempts)")
        }
        status_columns = {
            row["name"]
            for row in self.conn.execute("PRAGMA table_info(transcript_acquisition_status)")
        }
        self.assertTrue({"idempotency_key", "next_eligible_at"} <= attempt_columns)
        self.assertIn("next_eligible_at", status_columns)

        release = self._release(audited=[self.episode_ids[0]])
        self.assertTrue(verify_corpus_release(self.conn, release["id"])["ok"])
        run = self._run(release["id"])
        projected = build_atomic_claims_from_release(
            self.conn, release["id"], pipeline_run_id=run["id"]
        )
        self.assertEqual(projected["accepted"], 1)
        self.assertEqual(projected["needs_review"], 5)
        self.assertEqual(projected["quarantined"], 0)
        self.assertFalse(projected["semantic_inference_performed"])
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "UPDATE corpus_release_labels SET accepted_status = 'other' WHERE corpus_release_id = ?",
                (release["id"],),
            )
        changed = {"schema_version": "ai_discourse_v3_1", "events": [], "changed": True}
        self.conn.execute(
            "UPDATE labels SET output_json = ? WHERE id = ?",
            (json.dumps(changed), self.label_ids[0]),
        )
        verification = verify_corpus_release(self.conn, release["id"])
        self.assertFalse(verification["ok"])
        self.assertTrue(any("content hash changed" in error for error in verification["errors"]))

    def test_current_views_identity_consensus_contrarian_and_outcomes(self) -> None:
        release = self._release()
        run = self._run(release["id"])

        orphan = record_canonical_person_decision(
            self.conn,
            person_id="person_orphan",
            display_name="Orphan Person",
            normalized_name="orphan person",
            decision="accepted",
            confidence=0.9,
            judge_model="gpt-5.5",
            judge_schema_version="identity-v1",
            rationale="Explicit identity candidate, not yet resolved to evidence.",
            evidence={"source": "fixture"},
            corpus_release_id=release["id"],
            pipeline_run_id=run["id"],
            decided_at=TS,
        )
        self.assertEqual(orphan["status"], "accepted")

        for index in range(6):
            person_fields = {
                "person_id": f"person_{index}",
                "display_name": f"Person {index}",
                "normalized_name": f"person {index}",
                "confidence": 0.95,
                "judge_model": "gpt-5.5",
                "judge_schema_version": "identity-v1",
                "rationale": "Explicit fixture identity decision.",
                "evidence": {"mention_id": self.mention_ids[index]},
                "corpus_release_id": release["id"],
                "pipeline_run_id": run["id"],
            }
            if index == 0:
                candidate = record_canonical_person_decision(
                    self.conn, decision="candidate", decided_at="2026-07-20T10:00:00+00:00", **person_fields
                )
                self.assertEqual(candidate["status"], "candidate")
            accepted = record_canonical_person_decision(
                self.conn, decision="accepted", decided_at=TS, **person_fields
            )
            self.assertEqual(accepted["status"], "accepted")
            record_identity_resolution_judgment(
                self.conn,
                raw_mention_type="speaker",
                raw_mention_id=self.mention_ids[index],
                canonical_person_id=f"person_{index}",
                decision="accepted",
                rationale="The supplied evidence resolves this speaker.",
                evidence={"surface_name": f"Person {index}"},
                judge_model="gpt-5.5",
                judge_schema_version="identity-v1",
                confidence=0.95,
                review_status="accepted",
                corpus_release_id=release["id"],
                pipeline_run_id=run["id"],
                decided_at=TS,
            )

        result = build_atomic_claims_from_release(
            self.conn,
            release["id"],
            pipeline_run_id=run["id"],
            claims=[self._claim_payload(index) for index in range(6)],
            evidence_units=self.evidence,
        )
        self.assertEqual(result["accepted"], 6)
        with self.assertRaises(IntelligenceValidationError):
            invalid = self._claim_payload(0)
            invalid.update(id="bad_atomic", claim_lineage_id="bad_lineage", evidence_text="not exact")
            store_atomic_claim_v1(
                self.conn,
                invalid,
                corpus_release_id=release["id"],
                pipeline_run_id=run["id"],
            )

        subject = record_claim_subject(
            self.conn,
            subject_text="Adoption forecast",
            subject_type="forecast",
            domain="technology",
            judge_model="gpt-5.5",
            judge_schema_version="subject-v1",
            confidence=0.95,
            rationale="The accepted judgments share this subject.",
            evidence={"claim_ids": result["claim_ids"]},
            corpus_release_id=release["id"],
            pipeline_run_id=run["id"],
            decided_at=TS,
        )
        variant = record_proposition_variant(
            self.conn,
            subject_id=subject["id"],
            proposition_text="The forecasted adoption will occur.",
            predicate_text="will occur",
            polarity="positive",
            time_horizon="near_term",
            judge_model="gpt-5.5",
            judge_schema_version="variant-v1",
            confidence=0.95,
            rationale="The accepted judgments address the same proposition.",
            evidence={"claim_ids": result["claim_ids"]},
            corpus_release_id=release["id"],
            pipeline_run_id=run["id"],
            decided_at=TS,
        )
        for index in range(6):
            record_position_observation(
                self.conn,
                subject_id=subject["id"],
                variant_id=variant["id"],
                atomic_claim_id=f"atomic_{index}",
                canonical_person_id=f"person_{index}",
                position="affirming",
                certainty="high",
                judge_model="gpt-5.5",
                judge_schema_version="position-v1",
                confidence=0.95,
                rationale="Explicit accepted position observation.",
                evidence={"claim_id": f"atomic_{index}"},
                corpus_release_id=release["id"],
                pipeline_run_id=run["id"],
                decided_at=TS,
            )
        for source_index, network in enumerate(("network-a", "network-a", "network-b")):
            record_source_affiliation(
                self.conn,
                source_id=f"source_{source_index}",
                affiliation_kind="network",
                affiliation_key=network,
                affiliation_name=network.title(),
                confidence=0.95,
                judge_model="gpt-5.5",
                evidence={"source_id": f"source_{source_index}"},
                corpus_release_id=release["id"],
                pipeline_run_id=run["id"],
                created_at=TS,
            )
        for index, relation in enumerate(
            ("contradicts", "contradicts", "contradicts", "contradicts", "supports"),
            start=1,
        ):
            record_claim_relation_judgment(
                self.conn,
                source_claim_id=f"atomic_{index}",
                target_claim_id="atomic_0",
                relation=relation,
                temporal_scope="At the observation time within the accepted 90-day window.",
                confidence=0.95,
                rationale="Explicit accepted relation judgment.",
                evidence={"source": f"atomic_{index}", "target": "atomic_0"},
                judge_model="gpt-5.5",
                review_status="accepted",
                corpus_release_id=release["id"],
                pipeline_run_id=run["id"],
                decided_at=TS,
            )

        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM current_accepted_atomic_claims").fetchone()[0],
            0,
        )
        transition_pipeline_run(
            self.conn,
            run["id"],
            status="succeeded",
            expected_status="running",
            output_count=30,
            failure_count=0,
            receipt={"verified": True},
            at=TS,
        )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM current_accepted_atomic_claims").fetchone()[0],
            0,
        )
        promote_corpus_release(
            self.conn,
            release["id"],
            pipeline_run_id=run["id"],
            promoted_by="fixture-reviewer",
            rationale="Verified fixture release.",
            created_at=TS,
        )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM current_accepted_atomic_claims").fetchone()[0],
            6,
        )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM current_accepted_people").fetchone()[0],
            6,
        )
        orphan_visible = self.conn.execute(
            "SELECT 1 FROM current_accepted_people WHERE id = 'person_orphan'"
        ).fetchone()
        self.assertIsNone(orphan_visible)

        consensus = compute_consensus_snapshot(
            self.conn,
            focal_claim_id="atomic_0",
            as_of=TS,
            exclude_claim_id="atomic_0",
        )
        self.assertEqual(consensus["people_count"], 5)
        self.assertEqual(consensus["show_count"], 3)
        self.assertEqual(consensus["network_count"], 2)
        self.assertEqual(consensus["dominant_bucket"], "opposed")
        self.assertAlmostEqual(consensus["dominant_share"], 0.8)
        contrarian = compute_contrarian_snapshot(
            self.conn, target_claim_id="atomic_0", as_of=TS
        )
        self.assertTrue(contrarian["is_contrarian"])
        self.assertAlmostEqual(contrarian["target_share"], 0.2)

        resolution = record_outcome_resolution(
            self.conn,
            claim_id="atomic_0",
            resolution_question="Did the forecasted adoption occur?",
            due_at="2026-07-19T12:00:00+00:00",
            resolution_window_start="2026-07-01T00:00:00+00:00",
            resolution_window_end=TS,
            resolution_criteria="Resolve true only if the authoritative source confirms adoption.",
            outcome="true",
            confidence=0.95,
            rationale="The authoritative evidence satisfies the criteria.",
            evidence={"summary": "sanitized"},
            authoritative_evidence=[{"kind": "official", "verified": True}],
            as_of=TS,
            resolver_model="gpt-5.5",
            resolver_version="resolver-v1",
            reviewer_version="review-v1",
            corpus_release_id=release["id"],
            pipeline_run_id=run["id"],
            review_status="accepted",
            resolved_at=TS,
        )
        self.assertEqual(resolution["categorical_score"], 1.0)
        self.assertAlmostEqual(resolution["brier_score"], 0.04)
        summary = outcome_score_summary(self.conn)
        self.assertEqual(summary["categorical_scored_count"], 1)
        self.assertEqual(summary["brier_scored_count"], 1)
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "UPDATE atomic_claims SET claim_text = 'mutated' WHERE id = 'atomic_0'"
            )

        record_identity_resolution_judgment(
            self.conn,
            raw_mention_type="speaker",
            raw_mention_id=self.mention_ids[5],
            canonical_person_id="person_5",
            decision="rejected",
            rationale="A later accepted review rejected the earlier mapping.",
            evidence={"superseding_review": True},
            judge_model="gpt-5.5",
            judge_schema_version="identity-v1",
            confidence=0.99,
            review_status="accepted",
            corpus_release_id=release["id"],
            pipeline_run_id=run["id"],
            decided_at="2026-07-20T13:00:00+00:00",
        )
        self.assertIsNone(
            self.conn.execute(
                "SELECT 1 FROM current_accepted_people WHERE id = 'person_5'"
            ).fetchone()
        )
        self.assertIsNone(
            self.conn.execute(
                "SELECT 1 FROM current_accepted_atomic_claims WHERE id = 'atomic_5'"
            ).fetchone()
        )

    def test_scores_and_pipeline_counts_are_fail_closed(self) -> None:
        self.assertEqual(categorical_outcome_score("true"), 1.0)
        self.assertEqual(categorical_outcome_score("mixed"), 0.5)
        self.assertEqual(categorical_outcome_score("false"), 0.0)
        self.assertIsNone(categorical_outcome_score("unresolved"))
        self.assertIsNone(categorical_outcome_score("unverifiable"))
        self.assertAlmostEqual(brier_score(0.8, "true"), 0.04)
        self.assertIsNone(brier_score(0.8, "mixed"))
        release = self._release()
        with self.assertRaises(IntelligenceValidationError):
            create_pipeline_run(
                self.conn,
                run_type="invalid",
                run_schema="invalid",
                run_schema_version="1",
                corpus_release_id=release["id"],
                status="running",
                input_count=1,
                output_count=1,
                failure_count=1,
                created_at=TS,
            )


if __name__ == "__main__":
    unittest.main()
