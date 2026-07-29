from __future__ import annotations

import datetime as dt
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research_factory import db
from research_factory.daily_cycle import (
    OPERATIONAL_STAGE_NAMES,
    _record_scale_gate_state_receipt,
    _validate_current_accepted_release,
    ensure_daily_schema,
    run_daily_cycle,
)
from research_factory.util import dumps_json, sha256_text


TIMESTAMP = "2026-07-20T12:00:00+00:00"


def _seed_current_release(conn: sqlite3.Connection, base: Path) -> Path:
    evidence = "Exact evidence for the accepted claim."
    segment_path = base / "segment.txt"
    transcript_path = base / "transcript.txt"
    segment_path.write_text(evidence, encoding="utf-8")
    transcript_path.write_text(evidence, encoding="utf-8")
    segment_sha = sha256_text(evidence)
    manifest_sha = sha256_text("manifest")
    config_sha = sha256_text("config")
    label_output = dumps_json({"schema_version": "ai_discourse_v3_1"})
    conn.execute(
        "INSERT INTO sources (id, name, created_at, updated_at) VALUES ('source', 'Source', ?, ?)",
        (TIMESTAMP, TIMESTAMP),
    )
    conn.execute(
        """
        INSERT INTO episodes
          (id, source_id, guid, title, published_at, created_at, updated_at)
        VALUES ('episode', 'source', 'guid', 'Episode', ?, ?, ?)
        """,
        (TIMESTAMP, TIMESTAMP, TIMESTAMP),
    )
    conn.execute(
        """
        INSERT INTO transcripts
          (id, episode_id, source_kind, raw_text_path, raw_text_sha256,
           status, word_count, created_at, updated_at)
        VALUES ('transcript', 'episode', 'official', ?, ?, 'ready', 6, ?, ?)
        """,
        (str(transcript_path), segment_sha, TIMESTAMP, TIMESTAMP),
    )
    conn.execute(
        """
        INSERT INTO segments
          (id, transcript_id, episode_id, source_id, segment_index, start_char,
           end_char, text_path, text_sha256, word_count, created_at)
        VALUES ('segment', 'transcript', 'episode', 'source', 0, 0, ?, ?, ?, 6, ?)
        """,
        (len(evidence), str(segment_path), segment_sha, TIMESTAMP),
    )
    conn.execute(
        """
        INSERT INTO labels
          (id, segment_id, label_pack, label_pack_version, model, status,
           output_json, confidence, needs_review, created_at)
        VALUES ('label', 'segment', 'ai_discourse_v3_1', 'ai_discourse_v3_1',
                'gpt-5.5', 'ready', ?, 0.95, 0, ?)
        """,
        (label_output, TIMESTAMP),
    )
    conn.execute(
        """
        INSERT INTO discourse_events
          (id, label_id, segment_id, event_index, event_type, actor_name, stance,
           claim_text, claim_type, certainty, temporal_horizon, confidence,
           evidence_text, evidence_start, evidence_end, created_at)
        VALUES ('event', 'label', 'segment', 0, 'forecast', 'Speaker', 'affirming',
                'A checkable claim', 'prediction', 'high', 'near_term', 0.9,
                ?, 0, ?, ?)
        """,
        (evidence, len(evidence), TIMESTAMP),
    )

    def member_sha(table: str, record_id: str) -> str:
        row = conn.execute(f"SELECT * FROM {table} WHERE id = ?", (record_id,)).fetchone()
        content = {
            key: row[key]
            for key in row.keys()
            if key not in {"created_at", "updated_at", "fetched_at"}
        }
        return sha256_text(dumps_json(content))

    episode_member_sha = member_sha("episodes", "episode")
    transcript_member_sha = member_sha("transcripts", "transcript")
    segment_member_sha = member_sha("segments", "segment")
    label_member_sha = member_sha("labels", "label")
    conn.execute(
        """
        INSERT INTO corpus_releases
          (id, release_version, cutoff_at, manifest_sha256, manifest_json,
           source_count, item_count, claim_count, status, created_at)
        VALUES ('release', 1, ?, ?, '{}', 5, 25, 1, 'accepted', ?)
        """,
        (TIMESTAMP, manifest_sha, TIMESTAMP),
    )
    conn.execute(
        """
        INSERT INTO corpus_release_episodes
          (corpus_release_id, episode_id, content_sha256, member_index, created_at)
        VALUES ('release', 'episode', ?, 0, ?)
        """,
        (episode_member_sha, TIMESTAMP),
    )
    conn.execute(
        """
        INSERT INTO corpus_release_transcripts
          (corpus_release_id, transcript_id, episode_id, content_sha256, member_index, created_at)
        VALUES ('release', 'transcript', 'episode', ?, 0, ?)
        """,
        (transcript_member_sha, TIMESTAMP),
    )
    conn.execute(
        """
        INSERT INTO corpus_release_segments
          (corpus_release_id, segment_id, transcript_id, episode_id,
           content_sha256, member_index, created_at)
        VALUES ('release', 'segment', 'transcript', 'episode', ?, 0, ?)
        """,
        (segment_member_sha, TIMESTAMP),
    )
    conn.execute(
        """
        INSERT INTO corpus_release_labels
          (corpus_release_id, label_id, segment_id, content_sha256,
           member_index, accepted_status, created_at)
        VALUES ('release', 'label', 'segment', ?, 0, 'accepted', ?)
        """,
        (label_member_sha, TIMESTAMP),
    )
    conn.execute(
        """
        INSERT INTO pipeline_runs
          (id, run_type, run_schema, run_schema_version, corpus_release_id,
           model, configuration_sha256, status, parameters_json, metrics_json,
           receipt_json, input_count, output_count, failure_count,
           started_at, completed_at, created_at, updated_at)
        VALUES ('run-release', 'release_build', 'corpus_release_build',
                'corpus_release_build_v1', 'release',
                'gpt-5.5', ?, 'succeeded', '{}', '{}', '{}', 1, 1, 0,
                ?, ?, ?, ?)
        """,
        (config_sha, TIMESTAMP, TIMESTAMP, TIMESTAMP, TIMESTAMP),
    )
    conn.execute(
        """
        INSERT INTO pipeline_runs
          (id, run_type, run_schema, run_schema_version, corpus_release_id,
           model, configuration_sha256, status, parameters_json, metrics_json,
           receipt_json, input_count, output_count, failure_count,
           started_at, completed_at, created_at, updated_at)
        VALUES ('run-atomic', 'atomic_claim_import', 'atomic_claim_v1',
                'atomic_claim_import_v1', 'release', 'gpt-5.5', ?, 'succeeded',
                '{}', '{}', '{}', 1, 1, 0, ?, ?, ?, ?)
        """,
        (config_sha, TIMESTAMP, TIMESTAMP, TIMESTAMP, TIMESTAMP),
    )
    for decision_id, stage, run_id, run_type, run_schema, run_schema_version in (
        (
            "authority-release",
            "release",
            "run-release",
            "release_build",
            "corpus_release_build",
            "corpus_release_build_v1",
        ),
        (
            "authority-atomic",
            "atomic_claims",
            "run-atomic",
            "atomic_claim_import",
            "atomic_claim_v1",
            "atomic_claim_import_v1",
        ),
    ):
        conn.execute(
            """
            INSERT INTO pipeline_run_authority_decisions
              (id, authority_lineage_id, revision, stage, pipeline_run_id,
               run_type, run_schema, run_schema_version, configuration_sha256,
               model, corpus_release_id, run_status, decision, reviewed_by,
               rationale, decided_at, created_at)
            VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, 'gpt-5.5', 'release',
                    'succeeded', 'accepted', 'test', 'test', ?, ?)
            """,
            (
                decision_id,
                f"lineage-{stage}",
                stage,
                run_id,
                run_type,
                run_schema,
                run_schema_version,
                config_sha,
                TIMESTAMP,
                TIMESTAMP,
            ),
        )
    conn.execute(
        """
        INSERT INTO corpus_release_promotions
          (id, promotion_revision, corpus_release_id, pipeline_run_id, action,
           promoted_by, rationale, created_at)
        VALUES ('promotion', 1, 'release', 'run-release', 'promote', 'test', 'test', ?)
        """,
        (TIMESTAMP,),
    )
    conn.execute(
        """
        INSERT INTO atomic_claims
          (id, claim_lineage_id, revision, corpus_release_id, pipeline_run_id,
           claim_text, claim_type, raw_speaker, stance, certainty, time_horizon,
           discourse_event_id, segment_id, source_id, episode_id,
           evidence_unit_type, evidence_unit_id, evidence_text, evidence_start,
           evidence_end, extractor_model, extractor_schema,
           extractor_schema_version, source_artifact_sha256, provenance_json,
           confidence, review_status, reviewed_by_model, reviewed_at,
           observed_at, created_at)
        VALUES ('claim', 'lineage', 1, 'release', 'run-atomic', 'A checkable claim',
                'prediction', 'Speaker', 'affirming', 'high', 'near_term',
                'event', 'segment', 'source', 'episode', 'segment', 'segment',
                ?, 0, ?, 'gpt-5.5', 'ai_discourse_v3_1', 'ai_discourse_v3_1',
                ?, ?, 0.9, 'accepted', 'test-reviewer', ?, ?, ?)
        """,
        (
            evidence,
            len(evidence),
            segment_member_sha,
            dumps_json({"label_id": "label", "projection": "literal_discourse_event_fields_v1"}),
            TIMESTAMP,
            TIMESTAMP,
            TIMESTAMP,
        ),
    )
    conn.commit()
    return segment_path


class DailyAcceptanceGateTests(unittest.TestCase):
    def test_due_managed_extraction_cannot_be_reported_as_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            conn = db.connect(base / "factory.sqlite")
            try:
                db.init_db(conn)
                db.enqueue_job(
                    conn,
                    lane="podcast",
                    job_type="label_segment",
                    target_id="pending",
                    payload={"label_pack": "ai_discourse_v3_1", "model": "gpt-5.5"},
                )

                def completed(context):
                    return {"status": "completed", "processed": 0}

                result = run_daily_cycle(
                    conn,
                    run_date="2026-07-20",
                    receipt_dir=base / "receipts",
                    max_runtime_seconds=60,
                    max_items=5,
                    stage_handlers={
                        name: completed
                        for name in OPERATIONAL_STAGE_NAMES
                        if name != "bounded_baseline_extraction"
                    },
                )
                self.assertFalse(result["ok"])
                self.assertEqual(result["status"], "blocked_required_work")
                blockers = result["receipt"]["required_stage_truth"]["blockers"]
                self.assertEqual(blockers[0]["stage"], "bounded_baseline_extraction")
                self.assertFalse(result["receipt"]["scale_gate"]["genuinely_successful"])
            finally:
                conn.close()

    def test_every_current_claim_uses_exact_file_hash_offsets_and_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            conn = db.connect(base / "factory.sqlite")
            try:
                db.init_db(conn)
                segment_path = _seed_current_release(conn, base)
                with patch(
                    "research_factory.daily_cycle._observer_privacy_health",
                    return_value={
                        "ok": True,
                        "contract": "railway-operational-v2",
                        "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
                        "forbidden_keys": [],
                    },
                ):
                    valid = _validate_current_accepted_release(conn, at=TIMESTAMP)
                    self.assertTrue(valid["ok"], valid["issues"])
                    self.assertEqual(valid["claims_checked"], 1)
                    self.assertTrue(valid["lineage"]["exact_offsets_checked_for_every_current_claim"])

                    segment_path.write_text("tampered evidence", encoding="utf-8")
                    invalid = _validate_current_accepted_release(conn, at=TIMESTAMP)
                self.assertFalse(invalid["ok"])
                codes = {item["issue"] for item in invalid["issues"]}
                self.assertIn("segment_file_hash_mismatch", codes)
                self.assertIn("evidence_offsets_out_of_bounds", codes)
            finally:
                conn.close()

    def test_scale_gate_requires_seven_distinct_consecutive_days(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            conn = db.connect(base / "factory.sqlite")
            try:
                db.init_db(conn)
                _seed_current_release(conn, base)
                ensure_daily_schema(conn)
                validation_result = {
                    "lineage": {
                        "ok": True,
                        "claims_checked": 200,
                        "issue_count": 0,
                        "exact_offsets_checked_for_every_current_claim": True,
                        "segment_hash_checked_for_every_current_claim": True,
                    },
                    "privacy": {
                        "ok": True,
                        "contract": "railway-operational-v2",
                        "forbidden_keys": [],
                    },
                    "queue": {
                        "ok": True,
                        "pending_jobs": 0,
                        "claimed_jobs": 0,
                        "expired_or_missing_leases": 0,
                        "zombie_worker_runs": 0,
                        "orphan_queue_envelopes": 0,
                    },
                }
                stage_receipts = [
                    {
                        "stage_name": "evidence_schema_privacy_validation",
                        "status": "completed",
                        "elapsed_ms": 10,
                        "result": validation_result,
                    }
                ]
                stage_truth = {"ok": True, "blockers": [], "healthy_no_work_stages": []}
                quality = {
                    "passed": True,
                    "counts": {
                        "accepted_atomic_claims": 200,
                        "accepted_claim_subjects": 10,
                        "accepted_claim_relations": 50,
                        "accepted_outcome_resolutions": 10,
                    },
                }
                cost = {
                    "passed": True,
                    "telemetry_mode": "managed_auth_app_server_no_metered_api_billing",
                    "succeeded_pipeline_runs": 1,
                }
                receipts = []
                with patch("research_factory.daily_cycle._quality_gate", return_value=quality), patch(
                    "research_factory.daily_cycle._cost_gate", return_value=cost
                ):
                    first = dt.date(2026, 7, 20)
                    for offset in range(7):
                        run_date = (first + dt.timedelta(days=offset)).isoformat()
                        run_id = f"daily-{offset}"
                        conn.execute(
                            """
                            INSERT INTO pif_daily_runs
                              (id, idempotency_key, run_date, config_json, status, started_at)
                            VALUES (?, ?, ?, '{}', 'running', ?)
                            """,
                            (run_id, run_id, run_date, f"{run_date}T00:00:00+00:00"),
                        )
                        conn.commit()
                        receipts.append(
                            _record_scale_gate_state_receipt(
                                conn,
                                run_id=run_id,
                                run_date=run_date,
                                stage_receipts=stage_receipts,
                                stage_truth=stage_truth,
                                cycle_status="completed",
                                elapsed_seconds=10,
                                max_runtime_seconds=60,
                                max_items=25,
                                created_at=f"{run_date}T00:00:01+00:00",
                            )
                        )

                    same_day_id = "daily-same-day-replay"
                    seventh_date = (first + dt.timedelta(days=6)).isoformat()
                    conn.execute(
                        """
                        INSERT INTO pif_daily_runs
                          (id, idempotency_key, run_date, config_json, status, started_at)
                        VALUES (?, ?, ?, '{}', 'running', ?)
                        """,
                        (same_day_id, same_day_id, seventh_date, f"{seventh_date}T01:00:00+00:00"),
                    )
                    conn.commit()
                    same_day = _record_scale_gate_state_receipt(
                        conn,
                        run_id=same_day_id,
                        run_date=seventh_date,
                        stage_receipts=stage_receipts,
                        stage_truth=stage_truth,
                        cycle_status="completed",
                        elapsed_seconds=10,
                        max_runtime_seconds=60,
                        max_items=25,
                        created_at=f"{seventh_date}T01:00:01+00:00",
                    )

                self.assertEqual(receipts[0]["consecutive_success_days"], 1)
                self.assertFalse(receipts[5]["promotion_eligible"])
                self.assertEqual(receipts[6]["consecutive_success_days"], 7)
                self.assertTrue(receipts[6]["promotion_eligible"])
                self.assertEqual(receipts[6]["next_tier"], "100")
                self.assertEqual(
                    receipts[6]["action"],
                    "eligibility_receipt_only_no_scale_enqueue_or_publish",
                )
                self.assertEqual(same_day["consecutive_success_days"], 7)
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(
                        "UPDATE pif_scale_gate_state_receipts SET promotion_eligible = 0 WHERE id = ?",
                        (receipts[6]["id"],),
                    )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
