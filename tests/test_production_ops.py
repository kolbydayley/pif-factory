from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from research_factory import db
from research_factory.cli import build_parser, main
from research_factory.cohort import load_production_cohort
from research_factory.daily_cycle import (
    DAILY_STAGE_NAMES,
    MAX_EXCEPTION_RUNTIME_MS,
    OPERATIONAL_STAGE_NAMES,
    _checkpoint_authority,
    _recover_expired_queue_state,
    build_headless_exception_contract,
    run_daily_cycle,
)
from research_factory.production_ops import (
    build_release,
    plan_or_resolve_outcomes,
    pif_status,
    promote_release,
    publish_ops,
    reconcile_target,
    supersede_legacy_label_jobs,
    verify_release,
)
from research_factory.util import dumps_json, sha256_text


class DailyCycleTests(unittest.TestCase):
    def test_stage_order_receipts_and_idempotent_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            conn = db.connect(base / "factory.sqlite")
            try:
                db.init_db(conn)
                observed = []

                def handler(context):
                    observed.append(context.stage_name)
                    return {"status": "completed", "processed": 1, "stage": context.stage_name}

                handlers = {name: handler for name in OPERATIONAL_STAGE_NAMES}
                first = run_daily_cycle(
                    conn,
                    run_date="2026-07-20",
                    receipt_dir=base / "receipts",
                    max_runtime_seconds=60,
                    max_items=3,
                    stage_handlers=handlers,
                )
                self.assertTrue(first["ok"])
                self.assertEqual(first["status"], "completed")
                self.assertFalse(first["idempotent_replay"])
                self.assertEqual(observed, list(OPERATIONAL_STAGE_NAMES))
                self.assertEqual(
                    [item["stage_name"] for item in first["receipt"]["stage_receipts"]],
                    list(DAILY_STAGE_NAMES),
                )
                self.assertFalse(first["receipt"]["external_launch_attempted"])
                self.assertFalse(first["receipt"]["self_resuming_chats"])
                self.assertTrue(Path(first["receipt_path"]).exists())
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM pif_daily_stage_receipts").fetchone()[0],
                    len(DAILY_STAGE_NAMES),
                )

                replay = run_daily_cycle(
                    conn,
                    run_date="2026-07-20",
                    receipt_dir=base / "receipts",
                    max_runtime_seconds=60,
                    max_items=3,
                    stage_handlers=handlers,
                )
                self.assertTrue(replay["idempotent_replay"])
                self.assertEqual(replay["receipt"], first["receipt"])
                self.assertEqual(observed, list(OPERATIONAL_STAGE_NAMES))
            finally:
                conn.close()

    def test_item_bound_fails_closed_and_stops_following_stages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            conn = db.connect(Path(tmp) / "factory.sqlite")
            try:
                db.init_db(conn)
                called = []

                def too_many(context):
                    called.append(context.stage_name)
                    return {"status": "completed", "processed": context.max_items + 1}

                result = run_daily_cycle(
                    conn,
                    run_date="2026-07-21",
                    receipt_dir=Path(tmp) / "receipts",
                    max_runtime_seconds=60,
                    max_items=2,
                    stage_handlers={OPERATIONAL_STAGE_NAMES[0]: too_many},
                )
                self.assertFalse(result["ok"])
                self.assertEqual(result["status"], "failed")
                self.assertEqual(called, [OPERATIONAL_STAGE_NAMES[0]])
                self.assertEqual(
                    [item["stage_name"] for item in result["receipt"]["stage_receipts"]],
                    [OPERATIONAL_STAGE_NAMES[0], DAILY_STAGE_NAMES[-1]],
                )
            finally:
                conn.close()

    def test_headless_exception_contract_is_exact_and_bounded(self) -> None:
        contract = build_headless_exception_contract(
            source_task_id="daily:outcome:1",
            title="Bounded outcome check",
            prompt="Resolve one sanitized forecast claim and return a schema-valid packet.",
            max_runtime_ms=60_000,
        )
        self.assertEqual(
            set(contract),
            {
                "projectId",
                "sourceApp",
                "sourceTaskId",
                "cwd",
                "prompt",
                "policyProfile",
                "priority",
                "sandbox",
                "maxRuntimeMs",
                "title",
            },
        )
        self.assertEqual(contract["maxRuntimeMs"], 60_000)
        with self.assertRaises(ValueError):
            build_headless_exception_contract(
                source_task_id="too-long",
                title="Too long",
                prompt="bounded",
                max_runtime_ms=MAX_EXCEPTION_RUNTIME_MS + 1,
            )

    def test_checkpoint_fallback_is_shared_verified_and_reusable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            conn = db.connect(base / "factory.sqlite")
            try:
                db.init_db(conn)
                first = _checkpoint_authority(
                    conn,
                    receipt_root=base / "receipts",
                    run_date="2026-07-20",
                    retention=2,
                    _force_online_backup=True,
                )
                self.assertTrue(first["verified"])
                self.assertEqual(first["method"], "sqlite_online_backup_fallback")
                self.assertTrue(Path(first["path"]).exists())
                self.assertIn("_authority_checkpoints", Path(first["path"]).parts)

                replay = _checkpoint_authority(
                    conn,
                    receipt_root=base / "receipts",
                    run_date="2026-07-20",
                    retention=2,
                    _force_online_backup=True,
                )
                self.assertTrue(replay["verified"])
                self.assertTrue(replay["reused"])
                self.assertEqual(replay["path"], first["path"])
            finally:
                conn.close()

    def test_expired_leases_and_zombie_runs_recover_within_one_bound(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            conn = db.connect(Path(tmp) / "factory.sqlite")
            try:
                db.init_db(conn)
                job_requeue = db.enqueue_job(
                    conn,
                    lane="podcast",
                    job_type="label_segment",
                    target_id="expired-requeue",
                    payload={},
                    max_attempts=2,
                )
                job_fail = db.enqueue_job(
                    conn,
                    lane="podcast",
                    job_type="label_segment",
                    target_id="expired-fail",
                    payload={},
                    max_attempts=2,
                )
                job_live = db.enqueue_job(
                    conn,
                    lane="podcast",
                    job_type="label_segment",
                    target_id="live-claim",
                    payload={},
                    max_attempts=2,
                )
                conn.execute(
                    "UPDATE jobs SET status='claimed', attempts=1, lease_owner='old-worker', leased_until='2026-07-20T10:00:00+00:00' WHERE id=?",
                    (job_requeue,),
                )
                conn.execute(
                    "UPDATE jobs SET status='claimed', attempts=2, lease_owner='old-worker', leased_until='2026-07-20T10:00:00+00:00' WHERE id=?",
                    (job_fail,),
                )
                conn.execute(
                    "UPDATE jobs SET status='claimed', attempts=1, lease_owner='live-worker', leased_until='2026-07-20T14:00:00+00:00' WHERE id=?",
                    (job_live,),
                )
                for run_id, worker_id in (("run-zombie", "old-worker"), ("run-live", "live-worker")):
                    conn.execute(
                        """
                        INSERT INTO worker_runs
                          (id, worker_id, worker_role, model, status, metrics_json, created_at, updated_at)
                        VALUES (?, ?, 'extractor', 'gpt-5.5', 'running', '{}', ?, ?)
                        """,
                        (run_id, worker_id, "2026-07-20T10:00:00+00:00", "2026-07-20T10:00:00+00:00"),
                    )
                conn.commit()

                result = _recover_expired_queue_state(
                    conn,
                    limit=3,
                    at="2026-07-20T12:00:00+00:00",
                )
                self.assertEqual(result["recovered_total"], 3)
                self.assertEqual(result["expired_jobs_requeued"], 1)
                self.assertEqual(result["expired_jobs_failed"], 1)
                self.assertEqual(result["zombie_worker_runs_closed"], 1)
                self.assertEqual(conn.execute("SELECT status FROM jobs WHERE id=?", (job_requeue,)).fetchone()[0], "pending")
                self.assertEqual(conn.execute("SELECT status FROM jobs WHERE id=?", (job_fail,)).fetchone()[0], "failed")
                live = conn.execute("SELECT status, lease_owner FROM jobs WHERE id=?", (job_live,)).fetchone()
                self.assertEqual((live["status"], live["lease_owner"]), ("claimed", "live-worker"))
                self.assertEqual(conn.execute("SELECT status FROM worker_runs WHERE id='run-zombie'").fetchone()[0], "failed")
                self.assertEqual(conn.execute("SELECT status FROM worker_runs WHERE id='run-live'").fetchone()[0], "running")
            finally:
                conn.close()

    def test_baseline_stage_never_creates_headless_bulk_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            conn = db.connect(base / "factory.sqlite")
            try:
                db.init_db(conn)
                db.enqueue_job(
                    conn,
                    lane="podcast",
                    job_type="label_segment",
                    target_id="pending-managed-baseline",
                    payload={"label_pack": "ai_discourse_v3_1", "model": "gpt-5.5"},
                )

                def completed(context):
                    return {"status": "completed", "processed": 0}

                handlers = {
                    name: completed
                    for name in OPERATIONAL_STAGE_NAMES
                    if name != "bounded_baseline_extraction"
                }
                result = run_daily_cycle(
                    conn,
                    run_date="2026-07-22",
                    receipt_dir=base / "receipts",
                    max_runtime_seconds=60,
                    max_items=3,
                    record_exception_contracts=True,
                    stage_handlers=handlers,
                )
                self.assertTrue(result["ok"])
                row = conn.execute(
                    "SELECT receipt_json FROM pif_daily_stage_receipts WHERE stage_name='bounded_baseline_extraction'"
                ).fetchone()
                receipt = json.loads(row["receipt_json"])
                self.assertEqual(receipt["result"]["reason"], "managed_app_server_dispatch_required")
                self.assertEqual(receipt["result"]["dispatch_contracts"], [])
                self.assertFalse(receipt["result"]["headless_exception_dispatch_created"])
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM pif_exception_dispatches").fetchone()[0], 0)
            finally:
                conn.close()


class ProductionOpsTests(unittest.TestCase):
    def _seed_production_cohort(self, conn, base: Path) -> None:
        cohort = load_production_cohort()
        timestamp = "2026-07-20T12:00:00+00:00"
        source_ids = sorted({str(item["source_id"]) for item in cohort["episodes"]})
        for source_id in source_ids:
            conn.execute(
                "INSERT INTO sources (id, name, created_at, updated_at) VALUES (?, ?, ?, ?)",
                (source_id, source_id, timestamp, timestamp),
            )
        for index, item in enumerate(cohort["episodes"]):
            episode_id = str(item["id"])
            source_id = str(item["source_id"])
            transcript_id = f"transcript-{index}"
            segment_id = f"segment-{index}"
            label_id = f"label-{index}"
            evidence = f"Exact accepted evidence for cohort episode {index}."
            text_path = base / f"segment-{index}.txt"
            transcript_path = base / f"transcript-{index}.txt"
            text_path.write_text(evidence, encoding="utf-8")
            transcript_path.write_text(evidence, encoding="utf-8")
            conn.execute(
                """
                INSERT INTO episodes
                  (id, source_id, guid, title, published_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (episode_id, source_id, f"guid-{index}", f"Episode {index}", item["published_at"], timestamp, timestamp),
            )
            conn.execute(
                """
                INSERT INTO transcripts
                  (id, episode_id, source_kind, raw_text_path, raw_text_sha256,
                   status, word_count, created_at, updated_at)
                VALUES (?, ?, 'official', ?, ?, 'ready', 7, ?, ?)
                """,
                (transcript_id, episode_id, str(transcript_path), sha256_text(evidence), timestamp, timestamp),
            )
            conn.execute(
                """
                INSERT INTO segments
                  (id, transcript_id, episode_id, source_id, segment_index,
                   start_char, end_char, text_path, text_sha256, word_count, created_at)
                VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, 7, ?)
                """,
                (segment_id, transcript_id, episode_id, source_id, len(evidence), str(text_path), sha256_text(evidence), timestamp),
            )
            conn.execute(
                """
                INSERT INTO episode_context_runs
                  (id, episode_id, transcript_id, label_pack, model, status,
                   created_at, updated_at, completed_at)
                VALUES (?, ?, ?, 'ai_discourse_v3_1', 'gpt-5.5', 'completed', ?, ?, ?)
                """,
                (f"context-{index}", episode_id, transcript_id, timestamp, timestamp, timestamp),
            )
            output = {
                "schema_version": "ai_discourse_v3_1",
                "segment_id": segment_id,
                "episode_id": episode_id,
            }
            conn.execute(
                """
                INSERT INTO labels
                  (id, segment_id, label_pack, label_pack_version, model, status,
                   output_json, confidence, needs_review, created_at)
                VALUES (?, ?, 'ai_discourse_v3_1', 'ai_discourse_v3_1', 'gpt-5.5',
                        'ready', ?, 0.95, 0, ?)
                """,
                (label_id, segment_id, json.dumps(output, sort_keys=True), timestamp),
            )
            conn.execute(
                """
                INSERT INTO label_runs
                  (id, segment_id, label_pack, model, status, created_at, updated_at, completed_at)
                VALUES (?, ?, 'ai_discourse_v3_1', 'gpt-5.5', 'completed', ?, ?, ?)
                """,
                (f"label-run-{index}", segment_id, timestamp, timestamp, timestamp),
            )
            for event_index in range(8):
                conn.execute(
                    """
                    INSERT INTO discourse_events
                      (id, label_id, segment_id, event_index, event_type, actor_name,
                       stance, claim_text, claim_type, certainty, temporal_horizon,
                       confidence, evidence_text, evidence_start, evidence_end, created_at)
                    VALUES (?, ?, ?, ?, 'forecast', 'Accepted Speaker', 'affirming', ?,
                            'prediction', 'high', 'near_term', 0.9, ?, 0, ?, ?)
                    """,
                    (
                        f"event-{index}-{event_index}",
                        label_id,
                        segment_id,
                        event_index,
                        f"Accepted forecast {index}-{event_index}",
                        evidence,
                        len(evidence),
                        timestamp,
                    ),
                )
            db.enqueue_job(
                conn,
                lane="podcast",
                job_type="label_segment",
                target_id=segment_id,
                payload={
                    "pilot_id": "scale-gate-v31-2026-07-02",
                    "cohort_id": "pif-gold-25-v1",
                    "label_pack": "ai_discourse_v3_1",
                    "model": "gpt-5.5",
                },
            )
        conn.commit()

    def _seed_promoted_release_for_supersede(self, conn, base: Path) -> tuple[Path, Path]:
        timestamp = "2026-07-20T12:00:00+00:00"
        content_hash = sha256_text("supersede-fixture")
        conn.execute(
            "INSERT INTO sources (id, name, created_at, updated_at) VALUES ('source-release', 'Release source', ?, ?)",
            (timestamp, timestamp),
        )
        for episode_id in ("episode-release", "episode-outside"):
            conn.execute(
                """
                INSERT INTO episodes
                  (id, source_id, guid, title, published_at, created_at, updated_at)
                VALUES (?, 'source-release', ?, ?, ?, ?, ?)
                """,
                (
                    episode_id,
                    f"guid-{episode_id}",
                    episode_id,
                    timestamp,
                    timestamp,
                    timestamp,
                ),
            )
            transcript_id = f"transcript-{episode_id}"
            conn.execute(
                """
                INSERT INTO transcripts
                  (id, episode_id, source_kind, raw_text_path, raw_text_sha256,
                   status, word_count, created_at, updated_at)
                VALUES (?, ?, 'official', ?, ?, 'ready', 1, ?, ?)
                """,
                (
                    transcript_id,
                    episode_id,
                    str(base / f"{transcript_id}.txt"),
                    content_hash,
                    timestamp,
                    timestamp,
                ),
            )
        segment_specs = (
            ("seg-release", "episode-release", 0),
            ("seg-release-sibling", "episode-release", 1),
            ("seg-null-pilot", "episode-outside", 0),
            ("seg-other-pilot", "episode-outside", 1),
            ("seg-legacy-pilot", "episode-outside", 2),
        )
        for segment_id, episode_id, segment_index in segment_specs:
            transcript_id = f"transcript-{episode_id}"
            conn.execute(
                """
                INSERT INTO segments
                  (id, transcript_id, episode_id, source_id, segment_index,
                   start_char, end_char, text_path, text_sha256, word_count, created_at)
                VALUES (?, ?, ?, 'source-release', ?, 0, 1, ?, ?, 1, ?)
                """,
                (
                    segment_id,
                    transcript_id,
                    episode_id,
                    segment_index,
                    str(base / f"{segment_id}.txt"),
                    content_hash,
                    timestamp,
                ),
            )

        episode_member_hash = sha256_text("episode-release-member")
        transcript_member_hash = sha256_text("transcript-release-member")
        segment_member_hash = sha256_text("segment-release-member")
        manifest = {
            "schema_version": "corpus_release_manifest_v1",
            "pilot_id": "pif-gold-25-v1",
            "metadata": {},
            "episodes": [{"id": "episode-release", "content_sha256": episode_member_hash}],
            "transcripts": [
                {
                    "id": "transcript-episode-release",
                    "content_sha256": transcript_member_hash,
                }
            ],
            "segments": [{"id": "seg-release", "content_sha256": segment_member_hash}],
            "labels": [],
        }
        manifest_sha = sha256_text(dumps_json(manifest))
        conn.execute(
            """
            INSERT INTO corpus_releases
              (id, schema_version, release_version, cutoff_at, manifest_sha256,
               manifest_json, source_count, item_count, claim_count, status, created_at)
            VALUES ('release-test', 'corpus_release_v1', 1, ?, ?, ?, 1, 1, 0, 'accepted', ?)
            """,
            (timestamp, manifest_sha, dumps_json(manifest), timestamp),
        )
        conn.execute(
            """
            INSERT INTO corpus_release_episodes
              (corpus_release_id, episode_id, content_sha256, member_index, created_at)
            VALUES ('release-test', 'episode-release', ?, 0, ?)
            """,
            (episode_member_hash, timestamp),
        )
        conn.execute(
            """
            INSERT INTO corpus_release_transcripts
              (corpus_release_id, transcript_id, episode_id, content_sha256, member_index, created_at)
            VALUES ('release-test', 'transcript-episode-release', 'episode-release', ?, 0, ?)
            """,
            (transcript_member_hash, timestamp),
        )
        conn.execute(
            """
            INSERT INTO corpus_release_segments
              (corpus_release_id, segment_id, transcript_id, episode_id,
               content_sha256, member_index, created_at)
            VALUES ('release-test', 'seg-release', 'transcript-episode-release',
                    'episode-release', ?, 0, ?)
            """,
            (segment_member_hash, timestamp),
        )
        conn.execute(
            """
            INSERT INTO pipeline_runs
              (id, run_type, run_schema, run_schema_version, corpus_release_id,
               configuration_sha256, status, parameters_json, metrics_json,
               receipt_json, input_count, output_count, failure_count,
               started_at, completed_at, created_at, updated_at)
            VALUES ('pipeline-release-test', 'release_build', 'corpus_release_build',
                    'corpus_release_build_v1', 'release-test', ?, 'succeeded', '{}', '{}', '{}',
                    1, 1, 0, ?, ?, ?, ?)
            """,
            (sha256_text("pipeline-release-test"), timestamp, timestamp, timestamp, timestamp),
        )
        from research_factory.intelligence import accept_pipeline_run

        accept_pipeline_run(
            conn,
            "pipeline-release-test",
            stage="release",
            reviewed_by="test",
            rationale="accept verified release test run",
            decided_at=timestamp,
        )
        conn.execute(
            """
            INSERT INTO corpus_release_promotions
              (id, promotion_revision, corpus_release_id, pipeline_run_id,
               action, promoted_by, rationale, created_at)
            VALUES ('promotion-release-test', 1, 'release-test', 'pipeline-release-test',
                    'promote', 'test', 'verified test promotion', ?)
            """,
            (timestamp,),
        )
        conn.commit()

        manifest_path = base / "manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        verification = {
            "ok": True,
            "status": "verified",
            "release_id": "release-test",
            "accepted_schema": "corpus_release_v1",
            "manifest_sha256": manifest_sha,
        }
        verification_path = base / "verification.json"
        verification_path.write_text(json.dumps(verification), encoding="utf-8")
        return manifest_path, verification_path

    def test_release_build_verify_promote_is_real_idempotent_sqlite_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            conn = db.connect(base / "factory.sqlite")
            try:
                db.init_db(conn)
                self._seed_production_cohort(conn, base)
                artifacts = base / "release-artifacts"
                built = build_release(conn, output_dir=artifacts)
                self.assertTrue(built["ok"], built)
                self.assertEqual(built["pipeline_status"], "succeeded")
                self.assertTrue(Path(built["manifest_path"]).exists())
                self.assertTrue(Path(built["verification_path"]).exists())
                self.assertEqual(built["verification"]["membership_counts"]["episodes"], 25)
                self.assertEqual(built["verification"]["evidence_records_checked"], 200)

                verified = verify_release(
                    conn,
                    release_id=built["release_id"],
                    manifest_path=built["manifest_path"],
                    output_dir=artifacts,
                )
                self.assertTrue(verified["ok"], verified)

                promoted = promote_release(
                    conn,
                    release_id=built["release_id"],
                    manifest_path=built["manifest_path"],
                    verification_path=built["verification_path"],
                    output_dir=artifacts,
                )
                self.assertTrue(promoted["ok"], promoted)
                self.assertEqual(promoted["atomic_claim_count"], 200)
                self.assertEqual(promoted["accepted_atomic_claim_count"], 64)
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM current_accepted_atomic_claims").fetchone()[0],
                    64,
                )
                self.assertTrue(Path(promoted["promotion_path"]).exists())

                reconciliation = reconcile_target(
                    conn,
                    target="claims",
                    apply=True,
                    release_id=built["release_id"],
                    limit=5,
                    output_dir=base / "reconciliation",
                )
                self.assertTrue(reconciliation["ok"], reconciliation)
                self.assertTrue(reconciliation["packet_prepared"])
                self.assertFalse(reconciliation["canonical_mutation"])
                self.assertFalse(reconciliation["model_execution_attempted"])
                self.assertTrue(Path(reconciliation["result"]["packet_path"]).exists())

                claim_id = conn.execute(
                    "SELECT id FROM current_accepted_atomic_claims ORDER BY id LIMIT 1"
                ).fetchone()[0]
                resolved = plan_or_resolve_outcomes(
                    conn,
                    claim_id=claim_id,
                    outcome="true",
                    confidence=0.9,
                    rationale="The public outcome criterion was met.",
                    evidence={"source_url": "https://example.com/public-evidence"},
                    authoritative_evidence=[{"source_url": "https://example.com/public-evidence"}],
                    resolution_question="Did the forecasted event occur?",
                    due_at="2026-07-20T12:00:00+00:00",
                    resolution_window_start="2026-07-01T00:00:00+00:00",
                    resolution_window_end="2026-07-20T12:00:00+00:00",
                    resolution_criteria="A public authoritative source confirms the event.",
                    as_of="2026-07-20T12:00:00+00:00",
                    resolver_model="operator-supplied",
                    resolver_version="outcome_resolver_v1",
                    reviewer_version="outcome_reviewer_v1",
                    review_status="accepted",
                    resolved_at="2026-07-20T12:00:00+00:00",
                    categorical_score=1.0,
                    brier_score=None,
                    pipeline_run_id=None,
                    limit=10,
                    max_runtime_ms=60_000,
                    record_exceptions=False,
                )
                self.assertTrue(resolved["ok"], resolved)
                self.assertEqual(resolved["mode"], "explicit_resolution_revision")
                self.assertFalse(resolved["idempotent_replay"])
                self.assertTrue(resolved["pipeline_run_id"].startswith("pir_outcome_"))
                self.assertFalse(conn.in_transaction)
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM current_accepted_outcome_resolutions"
                    ).fetchone()[0],
                    1,
                )
                replay_resolution = plan_or_resolve_outcomes(
                    conn,
                    claim_id=claim_id,
                    outcome="true",
                    confidence=0.9,
                    rationale="The public outcome criterion was met.",
                    evidence={"source_url": "https://example.com/public-evidence"},
                    authoritative_evidence=[{"source_url": "https://example.com/public-evidence"}],
                    resolution_question="Did the forecasted event occur?",
                    due_at="2026-07-20T12:00:00+00:00",
                    resolution_window_start="2026-07-01T00:00:00+00:00",
                    resolution_window_end="2026-07-20T12:00:00+00:00",
                    resolution_criteria="A public authoritative source confirms the event.",
                    as_of="2026-07-20T12:00:00+00:00",
                    resolver_model="operator-supplied",
                    resolver_version="outcome_resolver_v1",
                    reviewer_version="outcome_reviewer_v1",
                    review_status="accepted",
                    resolved_at="2026-07-20T12:00:00+00:00",
                    categorical_score=1.0,
                    brier_score=None,
                    pipeline_run_id=None,
                    limit=10,
                    max_runtime_ms=60_000,
                    record_exceptions=False,
                )
                self.assertTrue(replay_resolution["idempotent_replay"])
                self.assertFalse(replay_resolution["canonical_mutation"])
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM outcome_resolution_revisions").fetchone()[0],
                    1,
                )

                replay = promote_release(
                    conn,
                    release_id=built["release_id"],
                    output_dir=artifacts,
                )
                self.assertTrue(replay["ok"])
                self.assertTrue(replay["idempotent_replay"])
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM corpus_release_promotions").fetchone()[0],
                    1,
                )
            finally:
                conn.close()

    def test_status_and_local_publish_preserve_operational_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            conn = db.connect(Path(tmp) / "factory.sqlite")
            try:
                db.init_db(conn)
                status = pif_status(conn)
                self.assertIn("counts", status)
                self.assertIn("jobs", status)
                self.assertTrue(status["operations"]["local_source_of_truth"])
                self.assertFalse(status["operations"]["automatic_model_execution"])
                published = publish_ops(conn, output=Path(tmp) / "snapshot.json")
                self.assertTrue(published["ok"])
                self.assertFalse(published["published"])
                self.assertFalse(published["network_attempted"])
                self.assertTrue(published["privacy"]["ok"])
            finally:
                conn.close()

    def test_supersede_legacy_jobs_is_bounded_backed_up_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            conn = db.connect(base / "factory.sqlite")
            try:
                db.init_db(conn)
                manifest_path, verification_path = self._seed_promoted_release_for_supersede(
                    conn, base
                )
                protected = db.enqueue_job(
                    conn,
                    lane="podcast",
                    job_type="label_segment",
                    target_id="seg-release",
                    payload={},
                )
                protected_same_episode = db.enqueue_job(
                    conn,
                    lane="podcast",
                    job_type="label_segment",
                    target_id="seg-release-sibling",
                    payload={"pilot_id": "obsolete-other-pilot"},
                )
                protected_episode_target = db.enqueue_job(
                    conn,
                    lane="podcast",
                    job_type="label_segment",
                    target_id="episode-release",
                    payload={"pilot_id": None},
                )
                null_pilot = db.enqueue_job(
                    conn,
                    lane="podcast",
                    job_type="label_segment",
                    target_id="seg-null-pilot",
                    payload={},
                    priority=10,
                )
                other_pilot = db.enqueue_job(
                    conn,
                    lane="podcast",
                    job_type="label_segment",
                    target_id="seg-other-pilot",
                    payload={"pilot_id": "some-other-pilot"},
                    priority=20,
                )
                legacy_pilot = db.enqueue_job(
                    conn,
                    lane="podcast",
                    job_type="label_segment",
                    target_id="seg-legacy-pilot",
                    payload={"pilot_id": "scale-gate-v31-2026-07-02"},
                    priority=30,
                )
                other_type = db.enqueue_job(
                    conn,
                    lane="podcast",
                    job_type="prepare_transcript",
                    target_id="seg-null-pilot",
                    payload={},
                )
                conn.commit()
                verifier = patch(
                    "research_factory.production_ops._intelligence_function",
                    side_effect=lambda name: (
                        (lambda conn, release_id: {"ok": release_id == "release-test"})
                        if name == "verify_corpus_release"
                        else None
                    ),
                )
                verifier.start()
                self.addCleanup(verifier.stop)

                blocked_backup = base / "must-not-overwrite.sqlite"
                blocked_backup.write_text("not a verified backup", encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "backup output already exists"):
                    supersede_legacy_label_jobs(
                        conn,
                        manifest_path=manifest_path,
                        verification_path=verification_path,
                        limit=2,
                        backup_output=blocked_backup,
                    )
                self.assertEqual(
                    conn.execute("SELECT status FROM jobs WHERE id = ?", (null_pilot,)).fetchone()[0],
                    "pending",
                )
                with patch(
                    "research_factory.production_ops._verify_supersede_backup",
                    return_value={"ok": False},
                ), self.assertRaisesRegex(ValueError, "backup verification failed"):
                    supersede_legacy_label_jobs(
                        conn,
                        manifest_path=manifest_path,
                        verification_path=verification_path,
                        limit=2,
                        backup_output=base / "failed-verification-backup.sqlite",
                    )
                self.assertEqual(
                    conn.execute("SELECT status FROM jobs WHERE id = ?", (null_pilot,)).fetchone()[0],
                    "pending",
                )

                first = supersede_legacy_label_jobs(
                    conn,
                    manifest_path=manifest_path,
                    verification_path=verification_path,
                    limit=2,
                    backup_output=base / "backup-1.sqlite",
                )
                self.assertTrue(first["ok"])
                self.assertEqual(first["selected"], 2)
                self.assertEqual(first["updated"], 2)
                self.assertEqual(first["before"]["pending_label_jobs"], 6)
                self.assertEqual(first["before"]["protected_by_release"], 3)
                self.assertEqual(first["before"]["eligible_outside_release"], 3)
                self.assertEqual(first["after"]["eligible_outside_release"], 1)
                self.assertTrue(Path(first["backup_path"]).exists())
                self.assertTrue(first["backup_verification"]["ok"])
                self.assertEqual(first["backup_verification"]["integrity_check"], ["ok"])
                self.assertEqual(first["backup_verification"]["candidate_job_count"], 2)
                self.assertFalse(first["batch_complete"])
                self.assertFalse(first["idempotent"])

                second = supersede_legacy_label_jobs(
                    conn,
                    manifest_path=manifest_path,
                    verification_path=verification_path,
                    limit=2,
                    backup_output=base / "backup-2.sqlite",
                )
                self.assertEqual(second["updated"], 1)
                self.assertEqual(second["remaining_eligible"], 0)
                self.assertTrue(second["batch_complete"])
                self.assertFalse(second["idempotent"])

                replay = supersede_legacy_label_jobs(
                    conn,
                    manifest_path=manifest_path,
                    verification_path=verification_path,
                    limit=1,
                    backup_output=base / "unused.sqlite",
                )
                self.assertTrue(replay["idempotent"])
                self.assertTrue(replay["idempotent_replay"])
                self.assertEqual(replay["updated"], 0)
                self.assertFalse(replay["backup_created"])
                self.assertEqual(conn.execute("SELECT status FROM jobs WHERE id = ?", (protected,)).fetchone()[0], "pending")
                self.assertEqual(conn.execute("SELECT status FROM jobs WHERE id = ?", (protected_same_episode,)).fetchone()[0], "pending")
                self.assertEqual(conn.execute("SELECT status FROM jobs WHERE id = ?", (protected_episode_target,)).fetchone()[0], "pending")
                self.assertEqual(conn.execute("SELECT status FROM jobs WHERE id = ?", (null_pilot,)).fetchone()[0], "superseded")
                self.assertEqual(conn.execute("SELECT status FROM jobs WHERE id = ?", (other_pilot,)).fetchone()[0], "superseded")
                self.assertEqual(conn.execute("SELECT status FROM jobs WHERE id = ?", (legacy_pilot,)).fetchone()[0], "superseded")
                self.assertEqual(conn.execute("SELECT status FROM jobs WHERE id = ?", (other_type,)).fetchone()[0], "pending")
            finally:
                conn.close()


class CompactCliTests(unittest.TestCase):
    def test_compact_and_legacy_run_shapes_parse(self) -> None:
        parser = build_parser()
        daily = parser.parse_args(["run", "daily", "--max-items", "3"])
        legacy = parser.parse_args(["run", "--limit", "3"])
        self.assertEqual(daily.run_mode, "daily")
        self.assertIsNone(legacy.run_mode)
        self.assertEqual(parser.parse_args(["release", "build"]).release_command, "build")
        self.assertEqual(parser.parse_args(["reconcile", "relations"]).reconcile_command, "relations")
        self.assertEqual(parser.parse_args(["outcomes", "resolve"]).outcomes_command, "resolve")
        self.assertEqual(parser.parse_args(["publish-ops"]).command, "publish-ops")

    def test_lab_is_non_mutating_plan_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "must-not-exist.sqlite"
            output = io.StringIO()
            with redirect_stdout(output):
                code = main(["--db", str(db_path), "lab", "efficiency-backtest", "--output", "report.json"])
            result = json.loads(output.getvalue())
            self.assertEqual(code, 0)
            self.assertTrue(result["dry_run"])
            self.assertFalse(result["writes_enabled"])
            self.assertFalse(result["external_execution_enabled"])
            self.assertFalse(db_path.exists())


if __name__ == "__main__":
    unittest.main()
