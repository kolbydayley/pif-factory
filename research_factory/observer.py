from __future__ import annotations

import json
import os
import re
import urllib.request
from pathlib import Path
from typing import Any

from . import db
from .fast_quality import quality_velocity
from .paths import exports_dir, root
from .research_queue import sync_queue_envelopes
from .util import now_iso, write_text_atomic


def build_snapshot(conn) -> dict[str, Any]:
    generated_at = now_iso()
    sync_queue_envelopes(conn)
    job_counts = {
        row["status"]: row["count"]
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status").fetchall()
    }
    job_type_counts = [
        dict(row)
        for row in conn.execute(
            "SELECT lane, job_type, status, COUNT(*) AS count FROM jobs GROUP BY lane, job_type, status ORDER BY lane, job_type, status"
        ).fetchall()
    ]
    active_jobs = [
        _safe_job(row)
        for row in conn.execute(
            """
            SELECT id, lane, job_type, target_id, status, priority, attempts, lease_owner, leased_until, updated_at, error
            FROM jobs
            WHERE status IN ('claimed', 'failed')
            ORDER BY status DESC, updated_at DESC
            LIMIT 100
            """
        ).fetchall()
    ]
    recent_runs = [
        _safe_label_run(row)
        for row in conn.execute(
            """
            SELECT id, job_id, segment_id, label_pack, model, status, prompt_path, output_path, claimed_at, completed_at, error
            FROM label_runs
            ORDER BY updated_at DESC
            LIMIT 100
            """
        ).fetchall()
    ]
    artifacts = _recent_artifacts(exports_dir())
    counts = db.counts(conn)
    derived_table_counts = _derived_table_counts(conn)
    coding_metrics = _coding_metrics(conn)
    useful_signal_metrics = _useful_signal_metrics(conn)
    quality_velocity_metrics = quality_velocity(conn)
    transcript_preparation_metrics = _transcript_preparation_metrics(conn)
    transcript_acquisition_metrics = _transcript_acquisition_metrics(conn)
    research_queue_metrics = _research_queue_metrics(conn)
    content_metrics = _content_metrics(conn)
    source_yield = _source_yield(conn)
    intervention_flags = []
    expired_claims = [
        dict(row)
        for row in conn.execute(
            """
            SELECT job_type, COUNT(*) AS count
            FROM jobs
            WHERE status = 'claimed'
              AND leased_until IS NOT NULL
              AND leased_until < ?
            GROUP BY job_type
            """,
            (generated_at,),
        ).fetchall()
    ]
    missing_outputs = sum(
        1
        for row in conn.execute("SELECT output_path FROM label_runs WHERE status = 'claimed' AND output_path IS NOT NULL").fetchall()
        if not Path(row["output_path"]).exists()
    )
    source_error_jobs = sum(row["count"] for row in job_type_counts if row["job_type"] == "source_fetch_failed" and row["status"] in {"pending", "failed"})
    manual_transcript_jobs = sum(row["count"] for row in job_type_counts if row["job_type"] == "manual_transcript_required" and row["status"] == "pending")
    quarantined_transcripts = counts_by_status(conn, "transcripts").get("quarantined", 0)
    youtube_blocked = int(transcript_acquisition_metrics["status_counts"].get("youtube_caption_blocked", 0))
    transcription_eligible = int(transcript_acquisition_metrics["status_counts"].get("transcription_eligible", 0))
    if job_counts.get("failed", 0) > 0:
        intervention_flags.append({"severity": "warning", "message": f"{job_counts['failed']} failed job(s) need review."})
    if quarantined_transcripts:
        intervention_flags.append({"severity": "warning", "message": f"{quarantined_transcripts} quarantined transcript(s) need parser/source review."})
    if source_error_jobs:
        intervention_flags.append({"severity": "warning", "message": f"{source_error_jobs} source feed(s) failed and need source-list review."})
    if manual_transcript_jobs:
        intervention_flags.append({"severity": "info", "message": f"{manual_transcript_jobs} episode(s) lack creator-provided RSS transcript links."})
    if youtube_blocked:
        intervention_flags.append({"severity": "warning", "message": f"{youtube_blocked} episode(s) hit YouTube caption blocking; route through browser/manual discovery before transcription fallback."})
    if transcription_eligible and os.environ.get("ALLOW_PAID_TRANSCRIPTION", "").lower() not in {"1", "true", "yes", "on"}:
        intervention_flags.append({"severity": "info", "message": f"{transcription_eligible} episode(s) are eligible for audio transcription, but paid transcription fallback is disabled."})
    for item in expired_claims:
        intervention_flags.append({"severity": "critical", "message": f"{item['count']} expired claimed {item['job_type']} job(s) need recovery."})
    if missing_outputs:
        intervention_flags.append({"severity": "critical", "message": f"{missing_outputs} claimed label run(s) are missing output JSON files."})
    waiting_context = useful_signal_metrics.get("v3_1_labels_waiting_on_episode_context", 0)
    if waiting_context:
        intervention_flags.append({"severity": "info", "message": f"{waiting_context} v3.1 label job(s) are waiting on full-episode context."})
    reviewer_pending = useful_signal_metrics.get("reviewer_audits", {}).get("pending", 0)
    if reviewer_pending:
        intervention_flags.append({"severity": "info", "message": f"{reviewer_pending} GPT-5.5 reviewer audit(s) are pending completion."})
    claim_network = useful_signal_metrics.get("claim_network", {})
    if counts.get("claims", 0) and not claim_network.get("claim_clusters", 0):
        intervention_flags.append({"severity": "warning", "message": "Claims exist but claim clusters have not been built for agreement/disagreement review."})
    if job_counts.get("claimed", 0) > 25:
        intervention_flags.append({"severity": "warning", "message": "Large claimed-job backlog; check worker health."})
    if counts.get("segments", 0) and not counts.get("labels", 0):
        intervention_flags.append({"severity": "critical", "message": "Segments exist but labels have not landed yet."})
    if counts.get("labels", 0) and not counts.get("coded_observations", 0):
        intervention_flags.append({"severity": "warning", "message": "Labels exist but dense coded observations have not landed yet."})
    if counts.get("labels", 0) and counts.get("discourse_events", 0) == 0:
        intervention_flags.append({"severity": "info", "message": "No v3 discourse events have landed yet; dynamic shift detection will be empty."})
    # Snapshot construction can run while local workers are creating queue rows.
    # Refresh the envelope mirror and count surfaces at the end so the observer
    # does not show a transient jobs/envelopes mismatch.
    sync_queue_envelopes(conn)
    counts = db.counts(conn)
    derived_table_counts = _derived_table_counts(conn)
    research_queue_metrics = _research_queue_metrics(conn)
    return {
        "generated_at": generated_at,
        "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
        "counts": counts,
        "derived_table_counts": derived_table_counts,
        "coding_metrics": coding_metrics,
        "useful_signal_metrics": useful_signal_metrics,
        "quality_velocity_metrics": quality_velocity_metrics,
        "transcript_preparation_metrics": transcript_preparation_metrics,
        "transcript_acquisition_metrics": transcript_acquisition_metrics,
        "research_queue_metrics": research_queue_metrics,
        "content_metrics": content_metrics,
        "source_yield": source_yield,
        "job_counts": job_counts,
        "job_type_counts": job_type_counts,
        "active_jobs": active_jobs,
        "recent_runs": recent_runs,
        "artifacts": artifacts,
        "intervention_flags": intervention_flags,
    }


def counts_by_status(conn, table: str) -> dict[str, int]:
    return {
        row["status"]: int(row["count"])
        for row in conn.execute(f"SELECT status, COUNT(*) AS count FROM {table} GROUP BY status").fetchall()
    }


def _derived_table_counts(conn) -> dict[str, int]:
    tables = [
        "content_sources",
        "content_items",
        "content_artifacts",
        "content_spans",
        "context_packages",
        "extraction_runs",
        "queue_envelopes",
        "worker_runs",
        "job_claims",
        "output_submissions",
        "queue_events",
        "coded_observations",
        "topic_mentions",
        "term_mentions",
        "entity_mentions",
        "speaker_positions",
        "relationship_edges",
        "product_signals",
        "release_signal_links",
        "concepts",
        "concept_aliases",
        "concept_versions",
        "discourse_events",
        "discourse_event_contexts",
        "term_usages",
        "frame_usages",
        "actor_positions",
        "concept_candidates",
        "shift_signals",
        "signal_runs",
        "episode_context_runs",
        "reviewer_audits",
        "raw_speaker_mentions",
        "raw_actor_mentions",
        "identity_resolution_candidates",
        "claim_clusters",
        "agreement_edges",
        "disagreement_edges",
        "canonical_people",
        "canonical_orgs",
        "canonical_products",
        "canonical_models",
        "podcast_guest_edges",
        "person_concept_edges",
        "expert_authority_scores",
    ]
    return {table: int(conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()["count"]) for table in tables}


def _research_queue_metrics(conn) -> dict[str, Any]:
    depth_by_role = [
        dict(row)
        for row in conn.execute(
            """
            SELECT queue_envelopes.worker_role, queue_envelopes.content_type, jobs.status, COUNT(*) AS count
            FROM jobs
            JOIN queue_envelopes ON queue_envelopes.job_id = jobs.id
            GROUP BY queue_envelopes.worker_role, queue_envelopes.content_type, jobs.status
            ORDER BY queue_envelopes.worker_role, queue_envelopes.content_type, jobs.status
            """
        ).fetchall()
    ]
    privacy = {
        row["privacy_tier"]: int(row["count"])
        for row in conn.execute(
            """
            SELECT privacy_tier, COUNT(*) AS count
            FROM queue_envelopes
            GROUP BY privacy_tier
            """
        ).fetchall()
    }
    worker_runs = [
        dict(row)
        for row in conn.execute(
            """
            SELECT worker_role, status, COUNT(*) AS count,
                   COALESCE(SUM(claimed_jobs), 0) AS claimed_jobs,
                   COALESCE(SUM(completed_jobs), 0) AS completed_jobs,
                   COALESCE(SUM(failed_jobs), 0) AS failed_jobs
            FROM worker_runs
            GROUP BY worker_role, status
            ORDER BY worker_role, status
            """
        ).fetchall()
    ]
    remote_claimable = {
        row["privacy_tier"]: int(row["count"])
        for row in conn.execute(
            """
            SELECT privacy_tier, COUNT(*) AS count
            FROM queue_envelopes
            WHERE remote_claimable = 1
            GROUP BY privacy_tier
            """
        ).fetchall()
    }
    return {
        "project_mode": "research_intelligence_factory",
        "local_source_of_truth": True,
        "headless_codex_workers_enabled": True,
        "mcp_remote_worker_enabled": False,
        "remote_worker_contract_ready": True,
        "depth_by_role": depth_by_role,
        "privacy_tiers": privacy,
        "remote_claimable_by_privacy_tier": remote_claimable,
        "worker_runs": worker_runs,
    }


def _content_metrics(conn) -> dict[str, Any]:
    content_types = [
        dict(row)
        for row in conn.execute(
            """
            SELECT content_type, acquisition_status, privacy_tier, COUNT(*) AS count
            FROM content_items
            GROUP BY content_type, acquisition_status, privacy_tier
            ORDER BY content_type, acquisition_status, privacy_tier
            """
        ).fetchall()
    ]
    artifact_types = [
        dict(row)
        for row in conn.execute(
            """
            SELECT artifact_type, source_kind, status, COUNT(*) AS count
            FROM content_artifacts
            GROUP BY artifact_type, source_kind, status
            ORDER BY artifact_type, source_kind, status
            """
        ).fetchall()
    ]
    source_types = [
        dict(row)
        for row in conn.execute(
            """
            SELECT source_type, category, COUNT(*) AS count
            FROM content_sources
            GROUP BY source_type, category
            ORDER BY source_type, category
            """
        ).fetchall()
    ]
    return {
        "source_types": source_types,
        "content_types": content_types,
        "artifact_types": artifact_types,
        "supported_content_types": [
            "podcast_episode",
            "blog_post",
            "substack_post",
            "youtube_video",
            "paper",
            "release_note",
            "docs_page",
            "newsletter_issue",
        ],
    }


def _coding_metrics(conn) -> dict[str, Any]:
    labels_row = conn.execute(
        """
        SELECT
          COUNT(DISTINCT labels.id) AS v2_labels,
          COUNT(coded_observations.id) AS observations
        FROM labels
        JOIN segments ON segments.id = labels.segment_id
        LEFT JOIN coded_observations ON coded_observations.label_id = labels.id
        WHERE labels.label_pack = 'ai_discourse_v2'
        """
    ).fetchone()
    words = _distinct_labeled_segment_words(conn, "ai_discourse_v2")
    observations = int(labels_row["observations"] or 0)
    labels_by_pack = [dict(item) for item in conn.execute("SELECT label_pack, COUNT(*) AS count FROM labels GROUP BY label_pack ORDER BY label_pack").fetchall()]
    audits = [
        dict(item)
        for item in conn.execute(
            "SELECT label_pack, status, COUNT(*) AS count, AVG(score) AS avg_score FROM quality_audits GROUP BY label_pack, status ORDER BY label_pack, status"
        ).fetchall()
    ]
    v3 = _pack_discourse_metrics(conn, "ai_discourse_v3")
    v31 = _pack_discourse_metrics(conn, "ai_discourse_v3_1")
    return {
        "labels_by_pack": labels_by_pack,
        "quality_audits": audits,
        "v2_labels": int(labels_row["v2_labels"] or 0),
        "v2_observations": observations,
        "v2_observations_per_1000_segment_words": round((observations / words) * 1000, 2) if words else 0,
        "v3_labels": v3["labels"],
        "v3_discourse_events": v3["events"],
        "v3_events_per_1000_segment_words": v3["events_per_1000_words"],
        "v3_1_labels": v31["labels"],
        "v3_1_discourse_events": v31["events"],
        "v3_1_events_per_1000_segment_words": v31["events_per_1000_words"],
        "v3_1_full_episode_context_required": True,
        "v3_1_local_draft_disabled": True,
    }


def _pack_discourse_metrics(conn, label_pack: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
          COUNT(DISTINCT labels.id) AS labels,
          COUNT(DISTINCT discourse_events.id) AS events,
          COUNT(DISTINCT discourse_event_contexts.discourse_event_id) AS context_rows
        FROM labels
        JOIN segments ON segments.id = labels.segment_id
        LEFT JOIN discourse_events ON discourse_events.label_id = labels.id
        LEFT JOIN discourse_event_contexts ON discourse_event_contexts.discourse_event_id = discourse_events.id
        WHERE labels.label_pack = ?
        """,
        (label_pack,),
    ).fetchone()
    words = _distinct_labeled_segment_words(conn, label_pack)
    events = int(row["events"] or 0)
    return {
        "labels": int(row["labels"] or 0),
        "events": events,
        "context_rows": int(row["context_rows"] or 0),
        "events_per_1000_words": round((events / words) * 1000, 2) if words else 0,
    }


def _distinct_labeled_segment_words(conn, label_pack: str) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(word_count), 0) AS segment_words
        FROM (
          SELECT DISTINCT segments.id, segments.word_count
          FROM labels
          JOIN segments ON segments.id = labels.segment_id
          WHERE labels.label_pack = ?
        )
        """,
        (label_pack,),
    ).fetchone()
    return int(row["segment_words"] or 0)


def _useful_signal_metrics(conn) -> dict[str, Any]:
    candidate_status = {
        row["status"]: int(row["count"])
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM concept_candidates GROUP BY status").fetchall()
    }
    alias_status = {
        row["status"]: int(row["count"])
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM concept_aliases GROUP BY status").fetchall()
    }
    signal_type_counts = [
        dict(row)
        for row in conn.execute(
            "SELECT signal_type, COUNT(*) AS count, AVG(score) AS avg_score FROM shift_signals GROUP BY signal_type ORDER BY count DESC, signal_type"
        ).fetchall()
    ]
    burst_terms = [
        dict(row)
        for row in conn.execute(
            """
            SELECT term_a AS term, COUNT(*) AS count, MAX(score) AS max_score
            FROM shift_signals
            WHERE signal_type = 'term_burst'
            GROUP BY term_a
            ORDER BY max_score DESC, count DESC
            LIMIT 20
            """
        ).fetchall()
    ]
    actor_stance_changes = int(
        conn.execute("SELECT COUNT(*) AS count FROM shift_signals WHERE signal_type = 'stance_shift'").fetchone()["count"]
    )
    audit = conn.execute(
        "SELECT COUNT(*) AS count, AVG(score) AS avg_score FROM quality_audits WHERE label_pack = 'ai_discourse_v3'"
    ).fetchone()
    v31_audit = conn.execute(
        "SELECT COUNT(*) AS count, AVG(score) AS avg_score FROM quality_audits WHERE label_pack = 'ai_discourse_v3_1'"
    ).fetchone()
    episode_context_runs = {
        row["status"]: int(row["count"])
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM episode_context_runs GROUP BY status").fetchall()
    }
    raw_speaker_status = {
        row["resolution_status"]: int(row["count"])
        for row in conn.execute("SELECT resolution_status, COUNT(*) AS count FROM raw_speaker_mentions GROUP BY resolution_status").fetchall()
    }
    raw_actor_status = {
        row["resolution_status"]: int(row["count"])
        for row in conn.execute("SELECT resolution_status, COUNT(*) AS count FROM raw_actor_mentions GROUP BY resolution_status").fetchall()
    }
    reviewer_status = {
        row["status"]: int(row["count"])
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM reviewer_audits GROUP BY status").fetchall()
    }
    reviewer_scores = conn.execute(
        """
        SELECT
          COUNT(*) AS count,
          AVG(overall_score) AS overall,
          AVG(coverage_score) AS coverage,
          AVG(precision_score) AS precision,
          AVG(grounding_score) AS grounding,
          AVG(identity_score) AS identity,
          SUM(p0_issue_count) AS p0_issues
        FROM reviewer_audits
        WHERE status = 'completed'
        """
    ).fetchone()
    claim_network = {
        "claim_clusters": int(conn.execute("SELECT COUNT(*) AS count FROM claim_clusters").fetchone()["count"] or 0),
        "agreement_edges": int(conn.execute("SELECT COUNT(*) AS count FROM agreement_edges").fetchone()["count"] or 0),
        "disagreement_edges": int(conn.execute("SELECT COUNT(*) AS count FROM disagreement_edges").fetchone()["count"] or 0),
    }
    waiting_context = int(
        conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM jobs
            JOIN segments ON segments.id = jobs.target_id
            WHERE jobs.job_type = 'label_segment'
              AND jobs.status = 'pending'
              AND json_extract(jobs.payload_json, '$.label_pack') = 'ai_discourse_v3_1'
              AND NOT EXISTS (
                SELECT 1
                FROM episode_context_runs
                WHERE episode_context_runs.episode_id = segments.episode_id
                  AND episode_context_runs.label_pack = 'ai_discourse_v3_1'
                  AND episode_context_runs.model = 'gpt-5.5'
                  AND episode_context_runs.status = 'completed'
              )
            """
        ).fetchone()["count"]
    )
    return {
        "candidate_concepts": candidate_status,
        "concept_aliases": alias_status,
        "shift_signals_by_type": signal_type_counts,
        "burst_terms": burst_terms,
        "actor_stance_changes": actor_stance_changes,
        "v3_audit_count": int(audit["count"] or 0),
        "v3_avg_audit_score": round(float(audit["avg_score"] or 0), 3) if audit["avg_score"] is not None else 0,
        "v3_1_audit_count": int(v31_audit["count"] or 0),
        "v3_1_avg_audit_score": round(float(v31_audit["avg_score"] or 0), 3) if v31_audit["avg_score"] is not None else 0,
        "v3_1_scale_state": "blocked_until_gpt55_full_episode_pilot_passes",
        "episode_context_runs": episode_context_runs,
        "reviewer_audits": reviewer_status,
        "reviewer_audit_scores": {
            "completed": int(reviewer_scores["count"] or 0),
            "overall": round(float(reviewer_scores["overall"] or 0), 2) if reviewer_scores["overall"] is not None else 0,
            "coverage": round(float(reviewer_scores["coverage"] or 0), 2) if reviewer_scores["coverage"] is not None else 0,
            "precision": round(float(reviewer_scores["precision"] or 0), 2) if reviewer_scores["precision"] is not None else 0,
            "grounding": round(float(reviewer_scores["grounding"] or 0), 2) if reviewer_scores["grounding"] is not None else 0,
            "identity_graph_usefulness": round(float(reviewer_scores["identity"] or 0), 2) if reviewer_scores["identity"] is not None else 0,
            "p0_issues": int(reviewer_scores["p0_issues"] or 0),
        },
        "claim_network": claim_network,
        "v3_1_labels_waiting_on_episode_context": waiting_context,
        "raw_speaker_mentions_by_resolution": raw_speaker_status,
        "raw_actor_mentions_by_resolution": raw_actor_status,
        "unresolved_identity_mentions": int(raw_speaker_status.get("unresolved", 0)) + int(raw_actor_status.get("unresolved", 0)),
    }


def _transcript_preparation_metrics(conn) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT artifact_type, status, COUNT(*) AS count, AVG(boilerplate_ratio) AS avg_boilerplate_ratio, AVG(quality_score) AS avg_quality_score
            FROM transcript_preparations
            GROUP BY artifact_type, status
            ORDER BY artifact_type, status
            """
        ).fetchall()
    ]


def _transcript_acquisition_metrics(conn) -> dict[str, Any]:
    status_counts = {
        row["status"]: int(row["count"])
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM transcript_acquisition_status GROUP BY status").fetchall()
    }
    attempt_counts = [
        dict(row)
        for row in conn.execute(
            """
            SELECT method, status, COUNT(*) AS count
            FROM transcript_acquisition_attempts
            GROUP BY method, status
            ORDER BY count DESC, method, status
            """
        ).fetchall()
    ]
    transcription_runs = [
        dict(row)
        for row in conn.execute(
            """
            SELECT provider, status, COUNT(*) AS count
            FROM transcription_runs
            GROUP BY provider, status
            ORDER BY provider, status
            """
        ).fetchall()
    ]
    transcript_source_kinds = [
        dict(row)
        for row in conn.execute(
            """
            SELECT source_kind, status, COUNT(*) AS count
            FROM transcripts
            GROUP BY source_kind, status
            ORDER BY source_kind, status
            """
        ).fetchall()
    ]
    return {
        "status_counts": status_counts,
        "attempt_counts": attempt_counts,
        "transcription_runs": transcription_runs,
        "transcript_source_kinds": transcript_source_kinds,
    }


def _source_yield(conn) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """
            WITH source_segment_words AS (
              SELECT source_id, SUM(word_count) AS segment_words
              FROM (
                SELECT DISTINCT segments.source_id, segments.id, segments.word_count
                FROM segments
              )
              GROUP BY source_id
            )
            SELECT
              sources.name AS source_name,
              sources.category,
              COUNT(DISTINCT transcripts.id) AS transcripts,
              COUNT(DISTINCT segments.id) AS segments,
              COUNT(DISTINCT labels.id) AS labels,
              COUNT(DISTINCT coded_observations.id) AS observations,
              COUNT(DISTINCT discourse_events.id) AS discourse_events,
              ROUND(CASE WHEN COALESCE(source_segment_words.segment_words, 0) > 0 THEN
                COUNT(DISTINCT coded_observations.id) * 1000.0 / source_segment_words.segment_words
              ELSE 0 END, 2) AS observations_per_1000_words
            FROM sources
            LEFT JOIN episodes ON episodes.source_id = sources.id
            LEFT JOIN transcripts ON transcripts.episode_id = episodes.id
            LEFT JOIN segments ON segments.transcript_id = transcripts.id
            LEFT JOIN labels ON labels.segment_id = segments.id
            LEFT JOIN coded_observations ON coded_observations.label_id = labels.id
            LEFT JOIN discourse_events ON discourse_events.label_id = labels.id
            LEFT JOIN source_segment_words ON source_segment_words.source_id = sources.id
            GROUP BY sources.id
            HAVING transcripts > 0 OR labels > 0 OR observations > 0
            ORDER BY discourse_events DESC, observations DESC, labels DESC, segments DESC
            LIMIT 25
            """
        ).fetchall()
    ]


def write_snapshot(conn, output: str | Path | None = None) -> Path:
    path = Path(output).expanduser().resolve() if output else exports_dir() / "observer-snapshot.json"
    write_text_atomic(path, json.dumps(build_snapshot(conn), ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return path


def publish_snapshot(snapshot_path: str | Path, *, url: str | None = None, token: str | None = None) -> dict[str, Any]:
    target_url = url or os.environ.get("RAILWAY_UI_URL")
    ingest_token = token or os.environ.get("RAILWAY_UI_INGEST_TOKEN")
    if not target_url:
        raise ValueError("Missing Railway UI URL. Set RAILWAY_UI_URL or pass --url.")
    if not ingest_token:
        raise ValueError("Missing ingest token. Set RAILWAY_UI_INGEST_TOKEN or pass --token.")
    endpoint = target_url.rstrip("/") + "/ingest-snapshot"
    body = Path(snapshot_path).read_bytes()
    request = urllib.request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {ingest_token}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        text = response.read().decode("utf-8", errors="replace")
        return {"status": response.status, "body": text}


def _safe_job(row) -> dict[str, Any]:
    return {
        "lane": row["lane"],
        "job_type": row["job_type"],
        "status": row["status"],
        "priority": row["priority"],
        "attempts": row["attempts"],
        "lease_owner": _public_worker(row["lease_owner"]),
        "leased_until": row["leased_until"],
        "updated_at": row["updated_at"],
        "error": _public_text(row["error"]),
    }


def _recent_artifacts(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    artifacts = []
    for item in sorted(path.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True)[:50]:
        if item.is_file():
            artifacts.append({"name": item.name, "bytes": item.stat().st_size})
    return artifacts


def _safe_label_run(row) -> dict[str, Any]:
    prompt_path = Path(row["prompt_path"]) if row["prompt_path"] else None
    output_path = Path(row["output_path"]) if row["output_path"] else None
    return {
        "label_pack": row["label_pack"],
        "model": row["model"],
        "status": row["status"],
        "prompt_artifact": _artifact_kind(prompt_path),
        "output_artifact": _artifact_kind(output_path),
        "output_exists": output_path.exists() if output_path else False,
        "claimed_at": row["claimed_at"],
        "completed_at": row["completed_at"],
        "error": _public_text(row["error"]),
    }


def _public_text(value: Any, *, limit: int = 160) -> str | None:
    if value is None:
        return None
    text = str(value)
    text = text.replace(str(root()), "<project>")
    text = re.sub(r"/Users/[^\s\"']+", "<local-path>", text)
    text = re.sub(r"https?://[^\s\"')]+", "<url>", text)
    text = re.sub(r"\b(ep|seg|tr|lbl|run|aud)_[a-f0-9]{12,}\b", r"\1_<id>", text)
    return text[:limit]


def _public_worker(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return re.sub(r"[^A-Za-z0-9_.:-]+", "", text)[:80]


def _artifact_kind(path: Path | None) -> str | None:
    if not path:
        return None
    return path.suffix.lstrip(".") or "artifact"
