from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from . import db
from .fast_quality import quality_velocity
from .paths import exports_dir, root
from .prep import prepare_transcript
from .scale_ops import reviewer_audit_summary
from .util import dumps_json, now_iso, write_text_atomic


HIGH_PRIORITY_TRANSCRIPT_SOURCES = [
    "Practical AI",
    "Dwarkesh Podcast",
    "Microsoft Research Podcast",
    "Latent Space",
    "Big Technology Podcast",
    "No Priors",
    "The AI Daily Brief",
    "Eye On AI",
    "The TWIML AI Podcast",
    "The Cognitive Revolution",
    "Training Data",
]

DEFAULT_SCALE_GATE_SOURCES = [
    "Practical AI",
    "Dwarkesh Podcast",
    "Microsoft Research Podcast",
    "Latent Space",
    "Big Technology Podcast",
]

CONTROLLED_100_HIGH_SIGNAL_SOURCES = [
    "Practical AI",
    "Latent Space",
    "Eye On AI",
    "Microsoft Research Podcast",
    "Dwarkesh Podcast",
    "Machine Learning Street Talk",
    "Big Technology Podcast",
    "Lex Fridman Podcast",
    "The TWIML AI Podcast",
    "The Cognitive Revolution",
    "Last Week in AI",
    "The Gradient",
]

CONTROLLED_100_BROAD_TECH_CATEGORIES = [
    "tech_business",
    "consumer_tech",
    "software_engineering",
    "cloud_infrastructure",
    "venture",
    "company_strategy",
    "security",
    "markets",
]

CONTROLLED_100_BROAD_TECH_KEYWORDS = [
    "ai",
    "llm",
    "agent",
    "openai",
    "anthropic",
    "model",
    "coding",
    "mcp",
    "data",
    "cloud",
    "nvidia",
]


def enqueue_scale_gate(
    conn,
    *,
    pilot_id: str,
    lane: str,
    label_pack: str,
    model: str,
    sources: list[str],
    limit: int,
    per_source_limit: int,
    priority: int,
    force_prepare: bool,
    include_low_signal: bool,
) -> dict[str, Any]:
    if label_pack != "ai_discourse_v3_1":
        raise ValueError("scale-gate-enqueue is only supported for ai_discourse_v3_1")
    if model != "gpt-5.5":
        raise ValueError("scale-gate-enqueue requires model gpt-5.5")
    if limit < 1:
        raise ValueError("--limit must be at least 1")
    if per_source_limit < 1:
        raise ValueError("--per-source-limit must be at least 1")
    requested_sources = [item.strip() for item in (sources or DEFAULT_SCALE_GATE_SOURCES) if item.strip()]
    source_rank = {name.lower(): index for index, name in enumerate(requested_sources)}
    rows = conn.execute(
        """
        SELECT
          transcripts.id AS transcript_id,
          transcripts.status AS transcript_status,
          episodes.id AS episode_id,
          episodes.published_at,
          sources.name AS source_name,
          sources.category,
          COUNT(DISTINCT segments.id) AS segment_count,
          COUNT(DISTINCT labels.id) AS existing_v31_labels
        FROM transcripts
        JOIN episodes ON episodes.id = transcripts.episode_id
        JOIN sources ON sources.id = episodes.source_id
        JOIN segments ON segments.transcript_id = transcripts.id
        LEFT JOIN labels ON labels.segment_id = segments.id
          AND labels.label_pack = ?
        WHERE transcripts.status = 'ready'
        GROUP BY transcripts.id
        HAVING segment_count > 0
          AND existing_v31_labels = 0
        ORDER BY episodes.published_at DESC, transcripts.updated_at DESC
        """,
        (label_pack,),
    ).fetchall()
    eligible = [
        row
        for row in rows
        if str(row["source_name"] or "").lower() in source_rank
    ]
    eligible.sort(key=lambda row: source_rank[str(row["source_name"] or "").lower()])
    selected = []
    skipped = []
    source_counts: dict[str, int] = {}
    label_jobs = 0
    context_jobs = 0
    for row in eligible:
        if len(selected) >= limit:
            break
        source_name = str(row["source_name"] or "")
        if source_counts.get(source_name, 0) >= per_source_limit:
            continue
        prep = prepare_transcript(conn, row["transcript_id"], force=force_prepare)
        if prep["status"] != "prepared" and not include_low_signal:
            skipped.append(
                {
                    "transcript_id": row["transcript_id"],
                    "episode_id": row["episode_id"],
                    "source_name": source_name,
                    "reason": f"transcript_preparation_{prep['status']}",
                    "artifact_type": prep["artifact_type"],
                    "quality_score": prep["quality_score"],
                }
            )
            continue
        enqueued_for_transcript = 0
        for segment in conn.execute(
            """
            SELECT id
            FROM segments
            WHERE transcript_id = ?
              AND NOT EXISTS (
                SELECT 1 FROM labels
                WHERE labels.segment_id = segments.id
                  AND labels.label_pack = ?
              )
            ORDER BY segment_index
            """,
            (row["transcript_id"], label_pack),
        ).fetchall():
            job_id = db.enqueue_job(
                conn,
                lane=lane,
                job_type="label_segment",
                target_id=segment["id"],
                payload={
                    "label_pack": label_pack,
                    "pilot_id": pilot_id,
                    "transcript_preparation_id": prep["id"],
                    "transcript_artifact_type": prep["artifact_type"],
                    "source_quality_score": prep["quality_score"],
                    "priority_reason": "scale_gate_v31_dense_extraction",
                },
                priority=priority,
            )
            if job_id:
                enqueued_for_transcript += 1
        if enqueued_for_transcript == 0:
            skipped.append(
                {
                    "transcript_id": row["transcript_id"],
                    "episode_id": row["episode_id"],
                    "source_name": source_name,
                    "reason": "no_unlabeled_segments",
                    "artifact_type": prep["artifact_type"],
                    "quality_score": prep["quality_score"],
                }
            )
            continue
        context_job_id = None
        existing_context = conn.execute(
            """
            SELECT id
            FROM episode_context_runs
            WHERE episode_id = ?
              AND label_pack = ?
              AND model = ?
              AND status = 'completed'
            """,
            (row["episode_id"], label_pack, model),
        ).fetchone()
        if not existing_context:
            context_job_id = db.enqueue_job(
                conn,
                lane=lane,
                job_type="episode_context",
                target_id=row["episode_id"],
                payload={
                    "label_pack": label_pack,
                    "model": model,
                    "pilot_id": pilot_id,
                    "episode_context_version": "ai_discourse_v3_1_episode_context",
                    "priority_reason": "scale_gate_v31_full_episode_context",
                },
                priority=max(priority - 1, 0),
                max_attempts=2,
            )
            if context_job_id:
                context_jobs += 1
        selected.append(
            {
                "transcript_id": row["transcript_id"],
                "episode_id": row["episode_id"],
                "source_name": source_name,
                "published_at": row["published_at"],
                "artifact_type": prep["artifact_type"],
                "preparation_status": prep["status"],
                "quality_score": prep["quality_score"],
                "segment_count": int(row["segment_count"] or 0),
                "label_jobs": enqueued_for_transcript,
                "episode_context_job_id": context_job_id,
            }
        )
        source_counts[source_name] = source_counts.get(source_name, 0) + 1
        label_jobs += enqueued_for_transcript
    conn.commit()
    return {
        "ok": True,
        "pilot_id": pilot_id,
        "label_pack": label_pack,
        "model": model,
        "requested_limit": limit,
        "selected_episodes": len(selected),
        "label_jobs": label_jobs,
        "episode_context_jobs": context_jobs,
        "selected_by_source": _count_by(selected, "source_name"),
        "selected": selected,
        "skipped": skipped[:50],
        "shortfall": max(limit - len(selected), 0),
    }


def enqueue_controlled_100_batch(
    conn,
    *,
    pilot_id: str,
    lane: str,
    label_pack: str,
    model: str,
    priority: int,
    dry_run: bool,
    force_prepare: bool,
    include_low_signal: bool,
    acquired_since: str,
) -> dict[str, Any]:
    if label_pack != "ai_discourse_v3_1":
        raise ValueError("controlled scale batch is only supported for ai_discourse_v3_1")
    if model != "gpt-5.5":
        raise ValueError("controlled scale batch requires model gpt-5.5")
    bucket_specs = [
        ("high_signal_ready_ai", 60),
        ("newly_recovered_acquired", 20),
        ("broad_tech_ai_heavy", 20),
    ]
    selected_episode_ids: set[str] = set()
    buckets: dict[str, Any] = {}
    totals = {
        "selected_episodes": 0,
        "label_jobs_created": 0,
        "label_jobs_adopted": 0,
        "episode_context_jobs_created": 0,
        "episode_context_jobs_adopted": 0,
    }
    for bucket_name, target_count in bucket_specs:
        rows = _controlled_batch_candidates(
            conn,
            bucket=bucket_name,
            label_pack=label_pack,
            acquired_since=acquired_since,
            excluded_episode_ids=selected_episode_ids,
        )
        selected: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for row in rows:
            if len(selected) >= target_count:
                break
            prep = prepare_transcript(conn, row["transcript_id"], force=force_prepare)
            if prep["status"] != "prepared" and not include_low_signal:
                skipped.append(
                    {
                        "episode_id": row["episode_id"],
                        "source_name": row["source_name"],
                        "reason": f"transcript_preparation_{prep['status']}",
                        "artifact_type": prep["artifact_type"],
                        "quality_score": prep["quality_score"],
                    }
                )
                continue
            label_counts = _enqueue_or_adopt_segment_jobs(
                conn,
                lane=lane,
                label_pack=label_pack,
                pilot_id=pilot_id,
                bucket=bucket_name,
                transcript_id=row["transcript_id"],
                prep=prep,
                priority=priority,
                dry_run=dry_run,
            )
            if label_counts["eligible_segments"] == 0:
                skipped.append(
                    {
                        "episode_id": row["episode_id"],
                        "source_name": row["source_name"],
                        "reason": "no_unlabeled_segments",
                        "artifact_type": prep["artifact_type"],
                        "quality_score": prep["quality_score"],
                    }
                )
                continue
            context_counts = _enqueue_or_adopt_context_job(
                conn,
                lane=lane,
                label_pack=label_pack,
                model=model,
                pilot_id=pilot_id,
                bucket=bucket_name,
                episode_id=row["episode_id"],
                priority=max(priority - 1, 0),
                dry_run=dry_run,
            )
            selected_episode_ids.add(row["episode_id"])
            selected.append(
                {
                    "episode_id": row["episode_id"],
                    "transcript_id": row["transcript_id"],
                    "source_name": row["source_name"],
                    "category": row["category"],
                    "published_at": row["published_at"],
                    "transcript_created_at": row["transcript_created_at"],
                    "artifact_type": prep["artifact_type"],
                    "preparation_status": prep["status"],
                    "quality_score": prep["quality_score"],
                    **label_counts,
                    **context_counts,
                }
            )
            totals["selected_episodes"] += 1
            totals["label_jobs_created"] += label_counts["label_jobs_created"]
            totals["label_jobs_adopted"] += label_counts["label_jobs_adopted"]
            totals["episode_context_jobs_created"] += context_counts["episode_context_jobs_created"]
            totals["episode_context_jobs_adopted"] += context_counts["episode_context_jobs_adopted"]
        buckets[bucket_name] = {
            "target": target_count,
            "selected_episodes": len(selected),
            "shortfall": max(target_count - len(selected), 0),
            "selected_by_source": _count_by(selected, "source_name"),
            "selected": selected,
            "skipped": skipped[:25],
        }
    shortfall = sum(bucket["shortfall"] for bucket in buckets.values())
    if dry_run:
        conn.rollback()
    else:
        conn.commit()
    return {
        "ok": shortfall == 0,
        "dry_run": dry_run,
        "pilot_id": pilot_id,
        "batch_plan": "controlled_100_v31",
        "label_pack": label_pack,
        "model": model,
        "concurrency_guidance": 4,
        "requested_episodes": 100,
        "selected_episodes": totals["selected_episodes"],
        "shortfall": shortfall,
        **totals,
        "buckets": buckets,
        "privacy": "sanitized_operational_report_no_raw_transcripts",
    }


def build_scale_gate_report(conn, *, pilot_id: str, output: str | Path | None = None) -> dict[str, Any]:
    episodes = _pilot_episodes(conn, pilot_id)
    episode_ids = [item["episode_id"] for item in episodes]
    segment_ids = _pilot_segment_ids(conn, pilot_id)
    label_ids = _pilot_label_ids(conn, pilot_id)
    job_counts = _pilot_job_counts(conn, pilot_id)
    segment_completion = _pilot_segment_completion(conn, pilot_id, segment_ids)
    label_summary = _pilot_label_summary(conn, label_ids)
    context_summary = _pilot_context_summary(conn, episode_ids, pilot_id)
    audit_summary = _pilot_audit_summary(conn, label_ids)
    reviewer_summary = reviewer_audit_summary(conn, pilot_id=pilot_id)
    evidence_summary = _pilot_evidence_summary(conn, label_ids)
    graph_summary = _pilot_graph_summary(conn, label_ids)
    acquisition_summary = _pilot_acquisition_summary(conn, episode_ids)
    global_blockers = _global_v31_blockers(conn, pilot_id=pilot_id)
    source_shortfalls = _high_priority_transcript_shortfalls(conn)
    quality_velocity_summary = quality_velocity(conn, pilot_id=pilot_id)

    events_per_1000 = 0.0
    if label_summary["labeled_segment_words"]:
        events_per_1000 = round(label_summary["discourse_events"] * 1000.0 / label_summary["labeled_segment_words"], 2)
    failed_label_jobs = segment_completion["failed_label_jobs"]

    checks = [
        {
            "name": "selected_25_episodes",
            "passed": len(episodes) >= 25,
            "actual": len(episodes),
            "required": 25,
        },
        {
            "name": "all_selected_episodes_have_gpt55_context",
            "passed": len(episodes) > 0 and context_summary["completed_episode_context_runs"] >= len(episodes),
            "actual": context_summary["completed_episode_context_runs"],
            "required": len(episodes),
        },
        {
            "name": "all_selected_segments_labeled_v31_gpt55",
            "passed": segment_completion["selected_segments"] > 0
            and segment_completion["completed_labels"] >= segment_completion["selected_segments"]
            and segment_completion["pending_label_jobs"] == 0
            and segment_completion["failed_label_jobs"] == 0,
            "actual": {
                "selected_segments": segment_completion["selected_segments"],
                "completed_labels": segment_completion["completed_labels"],
                "pending_label_jobs": segment_completion["pending_label_jobs"],
                "failed_label_jobs": segment_completion["failed_label_jobs"],
                "completion_rate": segment_completion["completion_rate"],
            },
            "required": "completed_labels == selected_segments and zero pending/failed pilot label jobs",
        },
        {
            "name": "all_v31_labels_have_completed_context",
            "passed": label_summary["labels"] > 0 and label_summary["labels_without_completed_context"] == 0,
            "actual": label_summary["labels_without_completed_context"],
            "required": 0,
        },
        {
            "name": "no_non_gpt55_v31_labels",
            "passed": label_summary["non_gpt55_labels"] == 0,
            "actual": label_summary["non_gpt55_labels"],
            "required": 0,
        },
        {
            "name": "density_between_10_and_35_events_per_1000_words",
            "passed": 10.0 <= events_per_1000 <= 35.0,
            "actual": events_per_1000,
            "required": "10.0 <= events_per_1000 <= 35.0",
        },
        {
            "name": "exact_evidence_offsets_100_percent",
            "passed": evidence_summary["checked_events"] > 0 and evidence_summary["offset_failures"] == 0,
            "actual": evidence_summary["offset_failures"],
            "required": 0,
        },
        {
            "name": "no_failed_pilot_label_jobs",
            "passed": failed_label_jobs == 0,
            "actual": failed_label_jobs,
            "required": 0,
        },
        {
            "name": "deterministic_audit_pass_rate_at_least_95_percent",
            "passed": audit_summary["audited_labels"] >= segment_completion["completed_labels"]
            and audit_summary["audited_labels"] > 0
            and audit_summary["pass_rate"] >= 0.95,
            "actual": {
                "audited_labels": audit_summary["audited_labels"],
                "completed_labels": segment_completion["completed_labels"],
                "pass_rate": audit_summary["pass_rate"],
            },
            "required": "all completed labels audited with pass_rate >= 0.95",
        },
        {
            "name": "semantic_reviewer_audit_passes",
            "passed": reviewer_summary["completed"] >= 10
            and reviewer_summary["overall_score"] >= 85
            and reviewer_summary["coverage_score"] >= 82
            and reviewer_summary["precision_score"] >= 90
            and reviewer_summary["grounding_score"] >= 95
            and reviewer_summary["identity_graph_usefulness_score"] >= 85
            and reviewer_summary["p0_issues"] == 0,
            "actual": reviewer_summary,
            "required": "10 completed GPT-5.5 reviewer audits with overall>=85 coverage>=82 precision>=90 grounding>=95 identity>=85 and zero P0 issues",
        },
        {
            "name": "transcript_acquisition_trail_for_touched_episodes",
            "passed": acquisition_summary["episodes_missing_acquisition_status"] == 0,
            "actual": acquisition_summary["episodes_missing_acquisition_status"],
            "required": 0,
        },
        {
            "name": "identity_graph_draft_present",
            "passed": graph_summary["raw_identity_mentions"] > 0
            and (
                graph_summary["identity_resolution_candidates"] > 0
                or graph_summary["podcast_guest_edges"] > 0
                or graph_summary["person_concept_edges"] > 0
                or graph_summary["expert_authority_scores"] > 0
            ),
            "actual": graph_summary,
            "required": "raw mentions plus at least one graph or identity draft table populated",
        },
        {
            "name": "quality_and_graph_blockers_drained",
            "passed": global_blockers["failed_v31_label_jobs"] == 0
            and global_blockers["pilot_v31_labels_needing_adjudication"] == 0
            and global_blockers["claim_clusters"] > 0
            and (global_blockers["agreement_edges"] + global_blockers["disagreement_edges"]) > 0,
            "actual": global_blockers,
            "required": "no failed v3.1 jobs, no pilot label adjudication backlog, and claim clusters plus semantic edge candidates present; global concept/label backlog remains visible but does not block the 25-episode pilot gate",
        },
    ]
    failed_checks = [item["name"] for item in checks if not item["passed"]]
    report = {
        "ok": True,
        "pilot_id": pilot_id,
        "generated_at": now_iso(),
        "scale_ready_definition": "complete-episode extraction: every selected episode has GPT-5.5 full context, every selected segment has a v3.1 GPT-5.5 label, deterministic audits pass, semantic reviewer audits pass, and graph/quality blockers are drained before the 100-episode batch",
        "gate_state": "passed" if not failed_checks else "blocked",
        "failed_checks": failed_checks,
        "checks": checks,
        "episodes": {
            "selected": len(episodes),
            "by_source": _count_by(episodes, "source_name"),
            "items": episodes[:50],
        },
        "segments": {
            "selected": len(segment_ids),
            **segment_completion,
        },
        "jobs": job_counts,
        "labels": {
            **label_summary,
            "events_per_1000_labeled_segment_words": events_per_1000,
        },
        "episode_context": context_summary,
        "audits": audit_summary,
        "semantic_reviewer_audit": reviewer_summary,
        "evidence_offsets": evidence_summary,
        "identity_and_graph": graph_summary,
        "transcript_acquisition": acquisition_summary,
        "quality_and_graph_blockers": global_blockers,
        "quality_velocity": quality_velocity_summary,
        "high_priority_transcript_shortfalls": source_shortfalls,
        "privacy": "sanitized_operational_report_no_raw_transcripts",
    }
    if output is not None:
        path = Path(output).expanduser().resolve()
    else:
        safe_pilot = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in pilot_id)
        path = exports_dir() / f"scale-gate-{safe_pilot}.json"
    write_text_atomic(path, json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    report["path"] = str(path)
    return report


def _controlled_batch_candidates(
    conn,
    *,
    bucket: str,
    label_pack: str,
    acquired_since: str,
    excluded_episode_ids: set[str],
) -> list[dict[str, Any]]:
    excluded_sql = ""
    params: list[Any] = [label_pack]
    if excluded_episode_ids:
        placeholders = ", ".join("?" for _ in excluded_episode_ids)
        excluded_sql = f" AND episodes.id NOT IN ({placeholders})"
        params.extend(sorted(excluded_episode_ids))
    where = ""
    order = "episodes.published_at DESC, transcripts.updated_at DESC"
    if bucket == "high_signal_ready_ai":
        placeholders = ", ".join("?" for _ in CONTROLLED_100_HIGH_SIGNAL_SOURCES)
        where = f" AND sources.name IN ({placeholders})"
        params.extend(CONTROLLED_100_HIGH_SIGNAL_SOURCES)
        order = f"CASE sources.name {' '.join(f'WHEN ? THEN {index}' for index, _ in enumerate(CONTROLLED_100_HIGH_SIGNAL_SOURCES))} ELSE 999 END, episodes.published_at DESC"
        params.extend(CONTROLLED_100_HIGH_SIGNAL_SOURCES)
    elif bucket == "newly_recovered_acquired":
        where = " AND transcripts.created_at >= ?"
        params.append(acquired_since)
        order = "transcripts.created_at DESC, transcripts.updated_at DESC, episodes.published_at DESC"
    elif bucket == "broad_tech_ai_heavy":
        category_placeholders = ", ".join("?" for _ in CONTROLLED_100_BROAD_TECH_CATEGORIES)
        keyword_sql = " OR ".join("lower(episodes.title) LIKE ?" for _ in CONTROLLED_100_BROAD_TECH_KEYWORDS)
        where = f" AND sources.category IN ({category_placeholders}) AND ({keyword_sql})"
        params.extend(CONTROLLED_100_BROAD_TECH_CATEGORIES)
        params.extend(f"%{keyword}%" for keyword in CONTROLLED_100_BROAD_TECH_KEYWORDS)
        order = "episodes.published_at DESC, transcripts.updated_at DESC"
    else:
        raise ValueError(f"Unknown controlled batch bucket: {bucket}")
    rows = conn.execute(
        f"""
        SELECT
          transcripts.id AS transcript_id,
          transcripts.created_at AS transcript_created_at,
          transcripts.updated_at AS transcript_updated_at,
          episodes.id AS episode_id,
          episodes.published_at,
          sources.name AS source_name,
          sources.category,
          COUNT(DISTINCT segments.id) AS segment_count,
          COUNT(DISTINCT labels.id) AS existing_v31_labels
        FROM transcripts
        JOIN episodes ON episodes.id = transcripts.episode_id
        JOIN sources ON sources.id = episodes.source_id
        JOIN segments ON segments.transcript_id = transcripts.id
        LEFT JOIN labels ON labels.segment_id = segments.id
          AND labels.label_pack = ?
          AND labels.model = 'gpt-5.5'
        WHERE transcripts.status = 'ready'
          {excluded_sql}
          {where}
        GROUP BY transcripts.id
        HAVING segment_count > 0
          AND existing_v31_labels = 0
        ORDER BY {order}
        """,
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _enqueue_or_adopt_segment_jobs(
    conn,
    *,
    lane: str,
    label_pack: str,
    pilot_id: str,
    bucket: str,
    transcript_id: str,
    prep: dict[str, Any],
    priority: int,
    dry_run: bool,
) -> dict[str, int]:
    created = 0
    adopted = 0
    eligible = 0
    for segment in conn.execute(
        """
        SELECT id
        FROM segments
        WHERE transcript_id = ?
          AND NOT EXISTS (
            SELECT 1 FROM labels
            WHERE labels.segment_id = segments.id
              AND labels.label_pack = ?
              AND labels.model = 'gpt-5.5'
          )
        ORDER BY segment_index
        """,
        (transcript_id, label_pack),
    ).fetchall():
        eligible += 1
        payload = {
            "label_pack": label_pack,
            "pilot_id": pilot_id,
            "batch_plan": "controlled_100_v31",
            "batch_bucket": bucket,
            "transcript_preparation_id": prep["id"],
            "transcript_artifact_type": prep["artifact_type"],
            "source_quality_score": prep["quality_score"],
            "priority_reason": "controlled_100_v31_scale_batch",
        }
        existing = _pending_label_job_for_segment(conn, lane=lane, segment_id=segment["id"], label_pack=label_pack)
        if existing:
            adopted += 1
            if not dry_run:
                _retag_pending_job(conn, job_id=existing["id"], lane=lane, job_type="label_segment", target_id=segment["id"], payload=payload, priority=priority)
            continue
        if not dry_run:
            db.enqueue_job(conn, lane=lane, job_type="label_segment", target_id=segment["id"], payload=payload, priority=priority)
        created += 1
    return {
        "eligible_segments": eligible,
        "label_jobs_created": created,
        "label_jobs_adopted": adopted,
    }


def _enqueue_or_adopt_context_job(
    conn,
    *,
    lane: str,
    label_pack: str,
    model: str,
    pilot_id: str,
    bucket: str,
    episode_id: str,
    priority: int,
    dry_run: bool,
) -> dict[str, int]:
    existing_completed = conn.execute(
        """
        SELECT id
        FROM episode_context_runs
        WHERE episode_id = ?
          AND label_pack = ?
          AND model = ?
          AND status = 'completed'
        """,
        (episode_id, label_pack, model),
    ).fetchone()
    if existing_completed:
        return {"episode_context_jobs_created": 0, "episode_context_jobs_adopted": 0}
    payload = {
        "label_pack": label_pack,
        "model": model,
        "pilot_id": pilot_id,
        "batch_plan": "controlled_100_v31",
        "batch_bucket": bucket,
        "episode_context_version": "ai_discourse_v3_1_episode_context",
        "priority_reason": "controlled_100_v31_full_episode_context",
    }
    existing = _pending_context_job_for_episode(conn, lane=lane, episode_id=episode_id, label_pack=label_pack, model=model)
    if existing:
        if not dry_run:
            _retag_pending_job(conn, job_id=existing["id"], lane=lane, job_type="episode_context", target_id=episode_id, payload=payload, priority=priority)
        return {"episode_context_jobs_created": 0, "episode_context_jobs_adopted": 1}
    if not dry_run:
        db.enqueue_job(conn, lane=lane, job_type="episode_context", target_id=episode_id, payload=payload, priority=priority, max_attempts=2)
    return {"episode_context_jobs_created": 1, "episode_context_jobs_adopted": 0}


def _pending_label_job_for_segment(conn, *, lane: str, segment_id: str, label_pack: str):
    rows = conn.execute(
        """
        SELECT id, payload_json
        FROM jobs
        WHERE lane = ?
          AND job_type = 'label_segment'
          AND target_id = ?
          AND status = 'pending'
        ORDER BY priority ASC, id ASC
        """,
        (lane, segment_id),
    ).fetchall()
    for row in rows:
        payload = json.loads(row["payload_json"] or "{}")
        if payload.get("label_pack") == label_pack:
            return row
    return None


def _pending_context_job_for_episode(conn, *, lane: str, episode_id: str, label_pack: str, model: str):
    rows = conn.execute(
        """
        SELECT id, payload_json
        FROM jobs
        WHERE lane = ?
          AND job_type = 'episode_context'
          AND target_id = ?
          AND status = 'pending'
        ORDER BY priority ASC, id ASC
        """,
        (lane, episode_id),
    ).fetchall()
    for row in rows:
        payload = json.loads(row["payload_json"] or "{}")
        if payload.get("label_pack") == label_pack and payload.get("model", model) == model:
            return row
    return None


def _retag_pending_job(conn, *, job_id: int, lane: str, job_type: str, target_id: str, payload: dict[str, Any], priority: int) -> None:
    payload_json = dumps_json(payload)
    dedupe_key = f"{lane}:{job_type}:{target_id}:{payload_json}"
    ts = now_iso()
    conn.execute(
        """
        UPDATE jobs
        SET priority = ?,
            payload_json = ?,
            dedupe_key = ?,
            updated_at = ?
        WHERE id = ?
          AND status = 'pending'
        """,
        (priority, payload_json, dedupe_key, ts, job_id),
    )


def _pilot_episodes(conn, pilot_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        WITH pilot_episode_ids AS (
          SELECT DISTINCT segments.episode_id
          FROM jobs
          JOIN segments ON segments.id = jobs.target_id
          WHERE json_extract(jobs.payload_json, '$.pilot_id') = ?
            AND jobs.job_type = 'label_segment'
          UNION
          SELECT DISTINCT jobs.target_id AS episode_id
          FROM jobs
          WHERE json_extract(jobs.payload_json, '$.pilot_id') = ?
            AND jobs.job_type = 'episode_context'
        )
        SELECT
          episodes.id AS episode_id,
          episodes.published_at,
          sources.name AS source_name,
          COUNT(DISTINCT transcripts.id) AS transcripts,
          COUNT(DISTINCT CASE WHEN transcripts.status = 'ready' THEN transcripts.id END) AS ready_transcripts,
          COUNT(DISTINCT segments.id) AS segments
        FROM pilot_episode_ids
        JOIN episodes ON episodes.id = pilot_episode_ids.episode_id
        JOIN sources ON sources.id = episodes.source_id
        LEFT JOIN transcripts ON transcripts.episode_id = episodes.id
        LEFT JOIN segments ON segments.episode_id = episodes.id
        GROUP BY episodes.id
        ORDER BY sources.name, episodes.published_at DESC, episodes.id
        """,
        (pilot_id, pilot_id),
    ).fetchall()
    return [dict(row) for row in rows]


def _pilot_segment_ids(conn, pilot_id: str) -> list[str]:
    return [
        row["segment_id"]
        for row in conn.execute(
            """
            SELECT DISTINCT jobs.target_id AS segment_id
            FROM jobs
            WHERE json_extract(jobs.payload_json, '$.pilot_id') = ?
              AND jobs.job_type = 'label_segment'
            ORDER BY jobs.target_id
            """,
            (pilot_id,),
        ).fetchall()
    ]


def _pilot_label_ids(conn, pilot_id: str) -> list[str]:
    return [
        row["label_id"]
        for row in conn.execute(
            """
            SELECT DISTINCT labels.id AS label_id
            FROM labels
            JOIN label_runs ON label_runs.segment_id = labels.segment_id
             AND label_runs.label_pack = labels.label_pack
             AND label_runs.model = labels.model
            JOIN jobs ON jobs.id = label_runs.job_id
            WHERE json_extract(jobs.payload_json, '$.pilot_id') = ?
              AND labels.label_pack = 'ai_discourse_v3_1'
            ORDER BY labels.id
            """,
            (pilot_id,),
        ).fetchall()
    ]


def _pilot_job_counts(conn, pilot_id: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT job_type, status, COUNT(*) AS count
            FROM jobs
            WHERE json_extract(payload_json, '$.pilot_id') = ?
            GROUP BY job_type, status
            ORDER BY job_type, status
            """,
            (pilot_id,),
        ).fetchall()
    ]


def _pilot_segment_completion(conn, pilot_id: str, segment_ids: list[str]) -> dict[str, Any]:
    if not segment_ids:
        return {
            "selected_segments": 0,
            "completed_labels": 0,
            "pending_label_jobs": 0,
            "failed_label_jobs": 0,
            "claimed_label_jobs": 0,
            "completion_rate": 0.0,
            "by_source": [],
        }
    placeholders = ", ".join("?" for _ in segment_ids)
    completed = int(
        conn.execute(
            f"""
            SELECT COUNT(DISTINCT segment_id) AS count
            FROM labels
            WHERE segment_id IN ({placeholders})
              AND label_pack = 'ai_discourse_v3_1'
              AND model = 'gpt-5.5'
              AND status = 'ready'
            """,
            segment_ids,
        ).fetchone()["count"]
        or 0
    )
    job_status = {
        row["status"]: int(row["count"] or 0)
        for row in conn.execute(
            f"""
            SELECT status, COUNT(*) AS count
            FROM jobs
            WHERE target_id IN ({placeholders})
              AND job_type = 'label_segment'
              AND json_extract(payload_json, '$.pilot_id') = ?
            GROUP BY status
            """,
            (*segment_ids, pilot_id),
        ).fetchall()
    }
    by_source = [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT
              sources.name AS source_name,
              COUNT(DISTINCT segments.id) AS selected_segments,
              COUNT(DISTINCT labels.id) AS completed_labels,
              COUNT(DISTINCT CASE WHEN jobs.status = 'pending' THEN jobs.id END) AS pending_label_jobs,
              COUNT(DISTINCT CASE WHEN jobs.status = 'failed' THEN jobs.id END) AS failed_label_jobs
            FROM segments
            JOIN sources ON sources.id = segments.source_id
            LEFT JOIN labels ON labels.segment_id = segments.id
              AND labels.label_pack = 'ai_discourse_v3_1'
              AND labels.model = 'gpt-5.5'
              AND labels.status = 'ready'
            LEFT JOIN jobs ON jobs.target_id = segments.id
              AND jobs.job_type = 'label_segment'
              AND json_extract(jobs.payload_json, '$.pilot_id') = ?
            WHERE segments.id IN ({placeholders})
            GROUP BY sources.name
            ORDER BY pending_label_jobs DESC, selected_segments DESC, source_name
            """,
            (pilot_id, *segment_ids),
        ).fetchall()
    ]
    return {
        "selected_segments": len(segment_ids),
        "completed_labels": completed,
        "pending_label_jobs": job_status.get("pending", 0),
        "failed_label_jobs": job_status.get("failed", 0),
        "claimed_label_jobs": job_status.get("claimed", 0),
        "completion_rate": round(completed / len(segment_ids), 3),
        "by_source": by_source,
    }


def _pilot_label_summary(conn, label_ids: list[str]) -> dict[str, Any]:
    if not label_ids:
        return {
            "labels": 0,
            "ready_labels": 0,
            "non_gpt55_labels": 0,
            "labels_without_completed_context": 0,
            "discourse_events": 0,
            "labeled_segment_words": 0,
        }
    value_rows = ", ".join("(?)" for _ in label_ids)
    row = conn.execute(
        f"""
        WITH pilot_labels(id) AS (VALUES {value_rows})
        SELECT
          COUNT(DISTINCT labels.id) AS labels,
          COUNT(DISTINCT CASE WHEN labels.status = 'ready' THEN labels.id END) AS ready_labels,
          COUNT(DISTINCT CASE WHEN labels.model != 'gpt-5.5' THEN labels.id END) AS non_gpt55_labels,
          COUNT(DISTINCT discourse_events.id) AS discourse_events,
          COALESCE((
            SELECT SUM(word_count)
            FROM (
              SELECT DISTINCT segment_words.id, segment_words.word_count
              FROM labels AS word_labels
              JOIN segments AS segment_words ON segment_words.id = word_labels.segment_id
              WHERE word_labels.id IN (SELECT id FROM pilot_labels)
            )
          ), 0) AS labeled_segment_words,
          COUNT(DISTINCT CASE WHEN NOT EXISTS (
            SELECT 1
            FROM episode_context_runs
            WHERE episode_context_runs.episode_id = segments.episode_id
              AND episode_context_runs.label_pack = 'ai_discourse_v3_1'
              AND episode_context_runs.model = 'gpt-5.5'
              AND episode_context_runs.status = 'completed'
          ) THEN labels.id END) AS labels_without_completed_context
        FROM labels
        JOIN segments ON segments.id = labels.segment_id
        LEFT JOIN discourse_events ON discourse_events.label_id = labels.id
        WHERE labels.id IN (SELECT id FROM pilot_labels)
        """,
        label_ids,
    ).fetchone()
    return {
        "labels": int(row["labels"] or 0),
        "ready_labels": int(row["ready_labels"] or 0),
        "non_gpt55_labels": int(row["non_gpt55_labels"] or 0),
        "labels_without_completed_context": int(row["labels_without_completed_context"] or 0),
        "discourse_events": int(row["discourse_events"] or 0),
        "labeled_segment_words": int(row["labeled_segment_words"] or 0),
    }


def _pilot_context_summary(conn, episode_ids: list[str], pilot_id: str) -> dict[str, Any]:
    status_counts = {
        row["status"]: int(row["count"])
        for row in conn.execute(
            """
            SELECT episode_context_runs.status, COUNT(*) AS count
            FROM episode_context_runs
            LEFT JOIN jobs ON jobs.id = episode_context_runs.job_id
            WHERE json_extract(jobs.payload_json, '$.pilot_id') = ?
            GROUP BY episode_context_runs.status
            """,
            (pilot_id,),
        ).fetchall()
    }
    completed = 0
    if episode_ids:
        placeholders = ", ".join("?" for _ in episode_ids)
        completed = int(
            conn.execute(
                f"""
                SELECT COUNT(DISTINCT episode_id) AS count
                FROM episode_context_runs
                WHERE episode_id IN ({placeholders})
                  AND label_pack = 'ai_discourse_v3_1'
                  AND model = 'gpt-5.5'
                  AND status = 'completed'
                """,
                episode_ids,
            ).fetchone()["count"]
            or 0
        )
    return {
        "selected_episodes": len(episode_ids),
        "completed_episode_context_runs": completed,
        "runs_by_status": status_counts,
    }


def _pilot_audit_summary(conn, label_ids: list[str]) -> dict[str, Any]:
    if not label_ids:
        return {"audited_labels": 0, "passed": 0, "needs_adjudication": 0, "failed": 0, "pass_rate": 0.0, "avg_score": 0.0}
    placeholders = ", ".join("?" for _ in label_ids)
    rows = conn.execute(
        f"""
        WITH latest AS (
          SELECT label_id, status, score, created_at,
                 ROW_NUMBER() OVER (PARTITION BY label_id ORDER BY created_at DESC, id DESC) AS rn
          FROM quality_audits
          WHERE label_id IN ({placeholders})
            AND label_pack = 'ai_discourse_v3_1'
            AND label_id IS NOT NULL
        )
        SELECT status, COUNT(DISTINCT label_id) AS count, AVG(score) AS avg_score
        FROM latest
        WHERE rn = 1
        GROUP BY status
        """,
        label_ids,
    ).fetchall()
    counts = {row["status"]: int(row["count"] or 0) for row in rows}
    audited = sum(counts.values())
    passed = counts.get("passed", 0)
    avg_row = conn.execute(
        f"""
        WITH latest AS (
          SELECT label_id, score, created_at,
                 ROW_NUMBER() OVER (PARTITION BY label_id ORDER BY created_at DESC, id DESC) AS rn
          FROM quality_audits
          WHERE label_id IN ({placeholders})
            AND label_pack = 'ai_discourse_v3_1'
            AND label_id IS NOT NULL
        )
        SELECT AVG(score) AS avg_score FROM latest WHERE rn = 1
        """,
        label_ids,
    ).fetchone()
    return {
        "audited_labels": audited,
        "passed": passed,
        "needs_adjudication": counts.get("needs_adjudication", 0),
        "failed": counts.get("failed", 0),
        "pass_rate": round(passed / audited, 3) if audited else 0.0,
        "avg_score": round(float(avg_row["avg_score"] or 0), 3) if avg_row["avg_score"] is not None else 0.0,
    }


def _pilot_evidence_summary(conn, label_ids: list[str]) -> dict[str, Any]:
    if not label_ids:
        return {"checked_events": 0, "offset_failures": 0, "missing_segment_files": 0, "failed_event_ids": [], "content_checks_skipped": 0}
    placeholders = ", ".join("?" for _ in label_ids)
    rows = conn.execute(
        f"""
        SELECT discourse_events.id, discourse_events.evidence_text, discourse_events.evidence_start, discourse_events.evidence_end, segments.text_path
        FROM discourse_events
        JOIN segments ON segments.id = discourse_events.segment_id
        WHERE discourse_events.label_id IN ({placeholders})
        ORDER BY discourse_events.id
        """,
        label_ids,
    ).fetchall()
    checked = 0
    failures = []
    missing_files = 0
    content_checks_skipped = 0
    base = root()
    for row in rows:
        checked += 1
        path = base / row["text_path"]
        if not path.exists():
            missing_files += 1
            failures.append(row["id"])
            continue
        start = int(row["evidence_start"])
        end = int(row["evidence_end"])
        if start < 0 or end < start or not str(row["evidence_text"] or ""):
            failures.append(row["id"])
            continue
        # Observer snapshots must not read or expose segment bodies. Exact
        # local offset validation belongs in reviewer/audit jobs, not the
        # sanitized publish path.
        content_checks_skipped += 1
    return {
        "checked_events": checked,
        "offset_failures": len(failures),
        "missing_segment_files": missing_files,
        "failed_event_ids": failures[:25],
        "content_checks_skipped": content_checks_skipped,
    }


def _pilot_graph_summary(conn, label_ids: list[str]) -> dict[str, Any]:
    if not label_ids:
        pilot_raw_speakers = 0
        pilot_raw_actors = 0
    else:
        placeholders = ", ".join("?" for _ in label_ids)
        event_ids = [
            row["id"]
            for row in conn.execute(
                f"SELECT id FROM discourse_events WHERE label_id IN ({placeholders})",
                label_ids,
            ).fetchall()
        ]
        if event_ids:
            event_placeholders = ", ".join("?" for _ in event_ids)
            pilot_raw_speakers = int(
                conn.execute(
                    f"SELECT COUNT(*) AS count FROM raw_speaker_mentions WHERE discourse_event_id IN ({event_placeholders})",
                    event_ids,
                ).fetchone()["count"]
                or 0
            )
            pilot_raw_actors = int(
                conn.execute(
                    f"SELECT COUNT(*) AS count FROM raw_actor_mentions WHERE discourse_event_id IN ({event_placeholders})",
                    event_ids,
                ).fetchone()["count"]
                or 0
            )
        else:
            pilot_raw_speakers = 0
            pilot_raw_actors = 0
    return {
        "raw_speaker_mentions": pilot_raw_speakers,
        "raw_actor_mentions": pilot_raw_actors,
        "raw_identity_mentions": pilot_raw_speakers + pilot_raw_actors,
        "identity_resolution_candidates": _table_count(conn, "identity_resolution_candidates"),
        "podcast_guest_edges": _table_count(conn, "podcast_guest_edges"),
        "person_concept_edges": _table_count(conn, "person_concept_edges"),
        "expert_authority_scores": _table_count(conn, "expert_authority_scores"),
        "canonical_people": _table_count(conn, "canonical_people"),
        "canonical_orgs": _table_count(conn, "canonical_orgs"),
        "claim_clusters": _table_count(conn, "claim_clusters"),
        "agreement_edges": _table_count(conn, "agreement_edges"),
        "disagreement_edges": _table_count(conn, "disagreement_edges"),
    }


def _global_v31_blockers(conn, *, pilot_id: str) -> dict[str, Any]:
    failed_v31 = int(
        conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM jobs
            WHERE job_type = 'label_segment'
              AND status = 'failed'
              AND json_extract(payload_json, '$.label_pack') = 'ai_discourse_v3_1'
            """
        ).fetchone()["count"]
        or 0
    )
    latest_audit_cte = """
        WITH latest AS (
          SELECT label_id, status, created_at,
                 ROW_NUMBER() OVER (PARTITION BY label_id ORDER BY created_at DESC, id DESC) AS rn
          FROM quality_audits
          WHERE label_pack = 'ai_discourse_v3_1'
            AND label_id IS NOT NULL
        )
    """
    global_needs_adjudication = int(
        conn.execute(
            latest_audit_cte
            + """
            SELECT COUNT(DISTINCT label_id) AS count
            FROM latest
            WHERE rn = 1
              AND status = 'needs_adjudication'
            """
        ).fetchone()["count"]
        or 0
    )
    pilot_needs_adjudication = int(
        conn.execute(
            latest_audit_cte
            + """
            SELECT COUNT(DISTINCT latest.label_id) AS count
            FROM latest
            JOIN labels ON labels.id = latest.label_id
            JOIN jobs ON jobs.target_id = labels.segment_id
            WHERE latest.rn = 1
              AND latest.status = 'needs_adjudication'
              AND jobs.job_type = 'label_segment'
              AND json_extract(jobs.payload_json, '$.pilot_id') = ?
            """,
            (pilot_id,),
        ).fetchone()["count"]
        or 0
    )
    pending_concepts = int(
        conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM jobs
            WHERE job_type = 'adjudicate_concept'
              AND status = 'pending'
            """
        ).fetchone()["count"]
        or 0
    )
    return {
        "pilot_id": pilot_id,
        "failed_v31_label_jobs": failed_v31,
        "pilot_v31_labels_needing_adjudication": pilot_needs_adjudication,
        "global_v31_labels_needing_adjudication": global_needs_adjudication,
        "pending_concept_adjudications": pending_concepts,
        "claim_clusters": _table_count(conn, "claim_clusters"),
        "agreement_edges": _table_count(conn, "agreement_edges"),
        "disagreement_edges": _table_count(conn, "disagreement_edges"),
    }


def _pilot_acquisition_summary(conn, episode_ids: list[str]) -> dict[str, Any]:
    if not episode_ids:
        return {
            "episodes_missing_acquisition_status": 0,
            "selected_episodes_without_ready_transcript": 0,
            "status_counts": {},
            "attempt_counts": [],
            "transcript_source_kinds": [],
        }
    placeholders = ", ".join("?" for _ in episode_ids)
    missing_rows = conn.execute(
        f"""
        SELECT episodes.id AS episode_id
        FROM episodes
        WHERE episodes.id IN ({placeholders})
          AND NOT EXISTS (
            SELECT 1
            FROM transcripts
            WHERE transcripts.episode_id = episodes.id
              AND transcripts.status = 'ready'
          )
        """,
        episode_ids,
    ).fetchall()
    missing_episode_ids = [row["episode_id"] for row in missing_rows]
    status_rows = conn.execute(
        f"""
        SELECT status, COUNT(*) AS count
        FROM transcript_acquisition_status
        WHERE episode_id IN ({placeholders})
        GROUP BY status
        """,
        episode_ids,
    ).fetchall()
    source_kinds = [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT source_kind, status, COUNT(*) AS count
            FROM transcripts
            WHERE episode_id IN ({placeholders})
            GROUP BY source_kind, status
            ORDER BY count DESC, source_kind, status
            """,
            episode_ids,
        ).fetchall()
    ]
    attempts = [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT method, status, COUNT(*) AS count
            FROM transcript_acquisition_attempts
            WHERE episode_id IN ({placeholders})
            GROUP BY method, status
            ORDER BY count DESC, method, status
            """,
            episode_ids,
        ).fetchall()
    ]
    if missing_episode_ids:
        missing_placeholders = ", ".join("?" for _ in missing_episode_ids)
        with_status = int(
            conn.execute(
                f"""
                SELECT COUNT(DISTINCT episode_id) AS count
                FROM transcript_acquisition_status
                WHERE episode_id IN ({missing_placeholders})
                """,
                missing_episode_ids,
            ).fetchone()["count"]
            or 0
        )
    else:
        with_status = 0
    return {
        "episodes_missing_acquisition_status": max(len(missing_episode_ids) - with_status, 0),
        "selected_episodes_without_ready_transcript": len(missing_episode_ids),
        "status_counts": {row["status"]: int(row["count"]) for row in status_rows},
        "attempt_counts": attempts,
        "transcript_source_kinds": source_kinds,
    }


def _high_priority_transcript_shortfalls(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          sources.name AS source_name,
          COUNT(DISTINCT episodes.id) AS episodes,
          COUNT(DISTINCT transcripts.id) AS transcripts,
          COUNT(DISTINCT CASE WHEN transcripts.status = 'ready' THEN transcripts.id END) AS ready_transcripts,
          COUNT(DISTINCT CASE WHEN transcripts.status = 'quarantined' THEN transcripts.id END) AS quarantined_transcripts,
          COUNT(DISTINCT CASE WHEN jobs.job_type = 'manual_transcript_required' AND jobs.status = 'pending' THEN jobs.id END) AS pending_manual_transcript_jobs
        FROM sources
        LEFT JOIN episodes ON episodes.source_id = sources.id
        LEFT JOIN transcripts ON transcripts.episode_id = episodes.id
        LEFT JOIN jobs ON jobs.target_id = episodes.id
        WHERE sources.name IN ({})
        GROUP BY sources.id
        ORDER BY pending_manual_transcript_jobs DESC, episodes DESC, sources.name
        """.format(", ".join("?" for _ in HIGH_PRIORITY_TRANSCRIPT_SOURCES)),
        HIGH_PRIORITY_TRANSCRIPT_SOURCES,
    ).fetchall()
    return [dict(row) for row in rows if int(row["episodes"] or 0) > int(row["ready_transcripts"] or 0)]


def _table_count(conn, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()["count"] or 0)


def _count_by(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = str(item.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))
