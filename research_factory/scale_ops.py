from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

from . import db
from .paths import corpus_dir, runs_dir
from .util import dumps_json, now_iso, read_text, stable_id, write_text_atomic
from .worker import episode_context_for_episode, submit_label_output


REVIEWER_AUDIT_SCHEMA_VERSION = "reviewer_audit_v1"


def retry_failed_labels(
    conn,
    *,
    label_pack: str,
    mode: str,
    pilot_id: str | None = None,
    limit: int | None = None,
    worker_id: str = "retry-failed-labels",
) -> dict[str, Any]:
    if mode not in {"repair-or-requeue", "requeue-only", "repair-only"}:
        raise ValueError("--mode must be repair-or-requeue, requeue-only, or repair-only")
    sql = """
        SELECT *
        FROM jobs
        WHERE job_type = 'label_segment'
          AND status = 'failed'
          AND json_extract(payload_json, '$.label_pack') = ?
    """
    params: list[Any] = [label_pack]
    if pilot_id:
        sql += " AND json_extract(payload_json, '$.pilot_id') = ?"
        params.append(pilot_id)
    sql += " ORDER BY updated_at ASC, id ASC"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    stats: dict[str, Any] = {
        "ok": True,
        "label_pack": label_pack,
        "mode": mode,
        "pilot_id": pilot_id,
        "inspected": len(rows),
        "submitted_after_repair": 0,
        "requeued": 0,
        "left_failed": 0,
        "details": [],
    }
    for row in rows:
        payload = json.loads(row["payload_json"] or "{}")
        repaired = False
        if mode in {"repair-or-requeue", "repair-only"}:
            output_path = payload.get("output_path")
            if output_path and Path(output_path).expanduser().exists():
                try:
                    _claim_failed_job_for_repair(conn, row["id"], worker_id=worker_id)
                    submit_label_output(
                        conn,
                        job_id=int(row["id"]),
                        output_json_path=output_path,
                        worker_id=worker_id,
                        allow_expired=True,
                    )
                    repaired = True
                    stats["submitted_after_repair"] += 1
                    stats["details"].append({"job_id": row["id"], "result": "submitted_after_repair"})
                except Exception as exc:  # noqa: BLE001 - surfaced in operator report.
                    _restore_failed_job(conn, row["id"], str(exc))
                    stats["details"].append({"job_id": row["id"], "repair_error": str(exc)[:500]})
        if repaired:
            continue
        if mode in {"repair-or-requeue", "requeue-only"}:
            _requeue_failed_label_job(conn, row, reason="retry-failed-labels repair-or-requeue")
            stats["requeued"] += 1
            stats["details"].append({"job_id": row["id"], "result": "requeued"})
        else:
            stats["left_failed"] += 1
    conn.commit()
    return stats


def _claim_failed_job_for_repair(conn, job_id: int, *, worker_id: str) -> None:
    ts = now_iso()
    leased_until = (dt.datetime.fromisoformat(ts) + dt.timedelta(minutes=30)).isoformat()
    conn.execute(
        """
        UPDATE jobs
        SET status = 'claimed',
            lease_owner = ?,
            leased_until = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (worker_id, leased_until, ts, job_id),
    )


def _restore_failed_job(conn, job_id: int, error: str) -> None:
    ts = now_iso()
    conn.execute(
        """
        UPDATE jobs
        SET status = 'failed',
            lease_owner = NULL,
            leased_until = NULL,
            error = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (error, ts, job_id),
    )


def _requeue_failed_label_job(conn, row, *, reason: str) -> None:
    ts = now_iso()
    payload = json.loads(row["payload_json"] or "{}")
    old_handoff = {
        key: payload.pop(key)
        for key in ["prompt_path", "output_path", "label_run_id"]
        if key in payload
    }
    if old_handoff.get("label_run_id"):
        conn.execute(
            """
            UPDATE label_runs
            SET status = 'failed',
                error = COALESCE(error, ?),
                updated_at = ?
            WHERE id = ?
              AND status IN ('claimed', 'failed')
            """,
            (reason, ts, old_handoff["label_run_id"]),
        )
    conn.execute(
        """
        UPDATE jobs
        SET status = 'pending',
            payload_json = ?,
            lease_owner = NULL,
            leased_until = NULL,
            attempts = CASE WHEN attempts >= max_attempts THEN max_attempts - 1 ELSE attempts END,
            error = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (dumps_json(payload), reason, ts, row["id"]),
    )


def acquisition_funnel(conn, *, source: str | None = None, limit: int = 25) -> dict[str, Any]:
    params: list[Any] = []
    source_filter = ""
    if source:
        source_filter = "WHERE sources.name = ?"
        params.append(source)
    rows = conn.execute(
        f"""
        SELECT
          sources.name AS source_name,
          sources.category,
          COUNT(DISTINCT episodes.id) AS episodes,
          COUNT(DISTINCT transcripts.id) AS transcripts,
          COUNT(DISTINCT CASE WHEN transcripts.status = 'ready' THEN transcripts.id END) AS ready_transcripts,
          COUNT(DISTINCT CASE WHEN transcripts.status = 'quarantined' THEN transcripts.id END) AS quarantined_transcripts,
          COUNT(DISTINCT segments.id) AS segments,
          COUNT(DISTINCT CASE WHEN labels.label_pack = 'ai_discourse_v3_1' THEN labels.id END) AS v31_labels,
          COUNT(DISTINCT CASE WHEN jobs.job_type = 'label_segment' AND jobs.status = 'pending' THEN jobs.id END) AS pending_label_jobs,
          COUNT(DISTINCT CASE WHEN jobs.job_type = 'label_segment' AND jobs.status = 'failed' THEN jobs.id END) AS failed_label_jobs,
          COUNT(DISTINCT CASE WHEN manual_jobs.job_type = 'manual_transcript_required' AND manual_jobs.status = 'pending' THEN manual_jobs.id END) AS pending_manual_transcript_jobs
        FROM sources
        LEFT JOIN episodes ON episodes.source_id = sources.id
        LEFT JOIN transcripts ON transcripts.episode_id = episodes.id
        LEFT JOIN segments ON segments.episode_id = episodes.id
        LEFT JOIN labels ON labels.segment_id = segments.id
        LEFT JOIN jobs ON jobs.target_id = segments.id
        LEFT JOIN jobs AS manual_jobs ON manual_jobs.target_id = episodes.id
        {source_filter}
        GROUP BY sources.id
        ORDER BY pending_manual_transcript_jobs DESC, quarantined_transcripts DESC, pending_label_jobs DESC, episodes DESC
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()
    status_rows = conn.execute(
        f"""
        SELECT sources.name AS source_name, transcript_acquisition_status.status, COUNT(*) AS count
        FROM transcript_acquisition_status
        JOIN sources ON sources.id = transcript_acquisition_status.source_id
        {source_filter}
        GROUP BY sources.name, transcript_acquisition_status.status
        ORDER BY sources.name, count DESC
        """,
        params,
    ).fetchall()
    status_by_source: dict[str, dict[str, int]] = {}
    for row in status_rows:
        status_by_source.setdefault(row["source_name"], {})[row["status"]] = int(row["count"] or 0)
    items = []
    for row in rows:
        item = dict(row)
        item["acquisition_status"] = status_by_source.get(row["source_name"], {})
        items.append(item)
    return {"ok": True, "source": source, "limit": limit, "sources": items}


def create_reviewer_audits(
    conn,
    *,
    pilot_id: str,
    episodes: int,
    model: str,
    fresh: bool = False,
    mode: str = "full",
    patch_tag: str | None = None,
) -> dict[str, Any]:
    if model != "gpt-5.5":
        raise ValueError("reviewer-audit requires model gpt-5.5")
    if mode not in {"full", "targeted"}:
        raise ValueError("reviewer-audit mode must be full or targeted")
    selected = _select_reviewer_episodes(conn, pilot_id=pilot_id, limit=episodes, mode=mode)
    created = []
    existing = []
    refreshed = []
    focus = {
        "mode": mode,
        "patch_tag": patch_tag,
        "selection": "p0_p1_impacted_episodes" if mode == "targeted" else "stratified_full_gate",
    }
    for row in selected:
        audit_id = stable_id("reviewer_audit", pilot_id, row["episode_id"], model, mode, patch_tag or "unpatched", prefix="ra_")
        prior = conn.execute("SELECT * FROM reviewer_audits WHERE id = ?", (audit_id,)).fetchone()
        prompt_path = runs_dir() / "reviewer_prompts" / f"{audit_id}.md"
        output_path = runs_dir() / "reviewer_outputs" / f"{audit_id}.json"
        if prior and not fresh:
            existing.append({"audit_id": audit_id, "episode_id": row["episode_id"], "status": prior["status"]})
            continue
        prompt = _render_reviewer_prompt(conn, pilot_id=pilot_id, row=row)
        write_text_atomic(prompt_path, prompt)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        ts = now_iso()
        handoff = {
            "audit_id": audit_id,
            "episode_id": row["episode_id"],
            "source_name": row["source_name"],
            "prompt_path": str(prompt_path),
            "output_path": str(output_path),
        }
        if prior and fresh:
            conn.execute(
                """
                UPDATE reviewer_audits
                SET status = 'pending',
                    review_mode = ?,
                    patch_tag = ?,
                    focus_json = ?,
                    overall_score = 0,
                    coverage_score = 0,
                    precision_score = 0,
                    grounding_score = 0,
                    identity_score = 0,
                    product_market_score = 0,
                    missed_signals_count = 0,
                    false_or_weak_events_count = 0,
                    p0_issue_count = 0,
                    review_json = '{}',
                    prompt_path = ?,
                    output_path = ?,
                    updated_at = ?,
                    completed_at = NULL
                WHERE id = ?
                """,
                (mode, patch_tag, dumps_json(focus), str(prompt_path), str(output_path), ts, audit_id),
            )
            refreshed.append(handoff)
        else:
            conn.execute(
                """
                INSERT INTO reviewer_audits
                  (id, pilot_id, episode_id, model, review_mode, patch_tag, focus_json, status, prompt_path, output_path, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)
                """,
                (audit_id, pilot_id, row["episode_id"], model, mode, patch_tag, dumps_json(focus), str(prompt_path), str(output_path), ts, ts),
            )
            created.append(handoff)
    conn.commit()
    return {
        "ok": True,
        "pilot_id": pilot_id,
        "model": model,
        "mode": mode,
        "patch_tag": patch_tag,
        "requested_episodes": episodes,
        "selected_episodes": len(selected),
        "created": len(created),
        "refreshed": len(refreshed),
        "existing": len(existing),
        "audits": created + refreshed + existing,
        "summary": reviewer_audit_summary(conn, pilot_id=pilot_id, review_mode=mode),
    }


def reviewer_findings(conn, *, pilot_id: str, severities: list[str] | None = None, patch_tag: str | None = None) -> dict[str, Any]:
    severity_filter = {item.strip().upper() for item in (severities or []) if item.strip()}
    if not severity_filter:
        severity_filter = {"P0", "P1", "P2"}
    sql = """
        SELECT id, episode_id, status, review_json
        FROM reviewer_audits
        WHERE pilot_id = ?
          AND status = 'completed'
    """
    params: list[Any] = [pilot_id]
    if patch_tag:
        sql += " AND patch_tag = ?"
        params.append(patch_tag)
    sql += " ORDER BY completed_at, id"
    rows = conn.execute(sql, params).fetchall()
    event_segments = _event_segment_map(conn, pilot_id=pilot_id)
    findings: list[dict[str, Any]] = []
    impacted_segments: set[str] = set()
    impacted_episodes: set[str] = set()
    for row in rows:
        payload = json.loads(row["review_json"] or "{}")
        episode_id = row["episode_id"]
        for item in payload.get("missed_signals") or []:
            if not isinstance(item, dict):
                continue
            severity = _finding_severity(item.get("severity"), default="P1")
            if severity not in severity_filter:
                continue
            segment_id = _clean_optional_id(item.get("segment_id"))
            finding = {
                "reviewer_audit_id": row["id"],
                "episode_id": episode_id,
                "segment_id": segment_id,
                "discourse_event_id": None,
                "finding_type": "missed_signal",
                "severity": severity,
                "category": _finding_category(item.get("description")),
                "description": _short_text(item.get("description")),
                "has_evidence_excerpt": bool(item.get("evidence")),
            }
            findings.append(finding)
            impacted_episodes.add(episode_id)
            if segment_id:
                impacted_segments.add(segment_id)
        for item in payload.get("false_or_weak_events") or []:
            if not isinstance(item, dict):
                continue
            severity = _finding_severity(item.get("severity"), default="P1")
            if severity not in severity_filter:
                continue
            event_id = _clean_optional_id(item.get("discourse_event_id"))
            segment_id = event_segments.get(event_id or "")
            finding = {
                "reviewer_audit_id": row["id"],
                "episode_id": episode_id,
                "segment_id": segment_id,
                "discourse_event_id": event_id,
                "finding_type": "false_or_weak_event",
                "severity": severity,
                "category": _finding_category(item.get("reason")),
                "description": _short_text(item.get("reason")),
                "has_evidence_excerpt": False,
            }
            findings.append(finding)
            impacted_episodes.add(episode_id)
            if segment_id:
                impacted_segments.add(segment_id)
        for issue in payload.get("p0_issues") or []:
            if "P0" not in severity_filter:
                continue
            description = _short_text(issue)
            fallback_segments = _pilot_episode_segment_ids(conn, pilot_id=pilot_id, episode_id=episode_id)
            finding = {
                "reviewer_audit_id": row["id"],
                "episode_id": episode_id,
                "segment_id": None,
                "discourse_event_id": None,
                "finding_type": "p0_issue",
                "severity": "P0",
                "category": _finding_category(description),
                "description": description,
                "has_evidence_excerpt": False,
                "fallback_scope": "episode_segments",
                "fallback_segment_count": len(fallback_segments),
            }
            findings.append(finding)
            impacted_episodes.add(episode_id)
            impacted_segments.update(fallback_segments)
    counts: dict[str, int] = {}
    for finding in findings:
        key = f"{finding['severity']}:{finding['finding_type']}:{finding['category']}"
        counts[key] = counts.get(key, 0) + 1
    return {
        "ok": True,
        "pilot_id": pilot_id,
        "patch_tag": patch_tag,
        "severity_filter": sorted(severity_filter),
        "reviewer_audits_completed": len(rows),
        "findings": findings,
        "finding_counts": counts,
        "impacted_episode_ids": sorted(impacted_episodes),
        "impacted_segment_ids": sorted(impacted_segments),
        "privacy": "sanitized_no_transcript_text",
    }


def requeue_reviewed_segments(
    conn,
    *,
    pilot_id: str,
    mode: str,
    label_pack: str = "ai_discourse_v3_1",
    model: str = "gpt-5.5",
    priority: int = 5,
    worker_id: str = "reviewer-remediation",
    patch_tag: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    if mode not in {"failed-review-only", "reviewed-episodes"}:
        raise ValueError("--mode must be failed-review-only or reviewed-episodes")
    if label_pack != "ai_discourse_v3_1":
        raise ValueError("reviewer remediation currently supports ai_discourse_v3_1 only")
    if model != "gpt-5.5":
        raise ValueError("reviewer remediation requires model gpt-5.5")
    if mode == "reviewed-episodes":
        segment_ids = _reviewed_episode_segment_ids(conn, pilot_id=pilot_id)
        selection_reason = "all_segments_from_completed_reviewer_audits"
    else:
        finding_report = reviewer_findings(conn, pilot_id=pilot_id, severities=["P0", "P1"], patch_tag=patch_tag)
        segment_ids = finding_report["impacted_segment_ids"]
        selection_reason = "segments_linked_to_p0_p1_reviewer_findings"
        if patch_tag:
            selection_reason += f":{patch_tag}"
    remediation_id = stable_id("reviewer_remediation", pilot_id, mode, now_iso(), prefix="rr_")
    enqueued = []
    skipped = []
    for segment_id in segment_ids:
        existing_active = conn.execute(
            """
            SELECT id, status
            FROM jobs
            WHERE job_type = 'label_segment'
              AND target_id = ?
              AND json_extract(payload_json, '$.pilot_id') = ?
              AND status IN ('pending', 'claimed')
            ORDER BY id DESC
            LIMIT 1
            """,
            (segment_id, pilot_id),
        ).fetchone()
        if existing_active:
            skipped.append({"segment_id": segment_id, "reason": f"active_job_{existing_active['status']}", "job_id": existing_active["id"]})
            continue
        payload = {
            "label_pack": label_pack,
            "model": model,
            "pilot_id": pilot_id,
            "reviewer_remediation_id": remediation_id,
            "reviewer_remediation_mode": mode,
            "reviewer_remediation_reason": selection_reason,
            "worker_id": worker_id,
            "priority_reason": "semantic_reviewer_remediation_before_scale",
            "reviewer_patch_tag": patch_tag,
        }
        if dry_run:
            enqueued.append({"segment_id": segment_id, "job_id": None, "dry_run": True})
            continue
        job_id = db.enqueue_job(
            conn,
            lane="podcast",
            job_type="label_segment",
            target_id=segment_id,
            payload=payload,
            priority=priority,
            max_attempts=2,
        )
        enqueued.append({"segment_id": segment_id, "job_id": job_id})
    if not dry_run:
        conn.commit()
    return {
        "ok": True,
        "pilot_id": pilot_id,
        "mode": mode,
        "label_pack": label_pack,
        "model": model,
        "patch_tag": patch_tag,
        "priority": priority,
        "dry_run": dry_run,
        "remediation_id": remediation_id,
        "selected_segments": len(segment_ids),
        "enqueued": len(enqueued),
        "skipped": len(skipped),
        "jobs": enqueued[:200],
        "skipped_details": skipped[:100],
    }


def submit_reviewer_audit(conn, *, audit_id: str, output_json_path: str | Path) -> dict[str, Any]:
    audit = conn.execute("SELECT * FROM reviewer_audits WHERE id = ?", (audit_id,)).fetchone()
    if not audit:
        raise ValueError(f"Reviewer audit not found: {audit_id}")
    output_path = Path(output_json_path).expanduser().resolve()
    if audit["output_path"] and output_path != Path(audit["output_path"]).expanduser().resolve():
        raise ValueError("Output path does not match reviewer audit handoff")
    payload = json.loads(read_text(output_path))
    normalized = _validate_reviewer_output(payload, expected_pilot_id=audit["pilot_id"], expected_episode_id=audit["episode_id"])
    ts = now_iso()
    conn.execute(
        """
        UPDATE reviewer_audits
        SET status = 'completed',
            overall_score = ?,
            coverage_score = ?,
            precision_score = ?,
            grounding_score = ?,
            identity_score = ?,
            product_market_score = ?,
            missed_signals_count = ?,
            false_or_weak_events_count = ?,
            p0_issue_count = ?,
            review_json = ?,
            updated_at = ?,
            completed_at = ?
        WHERE id = ?
        """,
        (
            normalized["scores"]["overall"],
            normalized["scores"]["coverage"],
            normalized["scores"]["precision"],
            normalized["scores"]["grounding"],
            normalized["scores"]["identity_graph_usefulness"],
            normalized["scores"]["product_market_signal_usefulness"],
            len(normalized["missed_signals"]),
            len(normalized["false_or_weak_events"]),
            len(normalized["p0_issues"]),
            json.dumps(normalized, ensure_ascii=True, indent=2, sort_keys=True),
            ts,
            ts,
            audit_id,
        ),
    )
    conn.commit()
    return {"ok": True, "audit_id": audit_id, "summary": reviewer_audit_summary(conn, pilot_id=audit["pilot_id"], review_mode=audit["review_mode"])}


def reviewer_audit_summary(conn, *, pilot_id: str, review_mode: str = "full") -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM reviewer_audits
        WHERE pilot_id = ?
          AND review_mode = ?
        GROUP BY status
        """,
        (pilot_id, review_mode),
    ).fetchall()
    counts = {row["status"]: int(row["count"] or 0) for row in rows}
    score = conn.execute(
        """
        SELECT
          AVG(overall_score) AS overall,
          AVG(coverage_score) AS coverage,
          AVG(precision_score) AS precision,
          AVG(grounding_score) AS grounding,
          AVG(identity_score) AS identity_graph_usefulness,
          AVG(product_market_score) AS product_market_signal_usefulness,
          SUM(p0_issue_count) AS p0_issues,
          SUM(false_or_weak_events_count) AS false_or_weak_events,
          SUM(missed_signals_count) AS missed_signals
        FROM reviewer_audits
        WHERE pilot_id = ?
          AND review_mode = ?
          AND status = 'completed'
        """,
        (pilot_id, review_mode),
    ).fetchone()
    completed = counts.get("completed", 0)
    return {
        "requested_minimum_completed": 10,
        "review_mode": review_mode,
        "by_status": counts,
        "completed": completed,
        "overall_score": round(float(score["overall"] or 0), 2) if completed else 0.0,
        "coverage_score": round(float(score["coverage"] or 0), 2) if completed else 0.0,
        "precision_score": round(float(score["precision"] or 0), 2) if completed else 0.0,
        "grounding_score": round(float(score["grounding"] or 0), 2) if completed else 0.0,
        "identity_graph_usefulness_score": round(float(score["identity_graph_usefulness"] or 0), 2) if completed else 0.0,
        "product_market_signal_usefulness_score": round(float(score["product_market_signal_usefulness"] or 0), 2) if completed else 0.0,
        "p0_issues": int(score["p0_issues"] or 0) if completed else 0,
        "false_or_weak_events": int(score["false_or_weak_events"] or 0) if completed else 0,
        "missed_signals": int(score["missed_signals"] or 0) if completed else 0,
    }


def _select_reviewer_episodes(conn, *, pilot_id: str, limit: int, mode: str = "full") -> list[dict[str, Any]]:
    rows = [
        dict(row)
        for row in conn.execute(
            """
            WITH pilot_segments AS (
              SELECT DISTINCT jobs.target_id AS segment_id
              FROM jobs
              WHERE jobs.job_type = 'label_segment'
                AND json_extract(jobs.payload_json, '$.pilot_id') = ?
            ),
            latest_labels AS (
              SELECT id, segment_id
              FROM (
                SELECT
                  labels.id,
                  labels.segment_id,
                  ROW_NUMBER() OVER (
                    PARTITION BY labels.segment_id
                    ORDER BY labels.created_at DESC, labels.id DESC
                  ) AS rn
                FROM labels
                JOIN pilot_segments ON pilot_segments.segment_id = labels.segment_id
                WHERE labels.label_pack = 'ai_discourse_v3_1'
                  AND labels.status IN ('ready', 'completed')
              )
              WHERE rn = 1
            )
            SELECT
              episodes.id AS episode_id,
              episodes.title,
              episodes.published_at,
              sources.name AS source_name,
              COUNT(DISTINCT segments.id) AS selected_segments,
              COUNT(DISTINCT labels.id) AS completed_labels,
              COUNT(DISTINCT discourse_events.id) AS discourse_events
            FROM pilot_segments
            JOIN segments ON segments.id = pilot_segments.segment_id
            JOIN episodes ON episodes.id = segments.episode_id
            JOIN sources ON sources.id = segments.source_id
            LEFT JOIN labels ON labels.segment_id = segments.id
              AND labels.label_pack = 'ai_discourse_v3_1'
              AND labels.model = 'gpt-5.5'
            LEFT JOIN discourse_events ON discourse_events.label_id = labels.id
            GROUP BY episodes.id
            HAVING completed_labels > 0
            ORDER BY sources.name, episodes.published_at DESC, episodes.id
            """,
            (pilot_id,),
        ).fetchall()
    ]
    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_source.setdefault(row["source_name"], []).append(row)
    if mode == "targeted":
        findings = reviewer_findings(conn, pilot_id=pilot_id, severities=["P0", "P1"])
        impacted = set(findings.get("impacted_episode_ids") or [])
        targeted = [row for row in rows if row["episode_id"] in impacted]
        if targeted:
            return targeted[:limit]
    selected: list[dict[str, Any]] = []
    while len(selected) < limit and any(by_source.values()):
        for source_name in sorted(by_source):
            if by_source[source_name]:
                selected.append(by_source[source_name].pop(0))
                if len(selected) >= limit:
                    break
    return selected


def _render_reviewer_prompt(conn, *, pilot_id: str, row: dict[str, Any]) -> str:
    episode_context = episode_context_for_episode(conn, row["episode_id"])
    events = _episode_events(conn, pilot_id=pilot_id, episode_id=row["episode_id"])
    identities = _episode_identity_mentions(conn, episode_id=row["episode_id"])
    return "\n\n".join(
        [
            "You are a GPT-5.5 semantic reviewer auditing ai_discourse_v3_1 extraction quality.",
            "Read the full direct episode text in context and the extracted discourse events. Score whether the extraction missed important research signals, hallucinated weak events, or failed to support identity/product/market analysis.",
            "Important exclusion: sponsor/ad-read copy is intentionally excluded from durable ai_discourse_v3_1 discourse events. Do not count omitted sponsor/ad-read product claims as missed discourse signals unless the event was incorrectly included as core episode discourse.",
            "Return strict JSON only. Do not include the full transcript text or long excerpts in your output.",
            "Required JSON schema:",
            json.dumps(
                {
                    "schema_version": REVIEWER_AUDIT_SCHEMA_VERSION,
                    "pilot_id": pilot_id,
                    "episode_id": row["episode_id"],
                    "scores": {
                        "overall": "0-100",
                        "coverage": "0-100",
                        "precision": "0-100",
                        "grounding": "0-100",
                        "identity_graph_usefulness": "0-100",
                        "product_market_signal_usefulness": "0-100",
                    },
                    "missed_signals": [
                        {
                            "severity": "P0|P1|P2",
                            "description": "short description",
                            "evidence": "short exact excerpt only",
                            "segment_id": "id if known",
                        }
                    ],
                    "false_or_weak_events": [
                        {
                            "severity": "P0|P1|P2",
                            "discourse_event_id": "id if known",
                            "reason": "why unsupported or weak",
                        }
                    ],
                    "p0_issues": ["short issue descriptions"],
                    "summary": "short review summary",
                    "recommendation": "scale|scale_with_fixes|do_not_scale",
                },
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            ),
            "Acceptance thresholds: overall >= 85, coverage >= 82, precision >= 90, grounding >= 95, identity_graph_usefulness >= 85, and zero P0 false or weak events.",
            "Episode metadata:",
            json.dumps(
                {
                    **{key: row[key] for key in ["episode_id", "title", "published_at", "source_name", "selected_segments", "completed_labels"]},
                    "discourse_events": len(events),
                    "discourse_events_note": "Count reflects the embedded deduplicated reviewer event array.",
                },
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            ),
            "Extracted discourse events:",
            json.dumps(events, ensure_ascii=True, indent=2, sort_keys=True),
            "Identity mentions and graph evidence:",
            json.dumps(identities, ensure_ascii=True, indent=2, sort_keys=True),
            "Full direct episode text for private review:",
            episode_context["full_segmented_episode_text"],
        ]
    )


def _episode_events(conn, *, pilot_id: str, episode_id: str) -> list[dict[str, Any]]:
    rows = [
        dict(row)
        for row in conn.execute(
            """
            WITH pilot_segments AS (
              SELECT DISTINCT jobs.target_id AS segment_id
              FROM jobs
              WHERE jobs.job_type = 'label_segment'
                AND json_extract(jobs.payload_json, '$.pilot_id') = ?
            ),
            latest_labels AS (
              SELECT id, segment_id
              FROM (
                SELECT
                  labels.id,
                  labels.segment_id,
                  ROW_NUMBER() OVER (
                    PARTITION BY labels.segment_id
                    ORDER BY labels.created_at DESC, labels.id DESC
                  ) AS rn
                FROM labels
                JOIN pilot_segments ON pilot_segments.segment_id = labels.segment_id
                WHERE labels.label_pack = 'ai_discourse_v3_1'
                  AND labels.status IN ('ready', 'completed')
              )
              WHERE rn = 1
            )
            SELECT
              discourse_events.id AS discourse_event_id,
              discourse_events.segment_id,
              discourse_events.event_type,
              discourse_events.actor_name,
              discourse_events.actor_affiliation,
              discourse_events.candidate_concept,
              discourse_events.stance,
              discourse_events.claim_type,
              discourse_events.certainty,
              discourse_events.temporal_horizon,
              discourse_events.claim_text,
              discourse_events.evidence_text,
              discourse_events.evidence_start,
              discourse_events.evidence_end,
              discourse_events.confidence
            FROM discourse_events
            JOIN latest_labels ON latest_labels.id = discourse_events.label_id
            WHERE discourse_events.segment_id IN (SELECT id FROM segments WHERE episode_id = ?)
            ORDER BY discourse_events.segment_id, discourse_events.event_index
            """,
            (pilot_id, episode_id),
        ).fetchall()
    ]
    seen_keys: set[tuple[str, str, str, str, str]] = set()
    seen_rows: list[dict[str, Any]] = []
    deduped: list[dict[str, Any]] = []
    for row in rows:
        key = (
            _review_dedupe_key(row.get("event_type")),
            _review_dedupe_key(row.get("actor_name")),
            _review_dedupe_key(row.get("candidate_concept")),
            _review_dedupe_key(row.get("stance")),
            _review_claim_fingerprint(row.get("claim_text")),
        )
        if key in seen_keys or any(_review_events_are_near_duplicates(row, prior) for prior in seen_rows):
            continue
        seen_keys.add(key)
        seen_rows.append(row)
        deduped.append(row)
    return deduped


def _review_dedupe_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")


def _review_claim_fingerprint(value: Any) -> str:
    text = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())
    words = [word for word in text.split() if word not in {"the", "a", "an", "and", "or", "to", "of", "in", "on", "for", "that", "this", "is", "are", "was", "were"}]
    return " ".join(words[:18])


def _review_events_are_near_duplicates(left: dict[str, Any], right: dict[str, Any]) -> bool:
    if _review_dedupe_key(left.get("event_type")) != _review_dedupe_key(right.get("event_type")):
        return False
    if _review_dedupe_key(left.get("actor_name")) != _review_dedupe_key(right.get("actor_name")):
        return False
    left_evidence = _review_text_tokens(left.get("evidence_text"))
    right_evidence = _review_text_tokens(right.get("evidence_text"))
    if left_evidence and right_evidence and _token_overlap(left_evidence, right_evidence) >= 0.82:
        return True
    left_claim = _review_text_tokens(left.get("claim_text"))
    right_claim = _review_text_tokens(right.get("claim_text"))
    if left_claim and right_claim and _token_overlap(left_claim, right_claim) >= 0.62:
        return True
    return False


def _review_text_tokens(value: Any) -> set[str]:
    text = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())
    stop = {"the", "a", "an", "and", "or", "to", "of", "in", "on", "for", "that", "this", "is", "are", "was", "were", "it", "its", "as", "with", "by", "from"}
    return {word for word in text.split() if len(word) > 2 and word not in stop}


def _token_overlap(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left.intersection(right)) / min(len(left), len(right))


def _episode_identity_mentions(conn, *, episode_id: str) -> dict[str, list[dict[str, Any]]]:
    episode_person_ids = [
        row["canonical_entity_id"]
        for row in conn.execute(
            """
            SELECT DISTINCT canonical_entity_id
            FROM raw_actor_mentions
            WHERE episode_id = ?
              AND canonical_entity_type = 'person'
              AND canonical_entity_id IS NOT NULL
              AND resolution_status = 'candidate_match'
            """,
            (episode_id,),
        ).fetchall()
        if row["canonical_entity_id"]
    ]
    person_mentions = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
              person_person_mentions.id,
              person_person_mentions.segment_id,
              person_person_mentions.discourse_event_id,
              person_person_mentions.source_surface,
              source_people.display_name AS source_person,
              person_person_mentions.target_surface,
              target_people.display_name AS target_person,
              person_person_mentions.confidence
            FROM person_person_mentions
            LEFT JOIN canonical_people AS source_people
              ON source_people.id = person_person_mentions.source_person_id
            LEFT JOIN canonical_people AS target_people
              ON target_people.id = person_person_mentions.target_person_id
            WHERE person_person_mentions.episode_id = ?
            ORDER BY person_person_mentions.confidence DESC, person_person_mentions.created_at DESC
            LIMIT 100
            """,
            (episode_id,),
        ).fetchall()
    ]
    concept_edges = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
              canonical_people.display_name AS person,
              COALESCE(discourse_events.canonical_concept_name, discourse_events.candidate_concept) AS concept_name,
              discourse_events.event_type AS edge_type,
              COUNT(*) AS weight,
              ROUND(AVG(COALESCE(discourse_events.confidence, 0.6)), 3) AS confidence
            FROM raw_speaker_mentions
            JOIN canonical_people
              ON canonical_people.id = raw_speaker_mentions.canonical_person_id
            JOIN discourse_events
              ON discourse_events.id = raw_speaker_mentions.discourse_event_id
            WHERE raw_speaker_mentions.episode_id = ?
              AND raw_speaker_mentions.resolution_status = 'candidate_match'
              AND COALESCE(discourse_events.canonical_concept_name, discourse_events.candidate_concept, '') != ''
            GROUP BY raw_speaker_mentions.canonical_person_id, concept_name, discourse_events.event_type
            ORDER BY weight DESC, confidence DESC
            LIMIT 100
            """,
            (episode_id,),
        ).fetchall()
    ]
    return {
        "speakers": [
            dict(row)
            for row in conn.execute(
                """
                SELECT
                  raw_speaker_mentions.id,
                  raw_speaker_mentions.segment_id,
                  raw_speaker_mentions.discourse_event_id,
                  raw_speaker_mentions.surface_name,
                  raw_speaker_mentions.role,
                  raw_speaker_mentions.affiliation_surface,
                  raw_speaker_mentions.confidence,
                  raw_speaker_mentions.resolution_status,
                  raw_speaker_mentions.canonical_person_id,
                  canonical_people.display_name AS canonical_person
                FROM raw_speaker_mentions
                LEFT JOIN canonical_people
                  ON canonical_people.id = raw_speaker_mentions.canonical_person_id
                WHERE raw_speaker_mentions.episode_id = ?
                  AND raw_speaker_mentions.resolution_status != 'ignored_low_value'
                ORDER BY raw_speaker_mentions.created_at DESC
                LIMIT 100
                """,
                (episode_id,),
            ).fetchall()
        ],
        "actors": [
            dict(row)
            for row in conn.execute(
                """
                SELECT
                  raw_actor_mentions.id,
                  raw_actor_mentions.segment_id,
                  raw_actor_mentions.discourse_event_id,
                  raw_actor_mentions.mention_type,
                  raw_actor_mentions.surface_name,
                  raw_actor_mentions.speaker_surface,
                  raw_actor_mentions.reported_actor_surface,
                  raw_actor_mentions.role_context,
                  raw_actor_mentions.canonical_entity_type,
                  raw_actor_mentions.canonical_entity_id,
                  COALESCE(canonical_people.display_name, canonical_orgs.display_name, canonical_products.display_name, canonical_models.display_name) AS canonical_entity,
                  raw_actor_mentions.confidence,
                  raw_actor_mentions.resolution_status,
                  raw_actor_mentions.why_matters
                FROM raw_actor_mentions
                LEFT JOIN canonical_people
                  ON raw_actor_mentions.canonical_entity_type = 'person'
                 AND canonical_people.id = raw_actor_mentions.canonical_entity_id
                LEFT JOIN canonical_orgs
                  ON raw_actor_mentions.canonical_entity_type = 'org'
                 AND canonical_orgs.id = raw_actor_mentions.canonical_entity_id
                LEFT JOIN canonical_products
                  ON raw_actor_mentions.canonical_entity_type = 'product'
                 AND canonical_products.id = raw_actor_mentions.canonical_entity_id
                LEFT JOIN canonical_models
                  ON raw_actor_mentions.canonical_entity_type = 'model'
                 AND canonical_models.id = raw_actor_mentions.canonical_entity_id
                WHERE raw_actor_mentions.episode_id = ?
                  AND raw_actor_mentions.resolution_status != 'ignored_low_value'
                ORDER BY raw_actor_mentions.created_at DESC
                LIMIT 100
                """,
                (episode_id,),
            ).fetchall()
        ],
        "guest_edges": [
            dict(row)
            for row in conn.execute(
                """
                SELECT
                  podcast_guest_edges.id,
                  podcast_guest_edges.role,
                  podcast_guest_edges.confidence,
                  canonical_people.display_name AS canonical_person
                FROM podcast_guest_edges
                JOIN canonical_people
                  ON canonical_people.id = podcast_guest_edges.canonical_person_id
                WHERE podcast_guest_edges.episode_id = ?
                  AND EXISTS (
                    SELECT 1
                    FROM raw_speaker_mentions
                    WHERE raw_speaker_mentions.episode_id = podcast_guest_edges.episode_id
                      AND raw_speaker_mentions.canonical_person_id = podcast_guest_edges.canonical_person_id
                      AND raw_speaker_mentions.resolution_status = 'candidate_match'
                  )
                ORDER BY podcast_guest_edges.confidence DESC, canonical_people.display_name
                LIMIT 50
                """,
                (episode_id,),
            ).fetchall()
        ],
        "person_person_mentions": person_mentions,
        "person_concept_edges": concept_edges,
    }


def _validate_reviewer_output(payload: dict[str, Any], *, expected_pilot_id: str, expected_episode_id: str) -> dict[str, Any]:
    if payload.get("schema_version") != REVIEWER_AUDIT_SCHEMA_VERSION:
        raise ValueError(f"schema_version must be {REVIEWER_AUDIT_SCHEMA_VERSION}")
    if payload.get("pilot_id") != expected_pilot_id:
        raise ValueError("pilot_id does not match reviewer audit")
    if payload.get("episode_id") != expected_episode_id:
        raise ValueError("episode_id does not match reviewer audit")
    scores = payload.get("scores")
    if not isinstance(scores, dict):
        raise ValueError("scores must be an object")
    normalized_scores = {
        key: _score(scores.get(key), key)
        for key in [
            "overall",
            "coverage",
            "precision",
            "grounding",
            "identity_graph_usefulness",
            "product_market_signal_usefulness",
        ]
    }
    missed = _list_of_dicts(payload.get("missed_signals"), "missed_signals")
    weak = _list_of_dicts(payload.get("false_or_weak_events"), "false_or_weak_events")
    p0 = payload.get("p0_issues") or []
    if not isinstance(p0, list):
        raise ValueError("p0_issues must be a list")
    return {
        "schema_version": REVIEWER_AUDIT_SCHEMA_VERSION,
        "pilot_id": expected_pilot_id,
        "episode_id": expected_episode_id,
        "scores": normalized_scores,
        "missed_signals": missed,
        "false_or_weak_events": weak,
        "p0_issues": [str(item)[:500] for item in p0],
        "summary": str(payload.get("summary") or "")[:2000],
        "recommendation": str(payload.get("recommendation") or "scale_with_fixes"),
    }


def _score(value: Any, name: str) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} score must be numeric") from None
    if score < 0 or score > 100:
        raise ValueError(f"{name} score must be between 0 and 100")
    return round(score, 2)


def _list_of_dicts(value: Any, name: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError(f"{name} must be a list of objects")
    return value


def _finding_severity(value: Any, *, default: str) -> str:
    severity = str(value or default).strip().upper()
    return severity if severity in {"P0", "P1", "P2"} else default


def _clean_optional_id(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"id if known", "unknown", "none", "null", "n/a"}:
        return None
    return text[:160]


def _short_text(value: Any, *, limit: int = 500) -> str:
    if isinstance(value, dict):
        text = json.dumps({k: v for k, v in value.items() if k not in {"evidence", "quote", "text", "excerpt", "full_text"}}, ensure_ascii=True, sort_keys=True)
    else:
        text = str(value or "")
    return re.sub(r"\s+", " ", text).strip()[:limit]


def _finding_category(value: Any) -> str:
    text = _short_text(value).lower()
    if re.search(r"\b(identity|speaker|guest|host|alias|affiliation|person|actor|org|organization|who talks|mentions whom)\b", text):
        return "identity_graph"
    if re.search(r"\b(footnote|marker|timestamp|asr|numeric|number|metric|quantitative|4x|list item)\b", text):
        return "numeric_artifact"
    if re.search(r"\b(product|market|pricing|valuation|arr|revenue|launch|release|model|customer|adoption|capex|investment)\b", text):
        return "product_market_signal"
    if re.search(r"\b(ground|unsupported|false|weak|hallucinat|evidence)\b", text):
        return "grounding_precision"
    if re.search(r"\b(missed|coverage|underextract|omitted)\b", text):
        return "coverage"
    return "general_quality"


def _event_segment_map(conn, *, pilot_id: str) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT DISTINCT discourse_events.id AS discourse_event_id, discourse_events.segment_id
        FROM discourse_events
        JOIN labels ON labels.id = discourse_events.label_id
        JOIN jobs ON jobs.target_id = labels.segment_id
        WHERE labels.label_pack = 'ai_discourse_v3_1'
          AND json_extract(jobs.payload_json, '$.pilot_id') = ?
        """,
        (pilot_id,),
    ).fetchall()
    return {row["discourse_event_id"]: row["segment_id"] for row in rows}


def _pilot_episode_segment_ids(conn, *, pilot_id: str, episode_id: str) -> list[str]:
    return [
        row["segment_id"]
        for row in conn.execute(
            """
            SELECT DISTINCT segments.id AS segment_id
            FROM segments
            JOIN jobs ON jobs.target_id = segments.id
            WHERE segments.episode_id = ?
              AND jobs.job_type = 'label_segment'
              AND json_extract(jobs.payload_json, '$.pilot_id') = ?
            ORDER BY segments.segment_index, segments.id
            """,
            (episode_id, pilot_id),
        ).fetchall()
    ]


def _reviewed_episode_segment_ids(conn, *, pilot_id: str) -> list[str]:
    return [
        row["segment_id"]
        for row in conn.execute(
            """
            SELECT DISTINCT segments.id AS segment_id
            FROM reviewer_audits
            JOIN segments ON segments.episode_id = reviewer_audits.episode_id
            JOIN jobs ON jobs.target_id = segments.id
            WHERE reviewer_audits.pilot_id = ?
              AND reviewer_audits.status = 'completed'
              AND jobs.job_type = 'label_segment'
              AND json_extract(jobs.payload_json, '$.pilot_id') = ?
            ORDER BY reviewer_audits.episode_id, segments.segment_index, segments.id
            """,
            (pilot_id, pilot_id),
        ).fetchall()
    ]


def cluster_claims(conn, *, pilot_id: str | None, model: str, limit: int | None = None) -> dict[str, Any]:
    claims = _claim_rows(conn, pilot_id=pilot_id, limit=limit)
    groups: dict[str, list[dict[str, Any]]] = {}
    for claim in claims:
        key = _claim_cluster_key(claim)
        groups.setdefault(key, []).append(claim)
    ts = now_iso()
    upserted = 0
    for key, items in groups.items():
        canonical = _canonical_claim_text(items)
        evidence = {
            "pilot_id": pilot_id,
            "status_note": "candidate cluster pending GPT-5.5 semantic review",
            "claim_ids": [item["claim_id"] for item in items],
            "episode_ids": sorted({item["episode_id"] for item in items}),
            "segment_ids": sorted({item["segment_id"] for item in items}),
        }
        conn.execute(
            """
            INSERT INTO claim_clusters
              (id, canonical_claim_text, concept_id, status, judge_model, confidence, evidence_json, created_at, updated_at)
            VALUES (?, ?, NULL, 'candidate_pending_gpt55_judge', ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              canonical_claim_text = excluded.canonical_claim_text,
              evidence_json = excluded.evidence_json,
              updated_at = excluded.updated_at
            """,
            (
                stable_id("claim_cluster", pilot_id or "all", key, prefix="ccl_"),
                canonical,
                model,
                0.55 if len(items) == 1 else 0.65,
                dumps_json(evidence),
                ts,
                ts,
            ),
        )
        upserted += 1
    conn.commit()
    return {"ok": True, "pilot_id": pilot_id, "model": model, "claims_seen": len(claims), "clusters_upserted": upserted}


def judge_claim_edges(conn, *, pilot_id: str | None, model: str, limit: int = 100) -> dict[str, Any]:
    claims = _claim_rows(conn, pilot_id=pilot_id, limit=None)
    by_key: dict[str, list[dict[str, Any]]] = {}
    for claim in claims:
        by_key.setdefault(_claim_cluster_key(claim), []).append(claim)
    ts = now_iso()
    agreement = 0
    disagreement = 0
    considered = 0
    for items in by_key.values():
        if len(items) < 2:
            continue
        for index, source in enumerate(items):
            for target in items[index + 1 :]:
                if considered >= limit:
                    break
                considered += 1
                table = "agreement_edges"
                relation = "candidate_supporting_pending_gpt55_review"
                if _claims_probably_conflict(source, target):
                    table = "disagreement_edges"
                    relation = "candidate_conflicting_pending_gpt55_review"
                conn.execute(
                    f"""
                    INSERT OR IGNORE INTO {table}
                      (id, source_claim_id, target_claim_id, relation, judge_model, confidence, rationale, evidence_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        stable_id(table, source["claim_id"], target["claim_id"], prefix="cedge_"),
                        source["claim_id"],
                        target["claim_id"],
                        relation,
                        model,
                        0.55,
                        "Deterministic candidate edge for GPT-5.5 semantic review; not a final agreement judgment.",
                        dumps_json(
                            {
                                "pilot_id": pilot_id,
                                "source_segment_id": source["segment_id"],
                                "target_segment_id": target["segment_id"],
                                "source_label_id": source["label_id"],
                                "target_label_id": target["label_id"],
                                "audit_status": "pending_gpt55_semantic_judge",
                            }
                        ),
                        ts,
                    ),
                )
                if table == "agreement_edges":
                    agreement += 1
                else:
                    disagreement += 1
            if considered >= limit:
                break
        if considered >= limit:
            break
    conn.commit()
    return {
        "ok": True,
        "pilot_id": pilot_id,
        "model": model,
        "candidate_pairs_considered": considered,
        "agreement_edges_upserted": agreement,
        "disagreement_edges_upserted": disagreement,
        "status": "candidate_edges_pending_gpt55_semantic_judge",
    }


def _claim_rows(conn, *, pilot_id: str | None, limit: int | None) -> list[dict[str, Any]]:
    sql = """
        SELECT
          claims.id AS claim_id,
          claims.label_id,
          claims.segment_id,
          claims.text,
          claims.stance,
          claims.confidence,
          labels.label_pack,
          segments.episode_id,
          sources.name AS source_name
        FROM claims
        JOIN labels ON labels.id = claims.label_id
        JOIN segments ON segments.id = claims.segment_id
        JOIN sources ON sources.id = segments.source_id
        WHERE labels.label_pack = 'ai_discourse_v3_1'
    """
    params: list[Any] = []
    if pilot_id:
        sql += """
          AND EXISTS (
            SELECT 1
            FROM jobs
            WHERE jobs.target_id = claims.segment_id
              AND jobs.job_type = 'label_segment'
              AND json_extract(jobs.payload_json, '$.pilot_id') = ?
          )
        """
        params.append(pilot_id)
    sql += " ORDER BY claims.created_at DESC, claims.id"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _claim_cluster_key(claim: dict[str, Any]) -> str:
    text = str(claim.get("text") or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = [token for token in text.split() if len(token) > 2 and token not in {"the", "and", "that", "with", "from", "this"}]
    return " ".join(tokens[:12]) or "empty_claim"


def _canonical_claim_text(items: list[dict[str, Any]]) -> str:
    items = sorted(items, key=lambda item: len(str(item.get("text") or "")), reverse=True)
    return str(items[0].get("text") or "")[:1000]


def _claims_probably_conflict(source: dict[str, Any], target: dict[str, Any]) -> bool:
    left = str(source.get("stance") or "").lower()
    right = str(target.get("stance") or "").lower()
    opposing = {"negative", "opposes", "opposing", "counterclaim", "skeptical"}
    supportive = {"positive", "supports", "supporting", "affirming"}
    return (left in opposing and right in supportive) or (right in opposing and left in supportive)
