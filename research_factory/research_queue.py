from __future__ import annotations

import datetime as dt
from typing import Any

from . import db
from .util import dumps_json, loads_json, now_iso, stable_id
from .worker import claim_next_job, fail_job, release_job, run_jobs


LOCAL_ONLY_PRIVACY_TIERS = {"local_only"}
REMOTE_ALLOWED_PRIVACY_TIERS = {"public_link_only", "excerpt_context", "full_text_allowed"}
ROLE_JOB_TYPES: dict[str, tuple[str, ...]] = {
    "acquisition": ("fetch_transcript", "transcribe_audio", "manual_transcript_required", "source_fetch_failed"),
    "extractor": ("episode_context", "label_segment"),
    "reviewer": ("audit_label",),
    "identity_judge": ("identity_judge",),
    "claim_judge": ("claim_cluster", "claim_edge_judge"),
    "snapshot_publisher": ("snapshot",),
}


def sync_queue_envelopes(
    conn,
    *,
    statuses: list[str] | tuple[str, ...] | None = None,
    job_types: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    db.init_db(conn)
    where: list[str] = []
    params: list[Any] = []
    if statuses:
        where.append(f"jobs.status IN ({','.join('?' for _ in statuses)})")
        params.extend(statuses)
    if job_types:
        where.append(f"jobs.job_type IN ({','.join('?' for _ in job_types)})")
        params.extend(job_types)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""
        SELECT jobs.*, segments.episode_id AS segment_episode_id, segments.transcript_id AS segment_transcript_id,
               episodes.source_id AS episode_source_id
        FROM jobs
        LEFT JOIN segments ON segments.id = jobs.target_id
        LEFT JOIN episodes ON episodes.id = COALESCE(segments.episode_id, jobs.target_id)
        {where_sql}
        """,
        params,
    ).fetchall()
    created = 0
    updated = 0
    for row in rows:
        payload = loads_json(row["payload_json"], {})
        envelope = _envelope_for_job(row, payload)
        existing = conn.execute("SELECT job_id FROM queue_envelopes WHERE job_id = ?", (row["id"],)).fetchone()
        if existing:
            updated += 1
        else:
            created += 1
        conn.execute(
            """
            INSERT INTO queue_envelopes
              (job_id, queue_name, worker_role, content_type, source_id, item_id, artifact_id, label_pack,
               model_required, privacy_tier, lease_ttl_seconds, input_context_ref, output_schema_ref,
               callback_mode, run_tag, allowed_tools_json, capabilities_json, remote_claimable, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
              queue_name = excluded.queue_name,
              worker_role = excluded.worker_role,
              content_type = excluded.content_type,
              source_id = excluded.source_id,
              item_id = excluded.item_id,
              artifact_id = excluded.artifact_id,
              label_pack = excluded.label_pack,
              model_required = excluded.model_required,
              privacy_tier = excluded.privacy_tier,
              lease_ttl_seconds = excluded.lease_ttl_seconds,
              input_context_ref = excluded.input_context_ref,
              output_schema_ref = excluded.output_schema_ref,
              callback_mode = excluded.callback_mode,
              run_tag = excluded.run_tag,
              allowed_tools_json = excluded.allowed_tools_json,
              capabilities_json = excluded.capabilities_json,
              remote_claimable = excluded.remote_claimable,
              updated_at = excluded.updated_at
            """,
            (
                row["id"],
                envelope["queue_name"],
                envelope["worker_role"],
                envelope["content_type"],
                envelope["source_id"],
                envelope["item_id"],
                envelope["artifact_id"],
                envelope["label_pack"],
                envelope["model_required"],
                envelope["privacy_tier"],
                envelope["lease_ttl_seconds"],
                envelope["input_context_ref"],
                envelope["output_schema_ref"],
                envelope["callback_mode"],
                envelope["run_tag"],
                dumps_json(envelope["allowed_tools"]),
                dumps_json(envelope["capabilities"]),
                1 if envelope["remote_claimable"] else 0,
                envelope["created_at"],
                envelope["updated_at"],
            ),
        )
    conn.commit()
    return {"ok": True, "created": created, "updated": updated, "total": len(rows)}


def queue_status(conn, *, group_by: list[str] | None = None, sync: bool = True) -> dict[str, Any]:
    db.init_db(conn)
    if sync:
        sync_queue_envelopes(conn)
    allowed = {
        "lane": "jobs.lane",
        "job_type": "jobs.job_type",
        "status": "jobs.status",
        "role": "queue_envelopes.worker_role",
        "content_type": "queue_envelopes.content_type",
        "privacy_tier": "queue_envelopes.privacy_tier",
    }
    group_by = group_by or ["lane", "content_type", "role", "status"]
    selected = [item for item in group_by if item in allowed]
    if not selected:
        selected = ["lane", "content_type", "role", "status"]
    select_sql = ", ".join(f"{allowed[item]} AS {item}" for item in selected)
    group_sql = ", ".join(allowed[item] for item in selected)
    rows = [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT {select_sql}, COUNT(*) AS count
            FROM jobs
            JOIN queue_envelopes ON queue_envelopes.job_id = jobs.id
            GROUP BY {group_sql}
            ORDER BY {group_sql}
            """
        ).fetchall()
    ]
    active_workers = [
        dict(row)
        for row in conn.execute(
            """
            SELECT worker_role, status, COUNT(*) AS count, SUM(claimed_jobs) AS claimed_jobs,
                   SUM(completed_jobs) AS completed_jobs, SUM(failed_jobs) AS failed_jobs
            FROM worker_runs
            GROUP BY worker_role, status
            ORDER BY worker_role, status
            """
        ).fetchall()
    ]
    remote_ready = {
        row["privacy_tier"]: int(row["count"])
        for row in conn.execute(
            """
            SELECT queue_envelopes.privacy_tier, COUNT(*) AS count
            FROM queue_envelopes
            JOIN jobs ON jobs.id = queue_envelopes.job_id
            WHERE queue_envelopes.remote_claimable = 1
              AND jobs.status = 'pending'
              AND jobs.attempts < jobs.max_attempts
            GROUP BY queue_envelopes.privacy_tier
            """
        ).fetchall()
    }
    return {
        "ok": True,
        "group_by": selected,
        "queues": rows,
        "active_workers": active_workers,
        "future_mcp": {
            "enabled": False,
            "remote_claimable_by_privacy_tier": remote_ready,
            "source_of_truth": "local_sqlite",
        },
    }


def run_worker(
    conn,
    *,
    role: str,
    lane: str,
    limit: int,
    model: str,
    label_pack: str,
    worker_id: str,
    local_draft: bool = False,
    claim_prompts: bool = True,
    burst: bool = False,
) -> dict[str, Any]:
    db.init_db(conn)
    if role not in ROLE_JOB_TYPES:
        raise ValueError(f"Unknown worker role: {role}")
    if limit > 6 and burst:
        raise ValueError("Burst workers are capped at 6 jobs")
    if limit > 4 and not burst:
        raise ValueError("Default local worker runs are capped at 4 jobs; pass --burst for small clean batches up to 6")
    run_id = stable_id(worker_id, role, now_iso(), prefix="wr_")
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO worker_runs
          (id, worker_id, worker_role, model, status, metrics_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, 'running', '{}', ?, ?)
        """,
        (run_id, worker_id, role, model, ts, ts),
    )
    conn.commit()
    try:
        stats = run_jobs(
            conn,
            lane=lane,
            limit=limit,
            model=model,
            label_pack=label_pack,
            worker_id=worker_id,
            local_draft=local_draft,
            claim_prompts=claim_prompts,
            job_types=ROLE_JOB_TYPES[role],
            max_label_prompts=limit,
        )
        _record_worker_result(conn, run_id, status="completed", stats=stats)
        return {"ok": True, "worker_run_id": run_id, "role": role, **stats}
    except Exception as exc:
        _record_worker_result(conn, run_id, status="failed", stats={"error": str(exc)})
        raise


def claim_remote_job(
    conn,
    *,
    worker_id: str,
    capabilities: list[str],
    max_items: int = 1,
) -> dict[str, Any]:
    db.init_db(conn)
    sync_queue_envelopes(conn)
    claimed = []
    for _ in range(max_items):
        job = _claim_next_remote_job(conn, worker_id=worker_id, capabilities=capabilities)
        if not job:
            break
        payload = loads_json(job["payload_json"], {})
        envelope = conn.execute("SELECT * FROM queue_envelopes WHERE job_id = ?", (job["id"],)).fetchone()
        event_id = _record_queue_event(
            conn,
            job_id=job["id"],
            event_type="remote_claim",
            event={"worker_id": worker_id, "capabilities": capabilities},
        )
        claimed.append(
            {
                "job_id": job["id"],
                "job_type": job["job_type"],
                "lane": job["lane"],
                "target_id": job["target_id"],
                "payload": payload,
                "envelope": _public_envelope(envelope),
                "queue_event_id": event_id,
            }
        )
    conn.commit()
    return {"ok": True, "enabled": False, "message": "MCP remote claims are a future adapter; this is a local contract smoke surface.", "claimed": claimed}


def submit_remote_output(
    conn,
    *,
    job_id: int,
    worker_id: str,
    output_ref: str,
    status: str,
    validation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    db.init_db(conn)
    job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not job:
        raise ValueError(f"Job not found: {job_id}")
    if job["lease_owner"] != worker_id:
        raise ValueError(f"Job {job_id} is leased to {job['lease_owner']}, not {worker_id}")
    submission_id = stable_id(str(job_id), worker_id, output_ref, now_iso(), prefix="os_")
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO output_submissions
          (id, job_id, worker_id, output_ref, status, validation_json, submitted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (submission_id, job_id, worker_id, output_ref, status, dumps_json(validation or {}), ts),
    )
    _record_queue_event(conn, job_id=job_id, event_type="remote_output_submitted", event={"worker_id": worker_id, "status": status})
    conn.commit()
    return {
        "ok": True,
        "enabled": False,
        "submission_id": submission_id,
        "message": "Stored remote-style submission for local validation/import; canonical DB mutation stays local.",
    }


def release_or_fail_remote_job(conn, *, job_id: int, worker_id: str, reason: str, fail: bool = False) -> dict[str, Any]:
    db.init_db(conn)
    job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not job:
        raise ValueError(f"Job not found: {job_id}")
    if job["lease_owner"] != worker_id:
        raise ValueError(f"Job {job_id} is leased to {job['lease_owner']}, not {worker_id}")
    if fail:
        fail_job(conn, job_id, reason)
        event_type = "remote_fail"
    else:
        release_job(conn, job_id)
        event_type = "remote_release"
    _record_queue_event(conn, job_id=job_id, event_type=event_type, event={"worker_id": worker_id, "reason": reason})
    conn.commit()
    return {"ok": True, "job_id": job_id, "status": "failed" if fail else "released"}


def _claim_next_remote_job(conn, *, worker_id: str, capabilities: list[str]):
    now = now_iso()
    leased_until = (dt.datetime.fromisoformat(now) + dt.timedelta(minutes=45)).isoformat()
    capability_set = set(capabilities)
    candidate_rows = conn.execute(
        """
        SELECT jobs.id, queue_envelopes.worker_role
        FROM jobs
        JOIN queue_envelopes ON queue_envelopes.job_id = jobs.id
        WHERE jobs.status = 'pending'
          AND jobs.attempts < jobs.max_attempts
          AND queue_envelopes.remote_claimable = 1
          AND queue_envelopes.privacy_tier IN ('public_link_only', 'excerpt_context', 'full_text_allowed')
        ORDER BY jobs.priority ASC, jobs.id ASC
        LIMIT 50
        """
    ).fetchall()
    for candidate in candidate_rows:
        if capability_set and candidate["worker_role"] not in capability_set:
            continue
        row = conn.execute(
            """
            UPDATE jobs
            SET status = 'claimed',
                lease_owner = ?,
                leased_until = ?,
                attempts = attempts + 1,
                updated_at = ?
            WHERE id = ? AND status = 'pending'
            RETURNING *
            """,
            (worker_id, leased_until, now, candidate["id"]),
        ).fetchone()
        if row:
            return row
    return None


def _envelope_for_job(row, payload: dict[str, Any]) -> dict[str, Any]:
    job_type = row["job_type"]
    worker_role = _role_for_job_type(job_type)
    content_type = "podcast_episode" if row["episode_source_id"] or row["segment_episode_id"] else payload.get("content_type", "unknown")
    item_id = payload.get("content_item_id")
    if not item_id:
        episode_id = row["segment_episode_id"] or (row["target_id"] if content_type == "podcast_episode" else None)
        item_id = f"ci_{episode_id}" if episode_id else None
    artifact_id = payload.get("content_artifact_id")
    if not artifact_id and row["segment_transcript_id"]:
        artifact_id = f"ca_{row['segment_transcript_id']}"
    privacy_tier = payload.get("privacy_tier", "local_only")
    return {
        "queue_name": f"{row['lane']}:{worker_role}",
        "worker_role": worker_role,
        "content_type": content_type,
        "source_id": f"cs_{row['episode_source_id']}" if row["episode_source_id"] else payload.get("source_id"),
        "item_id": item_id,
        "artifact_id": artifact_id,
        "label_pack": payload.get("label_pack"),
        "model_required": payload.get("model"),
        "privacy_tier": privacy_tier,
        "lease_ttl_seconds": int(payload.get("lease_ttl_seconds", 2700)),
        "input_context_ref": payload.get("prompt_path") or payload.get("context_artifact_path"),
        "output_schema_ref": payload.get("schema_version") or payload.get("episode_context_schema_version"),
        "callback_mode": payload.get("callback_mode", "local_submit"),
        "run_tag": payload.get("patch_tag") or payload.get("pilot_id") or payload.get("run_tag"),
        "allowed_tools": _allowed_tools_for_role(worker_role),
        "capabilities": [worker_role, f"model_speed:{payload.get('model_speed', payload.get('speed', 'regular'))}"],
        "remote_claimable": privacy_tier in REMOTE_ALLOWED_PRIVACY_TIERS,
        "created_at": row["created_at"],
        "updated_at": now_iso(),
    }


def _role_for_job_type(job_type: str) -> str:
    for role, job_types in ROLE_JOB_TYPES.items():
        if job_type in job_types:
            return role
    if job_type.startswith("fetch") or "transcript" in job_type:
        return "acquisition"
    if "audit" in job_type or "review" in job_type:
        return "reviewer"
    return "extractor"


def _allowed_tools_for_role(role: str) -> list[str]:
    if role == "acquisition":
        return ["rss", "official_page", "public_captions"]
    if role in {"extractor", "reviewer", "identity_judge", "claim_judge"}:
        return ["read_context", "write_structured_json"]
    if role == "snapshot_publisher":
        return ["publish_sanitized_snapshot"]
    return []


def _record_worker_result(conn, run_id: str, *, status: str, stats: dict[str, Any]) -> None:
    ts = now_iso()
    conn.execute(
        """
        UPDATE worker_runs
        SET status = ?,
            claimed_jobs = ?,
            completed_jobs = ?,
            failed_jobs = ?,
            metrics_json = ?,
            updated_at = ?,
            completed_at = ?
        WHERE id = ?
        """,
        (
            status,
            int(stats.get("processed", 0) or 0),
            int(stats.get("completed", 0) or 0),
            int(stats.get("failed", 0) or 0),
            dumps_json(stats),
            ts,
            ts,
            run_id,
        ),
    )
    conn.commit()


def _record_queue_event(conn, *, job_id: int | None, event_type: str, event: dict[str, Any]) -> str:
    event_id = stable_id(str(job_id), event_type, dumps_json(event), now_iso(), prefix="qe_")
    conn.execute(
        "INSERT INTO queue_events (id, job_id, event_type, event_json, created_at) VALUES (?, ?, ?, ?, ?)",
        (event_id, job_id, event_type, dumps_json(event), now_iso()),
    )
    return event_id


def _public_envelope(row) -> dict[str, Any]:
    if not row:
        return {}
    return {
        "worker_role": row["worker_role"],
        "content_type": row["content_type"],
        "label_pack": row["label_pack"],
        "model_required": row["model_required"],
        "privacy_tier": row["privacy_tier"],
        "callback_mode": row["callback_mode"],
        "run_tag": row["run_tag"],
    }
