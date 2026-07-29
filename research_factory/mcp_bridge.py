from __future__ import annotations

import json
import signal
import urllib.request
from pathlib import Path
from typing import Any

from . import db
from .observer import build_snapshot
from .paths import corpus_dir, runs_dir
from .research_queue import queue_status, sync_queue_envelopes
from .util import dumps_json, loads_json, now_iso, stable_id, write_text_atomic
from .worker import EPISODE_CONTEXT_SCHEMA_VERSION, complete_job, completed_episode_context_for_episode, fail_or_retry_job, submit_episode_context_output
from .mcp_broker import chunk_context_text, chunk_manifest, context_chunk_chars, source_card_manifest, source_cards_for_transcript, split_episode_context_prompt, validate_source_card


SOURCE_CARD_SEGMENT_WINDOW_CHARS = 4000


def publish_broker_snapshot(
    conn,
    *,
    broker_url: str,
    token: str,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    payload = build_snapshot(conn)
    endpoint = broker_url.rstrip("/") + "/ingest-snapshot"
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload, ensure_ascii=True).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        body = response.read().decode("utf-8")
        parsed = json.loads(body) if body else {}
        return {"ok": 200 <= response.status < 300, "status": response.status, "body": parsed}


def publish_broker_status(
    conn,
    *,
    broker_url: str,
    token: str,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    queue = queue_status(conn, group_by=["lane", "content_type", "role", "status"], sync=False)
    remote_submissions = [
        dict(row)
        for row in conn.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM output_submissions
            GROUP BY status
            ORDER BY status
            """
        ).fetchall()
    ]
    import_failures = int(
        conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM output_submissions
            WHERE status IN ('rejected', 'validation_failed', 'import_failed')
            """
        ).fetchone()["count"]
    )
    by_status: dict[str, int] = {}
    by_role_content_status: list[dict[str, Any]] = []
    for row in queue.get("queues") or []:
        status = str(row.get("status") or "unknown")
        count = int(row.get("count") or 0)
        by_status[status] = by_status.get(status, 0) + count
        by_role_content_status.append(
            {
                "worker_role": row.get("role") or "unknown",
                "content_type": row.get("content_type") or "unknown",
                "status": status,
                "count": count,
            }
        )
    active_remote_leases = int(
        conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM jobs
            WHERE status = 'claimed'
              AND COALESCE(lease_owner, '') LIKE 'mcp-%'
            """
        ).fetchone()["count"]
    )
    payload = {
        "contract_version": "railway-operational-v2",
        "generated_at": now_iso(),
        "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
        "snapshot_health": {"mode": "mcp_broker_status_fast"},
        "counts": {
            "output_submissions": sum(int(row.get("count") or 0) for row in remote_submissions),
        },
        "queues": {
            "by_status": by_status,
            "by_lane_type_status": [],
            "by_role_content_status": by_role_content_status,
            "privacy_tiers": {},
            "remote_claimable_by_privacy_tier": (queue.get("future_mcp") or {}).get("remote_claimable_by_privacy_tier") or {},
        },
        "runs": {
            "worker_runs": queue.get("active_workers") or [],
            "label_runs": [],
            "worker_status": {
                "remote": {
                    "state": "active" if active_remote_leases else "disabled",
                    "active_claims": active_remote_leases,
                    "evidence": "database_leases",
                }
            },
            "service_status": {
                "mcp_broker": {"state": "configured", "evidence": "publish_status_call"}
            },
        },
        "failures": {
            "jobs": [],
            "expired_claims": [],
            "label_runs": [],
            "missing_claimed_run_outputs": 0,
            "queue_sync": "ok",
            "output_submissions": remote_submissions,
            "remote_import_failures": import_failures,
        },
        "artifacts": [],
        "intervention_flags": [],
    }
    parsed = _post_json(
        broker_url.rstrip("/") + "/ingest-snapshot",
        token=token,
        payload=payload,
        timeout_seconds=timeout_seconds,
    )
    return {"ok": bool(parsed.get("ok")), "snapshot_mode": "mcp_broker_status_fast", "broker": parsed}


def publish_remote_work_packages(
    conn,
    *,
    broker_url: str,
    token: str,
    limit: int = 1,
    worker_id: str = "mcp-broker-bridge",
    replace: bool = False,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    sync_queue_envelopes(conn, statuses=("pending",), job_types=("episode_context",))
    packages: list[dict[str, Any]] = []
    claimed_job_ids: list[int] = []
    rows = conn.execute(
        """
        SELECT jobs.*, queue_envelopes.privacy_tier, queue_envelopes.worker_role
        FROM jobs
        JOIN queue_envelopes ON queue_envelopes.job_id = jobs.id
        WHERE jobs.status = 'pending'
          AND jobs.job_type = 'episode_context'
          AND jobs.attempts < jobs.max_attempts
          AND queue_envelopes.remote_claimable = 1
          AND queue_envelopes.privacy_tier = 'full_text_allowed'
        ORDER BY jobs.priority ASC, jobs.id ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    for row in rows:
        ts = now_iso()
        claimed = conn.execute(
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
            (worker_id, _lease_until(minutes=90), ts, row["id"]),
        ).fetchone()
        if not claimed:
            continue
        claimed_job_ids.append(int(claimed["id"]))
        try:
            package = create_source_card_work_package(conn, claimed, worker_id=worker_id)
        except Exception as exc:
            fail_or_retry_job(conn, claimed, f"source_card_package_failed:{str(exc)[:240]}")
            continue
        if package is None:
            continue
        packages.append(package)
    if not packages:
        return {"ok": True, "published": 0, "claimed_job_ids": claimed_job_ids}
    body = {"packages": packages, "replace": replace}
    parsed = _post_json(broker_url.rstrip("/") + "/ingest-work-packages", token=token, payload=body, timeout_seconds=timeout_seconds)
    return {"ok": bool(parsed.get("ok")), "published": len(packages), "claimed_job_ids": claimed_job_ids, "broker": parsed}


def create_source_card_work_package(conn, job, *, worker_id: str) -> dict[str, Any] | None:
    payload = loads_json(job["payload_json"], {})
    label_pack = payload.get("label_pack", "ai_discourse_v3_1")
    model = payload.get("model", "gpt-5.5")
    if label_pack != "ai_discourse_v3_1":
        raise ValueError("source-card remote episode_context jobs require ai_discourse_v3_1")
    if model != "gpt-5.5":
        raise ValueError("source-card remote episode_context jobs require model gpt-5.5")
    existing = completed_episode_context_for_episode(conn, job["target_id"], label_pack=label_pack, model=model)
    if existing:
        complete_job(conn, job["id"])
        conn.commit()
        return None

    source_text, metadata = bounded_episode_source_text(conn, str(job["target_id"]))
    cards = source_cards_for_transcript(source_text, episode_id=str(job["target_id"]))
    if not cards:
        raise ValueError(f"No source cards could be built for job {job['id']}")
    manifest = source_card_manifest(cards, source_text)
    chunks = chunk_context_text(source_text, chunk_chars=context_chunk_chars())
    chunks_manifest = chunk_manifest(chunks, source_text)
    for card in cards:
        validate_source_card(card)

    run_id = stable_id(str(job["target_id"]), label_pack, model, prefix="ectx_")
    prompt_path = runs_dir() / "prompts" / f"{run_id}.source-card.md"
    output_path = runs_dir() / "outputs" / f"{run_id}.json"
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(
        prompt_path,
        "\n".join(
            [
                "# Podcast Intelligence Factory Source-Card Package",
                "This local prompt intentionally omits raw transcript text.",
                f"job_id: {job['id']}",
                f"episode_id: {job['target_id']}",
                f"source_card_count: {len(cards)}",
                "Remote workers receive bounded source-card features only; canonical import validates locally.",
                "",
            ]
        ),
    )

    ts = now_iso()
    transcript_id = metadata.get("transcript_id")
    conn.execute(
        """
        INSERT INTO episode_context_runs
          (id, job_id, episode_id, transcript_id, label_pack, model, status, prompt_path, output_path,
           speaker_map_json, section_map_json, entity_seed_json, concept_seed_json, extraction_guidance,
           error, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, 'claimed', ?, ?, '[]', '[]', '{}', '[]', NULL, NULL, ?, ?)
        ON CONFLICT(episode_id, label_pack, model) DO UPDATE SET
          job_id = excluded.job_id,
          transcript_id = excluded.transcript_id,
          status = 'claimed',
          prompt_path = excluded.prompt_path,
          output_path = excluded.output_path,
          error = NULL,
          updated_at = excluded.updated_at,
          completed_at = NULL
        """,
        (run_id, job["id"], job["target_id"], transcript_id, label_pack, model, str(prompt_path), str(output_path), ts, ts),
    )
    conn.execute(
        "UPDATE jobs SET payload_json = ?, updated_at = ? WHERE id = ?",
        (
            dumps_json(
                {
                    **payload,
                    "label_pack": label_pack,
                    "model": model,
                    "episode_context_run_id": run_id,
                    "episode_context_schema_version": EPISODE_CONTEXT_SCHEMA_VERSION,
                    "prompt_path": str(prompt_path),
                    "output_path": str(output_path),
                    "source_text_kind": "bounded_segment_windows",
                    "source_cards": cards,
                    "source_card_manifest": manifest,
                }
            ),
            ts,
            job["id"],
        ),
    )
    conn.commit()
    return {
        "id": f"job_{job['id']}",
        "title": f"episode_context job {job['id']}",
        "privacy_tier": "full_text_allowed",
        "context": {
            "task": "episode_context",
            "job_id": int(job["id"]),
            "job_type": "episode_context",
            "episode_id": str(job["target_id"]),
            "label_pack": label_pack,
            "model_required": model,
            "worker_id": worker_id,
            "content_type": "podcast_episode",
            "source_text_kind": "bounded_segment_windows",
            "context_protocol": "chunked_v1",
            "instructions_text": "Use bounded source-card features only. Do not quote, reconstruct, or request raw transcript text.",
            "output_contract": "Return only the JSON object requested by the schema. Do not include Markdown fences.",
                    "chunks": chunks,
                    "chunk_manifest": chunks_manifest,
            "source_cards": cards,
            "source_card_manifest": manifest,
            "notes": [],
        },
        "output_schema": {
            "type": "episode_context_output",
            "schema_version": EPISODE_CONTEXT_SCHEMA_VERSION,
        },
    }


def bounded_episode_source_text(conn, episode_id: str) -> tuple[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          segments.id,
          segments.segment_index,
          segments.start_char,
          segments.end_char,
          segments.word_count,
          segments.text_path,
          segments.transcript_id
        FROM segments
        WHERE segments.episode_id = ?
        ORDER BY segments.transcript_id, segments.segment_index
        """,
        (episode_id,),
    ).fetchall()
    if not rows:
        raise ValueError(f"No prepared segments found for episode: {episode_id}")
    parts: list[str] = []
    for row in rows:
        text = read_bounded_segment_text(row["text_path"], SOURCE_CARD_SEGMENT_WINDOW_CHARS)
        if not text:
            continue
        parts.append(
            "\n".join(
                [
                    f"===== SEGMENT {row['segment_index']} | {row['id']} | chars={row['start_char']}-{row['end_char']} | words={row['word_count']} =====",
                    text,
                ]
            )
        )
    if not parts:
        raise ValueError(f"No readable prepared segment text found for episode: {episode_id}")
    return "\n\n".join(parts), {"transcript_id": rows[0]["transcript_id"], "segment_count": len(rows)}


def read_bounded_segment_text(text_path: str, max_chars: int) -> str:
    path = (corpus_dir().parent / text_path).resolve()
    previous_handler = signal.getsignal(signal.SIGALRM)

    def timeout_handler(_signum, _frame):
        raise TimeoutError(f"Timed out reading segment text: {text_path}")

    try:
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.setitimer(signal.ITIMER_REAL, 1.0)
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            text = handle.read(max_chars + 1)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
    if len(text) <= max_chars:
        return text.strip()
    boundary = text.rfind(" ", 0, max_chars)
    if boundary < max_chars // 2:
        boundary = max_chars
    return text[:boundary].strip()


def ensure_source_card_buffer(
    conn,
    *,
    broker_url: str,
    token: str,
    target_buffer: int = 2,
    max_new: int = 1,
    worker_id: str = "pif-source-card-buffer-bridge",
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    target_buffer = max(0, int(target_buffer))
    max_new = max(0, int(max_new))
    if target_buffer == 0 or max_new == 0:
        return {"ok": True, "target_buffer": target_buffer, "outstanding": 0, "enqueued": 0, "published": 0}
    sync_queue_envelopes(conn, statuses=("pending", "claimed"), job_types=("episode_context",))
    outstanding = _source_card_buffer_outstanding(conn)
    needed = min(max_new, max(0, target_buffer - outstanding))
    enqueued: list[dict[str, Any]] = []
    if needed > 0:
        ts = now_iso()
        for index, row in enumerate(_source_card_buffer_candidates(conn, needed)):
            run_tag = f"chatgpt_source_card_buffer_{ts.replace(':', '').replace('+', 'Z')}_{index + 1}"
            payload = {
                "content_type": "podcast_episode",
                "episode_context_schema_version": "ai_discourse_v3_1_episode_context",
                "episode_context_version": "ai_discourse_v3_1_episode_context",
                "label_pack": "ai_discourse_v3_1",
                "lease_ttl_seconds": 5400,
                "model": "gpt-5.5",
                "priority_reason": "chatgpt_source_card_buffer_refill",
                "privacy_tier": "full_text_allowed",
                "run_tag": run_tag,
            }
            job_id = db.enqueue_job(
                conn,
                lane="podcast",
                job_type="episode_context",
                target_id=row["id"],
                payload=payload,
                priority=12,
                max_attempts=2,
            )
            enqueued.append({"job_id": job_id, "episode_id": row["id"], "segment_count": int(row["segment_count"])})
        if enqueued:
            conn.commit()
    if needed == 0:
        return {
            "ok": True,
            "target_buffer": target_buffer,
            "outstanding_before": outstanding,
            "needed": needed,
            "enqueued": 0,
            "enqueued_jobs": [],
            "published": 0,
            "claimed_job_ids": [],
        }
    published = publish_remote_work_packages(
        conn,
        broker_url=broker_url,
        token=token,
        limit=max(1, needed),
        worker_id=worker_id,
        replace=False,
        timeout_seconds=timeout_seconds,
    )
    return {
        "ok": bool(published.get("ok")),
        "target_buffer": target_buffer,
        "outstanding_before": outstanding,
        "needed": needed,
        "enqueued": len(enqueued),
        "enqueued_jobs": enqueued,
        "published": int(published.get("published") or 0),
        "claimed_job_ids": published.get("claimed_job_ids") or [],
    }


def _source_card_buffer_outstanding(conn) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM jobs
        WHERE job_type = 'episode_context'
          AND status = 'claimed'
          AND json_extract(payload_json, '$.privacy_tier') = 'full_text_allowed'
          AND (
            json_extract(payload_json, '$.priority_reason') LIKE 'controlled_chatgpt_source_card_smoke%'
            OR json_extract(payload_json, '$.priority_reason') = 'chatgpt_source_card_buffer_refill'
          )
        """
    ).fetchone()
    return int(row["count"] if row else 0)


def _source_card_buffer_candidates(conn, limit: int):
    return conn.execute(
        """
        SELECT
          episodes.id,
          COUNT(segments.id) AS segment_count,
          CASE
            WHEN lower(episodes.title) LIKE '%ai%' THEN 1
            WHEN lower(episodes.title) LIKE '%gpt%' THEN 1
            WHEN lower(episodes.title) LIKE '%model%' THEN 1
            WHEN lower(episodes.title) LIKE '%agent%' THEN 1
            WHEN lower(episodes.title) LIKE '%enterprise%' THEN 1
            ELSE 0
          END AS ai_relevant
        FROM episodes
        JOIN segments ON segments.episode_id = episodes.id
        LEFT JOIN episode_context_runs completed_context
          ON completed_context.episode_id = episodes.id
         AND completed_context.status = 'completed'
        WHERE completed_context.id IS NULL
          AND NOT EXISTS (
            SELECT 1
            FROM jobs active_job
            WHERE active_job.job_type = 'episode_context'
              AND active_job.target_id = episodes.id
              AND active_job.status IN ('pending', 'claimed')
          )
        GROUP BY episodes.id
        HAVING segment_count BETWEEN 1 AND 12
        ORDER BY ai_relevant DESC, segment_count ASC, episodes.id ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def import_remote_submissions(
    conn,
    *,
    broker_url: str,
    token: str,
    limit: int = 10,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    exported = _post_json(
        broker_url.rstrip("/") + "/export-submissions",
        token=token,
        payload={"limit": limit},
        timeout_seconds=timeout_seconds,
    )
    imported = 0
    failed = 0
    skipped = 0
    details: list[dict[str, Any]] = []
    for submission in exported.get("submissions", []):
        work_id = submission.get("work_id")
        status = "imported"
        validation: dict[str, Any] = {"ok": True}
        try:
            if not isinstance(work_id, str) or not work_id.startswith("job_"):
                raise ValueError("Unsupported work_id")
            job_id = int(work_id.removeprefix("job_"))
            job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if not job:
                raise ValueError(f"Job not found: {job_id}")
            if job["job_type"] != "episode_context":
                raise ValueError(f"Unsupported remote job_type: {job['job_type']}")
            payload = loads_json(job["payload_json"], {})
            _validate_local_source_cards(payload, episode_id=str(job["target_id"]))
            output_path = Path(payload.get("output_path") or "").expanduser().resolve()
            if not payload.get("output_path"):
                raise ValueError("Remote job has no local output_path")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(submission.get("output") or {}, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            if job["status"] == "pending":
                remote_worker_id = str(submission.get("worker_id") or "mcp-remote-worker")
                ts = now_iso()
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'claimed',
                        lease_owner = ?,
                        leased_until = ?,
                        attempts = attempts + 1,
                        updated_at = ?
                    WHERE id = ? AND status = 'pending'
                    """,
                    (remote_worker_id, _lease_until(minutes=90), ts, job_id),
                )
                job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            result = submit_episode_context_output(
                conn,
                job_id=job_id,
                output_json_path=output_path,
                worker_id=job["lease_owner"],
                allow_expired=True,
            )
            imported += 1
            validation = {"ok": True, "result": result}
        except Exception as exc:
            failed += 1
            status = "import_failed"
            validation = {"ok": False, "error": str(exc)[:500]}
        if work_id:
            try:
                _post_json(
                    broker_url.rstrip("/") + "/mark-submission-imported",
                    token=token,
                    payload={"work_id": work_id, "status": status, "validation": validation},
                    timeout_seconds=timeout_seconds,
                )
            except Exception:
                pass
        details.append({"work_id": work_id, "status": status, "validation": validation})
    skip_details = reconcile_remote_skips(conn, broker_url=broker_url, token=token, limit=limit, timeout_seconds=timeout_seconds)
    skipped = int(skip_details.get("skipped") or 0)
    failed += int(skip_details.get("failed") or 0)
    details.extend(skip_details.get("details") or [])
    return {
        "ok": failed == 0,
        "seen": len(exported.get("submissions", [])),
        "imported": imported,
        "skipped": skipped,
        "failed": failed,
        "details": details,
    }


def reconcile_remote_skips(
    conn,
    *,
    broker_url: str,
    token: str,
    limit: int = 10,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    try:
        exported = _post_json(
            broker_url.rstrip("/") + "/export-skips",
            token=token,
            payload={"limit": limit},
            timeout_seconds=timeout_seconds,
        )
    except Exception as exc:
        return {"ok": False, "skipped": 0, "failed": 1, "details": [{"status": "skip_export_failed", "error": str(exc)[:300]}]}
    skipped = 0
    failed = 0
    details: list[dict[str, Any]] = []
    for skip in exported.get("skips", []):
        work_id = skip.get("work_id")
        status = "skip_imported"
        validation = {"ok": True, "skip": skip.get("validation") or {}}
        try:
            if not isinstance(work_id, str) or not work_id.startswith("job_"):
                raise ValueError("Unsupported work_id")
            job_id = int(work_id.removeprefix("job_"))
            job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if not job:
                raise ValueError(f"Job not found: {job_id}")
            reason = (skip.get("validation") or {}).get("reason_code") or "remote_source_card_skipped"
            fail_or_retry_job(conn, job, f"remote_source_card_skipped:{reason}")
            skipped += 1
        except Exception as exc:
            failed += 1
            status = "import_failed"
            validation = {"ok": False, "error": str(exc)[:500]}
        if work_id:
            try:
                _post_json(
                    broker_url.rstrip("/") + "/mark-submission-imported",
                    token=token,
                    payload={"work_id": work_id, "status": status, "validation": validation},
                    timeout_seconds=timeout_seconds,
                )
            except Exception:
                pass
        details.append({"work_id": work_id, "status": status, "validation": validation})
    conn.commit()
    return {"ok": failed == 0, "seen": len(exported.get("skips", [])), "skipped": skipped, "failed": failed, "details": details}


def _validate_local_source_cards(payload: dict[str, Any], *, episode_id: str) -> None:
    payload_cards = payload.get("source_cards")
    if isinstance(payload_cards, list) and payload_cards:
        for card in payload_cards:
            if str(card.get("episode_id") or "") != str(episode_id):
                raise ValueError("Remote job source-card episode_id mismatch")
            validate_source_card(card)
        return
    prompt_path = payload.get("prompt_path")
    if not prompt_path:
        return
    path = Path(prompt_path).expanduser().resolve()
    if not path.exists():
        raise ValueError("Remote job prompt_path is missing; cannot validate source-card traceability")
    _, transcript, _ = split_episode_context_prompt(path.read_text(encoding="utf-8"))
    cards = source_cards_for_transcript(transcript, episode_id=episode_id)
    if not cards:
        raise ValueError("Remote job has no locally rebuildable source cards")
    for card in cards:
        validate_source_card(card)


def fetch_broker_audit_events(
    *,
    broker_url: str,
    token: str,
    limit: int = 100,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    return _post_json(
        broker_url.rstrip("/") + "/audit-events",
        token=token,
        payload={"limit": limit},
        timeout_seconds=timeout_seconds,
    )


def read_token(token: str | None = None, token_file: str | Path | None = None) -> str:
    if token:
        return token
    if token_file:
        return Path(token_file).expanduser().read_text(encoding="utf-8").strip()
    raise ValueError("Missing broker token. Pass --token or --token-file.")


def _post_json(endpoint: str, *, token: str, payload: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload, ensure_ascii=True).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        body = response.read().decode("utf-8")
        parsed = json.loads(body) if body else {}
        if response.status < 200 or response.status >= 300:
            raise RuntimeError(f"{endpoint} returned HTTP {response.status}: {parsed}")
        return parsed


def _lease_until(*, minutes: int) -> str:
    import datetime as dt

    return (dt.datetime.fromisoformat(now_iso()) + dt.timedelta(minutes=minutes)).isoformat()
