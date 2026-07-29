from __future__ import annotations

import datetime as dt
import json
import os
import signal
import shutil
import subprocess
from pathlib import Path
from typing import Any

from . import db
from .discourse import insert_discourse_events
from .ingest import fetch_and_segment_transcript, record_fetch_transcript_failure
from .labels import audit_label_grounding, load_label_pack, local_draft_label, render_prompt, repair_label_output_for_submission, validate_label_output
from .paths import corpus_dir, runs_dir
from .prep import prepare_transcript
from .transcription import run_transcription_job
from .util import dumps_json, loads_json, now_iso, read_text, stable_id, write_text_atomic


CORPUS_TEXT_READ_TIMEOUT_SECONDS = 3.0
# Darwin exposes this as SF_DATALESS in sys/stat.h, but Python's stat module
# does not currently export it.  Checking the inode flag does not hydrate the
# file-provider placeholder.
SF_DATALESS = 0x40000000


def preflight(model: str) -> dict[str, Any]:
    codex = shutil.which("codex")
    result = {"codex_path": codex, "model": model, "codex_cli_available": False, "error": None}
    if not codex:
        result["error"] = "codex command not found"
        return result
    try:
        completed = subprocess.run([codex, "--version"], capture_output=True, text=True, timeout=10)
        result["codex_cli_available"] = completed.returncode == 0
        if completed.returncode != 0:
            result["error"] = (completed.stderr or completed.stdout).strip()
    except Exception as exc:
        result["error"] = str(exc)
    return result


def run_jobs(
    conn,
    *,
    lane: str,
    limit: int,
    model: str,
    label_pack: str,
    worker_id: str,
    local_draft: bool = False,
    claim_prompts: bool = True,
    job_types: tuple[str, ...] | None = None,
    max_label_prompts: int | None = 25,
) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "processed": 0,
        "completed": 0,
        "claimed_prompts": 0,
        "failed": 0,
        "deferred": 0,
        "details": [],
    }
    job_types = job_types or ("fetch_transcript", "transcribe_audio", "prepare_transcript", "episode_context", "label_segment", "audit_label")
    if worker_id.startswith("transcript-discovery") and "label_segment" in job_types:
        raise ValueError("transcript-discovery workers are fetch-only and may not claim label_segment jobs")
    deferred_job_ids: set[int] = set()
    for _ in range(limit):
        job = claim_next_job(
            conn,
            lane=lane,
            worker_id=worker_id,
            job_types=job_types,
            exclude_job_ids=deferred_job_ids,
        )
        if not job:
            break
        readiness = prompt_input_readiness(conn, job)
        if not readiness["ready"]:
            release_job(conn, job["id"])
            deferred_job_ids.add(int(job["id"]))
            reason = "corpus_input_not_hydrated:" + ",".join(
                item["reason"] for item in readiness["unready"]
            )
            conn.execute(
                "UPDATE jobs SET error = ?, updated_at = ? WHERE id = ?",
                (reason, now_iso(), job["id"]),
            )
            stats["deferred"] += 1
            stats["details"].append(
                {
                    "job_id": job["id"],
                    "type": job["job_type"],
                    "result": "deferred_corpus_input_not_ready",
                    "readiness": readiness,
                }
            )
            continue
        stats["processed"] += 1
        try:
            payload = loads_json(job["payload_json"], {})
            if job["job_type"] == "fetch_transcript":
                result = fetch_and_segment_transcript(
                    conn,
                    job["target_id"],
                    label_pack=payload.get("label_pack", label_pack),
                    lane=lane,
                    source_kind=payload.get("source_kind", "creator_provided_rss_transcript"),
                )
                complete_job(conn, job["id"])
                stats["completed"] += 1
                stats["details"].append({"job_id": job["id"], "type": job["job_type"], "result": result})
            elif job["job_type"] == "transcribe_audio":
                result = run_transcription_job(
                    conn,
                    job,
                    lane=lane,
                    label_pack=payload.get("label_pack", label_pack),
                    worker_id=worker_id,
                )
                complete_job(conn, job["id"])
                stats["completed"] += 1
                stats["details"].append({"job_id": job["id"], "type": job["job_type"], "result": result})
            elif job["job_type"] == "prepare_transcript":
                result = prepare_transcript(conn, job["target_id"], force=bool(payload.get("force")))
                complete_job(conn, job["id"])
                stats["completed"] += 1
                stats["details"].append({"job_id": job["id"], "type": job["job_type"], "result": result})
            elif job["job_type"] == "manual_transcript_required":
                complete_job(conn, job["id"])
                stats["completed"] += 1
                stats["details"].append({"job_id": job["id"], "type": job["job_type"], "result": "recorded_missing_transcript"})
            elif job["job_type"] == "episode_context":
                if local_draft:
                    raise ValueError("episode_context requires GPT-5.5 full-episode reading; local-draft context is disabled")
                if not claim_prompts:
                    release_job(conn, job["id"])
                    stats["details"].append({"job_id": job["id"], "type": job["job_type"], "result": "released_no_prompt_claim"})
                    continue
                if max_label_prompts is not None and stats["claimed_prompts"] >= max_label_prompts:
                    release_job(conn, job["id"])
                    stats["details"].append({"job_id": job["id"], "type": job["job_type"], "result": "prompt_cap_reached_released"})
                    break
                effective_pack = payload.get("label_pack", label_pack)
                effective_model = payload.get("model", model)
                claim = create_episode_context_prompt(conn, job, label_pack=effective_pack, model=effective_model, worker_id=worker_id)
                stats["claimed_prompts"] += 1
                stats["details"].append({"job_id": job["id"], "type": job["job_type"], "prompt": claim})
            elif job["job_type"] == "label_segment":
                effective_pack = payload.get("label_pack", label_pack)
                effective_model = payload.get("model", model)
                if local_draft:
                    output = build_local_draft_for_job(conn, job, label_pack=effective_pack, model="local-draft")
                    complete_job(conn, job["id"])
                    stats["completed"] += 1
                    stats["details"].append({"job_id": job["id"], "type": job["job_type"], "result": output["label_id"]})
                elif claim_prompts:
                    if max_label_prompts is not None and stats["claimed_prompts"] >= max_label_prompts:
                        release_job(conn, job["id"])
                        stats["details"].append({"job_id": job["id"], "type": job["job_type"], "result": "prompt_cap_reached_released"})
                        break
                    context_gate = gate_v31_label_on_episode_context(conn, job, label_pack=effective_pack, model=effective_model)
                    if context_gate:
                        stats["details"].append({"job_id": job["id"], "type": job["job_type"], "result": context_gate})
                        continue
                    claim = create_label_prompt(conn, job, label_pack=effective_pack, model=effective_model, worker_id=worker_id)
                    stats["claimed_prompts"] += 1
                    stats["details"].append({"job_id": job["id"], "type": job["job_type"], "prompt": claim})
                else:
                    release_job(conn, job["id"])
            elif job["job_type"] == "audit_label":
                audit_id = perform_label_audit(conn, job, model=model)
                complete_job(conn, job["id"])
                stats["completed"] += 1
                stats["details"].append({"job_id": job["id"], "type": job["job_type"], "result": audit_id})
            else:
                fail_job(conn, job["id"], f"Unknown job type: {job['job_type']}")
                stats["failed"] += 1
        except Exception as exc:
            payload = loads_json(job["payload_json"], {})
            if job["job_type"] == "fetch_transcript":
                record_fetch_transcript_failure(
                    conn,
                    episode_id=job["target_id"],
                    source_kind=payload.get("source_kind", "creator_provided_rss_transcript"),
                    error=str(exc),
                    worker_id=worker_id,
                )
            fail_or_retry_job(conn, job, str(exc))
            stats["failed"] += 1
            stats["details"].append({"job_id": job["id"], "error": str(exc)})
    conn.commit()
    return stats


def claim_next_job(
    conn,
    *,
    lane: str,
    worker_id: str,
    lease_minutes: int = 45,
    job_types: tuple[str, ...] | None = None,
    exclude_job_ids: set[int] | None = None,
):
    now = now_iso()
    leased_until = (dt.datetime.fromisoformat(now) + dt.timedelta(minutes=lease_minutes)).isoformat()
    params: list[Any] = [worker_id, leased_until, now, lane, now]
    job_type_filter = ""
    if job_types:
        placeholders = ", ".join("?" for _ in job_types)
        job_type_filter = f" AND job_type IN ({placeholders})"
        params.extend(job_types)
    excluded_filter = ""
    if exclude_job_ids:
        excluded = sorted(int(job_id) for job_id in exclude_job_ids)
        placeholders = ", ".join("?" for _ in excluded)
        excluded_filter = f" AND id NOT IN ({placeholders})"
        params.extend(excluded)
    claimed = conn.execute(
        f"""
        UPDATE jobs
        SET status = 'claimed',
            lease_owner = ?,
            leased_until = ?,
            attempts = attempts + 1,
            updated_at = ?
        WHERE id = (
          SELECT id FROM jobs
          WHERE lane = ?
            AND attempts < max_attempts
            AND (status = 'pending' OR (status = 'claimed' AND leased_until < ?))
            {job_type_filter}
            {excluded_filter}
          ORDER BY priority ASC, id ASC
          LIMIT 1
        )
        RETURNING *
        """,
        params,
    ).fetchone()
    if not claimed:
        conn.commit()
        return None
    conn.commit()
    return claimed


def complete_job(conn, job_id: int) -> None:
    ts = now_iso()
    conn.execute(
        """
        UPDATE jobs
        SET status = 'completed',
            completed_at = ?,
            updated_at = ?,
            error = NULL,
            lease_owner = NULL,
            leased_until = NULL
        WHERE id = ?
        """,
        (ts, ts, job_id),
    )


def release_job(conn, job_id: int) -> None:
    ts = now_iso()
    conn.execute(
        """
        UPDATE jobs
        SET status = 'pending',
            lease_owner = NULL,
            leased_until = NULL,
            attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
            updated_at = ?
        WHERE id = ?
        """,
        (ts, job_id),
    )


def prompt_input_readiness(conn, job) -> dict[str, Any]:
    """Inspect every filesystem input a prompt job will read without opening it."""

    paths: list[tuple[str, str]] = []
    if job["job_type"] == "label_segment":
        row = conn.execute(
            """
            SELECT episode_id, segment_index, text_path
            FROM segments
            WHERE id = ?
            """,
            (job["target_id"],),
        ).fetchone()
        if row is None:
            return {
                "ready": False,
                "checked": 0,
                "unready": [
                    {
                        "kind": "segment",
                        "id": job["target_id"],
                        "reason": "segment_missing",
                    }
                ],
            }
        payload = loads_json(job["payload_json"], {})
        if payload.get("label_pack") == "ai_discourse_v3_1":
            rows = conn.execute(
                """
                SELECT id, text_path
                FROM segments
                WHERE episode_id = ?
                  AND segment_index BETWEEN ? AND ?
                ORDER BY segment_index
                """,
                (
                    row["episode_id"],
                    int(row["segment_index"]) - 1,
                    int(row["segment_index"]) + 1,
                ),
            ).fetchall()
            paths.extend(
                (f"segment:{neighbor['id']}", neighbor["text_path"])
                for neighbor in rows
            )
        else:
            paths.append((f"segment:{job['target_id']}", row["text_path"]))
    elif job["job_type"] == "episode_context":
        rows = conn.execute(
            """
            SELECT id, text_path
            FROM segments
            WHERE episode_id = ?
            ORDER BY transcript_id, segment_index
            """,
            (job["target_id"],),
        ).fetchall()
        if not rows:
            return {
                "ready": False,
                "checked": 0,
                "unready": [
                    {
                        "kind": "episode_context",
                        "id": job["target_id"],
                        "reason": "episode_segments_missing",
                    }
                ],
            }
        paths.extend(
            (f"segment:{segment['id']}", segment["text_path"])
            for segment in rows
        )
    else:
        return {"ready": True, "checked": 0, "unready": []}

    unready: list[dict[str, Any]] = []
    project_root = corpus_dir().parent
    for identifier, text_path in paths:
        path = project_root / text_path
        identity = corpus_path_readiness(path)
        if not identity["ready"]:
            unready.append(
                {
                    "kind": identifier.split(":", 1)[0],
                    "id": identifier.split(":", 1)[1],
                    "reason": identity["reason"],
                    "text_path": text_path,
                }
            )
    return {
        "ready": not unready,
        "checked": len(paths),
        "unready": unready,
    }


def corpus_path_readiness(path: str | Path) -> dict[str, Any]:
    """Return storage readiness without causing File Provider materialization."""

    resolved = Path(path)
    try:
        info = os.stat(resolved)
    except FileNotFoundError:
        return {"ready": False, "reason": "missing"}
    except OSError as exc:
        return {
            "ready": False,
            "reason": f"stat_failed:{exc.__class__.__name__}:{exc.errno}",
        }
    flags = int(getattr(info, "st_flags", 0))
    if flags & SF_DATALESS:
        return {
            "ready": False,
            "reason": "dataless",
            "size_bytes": int(info.st_size),
            "flags": flags,
        }
    if not resolved.is_file():
        return {"ready": False, "reason": "not_regular_file", "flags": flags}
    return {
        "ready": True,
        "reason": None,
        "size_bytes": int(info.st_size),
        "flags": flags,
    }


def fail_job(conn, job_id: int, reason: str) -> None:
    ts = now_iso()
    conn.execute(
        "UPDATE jobs SET status = 'failed', error = ?, updated_at = ?, lease_owner = NULL, leased_until = NULL WHERE id = ?",
        (reason, ts, job_id),
    )


def fail_or_retry_job(conn, job, reason: str) -> None:
    if job["attempts"] >= job["max_attempts"]:
        fail_job(conn, job["id"], reason)
    else:
        ts = now_iso()
        conn.execute(
            """
            UPDATE jobs
            SET status = 'pending', lease_owner = NULL, leased_until = NULL, error = ?, updated_at = ?
            WHERE id = ?
            """,
            (reason, ts, job["id"]),
        )


EPISODE_CONTEXT_SCHEMA_VERSION = "ai_discourse_v3_1_episode_context"


def gate_v31_label_on_episode_context(conn, job, *, label_pack: str, model: str) -> dict[str, Any] | None:
    if label_pack != "ai_discourse_v3_1":
        return None
    if model != "gpt-5.5":
        raise ValueError("ai_discourse_v3_1 requires model gpt-5.5 so every podcast is read by the intended extractor")
    context = completed_episode_context_for_segment(conn, job["target_id"], label_pack=label_pack, model=model)
    if context:
        return None
    failed_context = failed_episode_context_for_segment(conn, job["target_id"], label_pack=label_pack, model=model)
    if failed_context:
        fail_job(conn, job["id"], "required_episode_context_failed")
        conn.commit()
        return {
            "status": "episode_context_failed",
            "message": "v3.1 segment extraction skipped because the required full-episode context job failed.",
            "failed_label_job_id": job["id"],
            "episode_context_job_id": failed_context["id"],
        }
    context_job_id = enqueue_episode_context_job_for_segment(conn, job, label_pack=label_pack, model=model)
    release_job(conn, job["id"])
    conn.commit()
    return {
        "status": "waiting_for_episode_context",
        "message": "v3.1 segment extraction waits until a GPT-5.5 full-episode context pass completes.",
        "released_label_job_id": job["id"],
        "episode_context_job_id": context_job_id,
    }


def enqueue_episode_context_job_for_segment(conn, job, *, label_pack: str, model: str) -> int | None:
    row = conn.execute(
        """
        SELECT episode_id
        FROM segments
        WHERE id = ?
        """,
        (job["target_id"],),
    ).fetchone()
    if not row:
        raise ValueError(f"Segment not found: {job['target_id']}")
    payload = loads_json(job["payload_json"], {})
    context_payload = {
        "label_pack": label_pack,
        "model": model,
        "episode_context_version": EPISODE_CONTEXT_SCHEMA_VERSION,
        "priority_reason": "required_before_v3_1_segment_extraction",
    }
    if payload.get("pilot_id"):
        context_payload["pilot_id"] = payload["pilot_id"]
    priority = max(int(job["priority"] or 100) - 1, 0)
    context_job_id = db.enqueue_job(
        conn,
        lane=job["lane"],
        job_type="episode_context",
        target_id=row["episode_id"],
        payload=context_payload,
        priority=priority,
        max_attempts=2,
    )
    if context_job_id:
        existing = conn.execute("SELECT status FROM jobs WHERE id = ?", (context_job_id,)).fetchone()
        if existing and existing["status"] == "completed":
            conn.execute(
                """
                UPDATE jobs
                SET status = 'pending',
                    attempts = 0,
                    completed_at = NULL,
                    error = NULL,
                    priority = MIN(priority, ?),
                    updated_at = ?
                WHERE id = ?
                """,
                (priority, now_iso(), context_job_id),
            )
    return context_job_id


def completed_episode_context_for_segment(conn, segment_id: str, *, label_pack: str, model: str):
    row = conn.execute("SELECT episode_id FROM segments WHERE id = ?", (segment_id,)).fetchone()
    if not row:
        raise ValueError(f"Segment not found: {segment_id}")
    return completed_episode_context_for_episode(conn, row["episode_id"], label_pack=label_pack, model=model)


def failed_episode_context_for_segment(conn, segment_id: str, *, label_pack: str, model: str):
    row = conn.execute("SELECT episode_id FROM segments WHERE id = ?", (segment_id,)).fetchone()
    if not row:
        raise ValueError(f"Segment not found: {segment_id}")
    return conn.execute(
        """
        SELECT id, error
        FROM jobs
        WHERE job_type = 'episode_context'
          AND target_id = ?
          AND status = 'failed'
          AND json_extract(payload_json, '$.label_pack') = ?
          AND json_extract(payload_json, '$.model') = ?
          AND json_extract(payload_json, '$.episode_context_version') = ?
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        """,
        (row["episode_id"], label_pack, model, EPISODE_CONTEXT_SCHEMA_VERSION),
    ).fetchone()


def completed_episode_context_for_episode(conn, episode_id: str, *, label_pack: str, model: str):
    row = conn.execute(
        """
        SELECT *
        FROM episode_context_runs
        WHERE episode_id = ?
          AND label_pack = ?
          AND model = ?
          AND status = 'completed'
          AND context_artifact_path IS NOT NULL
        """,
        (episode_id, label_pack, model),
    ).fetchone()
    if not row:
        return None
    artifact_path = Path(row["context_artifact_path"]).expanduser()
    if not artifact_path.exists():
        return None
    return row


def create_episode_context_prompt(conn, job, *, label_pack: str, model: str, worker_id: str) -> dict[str, str]:
    if label_pack != "ai_discourse_v3_1":
        raise ValueError("episode_context jobs are currently only supported for ai_discourse_v3_1")
    if model != "gpt-5.5":
        raise ValueError("ai_discourse_v3_1 episode_context requires model gpt-5.5")
    existing = completed_episode_context_for_episode(conn, job["target_id"], label_pack=label_pack, model=model)
    if existing:
        complete_job(conn, job["id"])
        conn.commit()
        return {
            "job_id": str(job["id"]),
            "episode_context_run_id": existing["id"],
            "context_artifact_path": existing["context_artifact_path"],
            "status": "already_completed",
        }
    episode_context = episode_context_for_episode(conn, job["target_id"])
    prompt = render_episode_context_prompt(label_pack, episode_context)
    run_id = stable_id(job["target_id"], label_pack, model, prefix="ectx_")
    prompt_path = runs_dir() / "prompts" / f"{run_id}.md"
    output_path = runs_dir() / "outputs" / f"{run_id}.json"
    write_text_atomic(prompt_path, prompt)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ts = now_iso()
    transcript_id = episode_context["episode"].get("transcript_id")
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
        (
            run_id,
            job["id"],
            job["target_id"],
            transcript_id,
            label_pack,
            model,
            str(prompt_path),
            str(output_path),
            ts,
            ts,
        ),
    )
    conn.execute(
        "UPDATE jobs SET payload_json = ?, updated_at = ? WHERE id = ?",
        (
            dumps_json(
                {
                    **loads_json(job["payload_json"], {}),
                    "label_pack": label_pack,
                    "model": model,
                    "episode_context_run_id": run_id,
                    "episode_context_schema_version": EPISODE_CONTEXT_SCHEMA_VERSION,
                    "prompt_path": str(prompt_path),
                    "output_path": str(output_path),
                }
            ),
            ts,
            job["id"],
        ),
    )
    conn.commit()
    return {"job_id": str(job["id"]), "episode_context_run_id": run_id, "prompt_path": str(prompt_path), "output_path": str(output_path)}


def render_episode_context_prompt(label_pack: str, episode_context: dict[str, Any]) -> str:
    pack = load_label_pack(label_pack)
    static = {
        "label_pack": pack.name,
        "label_pack_version": pack.version,
        "episode_context_schema_version": EPISODE_CONTEXT_SCHEMA_VERSION,
        "output_schema": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "schema_version",
                "episode_id",
                "context_summary",
                "speaker_map",
                "section_map",
                "entity_seed",
                "concept_seed",
                "extraction_guidance",
                "quality_flags",
                "overall_confidence",
                "needs_review",
                "review_reason",
            ],
            "properties": {
                "schema_version": {"const": EPISODE_CONTEXT_SCHEMA_VERSION},
                "episode_id": {"type": "string"},
                "context_summary": {"type": "string"},
                "speaker_map": {"type": "array"},
                "section_map": {"type": "array"},
                "entity_seed": {"type": "object"},
                "concept_seed": {"type": "array"},
                "extraction_guidance": {"type": "string"},
                "quality_flags": {"type": "array"},
                "overall_confidence": {"type": "number"},
                "needs_review": {"type": "boolean"},
                "review_reason": {"type": ["string", "null"]},
            },
        },
    }
    return "\n\n".join(
        [
            "# Static Instructions",
            "You are the GPT-5.5 full-episode context reader for ai_discourse_v3_1.",
            "Read the entire prepared episode transcript below. Produce a compact reusable context artifact for later segment-level extraction.",
            "Do not extract final discourse events here. Do not include the full transcript or long transcript passages in the JSON output.",
            "The artifact should help later extractors map speakers, aliases, orgs, products, models, sections, recurring concepts, and likely high-value discourse shifts.",
            "Build the speaker_map as an explicit roster: hosts, guests, quoted/reported actors, affiliations, titles, aliases, handles, misspellings, and confidence. Preserve uncertainty instead of merging names.",
            "In extraction_guidance, infer the episode's actual domain and label scope from the complete transcript, describe how later extractors should distinguish substantive dialogue, quoted sources, ads, setup, page chrome, and mixed passages, and call out who-mentioned-whom patterns, authority clues, aliases, and transcript residue. Make these decisions semantically; do not propose keyword, regex, or phrase gates.",
            "# v3.1 Codebook Reference",
            pack.codebook.strip() or pack.prompt.strip(),
            "# Context Output Schema",
            json.dumps(static, ensure_ascii=True, indent=2, sort_keys=True),
            "# Episode Metadata",
            json.dumps(episode_context["episode"], ensure_ascii=True, indent=2, sort_keys=True),
            "# Full Prepared Episode Transcript",
            episode_context["full_segmented_episode_text"],
            "# Output Contract",
            "Return only one JSON object. Keep it compact and reusable. No Markdown fences. No local paths. No full transcript text. Short evidence snippets are allowed only when needed to justify speaker/entity context.",
        ]
    )


def submit_episode_context_output(
    conn,
    *,
    job_id: int,
    output_json_path: str | Path,
    worker_id: str | None = None,
    allow_expired: bool = False,
    commit: bool = True,
) -> dict[str, str]:
    job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not job:
        raise ValueError(f"Job not found: {job_id}")
    if job["job_type"] != "episode_context":
        raise ValueError(f"Job {job_id} is {job['job_type']}, not episode_context")
    if job["status"] != "claimed":
        raise ValueError(f"Job {job_id} is {job['status']}, not claimed")
    if worker_id and job["lease_owner"] and worker_id != job["lease_owner"]:
        raise ValueError(f"Job {job_id} is leased to {job['lease_owner']}, not {worker_id}")
    leased_until = job["leased_until"]
    if leased_until and leased_until < now_iso() and not allow_expired:
        raise ValueError(f"Job {job_id} lease expired at {leased_until}")
    payload = loads_json(job["payload_json"], {})
    context_run_id = payload.get("episode_context_run_id")
    if not context_run_id:
        raise ValueError(f"Job {job_id} does not specify an episode_context_run_id")
    output_path = Path(output_json_path).expanduser().resolve()
    expected_output = payload.get("output_path")
    if expected_output and output_path != Path(expected_output).expanduser().resolve():
        raise ValueError(f"Output path does not match current episode_context handoff for job {job_id}")
    context_run = conn.execute("SELECT * FROM episode_context_runs WHERE id = ?", (context_run_id,)).fetchone()
    if not context_run:
        raise ValueError(f"Episode context run not found: {context_run_id}")
    if context_run["status"] != "claimed":
        raise ValueError(f"Episode context run {context_run_id} is {context_run['status']}, not claimed")
    output = json.loads(output_path.read_text(encoding="utf-8"))
    artifact = validate_episode_context_output(output, expected_episode_id=job["target_id"])
    artifact_path = runs_dir() / "episode_contexts" / f"{context_run_id}.json"
    write_text_atomic(artifact_path, json.dumps(artifact, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    ts = now_iso()
    conn.execute(
        """
        UPDATE episode_context_runs
        SET status = 'completed',
            context_artifact_path = ?,
            speaker_map_json = ?,
            section_map_json = ?,
            entity_seed_json = ?,
            concept_seed_json = ?,
            extraction_guidance = ?,
            error = NULL,
            completed_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            str(artifact_path),
            dumps_json(artifact["speaker_map"]),
            dumps_json(artifact["section_map"]),
            dumps_json(artifact["entity_seed"]),
            dumps_json(artifact["concept_seed"]),
            artifact["extraction_guidance"],
            ts,
            ts,
            context_run_id,
        ),
    )
    complete_job(conn, job_id)
    if commit:
        conn.commit()
    return {"job_id": str(job_id), "episode_context_run_id": context_run_id, "context_artifact_path": str(artifact_path)}


def validate_episode_context_output(output: dict[str, Any], *, expected_episode_id: str) -> dict[str, Any]:
    if not isinstance(output, dict):
        raise ValueError("Episode context output must be a JSON object")
    required = {
        "schema_version",
        "episode_id",
        "context_summary",
        "speaker_map",
        "section_map",
        "entity_seed",
        "concept_seed",
        "extraction_guidance",
        "quality_flags",
        "overall_confidence",
        "needs_review",
        "review_reason",
    }
    missing = sorted(required - set(output))
    if missing:
        raise ValueError("Episode context output missing required fields: " + ", ".join(missing))
    if output["schema_version"] != EPISODE_CONTEXT_SCHEMA_VERSION:
        raise ValueError(f"Episode context schema_version must be {EPISODE_CONTEXT_SCHEMA_VERSION}")
    if output["episode_id"] != expected_episode_id:
        raise ValueError("Episode context episode_id does not match job target")
    if not isinstance(output["speaker_map"], list):
        raise ValueError("Episode context speaker_map must be a list")
    if not isinstance(output["section_map"], list):
        raise ValueError("Episode context section_map must be a list")
    if not isinstance(output["entity_seed"], dict):
        raise ValueError("Episode context entity_seed must be an object")
    if not isinstance(output["concept_seed"], list):
        raise ValueError("Episode context concept_seed must be a list")
    if not isinstance(output["quality_flags"], list):
        raise ValueError("Episode context quality_flags must be a list")
    if not isinstance(output["extraction_guidance"], str) or len(output["extraction_guidance"].split()) < 6:
        raise ValueError("Episode context extraction_guidance must be a useful string")
    if not isinstance(output["context_summary"], str):
        raise ValueError("Episode context context_summary must be a string")
    if isinstance(output["overall_confidence"], bool) or not isinstance(
        output["overall_confidence"], (int, float)
    ):
        raise ValueError("Episode context overall_confidence must be a number")
    if not isinstance(output["needs_review"], bool):
        raise ValueError("Episode context needs_review must be a boolean")
    if output["review_reason"] is not None and not isinstance(output["review_reason"], str):
        raise ValueError("Episode context review_reason must be a string or null")

    persisted_fields = (
        "context_summary",
        "speaker_map",
        "section_map",
        "entity_seed",
        "concept_seed",
    )
    has_canonical_authority = (
        "episode_context" in output or "excluded_source_context" in output
    )
    if has_canonical_authority:
        if "episode_context" not in output or "excluded_source_context" not in output:
            raise ValueError(
                "Canonical episode context output requires episode_context and excluded_source_context"
            )
        nested = output["episode_context"]
        exclusions = output["excluded_source_context"]
        if not isinstance(nested, dict) or set(nested) != set(persisted_fields):
            raise ValueError(
                "Canonical episode_context must contain exactly the persisted context fields"
            )
        if not isinstance(exclusions, list) or any(
            not isinstance(item, str) for item in exclusions
        ):
            raise ValueError("Episode context excluded_source_context must be a string array")
        if any(nested[field] != output[field] for field in persisted_fields):
            raise ValueError(
                "Canonical episode_context fields must exactly match the persisted top-level fields"
            )
    else:
        # Preserve compatibility with the existing worker prompt while writing the
        # same canonical artifact shape used by the managed app-server runner.
        nested = {field: output[field] for field in persisted_fields}
        exclusions = []
    serialized = json.dumps(output, ensure_ascii=True, sort_keys=True)
    forbidden_markers = ["full_segmented_episode_text", "===== SEGMENT", "WEBVTT"]
    for marker in forbidden_markers:
        if marker in serialized:
            raise ValueError(f"Episode context artifact appears to contain raw transcript marker: {marker}")
    for path, value in _iter_strings(output):
        if len(value) > 6000:
            raise ValueError(f"Episode context field {path} is too large for a compact reusable artifact")
    return {
        "schema_version": output["schema_version"],
        "episode_id": output["episode_id"],
        "context_summary": output["context_summary"],
        "speaker_map": output["speaker_map"],
        "section_map": output["section_map"],
        "entity_seed": output["entity_seed"],
        "concept_seed": output["concept_seed"],
        "extraction_guidance": output["extraction_guidance"],
        "quality_flags": output["quality_flags"],
        "overall_confidence": output["overall_confidence"],
        "needs_review": output["needs_review"],
        "review_reason": output["review_reason"],
        "episode_context": dict(nested),
        "excluded_source_context": list(exclusions),
    }


def _iter_strings(value: Any, path: str = "$"):
    if isinstance(value, str):
        yield path, value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from _iter_strings(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _iter_strings(item, f"{path}[{index}]")


def create_label_prompt(conn, job, *, label_pack: str, model: str, worker_id: str) -> dict[str, str]:
    if label_pack == "ai_discourse_v3_1" and model != "gpt-5.5":
        raise ValueError("ai_discourse_v3_1 requires model gpt-5.5 so every podcast is read by the intended extractor")
    segment_context = segment_for_job(conn, job)
    if label_pack == "ai_discourse_v3_1":
        context_run = completed_episode_context_for_segment(conn, job["target_id"], label_pack=label_pack, model=model)
        if not context_run:
            raise ValueError("ai_discourse_v3_1 label prompt requires a completed GPT-5.5 episode_context run")
        artifact = json.loads(Path(context_run["context_artifact_path"]).read_text(encoding="utf-8"))
        segment_context["context"]["episode_context_artifact"] = artifact
        segment_context["context"]["episode_context_run_id"] = context_run["id"]
        segment_context["context"]["adjacent_segment_context"] = adjacent_segment_context_for_segment(conn, job["target_id"])
        segment_context["context"]["episode_context_contract"] = (
            "This compact artifact came from a GPT-5.5 full-episode read. Use it for speaker/entity/concept context, "
            "Use adjacent_segment_context to resolve speaker continuity across segment boundaries, "
            "but emit evidence only from the current Segment Text section."
        )
    prompt = render_prompt(label_pack, {"text": segment_context["segment_text"]}, segment_context["context"])
    run_id = stable_id(str(job["id"]), label_pack, model, now_iso(), prefix="run_")
    prompt_path = runs_dir() / "prompts" / f"{run_id}.md"
    output_path = runs_dir() / "outputs" / f"{run_id}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(prompt_path, prompt)
    pack = load_label_pack(label_pack)
    ts = now_iso()
    conn.execute(
        """
        UPDATE label_runs
        SET status = 'superseded',
            error = COALESCE(error, 'Superseded by a later prompt claim.'),
            updated_at = ?
        WHERE job_id = ?
          AND status = 'claimed'
        """,
        (ts, job["id"]),
    )
    conn.execute(
        """
        INSERT INTO label_runs
          (id, job_id, segment_id, label_pack, model, prompt_path, output_path, status, claimed_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'claimed', ?, ?, ?)
        """,
        (
            run_id,
            job["id"],
            job["target_id"],
            label_pack,
            model,
            str(prompt_path),
            str(output_path),
            ts,
            ts,
            ts,
        ),
    )
    conn.execute(
        "UPDATE jobs SET payload_json = ?, updated_at = ? WHERE id = ?",
        (
            dumps_json({**loads_json(job["payload_json"], {}), "label_pack": label_pack, "model": model, "label_pack_version": pack.version, "prompt_path": str(prompt_path), "output_path": str(output_path), "label_run_id": run_id}),
            ts,
            job["id"],
        ),
    )
    conn.commit()
    return {"job_id": str(job["id"]), "label_run_id": run_id, "prompt_path": str(prompt_path), "output_path": str(output_path)}


def submit_label_output(
    conn,
    *,
    job_id: int,
    output_json_path: str | Path,
    worker_id: str | None = None,
    allow_expired: bool = False,
    repair_output: bool = True,
    derive_semantics: bool = True,
) -> dict[str, str]:
    if not isinstance(repair_output, bool):
        raise ValueError("repair_output must be boolean")
    if not isinstance(derive_semantics, bool):
        raise ValueError("derive_semantics must be boolean")
    job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not job:
        raise ValueError(f"Job not found: {job_id}")
    if job["status"] != "claimed":
        raise ValueError(f"Job {job_id} is {job['status']}, not claimed")
    if worker_id and job["lease_owner"] and worker_id != job["lease_owner"]:
        raise ValueError(f"Job {job_id} is leased to {job['lease_owner']}, not {worker_id}")
    leased_until = job["leased_until"]
    if leased_until and leased_until < now_iso() and not allow_expired:
        raise ValueError(f"Job {job_id} lease expired at {leased_until}")
    payload = loads_json(job["payload_json"], {})
    label_pack = payload.get("label_pack")
    if not label_pack:
        raise ValueError(f"Job {job_id} does not specify a label_pack")
    output_path = Path(output_json_path).expanduser().resolve()
    expected_output = payload.get("output_path")
    if expected_output and output_path != Path(expected_output).expanduser().resolve():
        raise ValueError(f"Output path does not match current job handoff for job {job_id}")
    label_run_id = payload.get("label_run_id")
    if not label_run_id:
        raise ValueError(f"Job {job_id} does not specify a label_run_id")
    label_run = conn.execute("SELECT * FROM label_runs WHERE id = ?", (label_run_id,)).fetchone()
    if not label_run:
        raise ValueError(f"Label run not found: {label_run_id}")
    if label_run["status"] != "claimed":
        raise ValueError(f"Label run {label_run_id} is {label_run['status']}, not claimed")
    if label_run["job_id"] != job_id:
        raise ValueError(f"Label run {label_run_id} does not belong to job {job_id}")
    if label_run["segment_id"] != job["target_id"]:
        raise ValueError(f"Label run {label_run_id} segment does not match job {job_id}")
    if label_run["label_pack"] != label_pack:
        raise ValueError(f"Label run {label_run_id} label_pack does not match job {job_id}")
    expected_model = payload.get("model")
    if expected_model and label_run["model"] != expected_model:
        raise ValueError(f"Label run {label_run_id} model does not match job {job_id}")
    if label_run["output_path"] and output_path != Path(label_run["output_path"]).expanduser().resolve():
        raise ValueError(f"Output path does not match label run {label_run_id}")
    segment_context = segment_for_job(conn, job)
    output = json.loads(output_path.read_text(encoding="utf-8"))
    if repair_output:
        repair_label_output_for_submission(
            label_pack,
            output,
            segment_text=segment_context["segment_text"],
        )
    validate_label_output(label_pack, output)
    if output.get("segment_id") != job["target_id"]:
        raise ValueError(f"Output segment_id does not match job target_id for job {job_id}")
    if output.get("episode_id") != segment_context["context"]["episode_id"]:
        raise ValueError(f"Output episode_id does not match job episode_id for job {job_id}")
    validate_label_output(label_pack, output, segment_text=segment_context["segment_text"])
    label_id = insert_label(
        conn,
        segment_id=job["target_id"],
        label_pack=label_pack,
        model=payload.get("model", "codex-app"),
        output=output,
        worker_id=worker_id or job["lease_owner"],
        prompt_path=payload.get("prompt_path"),
        output_path=str(output_path),
        derive_semantics=derive_semantics,
    )
    complete_job(conn, job_id)
    if payload.get("label_run_id"):
        ts = now_iso()
        updated = conn.execute(
            """
            UPDATE label_runs
            SET status = 'completed',
                completed_at = ?,
                updated_at = ?
            WHERE id = ?
              AND job_id = ?
              AND status = 'claimed'
            """,
            (ts, ts, payload["label_run_id"], job_id),
        )
        if updated.rowcount != 1:
            raise ValueError(f"Label run {payload['label_run_id']} was not completed for job {job_id}")
    conn.commit()
    return {"label_id": label_id, "job_id": str(job_id)}


def recover_label_handoffs(conn, *, release_missing_outputs: bool = False) -> dict[str, int]:
    ts = now_iso()
    stats = {"checked": 0, "missing_outputs": 0, "released_jobs": 0, "failed_runs": 0}
    rows = conn.execute(
        """
        SELECT label_runs.*, jobs.status AS job_status, jobs.payload_json
        FROM label_runs
        LEFT JOIN jobs ON jobs.id = label_runs.job_id
        WHERE label_runs.status = 'claimed'
        ORDER BY label_runs.created_at ASC
        """
    ).fetchall()
    for row in rows:
        stats["checked"] += 1
        output_path = Path(row["output_path"]).expanduser() if row["output_path"] else None
        output_missing = not output_path or not output_path.exists()
        if not output_missing:
            continue
        stats["missing_outputs"] += 1
        if not release_missing_outputs:
            continue
        conn.execute(
            """
            UPDATE label_runs
            SET status = 'failed',
                error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            ("Recovered stranded handoff with missing output JSON; job released for fresh claim.", ts, row["id"]),
        )
        stats["failed_runs"] += 1
        if row["job_id"] and row["job_status"] != "completed":
            payload = loads_json(row["payload_json"], {})
            for key in ["prompt_path", "output_path", "label_run_id"]:
                payload.pop(key, None)
            payload["recovered_label_run_id"] = row["id"]
            conn.execute(
                """
                    UPDATE jobs
                    SET status = 'pending',
                        lease_owner = NULL,
                        leased_until = NULL,
                        attempts = CASE WHEN max_attempts > 0 AND attempts >= max_attempts THEN max_attempts - 1 ELSE attempts END,
                        payload_json = ?,
                        error = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                (
                    dumps_json(payload),
                    "Recovered missing label output; ready for fresh label claim.",
                    ts,
                    row["job_id"],
                ),
            )
            stats["released_jobs"] += 1
    conn.commit()
    return stats


def build_local_draft_for_job(conn, job, *, label_pack: str, model: str) -> dict[str, str]:
    if label_pack == "ai_discourse_v3_1":
        raise ValueError("ai_discourse_v3_1 requires GPT-5.5 full-episode extraction; local-draft extraction is disabled")
    segment_context = segment_for_job(conn, job)
    output = local_draft_label(label_pack, segment_text=segment_context["segment_text"], context=segment_context["context"])
    label_id = insert_label(conn, segment_id=job["target_id"], label_pack=label_pack, model=model, output=output)
    return {"label_id": label_id}


def insert_label(
    conn,
    *,
    segment_id: str,
    label_pack: str,
    model: str,
    output: dict[str, Any],
    worker_id: str | None = None,
    prompt_path: str | None = None,
    output_path: str | None = None,
    derive_semantics: bool = True,
) -> str:
    if not isinstance(derive_semantics, bool):
        raise ValueError("derive_semantics must be boolean")
    pack = load_label_pack(label_pack)
    validate_label_output(label_pack, output, segment_text=segment_text_by_id(conn, segment_id))
    label_id = stable_id(segment_id, label_pack, pack.version, model, prefix="lbl_")
    ts = now_iso()
    confidence = float(output.get("overall_confidence") or 0)
    needs_review = 1 if output.get("needs_review") else 0
    conn.execute(
        """
        INSERT INTO labels
          (id, segment_id, label_pack, label_pack_version, model, status, output_json, confidence, needs_review, worker_id, prompt_path, output_path, created_at)
        VALUES (?, ?, ?, ?, ?, 'ready', ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(segment_id, label_pack, label_pack_version, model) DO UPDATE SET
          output_json = excluded.output_json,
          confidence = excluded.confidence,
          needs_review = excluded.needs_review,
          worker_id = excluded.worker_id,
          prompt_path = excluded.prompt_path,
          output_path = excluded.output_path
        """,
        (
            label_id,
            segment_id,
            label_pack,
            pack.version,
            model,
            json.dumps(output, ensure_ascii=True, indent=2, sort_keys=True),
            confidence,
            needs_review,
            worker_id,
            prompt_path,
            output_path,
            ts,
        ),
    )
    _delete_label_derivatives(conn, label_id)
    if derive_semantics:
        for claim in _claim_like_items(output):
            text = claim.get("claim_text") or claim.get("narrative") or claim.get("signal") or claim.get("description")
            if not text:
                continue
            claim_id = stable_id(label_id, text, prefix="clm_")
            conn.execute(
                """
                INSERT OR IGNORE INTO claims
                  (id, label_id, segment_id, text, stance, confidence, evidence_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    claim_id,
                    label_id,
                    segment_id,
                    text,
                    claim.get("stance") or claim.get("claim_type"),
                    claim.get("confidence"),
                    dumps_json({"evidence": claim.get("evidence")}),
                    ts,
                ),
            )
        _insert_coded_observations(conn, output, segment_id=segment_id, label_id=label_id)
        _insert_discourse_event_derivatives(conn, output, segment_id=segment_id, label_id=label_id)
        _insert_entities(conn, output, segment_id=segment_id, label_id=label_id)
    return label_id


def segment_for_job(conn, job, *, include_episode_text: bool = False) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
          segments.*,
          episodes.title AS episode_title,
          episodes.published_at AS episode_published_at,
          sources.name AS source_name,
          transcript_preparations.id AS transcript_preparation_id,
          transcript_preparations.artifact_type AS transcript_artifact_type,
          transcript_preparations.status AS transcript_preparation_status,
          transcript_preparations.substantive_word_count AS transcript_substantive_word_count,
          transcript_preparations.boilerplate_ratio AS transcript_boilerplate_ratio,
          transcript_preparations.speaker_turn_count AS transcript_speaker_turn_count,
          transcript_preparations.quality_score AS transcript_quality_score
        FROM segments
        JOIN episodes ON episodes.id = segments.episode_id
        JOIN sources ON sources.id = segments.source_id
        LEFT JOIN transcript_preparations ON transcript_preparations.transcript_id = segments.transcript_id
        WHERE segments.id = ?
        """,
        (job["target_id"],),
    ).fetchone()
    if not row:
        raise ValueError(f"Segment not found: {job['target_id']}")
    segment_text = _read_corpus_text_with_timeout(row["text_path"], error_prefix="segment")
    context = {
        "segment_id": row["id"],
        "episode_id": row["episode_id"],
        "source_id": row["source_id"],
        "source_name": row["source_name"],
        "episode_title": row["episode_title"],
        "episode_published_at": row["episode_published_at"],
        "segment_index": row["segment_index"],
        "start_char": row["start_char"],
        "end_char": row["end_char"],
        "transcript_preparation_id": row["transcript_preparation_id"],
        "transcript_artifact_type": row["transcript_artifact_type"],
        "transcript_preparation_status": row["transcript_preparation_status"],
        "transcript_substantive_word_count": row["transcript_substantive_word_count"],
        "transcript_boilerplate_ratio": row["transcript_boilerplate_ratio"],
        "transcript_speaker_turn_count": row["transcript_speaker_turn_count"],
        "transcript_quality_score": row["transcript_quality_score"],
        "privacy_boundary": "private_analysis_only_do_not_output_full_transcript",
    }
    if include_episode_text:
        context["full_episode_context"] = episode_context_for_segment(conn, row)
    return {"context": context, "segment_text": segment_text}


def episode_context_for_episode(conn, episode_id: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT
          segments.id,
          segments.transcript_id,
          segments.episode_id,
          segments.source_id,
          segments.segment_index,
          segments.start_char,
          segments.end_char,
          segments.word_count,
          segments.text_path,
          episodes.title AS episode_title,
          episodes.published_at AS episode_published_at,
          sources.name AS source_name,
          transcript_preparations.id AS transcript_preparation_id,
          transcript_preparations.artifact_type AS transcript_artifact_type,
          transcript_preparations.status AS transcript_preparation_status,
          transcript_preparations.substantive_word_count AS transcript_substantive_word_count,
          transcript_preparations.boilerplate_ratio AS transcript_boilerplate_ratio,
          transcript_preparations.speaker_turn_count AS transcript_speaker_turn_count,
          transcript_preparations.quality_score AS transcript_quality_score
        FROM segments
        JOIN episodes ON episodes.id = segments.episode_id
        JOIN sources ON sources.id = segments.source_id
        LEFT JOIN transcript_preparations ON transcript_preparations.transcript_id = segments.transcript_id
        WHERE segments.episode_id = ?
        ORDER BY segments.transcript_id, segments.segment_index
        """,
        (episode_id,),
    ).fetchall()
    if not rows:
        raise ValueError(f"No prepared segments found for episode: {episode_id}")
    parts = []
    total_words = 0
    for row in rows:
        text = _read_corpus_text_with_timeout(row["text_path"], error_prefix="episode_context_segment")
        total_words += int(row["word_count"] or len(text.split()))
        parts.append(
            "\n".join(
                [
                    f"===== SEGMENT {row['segment_index']} | {row['id']} | chars={row['start_char']}-{row['end_char']} | words={row['word_count']} =====",
                    text,
                ]
            )
        )
    first = rows[0]
    return {
        "episode": {
            "episode_id": first["episode_id"],
            "transcript_id": first["transcript_id"],
            "source_id": first["source_id"],
            "source_name": first["source_name"],
            "episode_title": first["episode_title"],
            "episode_published_at": first["episode_published_at"],
            "transcript_preparation_id": first["transcript_preparation_id"],
            "transcript_artifact_type": first["transcript_artifact_type"],
            "transcript_preparation_status": first["transcript_preparation_status"],
            "transcript_substantive_word_count": first["transcript_substantive_word_count"],
            "transcript_boilerplate_ratio": first["transcript_boilerplate_ratio"],
            "transcript_speaker_turn_count": first["transcript_speaker_turn_count"],
            "transcript_quality_score": first["transcript_quality_score"],
            "segment_count": len(rows),
            "total_segment_words": total_words,
            "privacy_boundary": "private_analysis_only_do_not_output_full_transcript",
        },
        "full_segmented_episode_text": "\n\n".join(parts),
    }


def _read_corpus_text_with_timeout(text_path: str, *, error_prefix: str) -> str:
    path = corpus_dir().parent / text_path
    previous_handler = signal.getsignal(signal.SIGALRM)

    def timeout_handler(_signum, _frame):
        raise TimeoutError(f"{error_prefix}_read_timeout")

    try:
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.setitimer(signal.ITIMER_REAL, CORPUS_TEXT_READ_TIMEOUT_SECONDS)
        return path.read_text(encoding="utf-8")
    except TimeoutError as exc:
        raise ValueError(str(exc)) from None
    except OSError as exc:
        raise ValueError(f"{error_prefix}_read_failed:{exc.__class__.__name__}:{exc.errno}") from None
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def episode_context_for_segment(conn, segment_row) -> dict[str, Any]:
    episode_context = episode_context_for_episode(conn, segment_row["episode_id"])
    return {
        "context_scope": "full_prepared_episode_transcript",
        "instruction": "Use this full episode text for context, speaker mapping, section awareness, and concept discovery. Emit evidence only from the current Segment Text section.",
        "segment_count": episode_context["episode"]["segment_count"],
        "total_segment_words": episode_context["episode"]["total_segment_words"],
        "full_segmented_episode_text": episode_context["full_segmented_episode_text"],
    }


def adjacent_segment_context_for_segment(conn, segment_id: str, *, max_chars: int = 3500) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT id, episode_id, segment_index
        FROM segments
        WHERE id = ?
        """,
        (segment_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"Segment not found: {segment_id}")
    neighbors = conn.execute(
        """
        SELECT id, segment_index, start_char, end_char, text_path
        FROM segments
        WHERE episode_id = ?
          AND segment_index BETWEEN ? AND ?
        ORDER BY segment_index
        """,
        (row["episode_id"], int(row["segment_index"]) - 1, int(row["segment_index"]) + 1),
    ).fetchall()
    items = []
    for neighbor in neighbors:
        text = _read_corpus_text_with_timeout(neighbor["text_path"], error_prefix="adjacent_segment")
        role = "current"
        if neighbor["segment_index"] < row["segment_index"]:
            role = "previous"
        elif neighbor["segment_index"] > row["segment_index"]:
            role = "next"
        items.append(
            {
                "role": role,
                "segment_id": neighbor["id"],
                "segment_index": neighbor["segment_index"],
                "char_range": [neighbor["start_char"], neighbor["end_char"]],
                "text_excerpt": _bounded_context_excerpt(text, role=role, max_chars=max_chars),
            }
        )
    return {
        "purpose": "Resolve speaker continuity and overlap duplicates. Do not use adjacent excerpts as evidence for current-segment events.",
        "current_segment_id": segment_id,
        "segments": items,
    }


def _bounded_context_excerpt(text: str, *, role: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    if role == "previous":
        return text[-max_chars:]
    if role == "next":
        return text[:max_chars]
    half = max_chars // 2
    return text[:half] + "\n[...current segment middle omitted for context budget...]\n" + text[-half:]


def segment_text_by_id(conn, segment_id: str) -> str:
    row = conn.execute("SELECT text_path FROM segments WHERE id = ?", (segment_id,)).fetchone()
    if not row:
        raise ValueError(f"Segment not found: {segment_id}")
    return _read_corpus_text_with_timeout(row["text_path"], error_prefix="segment")


def perform_label_audit(conn, job, *, model: str) -> str:
    payload = loads_json(job["payload_json"], {})
    label = conn.execute("SELECT * FROM labels WHERE id = ?", (job["target_id"],)).fetchone()
    if not label:
        raise ValueError(f"Label not found for audit job: {job['target_id']}")
    segment_context = segment_for_job(conn, {"target_id": label["segment_id"]})
    output = json.loads(label["output_json"])
    result = audit_label_grounding(
        label["label_pack"],
        output,
        segment_text=segment_context["segment_text"],
        expected_context=segment_context["context"],
    )
    audit_id = stable_id(str(job["id"]), job["target_id"], model, prefix="aud_")
    notes = "Deterministic grounding audit completed."
    conn.execute(
        """
        INSERT INTO quality_audits
          (id, label_id, segment_id, label_pack, auditor_model, status, score, disagreement_json, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          status = excluded.status,
          score = excluded.score,
          disagreement_json = excluded.disagreement_json,
          notes = excluded.notes
        """,
        (
            audit_id,
            label["id"],
            label["segment_id"],
            label["label_pack"],
            model,
            result["status"],
            result["score"],
            dumps_json({"checks": result["checks"], "issues": result["issues"], "payload": payload}),
            notes,
            now_iso(),
        ),
    )
    if result["score"] < 0.85:
        conn.execute("UPDATE labels SET needs_review = 1 WHERE id = ?", (label["id"],))
    return audit_id


def _delete_label_derivatives(conn, label_id: str) -> None:
    conn.execute("DELETE FROM quality_audits WHERE label_id = ?", (label_id,))
    claim_ids = [
        row["id"]
        for row in conn.execute("SELECT id FROM claims WHERE label_id = ?", (label_id,)).fetchall()
    ]
    if claim_ids:
        claim_placeholders = ", ".join("?" for _ in claim_ids)
        for table in [
            "claim_cluster_members",
            "claim_subject_members",
            "claim_position_observations",
            "forecast_outcome_checks",
        ]:
            conn.execute(f"DELETE FROM {table} WHERE claim_id IN ({claim_placeholders})", claim_ids)
        for table in ["agreement_edges", "disagreement_edges"]:
            conn.execute(
                f"DELETE FROM {table} WHERE source_claim_id IN ({claim_placeholders}) OR target_claim_id IN ({claim_placeholders})",
                (*claim_ids, *claim_ids),
            )
    observation_ids = [
        row["id"]
        for row in conn.execute("SELECT id FROM coded_observations WHERE label_id = ?", (label_id,)).fetchall()
    ]
    discourse_event_ids = [
        row["id"]
        for row in conn.execute("SELECT id FROM discourse_events WHERE label_id = ?", (label_id,)).fetchall()
    ]
    if observation_ids:
        observation_placeholders = ", ".join("?" for _ in observation_ids)
        for table in [
            "topic_mentions",
            "term_mentions",
            "entity_mentions",
            "speaker_positions",
            "relationship_edges",
            "product_signals",
            "release_signal_links",
        ]:
            conn.execute(f"DELETE FROM {table} WHERE observation_id IN ({observation_placeholders})", observation_ids)
    if discourse_event_ids:
        discourse_placeholders = ", ".join("?" for _ in discourse_event_ids)
        for table in ["claim_position_observations", "claim_subject_event_observations"]:
            conn.execute(
                f"DELETE FROM {table} WHERE discourse_event_id IN ({discourse_placeholders})",
                discourse_event_ids,
            )
        for table in ["raw_speaker_mentions", "raw_actor_mentions", "person_person_mentions"]:
            conn.execute(f"DELETE FROM {table} WHERE discourse_event_id IN ({discourse_placeholders})", discourse_event_ids)
        for table in ["term_usages", "frame_usages", "actor_positions"]:
            conn.execute(f"DELETE FROM {table} WHERE discourse_event_id IN ({discourse_placeholders})", discourse_event_ids)
        conn.execute(f"DELETE FROM discourse_event_contexts WHERE discourse_event_id IN ({discourse_placeholders})", discourse_event_ids)
        conn.execute(f"DELETE FROM discourse_events WHERE id IN ({discourse_placeholders})", discourse_event_ids)
    if observation_ids:
        observation_placeholders = ", ".join("?" for _ in observation_ids)
        conn.execute(f"DELETE FROM coded_observations WHERE id IN ({observation_placeholders})", observation_ids)
    conn.execute("DELETE FROM claims WHERE label_id = ?", (label_id,))


def _claim_like_items(output: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key in ["claims", "market_narratives", "release_signals", "correlation_hypotheses"]:
        values = output.get(key)
        if isinstance(values, list):
            items.extend(item for item in values if isinstance(item, dict))
    for observation in output.get("observations") or []:
        if not isinstance(observation, dict):
            continue
        if observation.get("construct_type") not in {"extracted_claim", "signal", "entity_relation"} and observation.get("code_family") not in {
            "forecast",
            "causal_claim",
            "product_release_signal",
            "market_investment_narrative",
            "adoption_pattern",
            "counterclaim",
        }:
            continue
        items.append(
            {
                "claim_text": observation.get("evidence"),
                "claim_type": observation.get("code_family"),
                "stance": observation.get("stance"),
                "confidence": observation.get("confidence"),
                "evidence": observation.get("evidence"),
            }
        )
    for event in output.get("discourse_events") or []:
        if not isinstance(event, dict):
            continue
        claim_text = event.get("claim_text") or event.get("evidence")
        if not claim_text:
            continue
        if event.get("event_type") not in {
            "forecast",
            "causal_mechanism",
            "capability_claim",
            "product_signal",
            "market_signal",
            "risk_signal",
            "counterclaim",
            "adoption_signal",
            "stance_position",
        }:
            continue
        items.append(
            {
                "claim_text": claim_text,
                "claim_type": event.get("claim_type") or event.get("event_type"),
                "stance": event.get("stance"),
                "confidence": event.get("confidence"),
                "evidence": event.get("evidence"),
            }
        )
    return items


def _insert_discourse_event_derivatives(conn, output: dict[str, Any], *, segment_id: str, label_id: str) -> None:
    insert_discourse_events(conn, output, segment_id=segment_id, label_id=label_id)


def _insert_coded_observations(conn, output: dict[str, Any], *, segment_id: str, label_id: str) -> None:
    observations = output.get("observations")
    if not isinstance(observations, list):
        return
    ts = now_iso()
    for index, observation in enumerate(observations):
        if not isinstance(observation, dict):
            continue
        observation_id = stable_id(
            label_id,
            str(index),
            str(observation.get("code_family")),
            str(observation.get("code_id")),
            str(observation.get("evidence")),
            prefix="obs_",
        )
        entities = observation.get("entities") if isinstance(observation.get("entities"), dict) else {}
        conn.execute(
            """
            INSERT INTO coded_observations
              (id, label_id, segment_id, observation_index, code_family, code_id, construct_type,
               speaker, speaker_role, stance, temporal_horizon, claim_strength, confidence,
               evidence_text, evidence_start, evidence_end, entities_json, status, audit_status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', NULL, ?)
            """,
            (
                observation_id,
                label_id,
                segment_id,
                index,
                observation.get("code_family"),
                observation.get("code_id"),
                observation.get("construct_type"),
                observation.get("speaker"),
                observation.get("speaker_role"),
                observation.get("stance"),
                observation.get("temporal_horizon"),
                observation.get("claim_strength"),
                observation.get("confidence"),
                observation.get("evidence"),
                observation.get("evidence_start"),
                observation.get("evidence_end"),
                dumps_json(entities),
                ts,
            ),
        )
        evidence_json = dumps_json(
            {
                "evidence": observation.get("evidence"),
                "start": observation.get("evidence_start"),
                "end": observation.get("evidence_end"),
                "label_id": label_id,
            }
        )
        _insert_observation_derivatives(conn, observation_id, segment_id, observation, entities, evidence_json, ts)


def _insert_observation_derivatives(
    conn,
    observation_id: str,
    segment_id: str,
    observation: dict[str, Any],
    entities: dict[str, Any],
    evidence_json: str,
    ts: str,
) -> None:
    code_family = observation.get("code_family")
    code_id = observation.get("code_id")
    if code_family == "topic":
        conn.execute(
            "INSERT OR IGNORE INTO topic_mentions (id, observation_id, segment_id, topic, stance, confidence, evidence_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (stable_id(observation_id, "topic", prefix="top_"), observation_id, segment_id, code_id, observation.get("stance"), observation.get("confidence"), evidence_json, ts),
        )
    if code_family == "terminology_drift":
        conn.execute(
            "INSERT OR IGNORE INTO term_mentions (id, observation_id, segment_id, term, term_role, speaker, confidence, evidence_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (stable_id(observation_id, "term", prefix="term_"), observation_id, segment_id, code_id, "drift_signal", observation.get("speaker"), observation.get("confidence"), evidence_json, ts),
        )
    for entity_type, key in [("person", "people"), ("organization", "organizations"), ("product", "products")]:
        for name in _entity_names(entities.get(key) if isinstance(entities, dict) else []):
            conn.execute(
                "INSERT OR IGNORE INTO entity_mentions (id, observation_id, segment_id, entity_type, name, role, confidence, evidence_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    stable_id(observation_id, entity_type, name.lower(), prefix="entm_"),
                    observation_id,
                    segment_id,
                    entity_type,
                    name,
                    code_family,
                    observation.get("confidence"),
                    evidence_json,
                    ts,
                ),
            )
    if observation.get("speaker") or code_family == "actor_org_position":
        conn.execute(
            "INSERT OR IGNORE INTO speaker_positions (id, observation_id, segment_id, speaker, speaker_role, code_id, stance, confidence, evidence_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                stable_id(observation_id, "speaker", str(observation.get("speaker")), prefix="spk_"),
                observation_id,
                segment_id,
                observation.get("speaker"),
                observation.get("speaker_role"),
                code_id,
                observation.get("stance"),
                observation.get("confidence"),
                evidence_json,
                ts,
            ),
        )
    if code_family == "relationship_edge":
        names = _entity_names((entities.get("people") or []) + (entities.get("organizations") or []) + (entities.get("products") or [])) if isinstance(entities, dict) else []
        source_name = names[0] if names else str(observation.get("speaker") or "unknown")
        target_name = names[1] if len(names) > 1 else str(code_id)
        conn.execute(
            "INSERT OR IGNORE INTO relationship_edges (id, observation_id, segment_id, source_name, target_name, relationship, confidence, evidence_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (stable_id(observation_id, "edge", prefix="edge_"), observation_id, segment_id, source_name, target_name, str(code_id), observation.get("confidence"), evidence_json, ts),
        )
    if code_family == "product_release_signal":
        product = _first_entity(entities, "products")
        organization = _first_entity(entities, "organizations")
        conn.execute(
            "INSERT OR IGNORE INTO product_signals (id, observation_id, segment_id, product, organization, signal_type, signal_text, confidence, evidence_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                stable_id(observation_id, "product", prefix="psig_"),
                observation_id,
                segment_id,
                product,
                organization,
                str(code_id),
                str(observation.get("evidence")),
                observation.get("confidence"),
                evidence_json,
                ts,
            ),
        )
        conn.execute(
            "INSERT OR IGNORE INTO release_signal_links (id, observation_id, segment_id, organization, product, signal_type, release_event_id, confidence, evidence_json, created_at) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)",
            (
                stable_id(observation_id, "release", prefix="rsig_"),
                observation_id,
                segment_id,
                organization,
                product,
                str(code_id),
                observation.get("confidence"),
                evidence_json,
                ts,
            ),
        )


def _first_entity(entities: dict[str, Any], key: str) -> str | None:
    names = _entity_names(entities.get(key) if isinstance(entities, dict) else [])
    return names[0] if names else None


def _insert_entities(conn, output: dict[str, Any], *, segment_id: str, label_id: str) -> None:
    entity_sources = []
    entities = output.get("entities") if isinstance(output.get("entities"), dict) else output
    if isinstance(entities, dict):
        entity_sources.append(entities)
    for observation in output.get("observations") or []:
        if isinstance(observation, dict) and isinstance(observation.get("entities"), dict):
            entity_sources.append(observation["entities"])
    for event in output.get("discourse_events") or []:
        if not isinstance(event, dict):
            continue
        entity_sources.append(
            {
                "people": event.get("people") or [],
                "organizations": event.get("organizations") or [],
                "products": (event.get("product_names") or []) + (event.get("model_names") or []),
            }
        )
        actor = event.get("actor")
        if isinstance(actor, dict) and actor.get("name") and actor.get("actor_type") != "unknown":
            if actor.get("actor_type") == "organization":
                entity_sources.append({"organizations": [actor["name"]]})
            else:
                entity_sources.append({"people": [actor["name"]]})
    ts = now_iso()
    organizations = []
    products = []
    people = []
    for entity_source in entity_sources:
        organizations.extend(_entity_names(entity_source.get("organizations") if isinstance(entity_source, dict) else []))
        products.extend(_entity_names(entity_source.get("products") if isinstance(entity_source, dict) else []))
        people.extend(_entity_names(entity_source.get("people") if isinstance(entity_source, dict) else []))
    for name in sorted(set(organizations)):
        org_id = stable_id(name.lower(), prefix="org_")
        conn.execute(
            "INSERT OR IGNORE INTO orgs (id, name, metadata_json, created_at, updated_at) VALUES (?, ?, '{}', ?, ?)",
            (org_id, name, ts, ts),
        )
    for name in sorted(set(products)):
        product_id = stable_id(name.lower(), prefix="prod_")
        conn.execute(
            "INSERT OR IGNORE INTO products (id, name, metadata_json, created_at, updated_at) VALUES (?, ?, '{}', ?, ?)",
            (product_id, name, ts, ts),
        )
    people.extend(_entity_names(output.get("hosts") or []))
    people.extend(_entity_names(output.get("guests") or []))
    people.extend(_entity_names(output.get("mentioned_people") or []))
    for name in sorted(set(people)):
        person_id = stable_id(name.lower(), prefix="per_")
        conn.execute(
            "INSERT OR IGNORE INTO people (id, name, metadata_json, created_at, updated_at) VALUES (?, ?, '{}', ?, ?)",
            (person_id, name, ts, ts),
        )


def _entity_names(values: Any) -> list[str]:
    names = []
    if not isinstance(values, list):
        return names
    for value in values:
        if isinstance(value, str):
            names.append(value)
        elif isinstance(value, dict):
            name = value.get("name") or value.get("person") or value.get("organization") or value.get("product")
            if name:
                names.append(str(name))
    return names
