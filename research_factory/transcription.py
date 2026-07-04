from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path
from typing import Any

from .ingest import record_transcript_acquisition_attempt, store_transcript_text
from .paths import root
from .util import loads_json, now_iso, parse_bool, read_text, stable_id


def run_transcription_job(conn, job, *, lane: str, label_pack: str, worker_id: str) -> dict[str, Any]:
    payload = loads_json(job["payload_json"], {})
    provider = payload.get("provider", "voyager")
    if provider != "voyager":
        raise ValueError(f"Unsupported transcription provider: {provider}")
    run_id = stable_id(str(job["id"]), job["target_id"], provider, prefix="tx_")
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO transcription_runs
          (id, job_id, episode_id, provider, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, 'running', ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          status = 'running',
          error = NULL,
          updated_at = excluded.updated_at
        """,
        (run_id, int(job["id"]), job["target_id"], provider, ts, ts),
    )
    try:
        episode = _eligible_episode_for_transcription(conn, job["target_id"], allow_unverified=bool(payload.get("allow_unverified")))
        if not parse_bool(os.environ.get("ALLOW_PAID_TRANSCRIPTION", "false")):
            raise ValueError("Paid transcription fallback is disabled. Set ALLOW_PAID_TRANSCRIPTION=true only after online transcript discovery is exhausted.")
        text = _transcribe_with_voyager(episode, payload)
        source_url = episode["audio_url"] or f"voyager://{episode['id']}"
        record_transcript_acquisition_attempt(
            conn,
            episode_id=episode["id"],
            method="voyager_transcription",
            status="voyager_transcription_completed",
            result_url=source_url,
            result_source_kind="voyager_transcription",
            official_public=False,
            policy_allowed=True,
            worker_id=worker_id,
            metadata={"provider": provider, "job_id": job["id"]},
        )
        result = store_transcript_text(
            conn,
            episode_id=episode["id"],
            text=text,
            source_kind="voyager_transcription",
            source_url=source_url,
            content_type="text/plain",
            label_pack=payload.get("label_pack", label_pack),
            lane=lane,
        )
        transcript = conn.execute("SELECT raw_text_path FROM transcripts WHERE id = ?", (result["transcript_id"],)).fetchone()
        completed_at = now_iso()
        conn.execute(
            """
            UPDATE transcription_runs
            SET status = 'completed',
                transcript_id = ?,
                output_path = ?,
                completed_at = ?,
                updated_at = ?,
                error = NULL
            WHERE id = ?
            """,
            (result["transcript_id"], transcript["raw_text_path"] if transcript else None, completed_at, completed_at, run_id),
        )
        return {"transcription_run_id": run_id, **result}
    except Exception as exc:
        ts = now_iso()
        conn.execute(
            """
            UPDATE transcription_runs
            SET status = 'failed',
                error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (str(exc)[:1000], ts, run_id),
        )
        raise


def _eligible_episode_for_transcription(conn, episode_id: str, *, allow_unverified: bool = False):
    episode = conn.execute("SELECT * FROM episodes WHERE id = ?", (episode_id,)).fetchone()
    if not episode:
        raise ValueError(f"Episode not found: {episode_id}")
    if not episode["audio_url"] and not allow_unverified:
        raise ValueError(f"Episode has no audio URL for transcription fallback: {episode_id}")
    if allow_unverified:
        return episode
    status = conn.execute(
        """
        SELECT status, eligible_for_transcription
        FROM transcript_acquisition_status
        WHERE episode_id = ?
        """,
        (episode_id,),
    ).fetchone()
    if not status or not int(status["eligible_for_transcription"] or 0):
        raise ValueError("Transcription fallback requires an explicit transcription_eligible acquisition status.")
    return episode


def _transcribe_with_voyager(episode, payload: dict[str, Any]) -> str:
    fixture_text_path = payload.get("fixture_text_path")
    api_key = os.environ.get("VOYAGER_API_KEY")
    if not api_key:
        raise ValueError("VOYAGER_API_KEY is required for Voyager transcription fallback.")
    if fixture_text_path:
        path = Path(fixture_text_path).expanduser()
        if not path.is_absolute():
            path = root() / path
        return read_text(path)
    endpoint = os.environ.get("VOYAGER_TRANSCRIPTION_ENDPOINT")
    if not endpoint:
        raise ValueError("VOYAGER_TRANSCRIPTION_ENDPOINT is not configured.")
    body = json.dumps(
        {
            "episode_id": episode["id"],
            "audio_url": episode["audio_url"],
            "title": episode["title"],
            "source_url": episode["url"],
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        payload = json.loads(response.read().decode("utf-8", errors="replace"))
    text = payload.get("text") or payload.get("transcript")
    if not isinstance(text, str) or len(text.split()) < 20:
        raise ValueError("Voyager transcription response did not include substantive transcript text.")
    return text
